"""One-shot priority embedding backfill for chain-head services.

Why this exists: the nightly cron `embed_new_services` iterates services
by `ORDER BY id` indiscriminately, so older salons (low booksy_id) stay
without embeddings far longer than necessary. For the upcoming variant-
clustering work we need 100% embedding coverage on CHAIN HEAD services
(active state) before any pricing comparison can run.

Strategy:
  1. Fetch chain head services with embedding_applied_at IS NULL,
     ordered by scraped_at DESC (newest snapshots first).
  2. Batch by 1000, embed via OpenAI (parallel 8 workers), upsert
     via bulk_update_service_embeddings RPC.
  3. Loop until nothing left to embed.

Run with:
  cd /home/booksy/webapps/bagent-booksyauditor
  .venv/bin/python scripts/embed_chain_heads_priority.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

# Make `services` and `config` importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


BATCH_FETCH = 2000
BATCH_EMBED = 1000
PARALLELISM = 8


async def main() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY missing — aborting")
        sys.exit(1)

    from openai import AsyncOpenAI
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    oai = AsyncOpenAI()
    sem = asyncio.Semaphore(PARALLELISM)
    t_start = time.time()
    total_done = 0

    async def embed_chunk(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        names = [r["name"].strip()[:512] for r in rows]
        async with sem:
            for attempt in (1, 2, 3):
                try:
                    resp = await oai.embeddings.create(
                        model="text-embedding-3-small", input=names,
                    )
                    break
                except Exception as exc:
                    logger.warning(
                        "OpenAI embedding error (attempt %d): %s", attempt, exc,
                    )
                    if attempt == 3:
                        raise
                    await asyncio.sleep(2 ** attempt)
        return [
            {"id": r["id"], "embedding": list(d.embedding)}
            for r, d in zip(rows, resp.data)
        ]

    while True:
        # Priority query: chain head services first, newest scrapes first.
        # Uses sql RPC to avoid Supabase client query limitations on JOINs.
        # The RPC must exist as `get_chain_head_services_missing_embedding(p_limit)`
        # — see migration 056 if not present.
        rows = await asyncio.to_thread(
            lambda: client.rpc(
                "get_chain_head_services_missing_embedding",
                {"p_limit": BATCH_FETCH},
            ).execute().data
        )
        rows = [r for r in (rows or []) if r.get("name") and len(r["name"].strip()) >= 3]
        if not rows:
            logger.info("No more chain head services to embed — done.")
            break

        chunks = [rows[i:i + BATCH_EMBED] for i in range(0, len(rows), BATCH_EMBED)]
        try:
            results = await asyncio.gather(*(embed_chunk(c) for c in chunks))
        except Exception as exc:
            logger.error("Failed batch after retries: %s — sleeping 30s", exc)
            await asyncio.sleep(30)
            continue

        for payload in results:
            await asyncio.to_thread(
                lambda p=payload: client.rpc(
                    "bulk_update_service_embeddings", {"payloads": p},
                ).execute()
            )

        total_done += sum(len(c) for c in chunks)
        dt = time.time() - t_start
        rate = total_done / max(dt, 1)
        logger.info("progress: %d embedded in %.0fs (%.0f/s)", total_done, dt, rate)

    dt = time.time() - t_start
    logger.info("done: %d total in %.0fs (%.0f/s)", total_done, dt, total_done / max(dt, 1))


if __name__ == "__main__":
    asyncio.run(main())
