"""Embed all 2.28M services in salon_scrape_services using OpenAI
text-embedding-3-small. Bulk-update via Postgres RPC (single transaction
per batch, no N+1 HTTP). Parallel OpenAI calls for throughput.

Idempotent: skips rows where embedding_applied_at IS NOT NULL.
Resumable: kill anytime, restart picks up where left off.
"""

from __future__ import annotations

import concurrent.futures
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from config import settings
from services.sb_client import make_supabase_client


BATCH_FETCH = 5000   # rows pulled from DB per round
BATCH_EMBED = 500    # rows per OpenAI request (max 2048)
PARALLELISM = 6      # concurrent OpenAI requests

# Rich embedding cap — text-embedding-3-small handles up to ~8K tokens.
# Empirycznie: name ~30-100 znaków, description ~800-2500 dla rozbudowanych
# katalogów (Beauty4ever ma opisy 989-2537 zn.). Cap 1500 znaków łącznie
# daje embedding bogaty w kontekst (brand+procedura+technologia) ale
# trzyma latency przewidywalne.
INPUT_CHAR_CAP = 1500


def _build_input(row: dict) -> str:
    """Compose embedding input from name + description.

    Wcześniej embed brał tylko name. Dla salonów które mają custom branded
    services (Plexr, Thunder, Onda, Light&Bright, EMBODY, X-Wave, DR CYJ,
    Red Touch — Beauty4ever ma ich kilkadziesiąt), name jest niejasny
    ("Onda 4 zabiegi"), a opis explicite mówi co to za zabieg
    ("Onda – modelowanie sylwetki za pomocą technologii Coolwaves").
    Włączenie description w embedding sprawia że matching variant
    clustering / opportunity overlap działa też dla custom names —
    "Thunder Całe ciało" zostanie z embedding podobny do "Depilacja
    laserowa całe ciało" u konkurenta.
    """
    name = (row.get("name") or "").strip()
    desc = (row.get("description") or "").strip()
    if not desc:
        return name[:INPUT_CHAR_CAP]
    # Name+description z separatorem; obcięcie do INPUT_CHAR_CAP zostawia
    # priorytet dla name (jest first), opis dopełnia kontekst.
    combined = f"{name}. {desc}"
    return combined[:INPUT_CHAR_CAP]


def embed_chunk(oai: OpenAI, rows: list[dict]) -> list[dict]:
    inputs = [_build_input(r) for r in rows]
    try:
        resp = oai.embeddings.create(model="text-embedding-3-small", input=inputs)
    except Exception as e:
        # One retry after brief sleep — typical rate-limit relief
        print(f"  OpenAI error: {e}; retry after 3s", flush=True)
        time.sleep(3)
        resp = oai.embeddings.create(model="text-embedding-3-small", input=inputs)
    return [
        {"id": r["id"], "embedding": list(ed.embedding)}
        for r, ed in zip(rows, resp.data)
    ]


def main() -> int:
    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    oai = OpenAI()

    total_done = 0
    t_start = time.time()
    round_idx = 0

    while True:
        rows = (
            client.table("salon_scrape_services")
            .select("id,name,description")
            .is_("embedding_applied_at", "null")
            .not_.is_("name", "null")
            .order("id")
            .limit(BATCH_FETCH)
            .execute()
            .data
        )
        rows = [r for r in (rows or []) if r.get("name") and len(r["name"].strip()) >= 3]
        if not rows:
            print(f"\nDone. Total embedded: {total_done} in {time.time()-t_start:.1f}s", flush=True)
            return 0

        round_idx += 1
        # Split into chunks for parallel embedding
        chunks = [rows[i:i + BATCH_EMBED] for i in range(0, len(rows), BATCH_EMBED)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
            results = list(pool.map(lambda c: embed_chunk(oai, c), chunks))

        # Bulk update via RPC — single transaction per chunk
        for chunk_payload in results:
            client.rpc("bulk_update_service_embeddings", {"payloads": chunk_payload}).execute()

        total_done += sum(len(c) for c in chunks)
        elapsed = time.time() - t_start
        rate = total_done / max(elapsed, 1)
        print(
            f"  round {round_idx}: total={total_done} | rate={rate:.0f}/s | elapsed={elapsed:.0f}s",
            flush=True,
        )


if __name__ == "__main__":
    sys.exit(main())
