#!/usr/bin/env python3
"""Rebuild ALL treatment_methods embeddings WITHOUT aliases.

One-shot fixup for the cascading-poisoning issue surfaced 2026-05-21:
the original mig 095 / seed_treatment_methods.py embedded methods using
canonical_name + display_name + "Aliases: ..." + description, which
made the vector for kroplowka_iv learn the semantics of the generic
alias 'infuzja' (later identified as poisoning) and produce false
high-cosine matches to unrelated services like "Infuzja tlenowa".

This script rebuilds every method's embedding using ONLY:
    canonical_name + display_name + description

Aliases stay in the table column (they're still used by the
alias_exact / regex_exact hard-match tiers in the classifier) — they
just stop polluting the semantic vector.

Usage on tytan:
    cd /home/booksy/webapps/bagent-booksyauditor
    sudo -u booksy bash -lc '.venv/bin/python scripts/reembed_methods_no_aliases.py --dry-run --limit 5'
    sudo -u booksy bash -lc 'nohup .venv/bin/python scripts/reembed_methods_no_aliases.py > /tmp/reembed_methods.log 2>&1 &'
    tail -f /tmp/reembed_methods.log

Pre-condition: snapshot of treatment_methods.embedding to a backup table
was created — see migration `supabase/migrations/110_treatment_methods_embedding_backup.sql`.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from openai import OpenAI  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("reembed_methods_no_aliases")

EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 100  # OpenAI embeddings batch-friendly
DB_PAGE_SIZE = 1000  # supabase select pagination


def _embedding_input(row: dict) -> str:
    """Build embed text — canonical_name + display_name + description.

    DELIBERATELY does NOT include aliases. See module docstring.
    """
    parts = [
        row["canonical_name"],
        row.get("display_name") or "",
    ]
    desc = (row.get("description") or "").strip()
    if desc:
        parts.append(desc)
    return "\n".join(p for p in parts if p)


def _embed_batch(openai_client: OpenAI, texts: list[str]) -> list[list[float]]:
    """Single OpenAI batch embed call. Returns 1536-dim vectors in order."""
    resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def _pgvector_str(vec: list[float]) -> str:
    """pgvector accepts text format [v1,v2,...]"""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def _fetch_all_methods(supabase: SupabaseService, since_id: int | None, limit: int | None) -> list[dict]:
    """Paginate through treatment_methods, return id+canonical_name+display_name+description."""
    rows: list[dict] = []
    offset = 0
    while True:
        page_size = DB_PAGE_SIZE
        if limit is not None:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            page_size = min(page_size, remaining)

        q = (
            supabase.client.table("treatment_methods")
            .select("id, canonical_name, display_name, description")
            .order("id")
        )
        if since_id is not None:
            q = q.gt("id", since_id)
        res = q.range(offset, offset + page_size - 1).execute()
        batch = res.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build inputs and call OpenAI embed for first batch only — print sample, do NOT write to DB.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process first N methods (smoke test).",
    )
    parser.add_argument(
        "--since-id", type=int, default=None,
        help="Resume: skip methods with id <= this (set to last successful id from a previous run).",
    )
    args = parser.parse_args()

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("OPENAI_API_KEY not set")
        return 1
    openai_client = OpenAI(api_key=openai_key)

    supabase = SupabaseService()

    logger.info(
        "Fetching methods (since_id=%s, limit=%s) …",
        args.since_id, args.limit,
    )
    rows = _fetch_all_methods(supabase, since_id=args.since_id, limit=args.limit)
    logger.info("Fetched %d method rows for re-embedding.", len(rows))

    if not rows:
        logger.info("Nothing to do.")
        return 0

    # Sanity print of first 3 inputs
    for r in rows[:3]:
        logger.info(
            "Sample input (id=%s, canonical=%s):\n%s",
            r["id"], r["canonical_name"], _embedding_input(r),
        )

    t0 = time.time()
    updated = 0
    failed = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        texts = [_embedding_input(r) for r in chunk]
        try:
            vecs = _embed_batch(openai_client, texts)
        except Exception as e:
            logger.error(
                "OpenAI embed batch %d-%d failed: %s — sleeping 30s and retrying once",
                i + 1, i + len(chunk), e,
            )
            time.sleep(30)
            try:
                vecs = _embed_batch(openai_client, texts)
            except Exception as e2:
                logger.error("Retry also failed: %s. Aborting at id=%s.", e2, chunk[0]["id"])
                return 1

        if args.dry_run:
            logger.info(
                "[DRY RUN] Would write %d embeddings. First vec sample (id=%s, first 4 dims): %s",
                len(vecs), chunk[0]["id"], vecs[0][:4],
            )
            # Only do one batch in dry-run mode to save API cost
            return 0

        # Write back row-by-row (supabase-py + pgvector REST quirks)
        for row, vec in zip(chunk, vecs, strict=True):
            try:
                supabase.client.table("treatment_methods").update(
                    {"embedding": _pgvector_str(vec)}
                ).eq("id", row["id"]).execute()
                updated += 1
            except Exception as e:
                logger.error("Update failed for id=%s (%s): %s", row["id"], row["canonical_name"], e)
                failed += 1

        elapsed = time.time() - t0
        rate = updated / elapsed if elapsed > 0 else 0
        eta = (len(rows) - updated) / rate if rate > 0 else 0
        logger.info(
            "Progress: %d/%d updated (failed=%d) — %.1f/s — ETA %.1f min",
            updated, len(rows), failed, rate, eta / 60,
        )

    logger.info(
        "Done. updated=%d failed=%d total=%d elapsed=%.1fs",
        updated, failed, len(rows), time.time() - t0,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
