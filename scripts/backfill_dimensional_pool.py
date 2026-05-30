"""Backfill the dimensional comparison pool: score every chain-head salon in
the universe with the deterministic 28-dimension scorer and persist the raw
vectors to `salon_dimensional_scores` in Supabase.

Zero LLM. Pure-function scoring via
`pipelines.competitor_dimensional_scores.compute_all_dimensions_for_salon`,
fed by `SupabaseService.get_competitor_full_data` (the SAME loader the live
competitor pipeline uses — so pool vectors stay consistent with live-computed
vectors). Idempotent: upserts on `booksy_id`, so re-running refreshes in place.

Logic:
  1. Page through `salon_scrapes WHERE is_chain_head=True AND booksy_id IS NOT
     NULL` (PostgREST caps ~1000 rows/req → `.range()` pagination, ordered by
     booksy_id for stable paging). Build a meta map booksy_id → {salon_ref_id,
     salon_city, primary_category_id, scraped_at}. This is the source of
     city/category/scraped_at for the pool row (the bulk loader doesn't return
     all of these).
  2. For each chunk of booksy_ids, bulk-load full salon data via
     get_competitor_full_data, compute dims per salon, skip salons with no
     services (a serviceless vector is meaningless).
  3. Upsert pool rows in batches on conflict `booksy_id`.

Run on tytan (test on a small sample first):
    sudo -u booksy bash -lc 'cd /home/booksy/webapps/bagent-booksyauditor \\
        && set -a && source .env && set +a \\
        && .venv/bin/python scripts/backfill_dimensional_pool.py --limit 50'

Full run (whole universe):
    sudo -u booksy bash -lc 'cd /home/booksy/webapps/bagent-booksyauditor \\
        && set -a && source .env && set +a \\
        && .venv/bin/python scripts/backfill_dimensional_pool.py'
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# CRITICAL: a bare `python script.py` does NOT run the arq startup hook, so
# env (SUPABASE_URL, SUPABASE_SERVICE_KEY, ...) must be loaded manually BEFORE
# importing anything that reads it (config.settings, SupabaseService).
from dotenv import load_dotenv

load_dotenv()

from pipelines.competitor_dimensional_scores import (  # noqa: E402
    compute_all_dimensions_for_salon,
)
from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# How many pool rows to upsert per Supabase round-trip.
UPSERT_BATCH = 500
# PostgREST defaults to 1000 rows per request.
PAGE_SIZE = 1000


def load_chain_head_meta(cli: Any) -> dict[int, dict[str, Any]]:
    """Page through chain-head scrapes and return a meta map keyed by booksy_id.

    Ordered by booksy_id so `.range()` pages don't shift between requests.
    """
    meta: dict[int, dict[str, Any]] = {}
    offset = 0
    while True:
        res = (
            cli.table("salon_scrapes")
            .select("booksy_id, salon_ref_id, salon_city, primary_category_id, scraped_at")
            .eq("is_chain_head", True)
            .not_.is_("booksy_id", "null")
            .order("booksy_id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        chunk = res.data or []
        if not chunk:
            break
        for row in chunk:
            bid = row.get("booksy_id")
            if bid is None:
                continue
            # First write wins; booksy_id is the chain-head unique key so there
            # should be exactly one row per booksy_id anyway.
            meta.setdefault(
                bid,
                {
                    "salon_ref_id": row.get("salon_ref_id"),
                    "salon_city": row.get("salon_city"),
                    "primary_category_id": row.get("primary_category_id"),
                    "scraped_at": row.get("scraped_at"),
                },
            )
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return meta


def _chunked(items: list[int], size: int) -> list[list[int]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


async def main_async(args: argparse.Namespace) -> int:
    svc = SupabaseService()
    cli = svc.client

    logger.info("Loading chain-head meta map...")
    meta = load_chain_head_meta(cli)
    logger.info("Loaded %d chain-head salons", len(meta))

    # booksy_ids to process, ordered for deterministic runs. Apply --limit
    # BEFORE the chunk loop so a test run only fetches N salons' full data.
    booksy_ids = sorted(meta.keys())
    if args.limit:
        booksy_ids = booksy_ids[: args.limit]
    total = len(booksy_ids)
    logger.info(
        "Processing %d salons (chunk=%d, limit=%s)",
        total, args.chunk, args.limit,
    )

    scored = 0
    skipped_no_services = 0
    errors = 0
    pending: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal pending, errors
        if not pending:
            return
        for batch in _chunked_rows(pending, UPSERT_BATCH):
            try:
                cli.table("salon_dimensional_scores").upsert(
                    batch, on_conflict="booksy_id"
                ).execute()
            except Exception as e:  # noqa: BLE001
                errors += len(batch)
                logger.error("Upsert batch of %d rows failed: %s", len(batch), e)
        pending = []

    chunks = _chunked(booksy_ids, args.chunk)
    processed = 0
    for ci, chunk_ids in enumerate(chunks, 1):
        try:
            data = await svc.get_competitor_full_data(chunk_ids)
        except Exception as e:  # noqa: BLE001
            errors += len(chunk_ids)
            logger.error(
                "get_competitor_full_data failed for chunk %d (n=%d): %s — skipping",
                ci, len(chunk_ids), e,
            )
            processed += len(chunk_ids)
            continue

        for bid in chunk_ids:
            salon_data = data.get(bid)
            if salon_data is None:
                # No scrape returned by the loader — count as skip.
                skipped_no_services += 1
                continue
            if not (salon_data.get("services") or []):
                skipped_no_services += 1
                continue
            try:
                dims = compute_all_dimensions_for_salon(salon_data)
            except Exception as e:  # noqa: BLE001
                errors += 1
                logger.error("Scoring failed for booksy_id=%s: %s", bid, e)
                continue

            m = meta.get(bid) or {}
            city_raw = m.get("salon_city")
            row = {
                "booksy_id": bid,
                "salon_ref_id": m.get("salon_ref_id"),
                "city": (city_raw or "").strip().lower() or None,
                "primary_category_id": m.get("primary_category_id"),
                "dims": dims,
                "source": "batch",
                # scraped_at comes from the chain-head meta map (normally the
                # same row the loader scored, but not guaranteed identical).
                "scraped_at": m.get("scraped_at"),
            }
            pending.append(row)
            scored += 1
            if len(pending) >= UPSERT_BATCH:
                flush()

        processed += len(chunk_ids)
        logger.info(
            "[chunk %d/%d] processed=%d/%d scored=%d skipped=%d errors=%d",
            ci, len(chunks), processed, total, scored,
            skipped_no_services, errors,
        )

    flush()

    logger.info(
        "DONE. total=%d scored=%d skipped_no_services=%d errors=%d",
        total, scored, skipped_no_services, errors,
    )
    return 0


def _chunked_rows(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Backfill salon_dimensional_scores for all chain-head salons.",
    )
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N salons (for a small test run).",
    )
    ap.add_argument(
        "--chunk", type=int, default=200,
        help="How many booksy_ids to bulk-load per get_competitor_full_data call.",
    )
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
