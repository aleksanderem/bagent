"""Backfill salons.{portfolio_embedding, focus_distribution, focus_variant_distribution}
dla wszystkich salonów które:
  - nie mają focus_computed_at (NULL) — fresh compute
  - mają focus_computed_at starsze niż --stale-after-days

Batched: 50 salonów per iteration. ETA na pełne 51K salonów: ~30-40 min.

Uruchomienie:
  cd /home/booksy/webapps/bagent-booksyauditor
  sudo -u booksy bash -lc '.venv/bin/python scripts/backfill_salon_focus.py'

Opcje:
  --stale-after-days N  refresh salonów ze starym computed_at (default 14)
  --limit N             max salons to process (debug, default unlimited)
  --batch-size N        salons per batch (default 50; mniejsza = mniejsze
                        zużycie pamięci, większa = szybciej)
  --dry-run             print stats but don't write
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any

sys.path.insert(0, "/home/booksy/webapps/bagent-booksyauditor")
from dotenv import load_dotenv

load_dotenv("/home/booksy/webapps/bagent-booksyauditor/.env")

from supabase import create_client

from services.focus_score import SalonFocusBundle

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _paginated_select(client, query_builder, page_size: int = 1000) -> list[dict]:
    """Paginate przez Supabase REST query. Supabase default page = 1000 rows.
    Range queries via .range(start, end) iterate aż empty page.
    """
    out: list[dict] = []
    start = 0
    while True:
        res = query_builder(start, start + page_size - 1).execute()
        rows = res.data or []
        out.extend(rows)
        if len(rows) < page_size:
            break
        start += page_size
    return out


def get_stale_salon_ids(
    client, stale_after_days: int, limit: int | None,
) -> list[int]:
    """Get salon_ids potrzebujących compute: focus_computed_at NULL lub stare.

    Używa range-based pagination żeby ominąć Supabase REST default 1000 row cap.
    """
    null_ids: list[int] = []
    null_ids = [
        r["id"]
        for r in _paginated_select(
            client,
            lambda s, e: client.table("salons").select("id")
                .is_("focus_computed_at", "null").range(s, e),
        )
    ]

    old_ids: list[int] = []
    if stale_after_days > 0:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(days=stale_after_days)).isoformat()
        old_ids = [
            r["id"]
            for r in _paginated_select(
                client,
                lambda s, e: client.table("salons").select("id")
                    .lt("focus_computed_at", cutoff).range(s, e),
            )
        ]

    ids = list(set(null_ids + old_ids))
    if limit:
        ids = ids[:limit]
    return ids


def load_salon_services(client, salon_id: int) -> tuple[list[dict], list[str]] | None:
    """Load salon's latest chain head scrape services + top_service_names.

    Returns (services, top_service_names) lub None gdy salon ma 0 chain head scrapes.
    """
    # Get booksy_id + top_service_names z salons
    salon_res = client.table("salons").select(
        "id, booksy_id, top_service_names",
    ).eq("id", salon_id).single().execute()
    if not salon_res.data:
        return None
    booksy_id = salon_res.data.get("booksy_id")
    top_names = salon_res.data.get("top_service_names") or []
    if booksy_id is None:
        return None

    # Latest chain head scrape
    scrape_res = client.table("salon_scrapes").select("id").eq(
        "booksy_id", booksy_id,
    ).eq("is_chain_head", True).order(
        "scraped_at", desc=True,
    ).limit(1).execute()
    if not scrape_res.data:
        return None
    scrape_id = scrape_res.data[0]["id"]

    # Services
    svc_res = client.table("salon_scrape_services").select(
        "id, name, description, photos, booksy_treatment_id, variant_id, "
        "price_grosze, name_embedding",
    ).eq("scrape_id", scrape_id).execute()
    services = svc_res.data or []
    return services, top_names


def compute_and_write_bundle(
    client, salon_id: int, dry_run: bool = False,
) -> tuple[bool, int, int]:
    """Compute + write bundle dla 1 salonu.

    Returns: (success, service_count, embedded_count)
    """
    loaded = load_salon_services(client, salon_id)
    if loaded is None:
        return False, 0, 0
    services, top_names = loaded

    bundle = SalonFocusBundle.from_services(
        salon_id=salon_id,
        booksy_id=None,
        services=services,
        top_service_names=top_names,
    )

    if dry_run:
        return True, bundle.service_count, bundle.embedded_count

    # Write back
    payload = bundle.to_db_payload()
    # focus_computed_at = NOW() handled by SQL `now()` literal not supported
    # via supabase-py update — explicitly set ISO timestamp
    from datetime import datetime, timezone
    payload["focus_computed_at"] = datetime.now(timezone.utc).isoformat()

    client.table("salons").update(payload).eq("id", salon_id).execute()
    return True, bundle.service_count, bundle.embedded_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stale-after-days", type=int, default=14)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = create_client(
        os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"),
    )

    logger.info("Discovering stale salons (stale_after_days=%d, limit=%s)...",
                args.stale_after_days, args.limit)
    salon_ids = get_stale_salon_ids(client, args.stale_after_days, args.limit)
    total = len(salon_ids)
    logger.info("Found %d salons needing compute", total)

    if total == 0:
        logger.info("Nothing to do")
        return

    t_start = time.time()
    success = 0
    skipped = 0
    total_services = 0
    total_embedded = 0

    for i, salon_id in enumerate(salon_ids):
        try:
            ok, svc_count, emb_count = compute_and_write_bundle(
                client, salon_id, dry_run=args.dry_run,
            )
            if ok:
                success += 1
                total_services += svc_count
                total_embedded += emb_count
            else:
                skipped += 1
        except Exception as e:
            logger.warning("Failed to process salon_id=%s: %s", salon_id, e)
            skipped += 1

        if (i + 1) % args.batch_size == 0 or i == total - 1:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / rate if rate > 0 else 0
            logger.info(
                "Progress: %d/%d  success=%d skipped=%d  rate=%.1f/s  ETA=%.0fs",
                i + 1, total, success, skipped, rate, eta,
            )

    dt = time.time() - t_start
    logger.info(
        "DONE in %.0fs: %d success (%d skipped), avg %.1f svcs/salon, "
        "avg %.1f embedded/salon, %.1f salons/sec",
        dt, success, skipped,
        total_services / max(1, success),
        total_embedded / max(1, success),
        success / max(0.1, dt),
    )


if __name__ == "__main__":
    main()
