"""Backfill `treatment_variants.brand_marker` from constituent services.

For each variant in `treatment_variants`, this script loads all active
salon_scrape_services rows currently pointing at that variant_id,
extracts brand_marker per service via the deterministic regex in
services.brand_marker.extract_brand_marker, and writes the dominant
brand_marker back to the variant.

Rules for "dominant":
  - Skip services with brand_marker = None.
  - Among services with a brand_marker, count occurrences.
  - If the most-common brand has >= 60% share of brand-tagged services AND
    >= 3 services, it becomes the variant's brand_marker.
  - Otherwise variant.brand_marker stays NULL (generic / mixed).

Idempotent: re-running overwrites any existing brand_marker value with
the freshly-computed dominant. Use `--dry-run` to print proposed changes
without writing.

Run on tytan:
    sudo -u booksy bash -lc 'cd /home/booksy/webapps/bagent-booksyauditor \\
        && set -a && source .env && set +a \\
        && .venv/bin/python scripts/backfill_variant_brand_marker.py'
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from services.brand_marker import extract_brand_marker
from services.supabase import SupabaseService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Threshold knobs (mirrored in migration 088 comment).
MIN_BRAND_TAGGED_SERVICES = 3
MIN_DOMINANT_SHARE = 0.60


def compute_dominant_brand(service_names: list[str]) -> tuple[str | None, dict]:
    """Return (dominant_brand_or_None, stats) for a variant's services."""
    counter: Counter[str] = Counter()
    untagged = 0
    for name in service_names:
        bm = extract_brand_marker(name or "")
        if bm is None:
            untagged += 1
        else:
            counter[bm] += 1
    total_tagged = sum(counter.values())
    stats = {
        "total_services": len(service_names),
        "untagged": untagged,
        "tagged": total_tagged,
        "distribution": dict(counter),
    }
    if total_tagged < MIN_BRAND_TAGGED_SERVICES:
        return None, stats
    top_brand, top_count = counter.most_common(1)[0]
    share = top_count / total_tagged
    if share < MIN_DOMINANT_SHARE:
        return None, stats
    stats["dominant"] = top_brand
    stats["dominant_share"] = share
    return top_brand, stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print proposed updates without writing")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N variants (for testing)")
    parser.add_argument("--variant-id", type=int, default=None,
                        help="Process only the variant with this id (debug)")
    args = parser.parse_args()

    svc = SupabaseService()
    cli = svc.client

    # 1. Load all variants. PostgREST defaults to 1000 rows per request, so
    # paginate via .range(offset, offset+999) until we get a short page.
    variants: list[dict] = []
    if args.variant_id:
        res = (
            cli.table("treatment_variants")
            .select("id, parent_treatment_id, canonical_variant_name, sample_count, brand_marker")
            .eq("id", args.variant_id)
            .execute()
        )
        variants = res.data or []
    else:
        page_size = 1000
        offset = 0
        while True:
            res = (
                cli.table("treatment_variants")
                .select("id, parent_treatment_id, canonical_variant_name, sample_count, brand_marker")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            chunk = res.data or []
            if not chunk:
                break
            variants.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
    if args.limit:
        variants = variants[:args.limit]
    logger.info("Loaded %d variants to process", len(variants))

    # 2. For each variant, load constituent services (active, chain-head only
    #    for cleanest signal). Use bulk fetch to avoid N round-trips.
    updated = 0
    cleared = 0
    unchanged = 0
    for i, v in enumerate(variants, 1):
        vid = v["id"]
        # Load services that point at this variant via salon_scrape_services
        res = (
            cli.table("salon_scrape_services")
            .select("name")
            .eq("variant_id", vid)
            .eq("is_active", True)
            .limit(500)
            .execute()
        )
        service_names = [r["name"] for r in (res.data or [])]
        if not service_names:
            unchanged += 1
            continue
        dominant, stats = compute_dominant_brand(service_names)
        current = v.get("brand_marker")
        if dominant == current:
            unchanged += 1
            if i % 100 == 0:
                logger.info("[%d/%d] vid=%s no change (brand=%r)",
                            i, len(variants), vid, current)
            continue
        action = "UPDATE" if dominant is not None else "CLEAR"
        logger.info(
            "[%d/%d] %s vid=%s tid=%s %r → %r (services=%d, tagged=%d, "
            "dist=%s)",
            i, len(variants), action, vid, v["parent_treatment_id"],
            current, dominant, stats["total_services"], stats["tagged"],
            stats.get("distribution"),
        )
        if not args.dry_run:
            cli.table("treatment_variants").update(
                {"brand_marker": dominant}
            ).eq("id", vid).execute()
        if dominant is None:
            cleared += 1
        else:
            updated += 1

    logger.info(
        "Done. updated=%d cleared=%d unchanged=%d (dry_run=%s)",
        updated, cleared, unchanged, args.dry_run,
    )


if __name__ == "__main__":
    main()
