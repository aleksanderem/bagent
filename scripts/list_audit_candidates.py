"""List salons in Supabase that already have a salon_scrapes row tied to a
convex_audit_id — those are candidates for running the competitor report
pipeline against (since the pipeline keys off convex_audit_id).

Usage:
    cd /Users/alex/Desktop/MOJE_PROJEKTY/bagent
    .venv/bin/python -m scripts.list_audit_candidates --limit 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings
from services.sb_client import make_supabase_client


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--min-services", type=int, default=10)
    args = ap.parse_args()

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Latest salon_scrapes that have convex_audit_id set
    rows = (
        client.table("salon_scrapes")
        .select(
            "id, convex_audit_id, booksy_id, salon_name, salon_city,"
            " primary_category_id, total_services, salon_lat, salon_lng,"
            " partner_system, scraped_at"
        )
        .not_.is_("convex_audit_id", "null")
        .gte("total_services", args.min_services)
        .not_.is_("salon_lat", "null")
        .not_.is_("salon_lng", "null")
        .order("scraped_at", desc=True)
        .limit(args.limit)
        .execute()
        .data
        or []
    )

    if not rows:
        print("No audit candidates found.")
        return 1

    print(f"{'audit_id':<40}  {'booksy_id':>9}  {'svc':>4}  {'city':<20}  name")
    print("-" * 110)
    for r in rows:
        aid = (r.get("convex_audit_id") or "")[:38]
        bid = r.get("booksy_id")
        svc = r.get("total_services") or 0
        city = (r.get("salon_city") or "")[:20]
        name = (r.get("salon_name") or "")[:50]
        print(f"{aid:<40}  {bid:>9}  {svc:>4}  {city:<20}  {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
