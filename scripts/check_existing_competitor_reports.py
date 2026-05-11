"""Check existing competitor_reports rows for the audit candidates."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings
from services.sb_client import make_supabase_client


def main() -> int:
    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    audit_ids = [
        "j574zcqr4xb7pgfp46b865ajs185857v",
        "j57e45tp7cjm8yhprz093ds05d84jd9s",
        "j57cx6t437kqmdwr605bb7sd6184gp1d",
        "j5796vaa2zbzn30bay75xv32d184gef9",
    ]

    rows = (
        client.table("competitor_reports")
        .select("id, convex_audit_id, status, tier, competitor_count, created_at, updated_at")
        .in_("convex_audit_id", audit_ids)
        .order("created_at", desc=True)
        .execute()
        .data
        or []
    )

    print(f"Found {len(rows)} competitor_reports for {len(audit_ids)} audit_ids\n")
    print(f"{'id':>4}  {'audit_id':<40}  {'status':<12}  {'tier':<8}  {'#':>3}  created")
    print("-" * 110)
    for r in rows:
        print(
            f"{r['id']:>4}  {r['convex_audit_id']:<40}  "
            f"{r.get('status') or '':<12}  {r.get('tier') or '':<8}  "
            f"{r.get('competitor_count') or 0:>3}  {r.get('created_at') or ''}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
