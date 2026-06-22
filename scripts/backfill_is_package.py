#!/usr/bin/env python3
"""Backfill salon_scrape_services.is_package (migracja 136).

Byte-identyczna logika z ingest (_insert_services): detect_package_keyword na
nazwie usługi. Batched po id, idempotentny (UPDATE tylko gdy flaga != żądana),
off-peak friendly (pg_sleep między batchami). Świadomie BEZ SQL-regexu —
eliminuje ryzyko rozjazdu PG \\y vs Python \\b; jedno źródło logiki = detektor.

Uruchomienie:
  python -m scripts.backfill_is_package --dry-run          # tylko policz, nie zapisuj
  python -m scripts.backfill_is_package                    # wykonaj
  python -m scripts.backfill_is_package --batch 5000       # rozmiar batcha id
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.sb_client import make_supabase_client  # noqa: E402
from services.pricing_verification import detect_package_keyword  # noqa: E402

DEFAULT_BATCH = 5000
UPDATE_CHUNK = 200  # ile id na jeden PATCH (krótkie URL-e PostgREST)


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill salon_scrape_services.is_package")
    ap.add_argument("--dry-run", action="store_true", help="policz bez zapisu")
    ap.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="rozmiar batcha po id")
    ap.add_argument("--sleep", type=float, default=0.1, help="pauza między batchami (s)")
    args = ap.parse_args()

    sb = make_supabase_client()

    res = (
        sb.table("salon_scrape_services")
        .select("id")
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    max_id = int(res.data[0]["id"]) if res.data else 0
    print(f"[backfill_is_package] max_id={max_id} batch={args.batch} dry_run={args.dry_run}")

    cur = 0
    total_scanned = 0
    total_to_flag = 0
    total_to_unflag = 0
    while cur < max_id:
        hi = cur + args.batch
        rows = (
            sb.table("salon_scrape_services")
            .select("id, name, is_package")
            .gt("id", cur)
            .lte("id", hi)
            .execute()
            .data
            or []
        )
        to_flag: list[int] = []
        to_unflag: list[int] = []
        for r in rows:
            want = detect_package_keyword(r.get("name") or "") is not None
            have = bool(r.get("is_package"))
            if want and not have:
                to_flag.append(int(r["id"]))
            elif not want and have:
                to_unflag.append(int(r["id"]))

        total_scanned += len(rows)
        total_to_flag += len(to_flag)
        total_to_unflag += len(to_unflag)

        if not args.dry_run:
            for ids, val in ((to_flag, True), (to_unflag, False)):
                for i in range(0, len(ids), UPDATE_CHUNK):
                    chunk = ids[i : i + UPDATE_CHUNK]
                    if chunk:
                        (
                            sb.table("salon_scrape_services")
                            .update({"is_package": val})
                            .in_("id", chunk)
                            .execute()
                        )
            if rows:
                time.sleep(args.sleep)

        if rows:
            print(
                f"  id {cur}-{hi}: scanned={len(rows)} +flag={len(to_flag)} "
                f"-flag={len(to_unflag)} | cumulative flagged={total_to_flag}"
            )
        cur = hi

    print(
        f"[backfill_is_package] DONE scanned={total_scanned} "
        f"to_flag={total_to_flag} to_unflag={total_to_unflag} "
        f"{'(dry-run, nic nie zapisano)' if args.dry_run else ''}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
