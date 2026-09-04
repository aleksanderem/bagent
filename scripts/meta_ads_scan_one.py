"""Ręczny skan Ad Library dla jednego salonu — bez czekania na nocny cron 06:15.

Robi to samo, co workers.meta_ads_refresh.meta_ads_refresh_cron, ale tylko dla
wskazanego salon_ref_id: kaskada znajdowania stron FB (widok
v_meta_ads_discovery_targets: Booksy → crawl WWW → wyszukiwarka) → resolve
oczekujących stron → skan reklam → historia → wnioski AI. Alertów NIE wysyła
(pierwszy skan i tak ich nie daje — baseline-guard).

Użycie na tytanie (jako booksy, z .env w środowisku):
    set -a && source .env && set +a
    uv run python scripts/meta_ads_scan_one.py 9401
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

# Skrypt odpalany po ścieżce (`uv run python scripts/…`) nie ma korzenia repo
# w sys.path — bez tego `import workers` pada (jak w innych scripts/backfill_*).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from workers.meta_ads_refresh import (  # noqa: E402
    _discover_targets,
    _get_client,
    _ingest_history,
    _refresh_insights,
    _resolve_pending,
    _scan_salon,
)


async def main(salon_ref_id: int) -> int:
    sb = _get_client()
    async with httpx.AsyncClient() as http:
        print("kaskada znajdowania stron FB:", await _discover_targets(sb, http))
        pending = (
            sb.table("salon_meta_pages")
            .select("salon_ref_id, facebook_url, facebook_source")
            .eq("resolve_status", "pending")
            .execute()
            .data
            or []
        )
        print(
            "oczekujące na resolve:",
            [(r["salon_ref_id"], r["facebook_url"], r.get("facebook_source")) for r in pending],
        )
        await _resolve_pending(sb, http)
        rows = (
            sb.table("salon_meta_pages")
            .select(
                "salon_ref_id, booksy_id, page_id, page_name, resolve_status, resolve_error, "
                "facebook_source"
            )
            .eq("salon_ref_id", salon_ref_id)
            .execute()
            .data
            or []
        )
        row = rows[0] if rows else None
        print("strona FB:", row)
        if not row:
            print("salon nie jest celem skanu — sprawdź v_meta_ads_scan_targets")
            return 2
        if row["resolve_status"] == "mismatch":
            print("strona znaleziona, ale nazwa nie pasuje do salonu — do ręcznego sprawdzenia:",
                  row.get("resolve_error"))
            return 4
        if row["resolve_status"] != "resolved":
            print("strony nie udało się rozwiązać:", row.get("resolve_error"))
            return 3

        alerts = await _scan_salon(sb, http, row)
        print("alerty (nie wysyłam):", len(alerts))
        await _ingest_history(sb, http, row)
        await _refresh_insights(sb, salon_ref_id, row.get("page_name") or "Salon")

    ads = (
        sb.table("salon_meta_ads")
        .select("ad_archive_id, is_active")
        .eq("salon_ref_id", salon_ref_id)
        .execute()
        .data
        or []
    )
    print(f"reklamy w bazie: {len(ads)}, aktywne: {sum(1 for a in ads if a['is_active'])}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print("użycie: uv run python scripts/meta_ads_scan_one.py <salon_ref_id>")
        sys.exit(1)
    sys.exit(asyncio.run(main(int(sys.argv[1]))))
