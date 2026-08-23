"""Dry-run Fazy 8a (koszyki wg pokrycia menu) dla istniejącego raportu — TYLKO ODCZYT.

Wywołuje PRAWDZIWĄ `_aggregate_verified_match_counts` z zapisem podmienionym
na no-op i drukuje, jakie koszyki dostałby każdy wybrany konkurent.

Użycie: PYTHONPATH=. uv run python scripts/dryrun_phase8a_buckets.py <report_id>
Wymaga .env z Supabase + Qdrant. Subject bez name_embedding (stare audit
scrape'y, BEAUTY_AUDIT-0rp6) → re-bucket pominięty, tak jak w pipeline.
"""
import asyncio
import sys
from collections import Counter
from types import SimpleNamespace

from dotenv import load_dotenv

load_dotenv()

from pipelines.competitor_analysis import (  # noqa: E402
    _aggregate_verified_match_counts,
    _apply_versum_mappings,
)
from services.supabase import SupabaseService  # noqa: E402


class _ReadOnly:
    """SupabaseService z no-op zapisem koszyków."""

    def __init__(self, svc: SupabaseService) -> None:
        self._svc = svc
        self.updates: list[dict] = []

    def __getattr__(self, name):
        return getattr(self._svc, name)

    async def update_competitor_matches_verify_buckets(self, report_id, updates):
        self.updates = list(updates)


async def main(report_id: int) -> None:
    svc = SupabaseService()
    rep = (
        svc.client.table("competitor_reports").select("convex_audit_id")
        .eq("id", report_id).single().execute().data
    )
    matches = await svc.get_competitor_matches(report_id)
    aligned = [
        (SimpleNamespace(booksy_id=int(m["booksy_id"]), salon_id=int(m["competitor_salon_id"]),
                         counts_in_aggregates=True), {})
        for m in matches if m.get("booksy_id")
    ]
    subject = await svc.get_subject_full_data(rep["convex_audit_id"])
    _apply_versum_mappings(subject, await svc.get_versum_mappings([subject["salon_id"]]))

    ro = _ReadOnly(svc)
    await _aggregate_verified_match_counts(ro, report_id, subject, aligned)
    if not ro.updates:
        print(f"[{report_id}] re-bucket POMINIĘTY (patrz log ERROR wyżej)")
        return
    name_by_match = {m["id"]: (m.get("competitor_name") or m.get("name") or m.get("competitor_salon_id"))
                     for m in matches}
    print(f"[{report_id}] koszyki: {dict(Counter(u['bucket'] for u in ro.updates))}")
    for u in sorted(ro.updates, key=lambda u: -u["verified_match_count"]):
        print(f"  {u['bucket']:<12} pokryte={u['verified_match_count']:3d} "
              f"przed={u['bucket_pre_verify']!s:<9} {name_by_match.get(u['id'])}")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
