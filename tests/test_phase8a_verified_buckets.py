"""Faza 8a (re-bucket po verified matches) — regresja BEAUTY_AUDIT-hx85.

Przed fixem próbki z silnika similarity niosły booksy_id w polu salon_id, więc
złączenie z competitor_matches.competitor_salon_id (salons.id) nigdy nie
trafiało: 15/15 konkurentów lądowało w 'excluded' z verified_match_count=0.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pipelines.competitor_analysis as ca
from pipelines.competitor_analysis import (
    _aggregate_verified_match_counts,
    _bucket_for_verified_count,
    _count_verified_tids,
)

# Dwie przestrzenie ID: salons.id (małe) vs booksy_id (duże) — jak na prodzie.
SALONS = {3822: 239352, 552: 116829, 3278: 21173}


def _rows(salon_id_field: str, n_tids: int = 10) -> list[dict]:
    """n_tids wierszy cen; w każdym po jednej próbce z każdego z 3 salonów."""
    rows = []
    for tid in range(1, n_tids + 1):
        rows.append({
            "booksy_treatment_id": tid,
            "competitor_samples": [
                {"salon_id": (sid if salon_id_field == "salon_id" else bid),
                 "booksy_id": bid, "service_id": tid * 100 + sid}
                for sid, bid in SALONS.items()
            ],
        })
    return rows


def _service(matches):
    svc = MagicMock()
    svc.get_competitor_matches = AsyncMock(return_value=matches)
    svc.update_competitor_matches_verify_buckets = AsyncMock()
    return svc


def _matches():
    return [{"id": 2840 + i, "competitor_salon_id": sid, "bucket": "direct"}
            for i, sid in enumerate(SALONS)]


def test_count_verified_tids_counts_distinct_categories_per_salon():
    counts = _count_verified_tids(_rows("salon_id", n_tids=4))
    assert counts == {3822: 4, 552: 4, 3278: 4}


def test_count_verified_tids_skips_llm_rejected():
    rows = _rows("salon_id", n_tids=2)
    rows[0]["competitor_samples"][0]["llm_verified"] = False
    counts = _count_verified_tids(rows)
    assert counts[3822] == 1 and counts[552] == 2


def test_bucket_thresholds():
    assert _bucket_for_verified_count(ca._VERIFIED_BUCKET_DIRECT_MIN) == "direct"
    assert _bucket_for_verified_count(ca._VERIFIED_BUCKET_CLUSTER_MIN) == "cluster"
    assert _bucket_for_verified_count(ca._VERIFIED_BUCKET_ASPIRATIONAL_MIN) == "aspirational"
    assert _bucket_for_verified_count(0) == "excluded"


def test_before_fix_booksy_id_in_salon_id_field_is_not_degraded_anymore():
    """Stan sprzed fixu: salon_id == booksy_id. Dawniej → 15/15 excluded.
    Teraz bezpiecznik widzi pusty przekrój z konkurentami i POMIJA re-bucket."""
    svc = _service(_matches())
    out = asyncio.run(_aggregate_verified_match_counts(svc, 250, _rows("booksy_id")))
    assert out == {}
    svc.update_competitor_matches_verify_buckets.assert_not_called()


def test_after_fix_internal_salon_id_keeps_direct():
    svc = _service(_matches())
    out = asyncio.run(_aggregate_verified_match_counts(svc, 250, _rows("salon_id", n_tids=10)))
    assert out == {3822: 10, 552: 10, 3278: 10}
    svc.update_competitor_matches_verify_buckets.assert_awaited_once()
    _, updates = svc.update_competitor_matches_verify_buckets.await_args.args
    assert {u["bucket"] for u in updates} == {"direct"}
    assert all(u["verified_match_count"] == 10 and u["counts_in_aggregates"] for u in updates)
    assert all(u["bucket_pre_verify"] == "direct" for u in updates)


def test_partial_overlap_still_rebuckets_by_count():
    """Jeden konkurent bez żadnej próbki → excluded; reszta wg liczników."""
    rows = _rows("salon_id", n_tids=6)
    for r in rows:
        r["competitor_samples"] = [s for s in r["competitor_samples"] if s["salon_id"] != 3278]
    svc = _service(_matches())
    asyncio.run(_aggregate_verified_match_counts(svc, 250, rows))
    _, updates = svc.update_competitor_matches_verify_buckets.await_args.args
    by_id = {u["id"]: u["bucket"] for u in updates}
    assert by_id[2840] == "cluster" and by_id[2841] == "cluster" and by_id[2842] == "excluded"
