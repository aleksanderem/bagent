"""Faza 8a (re-bucket wg pokrycia menu) — BEAUTY_AUDIT-hx85 + BEAUTY_AUDIT-xi18.

hx85: próbki niosły booksy_id w polu salon_id → 15/15 'excluded'.
xi18: stałe progi 10/5/3 (kalibrowane pod stary pricing) + brak indeksu
payload w Qdrant (filtrowany HNSW gubił całe salony) → nadal 15/15 'excluded'.
Teraz: osobne DOKŁADNE wyszukiwanie w puli wybranych + progi WZGLĘDNE.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pipelines.competitor_analysis as ca
from pipelines.competitor_analysis import _aggregate_verified_match_counts
from pipelines.competitor_buckets import (
    ASPIRATIONAL_SHARE,
    CLUSTER_SHARE,
    DIRECT_SHARE,
    MIN_COVERED,
    assign_coverage_buckets,
    bucket_for_coverage,
    coverage_by_salon,
)

# Dwie przestrzenie ID: salons.id (małe) vs booksy_id (duże) — jak na prodzie.
SALONS = {3822: 239352, 552: 116829, 3278: 21173}
BY_BOOKSY = {b: s for s, b in SALONS.items()}


# ── czyste funkcje ──────────────────────────────────────────────────────────

def test_bucket_for_coverage_relative_thresholds():
    assert bucket_for_coverage(int(100 * DIRECT_SHARE), 100) == "direct"
    assert bucket_for_coverage(int(100 * CLUSTER_SHARE), 100) == "cluster"
    assert bucket_for_coverage(int(100 * ASPIRATIONAL_SHARE), 100) == "aspirational"
    assert bucket_for_coverage(int(100 * ASPIRATIONAL_SHARE) - 1, 100) == "excluded"
    assert bucket_for_coverage(0, 100) == "excluded"
    assert bucket_for_coverage(0, 0) == "excluded"


def test_bucket_is_scale_invariant():
    """Ten sam udział → ten sam koszyk niezależnie od liczby próbek silnika."""
    assert bucket_for_coverage(6, 10) == bucket_for_coverage(600, 1000) == "direct"
    assert bucket_for_coverage(3, 10) == bucket_for_coverage(300, 1000) == "cluster"


def test_no_false_direct_without_shared_services():
    """Konkurent bez wspólnych usług NIGDY nie jest 'direct' — nawet gdy
    uniwersum jest malutkie i 1–2 wspólne usługi dałyby ≥50%."""
    assert bucket_for_coverage(MIN_COVERED - 1, MIN_COVERED - 1) == "excluded"
    assert bucket_for_coverage(1, 2) == "excluded"
    assert bucket_for_coverage(MIN_COVERED, MIN_COVERED) == "direct"


def test_coverage_by_salon_maps_booksy_to_salon_id_and_applies_threshold():
    clusters = {
        1: [{"booksy_id": 239352, "similarity": 0.9}, {"booksy_id": 116829, "similarity": 0.7}],
        2: [{"booksy_id": 239352, "similarity": 0.8}, {"booksy_id": 999999, "similarity": 0.99}],
    }
    cov = coverage_by_salon(clusters, BY_BOOKSY, 0.75)
    assert cov == {3822: {1, 2}, 552: set(), 3278: set()}


def test_assign_coverage_buckets_universe_is_union():
    cov = {3822: set(range(10)), 552: set(range(5)), 3278: {0, 1, 2, 10, 11}}
    out = assign_coverage_buckets(cov)
    # uniwersum = 12 usług; 10/12 direct, 5/12 cluster, 5/12 cluster
    assert out[3822].bucket == "direct" and out[3822].covered == 10
    assert out[552].bucket == "cluster" and out[3278].bucket == "cluster"
    assert out[3822].share == round(10 / 12, 4)


# ── integracja z competitor_matches ─────────────────────────────────────────

def _subject(n_services: int = 20) -> dict:
    return {"services": [{"id": i, "price_grosze": 10000, "is_active": True}
                         for i in range(1, n_services + 1)]}


def _aligned():
    return [(SimpleNamespace(booksy_id=b, salon_id=s), {}) for s, b in SALONS.items()]


def _matches():
    return [{"id": 2840 + i, "competitor_salon_id": sid, "bucket": "direct"}
            for i, sid in enumerate(SALONS)]


def _service(matches):
    svc = MagicMock()
    svc.get_competitor_matches = AsyncMock(return_value=matches)
    svc.update_competitor_matches_verify_buckets = AsyncMock()
    return svc


def _fake_search(per_booksy_services: dict[int, set[int]]):
    """search_twins zwracający bliźniaka z danego salonu dla danych usług."""
    def _search(subject_ids, booksy_ids, **kw):
        assert kw.get("exact") is True, "Faza 8a musi szukać DOKŁADNIE (brak indeksu payload)"
        out = {int(s): [] for s in subject_ids}
        for b, svcs in per_booksy_services.items():
            for s in svcs:
                out[s].append({"booksy_id": b, "similarity": 0.9, "service_id": 1000 * b + s})
        return out
    return _search


def _run(monkeypatch, per_booksy, matches=None, embeddings=None):
    monkeypatch.setattr(ca, "search_twins", _fake_search(per_booksy))
    monkeypatch.setattr(
        ca, "_fetch_subject_embeddings",
        lambda service, ids: {i: [0.1] for i in ids} if embeddings is None else embeddings,
    )
    svc = _service(_matches() if matches is None else matches)
    out = asyncio.run(_aggregate_verified_match_counts(svc, 250, _subject(), _aligned()))
    return svc, out


def test_full_coverage_keeps_direct(monkeypatch):
    svc, out = _run(monkeypatch, {b: set(range(1, 21)) for b in BY_BOOKSY})
    assert out == {3822: 20, 552: 20, 3278: 20}
    _, updates = svc.update_competitor_matches_verify_buckets.await_args.args
    assert {u["bucket"] for u in updates} == {"direct"}
    assert all(u["verified_match_count"] == 20 and u["counts_in_aggregates"] for u in updates)
    assert all(u["bucket_pre_verify"] == "direct" for u in updates)


def test_relative_buckets_per_competitor(monkeypatch):
    # uniwersum = 20 (salon 239352 pokrywa wszystko); 116829: 6/20=30% → cluster;
    # 21173: 0 → excluded, poza agregatami.
    svc, _ = _run(monkeypatch, {239352: set(range(1, 21)), 116829: set(range(1, 7)), 21173: set()})
    _, updates = svc.update_competitor_matches_verify_buckets.await_args.args
    by_id = {u["id"]: u for u in updates}
    assert by_id[2840]["bucket"] == "direct"
    assert by_id[2841]["bucket"] == "cluster"
    assert by_id[2842]["bucket"] == "excluded" and by_id[2842]["counts_in_aggregates"] is False
    assert by_id[2842]["verified_match_count"] == 0


def test_missing_subject_embeddings_skips_rebucket(monkeypatch):
    """Stare audit scrape'y (raporty 34/181) nie mają name_embedding — zamiast
    15/15 'excluded' re-bucket jest pominięty, stan sprzed zostaje."""
    svc, out = _run(monkeypatch, {b: set(range(1, 21)) for b in BY_BOOKSY}, embeddings={})
    assert out == {}
    svc.update_competitor_matches_verify_buckets.assert_not_called()


def test_wrong_id_space_skips_rebucket(monkeypatch):
    """Pokrycie dla salonów spoza competitor_matches raportu (pomylona
    przestrzeń ID, hx85) → bezpiecznik, bez degradacji do 'excluded'."""
    foreign = [{"id": 1, "competitor_salon_id": 777, "bucket": "direct"}]
    svc, out = _run(monkeypatch, {b: set(range(1, 21)) for b in BY_BOOKSY}, matches=foreign)
    assert out == {}
    svc.update_competitor_matches_verify_buckets.assert_not_called()
