"""Testy integracji 2-scope (wybrani + promień N km) z kontraktem
competitor_pricing_comparisons. Mockują Qdrant (search_twins), geo RPC i salons.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import services.similarity_pricing.report_pricing as rp
from .report_pricing import SCOPE_LOCAL, SCOPE_SELECTED, compute_pricing_comparisons_v2


class _Cand:
    def __init__(self, bid: int):
        self.booksy_id = bid
        self.counts_in_aggregates = True


class _FakeRPC:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return SimpleNamespace(data=self._data)


class _FakeTable:
    def __init__(self, rows):
        self._rows = rows

    def select(self, *a, **k):
        return self

    def in_(self, *a, **k):
        return self

    def execute(self):
        return SimpleNamespace(data=self._rows)


class _FakeClient:
    def __init__(self, geo_booksy, salon_rows):
        self._geo = geo_booksy
        self._salons = salon_rows
        self.rpc_called_with = None

    def rpc(self, name, params):
        self.rpc_called_with = (name, params)
        return _FakeRPC([{"fn_competitors_in_radius": b} for b in self._geo])

    def table(self, name):
        return _FakeTable(self._salons)


class _FakeService:
    def __init__(self, geo_booksy, salon_rows):
        self.client = _FakeClient(geo_booksy, salon_rows)


def _twin(sid, bid, name, price, dur, cat="Drenaż", pkg=False, sim=0.86):
    return {
        "service_id": sid, "booksy_id": bid, "salon_id": bid, "salon_name": "",
        "service_name": name, "price_grosze": price, "duration_minutes": dur,
        "category_name": cat, "is_package": pkg, "similarity": sim,
    }


def _run(coro):
    return asyncio.run(coro)


def _patch_search(monkeypatch, by_scope):
    """search_twins zwraca różne klastry zależnie od puli (selected vs geo)."""
    def fake(subject_ids, comp_booksy, **kw):
        key = "local" if len(comp_booksy) > 20 else "selected"
        cluster = by_scope.get(key, [])
        return {int(s): list(cluster) for s in subject_ids}
    monkeypatch.setattr(rp, "search_twins", fake)


def test_two_scopes_produced(monkeypatch):
    # selected: 1 bliźniak (thin→insufficient), local: 5 (sufficient)
    _patch_search(monkeypatch, {
        "selected": [_twin(10, 100, "Presoterapia", 9000, 40)],
        "local": [_twin(10 + i, 100 + i, "Presoterapia", 9000 + i * 200, 40) for i in range(5)],
    })
    service = _FakeService(geo_booksy=list(range(500, 600)), salon_rows=[{"booksy_id": 100, "name": "Salon X"}])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    competitors = [(_Cand(100), {})]
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, competitors))
    scopes = {r["comparison_scope"] for r in rows}
    assert scopes == {SCOPE_SELECTED, SCOPE_LOCAL}
    sel = [r for r in rows if r["comparison_scope"] == SCOPE_SELECTED][0]
    loc = [r for r in rows if r["comparison_scope"] == SCOPE_LOCAL][0]
    # wybrani: 1 salon → insufficient (brak ceny); okolica: 5 → cena
    assert sel["market_median_grosze"] is None and sel["verification_status"] == "subject_only"
    assert loc["market_median_grosze"] is not None and loc["sample_size"] == 5
    assert all(r["comparison_tier"] == "identity" for r in rows)


def test_geo_rpc_called_with_radius(monkeypatch):
    _patch_search(monkeypatch, {"selected": [], "local": []})
    service = _FakeService(geo_booksy=[], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30, "booksy_treatment_id": 1,
    }]}
    _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})], radius_km=15))
    name, params = service.client.rpc_called_with
    assert name == "fn_competitors_in_radius"
    assert params["p_subject_booksy_id"] == 163496
    assert params["p_radius_km"] == 15


def test_salon_name_enriched(monkeypatch):
    _patch_search(monkeypatch, {
        "selected": [_twin(10, 100, "Presoterapia", 9000, 40)],
        "local": [],
    })
    service = _FakeService(geo_booksy=[], salon_rows=[{"booksy_id": 100, "name": "Salon ABC"}])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))
    sel = [r for r in rows if r["comparison_scope"] == SCOPE_SELECTED][0]
    assert sel["competitor_samples"][0]["salon_name"] == "Salon ABC"


def test_empty_subject_returns_empty(monkeypatch):
    _patch_search(monkeypatch, {"selected": [], "local": []})
    service = _FakeService(geo_booksy=[], salon_rows=[])
    assert _run(compute_pricing_comparisons_v2(service, 181, {"booksy_id": 1, "services": []}, [])) == []


def test_packages_excluded(monkeypatch):
    _patch_search(monkeypatch, {
        "selected": [_twin(10 + i, 100 + i, "Presoterapia", 9000, 40) for i in range(5)]
                    + [_twin(90, 900, "Presoterapia pakiet 5", 70000, 40, pkg=True)],
        "local": [],
    })
    service = _FakeService(geo_booksy=[], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100 + i), {}) for i in range(6)]))
    sel = [r for r in rows if r["comparison_scope"] == SCOPE_SELECTED][0]
    assert sel["sample_size"] == 5  # pakiet wycięty
