"""Testy integracji: jeden matching, jeden wiersz per usługa, cena z rynku 15km,
wybrani konkurenci wyróżnieni flagą is_selected. Mockują Qdrant + geo RPC + salons.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import services.similarity_pricing.report_pricing as rp
from .report_pricing import compute_pricing_comparisons_v2


class _Cand:
    def __init__(self, bid: int):
        self.booksy_id = bid
        self.counts_in_aggregates = True


class _FakeRPC:
    def __init__(self, data):
        self._data = data

    def limit(self, n):
        return self

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
        return _FakeRPC(list(self._geo))

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


def _patch_search(monkeypatch, cluster, capture=None):
    def fake(subject_ids, comp_booksy, **kw):
        if capture is not None:
            capture["booksy"] = list(comp_booksy)
        return {int(s): [dict(c) for c in cluster] for s in subject_ids}
    monkeypatch.setattr(rp, "search_twins", fake)


def test_one_row_per_service_market_from_full_cluster(monkeypatch):
    # klaster: 4 z okolicy + 1 wybrany (booksy 100) => 5 salonów, cena z całości
    cluster = [_twin(10 + i, 500 + i, "Presoterapia", 9000 + i * 200, 40) for i in range(4)]
    cluster.append(_twin(20, 100, "Presoterapia", 10000, 40))  # wybrany
    _patch_search(monkeypatch, cluster)
    service = _FakeService(geo_booksy=list(range(500, 510)), salon_rows=[
        {"booksy_id": b, "name": f"Salon {b}"} for b in [100, 500, 501, 502, 503]
    ])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))
    assert len(rows) == 1                       # JEDEN wiersz per usługa (nie 2 scope)
    r = rows[0]
    assert r["comparison_tier"] == "identity"
    assert r["market_median_grosze"] is not None
    assert r["sample_size"] == 5               # cena z całego klastra (rynek okolicy)
    # competitor_samples: wybrany konkurent ma is_selected=True, reszta False
    selected = [s for s in r["competitor_samples"] if s["is_selected"]]
    assert len(selected) == 1 and selected[0]["booksy_id"] == 100
    assert all(s["salon_name"] for s in r["competitor_samples"])


def test_matching_runs_once_on_union(monkeypatch):
    cap: dict[str, Any] = {}
    _patch_search(monkeypatch, [], capture=cap)
    service = _FakeService(geo_booksy=[500, 501, 502], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30, "booksy_treatment_id": 1,
    }]}
    _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {}), (_Cand(501), {})]))
    # jedna pula = suma promienia i wybranych (501 jest w obu — bez duplikatu)
    assert set(cap["booksy"]) == {500, 501, 502, 100}


def test_geo_rpc_called_with_radius(monkeypatch):
    _patch_search(monkeypatch, [])
    service = _FakeService(geo_booksy=[], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30, "booksy_treatment_id": 1,
    }]}
    _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})], radius_km=20))
    name, params = service.client.rpc_called_with
    assert name == "fn_competitors_in_radius"
    assert params["p_subject_booksy_id"] == 163496
    assert params["p_radius_km"] == 20


def test_insufficient_when_too_few(monkeypatch):
    _patch_search(monkeypatch, [_twin(10, 100, "Refleksologia", 18000, 45, cat="Refleksologia")])
    service = _FakeService(geo_booksy=[100], salon_rows=[{"booksy_id": 100, "name": "S"}])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Refleksologia", "price_grosze": 20000, "duration_minutes": 45,
        "category_name": "Refleksologia", "booksy_treatment_id": 50,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))
    assert rows[0]["market_median_grosze"] is None
    assert rows[0]["verification_status"] == "subject_only"


def test_empty_returns_empty(monkeypatch):
    _patch_search(monkeypatch, [])
    service = _FakeService(geo_booksy=[], salon_rows=[])
    assert _run(compute_pricing_comparisons_v2(service, 181, {"booksy_id": 1, "services": []}, [])) == []
    # brak puli (geo pusty + brak wybranych) => brak wierszy
    subject_data = {"booksy_id": 1, "services": [{"id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30}]}
    assert _run(compute_pricing_comparisons_v2(service, 181, subject_data, [])) == []


def test_packages_excluded_from_price(monkeypatch):
    cluster = [_twin(10 + i, 500 + i, "Presoterapia", 9000, 40) for i in range(5)]
    cluster.append(_twin(90, 900, "Presoterapia pakiet 5", 70000, 40, pkg=True))
    _patch_search(monkeypatch, cluster)
    service = _FakeService(geo_booksy=list(range(500, 510)) + [900], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))
    assert rows[0]["sample_size"] == 5  # pakiet wycięty z ceny
