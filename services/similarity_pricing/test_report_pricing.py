"""Testy integracji silnika z kontraktem competitor_pricing_comparisons."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from .report_pricing import compute_pricing_comparisons_v2


class _FakeRPC:
    def __init__(self, data: list[dict]):
        self._data = data

    def execute(self) -> Any:
        return SimpleNamespace(data=self._data)


class _FakeClient:
    def __init__(self, data: list[dict]):
        self._data = data
        self.last_params: dict | None = None

    def rpc(self, name: str, params: dict) -> _FakeRPC:
        self.last_params = params
        return _FakeRPC(self._data)


class _FakeService:
    def __init__(self, data: list[dict]):
        self.client = _FakeClient(data)


class _Cand:
    def __init__(self, bid: int):
        self.booksy_id = bid
        self.counts_in_aggregates = True


def _fn_row(subject_id, sid, bid, name, price, dur, cat="Drenaż", pkg=False, sim=0.86,
            subj_name="Presoterapia", subj_cat="Drenaż", subj_price=15000, subj_dur=40):
    return {
        "subject_service_id": subject_id, "subject_name": subj_name,
        "subject_category_name": subj_cat, "subject_price_grosze": subj_price,
        "subject_duration_minutes": subj_dur,
        "service_id": sid, "booksy_id": bid, "salon_id": bid, "salon_name": f"S{bid}",
        "service_name": name, "price_grosze": price, "duration_minutes": dur,
        "category_name": cat, "is_package": pkg, "similarity": sim,
    }


def _run(coro):
    return asyncio.run(coro)


def test_sufficient_row_contract():
    # 5 nie-pakietowych bliźniaków z 5 salonów => sufficient, pełny wiersz
    fn_data = [
        _fn_row(1, 10 + i, 100 + i, "Presoterapia", 9000 + i * 500, 40)
        for i in range(5)
    ]
    subject_data = {"services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    competitors = [(_Cand(100 + i), {}) for i in range(5)]
    rows = _run(compute_pricing_comparisons_v2(_FakeService(fn_data), 181, subject_data, competitors))
    assert len(rows) == 1
    r = rows[0]
    assert r["report_id"] == 181
    assert r["comparison_tier"] == "identity"
    assert r["booksy_treatment_id"] == 233
    assert r["treatment_name"] == "Presoterapia"
    assert r["subject_price_grosze"] == 15000
    assert r["market_median_grosze"] is not None
    assert r["sample_size"] == 5
    assert r["recommended_action"] in ("raise", "lower", "hold")
    assert r["verification_status"] == "verified"
    assert isinstance(r["competitor_samples"], list) and len(r["competitor_samples"]) == 5
    assert r["deviation_pct"] is not None


def test_insufficient_row_is_subject_only():
    # 1 salon => insufficient => subject_only, brak ceny rynkowej
    fn_data = [_fn_row(1, 10, 100, "Refleksologia", 18000, 45, cat="Refleksologia",
                       subj_name="Refleksologia", subj_cat="Refleksologia")]
    subject_data = {"services": [{
        "id": 1, "name": "Refleksologia", "price_grosze": 20000, "duration_minutes": 45,
        "category_name": "Refleksologia", "is_package": False, "booksy_treatment_id": 50,
    }]}
    rows = _run(compute_pricing_comparisons_v2(_FakeService(fn_data), 181, subject_data, [(_Cand(100), {})]))
    r = rows[0]
    assert r["market_median_grosze"] is None
    assert r["recommended_action"] == "subject_only"
    assert r["verification_status"] == "subject_only"
    assert r["sample_size"] == 1


def test_packages_excluded_from_price():
    # 5 nie-pakietów + 2 pakiety => pakiety wycięte (oś package), cena z 5
    fn_data = [_fn_row(1, 10 + i, 100 + i, "Presoterapia", 9000, 40) for i in range(5)]
    fn_data += [
        _fn_row(1, 90, 900, "Presoterapia pakiet 5", 70000, 40, pkg=True),
        _fn_row(1, 91, 901, "Presoterapia pakiet 10", 130000, 40, pkg=True),
    ]
    subject_data = {"services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    competitors = [(_Cand(100 + i), {}) for i in range(5)] + [(_Cand(900), {}), (_Cand(901), {})]
    rows = _run(compute_pricing_comparisons_v2(_FakeService(fn_data), 181, subject_data, competitors))
    r = rows[0]
    # cena z 5 nie-pakietowych salonów (pakiety nie podbijają)
    assert r["sample_size"] == 5
    assert all(s["price_grosze"] < 70000 for s in r["competitor_samples"])


def test_empty_inputs_return_empty():
    assert _run(compute_pricing_comparisons_v2(_FakeService([]), 181, {"services": []}, [])) == []
    # brak konkurentów => []
    subject_data = {"services": [{"id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30}]}
    assert _run(compute_pricing_comparisons_v2(_FakeService([]), 181, subject_data, [])) == []


def test_rpc_failure_returns_empty():
    class _BoomClient:
        def rpc(self, *a, **k):
            raise RuntimeError("boom")
    svc = SimpleNamespace(client=_BoomClient())
    subject_data = {"services": [{"id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30}]}
    assert _run(compute_pricing_comparisons_v2(svc, 181, subject_data, [(_Cand(100), {})])) == []


def test_rpc_called_with_correct_params():
    fn_data = [_fn_row(1, 10, 100, "Presoterapia", 9000, 40)]
    subject_data = {"services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    svc = _FakeService(fn_data)
    _run(compute_pricing_comparisons_v2(svc, 181, subject_data, [(_Cand(100), {}), (_Cand(101), {})]))
    assert svc.client.last_params["p_subject_service_ids"] == [1]
    assert svc.client.last_params["p_competitor_booksy_ids"] == [100, 101]
