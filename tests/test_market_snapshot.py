"""Testy build_market_snapshot (mini raport rynkowy, S0082).

Offline: embed_texts i Qdrant zmockowane, Supabase = fake client zwracający
fixture wiersze w kształcie fn_market_service_matches (mig 153). Weryfikujemy
ORKIESTRACJĘ (grupowanie po znormalizowanej nazwie, syntetyczny subject,
routing do other_samples, sortowanie, kształt odpowiedzi) — warstwy silnika
mają własne testy w services/similarity_pricing/.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from services.market_snapshot import build_market_snapshot


class _FakeRpcResult:
    def __init__(self, data: list[dict[str, Any]]):
        self.data = data


class _FakeRpc:
    def __init__(self, data: list[dict[str, Any]]):
        self._data = data
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def rpc(self, name: str, params: dict[str, Any]):
        self.calls.append((name, params))
        return self

    def execute(self):
        return _FakeRpcResult(self._data)


def _row(
    sid: int,
    booksy: int,
    name: str,
    price: int,
    *,
    sim: float = 0.9,
    duration: int = 30,
    package: bool = False,
) -> dict[str, Any]:
    return {
        "service_id": sid,
        "booksy_id": booksy,
        "salon_ref_id": booksy,
        "salon_name": f"Salon {booksy}",
        "city": "Warszawa",
        "latitude": 52.2,
        "longitude": 21.0,
        "distance_m": 1000.0,
        "service_name": name,
        "price_grosze": price,
        "duration_minutes": duration,
        "is_from_price": False,
        "is_package": package,
        "category_name": "Medycyna estetyczna",
        "similarity": sim,
    }


_EMBED_OK = ([[0.1] * 1536], "openai-3-small")


def _run(rows: list[dict[str, Any]], **kwargs):
    kwargs.setdefault("city", "Warszawa")
    fake = _FakeRpc(rows)
    with (
        patch("services.market_snapshot.embed_texts", return_value=_EMBED_OK),
        patch("services.market_snapshot.fetch_twin_vectors", return_value={}),
        patch("services.market_snapshot.compute_peer_max_sims", return_value={}),
    ):
        out = build_market_snapshot("Botoks", supabase_client=fake, **kwargs)
    return out, fake


def test_empty_rows_gives_empty_snapshot():
    out, fake = _run([])
    assert out["groups"] == []
    assert out["other_samples"] == []
    assert out["total_offers"] == 0
    assert fake.calls[0][0] == "fn_market_service_matches"
    assert fake.calls[0][1]["p_city"] == "Warszawa"


def test_groups_by_normalized_name_and_sorts_by_salon_count():
    rows = [
        # "Botoks" — 5 salonów (diakrytyki/case nie rozbijają grupy)
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "BOTOKS", 152000),
        _row(3, 103, "botoks", 148000),
        _row(4, 104, "Botoks", 155000),
        _row(5, 105, "Botoks ", 149000),
        # "Botoks czoło" — 3 salony
        _row(6, 106, "Botoks czoło", 90000),
        _row(7, 107, "Botoks czolo", 95000),
        _row(8, 108, "Botoks czoło", 88000),
    ]
    out, _ = _run(rows)
    # Orkiestracja: dwie grupy (normalizacja scala case/diakrytyki), posortowane
    # malejąco po salonach. Dokładne n_salons drugiej grupy zależy od decyzji
    # silnika (własna suita testów) — assertujemy grupowanie, nie cięcia.
    assert len(out["groups"]) == 2
    assert out["groups"][0]["n_salons"] == 5
    assert out["groups"][0]["label"].lower().startswith("botoks")
    assert out["groups"][1]["n_offers"] == 3
    assert out["groups"][0]["n_salons"] >= out["groups"][1]["n_salons"]
    assert out["total_salons"] == 8


def test_singleton_names_go_to_other_samples():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
        _row(9, 109, "Botoks pełna twarz PREMIUM VIP", 250000, sim=0.7),
    ]
    out, _ = _run(rows)
    assert len(out["groups"]) == 1
    assert len(out["other_samples"]) == 1
    assert out["other_samples"][0]["service_name"].endswith("VIP")


def test_group_stats_come_from_engine_and_exclude_packages_from_price():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
        # pakiet ×3 w tej samej nazwie — nie może zawyżyć mediany
        _row(4, 104, "Botoks", 400000, package=True),
    ]
    out, _ = _run(rows)
    g = out["groups"][0]
    assert g["status"] in ("sufficient", "thin")
    assert g["median_grosze"] is not None
    assert g["median_grosze"] <= 160000  # pakiet 4000 zł nie wszedł do ceny
    assert g["n_salons"] >= 3


def test_same_salon_many_variants_counts_once():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 101, "Botoks", 90000),
        _row(3, 101, "Botoks", 210000),
        _row(4, 102, "Botoks", 152000),
        _row(5, 103, "Botoks", 149000),
    ]
    out, _ = _run(rows)
    assert out["groups"][0]["n_salons"] == 3  # dedup per salon (booksy_id)
    assert out["groups"][0]["n_offers"] == 5


def test_embedding_space_mismatch_raises():
    fake = _FakeRpc([])
    with (
        patch(
            "services.market_snapshot.embed_texts",
            return_value=([[0.1] * 1024], "mmlw-e5-large"),
        ),
        pytest.raises(RuntimeError, match="space mismatch"),
    ):
        build_market_snapshot("Botoks", supabase_client=fake, city="Warszawa")


def test_point_area_params_forwarded():
    _, fake = _run([], lat=52.19, lng=21.02, radius_km=10, city=None)
    params = fake.calls[0][1]
    assert params["p_lat"] == 52.19
    assert params["p_radius_km"] == 10
    assert params["p_city"] is None
