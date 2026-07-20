"""Testy build_market_snapshot v3 (mini raport rynkowy, S0082).

Offline: embed_texts, Qdrant (search_twins/fetch_twin_vectors/
compute_peer_max_sims) i Supabase zmockowane. Weryfikujemy ORKIESTRACJĘ:
anchory → grupowanie po znormalizowanej nazwie → ekspansja twinów z
przypisaniem (nazwa > twin-score) → enrichment → engine → kształt odpowiedzi.
Warstwy silnika mają własne testy w services/similarity_pricing/.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from services.market_snapshot import build_market_snapshot


class _FakeResult:
    def __init__(self, data: Any):
        self.data = data


class _FakeTable:
    """Minimalny chain .select().in_().execute() dla enrichmentu."""

    def __init__(self, name: str, tables: dict[str, list[dict[str, Any]]]):
        self._rows = tables.get(name, [])
        self._filter: tuple[str, list[Any]] | None = None

    def select(self, _cols: str):
        return self

    def in_(self, col: str, values: list[Any]):
        self._filter = (col, list(values))
        return self

    def execute(self):
        if not self._filter:
            return _FakeResult(self._rows)
        col, values = self._filter
        return _FakeResult([r for r in self._rows if r.get(col) in values])


class _FakeSupabase:
    def __init__(
        self,
        matches: list[dict[str, Any]],
        area_ids: list[int] | None = None,
        tables: dict[str, list[dict[str, Any]]] | None = None,
    ):
        self._matches = matches
        self._area_ids = area_ids if area_ids is not None else []
        self._tables = tables or {}
        self.rpc_calls: list[tuple[str, dict[str, Any]]] = []
        self._pending: Any = None

    def rpc(self, name: str, params: dict[str, Any]):
        self.rpc_calls.append((name, params))
        if name == "fn_market_service_matches":
            self._pending = _FakeResult(self._matches)
        elif name == "fn_market_area_booksy_ids":
            self._pending = _FakeResult(self._area_ids)
        else:
            self._pending = _FakeResult([])
        return self

    def execute(self):
        return self._pending

    def table(self, name: str):
        return _FakeTable(name, self._tables)


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


def _twin(sid: int, booksy: int, name: str, price: int, *, score: float = 0.9):
    """Kształt sample'a z search_twins (payload Qdranta — bez geo/salon_name)."""
    return {
        "service_id": sid,
        "booksy_id": booksy,
        "salon_id": booksy,
        "salon_name": "",
        "service_name": name,
        "price_grosze": price,
        "duration_minutes": 30,
        "category_name": "Medycyna estetyczna",
        "is_package": False,
        "similarity": score,
    }


_EMBED_OK = ([[0.1] * 1536], "openai-3-small")


def _run(
    rows: list[dict[str, Any]],
    *,
    area_ids: list[int] | None = None,
    twins: dict[int, list[dict[str, Any]]] | None = None,
    tables: dict[str, list[dict[str, Any]]] | None = None,
    **kwargs,
):
    kwargs.setdefault("city", "Warszawa")
    fake = _FakeSupabase(rows, area_ids=area_ids, tables=tables)
    with (
        patch("services.market_snapshot.embed_texts", return_value=_EMBED_OK),
        patch("services.market_snapshot.search_twins", return_value=twins or {}),
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
    assert fake.rpc_calls[0][0] == "fn_market_service_matches"
    assert fake.rpc_calls[0][1]["p_city"] == "Warszawa"


def test_groups_by_normalized_name_and_sorts_by_salon_count():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "BOTOKS", 152000),
        _row(3, 103, "botoks", 148000),
        _row(4, 104, "Botoks", 155000),
        _row(5, 105, "Botoks ", 149000),
        _row(6, 106, "Botoks czoło", 90000),
        _row(7, 107, "Botoks czolo", 95000),  # transliteracja scala ł/l
        _row(8, 108, "Botoks czoło", 88000),
    ]
    out, _ = _run(rows)
    assert len(out["groups"]) == 2
    assert out["groups"][0]["n_salons"] == 5
    assert out["groups"][0]["label"].lower().startswith("botoks")
    assert out["groups"][1]["n_offers"] == 3
    assert out["groups"][0]["n_salons"] >= out["groups"][1]["n_salons"]
    assert out["total_salons"] == 8


def test_twin_expansion_extends_group_beyond_anchors():
    """Bliźniak "Toksyna botulinowa" (inna nazwa, twin repa) dołącza do grupy."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
    ]
    twins = {
        1: [
            _twin(50, 201, "Toksyna botulinowa - 1 okolica", 145000, score=0.87),
            _twin(51, 202, "Toksyna botulinowa - 1 okolica", 155000, score=0.85),
        ]
    }
    tables = {
        "salons": [
            {"id": 9201, "booksy_id": 201, "name": "Med Estetic", "city": "Warszawa", "latitude": 52.21, "longitude": 21.01},
            {"id": 9202, "booksy_id": 202, "name": "Derm Clinic", "city": "Warszawa", "latitude": 52.22, "longitude": 21.02},
        ],
        "salon_scrape_services": [
            {"id": 50, "is_from_price": True},
            {"id": 51, "is_from_price": False},
        ],
    }
    out, fake = _run(rows, area_ids=[101, 102, 103, 201, 202], twins=twins, tables=tables)
    g = out["groups"][0]
    assert g["n_salons"] == 5  # 3 anchory + 2 twiny
    assert out["total_salons"] == 5
    # Enrichment: twin dostał nazwę salonu i is_from_price z Postgresa
    twin_samples = [s for s in g["samples"] if s["service_id"] in (50, 51)]
    assert any(s["salon_name"] == "Med Estetic" for s in twin_samples)
    assert any(s["is_from_price"] is True for s in twin_samples)
    assert fake.rpc_calls[1][0] == "fn_market_area_booksy_ids"


def test_twin_with_existing_group_name_goes_to_that_group():
    """Twin repa grupy A, ale o nazwie grupy B → ląduje w B (tożsamość nazwy)."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(6, 106, "Botoks czoło", 90000),
        _row(7, 107, "Botoks czoło", 95000),
    ]
    twins = {
        1: [_twin(60, 301, "Botoks czoło", 92000, score=0.9)],
    }
    out, _ = _run(rows, area_ids=[101, 102, 106, 107, 301], twins=twins)
    czolo = next(g for g in out["groups"] if "czoło" in g["label"] or "czolo" in g["label"].lower())
    assert czolo["n_salons"] == 3  # 2 anchory + twin przypisany po NAZWIE


def test_singleton_group_after_expansion_goes_to_other():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(9, 109, "Botoks pełna twarz PREMIUM VIP", 250000, sim=0.7),
    ]
    out, _ = _run(rows, area_ids=[101, 102, 109])
    assert len(out["groups"]) == 1
    assert len(out["other_samples"]) == 1
    assert out["other_samples"][0]["service_name"].endswith("VIP")


def test_group_stats_come_from_engine_and_exclude_packages_from_price():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
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
    assert out["groups"][0]["n_salons"] == 3
    assert out["groups"][0]["n_offers"] == 5


def test_embedding_space_mismatch_raises():
    fake = _FakeSupabase([])
    with (
        patch(
            "services.market_snapshot.embed_texts",
            return_value=([[0.1] * 1024], "mmlw-e5-large"),
        ),
        pytest.raises(RuntimeError, match="space mismatch"),
    ):
        build_market_snapshot("Botoks", supabase_client=fake, city="Warszawa")


def test_point_area_params_forwarded():
    _, fake = _run([_row(1, 101, "Botoks", 150000)], lat=52.19, lng=21.02, radius_km=10, city=None)
    params = fake.rpc_calls[0][1]
    assert params["p_lat"] == 52.19
    assert params["p_radius_km"] == 10
    assert params["p_city"] is None
    assert fake.rpc_calls[1][0] == "fn_market_area_booksy_ids"
    assert fake.rpc_calls[1][1]["p_lat"] == 52.19


def test_qdrant_down_falls_back_to_anchors_only():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
    ]
    fake = _FakeSupabase(rows, area_ids=[101, 102, 103])
    with (
        patch("services.market_snapshot.embed_texts", return_value=_EMBED_OK),
        patch("services.market_snapshot.search_twins", side_effect=RuntimeError("down")),
        patch("services.market_snapshot.fetch_twin_vectors", side_effect=RuntimeError("down")),
        patch("services.market_snapshot.compute_peer_max_sims", return_value={}),
    ):
        out = build_market_snapshot("Botoks", supabase_client=fake, city="Warszawa")
    assert out["groups"][0]["n_salons"] == 3  # anchory przeżyły awarię Qdranta
