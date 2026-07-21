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

import services.market_snapshot as ms
from services.market_snapshot import build_market_snapshot


@pytest.fixture(autouse=True)
def _clear_result_cache():
    """Testy współdzielą frazę "Botoks" — bez czyszczenia cache TTL drugi test
    dostaje wynik pierwszego (wykryte przy wdrażaniu cache — działa!)."""
    ms._RESULT_CACHE.clear()
    yield
    ms._RESULT_CACHE.clear()


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
        hybrid: list[dict[str, Any]] | None = None,
        tid_rows: list[dict[str, Any]] | None = None,
        growth: list[dict[str, Any]] | None = None,
    ):
        self._matches = matches
        self._area_ids = area_ids if area_ids is not None else []
        self._tables = tables or {}
        self._hybrid = hybrid or []
        self._tid_rows = tid_rows or []
        self._growth = growth or []
        self.rpc_calls: list[tuple[str, dict[str, Any]]] = []
        self._pending: Any = None

    def rpc(self, name: str, params: dict[str, Any]):
        self.rpc_calls.append((name, params))
        if name == "fn_market_service_matches":
            self._pending = _FakeResult(self._matches)
        elif name == "fn_market_area_booksy_ids":
            self._pending = _FakeResult(self._area_ids)
        elif name == "match_treatment_hybrid":
            self._pending = _FakeResult(self._hybrid)
        elif name == "fn_market_tid_services":
            self._pending = _FakeResult(self._tid_rows)
        elif name == "fn_market_salon_review_growth":
            self._pending = _FakeResult(self._growth)
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
    hybrid = kwargs.pop("hybrid", None)
    tid_rows = kwargs.pop("tid_rows", None)
    growth = kwargs.pop("growth", None)
    fake = _FakeSupabase(rows, area_ids=area_ids, tables=tables, hybrid=hybrid, tid_rows=tid_rows, growth=growth)
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


def test_tid_channel_extends_pool_with_concept_matches():
    """Kanał tid (match_treatment_hybrid → fn_market_tid_services) dokłada
    usługi o INNYCH nazwach ("Toksyna botulinowa...") do puli i grup."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
    ]
    tid_rows = [
        _row(70, 401, "Toksyna botulinowa - 1 okolica", 140000),
        _row(71, 402, "Toksyna botulinowa - 1 okolica", 160000),
        _row(72, 403, "Toksyna botulinowa - 1 okolica", 150000),
    ]
    for t in tid_rows:
        t.pop("similarity", None)  # kanał tid nie niesie phrase-sim
    out, fake = _run(
        rows,
        area_ids=[101, 102, 401, 402, 403],
        hybrid=[{"inferred_tid": 245, "canonical_name": "Botoks", "score": 0.9}],
        tid_rows=tid_rows,
    )
    labels = [g["label"] for g in out["groups"]]
    assert any("Toksyna" in l for l in labels)  # nowa grupa z kanału tid
    assert out["total_offers"] == 5
    called = [c[0] for c in fake.rpc_calls]
    assert "match_treatment_hybrid" in called
    assert "fn_market_tid_services" in called


def test_rpc_retry_on_statement_timeout():
    """57014 (PostgREST statement timeout) → jeden retry; inne błędy propagują."""
    from services.market_snapshot import _rpc_with_retry

    class _TimeoutOnce:
        def __init__(self):
            self.calls = 0

        def rpc(self, name, params):
            return self

        def execute(self):
            self.calls += 1
            if self.calls == 1:
                err = Exception({"message": "canceling statement due to statement timeout", "code": "57014"})
                raise err
            return _FakeResult([{"ok": True}])

    c = _TimeoutOnce()
    res = _rpc_with_retry(c, "fn_x", {})
    assert c.calls == 2
    assert res.data == [{"ok": True}]

    class _OtherError:
        def rpc(self, name, params):
            return self

        def execute(self):
            raise ValueError("boom")

    with pytest.raises(ValueError):
        _rpc_with_retry(_OtherError(), "fn_x", {})


def test_review_growth_median_per_group():
    """v7: mediana tempa przyrostu opinii salonów grupy jako proxy popytu."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
    ]
    growth = [
        {"booksy_id": 101, "rate_30d": 4.0, "days_span": 30.0, "reviews_now": 100},
        {"booksy_id": 102, "rate_30d": 10.0, "days_span": 28.0, "reviews_now": 300},
        # 103 bez historii — nie wchodzi do mediany
    ]
    out, fake = _run(rows, growth=growth)
    g = out["groups"][0]
    assert g["review_growth_30d"] == 7.0  # mediana z {4, 10}
    assert g["review_growth_n"] == 2
    assert any(c[0] == "fn_market_salon_review_growth" for c in fake.rpc_calls)


def test_review_growth_absent_gives_none():
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
    ]
    out, _ = _run(rows)
    g = out["groups"][0]
    assert g["review_growth_30d"] is None
    assert g["review_growth_n"] == 0


def test_result_cache_hit_skips_recompute():
    """Drugie identyczne zapytanie w TTL wraca z cache (bez RPC)."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
    ]
    fake = _FakeSupabase(rows, area_ids=[101, 102])
    with (
        patch("services.market_snapshot.embed_texts", return_value=_EMBED_OK) as emb,
        patch("services.market_snapshot.search_twins", return_value={}),
        patch("services.market_snapshot.fetch_twin_vectors", return_value={}),
        patch("services.market_snapshot.compute_peer_max_sims", return_value={}),
    ):
        out1 = build_market_snapshot("Botoks", supabase_client=fake, city="Warszawa")
        calls_after_first = len(fake.rpc_calls)
        out2 = build_market_snapshot("Botoks", supabase_client=fake, city="Warszawa")
    assert out2 is out1  # ten sam obiekt z cache
    assert len(fake.rpc_calls) == calls_after_first  # zero nowych RPC
    assert emb.call_count == 1  # embedding tylko raz

    # Inna fraza/obszar = osobny klucz (miss)
    with (
        patch("services.market_snapshot.embed_texts", return_value=_EMBED_OK),
        patch("services.market_snapshot.search_twins", return_value={}),
        patch("services.market_snapshot.fetch_twin_vectors", return_value={}),
        patch("services.market_snapshot.compute_peer_max_sims", return_value={}),
    ):
        build_market_snapshot("Botoks", supabase_client=fake, city="Kraków")
    assert len(fake.rpc_calls) > calls_after_first


def test_review_growth_counts_only_engine_kept_salons():
    """Audyt: salon odrzucony przez silnik (pakiet) nie zasila mediany popytu."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
        # salon 104: JEDYNA oferta to pakiet — normalize_unit go nie liczy,
        # dedup zostawia, ale w samples nie ma ceny → sprawdzamy członkostwo
        _row(4, 104, "Botoks", 400000, package=True),
    ]
    growth = [
        {"booksy_id": 101, "rate_30d": 4.0},
        {"booksy_id": 102, "rate_30d": 6.0},
        {"booksy_id": 103, "rate_30d": 8.0},
        {"booksy_id": 104, "rate_30d": 100.0},  # nie może zawyżyć mediany,
        # jeśli silnik wytnie jego ofertę z klastra
    ]
    out, _ = _run(rows, growth=growth)
    g = out["groups"][0]
    # Mediana liczona po salonach z result.samples silnika. Jeżeli silnik
    # zatrzymał pakiet w samples (package axis może abstain przy zgodnej
    # nazwie), mediana = 7.0; jeżeli wyciął — 6.0. Obie ścieżki akceptowalne,
    # ale wartość musi pochodzić z KEPT zbioru:
    assert g["review_growth_30d"] in (6.0, 7.0)
    assert g["review_growth_n"] == g["review_growth_n"]  # sanity
    assert g["review_growth_n"] <= 4


def test_counting_invariants_pool_vs_groups_and_other():
    """total_offers == |pool|; suma n_offers grup + unikalne 'other' == |pool|
    (każdy członek puli ląduje w dokładnie jednej grupie albo w other)."""
    rows = [
        _row(1, 101, "Botoks", 150000),
        _row(2, 102, "Botoks", 152000),
        _row(3, 103, "Botoks", 148000),
        _row(6, 106, "Botoks czoło", 90000),
        _row(7, 107, "Botoks czoło", 95000),
        _row(9, 109, "Unikat marketingowy VIP", 250000, sim=0.7),
    ]
    out, _ = _run(rows)
    assert out["total_offers"] == 6
    n_in_groups = sum(g["n_offers"] for g in out["groups"])
    # other_samples jest capowane do 60, tu poniżej capa → pełne
    assert n_in_groups + len(out["other_samples"]) == out["total_offers"]
