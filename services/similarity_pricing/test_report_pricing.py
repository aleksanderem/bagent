"""Testy integracji: jeden matching, jeden wiersz per usługa, cena z rynku 15km,
wybrani konkurenci wyróżnieni flagą is_selected. Mockują Qdrant + geo RPC + salons.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

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
        self._filter_col = None
        self._filter_ids = None

    def select(self, *a, **k):
        return self

    def in_(self, col, ids):
        self._filter_col = col
        self._filter_ids = set(ids)
        return self

    def execute(self):
        if self._filter_ids is None:
            return SimpleNamespace(data=self._rows)
        return SimpleNamespace(
            data=[r for r in self._rows if r.get(self._filter_col) in self._filter_ids]
        )


class _FakeEmbeddingTable:
    """Tabela `salon_scrape_services` (kolumna `id,name_embedding`).

    Auto-generuje wektor dla KAŻDEGO odpytanego id, chyba że jest w
    `missing_ids` — to symuluje wiersz z NULL `name_embedding` (świeży
    audyt/scrape sprzed nocnego crona, patrz BEAUTY_AUDIT-gqul).
    """
    def __init__(self, missing_ids: set[int] | None = None):
        self._missing = missing_ids or set()
        self._filter_ids: list[int] = []

    def select(self, *a, **k):
        return self

    def in_(self, col, ids):
        self._filter_ids = list(ids)
        return self

    def execute(self):
        rows = [
            {"id": i, "name_embedding": [0.1, 0.2, 0.3]}
            for i in self._filter_ids if i not in self._missing
        ]
        return SimpleNamespace(data=rows)


class _FakeClient:
    def __init__(self, geo_booksy, salon_rows, missing_embedding_ids=None):
        self._geo = geo_booksy
        self._salons = salon_rows
        self._embeddings = _FakeEmbeddingTable(missing_embedding_ids)
        self.rpc_called_with = None

    def rpc(self, name, params):
        self.rpc_called_with = (name, params)
        return _FakeRPC(list(self._geo))

    def table(self, name):
        if name == "salon_scrape_services":
            return self._embeddings
        return _FakeTable(self._salons)


class _FakeService:
    def __init__(self, geo_booksy, salon_rows, missing_embedding_ids=None, chain_head=None):
        self.client = _FakeClient(geo_booksy, salon_rows, missing_embedding_ids)
        # (chain_head_scrape_id, services) — domyślnie pusto (brak chain-head).
        self._chain_head = chain_head if chain_head is not None else (None, [])

    async def get_chain_head_services(self, booksy_id):
        return self._chain_head


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
        {"id": 7000 + b, "booksy_id": b, "name": f"Salon {b}"} for b in [100, 500, 501, 502, 503]
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
    # hx85: salon_id = salons.id (z lookupu), NIE booksy_id
    assert all(s["salon_id"] == 7000 + s["booksy_id"] for s in r["competitor_samples"])


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


# ── LUKA NAPRAWIONA (BEAUTY_AUDIT-23o3, diagnoza mol-ns5s → naprawa mol-n04f)
# counts_in_aggregates było respektowane TYLKO przy budowaniu selected_booksy
# (linia "if getattr(cand, 'counts_in_aggregates', True)" w
# compute_pricing_comparisons_v2). radius_booksy pochodzi z RPC
# fn_competitors_in_radius, który o tej fladze nic nie wie (supabase/
# migrations/146+147_*.sql — filtruje wyłącznie po is_chain_head/odległości,
# zero odwołania do competitor_matches). Union `set(radius_booksy) |
# selected_booksy` przywracał wykluczonego kandydata, jeśli tylko mieścił się
# geograficznie w promieniu. Fix: excluded_booksy odejmowany od unii w
# compute_pricing_comparisons_v2 (BEAUTY_AUDIT-2fv6). SQL wariant
# (fn_competitors_in_radius z opcjonalną listą wykluczeń) zostaje NIE
# ZROBIONY — poprawka Python już usuwa błąd obserwowalny przez klienta.

def test_counts_in_aggregates_false_still_enters_geo_pool(monkeypatch):
    cap: dict[str, Any] = {}
    _patch_search(monkeypatch, [], capture=cap)
    # 501 jawnie wykluczony z agregatów dla TEGO subjectu (np. wykryty
    # duplikat innego już policzonego konkurenta), ale geograficznie mieści
    # się w promieniu 15km — RPC go nie wie że jest wykluczony.
    excluded = _Cand(501)
    excluded.counts_in_aggregates = False
    service = _FakeService(geo_booksy=[500, 501, 502], salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "X", "price_grosze": 10000, "duration_minutes": 30, "booksy_treatment_id": 1,
    }]}
    _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(excluded, {})]))
    assert 501 not in cap["booksy"], (
        "kandydat counts_in_aggregates=False wrócił do puli wyceny przez geo-radius "
        f"(pula faktyczna: {sorted(cap['booksy'])})"
    )


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


def test_adaptive_broadens_when_sparse(monkeypatch):
    # Rzadkie otoczenie: przy precyzyjnym progu 0 twins → 0 verified → fallback
    # luźniejszym progiem daje 4 salony => verified, oznaczone flagą broadened.
    fallback_cluster = [_twin(10 + i, 500 + i, "Manicure", 5000 + i * 100, 30, cat="Paznokcie") for i in range(4)]

    def fake(subject_ids, comp_booksy, *, min_similarity, **kw):
        cl = fallback_cluster if min_similarity <= rp._ADAPTIVE_FALLBACK_SIMILARITY else []
        return {int(s): [dict(c) for c in cl] for s in subject_ids}
    monkeypatch.setattr(rp, "search_twins", fake)
    service = _FakeService(geo_booksy=list(range(500, 510)),
                           salon_rows=[{"booksy_id": b, "name": f"S{b}"} for b in range(500, 504)])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Manicure", "price_grosze": 6000, "duration_minutes": 30,
        "category_name": "Paznokcie", "booksy_treatment_id": 1,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 250, subject_data, [(_Cand(100), {})]))
    assert rows[0]["verification_status"] == "verified"          # broaden uratował rzadki salon
    assert rows[0]["market_median_grosze"] is not None
    vd = rows[0].get("verification_details") or {}
    assert vd.get("matching_broadened") is True
    assert vd.get("min_similarity_used") == rp._ADAPTIVE_FALLBACK_SIMILARITY


def test_no_broaden_when_dense(monkeypatch):
    # Gęste otoczenie: precyzyjny próg już daje verified → ŻADNEGO fallbacku
    # (search_twins wołane dokładnie raz, na _FN_MIN_SIMILARITY) — precyzja gęstych
    # raportów zostaje nietknięta.
    dense = [_twin(10 + i, 500 + i, "Presoterapia", 9000, 40) for i in range(5)]
    sims_used: list[float] = []

    def fake(subject_ids, comp_booksy, *, min_similarity, **kw):
        sims_used.append(min_similarity)
        return {int(s): [dict(c) for c in dense] for s in subject_ids}
    monkeypatch.setattr(rp, "search_twins", fake)
    service = _FakeService(geo_booksy=list(range(500, 510)), salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 222, subject_data, [(_Cand(100), {})]))
    assert rows[0]["verification_status"] == "verified"
    assert sims_used == [rp._FN_MIN_SIMILARITY]                  # tylko jeden przebieg, bez fallbacku
    vd = rows[0].get("verification_details") or {}
    assert "matching_broadened" not in vd


# ── chain-head fallback dla brakujących embeddingów subjecta (BEAUTY_AUDIT-gqul) ──
# Świeży audyt przed catch-upem nocnego crona / wyczerpana kwota OpenAI / błąd
# ingestu -> subject audit-scrape service ma NULL name_embedding. Zamiast cicho
# lądować w samych wierszach subject_only, próbujemy chain-head scrape'a tego
# samego salonu (te wektory inline embedding przy ingest ustawia zawsze).

def test_chain_head_fallback_when_subject_has_no_embeddings(monkeypatch):
    """Subject (audit scrape, id=1) bez wektora -> chain-head (id=999) MA
    wektor -> wycena liczy się na chain-head usłudze zamiast lądować w
    subject_only."""
    cluster = [_twin(10 + i, 500 + i, "Presoterapia", 9000 + i * 200, 40) for i in range(5)]
    _patch_search(monkeypatch, cluster)
    service = _FakeService(
        geo_booksy=list(range(500, 510)), salon_rows=[],
        missing_embedding_ids={1},
        chain_head=("chain-scrape-99", [{
            "id": 999, "name": "Presoterapia", "price_grosze": 14500,
            "duration_minutes": 40, "category_name": "Drenaż",
            "is_active": True, "booksy_treatment_id": 233,
        }]),
    )
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False, "booksy_treatment_id": 233,
    }]}
    rows = _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))
    assert len(rows) == 1
    r = rows[0]
    assert r["verification_status"] == "verified"
    assert r["market_median_grosze"] is not None
    # Wiersz policzony z usługi CHAIN-HEAD (id=999, 14500 gr) — nie audit (15000 gr) —
    # dowód, że subject_services/subject_ids zamieniły się RAZEM z embeddingami.
    assert r["subject_price_grosze"] == 14500


def test_raises_when_chain_head_has_no_services(monkeypatch):
    """Subject bez wektora, chain-head w ogóle nie istnieje (salon bez żadnego
    chain-head scrape'a) -> głośny błąd, NIE cichy raport samych subject_only."""
    _patch_search(monkeypatch, [])
    service = _FakeService(
        geo_booksy=[500], salon_rows=[], missing_embedding_ids={1},
    )  # chain_head domyślnie (None, [])
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "booksy_treatment_id": 233,
    }]}
    with pytest.raises(RuntimeError, match="name_embedding"):
        _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))


def test_raises_when_chain_head_services_also_lack_embeddings(monkeypatch):
    """Subject bez wektora, chain-head MA usługi, ale one też są bez wektora
    (ingest padł na obu scrape'ach) -> nadal głośny błąd, nie subject_only."""
    _patch_search(monkeypatch, [])
    service = _FakeService(
        geo_booksy=[500], salon_rows=[],
        missing_embedding_ids={1, 999},
        chain_head=("chain-scrape-99", [{
            "id": 999, "name": "Presoterapia", "price_grosze": 14500,
            "duration_minutes": 40, "is_active": True,
        }]),
    )
    subject_data = {"booksy_id": 163496, "services": [{
        "id": 1, "name": "Presoterapia", "price_grosze": 15000, "duration_minutes": 40,
        "booksy_treatment_id": 233,
    }]}
    with pytest.raises(RuntimeError, match="name_embedding"):
        _run(compute_pricing_comparisons_v2(service, 181, subject_data, [(_Cand(100), {})]))


# ── Regresja BEAUTY_AUDIT-bgp3: średnie pokrycie też zasługuje na fallback ──

def test_adaptive_broadens_at_medium_coverage(monkeypatch):
    """Salon z 20% pokrycia dostaje szerszy rynek, nie tylko salon z 0%.

    Raport 250 (klinika med-est, Warszawa) wyszedł z verified_rate 45/221 =
    0.204 — o 0.4 punktu ZA WYSOKO na próg 0.20, więc fallback się nie odpalił
    i klientka dostała 72% wierszy "Tylko Ty", choć przy progu 0.75 rynek
    istniał. Ten test odwzorowuje tamten układ: 5 usług, z czego jedna ma
    twins przy progu precyzyjnym (rate = 0.20, czyli DOKŁADNIE stary próg).

    Przy triggerze 0.20 fallback nie ruszał (0.20 < 0.20 jest fałszem).
    Przy 0.50 rusza i podnosi pokrycie.
    """
    precise_only = {1}          # tylko usługa 1 ma odpowiedniki przy 0.82
    cluster = [_twin(10 + i, 500 + i, "Mezoterapia", 30000, 60, cat="Twarz") for i in range(5)]
    sims_used: list[float] = []

    def fake(subject_ids, comp_booksy, *, min_similarity, **kw):
        sims_used.append(min_similarity)
        if min_similarity <= rp._ADAPTIVE_FALLBACK_SIMILARITY:
            return {int(s): [dict(c) for c in cluster] for s in subject_ids}
        return {int(s): ([dict(c) for c in cluster] if int(s) in precise_only else [])
                for s in subject_ids}

    monkeypatch.setattr(rp, "search_twins", fake)
    service = _FakeService(geo_booksy=list(range(500, 510)), salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [
        {"id": i, "name": "Mezoterapia", "price_grosze": 45000,
         "duration_minutes": 60, "booksy_treatment_id": 233}
        for i in range(1, 6)
    ]}
    rows = _run(compute_pricing_comparisons_v2(service, 250, subject_data, [(_Cand(100), {})]))

    assert len(sims_used) == 2, f"fallback nie ruszył przy 20% pokrycia: {sims_used}"
    assert sims_used[1] == rp._ADAPTIVE_FALLBACK_SIMILARITY
    verified = [r for r in rows if r["verification_status"] == "verified"]
    assert len(verified) == 5, f"po fallbacku powinno być 5 wierszy z ceną, jest {len(verified)}"
    assert (rows[0].get("verification_details") or {}).get("matching_broadened") is True


def test_trigger_covers_real_reports_that_missed_the_rescue():
    """Progi z ŻYWYCH raportów, które minęły się z ratunkiem, muszą być pokryte.

    Każdy z tych przypadków wyszedł na produkcji z pustym porównaniem cen mimo
    istniejącego rynku, bo verified_rate wypadł tuż NAD ówczesnym progiem:
      * raport 250 (klinika med-est): 45/221 = 0.204 przy progu 0.20
      * raport 259 (JETSET CLINIC):   54/101 = 0.535 przy progu 0.50
    Podnoszenie progu o krok przesuwało granicę zamiast ją usunąć — stąd 0.80.
    Nowy przypadek tego typu DOPISZ TUTAJ, zamiast tylko bumpować stałą.
    """
    zywe_przypadki = [
        ("raport 250 (klinika med-est)", 45 / 221),
        ("raport 259 (JETSET CLINIC)", 54 / 101),
    ]
    for opis, rate in zywe_przypadki:
        assert rp._ADAPTIVE_TRIGGER_VERIFIED_RATE > rate, (
            f"{opis}: rate {rate:.3f} nie zmieściłby się pod progiem "
            f"{rp._ADAPTIVE_TRIGGER_VERIFIED_RATE} — ratunek znów by nie ruszył"
        )


def test_dense_report_still_skips_the_rescue():
    """Salon z naprawdę wysokim pokryciem nie płaci za drugi przebieg wyceny.

    Próg 0.80 ma zostawiać precyzyjne 0.82 tam, gdzie raport i tak jest pełny.
    Bez tego testu kolejne podniesienie progu przeszłoby niezauważone aż do
    "zawsze dwa przebiegi", co podwaja czas etapu wyceny dla wszystkich.
    """
    assert rp._ADAPTIVE_TRIGGER_VERIFIED_RATE < 1.0
    assert rp._ADAPTIVE_TRIGGER_VERIFIED_RATE < 90 / 100


# ── Regresja BEAUTY_AUDIT-twvf: wiersz = usługa, nie szuflada Booksy ──

def test_row_is_named_after_service_not_booksy_bucket(monkeypatch):
    """Dwie usługi w jednej szufladzie i w tej samej cenie = dwa odrębne wiersze.

    Reguła jest strukturalna, nie zależy od żadnej konkretnej usługi: szuflada
    Booksy (`treatment_name`) grupuje usługi luźniej niż cennik salonu — pomiar
    na 1500 skanach pokazał, że 46% szuflad zawiera więcej niż jedną usługę, a
    rozkład rozrzutu ceny w nich nie ma doliny, więc nie da się wyznaczyć progu
    "ta szuflada jeszcze opisuje jedną usługę".

    Bez tego testu wraca podwójna wada: wiersz nazwany szufladą ORAZ zlanie się
    obu usług w jedną pozycję, bo `_dedup_pricing_rows` trzyma `treatment_name`
    w kluczu deduplikacji.
    """
    cluster = [_twin(10 + i, 500 + i, "Zabieg", 20000, 30) for i in range(5)]
    _patch_search(monkeypatch, cluster)
    service = _FakeService(geo_booksy=list(range(500, 510)), salon_rows=[])
    subject_data = {"booksy_id": 163496, "services": [
        # ta sama szuflada, ta sama cena, różne usługi
        {"id": 1, "name": "Usługa A", "treatment_name": "Wspólna szuflada",
         "price_grosze": 30000, "duration_minutes": 30, "booksy_treatment_id": 245},
        {"id": 2, "name": "Usługa B", "treatment_name": "Wspólna szuflada",
         "price_grosze": 30000, "duration_minutes": 30, "booksy_treatment_id": 245},
    ]}
    rows = _run(compute_pricing_comparisons_v2(service, 250, subject_data, [(_Cand(100), {})]))

    nazwy = sorted(r["treatment_name"] for r in rows)
    assert nazwy == ["Usługa A", "Usługa B"], (
        f"wiersze powinny nazywać się usługami, dostałem {nazwy}"
    )
    # klucz dedupu (competitor_analysis._dedup_pricing_rows) zawiera treatment_name
    # + subject_price_grosze — przy nazwie szuflady oba wiersze miałyby ten sam
    # klucz i jeden by zniknął przed zapisem do bazy.
    klucze = {(r["treatment_name"], r["subject_price_grosze"]) for r in rows}
    assert len(klucze) == 2, f"wiersze zlewają się w kluczu dedupu: {klucze}"
