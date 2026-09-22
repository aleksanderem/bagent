"""Destylacja profili na żądanie przy wycenie (services/typesafe_profile/destylacja.py).

Kontrakt: każda nazwa z wyceny dostaje profil raz — z bazy albo z modelu —
i zapisuje się trwale; bezpieczniki (budżet, czas, zły klucz) urywają pracę
bez wywracania raportu; flaga wyłączona = ścieżka GLM bez zmian.
Atrapa klienta SDK — zero sieci, zero klucza.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import httpx2
import pytest
from typesafe_sdk import TypeSafeAuthenticationError

import services.similarity_pricing.report_pricing as rp
from services.typesafe_profile import destylacja
from services.typesafe_profile.pytania import WERSJA

from .test_report_pricing import _Cand, _FakeClient, _FakeTable, _patch_search, _twin

MESKIE = {"odb:mezczyzni": 0.95, "odb:kobiety": 0.04}
DAMSKIE = {"odb:mezczyzni": 0.03, "odb:kobiety": 0.94}


@pytest.fixture(autouse=True)
def _koszty(monkeypatch):
    """Zdarzenia kosztu łapane lokalnie — nic nie leci do PostHoga."""
    zdarzenia: list[dict[str, Any]] = []

    async def _zapisz(**kw: Any) -> None:
        zdarzenia.append(kw)

    monkeypatch.setattr(destylacja, "capture_ai_generation", _zapisz)
    # pomost GLM nie może zawołać prawdziwego modelu, gdy lokalny .env ma klucz Z.ai
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: None)
    return zdarzenia


def _answer(profil: dict[str, float], tokens: int = 3000) -> SimpleNamespace:
    return SimpleNamespace(
        nouls={k: SimpleNamespace(noul=v) for k, v in profil.items()},
        scores={}, choices={}, usage=SimpleNamespace(input_tokens=tokens),
    )


class _FakeSDK:
    """system_one zwraca profil wg słowa w nazwie; liczy wywołania i zamknięcia."""

    def __init__(self, fail: BaseException | None = None, delay: float = 0.0, tokens: int = 3000):
        self.names: list[str] = []
        self.closed = False
        self._fail, self._delay, self._tokens = fail, delay, tokens

    async def system_one(self, state: dict[str, Any], questions: dict[str, Any]) -> SimpleNamespace:
        name = state["usluga"]["nazwa"]
        self.names.append(name)
        assert questions, "pytania muszą iść w wywołaniu"
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._fail is not None:
            raise self._fail
        return _answer(DAMSKIE if "damsk" in name.lower() else MESKIE, self._tokens)

    async def aclose(self) -> None:
        self.closed = True


class _ProfileTable:
    """Tabela service_profile: select/eq/in_/upsert w stylu supabase-py."""

    def __init__(self, stored: list[dict[str, Any]] | None = None):
        self.rows = list(stored or [])
        self.upserts: list[tuple[list[dict[str, Any]], str]] = []
        self._eq: dict[str, Any] = {}
        self._in: set[str] = set()

    def select(self, *a: Any) -> _ProfileTable:
        self._eq, self._in = {}, set()
        return self

    def eq(self, col: str, val: Any) -> _ProfileTable:
        self._eq[col] = val
        return self

    def in_(self, col: str, vals: list[str]) -> _ProfileTable:
        self._in = set(vals)
        return self

    def upsert(self, rows: list[dict[str, Any]], on_conflict: str | None = None) -> _ProfileTable:
        self.upserts.append((list(rows), on_conflict or ""))
        self.rows.extend(rows)
        self._in = set()
        return self

    def execute(self) -> SimpleNamespace:
        data = [
            r for r in self.rows
            if r["name_key"] in self._in and all(r.get(k) == v for k, v in self._eq.items())
        ]
        return SimpleNamespace(data=data)


class _Client(_FakeClient):
    def __init__(self, geo, salons, stored=None, branza="Fryzjer"):
        super().__init__(geo, salons)
        self.profiles = _ProfileTable(stored)
        self._branza = branza

    def table(self, name: str):
        if name == "service_profile":
            return self.profiles
        if name == "treatment_branch_map":
            return _FakeTable([{"treatment_id": 233, "branza": self._branza}])
        return super().table(name)


class _Service:
    def __init__(self, client: _Client):
        self.client = client

    async def get_chain_head_services(self, booksy_id):
        return (None, [])


def _session(stored=None) -> destylacja.ProfileSession:
    return destylacja.ProfileSession(_Service(_Client([], [], stored)))


def _run(coro):
    return asyncio.run(coro)


SUBJECT = [{"name": "Strzyżenie męskie", "category_name": "Panowie"}]
CLUSTERS = {1: [
    {"service_name": "Strzyżenie  DAMSKIE", "category_name": "Panie"},
    {"service_name": "Strzyżenie męskie"},  # ta sama nazwa co podmiot — jeden profil
]}


def test_destyluje_brakujace_i_zapisuje_trwale(monkeypatch, _koszty):
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    s = _session()
    prof = _run(s.profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    assert prof == {"strzyżenie męskie": MESKIE, "strzyżenie damskie": DAMSKIE}
    assert sorted(sdk.names) == ["Strzyżenie  DAMSKIE", "Strzyżenie męskie"]
    assert sdk.closed
    (rows, klucz), = s.service.client.profiles.upserts
    assert klucz == "name_key,branza,wersja"
    assert {r["branza"] for r in rows} == {"Fryzjer"} and {r["wersja"] for r in rows} == {WERSJA}
    assert {r["model"] for r in rows} == {destylacja.MODEL}
    assert s.tokens == 6000
    (koszt,) = _koszty  # jedno zdarzenie kosztu na przebieg, nie na nazwę
    assert koszt["provider"] == "typesafe" and koszt["input_tokens"] == 6000


def test_profile_z_bazy_nie_kosztuja(monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    stored = [
        {"name_key": "strzyżenie męskie", "branza": "Fryzjer", "wersja": WERSJA, "profil": MESKIE},
        {"name_key": "strzyżenie damskie", "branza": "Fryzjer", "wersja": WERSJA, "profil": DAMSKIE},
    ]
    prof = _run(_session(stored).profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    assert len(prof) == 2 and sdk.names == []


def test_profil_innej_wersji_lub_branzy_jest_ponawiany(monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    stored = [
        {"name_key": "strzyżenie męskie", "branza": "Fryzjer", "wersja": WERSJA - 1, "profil": DAMSKIE},
        {"name_key": "strzyżenie damskie", "branza": "Barber shop", "wersja": WERSJA, "profil": MESKIE},
    ]
    prof = _run(_session(stored).profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    assert prof["strzyżenie męskie"] == MESKIE and len(sdk.names) == 2


def test_drugi_przebieg_progu_placi_tylko_za_nowe_nazwy(monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    s = _session()
    _run(s.profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    wiecej = {1: [*CLUSTERS[1], {"service_name": "Strzyżenie męskie z brodą"}]}
    _run(s.profiles_for("Fryzjer", SUBJECT, wiecej))
    assert len(sdk.names) == 3


def test_budzet_urywa_destylacje_a_podmiot_idzie_pierwszy(monkeypatch):
    # nazwa kosztuje cały budżet raportu — po pierwszej reszta jest pomijana
    drogo = int(destylacja.BUDZET_USD_NA_RAPORT / destylacja.USD_ZA_TOKEN) + 1
    sdk = _FakeSDK(tokens=drogo)
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    monkeypatch.setattr(destylacja, "ROWNOLEGLE", 1)
    s = _session()
    prof = _run(s.profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    assert sdk.names == ["Strzyżenie męskie"]
    assert list(prof) == ["strzyżenie męskie"] and s.skipped == 1


def test_zly_klucz_zatrzymuje_kolejne_wywolania(monkeypatch):
    blad = TypeSafeAuthenticationError(401, {"error": "invalid key"}, httpx2.Headers(), "invalid key")
    sdk = _FakeSDK(fail=blad)
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    monkeypatch.setattr(destylacja, "ROWNOLEGLE", 1)
    s = _session()
    clusters = {1: [{"service_name": f"Usługa {i}"} for i in range(10)]}
    assert _run(s.profiles_for("Fryzjer", SUBJECT, clusters)) == {}
    assert len(sdk.names) == 1 and sdk.closed
    assert s.service.client.profiles.upserts == []


def test_limit_czasu_urywa_i_zostawia_to_co_zdazylo(monkeypatch):
    sdk = _FakeSDK(delay=0.5)
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    monkeypatch.setattr(destylacja, "LIMIT_CZASU_S", 0.05)
    s = _session()
    assert _run(s.profiles_for("Fryzjer", SUBJECT, CLUSTERS)) == {}
    assert s.skipped == 2 and sdk.closed


def test_bez_klucza_i_bez_branzy_nic_nie_wola(monkeypatch):
    monkeypatch.setattr(destylacja, "_client", lambda: None)
    assert _run(_session().profiles_for("Fryzjer", SUBJECT, CLUSTERS)) == {}
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    assert _run(_session().profiles_for(None, SUBJECT, CLUSTERS)) == {}
    assert sdk.names == []


def test_brak_tabeli_nie_wywraca_raportu(monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)

    class _Zepsuta:
        def table(self, name):
            raise RuntimeError('relation "service_profile" does not exist')

    s = destylacja.ProfileSession(SimpleNamespace(client=_Zepsuta()))
    prof = _run(s.profiles_for("Fryzjer", SUBJECT, CLUSTERS))
    assert len(prof) == 2  # profile działają w tym raporcie, zapis się nie udał


# ── integracja z wyceną raportu ─────────────────────────────────────────────

def _pricing(monkeypatch, source: str, sdk: _FakeSDK):
    monkeypatch.setattr(rp.settings, "taxonomy_veto_source", source)
    monkeypatch.setattr(rp.settings, "typesafe_api_key", "test-bez-sieci")
    monkeypatch.setattr(destylacja, "_client", lambda: sdk)
    cluster = [_twin(10 + i, 500 + i, "Strzyżenie męskie", 9000 + i * 100, 30) for i in range(5)]
    cluster += [_twin(20 + i, 600 + i, "Strzyżenie damskie", 9000 + i * 100, 30) for i in range(5)]
    _patch_search(monkeypatch, cluster)
    booksy = [*range(500, 505), *range(600, 605)]
    client = _Client(booksy, [{"id": 7000 + b, "booksy_id": b, "name": f"Salon {b}"} for b in booksy])
    subject = {"booksy_id": 1, "services": [{
        "id": 1, "name": "Strzyżenie męskie", "price_grosze": 9500, "duration_minutes": 30,
        "category_name": "Strzyżenie", "is_package": False, "booksy_treatment_id": 233,
    }]}
    rows = _run(rp.compute_pricing_comparisons_v2(_Service(client), 1, subject, [(_Cand(500), {})]))
    return rows[0], client


def test_wycena_z_profilami_tnie_druga_plec(monkeypatch):
    sdk = _FakeSDK()
    row, client = _pricing(monkeypatch, "typesafe", sdk)
    salony = {s["booksy_id"] for s in row["competitor_samples"]}
    assert salony == set(range(500, 505))  # damskie wycięte przez weto odbiorcy
    assert len(client.profiles.upserts) == 1  # profile zapisane trwale


def test_flaga_glm_nie_woła_typesafe(monkeypatch):
    sdk = _FakeSDK()
    row, client = _pricing(monkeypatch, "glm", sdk)
    assert sdk.names == [] and client.profiles.upserts == []
    salony = {s["booksy_id"] for s in row["competitor_samples"]}
    assert salony & set(range(600, 605))  # bez weta profili damskie zostają jak dotąd


def test_typesafe_bez_klucza_wraca_do_glm(monkeypatch):
    monkeypatch.setattr(rp.settings, "taxonomy_veto_source", "typesafe")
    monkeypatch.setattr(rp.settings, "typesafe_api_key", "")
    assert rp._profile_session(_Service(_Client([], []))) is None
