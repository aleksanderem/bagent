"""Sędzia par z pamięcią (services/typesafe_ocena).

Kontrakt: para (A,B) i (B,A) to jedna ocena; para oceniona raz nie trafia
do modelu drugi raz (ani w sesji, ani z bazy); nowe oceny zapisują się
trwale; budżet i błąd pojedynczej pary nie wywracają sesji.
Atrapa klienta SDK i bazy — zero sieci, zero klucza.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from services.typesafe_ocena import pamiec
from services.typesafe_ocena.pytania import WERSJA, para_klucz, stan, strona, werdykt, wynik

MANI = strona("Manicure hybrydowy", "Paznokcie", "Paznokcie")
MANI_CAPS = strona("  MANICURE   hybrydowy ", "paznokcie", "PAZNOKCIE")
PEDI = strona("Pedicure hybrydowy", "Paznokcie", "Paznokcie")
MANI_INNA_KAT = strona("Manicure hybrydowy", "Stopy", "Paznokcie")


@pytest.fixture(autouse=True)
def _bez_kosztow(monkeypatch):
    async def _nic(**_: Any) -> None:
        return None

    monkeypatch.setattr(pamiec, "capture_ai_generation", _nic)


class _Odp:
    def __init__(self, score: float, tokeny: int = 500) -> None:
        self.scores = {"relacja": SimpleNamespace(score=score, confidence=0.8, probabilities={0: 0.1, 1: 0.2, 2: 0.7})}
        self.nouls = {"ten_sam_zabieg": SimpleNamespace(noul=0.9)}
        self.usage = SimpleNamespace(input_tokens=tokeny)


class _Klient:
    def __init__(self, score: float = 1.9, blad_dla: str | None = None) -> None:
        self.wywolania: list[dict[str, Any]] = []
        self.score = score
        self.blad_dla = blad_dla

    async def system_one(self, state, questions, model=None):
        self.wywolania.append(state)
        if self.blad_dla and self.blad_dla in (state["usluga_a"]["nazwa"], state["usluga_b"]["nazwa"]):
            raise RuntimeError("503")
        return _Odp(self.score)


class _Tabela:
    def __init__(self, baza: "_Baza") -> None:
        self.baza = baza
        self._in: list[str] = []

    def select(self, *_):
        return self

    def eq(self, *_):
        return self

    def in_(self, _kol, wartosci):
        self._in = list(wartosci)
        return self

    def upsert(self, wiersze, on_conflict=None):
        self.baza.zapisane.extend(wiersze)
        return self

    def execute(self):
        rows = [{"para_klucz": k, "wynik": w} for k, w in self.baza.pamiec.items() if k in self._in]
        return SimpleNamespace(data=rows)


class _Baza:
    def __init__(self, pamiec_: dict[str, dict[str, Any]] | None = None) -> None:
        self.pamiec = pamiec_ or {}
        self.zapisane: list[dict[str, Any]] = []

    def table(self, _nazwa):
        return _Tabela(self)


def test_klucz_symetryczny_i_odporny_na_pisownie():
    assert para_klucz(MANI, PEDI) == para_klucz(PEDI, MANI)
    assert para_klucz(MANI_CAPS, PEDI) == para_klucz(MANI, PEDI)


def test_inna_kategoria_to_inna_ocena():
    assert para_klucz(MANI, PEDI) != para_klucz(MANI_INNA_KAT, PEDI)


def test_stan_niezalezny_od_kolejnosci():
    assert stan(MANI, PEDI) == stan(PEDI, MANI)


@pytest.mark.parametrize("score,oczekiwany", [(0.0, "rozne"), (0.49, "rozne"), (0.5, "powiazane"),
                                               (1.49, "powiazane"), (1.5, "tozsame"), (2.0, "tozsame")])
def test_werdykt_najblizszy_poziom(score, oczekiwany):
    assert werdykt(score) == oczekiwany


def test_wynik_zapisywalny():
    w = wynik(_Odp(1.2))
    assert w["werdykt"] == "powiazane"
    assert w["rozklad"] == [0.1, 0.2, 0.7]
    assert w["cechy"] == {"ten_sam_zabieg": 0.9}


async def test_para_pytana_raz_i_zapisana():
    baza, klient = _Baza(), _Klient()
    sesja = pamiec.OcenaPar(baza, klient, budzet_usd=1.0)
    out = await sesja.ocen([(MANI, PEDI), (PEDI, MANI), (MANI_CAPS, PEDI)])
    assert len(klient.wywolania) == 1
    assert len(out) == 1
    assert len(baza.zapisane) == 1
    assert baza.zapisane[0]["wersja"] == WERSJA
    # druga runda w tej samej sesji — bez modelu
    await sesja.ocen([(PEDI, MANI)])
    assert len(klient.wywolania) == 1


async def test_para_z_bazy_nie_idzie_do_modelu():
    k = para_klucz(MANI, PEDI)
    baza, klient = _Baza({k: {"werdykt": "rozne", "score": 0.1}}), _Klient()
    sesja = pamiec.OcenaPar(baza, klient, budzet_usd=1.0)
    out = await sesja.ocen([(MANI, PEDI)])
    assert klient.wywolania == []
    assert out[k]["werdykt"] == "rozne"
    assert sesja.z_pamieci == 1 and sesja.nowych == 0


async def test_budzet_zatrzymuje_pytania():
    klient = _Klient()
    sesja = pamiec.OcenaPar(_Baza(), klient, budzet_usd=0.0)
    out = await sesja.ocen([(MANI, PEDI)])
    assert out == {} and klient.wywolania == [] and sesja.pominietych == 1


async def test_blad_jednej_pary_nie_wywraca_sesji():
    baza = _Baza()
    sesja = pamiec.OcenaPar(baza, _Klient(blad_dla="Pedicure hybrydowy"), budzet_usd=1.0)
    out = await sesja.ocen([(MANI, PEDI), (MANI, MANI_INNA_KAT)])
    assert len(out) == 1 and sesja.pominietych == 1 and len(baza.zapisane) == 1


async def test_bez_bazy_dziala_w_pamieci_sesji():
    sesja = pamiec.OcenaPar(None, _Klient(), budzet_usd=1.0)
    out = await sesja.ocen([(MANI, PEDI)])
    assert len(out) == 1
