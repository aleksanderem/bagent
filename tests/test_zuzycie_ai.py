"""Zużycie modeli: cennik, zapis dobowy i sekcja „koszty” w autodiagnostyce.

Kontrakt: każde wywołanie modelu dopisuje się do podsumowania dobowego wraz
z kosztem, a model bez cennika ma koszt PUSTY (nie zero) — panel ma mówić
„nie znamy ceny", a nie pokazywać wymyśloną kwotę.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import services.diag_koszty as dk
import services.posthog_analytics as pa
import services.zuzycie_ai as za
from services.ceny_modeli import CENNIK, koszt_usd


class _Client:
    def __init__(self, data: Any = None):
        self.wywolania: list[tuple[str, dict[str, Any]]] = []
        self._data = data

    def rpc(self, name: str, params: dict[str, Any]):
        self.wywolania.append((name, params))
        return self

    def execute(self):
        if isinstance(self._data, Exception):
            raise self._data
        return SimpleNamespace(data=self._data)


def _run(coro):
    return asyncio.run(coro)


def _podepnij(monkeypatch, cli: _Client | None) -> None:
    monkeypatch.setattr(za, "_client", lambda: cli)


# ── cennik ──────────────────────────────────────────────────────────────────

def test_cena_liczona_z_tokenow_wejscia_i_wyjscia():
    usd, szacunek = koszt_usd("typesafe", "jev-1.13.0", 3_000_000, 999)
    assert usd == 0.126 and szacunek is False  # wyjście u nich darmowe


def test_model_bez_wpisu_nie_dostaje_wymyslonej_ceny():
    assert koszt_usd("minimax", "MiniMax-M3", 12_000, 3_000) == (None, False)


def test_cena_z_publicznego_cennika_jest_oznaczona_jako_szacunek():
    usd, szacunek = koszt_usd("openai", "gpt-4o-mini", 1_000_000, 0)
    assert usd == 0.15 and szacunek is True


def test_kazdy_wpis_cennika_mowi_skad_jest():
    assert all(c.zrodlo.strip() for c in CENNIK.values())


# ── zapis dobowy ────────────────────────────────────────────────────────────

def test_wywolanie_dopisuje_sie_z_kosztem(monkeypatch):
    cli = _Client()
    _podepnij(monkeypatch, cli)
    _run(za.dopisz(
        provider="typesafe", model="jev-1.13.0", span_name="destylacja",
        input_tokens=3000, output_tokens=0,
    ))
    (nazwa, p), = cli.wywolania
    assert nazwa == "fn_ai_zuzycie_dopisz"
    assert p["p_mechanizm"] == "destylacja" and p["p_tokeny_wej"] == 3000
    assert p["p_usd"] == 0.000126 and p["p_blad"] is False


def test_brak_cennika_zapisuje_tokeny_bez_kwoty(monkeypatch):
    cli = _Client()
    _podepnij(monkeypatch, cli)
    _run(za.dopisz(provider="minimax", model="MiniMax-M3", span_name="audyt", input_tokens=12_000))
    (_, p), = cli.wywolania
    assert p["p_usd"] is None and p["p_tokeny_wej"] == 12_000


def test_blad_modelu_tez_sie_liczy(monkeypatch):
    cli = _Client()
    _podepnij(monkeypatch, cli)
    _run(za.dopisz(provider="openai", model="gpt-4o-mini", span_name="synteza", error=True))
    (_, p), = cli.wywolania
    assert p["p_blad"] is True and p["p_usd"] == 0.0


def test_awaria_bazy_nie_wywraca_wywolania_modelu(monkeypatch):
    _podepnij(monkeypatch, _Client(RuntimeError("baza padła")))
    _run(za.dopisz(provider="typesafe", model="jev-1.13.0", span_name="destylacja", input_tokens=1))
    _podepnij(monkeypatch, None)
    _run(za.dopisz(provider="typesafe", model="jev-1.13.0", span_name="destylacja", input_tokens=1))


def test_pomiar_do_posthoga_pociaga_zapis_dobowy(monkeypatch):
    """Jedno miejsce pomiaru = wszystkie ścieżki modelu trafiają do podsumowania."""
    zapisane: list[dict[str, Any]] = []

    async def _fake(**kw: Any) -> None:
        zapisane.append(kw)

    monkeypatch.setattr(pa, "dopisz_zuzycie", _fake)
    monkeypatch.delenv("POSTHOG_PROJECT_KEY", raising=False)  # bez ruchu na zewnątrz
    _run(pa.capture_ai_generation(
        provider="minimax", model="MiniMax-M3", span_name="audyt",
        started_at=0.0, input_tokens=10, output_tokens=5,
    ))
    assert zapisane == [{
        "provider": "minimax", "model": "MiniMax-M3", "span_name": "audyt",
        "input_tokens": 10, "output_tokens": 5, "error": False,
    }]


# ── sekcja „koszty" w diagnostyce ───────────────────────────────────────────

WIERSZE = [
    {"mechanizm": "destylacja", "dostawca": "typesafe", "model": "jev-1.13.0", "wywolan": 700,
     "bledow": 0, "tokeny_wej": 2_170_000, "tokeny_wyj": 0, "usd": 0.0911, "bez_ceny": 0,
     "usd_szacunek": False},
    {"mechanizm": "audyt", "dostawca": "minimax", "model": "MiniMax-M3", "wywolan": 40,
     "bledow": 2, "tokeny_wej": 500_000, "tokeny_wyj": 120_000, "usd": None, "bez_ceny": 40,
     "usd_szacunek": False},
]


def test_sekcja_sumuje_koszt_tokeny_i_wywolania_bez_cennika():
    s = dk.zbierz(_Client(WIERSZE))
    assert s["mechanizmow"] == 2 and s["wywolan"] == 740 and s["bledow"] == 2
    assert s["tokeny"] == 2_790_000
    assert s["usd"] == 0.0911  # wiersz bez ceny nie udaje zera w sumie
    assert s["bez_ceny"] == 40
    assert s["pozycje"][0]["mechanizm"] == "destylacja"


def test_sekcja_przezywa_brak_bazy_i_blad():
    assert dk.zbierz(None)["error"] == "brak połączenia z bazą"
    assert dk.zbierz(_Client(RuntimeError("brak funkcji")))["error"].startswith("RuntimeError")
