"""Sekcja „destylacja” w autodiagnostyce — kontrakt z panelem admina.

Panel ma z tego złożyć kontrolkę: czym silnik odsiewa usługi tylko podobne,
czy ma czym, ile profili/tokenów/USD poszło w ostatniej dobie. Każdy błąd
bazy zostaje w sekcji jako `error` i nie wywraca reszty diagnostyki.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import services.diag_destylacja as dd
from services.typesafe_profile.destylacja import MODEL, USD_ZA_TOKEN


class _Client:
    def __init__(self, data: Any):
        self._data = data
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def rpc(self, name: str, params: dict[str, Any]):
        self.calls.append((name, params))
        return self

    def execute(self):
        if isinstance(self._data, Exception):
            raise self._data
        return SimpleNamespace(data=self._data)


STAT = [{
    "profili_razem": 12345,
    "profili_24h": 700,
    "tokeny_24h": 2_170_000,
    "branz_24h": 3,
    "ostatni": "2026-09-22T18:00:00+00:00",
}]


def test_liczby_z_jednego_zapytania_i_koszt_w_usd(monkeypatch):
    monkeypatch.setattr(dd.settings, "taxonomy_veto_source", "typesafe")
    monkeypatch.setattr(dd.settings, "typesafe_api_key", "klucz")
    cli = _Client(STAT)
    s = dd.zbierz(cli)
    assert cli.calls == [("fn_service_profile_stats", {})]  # nie skanujemy wierszy
    assert s["zrodlo"] == "typesafe" and s["klucz"] is True and s["model"] == MODEL
    assert s["profili_24h"] == 700 and s["tokeny_24h"] == 2_170_000
    assert s["usd_24h"] == round(2_170_000 * USD_ZA_TOKEN, 4)
    assert s["ostatni"] == "2026-09-22T18:00:00+00:00"
    assert "error" not in s


def test_brak_klucza_widac_w_sekcji(monkeypatch):
    monkeypatch.setattr(dd.settings, "taxonomy_veto_source", "typesafe")
    monkeypatch.setattr(dd.settings, "typesafe_api_key", "")
    s = dd.zbierz(_Client(STAT))
    assert s["zrodlo"] == "typesafe" and s["klucz"] is False


def test_bez_bazy_i_przy_bledzie_sekcja_nie_wywraca_diagnostyki():
    bez_bazy = dd.zbierz(None)
    assert bez_bazy["error"] == "brak połączenia z bazą" and bez_bazy["model"] == MODEL
    zepsuta = dd.zbierz(_Client(RuntimeError('relation "service_profile" does not exist')))
    assert zepsuta["error"].startswith("RuntimeError") and "profili_24h" not in zepsuta


def test_pusta_odpowiedz_to_zera(monkeypatch):
    s = dd.zbierz(_Client([]))
    assert s["profili_24h"] == 0 and s["tokeny_24h"] == 0 and s["usd_24h"] == 0.0
