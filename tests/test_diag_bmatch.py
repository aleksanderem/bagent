"""services/diag_bmatch — sekcja „bmatch” autodiagnostyki (zakładka panelu „b-card / b-match”)."""
from unittest.mock import MagicMock, patch

import httpx
import pytest

from services import diag_bmatch


def _odpowiedz(url: str, **_):
    if "/health" in url:
        return httpx.Response(200, json={"workers": {"running": 1, "idle": 0}, "jobs": {"inQueue": 0, "completed": 5}},
                              request=httpx.Request("GET", url))
    if "billing" in url:
        return httpx.Response(200, json=[
            {"endpointId": "bm", "amount": 0.5, "timeBilledMs": 600000, "time": "2099-01-01 00:00:00"},
            {"endpointId": "bc", "amount": 0.1, "timeBilledMs": 100000, "time": "2099-01-01 00:00:00"}],
            request=httpx.Request("GET", url))
    return httpx.Response(200, json={"data": {"myself": {"clientBalance": 4.71, "currentSpendPerHr": 0.003}}},
                          request=httpx.Request("POST", url))


def _klient_bazy():
    c = MagicMock()
    c.rpc.return_value.execute.return_value.data = [{"kart": 1566347, "par_7d": 1000, "par_nowych_7d": 250,
                                                     "przebiegow_7d": 3, "bledow_7d": 1, "ostatni_blad": "Timeout"}]
    return c


@pytest.fixture
def ustawienia(monkeypatch):
    s = diag_bmatch.settings
    monkeypatch.setattr(s, "matching_source", "bcard")
    monkeypatch.setattr(s, "runpod_api_key", "klucz")
    monkeypatch.setattr(s, "bmatch_endpoint_id", "bm")
    monkeypatch.setattr(s, "bcard_endpoint_id", "bc")


def test_pelna_sekcja(ustawienia):
    with patch.object(httpx.Client, "get", side_effect=_odpowiedz), \
         patch.object(httpx.Client, "post", side_effect=_odpowiedz):
        s = diag_bmatch.zbierz(_klient_bazy())
    assert s["zrodlo"] == "bcard" and s["klucz"] is True
    bm = next(p for p in s["punkty"] if p["rola"] == "bmatch")
    assert bm["pracownicy"] == {"running": 1} and bm["usd_7d"] == 0.5 and bm["sekundy_7d"] == 600.0
    assert s["saldo_usd"] == 4.71 and s["wydatek_usd_h"] == 0.003
    assert s["baza"]["kart"] == 1566347 and s["baza"]["z_pamieci_7d"] == 0.75


def test_bez_klucza_bez_runpod(monkeypatch):
    monkeypatch.setattr(diag_bmatch.settings, "runpod_api_key", "")
    s = diag_bmatch.zbierz(_klient_bazy())
    assert s["klucz"] is False and "punkty" not in s and s["baza"]["przebiegow_7d"] == 3


def test_awaria_runpod_nie_wywraca(ustawienia):
    with patch.object(httpx.Client, "get", side_effect=httpx.ConnectError("brak sieci")), \
         patch.object(httpx.Client, "post", side_effect=httpx.ConnectError("brak sieci")):
        s = diag_bmatch.zbierz(_klient_bazy())
    assert "rozliczenia_error" in s and "konto_error" in s and all("error" in p for p in s["punkty"])
    assert s["baza"]["kart"] == 1566347


def test_brak_bazy():
    s = diag_bmatch.zbierz(None)
    assert s["error"] == "brak połączenia z bazą"
