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


def _teraz(h=0):
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(hours=h)).isoformat()


PRZEBIEGI = [
    {"id": 3, "created_at": _teraz(1), "report_id": 250, "tryb": "bcard", "ok": True, "czas_s": 143.6, "uslug": 212,
     "par": 1000, "par_nowych": 250, "wierszy_bmatch": 148, "gpu_bmatch_s": 150.0, "gpu_bcard_s": None, "blad": None,
     "szczegoly": {"dostawca": "runpod", "proby": ["modal: brak karty", "runpod: ok"],
                   "etapy_s": {"przed_bmatch": 26.0, "bmatch_gotowy": 200.0}}},
    {"id": 2, "created_at": _teraz(2), "report_id": None, "tryb": "odswiezanie", "ok": True, "czas_s": 900.0,
     "uslug": 26935, "par": None, "par_nowych": None, "wierszy_bmatch": None, "gpu_bmatch_s": None, "gpu_bcard_s": 300.0,
     "blad": None, "szczegoly": {"salonow": 1000, "brakow": 52, "kart_nowych": 50, "dostawca": "modal"}},
    {"id": 1, "created_at": _teraz(30), "report_id": 249, "tryb": "bcard", "ok": False, "czas_s": None, "uslug": 100,
     "par": None, "par_nowych": None, "wierszy_bmatch": None, "gpu_bmatch_s": None, "gpu_bcard_s": None,
     "blad": "BladPunktu: żaden dostawca nie wstał", "szczegoly": None},
]


def _klient_bazy():
    c = MagicMock()
    c.rpc.return_value.execute.return_value.data = [{"kart": 1566347, "ofert": 10, "ofert_poza_beauty": 1, "werdyktow": 7}]
    t = c.table.return_value.select.return_value.gte.return_value.order.return_value.range.return_value
    t.execute.return_value.data = PRZEBIEGI
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
    b = s["baza"]
    assert b["kart"] == 1566347 and b["z_pamieci_7d"] == 0.75
    assert b["przebiegow_7d"] == 2 and b["bledow_7d"] == 1  # odświeżanie nie liczy się jako raport
    assert b["ostatni_blad"].startswith("BladPunktu")
    h = s["historia"]
    assert len(h["dni"]) == diag_bmatch.DNI and h["awaryjnie_7d"] == 1
    assert h["dostawcy_7d"]["runpod"] == {"raportow": 1, "gpu_s": 150.0, "rozruch_sredni_s": 174.0}
    assert h["dostawcy_7d"]["brak"]["raportow"] == 1
    assert h["ostatnie"][0]["dostawca"] == "runpod" and h["ostatnie"][0]["proby"][0].startswith("modal")
    assert h["odswiezanie"][0]["kart_nowych"] == 50 and h["odswiezanie"][0]["dostawca"] == "modal"
    assert sum(d["usd_runpod"] for d in h["dni"]) == 0  # rozliczenia z 2099 poza oknem 14 dni
    assert s["modal"] == {"skonfigurowany": False, "rozliczenia": False}


def test_bez_klucza_bez_runpod(monkeypatch):
    monkeypatch.setattr(diag_bmatch.settings, "runpod_api_key", "")
    s = diag_bmatch.zbierz(_klient_bazy())
    assert s["klucz"] is False and "punkty" not in s and s["baza"]["przebiegow_7d"] == 2


def test_awaria_runpod_nie_wywraca(ustawienia):
    with patch.object(httpx.Client, "get", side_effect=httpx.ConnectError("brak sieci")), \
         patch.object(httpx.Client, "post", side_effect=httpx.ConnectError("brak sieci")):
        s = diag_bmatch.zbierz(_klient_bazy())
    assert "rozliczenia_error" in s and "konto_error" in s and all("error" in p for p in s["punkty"])
    assert s["baza"]["kart"] == 1566347


def test_brak_bazy():
    s = diag_bmatch.zbierz(None)
    assert s["error"] == "brak połączenia z bazą"


def test_modal_koszty_i_stan_bez_budzenia(ustawienia, monkeypatch):
    import sys
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal
    from types import SimpleNamespace

    s = diag_bmatch.settings
    for k, v in (("modal_token_id", "ak"), ("modal_token_secret", "as"), ("modal_bmatch_token", "t")):
        monkeypatch.setattr(s, k, v)
    teraz = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    pozycje = [SimpleNamespace(description="bmatch", interval_start=teraz - timedelta(hours=2), cost=Decimal("0.40"),
                               cost_by_resource={"H100": Decimal("0.39"), "CPU": Decimal("0.01")}),
               SimpleNamespace(description="gpu-test", interval_start=teraz - timedelta(hours=2), cost=Decimal("9"),
                               cost_by_resource={"T4": Decimal("9")})]
    modal = MagicMock()
    modal.Function.from_name.return_value.get_current_stats.return_value = SimpleNamespace(
        num_total_runners=0, backlog=0, num_running_inputs=0)
    modal.Workspace.from_context.return_value.billing.report.side_effect = (
        lambda start, end=None, resolution="d": [] if resolution == "d" else pozycje)
    monkeypatch.setitem(sys.modules, "modal", modal)
    with patch.object(httpx.Client, "get", side_effect=_odpowiedz), \
         patch.object(httpx.Client, "post", side_effect=_odpowiedz):
        sek = diag_bmatch.zbierz(_klient_bazy())
    m = sek["modal"]
    assert m["skonfigurowany"] and m["rozliczenia"] and m["usd_24h"] == 0.4 and m["usd_7d"] == 0.4
    assert m["zasoby_7d"] == {"H100": 0.39, "CPU": 0.01} and m["funkcje"]["bmatch"]["kontenery"] == 0
    assert sum(d["usd_modal"] for d in sek["historia"]["dni"]) == 0.4  # inne aplikacje Modal pominięte
