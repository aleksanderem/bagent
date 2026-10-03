"""Stan b-card / b-match — sekcja autodiagnostyki „bmatch” (zakładka panelu „b-card / b-match”).

Panel ma pokazać bez czytania kodu i wchodzenia na serwer: który silnik liczy raporty (MATCHING_SOURCE), czy jest
klucz i punkty, czy punkty Runpod nie wiszą z pracownikiem (nalicza ~4,8 USD/h), ile kosztowały naprawdę (rozliczenie
Runpod per punkt), saldo konta, ile raportów przeszło przez b-match, ile par (ile z pamięci), ile trwało, błędy,
oraz rozmiar danych (karty, oferty, werdykty). Liczby z bazy: jedno zapytanie `fn_bmatch_stats` (mig 204).

Każda część osobno: błąd jednej = pole ``error`` w tej części, reszta diagnostyki leci normalnie.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from config import settings

logger = logging.getLogger(__name__)

REST = "https://rest.runpod.io/v1"
API = "https://api.runpod.ai/v2"
GRAPHQL = "https://api.runpod.io/graphql"
TIMEOUT_S = 10


def _blad(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e)[:160]}"


def _punkt(http: httpx.Client, rola: str, endpoint_id: str, rozliczenia: list[dict[str, Any]]) -> dict[str, Any]:
    p: dict[str, Any] = {"rola": rola, "id": endpoint_id or None}
    if not endpoint_id:
        return p
    try:
        r = http.get(f"{API}/{endpoint_id}/health")
        r.raise_for_status()
        d = r.json()
        p["pracownicy"] = {k: int(v) for k, v in (d.get("workers") or {}).items() if v}
        p["zadania"] = {k: int(v) for k, v in (d.get("jobs") or {}).items() if k in ("inQueue", "inProgress", "failed", "completed")}
    except Exception as e:  # noqa: BLE001
        p["error"] = _blad(e)
    teraz = datetime.now(timezone.utc)
    moje = [x for x in rozliczenia if x.get("endpointId") == endpoint_id]
    for nazwa, od in (("24h", teraz - timedelta(hours=24)), ("7d", teraz - timedelta(days=7))):
        okno = [x for x in moje if _czas(x.get("time")) >= od.replace(hour=0, minute=0, second=0, microsecond=0)]
        p[f"usd_{nazwa}"] = round(sum(float(x.get("amount") or 0) for x in okno), 4)
        p[f"sekundy_{nazwa}"] = round(sum(float(x.get("timeBilledMs") or 0) for x in okno) / 1000, 1)
    return p


def _czas(t: Any) -> datetime:
    try:
        return datetime.strptime(str(t), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _stat_bazy(client: Any) -> dict[str, Any]:
    rows = client.rpc("fn_bmatch_stats", {}).execute().data or []
    row = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
    par, nowych = int(row.get("par_7d") or 0), int(row.get("par_nowych_7d") or 0)
    return {**{k: row.get(k) for k in (
        "kart", "ofert", "ofert_poza_beauty", "werdyktow", "przebiegow_24h", "przebiegow_7d", "bledow_24h",
        "bledow_7d", "par_7d", "par_nowych_7d", "wierszy_bmatch_7d", "uslug_7d", "sredni_czas_s_7d",
        "gpu_bmatch_s_7d", "gpu_bcard_s_7d", "ostatni", "ostatni_blad", "ostatni_blad_kiedy")},
        "z_pamieci_7d": round(1 - nowych / par, 3) if par else None}


def zbierz(client: Any | None) -> dict[str, Any]:
    """Sekcja „bmatch” do /api/internal/diag. `client` = klient Supabase albo None."""
    sekcja: dict[str, Any] = {
        "zrodlo": (getattr(settings, "matching_source", "stary") or "stary").strip().lower(),
        "klucz": bool(getattr(settings, "runpod_api_key", "")),
        "wersja_bmatch": settings.bmatch_wersja,
        "wersja_bcard": settings.bcard_wersja,
        "wersja_slownika": settings.bcard_wersja_slownika,
        "kandydatow": settings.bmatch_kandydatow,
        "limit_s": settings.bmatch_limit_s,
    }
    klucz = getattr(settings, "runpod_api_key", "")
    if klucz:
        with httpx.Client(timeout=TIMEOUT_S, headers={"Authorization": f"Bearer {klucz}"}) as http:
            rozliczenia: list[dict[str, Any]] = []
            try:
                od = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
                r = http.get(f"{REST}/billing/endpoints", params={"bucketSize": "day", "startTime": od})
                r.raise_for_status()
                rozliczenia = r.json() if isinstance(r.json(), list) else []
            except Exception as e:  # noqa: BLE001
                sekcja["rozliczenia_error"] = _blad(e)
            sekcja["punkty"] = [_punkt(http, "bmatch", settings.bmatch_endpoint_id, rozliczenia),
                                _punkt(http, "bcard", settings.bcard_endpoint_id, rozliczenia)]
            try:
                r = http.post(GRAPHQL, json={"query": "query { myself { clientBalance currentSpendPerHr } }"})
                r.raise_for_status()
                m = (r.json().get("data") or {}).get("myself") or {}
                sekcja["saldo_usd"] = round(float(m.get("clientBalance") or 0), 2)
                sekcja["wydatek_usd_h"] = round(float(m.get("currentSpendPerHr") or 0), 3)
            except Exception as e:  # noqa: BLE001
                sekcja["konto_error"] = _blad(e)
    if client is None:
        sekcja["error"] = "brak połączenia z bazą"
        return sekcja
    try:
        sekcja["baza"] = _stat_bazy(client)
    except Exception as exc:  # noqa: BLE001 — diagnostyka nie może wywrócić endpointu
        logger.warning("diag bmatch niedostępny (%s): %s", type(exc).__name__, str(exc)[:160])
        sekcja["error"] = _blad(exc)
    return sekcja
