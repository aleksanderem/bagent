"""Stan b-card / b-match — sekcja autodiagnostyki „bmatch” (zakładka panelu „b-card / b-match”).

Panel ma pokazać bez czytania kodu i wchodzenia na serwer: który silnik liczy raporty (MATCHING_SOURCE), kolejkę
dostawców kart graficznych (BMATCH_DOSTAWCY: Modal, Runpod), czy któryś nie wisi z włączoną kartą, ile kosztowali
naprawdę (rozliczenia Runpod i Modal, dziennie), saldo Runpod, historię wycen (dzień po dniu, ostatnie przebiegi: kto
liczył, czy był potrzebny dostawca zapasowy, czas, błędy), nocne odświeżanie ofert oraz rozmiar danych.

Liczby z bazy: wiersze bmatch_przebieg z 14 dni (liczone tutaj; tryb „odswiezanie” = nocne odświeżanie ofert, reszta
= wyceny raportów) + rozmiary tabel z `fn_bmatch_stats` (mig 204/205). Stan Modal (`get_current_stats`) nie budzi
kontenera. Każda część osobno: błąd jednej = pole ``error`` w tej części, reszta diagnostyki leci normalnie.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from config import settings

logger = logging.getLogger(__name__)

REST = "https://rest.runpod.io/v1"
API = "https://api.runpod.ai/v2"
GRAPHQL = "https://api.runpod.io/graphql"
TIMEOUT_S = 10
DNI = 14
OSTATNICH = 20
WYCENY = ("bcard", "bcard_cien")
ODSWIEZANIE = "odswiezanie"
MODAL_APP = "bmatch"
POLA_PRZEBIEGU = ("id,created_at,report_id,tryb,ok,czas_s,uslug,z_karta,z_kandydatami,par,par_nowych,wierszy_bmatch,"
                  "gpu_bmatch_s,gpu_bcard_s,blad,szczegoly")


def _blad(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e)[:160]}"


def _czas(t: Any) -> datetime:
    s = str(t or "")
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            d = datetime.strptime(s, f)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


# ── Runpod ────────────────────────────────────────────────────────────────────

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


def _runpod(sekcja: dict[str, Any]) -> dict[str, float]:
    """Punkty, saldo i rozliczenia Runpod → {dzień: USD} (do kosztów dziennych)."""
    klucz = getattr(settings, "runpod_api_key", "")
    dziennie: dict[str, float] = defaultdict(float)
    if not klucz:
        return dziennie
    with httpx.Client(timeout=TIMEOUT_S, headers={"Authorization": f"Bearer {klucz}"}) as http:
        rozliczenia: list[dict[str, Any]] = []
        try:
            od = (datetime.now(timezone.utc) - timedelta(days=DNI)).strftime("%Y-%m-%dT00:00:00Z")
            r = http.get(f"{REST}/billing/endpoints", params={"bucketSize": "day", "startTime": od})
            r.raise_for_status()
            rozliczenia = r.json() if isinstance(r.json(), list) else []
        except Exception as e:  # noqa: BLE001
            sekcja["rozliczenia_error"] = _blad(e)
        nasze = {settings.bmatch_endpoint_id, settings.bcard_endpoint_id} - {""}
        for x in rozliczenia:
            if x.get("endpointId") in nasze:
                dziennie[_czas(x.get("time")).strftime("%Y-%m-%d")] += float(x.get("amount") or 0)
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
    return dziennie


# ── Modal ─────────────────────────────────────────────────────────────────────

def _modal() -> tuple[dict[str, Any], dict[str, float]]:
    """Konfiguracja, stan funkcji (bez budzenia) i rozliczenie Modal → (sekcja, {dzień: USD})."""
    m: dict[str, Any] = {
        "skonfigurowany": bool(settings.modal_bmatch_token and settings.modal_bmatch_url and settings.modal_bcard_url),
        "rozliczenia": bool(settings.modal_token_id and settings.modal_token_secret),
    }
    dziennie: dict[str, float] = defaultdict(float)
    if not m["rozliczenia"]:
        return m, dziennie
    try:
        import modal

        klient = modal.Client.from_credentials(settings.modal_token_id, settings.modal_token_secret)
        funkcje = {}
        for rola in ("bmatch", "bcard"):
            s = modal.Function.from_name(MODAL_APP, rola, client=klient).get_current_stats()
            funkcje[rola] = {"kontenery": int(s.num_total_runners), "kolejka": int(s.backlog),
                             "w_toku": int(s.num_running_inputs)}
        m["funkcje"] = funkcje
        teraz = datetime.now(timezone.utc)
        rozl = modal.Workspace.from_context(client=klient).billing
        # Dziennie: pełne dni z 14 dni; ostatnia doba i dzisiejszy niepełny dzień z raportu godzinowego (Modal: raport
        # godzinowy najwyżej 7 dni, dane tylko za pełne przedziały).
        dzis = teraz.replace(hour=0, minute=0, second=0, microsecond=0)
        dzienne = rozl.report(start=dzis - timedelta(days=DNI), end=dzis, resolution="d")
        godzinowe = rozl.report(start=dzis - timedelta(days=1), resolution="h")
        karty: dict[str, float] = defaultdict(float)
        usd_24h = 0.0
        for p in dzienne:
            if p.description == MODAL_APP:
                dziennie[p.interval_start.strftime("%Y-%m-%d")] += float(p.cost)
                if p.interval_start >= dzis - timedelta(days=7):
                    for zasob, k in (p.cost_by_resource or {}).items():
                        karty[zasob] += float(k)
        for p in godzinowe:
            if p.description != MODAL_APP:
                continue
            if p.interval_start >= dzis:
                dziennie[p.interval_start.strftime("%Y-%m-%d")] += float(p.cost)
                for zasob, k in (p.cost_by_resource or {}).items():
                    karty[zasob] += float(k)
            if p.interval_start >= teraz - timedelta(hours=24):
                usd_24h += float(p.cost)
        m["usd_24h"] = round(usd_24h, 4)
        m["usd_7d"] = round(sum(v for d, v in dziennie.items() if d >= (teraz - timedelta(days=7)).strftime("%Y-%m-%d")), 4)
        m["zasoby_7d"] = {k: round(v, 4) for k, v in sorted(karty.items(), key=lambda kv: -kv[1]) if v >= 0.001}
    except Exception as e:  # noqa: BLE001 — diagnostyka nie może wywrócić endpointu
        m["error"] = _blad(e)
    return m, dziennie


# ── Baza: rozmiary + historia przebiegów ──────────────────────────────────────

def _rozmiary(client: Any) -> dict[str, Any]:
    rows = client.rpc("fn_bmatch_stats", {}).execute().data or []
    row = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
    return {k: row.get(k) for k in ("kart", "ofert", "ofert_poza_beauty", "werdyktow")}


def _przebiegi(client: Any) -> list[dict[str, Any]]:
    od = (datetime.now(timezone.utc) - timedelta(days=DNI)).isoformat()
    out, start = [], 0
    while True:
        r = (client.table("bmatch_przebieg").select(POLA_PRZEBIEGU).gte("created_at", od)
             .order("created_at", desc=True).range(start, start + 999).execute().data or [])
        out += r
        if len(r) < 1000:
            return out
        start += 1000


def _rozruch(sz: dict[str, Any]) -> float | None:
    e = sz.get("etapy_s") or {}
    if e.get("bmatch_gotowy") is not None and e.get("przed_bmatch") is not None:
        return round(float(e["bmatch_gotowy"]) - float(e["przed_bmatch"]), 1)
    return None


def _statystyki(wyceny: list[dict[str, Any]], teraz: datetime) -> dict[str, Any]:
    """Pola zgodne z dawnym fn_bmatch_stats (kontrolka w Convex) — tylko wyceny raportów, bez odświeżania."""
    w7 = [w for w in wyceny if _czas(w["created_at"]) >= teraz - timedelta(days=7)]
    w24 = [w for w in w7 if _czas(w["created_at"]) >= teraz - timedelta(hours=24)]
    bledy = [w for w in w7 if not w.get("ok")]
    par = sum(int(w.get("par") or 0) for w in w7)
    nowych = sum(int(w.get("par_nowych") or 0) for w in w7)
    czasy = [float(w["czas_s"]) for w in w7 if w.get("ok") and w.get("czas_s") is not None]
    return {
        "przebiegow_24h": len(w24), "przebiegow_7d": len(w7),
        "bledow_24h": sum(1 for w in w24 if not w.get("ok")), "bledow_7d": len(bledy),
        "par_7d": par, "par_nowych_7d": nowych, "z_pamieci_7d": round(1 - nowych / par, 3) if par else None,
        "wierszy_bmatch_7d": sum(int(w.get("wierszy_bmatch") or 0) for w in w7),
        "uslug_7d": sum(int(w.get("uslug") or 0) for w in w7),
        "sredni_czas_s_7d": round(sum(czasy) / len(czasy), 1) if czasy else None,
        "gpu_bmatch_s_7d": round(sum(float(w.get("gpu_bmatch_s") or 0) for w in w7), 1),
        "gpu_bcard_s_7d": round(sum(float(w.get("gpu_bcard_s") or 0) for w in w7), 1),
        "ostatni": wyceny[0]["created_at"] if wyceny else None,
        "ostatni_blad": bledy[0].get("blad") if bledy else None,
        "ostatni_blad_kiedy": bledy[0]["created_at"] if bledy else None,
    }


def _historia(wiersze: list[dict[str, Any]], koszty: dict[str, dict[str, float]], teraz: datetime) -> dict[str, Any]:
    wyceny = [w for w in wiersze if w.get("tryb") in WYCENY]
    odswiez = [w for w in wiersze if w.get("tryb") == ODSWIEZANIE]
    dni: dict[str, dict[str, Any]] = {}
    for i in range(DNI):
        d = (teraz - timedelta(days=DNI - 1 - i)).strftime("%Y-%m-%d")
        dni[d] = {"dzien": d, "raportow": 0, "bledow": 0, "par": 0, "par_nowych": 0, "uslug": 0, "wierszy_bmatch": 0,
                  "gpu_s": 0.0, "kart_nocnych": 0, "dostawcy": defaultdict(int),
                  "usd_modal": round(koszty["modal"].get(d, 0.0), 4), "usd_runpod": round(koszty["runpod"].get(d, 0.0), 4)}
    for w in wyceny:
        x = dni.get(_czas(w["created_at"]).strftime("%Y-%m-%d"))
        if x is None:
            continue
        x["raportow"] += 1
        x["bledow"] += 0 if w.get("ok") else 1
        for k in ("par", "par_nowych", "uslug", "wierszy_bmatch"):
            x[k] += int(w.get(k) or 0)
        x["gpu_s"] += float(w.get("gpu_bmatch_s") or 0) + float(w.get("gpu_bcard_s") or 0)
        x["dostawcy"][(w.get("szczegoly") or {}).get("dostawca") or ("—" if w.get("ok") else "brak")] += 1
    for w in odswiez:
        x = dni.get(_czas(w["created_at"]).strftime("%Y-%m-%d"))
        if x is not None:
            x["kart_nocnych"] += int((w.get("szczegoly") or {}).get("kart_nowych") or 0)
    for x in dni.values():
        x["dostawcy"] = dict(x["dostawcy"])
        x["gpu_s"] = round(x["gpu_s"], 1)

    w7 = [w for w in wyceny if _czas(w["created_at"]) >= teraz - timedelta(days=7)]
    per_dostawca: dict[str, dict[str, Any]] = {}
    for w in w7:
        sz = w.get("szczegoly") or {}
        d = sz.get("dostawca") or ("—" if w.get("ok") else "brak")
        a = per_dostawca.setdefault(d, {"raportow": 0, "rozruchy": [], "gpu_s": 0.0})
        a["raportow"] += 1
        a["gpu_s"] += float(w.get("gpu_bmatch_s") or 0) + float(w.get("gpu_bcard_s") or 0)
        if (r := _rozruch(sz)) is not None:
            a["rozruchy"].append(r)
    dostawcy_7d = {d: {"raportow": a["raportow"], "gpu_s": round(a["gpu_s"], 1),
                       "rozruch_sredni_s": round(sum(a["rozruchy"]) / len(a["rozruchy"]), 1) if a["rozruchy"] else None}
                   for d, a in per_dostawca.items()}

    def przebieg(w: dict[str, Any]) -> dict[str, Any]:
        sz = w.get("szczegoly") or {}
        return {"kiedy": w["created_at"], "raport": w.get("report_id"), "tryb": w.get("tryb"), "ok": bool(w.get("ok")),
                "dostawca": sz.get("dostawca"), "proby": [str(p)[:160] for p in (sz.get("proby") or [])],
                "czas_s": w.get("czas_s"), "rozruch_s": _rozruch(sz), "uslug": w.get("uslug"),
                "wierszy_bmatch": w.get("wierszy_bmatch"), "par": w.get("par"), "par_nowych": w.get("par_nowych"),
                "gpu_s": round(float(w.get("gpu_bmatch_s") or 0) + float(w.get("gpu_bcard_s") or 0), 1),
                "blad": (w.get("blad") or None) and str(w["blad"])[:300]}

    def nocne(w: dict[str, Any]) -> dict[str, Any]:
        sz = w.get("szczegoly") or {}
        return {"kiedy": w["created_at"], "ok": bool(w.get("ok")), "czas_s": w.get("czas_s"),
                "salonow": sz.get("salonow"), "uslug": w.get("uslug"), "brakow": sz.get("brakow"),
                "kart_nowych": sz.get("kart_nowych"), "gpu_s": w.get("gpu_bcard_s"), "dostawca": sz.get("dostawca"),
                "blad": (w.get("blad") or None) and str(w["blad"])[:300]}

    return {"dni": list(dni.values()), "dostawcy_7d": dostawcy_7d,
            "awaryjnie_7d": sum(1 for w in w7 if len((w.get("szczegoly") or {}).get("proby") or []) > 1),
            "ostatnie": [przebieg(w) for w in wyceny[:OSTATNICH]], "odswiezanie": [nocne(w) for w in odswiez[:7]]}


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
        "kolejnosc": [x.strip().lower() for x in (settings.bmatch_dostawcy or "").split(",") if x.strip()],
        "gotowy_s": settings.bmatch_gotowy_s,
    }
    koszty = {"runpod": _runpod(sekcja)}
    sekcja["modal"], koszty["modal"] = _modal()
    if client is None:
        sekcja["error"] = "brak połączenia z bazą"
        return sekcja
    try:
        teraz = datetime.now(timezone.utc)
        wiersze = _przebiegi(client)
        wyceny = [w for w in wiersze if w.get("tryb") in WYCENY]
        sekcja["baza"] = {**_rozmiary(client), **_statystyki(wyceny, teraz)}
        sekcja["historia"] = _historia(wiersze, koszty, teraz)
    except Exception as exc:  # noqa: BLE001 — diagnostyka nie może wywrócić endpointu
        logger.warning("diag bmatch niedostępny (%s): %s", type(exc).__name__, str(exc)[:160])
        sekcja["error"] = _blad(exc)
    return sekcja
