"""Odczyt/zapis tabel b-card / b-match (migracja 203 w BEAUTY_AUDIT): bcard_karta, bcard_oferta, bmatch_werdykt,
bmatch_cien, funkcja fn_bcard_kandydaci. Klient = supabase-py (synchroniczny, jak reszta bagenta)."""
from __future__ import annotations

from typing import Any

PACZKA = 500
STRONA = 1000
POLA_USLUGI = ("id,booksy_id,name,category_name,description,variants,treatment_name,duration_minutes,"
               "price_grosze,is_package,is_active")


def _paczki(xs: list[Any], n: int = PACZKA):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


def karty(client: Any, klucze: set[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for p in _paczki(sorted(klucze)):
        for r in client.table("bcard_karta").select("klucz,karta").in_("klucz", p).execute().data or []:
            out[r["klucz"]] = r["karta"]
    return out


def zapisz_karty(client: Any, wiersze: list[dict[str, Any]]) -> None:
    for p in _paczki(wiersze):
        client.table("bcard_karta").upsert(p, on_conflict="klucz").execute()


def kandydaci(client: Any, booksy_ids: list[int], zabiegi: list[str]) -> list[dict[str, Any]]:
    """Oferty z salonów w promieniu z którymś z zabiegów (strony po 1000 — limit PostgREST)."""
    out: list[dict[str, Any]] = []
    od = 0
    while True:
        r = (client.rpc("fn_bcard_kandydaci", {"p_booksy_ids": booksy_ids, "p_zabiegi": zabiegi})
             .range(od, od + STRONA - 1).execute())
        dane = r.data or []
        out += dane
        if len(dane) < STRONA:
            return out
        od += STRONA


def uslugi(client: Any, service_ids: list[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for p in _paczki(sorted(set(service_ids))):
        for r in client.table("salon_scrape_services").select(POLA_USLUGI).in_("id", p).execute().data or []:
            out[int(r["id"])] = r
    return out


def werdykty(client: Any, pary: set[str], wersja: str) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for p in _paczki(sorted(pary)):
        res = (client.table("bmatch_werdykt").select("para_klucz,p_inna,p_odmiana,p_ta_sama")
               .eq("wersja_modelu", wersja).in_("para_klucz", p).execute())
        for r in res.data or []:
            out[r["para_klucz"]] = [r["p_inna"], r["p_odmiana"], r["p_ta_sama"]]
    return out


def zapisz_werdykty(client: Any, wynik: dict[str, list[float]], wersja: str) -> None:
    wiersze = [{"para_klucz": k, "wersja_modelu": wersja, "p_inna": p[0], "p_odmiana": p[1], "p_ta_sama": p[2]}
               for k, p in wynik.items()]
    for p in _paczki(wiersze):
        client.table("bmatch_werdykt").upsert(p, on_conflict="para_klucz,wersja_modelu").execute()


def zapisz_cien(client: Any, wiersze: list[dict[str, Any]]) -> None:
    for p in _paczki(wiersze):
        client.table("bmatch_cien").insert(p).execute()
