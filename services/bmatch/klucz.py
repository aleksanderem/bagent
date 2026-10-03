"""Kontrakt danych z projektami b-card / b-match (~/projects/b-card, ~/projects/b-match).

Klucz usługi i kształt wejścia modeli MUSZĄ być identyczne z tymi, na których modele się uczyły — inaczej karta nie
zostanie znaleziona w pamięci, a b-match dostanie wejście w innym kształcie niż przy nauce. Źródło prawdy:
b-card `kontrakt/kontrakt.py` (klucz_uslugi, wejscie) i b-match `trening/dane.py` (_strona_pelna, stan_z_karta).
Zgodność pilnuje tests/test_bmatch_klucz.py (wartości wyliczone kodem b-card).
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

SYSTEM_BCARD = ("Rozłóż usługę z cennika salonu beauty na kartę: paczka (wizyty, osoby) i lista składników "
                "(zabieg, metoda, obszar, etap, dlugosc, krotnosc, dodatki) nazwami ze słownika; wybór klienta → warianty. "
                "Odpowiedz jednym obiektem JSON.")
SYSTEM_BMATCH = ("Porównujesz dwie usługi z cenników salonów beauty (Booksy), każdą z pełnymi danymi i kartą. "
                 "Odpowiedz jedną cyfrą: 0 = inna usługa, 1 = odmiana tej samej usługi (inny zakres, obszar, etap, "
                 "liczba zabiegów, dodatek, wariant), 2 = ta sama usługa (różni się tylko pisownią, marketingiem albo czasem).")


def klucz_uslugi(u: dict[str, Any]) -> str:
    """Ten sam opis usługi = ta sama karta. u = wiersz salon_scrape_services (name, category_name, description, variants)."""
    raw = json.dumps([u["name"], u.get("category_name"), u.get("description"),
                      [v.get("label") for v in (u.get("variants") or [])]], ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _pola(u: dict[str, Any], typ_salonu: str) -> dict[str, Any]:
    w: dict[str, Any] = {"nazwa": u["name"], "kategoria": u.get("category_name") or "", "typ_salonu": typ_salonu or "",
                         "usluga_booksy": u.get("treatment_name") or ""}
    opis = " ".join((u.get("description") or "").split())
    if opis:
        w["opis"] = opis
    if u.get("duration_minutes"):
        w["czas_min"] = u["duration_minutes"]
    warianty = [{"nazwa": v.get("label") or "", "czas_min": v.get("duration")} for v in (u.get("variants") or [])]
    if len(warianty) > 1 or any(v["nazwa"] for v in warianty):
        w["warianty"] = warianty
    sklad = [c.get("name") for v in (u.get("variants") or []) for c in (v.get("combo_children") or []) if c.get("name")]
    if u.get("is_package") or sklad:
        w["pakiet"] = True
    if sklad:
        w["sklad_pakietu"] = sklad
    return w


def wiadomosci_bcard(u: dict[str, Any], typ_salonu: str = "") -> list[dict[str, str]]:
    """Wejście b-card (jak b-card kontrakt.wejscie bez klucza)."""
    return [{"role": "system", "content": SYSTEM_BCARD},
            {"role": "user", "content": json.dumps(_pola(u, typ_salonu), ensure_ascii=False)}]


def strona_bmatch(u: dict[str, Any], karta: dict[str, Any]) -> dict[str, Any]:
    """Jedna strona pary b-match: karta (bez pustych pól) + pełne dane usługi. Typ salonu pusty — tak było przy nauce."""
    return {"karta": {k: v for k, v in (karta or {}).items() if v not in (None, [], "")}, **_pola(u, "")}


def wiadomosci_bmatch(a: dict[str, Any], b: dict[str, Any]) -> list[dict[str, str]]:
    return [{"role": "system", "content": SYSTEM_BMATCH},
            {"role": "user", "content": json.dumps({"A": a, "B": b}, ensure_ascii=False)}]


def para_klucz(k1: str, k2: str) -> str:
    return "|".join(sorted((k1, k2)))
