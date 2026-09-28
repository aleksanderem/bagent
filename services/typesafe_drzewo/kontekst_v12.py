"""Pełny kontekst usługi z Booksy i pytania poziomów 0–1 drzewa v12 (bd BEAUTY_AUDIT-asrk, 28.09).

Model tej samej usługi (Alex, 28.09): informacja o usłudze jest w pięciu miejscach — kategoria
w cenniku (często sam zabieg: „Strzyżenie”), nazwa (często sam wariant: „Damskie włosy długie”,
„Pachy”), opis (skład), warianty Booksy (obszar albo czas) oraz zabieg wybrany w Booksy i profil
salonu (metoda: MiLLASER → „Depilacja laserowa”). TypeSafe dostaje wszystkie pięć.

Poziom 0 (pozycja) odsiewa to, czego nie porównujemy: produkty, dodatki rezerwowane obok głównej
usługi, informacje. Poziom 1 to dziedziny z grupy_uslug.GRUPY (podział według tego, co się robi).
Oba pytania dotyczą tego samego stanu, więc idą w jednym wywołaniu (niezależne pytania naraz).
"""

from __future__ import annotations

from typing import Any

from typesafe_sdk import Choice

from .grupy_uslug import GRUPY

OPIS_MAX = 600
WARIANTOW_MAX = 8

POZYCJE: dict[str, dict] = {
    "zabieg": {"what": "usługa wykonywana klientowi — sama albo jako zestaw kilku zabiegów",
               "not_for": "dopłata do innej usługi, produkt, sama konsultacja",
               "examples": ["Manicure hybrydowy", "Strzyżenie damskie włosy długie", "Pachy (depilacja laserowa)", "Henna komplet"]},
    "pakiet": {"what": "karnet albo seria kilku wizyt tego samego zabiegu",
               "not_for": "pojedyncza wizyta, zestaw różnych zabiegów w jednej wizycie",
               "examples": ["Pakiet 5 zabiegów", "Karnet 10 wejść", "Seria 4 zabiegów"]},
    "dodatek": {"what": "dopłata albo dodatek rezerwowany razem z inną usługą, niewykonywany osobno",
                "not_for": "samodzielny zabieg",
                "examples": ["Zdjęcie hybrydy", "Opatrunek — dodatek do pedicure", "Ampułka do zabiegu", "Zdobienie jednego paznokcia"]},
    "konsultacja": {"what": "konsultacja albo diagnoza bez zabiegu",
                    "not_for": "zabieg, w którego cenie jest konsultacja",
                    "examples": ["Konsultacja kosmetologiczna", "Konsultacja podologiczna", "Diagnoza skóry"]},
    "produkt": {"what": "produkt na sprzedaż",
                "not_for": "zabieg z użyciem produktu",
                "examples": ["Maść z mocznikiem", "Spray przeciwgrzybiczy", "Szampon"]},
    "inne": {"what": "voucher, ogłoszenie, promocja albo opis bez konkretnej usługi",
             "not_for": "usługa z ceną za konkretny zabieg",
             "examples": ["Karta podarunkowa", "Promocja grudnia", "Informacja o salonie"]},
}


def stan_v12(u: dict) -> dict[str, Any]:
    """u: wiersz z v12_eksport (nazwa, kategoria, opis, warianty, zabieg_booksy, salon, typ_salonu)."""
    usl: dict[str, Any] = {"nazwa": u.get("nazwa") or ""}
    if u.get("kategoria"):
        usl["kategoria_w_cenniku"] = u["kategoria"]
    if u.get("opis"):
        usl["opis"] = u["opis"][:OPIS_MAX]
    etyk = [w["label"] for w in u.get("warianty") or [] if w.get("label") and w["label"].strip().lower() != usl["nazwa"].strip().lower()]
    if etyk:
        usl["warianty"] = etyk[:WARIANTOW_MAX]
    if u.get("zabieg_booksy"):
        usl["zabieg_wybrany_w_booksy"] = u["zabieg_booksy"]
    salon = {"nazwa": u.get("salon") or "", "typ": u.get("typ_salonu") or ""}
    return {"usluga": usl, "salon": salon}


def pytania_0_1() -> dict[str, Choice]:
    return {
        "pozycja": Choice(
            instructions="Czym jest pozycja `usluga` w cenniku salonu `salon`? Rozstrzygają nazwa, kategoria, opis i warianty.",
            criteria=POZYCJE,
        ),
        "dziedzina": Choice(
            instructions="Do jakiej dziedziny należy usługa `usluga`? Rozstrzyga to, co się robi — nazwa, kategoria, opis, "
                         "warianty i zabieg wybrany w Booksy; typ salonu `salon` to tylko kontekst.",
            criteria={g: {"what": d["what"], "not_for": d["not_for"], "examples": d["examples"]} for g, d in GRUPY.items()},
        ),
    }


__all__ = ["POZYCJE", "stan_v12", "pytania_0_1"]
