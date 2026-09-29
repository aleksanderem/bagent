"""Katalog usług — słownik rdzeni słów (bez sieci)."""
from __future__ import annotations

from collections import Counter

from services.katalog_uslug.slownik import TO_SAMO, INNE, kandydaci, negatywy, odleglosc, pytanie_relacji, zbuduj


def test_odleglosc_edycyjna() -> None:
    assert odleglosc("botoks", "botox") == 2 and odleglosc("hybryd", "hybrydow") == 2 and odleglosc("łydk", "łydk") == 0


def test_kandydaci_to_podobna_pisownia_nie_liczby() -> None:
    c = Counter({"hybryd": 5, "hybrydow": 9, "hybrydz": 1, "manicur": 20, "123": 3, "łydk": 4, "łyżeczk": 1})
    k = kandydaci(c)
    assert ("hybryd", "hybrydow") in k and ("hybryd", "hybrydz") in k
    assert all("123" not in p for p in k) and ("łydk", "łyżeczk") not in k


def test_negatyw_gdy_salon_sprzedaje_oba_slowa_osobno() -> None:
    oferty = [("s1", frozenset({"depilacj", "łydk"})), ("s1", frozenset({"depilacj", "ud"})),
              ("s2", frozenset({"depilacj", "łydk"}))]
    assert negatywy(oferty) == {frozenset({"łydk", "ud"})}


def test_zbuduj_scala_tylko_to_samo_do_czestszej_formy() -> None:
    c = Counter({"hybrydow": 9, "hybryd": 5, "hybrydz": 1, "łydk": 4, "ud": 4})
    s = zbuduj(c, {("hybryd", "hybrydow"): TO_SAMO, ("hybrydz", "hybryd"): TO_SAMO, ("łydk", "ud"): INNE})
    assert s == {"hybryd": "hybrydow", "hybrydz": "hybrydow"}


def test_pytanie_ma_cztery_stany() -> None:
    assert set(pytanie_relacji("hybryd", "hybrydow").criteria) == {"to_samo", "wezsze", "szersze", "inne"}
