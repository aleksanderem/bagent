"""Katalog usług — klucz tekstu oferty (plan zatwierdzony 29.09): proste czyszczenie szumu bez zmiany znaczenia.

Zostaje: litery (z polskimi znakami), cyfry, „+” (komplet), „/” (alternatywa) i „:” w proporcjach (2:1).
Wycinane: emotki i symbole, interpunkcja, czas trwania („60 min”, „1,5 h”) — czas nie rozróżnia usług (Alex 28.09).
"""
from __future__ import annotations

import re
import unicodedata

_CZAS = re.compile(r"(?<!\w)\d+(?:[.,]\d+)?\s*(?:min(?:ut[ay]?)?|h|godz(?:in[ya]?)?)(?!\w)", re.IGNORECASE)
_PROPORCJA = re.compile(r"(\d)\s*:\s*(\d)")
_ZNACZACE = "+/"
_DWUKROPEK = "\u0000"  # chroni „:” proporcji przed zamianą interpunkcji na spację


# Lekkie ujednolicenie odmiany (twarz/twarzy, pudrowa/pudrową, włosy/włosów, średnie/średnich) na słowie bez
# polskich znaków — salony piszą też „rzesy”, „wlosow” (pomiar 29.09: 12 par T różniło się tylko „rzes”/„rzęs”).
# Nie łapie wymian samogłosek (stopa/stóp) ani zdrobnień (wąs/wąsik) — te scala słownik decyzją TypeSafe.
# Najdłuższa końcówka pierwsza, najwyżej dwie końcówki; rdzeń zostaje ≥ 4 znaki.
_BEZ_ZNAKOW = str.maketrans("ąćęłńóśźż", "acelnoszz")
_KONCOWKI = ("owie", "ami", "ach", "ego", "emu", "ymi", "imi", "ych", "ich", "ow", "om", "ej", "em", "ym", "im",
             "a", "e", "y", "i", "u", "o")


# Telefony i e-maile z opisów Booksy („tel. 600 100 200”) — nie należą do tekstu oferty ani do danych w repo (30.09).
_TELEFON = re.compile(r"(?<![\d#])(?:\+?48[ .-]?)?\d{3}[ .-]?\d{3}[ .-]?\d{3}(?![\d#])")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def bez_kontaktow(tekst: str) -> str:
    return _TELEFON.sub("[telefon]", _EMAIL.sub("[e-mail]", tekst))


def bez_polskich_znakow(slowo: str) -> str:
    return slowo.translate(_BEZ_ZNAKOW)


def rdzen_slowa(slowo: str) -> str:
    s = bez_polskich_znakow(slowo)
    for _krok in range(2):
        if len(s) <= 4 or not s.isalpha():
            break
        nowy = next((s[: -len(k)] for k in _KONCOWKI if s.endswith(k) and len(s) - len(k) >= 4), s)
        if nowy == s:
            break
        s = nowy
    return s


def normalizuj(tekst: str | None) -> str:
    """Tekst oferty → klucz: małe litery, bez szumu, pojedyncze spacje."""
    if not tekst:
        return ""
    t = _PROPORCJA.sub(rf"\1{_DWUKROPEK}\2", _CZAS.sub(" ", unicodedata.normalize("NFC", tekst).lower()))
    znaki = [f" {c} " if c in _ZNACZACE else ":" if c == _DWUKROPEK else c if c.isalnum() else " " for c in t]
    return " ".join("".join(znaki).split())
