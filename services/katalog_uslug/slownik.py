"""Katalog usług — słownik rdzeni słów z danych (plan 29.09): kandydaci na „to samo słowo” i relacja 4-stanowa.

Kandydaci: rdzenie tej samej długości rdzenia pisowni (wspólny początek ≥ 4 znaki i odległość edycyjna ≤ 2 —
odmiana, literówka, „botox” / „botoks”). Twardy negatyw bez pytania: ten sam salon sprzedaje oba słowa w różnych
ofertach tego samego zabiegu (łydki i uda) → to nie synonimy. O reszcie rozstrzyga TypeSafe (Choice 4-stanowy:
to samo / węższe / szersze / inne) — scala tylko „to samo”; forma kanoniczna = częstsza.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable

from typesafe_sdk import Choice

MIN_WSPOLNY_POCZATEK = 4
MAX_ODLEGLOSC = 2
TO_SAMO, WEZSZE, SZERSZE, INNE = "to_samo", "wezsze", "szersze", "inne"


def odleglosc(a: str, b: str) -> int:
    """Odległość edycyjna Levenshteina (krótkie słowa — koszt pomijalny)."""
    if len(a) < len(b):
        a, b = b, a
    poprzedni = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        biezacy = [i]
        for j, cb in enumerate(b, 1):
            biezacy.append(min(poprzedni[j] + 1, biezacy[j - 1] + 1, poprzedni[j - 1] + (ca != cb)))
        poprzedni = biezacy
    return poprzedni[-1]


def kandydaci(czestosc: Counter[str]) -> list[tuple[str, str]]:
    """Pary rdzeni podobnych w pisowni; słowa z cyframi pomijane (liczby rozstrzyga kod)."""
    slowa = sorted(w for w in czestosc if w.isalpha() and len(w) >= MIN_WSPOLNY_POCZATEK)
    po_poczatku: dict[str, list[str]] = defaultdict(list)
    for w in slowa:
        po_poczatku[w[:MIN_WSPOLNY_POCZATEK]].append(w)
    pary = []
    for grupa in po_poczatku.values():
        for i, a in enumerate(grupa):
            pary += [(a, b) for b in grupa[i + 1:] if odleglosc(a, b) <= MAX_ODLEGLOSC]
    return pary


def negatywy(oferty_salonu: Iterable[tuple[str, frozenset[str], float | None]]) -> set[frozenset[str]]:
    """(salon, zbiór słów oferty, cena) → pary słów sprzedawane przez ten sam salon w RÓŻNYCH ofertach w RÓŻNYCH cenach,
    które różnią się dokładnie tym jednym słowem (łydki / uda przy tej samej reszcie) — takie słowa nie są synonimami.
    Ta sama cena to duplikat wpisu, nie dwie usługi (30.09: „Przedłużanie Paznokci” i „Przedłużenie Paznokci” po 190 zł
    w jednym salonie blokowały scalenie form słowa) — plan 29.09, cecha opisowa: „żaden salon nie sprzedaje obu wersji
    w różnych cenach”."""
    po_salonie: dict[str, list[tuple[frozenset[str], float | None]]] = defaultdict(list)
    for salon, zbior, cena in oferty_salonu:
        po_salonie[salon].append((zbior, cena))
    wynik: set[frozenset[str]] = set()
    for oferty in po_salonie.values():
        for i, (a, ca) in enumerate(oferty):
            for b, cb in oferty[i + 1:]:
                ra, rb = a - b, b - a
                if len(ra) == 1 and len(rb) == 1 and a & b and not (ca and cb and ca == cb):
                    wynik.add(frozenset(ra | rb))
    return wynik


def pytanie_relacji(a: str, b: str) -> Choice:
    return Choice(
        instructions=(f"W nazwach usług salonów beauty pojawiają się słowa „{a}” i „{b}” (przykłady ofert w `oferty_a` "
                      f"i `oferty_b`). Jak ma się znaczenie „{a}” do „{b}” w tych usługach?"),
        criteria={TO_SAMO: {"what": f"to samo słowo w innej formie, pisowni albo synonim — „{a}” i „{b}” znaczą to samo",
                            "not_for": "różne obszary, metody, rozmiary albo zabiegi"},
                  WEZSZE: {"what": f"„{a}” to część albo szczególny przypadek „{b}”", "not_for": "to samo znaczenie"},
                  SZERSZE: {"what": f"„{a}” obejmuje „{b}” i coś więcej", "not_for": "to samo znaczenie"},
                  INNE: {"what": "różne znaczenia", "not_for": "to samo słowo w innej formie"}})


def zbuduj(czestosc: Counter[str], relacje: dict[tuple[str, str], str]) -> dict[str, str]:
    """Rdzeń → rdzeń kanoniczny (częstszy) dla par „to samo”; łańcuchy scalane do jednego kanonu."""
    rodzic: dict[str, str] = {}

    def korzen(w: str) -> str:
        while rodzic.get(w, w) != w:
            w = rodzic[w]
        return w

    for (a, b), rel in relacje.items():
        if rel != TO_SAMO:
            continue
        ka, kb = korzen(a), korzen(b)
        if ka != kb:
            glowny, drugi = (ka, kb) if (czestosc[ka], kb) >= (czestosc[kb], ka) else (kb, ka)
            rodzic[drugi] = glowny
    return {w: korzen(w) for w in rodzic}


__all__ = ["INNE", "SZERSZE", "TO_SAMO", "WEZSZE", "kandydaci", "negatywy", "odleglosc", "pytanie_relacji", "zbuduj"]
