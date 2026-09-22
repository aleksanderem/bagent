"""Weto na profilach liczbowych z badania (pytania.py) — cała polityka w kodzie.

Odpowiednik słownego layer_identity.vote_taxonomy_axis dla profili TypeSafe.
Zasada jak dotąd: weto tylko przy SPRZECZNOŚCI stwierdzonej po obu stronach;
brak danych lub niepewność = brak weta (abstain), nigdy „za”.

Progi z dokumentacji TypeSafe, NIE strojone na holdoucie:
* TAK = 0,8 / NIE = 0,2 — przykład progów YES/NO z docs primitives/noul;
  wartości pomiędzy to strefa szarości → brak weta.
* Skala długości: sprzeczność przy różnicy ≥ 1,5 poziomu — czyli dalej niż
  sąsiednie poziomy (krótkie/średnie salony rozgraniczają różnie).
* Skale zakresu i objętości: sprzeczność przy różnicy ≥ 1 pełnego poziomu.
  Porównanie zaokrąglonych poziomów przeskakiwało granicę 0,5 w obie strony
  (9 fałszywych cięć tożsamych paznokci w v1).
* Metoda: obie strony pewne ≥ 0,8 (przykład progu działania z docs
  how-to-build „route on uncertainty”) i różne techniki, żadna „inna”.

Moduł bez zależności od SDK — importuje go silnik wyceny.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

TAK = 0.8
NIE = 0.2
DLUGOSC_ROZNICA = 1.5
SKALA_ROZNICA = 1.0
METODA_PEWNOSC = 0.8

Profil = dict[str, Any]

# Pary wzajemnie wykluczające się (usługa nie może być jednocześnie jednym i drugim).
ODBIORCY_WYKLUCZAJACY = [
    ("odb:kobiety", "odb:mezczyzni"),
    ("odb:dzieci", "odb:kobiety"),
    ("odb:dzieci", "odb:mezczyzni"),
    ("odb:zwierze", "odb:kobiety"),
    ("odb:zwierze", "odb:mezczyzni"),
    ("odb:zwierze", "odb:dzieci"),
]
ETAPY_WYKLUCZAJACE = [
    ("etap:zalozenie", "etap:uzupelnienie"),
    ("etap:zalozenie", "etap:zdjecie"),
    ("etap:uzupelnienie", "etap:zdjecie"),
    ("etap:konsultacja", "etap:zalozenie"),
    ("etap:konsultacja", "etap:uzupelnienie"),
    ("etap:konsultacja", "etap:zdjecie"),
    ("etap:konsultacja", "etap:korekta"),
]


def _exclusive(a: Profil, b: Profil, pairs: list[tuple[str, str]]) -> bool:
    """Sprzeczne, gdy jedna strona pewnie ma X i pewnie nie ma Y, a druga odwrotnie."""
    for x, y in pairs:
        for p, q in ((a, b), (b, a)):
            if p.get(x, 0) >= TAK and q.get(y, 0) >= TAK and p.get(y, 1) <= NIE and q.get(x, 1) <= NIE:
                return True
    return False


def odbiorca(a: Profil, b: Profil) -> bool:
    return _exclusive(a, b, ODBIORCY_WYKLUCZAJACY)


def etap(a: Profil, b: Profil) -> bool:
    return _exclusive(a, b, ETAPY_WYKLUCZAJACE)


def _scale(a: Profil, b: Profil, present: str, scale: str, gap: float) -> bool:
    """Obie strony pewnie podają wartość i różnią się o co najmniej `gap` poziomów."""
    if a.get(present, 0) < TAK or b.get(present, 0) < TAK:
        return False
    sa, sb = a.get(scale), b.get(scale)
    if sa is None or sb is None:
        return False
    return abs(sa - sb) >= gap


def dlugosc(a: Profil, b: Profil) -> bool:
    return _scale(a, b, "dl:obecna", "dl:skala", DLUGOSC_ROZNICA)


def zakres(a: Profil, b: Profil) -> bool:
    return _scale(a, b, "zakres:obecny", "zakres:skala", SKALA_ROZNICA)


def objetosc(a: Profil, b: Profil) -> bool:
    return _scale(a, b, "obj:obecna", "obj:skala", SKALA_ROZNICA)


def obszar(a: Profil, b: Profil) -> bool:
    """Rozłączne okolice: obie strony pewnie gdzieś, żadna pewnie tam, gdzie druga."""
    keys = {k for k in (*a, *b) if k.startswith("ob:")}
    sa = {k for k in keys if a.get(k, 0) >= TAK}
    sb = {k for k in keys if b.get(k, 0) >= TAK}
    if not sa or not sb or sa & sb:
        return False
    return all(b.get(k, 1) <= NIE for k in sa) and all(a.get(k, 1) <= NIE for k in sb)


def pakiet(a: Profil, b: Profil) -> bool:
    return (a.get("pakiet", 0) >= TAK and b.get("pakiet", 1) <= NIE) or (
        b.get("pakiet", 0) >= TAK and a.get("pakiet", 1) <= NIE
    )


def metoda(a: Profil, b: Profil) -> bool:
    ma, mb = a.get("metoda"), b.get("metoda")
    if not ma or not mb or "inna" in (ma, mb) or ma == mb:
        return False
    return a.get("metoda#pewnosc", 0) >= METODA_PEWNOSC and b.get("metoda#pewnosc", 0) >= METODA_PEWNOSC


REGULY: dict[str, Callable[[Profil, Profil], bool]] = {
    "odbiorca": odbiorca,
    "etap": etap,
    "dlugosc": dlugosc,
    "obszar": obszar,
    "metoda": metoda,
    "zakres": zakres,
    "objetosc": objetosc,
    "pakiet": pakiet,
}

# Oś silnika (layer_identity.TAXONOMY_VETO_AXES + "metoda") → cechy profilu.
# Słowna oś „rozmiar” zbierała każdą ilość w jednym napisie; badanie mierzy ją
# trzema osobnymi pomiarami, więc konflikt któregokolwiek = konflikt osi.
# Tak samo liczył test w cieniu na prawdziwym silniku (22.09.2026).
OSIE_SILNIKA: dict[str, tuple[str, ...]] = {
    "obszar": ("obszar",),
    "odbiorca": ("odbiorca",),
    "etap": ("etap",),
    "dlugosc": ("dlugosc",),
    "rozmiar": ("objetosc", "zakres", "pakiet"),
    "metoda": ("metoda",),
}


def axis_conflict(a: Profil, b: Profil, axis: str) -> bool:
    """Czy profile są sprzeczne na osi silnika (oś nieznana = brak konfliktu)."""
    return any(REGULY[c](a, b) for c in OSIE_SILNIKA.get(axis, ()))


def veto(a: Profil | None, b: Profil | None, cechy: tuple[str, ...]) -> list[str]:
    """Lista cech, na których profile są sprzeczne (narzędzia pomiarowe)."""
    if not a or not b:
        return []
    return [c for c in cechy if REGULY[c](a, b)]
