"""Schemat v10 — poprawki porównania z rozbioru podologii i masażu (bd BEAUTY_AUDIT-asrk, 27–28.09).

Pytania destylacji zostają jak w v9 — v10 zmienia tylko to, jak kod porównuje dwie
destylacje. Każda poprawka zmierzona osobno (28.09, 18 salonów roboczych, 7733 par,
sędzia par v3 jako miernik; +lepiej/−gorzej względem v9):

  1. dodatek po jednej stronie liczy się od „raczej tak” (≥ 0,5), +88/−9. Próg 0,8 był
     progiem pewnego działania, a tu działanie jest ostrożne: zdejmuje „ta sama”, a wiersz
     ceny spada do cen podobnych usług (decyzja A + C). Fałszywa „ta sama” psuje cenę,
     fałszywa „powiązana” tylko ją odkłada;
  2. liczby sztuk z nazwy — palce, paznokcie, sztuki, osoby, zęby, zmiany, +13/−0.
     Liczba 1 znaczy to samo co brak liczby: nazwa bez liczby opisuje jedną sztukę;
     tak samo liczba osób — brak = jedna osoba;
  3. poziom zabiegu z nazwy (kod, nie model): słowa zakresu — zaawansowany, rozszerzony,
     kompleksowy / mini, express / podstawowy. Brak słowa = wersja podstawowa. Słowa
     marketingowe (premium, lux, VIP) nie są poziomem — sędzia traktuje je jak „dodatki
     marketingowe”, które nie zmieniają usługi. Pytanie modelu o poziom odrzucone: zwykły
     „Manicure hybrydowy” dostawał „inna”, a czułość w paznokciach spadała o 15 pkt;
  4. reguła czasu (≥ 2× = powiązane) — przełącznik. Decyzja Alexa 28.09: ten sam zabieg
     o innym czasie to ta sama usługa. Pomiar: bez reguły 23 pary lepiej, 55 gorzej —
     czas łapie różnice, których taksonomia nie widzi (szkolenie 8 h vs zabieg 45 min,
     masaż strefy vs całego ciała, podologia bez własnej dziedziny).

Odrzucone w pomiarze (zapis, żeby nie wracać bez nowego sygnału): usunięcie „celu”
(+45/−205 — cel odróżnia „ze wzmocnieniem”, „antycellulitowy”, schorzenie w podologii)
i kontrastowe opisy „nie podano”/„inna” (niepewnych odpowiedzi 1475 → 2374, depilacja
czułość 63% → 28%).
"""

from __future__ import annotations

import copy
import re
from typing import Any

from services.typesafe_profile.weto import NIE

from .podzial import _nazwa, jako_schemat, porownaj_v8
from .schemat import liczby

WERSJA_SCHEMATU = 10
TAK_DODATEK = 0.5  # punkt neutralny tak/nie (docs TypeSafe) — patrz pkt 1 wyżej
JEDNA_OSOBA = "jedna osoba"
POZIOM = "poziom"
PODSTAWOWY = "podstawowy"

_POZIOMY = (  # kolejność = pierwszeństwo, gdy nazwa ma dwa słowa zakresu
    ("rozszerzony", re.compile(r"\b(rozszerz\w*|komplek\w*|zaawans\w*)", re.I)),
    ("skrócony", re.compile(r"\b(mini|express|ekspres\w*|skrócon\w*)\b", re.I)),
    (PODSTAWOWY, re.compile(r"\b(podstaw\w*|basic|standard\w*)", re.I)),
)

_SZTUKI = re.compile(
    r"(\d+)\s*(pal(?:ec|ce|ca|ców|cy)\b|paznok\w*|sztuk\w*|szt\b|osob\w*|osób|os\b|zęb\w*|ząb|zmian\w*|brodaw\w*|kurzaj\w*)",
    re.I,
)
_JEDNOSTKI = (("pal", "palec"), ("paznok", "paznokieć"), ("szt", "szt"), ("os", "osoba"), ("zęb", "ząb"),
              ("ząb", "ząb"), ("zmian", "zmiana"), ("brodaw", "brodawka"), ("kurzaj", "brodawka"))


def _jednostka(s: str) -> str:
    s = s.lower()
    return next(k for p, k in _JEDNOSTKI if s.startswith(p))


def liczby_v10(nazwa: str | None, opis: str | None) -> dict[str, Any]:
    """liczby() + sztuki z nazwy; sztuka pojedyncza (1) to brak liczby."""
    base = liczby(nazwa, opis)
    il = set(base["ilosc"] or [])
    il |= {f"{int(n)} {_jednostka(j)}" for n, j in _SZTUKI.findall(nazwa or "") if int(n) != 1}
    return {**base, "ilosc": sorted(il) or None}


def poziom_z_nazwy(nazwa: str | None) -> str:
    """Zakres usługi nazwany wprost w nazwie; brak słowa = wersja podstawowa."""
    return next((p for p, rx in _POZIOMY if rx.search(nazwa or "")), PODSTAWOWY)


def schemat_v10(podzial: dict) -> dict:
    """Schemat v9 → v10: liczba osób ma wartość domyślną „jedna osoba” (brak = jedna)."""
    p = copy.deepcopy(podzial)
    for r, c in p["cechy"].items():
        if "liczba_osob" in c:
            wart = [JEDNA_OSOBA] + [v for v in c["liczba_osob"].get("wartosci", []) if v != JEDNA_OSOBA]
            c["liczba_osob"] = {**c["liczba_osob"], "wartosci": wart, "domyslna": JEDNA_OSOBA}
    p["wersja_schematu"] = WERSJA_SCHEMATU
    return p


def _jednostronne(a: dict, b: dict, klucz: str) -> bool:
    pa, pb = a.get(klucz), b.get(klucz)
    return pa is not None and pb is not None and ((pa >= TAK_DODATEK and pb <= NIE) or (pb >= TAK_DODATEK and pa <= NIE))


def _bez_czasu(f: dict | None) -> dict | None:
    return None if f is None else {k: v for k, v in f.items() if k != "duration_minutes"}


def _z_liczbami(rek: dict | None, f: dict | None) -> dict | None:
    if not rek or not f or not f.get("nazwa"):
        return rek
    lz = liczby_v10(f["nazwa"], None)
    return {**rek, "ilosc": lz["ilosc"], "liczba_zabiegow": lz["liczba_zabiegow"]}


def porownaj_v10(a: dict | None, b: dict | None, podzial: dict, fa: dict | None = None, fb: dict | None = None,
                 _sch: dict | None = None, regula_czasu: bool = True) -> tuple[str, str]:
    """porownaj_v8 + dodatek od „raczej tak”, liczby sztuk i poziom zabiegu z nazwy."""
    sch = _sch or jako_schemat(podzial)
    if a and b and "inny" not in (a["rodzaj"], b["rodzaj"]):
        ka = podzial["korzen"].get(a["rodzaj"], a["rodzaj"])
        kb = podzial["korzen"].get(b["rodzaj"], b["rodzaj"])
        if fa and fb and ka == kb and _nazwa(fa.get("nazwa")) and _nazwa(fa.get("nazwa")) == _nazwa(fb.get("nazwa")):
            return "tozsame", "identyczna nazwa"
        for klucz, powod in (("zestaw", "zestaw"), ("rozszerzenie", "dodatek")):
            if _jednostronne(a, b, klucz):
                return "powiazane", powod
    ca, cb = (fa, fb) if regula_czasu else (_bez_czasu(fa), _bez_czasu(fb))
    w = porownaj_v8(_z_liczbami(a, fa), _z_liczbami(b, fb), podzial, ca, cb, _sch=sch)
    if w[0] == "tozsame" and fa and fb and poziom_z_nazwy(fa.get("nazwa")) != poziom_z_nazwy(fb.get("nazwa")):
        return "powiazane", POZIOM
    return w


__all__ = ["WERSJA_SCHEMATU", "JEDNA_OSOBA", "POZIOM", "PODSTAWOWY", "TAK_DODATEK",
           "liczby_v10", "poziom_z_nazwy", "schemat_v10", "porownaj_v10"]
