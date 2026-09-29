"""Katalog usług — podpis oferty i porównanie dwóch podpisów (hybryda z planu zatwierdzonego 29.09).

„Ta sama” porównuje ZBIÓR rdzeni słów oferty, nie przydział słów do ról: test 29.09 (120 ofert, GLM) — ten sam
rozkład w dwóch przebiegach 43%, ten sam przydział do poziomów modelu 58%, ten sam zbiór słów 82%; model
przerzuca słowo między rolami („laminacja” raz metoda, raz skład), ale bierze te same słowa.
Do zbioru wchodzą: słowa własne oferty (nazwa, wariant) bez szumu i wykonawcy; z kategorii / zabiegu Booksy
tylko to, czego nazwa nie mówi — zabieg i metoda, gdy nazwa nie ma żadnego, oraz „dla kogo”; z opisu tylko
wyłączenia i liczby. Role zostają do tego wyboru, do opisu różnicy i do pytań o klasy.
Różnica jednostronna (dopisek tylko po jednej stronie) jest rozstrzygana RAZ na klasę (wspólne słowa, poziom,
dopisek) — nieistotna tylko po decyzji „nie zmienia”; różnica po obu stronach = zawsze „podobna”.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from services.katalog_uslug.ekstrakcja import LACZNIKI
from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa

TA_SAMA, PODOBNA, INNA = "ta_sama", "podobna", "inna"
POZIOM_ROLI = {"zabieg": "rdzen", "metoda": "rdzen", "obszar": "gdzie_ile", "rozmiar": "gdzie_ile",
               "liczba": "gdzie_ile", "dla_kogo": "gdzie_ile", "etap": "etap", "sesje": "sesje", "sklad": "sklad",
               "wylaczenie": "wylaczenie", "poziom": "poziom", "miejsce": "gdzie_ile", "inne": "inne"}
OGOLNE = frozenset({"zabieg", "usług", "usluga"})
WLASNE = frozenset({"nazwa", "wariant"})
Z_OPISU = frozenset({"wylaczenie", "liczba"})
NIE_USLUGA = frozenset({"produkt", "voucher", "konsultacja", "dodatek"})
# „do roku” ≠ „po roku”, „do 3 tyg.” ≠ „od 3 tyg.” — te łączniki niosą znaczenie w zakresach (pomiar 29.09)
POMIJANE = LACZNIKI - {"do", "po", "od"}
# Nazwa złożona WYŁĄCZNIE z takich słów nic nie mówi o zawartości („Combo Premium” — w dwóch zbiorach ocenione
# jako podobne): bez jawnego składu nigdy „ta sama” (przegląd planu 29.09).
PUSTE_NAZWY = frozenset({"comb", "combo", "pakiet", "zestaw", "komplet", "seri", "premium", "basic", "lux", "luxury",
                         "gold", "vip", "standard", "express", "ekspres", "mini", "maxi", "classic"})
Klasa = tuple[str, str, str]  # (wspólne słowa, poziom dopisku, dopisek)
Zamiana = tuple[str, str, str]  # (wspólne słowa, słowa tylko po jednej stronie, słowa tylko po drugiej) — strony w porządku
# Najwyżej tyle słów po każdej stronie różnicy dwustronnej pytamy jako „inna nazwa tej samej rzeczy” — dłuższa różnica
# to inny zestaw cech, a pytanie o nią byłoby sędzią całej pary (wyłączonym 28.09).
MAKS_ZAMIANY = 2
# Kategoria wymieniająca kilka pozycji („Manicure / manicure hybrydowy”, „Brwi i rzęsy”) nie mówi, która dotyczy oferty.
LISTA = re.compile(r"[/|,;&+]|\s(?:i|oraz|lub|albo)\s", re.IGNORECASE)


@dataclass(frozen=True)
class Podpis:
    zbior: frozenset[str]
    poziomy: frozenset[tuple[str, str]]  # (poziom, rdzeń słowa) — do opisu różnicy i pytań o klasy
    pozycja: str = "zabieg"
    blokady: tuple[str, ...] = ()
    wlasne: frozenset[str] = frozenset()  # słowa samej oferty (nazwa, wariant) — do testu „nazwy bez treści”


@dataclass(frozen=True)
class Klasy:
    """Rozstrzygnięcia klas: dopisek opisowy albo wartość domyślna zabiegu — w obu przypadkach nie rozróżnia."""
    opisowe: Iterable[Klasa] = field(default_factory=frozenset)
    domyslne: Iterable[Klasa] = field(default_factory=frozenset)
    rownowazne: Iterable[Zamiana] = field(default_factory=frozenset)  # różne słowa, ta sama rzecz („głowy” / „włosów”)


def _slowa(fraza: str, slownik: dict[str, str]) -> list[str]:
    return [slownik.get(r, r) for t in normalizuj(fraza).split() if t not in POMIJANE
            if (r := rdzen_slowa(t)) not in OGOLNE]


def _frazy(rek: dict[str, Any]) -> list[tuple[str, str, str]]:
    z = rek.get("zabieg") or {}
    return [("zabieg", z.get("zrodlo") or "nazwa", z.get("fraza") or "")] + \
        [(c.get("rola") or "", c.get("zrodlo") or "nazwa", c.get("fraza") or "") for c in rek.get("cechy") or []]


def _z_kategorii(kontekst: dict[str, Any], wlasne: list[tuple[str, str]], slownik: dict[str, str]) -> list[tuple[str, str]]:
    """(rola, fraza) z rozkładu kategorii, które uzupełniają to, czego nazwa i wariant nie mówią — kategoria to nagłówek
    salonu nad WSZYSTKIMI jej ofertami. Rola spoza zabiegu i metody wchodzi, gdy nazwa nie podaje tej roli (obszar,
    poziom „PREMIUM”, dla kogo); miejsce zawsze; „inne” i wykonawca nigdy. Zabieg i metoda: gdy nazwa nie ma żadnego —
    wszystko; gdy ma — tylko z kategorii nazywającej JEDNĄ rzecz, i to tę samą co nazwa, dokładniej („Depilacja pach”
    w „Depilacja laserowa”) albo samą metodę („LASER”). Kategoria-lista („Manicure / manicure hybrydowy”) nie mówi,
    która pozycja dotyczy oferty (pomiar 29.09: model wpisuje metodę kategorii do frazy zabiegu, np. „Depilacja
    laserowa” bez osobnej metody — stąd porównanie słów, nie ról)."""
    role_wlasne = {r for r, _f in wlasne}
    rdzen_wlasny = {w for r, f in wlasne if POZIOM_ROLI[r] == "rdzen" for w in _slowa(f, slownik)}
    frazy = [(r, f) for r, _zr, f in _frazy(kontekst) if r in POZIOM_ROLI and r != "inne" and f]
    zabieg_kat = {w for r, f in frazy if r == "zabieg" for w in _slowa(f, slownik)}
    jedna_rzecz = not LISTA.search(f" {kontekst.get('nazwa') or ''} ")
    doprecyzowuje = jedna_rzecz and (not zabieg_kat or bool(zabieg_kat & rdzen_wlasny))
    return [(r, f) for r, f in frazy
            if (POZIOM_ROLI[r] == "rdzen" and (not rdzen_wlasny or doprecyzowuje))
            or (POZIOM_ROLI[r] != "rdzen" and (r == "miejsce" or r not in role_wlasne) and not LISTA.search(f" {f} "))]


def _wybrane_frazy(rek: dict[str, Any], kontekst: dict[str, Any] | None = None,
                   slownik: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """(poziom, fraza) wchodzące do podpisu — reguła doboru źródeł z nagłówka modułu.

    kontekst: rozkład samej KATEGORII cennika (raz na kategorię salonu, ten sam dla wszystkich jej ofert). Gdy jest,
    zastępuje frazy z kategorii wyciągnięte przy ofercie (niespójne między przebiegami) — zasady w `_z_kategorii` —
    a zabieg Booksy liczy się tylko, gdy kategoria nie mówi, co się robi (pomiar 29.09: „Uda + pośladki” w „Fale
    radiowe” dostało zabieg z etykiety Booksy „Liposukcja ultradźwiękowa”)."""
    frazy = [(r, zr, f) for r, zr, f in _frazy(rek) if r in POZIOM_ROLI and f]
    wlasne = [(r, f) for r, zr, f in frazy if zr in WLASNE]
    wynik = [(POZIOM_ROLI[r], f) for r, zr, f in frazy if zr in WLASNE or (zr == "opis" and r in Z_OPISU)]
    if kontekst is not None:
        z_kat = _z_kategorii(kontekst, wlasne, slownik or {})
        wynik += [(POZIOM_ROLI[r], f) for r, f in z_kat]
        if any(POZIOM_ROLI[r] == "rdzen" for r, _zr, f in _frazy(kontekst) if r in POZIOM_ROLI and f):
            return wynik
        frazy = [x for x in frazy if x[1] != "kategoria"]  # zostaje etykieta Booksy (własne i opis odpadną niżej)
    wlasne_rdzen = any(POZIOM_ROLI[r] == "rdzen" for r, _f in wlasne)
    wlasne_dla_kogo = any(r == "dla_kogo" for r, _f in wlasne)
    return wynik + [(POZIOM_ROLI[r], f) for r, zr, f in frazy if zr not in WLASNE and zr != "opis"
                    and ((POZIOM_ROLI[r] == "rdzen" and not wlasne_rdzen) or (r == "dla_kogo" and not wlasne_dla_kogo)
                         or r == "miejsce")]


def podpis(rek: dict[str, Any], slownik: dict[str, str] | None = None, kontekst: dict[str, Any] | None = None) -> Podpis:
    """Słowa nazwy i wariantu, których model nie przypisał (ani do cechy, ani do szumu), wchodzą do zbioru jako „inne”
    — pomiar 29.09: to głównie słowa oczywiste („włosy” w wariancie „Włosy długie”) i pominięte „combo” / „komplet”;
    więcej słów = mniej fałszywych „ta sama”, a blokada gubiła pary."""
    s = slownik or {}
    poziomy = {(poz, w) for poz, f in _wybrane_frazy(rek, kontekst) for w in _slowa(f, s)}
    dopisane = {("inne", w) for w in _slowa(" ".join(rek.get("nieprzypisane") or ()), s)}
    wlasne = {w for r, zr, f in _frazy(rek) if zr in WLASNE and r not in ("specjalista",) for w in _slowa(f, s)}
    poziomy |= dopisane
    return Podpis(frozenset(w for _p, w in poziomy), frozenset(poziomy), rek.get("pozycja") or "zabieg",
                  wlasne=frozenset(wlasne | {w for _p, w in dopisane}))


def _rdzen(p: Podpis) -> set[str]:
    return {w for poz, w in p.poziomy if poz == "rdzen"}


KOLEJNOSC = ("rdzen", "gdzie_ile", "etap", "sesje", "sklad", "wylaczenie", "poziom", "inne")


def klucz_roznicy(slowa: Iterable[str]) -> str:
    return " ".join(sorted(slowa))


def klasy_roznicy(a: Podpis, b: Podpis) -> list[tuple[Klasa, Podpis]] | None:
    """Różnica jednostronna rozbita na poziomy → [(klasa, strona z dopiskiem)]; None, gdy równe albo różnica po
    obu stronach. Każde słowo dopisku trafia do pierwszego poziomu, na którym stoi po stronie, która je ma."""
    if a.zbior == b.zbior or not (a.zbior <= b.zbior or b.zbior <= a.zbior):
        return None
    z, dopisek = (b, b.zbior - a.zbior) if a.zbior < b.zbior else (a, a.zbior - b.zbior)
    wspolne = klucz_roznicy(a.zbior & b.zbior)
    po_poziomie: dict[str, set[str]] = {}
    for w in dopisek:
        poz = next((q for q in KOLEJNOSC if (q, w) in z.poziomy), "inne")
        po_poziomie.setdefault(poz, set()).add(w)
    return [((wspolne, poz, klucz_roznicy(po_poziomie[poz])), z) for poz in KOLEJNOSC if poz in po_poziomie]


def zamiana_slow(a: Podpis, b: Podpis) -> Zamiana | None:
    """Różnica po obu stronach, która może być inną nazwą tej samej rzeczy („Strzyżenie głowy i brody” / „Strzyżenie
    włosów i brody”): wspólna reszta, po każdej stronie ≤ MAKS_ZAMIANY słów, żadne ze słowem składu (dodatek w nazwie
    = podobna). Rozstrzyga TypeSafe raz na klasę; bez rozstrzygnięcia para zostaje podobna."""
    da, db = a.zbior - b.zbior, b.zbior - a.zbior
    if not (da and db and a.zbior & b.zbior) or max(len(da), len(db)) > MAKS_ZAMIANY:
        return None
    if any(("sklad", w) in p.poziomy for p, d in ((a, da), (b, db)) for w in d):
        return None
    x, y = sorted((klucz_roznicy(da), klucz_roznicy(db)))
    return klucz_roznicy(a.zbior & b.zbior), x, y


def _przeszkoda(a: Podpis, b: Podpis) -> tuple[str, str] | None:
    """Powody, dla których para nigdy nie jest „ta sama” — przed porównaniem zbiorów."""
    if not (_rdzen(a) & _rdzen(b)) and not (a.zbior & b.zbior):
        return INNA, "inny zabieg"
    if a.blokady or b.blokady:
        return PODOBNA, "nieprzypisane słowa: " + ", ".join(dict.fromkeys(a.blokady + b.blokady))
    if a.pozycja != b.pozycja and {a.pozycja, b.pozycja} & NIE_USLUGA:
        return PODOBNA, f"pozycja: {a.pozycja} / {b.pozycja}"
    if any(p.wlasne and p.wlasne <= PUSTE_NAZWY for p in (a, b)):
        return PODOBNA, "nazwa bez treści (combo / pakiet / premium) bez składu"
    return None


def porownaj(a: Podpis, b: Podpis, klasy: Klasy) -> tuple[str, str]:
    """→ (werdykt, powód). Dopisek na poziomie składu = dodatek w nazwie = podobna (model tej samej usługi) —
    bez pytania; pozostałe poziomy dopisku muszą być WSZYSTKIE rozstrzygnięte jako nieistotne."""
    if (p := _przeszkoda(a, b)) is not None:
        return p
    if a.zbior == b.zbior:
        return TA_SAMA, "równe podpisy"
    kl = klasy_roznicy(a, b)
    if kl is None:
        if (z := zamiana_slow(a, b)) is not None and z in set(klasy.rownowazne):
            return TA_SAMA, f"inna nazwa tego samego: „{z[1]}” / „{z[2]}”"
        return PODOBNA, f"różne słowa: {klucz_roznicy(a.zbior - b.zbior)} / {klucz_roznicy(b.zbior - a.zbior)}"
    nieistotne = set(klasy.opisowe) | set(klasy.domyslne)
    for klasa, _z in kl:
        if klasa[1] == "sklad":
            return PODOBNA, f"dodatek w nazwie: „{klasa[2]}”"
        if klasa not in nieistotne:
            return PODOBNA, f"{klasa[1]}: „{klasa[2]}” tylko po jednej stronie"
    return TA_SAMA, "dopisek nie zmienia usługi: " + " | ".join(k[2] for k, _z in kl)


def zamiana_do_pytania(a: Podpis, b: Podpis) -> Zamiana | None:
    """Klasa zamiany słów, o którą warto zapytać TypeSafe: para bez przeszkód, różnica po obu stronach."""
    return None if _przeszkoda(a, b) is not None else zamiana_slow(a, b)


def roznica_do_pytania(a: Podpis, b: Podpis) -> list[tuple[Klasa, Podpis]]:
    """Klasy, o które warto zapytać TypeSafe: para bez przeszkód, dopisek jednostronny bez składu."""
    if _przeszkoda(a, b) is not None:
        return []
    kl = klasy_roznicy(a, b) or []
    return [] if any(k[1] == "sklad" for k, _z in kl) else kl
