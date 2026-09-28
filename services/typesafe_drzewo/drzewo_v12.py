"""Drzewo v12 — przejście i porównanie według modelu tej samej usługi (bd BEAUTY_AUDIT-asrk, 28.09).

Model (Alex, 28.09): dziedzina → zabieg → metoda → gdzie i ile → etap, od ogółu do szczegółu.
Wynikiem porównania jest poziom pokrycia ścieżek: im wyżej ścieżki się rozchodzą, tym mniej
usługi są podobne. Sędzia par nie bierze udziału.

Przejście jak w cookbooku TypeSafe hierarchical_classification (zasada niezmienna):
  * poziom 0–1 (pozycja, dziedzina) — kontekst_v12.pytania_0_1, gotowy rozkład z budowy albo jedno wywołanie;
  * poziom 2 (zabieg) — wybór między zabiegami dziedziny + „ogolnie”, dla 3 najlepszych dziedzin
    naraz; w tym samym wywołaniu tak/nie o zestaw i dodatek w nazwie;
  * poziomy 3–5 zależą tylko od zabiegu, więc pytania o każdy zabieg z wiązki idą w jednym wywołaniu
    (niezależne pytania o ten sam stan). Poziom „gdzie i ile” to cztery osobne pytania — obszar,
    wielkość, dla kogo, cel — bo usługa bywa naraz „męska” i „długa”: jeden wybór między nimi nie
    byłby podziałem (v12b, 28.09);
  * wiązka 3 par dziedzina–zabieg; prawdopodobieństwo tej samej usługi = suma po wspólnych zabiegach
    iloczynu krawędzi i zgodności każdego wymiaru (Σ p_a·p_b po opcjach) — liść = iloczyn krawędzi.
Wymiar bez opcji dla danego zabiegu to „—” (nie dotyczy); „ogolnie” = usługa tego nie mówi.
"""

from __future__ import annotations

import itertools
import math
from typing import Any

from typesafe_sdk import Choice

from .klasyfikacja import PYTANIA_WSPOLNE
from .kontekst_v12 import pytania_0_1
from .podzial import _nazwa
from .schemat import MODEL
from .schemat_v10 import _jednostronne

K = 3
MIN_KRAWEDZ = 0.05
MIN_ROZKLAD = 0.01
MAX_OPCJI = 240
OGOLNIE = "ogolnie"
NIE_DOTYCZY = "—"
FACETY = ("metoda", "obszar", "wielkosc", "dla_kogo", "cel", "etap")
# Ścieżka: (dziedzina, zabieg, metoda, obszar, wielkość, dla kogo, cel, etap); poziom modelu = wycinek ścieżki.
POZIOMY_MODELU = (("dziedzina", 0, 1), ("zabieg", 1, 2), ("metoda", 2, 3), ("gdzie i ile", 3, 7), ("etap", 7, 8))
NAZWY_WYMIAROW = ("dziedzina", "zabieg", "metoda", "obszar", "wielkość", "dla kogo", "cel", "etap")
POROWNYWANE = frozenset({"zabieg", "pakiet"})
EPS = 1e-9

PYTANIA_WYMIARU = {
    "metoda": "Jaką metodą, techniką, urządzeniem albo preparatem wykonuje się zabieg „{z}” w usłudze `usluga`?",
    "obszar": "Gdzie wykonuje się zabieg „{z}” w usłudze `usluga` — na jakiej części ciała albo na czym?",
    "wielkosc": "Ile obejmuje zabieg „{z}” w usłudze `usluga` — długość, rozmiar, gęstość, liczba, objętość?",
    "dla_kogo": "Dla kogo jest zabieg „{z}” w usłudze `usluga` — płeć, wiek, rodzaj klienta albo zwierzęcia?",
    "cel": "Na jaki problem albo po jaki efekt jest zabieg „{z}” w usłudze `usluga`?",
    "etap": "Którą wizytą w cyklu zabiegu „{z}” jest usługa `usluga`?",
}


def _opcje(wezly: dict, ogolnie: dict) -> dict:
    opcje = sorted(wezly.items(), key=lambda kv: -kv[1].get("n", 0))[:MAX_OPCJI]
    return {**{k: {"examples": d.get("examples", [])[:3], "inne_nazwy": [s for s in d.get("synonimy", []) if s != k][:5]}
               for k, d in opcje}, OGOLNIE: ogolnie}


def pytanie_zabiegu(drzewo: dict, g: str) -> Choice:
    return Choice(
        instructions=f"Jaki zabieg z dziedziny „{g}” wykonuje się w usłudze `usluga`? Rozstrzygają nazwa, kategoria, "
                     "opis, warianty i zabieg wybrany w Booksy.",
        criteria=_opcje(drzewo["zabiegi"].get(g, {}), {"what": f"usługa z dziedziny „{g}” bez wskazania, który to zabieg",
                                                       "not_for": "usługa wskazuje jeden z zabiegów z listy"}),
    )


def pytanie_poziomu(drzewo: dict, g: str, z: str, wymiar: str) -> Choice | None:
    wezly = drzewo["zabiegi"].get(g, {}).get(z, {}).get(wymiar) or {}
    if not wezly:
        return None
    return Choice(instructions=PYTANIA_WYMIARU[wymiar].format(z=z),
                  criteria=_opcje(wezly, {"what": "usługa tego nie określa", "not_for": "usługa wskazuje jedną z opcji"}))


def _top(prawd: dict[str, float]) -> list[tuple[str, float]]:
    naj = max(prawd, key=prawd.get)
    return [(e, p) for e, p in sorted(prawd.items(), key=lambda kv: -kv[1]) if p >= MIN_KRAWEDZ or e == naj]


def _sciezka(etykiety: tuple, prawd: list[float | None]) -> dict:
    decyzje = [p for p in prawd if p is not None]
    log_p = sum(math.log(max(p, EPS)) for p in decyzje)
    return {"sciezka": etykiety, "log_p": log_p, "wynik": math.exp(log_p / len(decyzje)) if decyzje else 1.0}


async def wiazka_v12(client: Any, st: dict, drzewo: dict, tok: list, p01: dict | None = None, k: int = K) -> dict:
    """→ {"pozycja", "zabiegi": [[dziedzina, zabieg, p krawędzi, {wymiar: rozkład | None}]],
    "sciezki": [[ścieżka 8 pozycji, p, wynik]], "zestaw", "rozszerzenie"}."""
    if p01 is None:
        r0 = await client.system_one(st, pytania_0_1(), model=MODEL)
        tok[0] += r0.usage.input_tokens or 0
        p01 = {kl: dict(r0.choices[kl].probabilities) for kl in ("pozycja", "dziedzina")}
    dziedziny = _top(p01["dziedzina"])[:k]
    q2: dict[str, Any] = dict(PYTANIA_WSPOLNE)
    q2.update({f"z|{g}": pytanie_zabiegu(drzewo, g) for g, _p in dziedziny if drzewo["zabiegi"].get(g)})
    r2 = await client.system_one(st, q2, model=MODEL)
    tok[0] += r2.usage.input_tokens or 0
    pary = []  # (dziedzina, zabieg, p_d, p_z)
    for g, pg in dziedziny:
        a = r2.choices.get(f"z|{g}")
        pary += [(g, z, pg, pz) for z, pz in _top(dict(a.probabilities))] if a else [(g, OGOLNIE, pg, None)]
    pary = sorted(pary, key=lambda x: -_sciezka((), [x[2], x[3]])["wynik"])[:k]
    q3: dict[str, Choice] = {}
    for g, z, _pg, _pz in pary:
        if z != OGOLNIE:
            for w in FACETY:
                q = pytanie_poziomu(drzewo, g, z, w)
                if q is not None:
                    q3[f"{w}|{g}|{z}"] = q
    r3 = None
    if q3:
        r3 = await client.system_one(st, q3, model=MODEL)
        tok[0] += r3.usage.input_tokens or 0
    zabiegi, kandydaci = [], []
    for g, z, pg, pz in pary:
        rozk: dict[str, dict | None] = {}
        for w in FACETY:
            a = r3.choices.get(f"{w}|{g}|{z}") if r3 else None
            rozk[w] = {o: round(p, 4) for o, p in a.probabilities.items() if p >= MIN_ROZKLAD} if a else None
        zabiegi.append([g, z, round(pg * (pz if pz is not None else 1.0), 5), rozk])
        wymiary = [_top(rozk[w])[:k] if rozk[w] else [(NIE_DOTYCZY if z != OGOLNIE else OGOLNIE, None)] for w in FACETY]
        for kombinacja in itertools.product(*wymiary):
            et = (g, z) + tuple(e for e, _p in kombinacja)
            kandydaci.append(_sciezka(et, [pg, pz] + [p for _e, p in kombinacja]))
    beam = sorted(kandydaci, key=lambda s: -s["wynik"])[:k]
    return {"pozycja": {o: round(p, 4) for o, p in p01["pozycja"].items()}, "zabiegi": zabiegi,
            "sciezki": [[list(s["sciezka"]), round(math.exp(s["log_p"]), 5), round(s["wynik"], 5)] for s in beam],
            **{kl: round(r2.nouls[kl].noul, 3) for kl in PYTANIA_WSPOLNE if kl in r2.nouls}}


def pozycja(rek: dict) -> str:
    return max(rek["pozycja"], key=rek["pozycja"].get) if rek.get("pozycja") else "zabieg"


def konkretna(s: tuple) -> bool:
    return len(s) >= 2 and s[1] != OGOLNIE


def glebokosc(a: tuple, b: tuple) -> int:
    """Liczba zgodnych poziomów modelu od góry (0–5): poziom pokrycia ścieżek."""
    n = 0
    for _poziom, od, do in POZIOMY_MODELU:
        if tuple(a[od:do]) != tuple(b[od:do]):
            break
        n += 1
    return n


def p_ta_sama(a: dict, b: dict) -> float:
    """Prawdopodobieństwo wspólnego konkretnego liścia przy niezależnych rozkładach obu usług: suma po wspólnych
    zabiegach iloczynu krawędzi i zgodności każdego wymiaru (Σ p_a·p_b po opcjach). Informacja, nie werdykt."""
    zb = {(g, z): (p, r) for g, z, p, r in b.get("zabiegi", [])}
    suma = 0.0
    for g, z, pa, ra in a.get("zabiegi", []):
        if z == OGOLNIE or (g, z) not in zb:
            continue
        pb, rb = zb[(g, z)]
        s = pa * pb
        for w in FACETY:
            da, db = ra.get(w), rb.get(w)
            if da and db:
                s *= sum(p * db.get(o, 0.0) for o, p in da.items())
        suma += s
    return round(suma, 4)


def porownaj_v12(a: dict | None, b: dict | None, fa: dict | None = None, fb: dict | None = None) -> tuple[str, str, int]:
    """→ (werdykt, powód, poziom pokrycia 0–5). Werdykt: tozsame | powiazane | niepelne | rozne.

    Ta sama usługa = ten sam konkretny liść (najlepsza ścieżka wiązki, jak w cookbooku); inaczej werdykt
    z poziomu, na którym ścieżki się rozchodzą."""
    if not a or not b or not a.get("sciezki") or not b.get("sciezki"):
        return "niepelne", "brak destylacji", 0
    pa, pb = pozycja(a), pozycja(b)
    if pa not in POROWNYWANE or pb not in POROWNYWANE:
        return "rozne", f"nie porównujemy: {pa if pa not in POROWNYWANE else pb}", 0
    na, nb = tuple(a["sciezki"][0][0]), tuple(b["sciezki"][0][0])
    g = glebokosc(na, nb)
    if fa and fb and na[:1] == nb[:1] and _nazwa(fa.get("nazwa")) and _nazwa(fa.get("nazwa")) == _nazwa(fb.get("nazwa")):
        return "tozsame", "identyczna nazwa", 5
    if pa != pb:
        return "powiazane", "pakiet i pojedyncza wizyta", g
    for klucz, powod in (("zestaw", "zestaw"), ("rozszerzenie", "dodatek")):
        if _jednostronne(a, b, klucz):
            return "powiazane", powod, g
    if not (konkretna(na) and konkretna(nb)):
        return "niepelne", "zabieg nieustalony", g
    if na == nb:
        return "tozsame", "ta sama ścieżka", 5
    if g <= 1:
        return "rozne", "inny zabieg", g
    _poziom, od, do = POZIOMY_MODELU[g]
    i = next(i for i in range(od, do) if na[i] != nb[i])
    if OGOLNIE in (na[i], nb[i]):
        return "niepelne", f"{NAZWY_WYMIAROW[i]} nieustalony", g
    return "powiazane", f"inny poziom: {NAZWY_WYMIAROW[i]}", g


__all__ = ["K", "OGOLNIE", "NIE_DOTYCZY", "FACETY", "wiazka_v12", "porownaj_v12", "p_ta_sama", "glebokosc",
           "pytanie_zabiegu", "pytanie_poziomu"]
