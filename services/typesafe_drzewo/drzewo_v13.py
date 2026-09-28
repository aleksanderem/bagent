"""Drzewo v13 — przejście wiązką po poziomach i porównanie usług (bd BEAUTY_AUDIT-asrk, 28.09).

Model Alexa: 1 dziedzina (czego dotyczy) → 2 zabieg (co się robi) → 3 metoda → 4 gdzie i ile → 5 etap.
Cookbook TypeSafe hierarchical_classification (zasada niezmienna):
  * każdy poziom to wybór spośród dzieci węzła z poprzedniego poziomu; wiązka 3 ścieżek na każdym poziomie,
    średnia geometryczna krawędzi; węzły frontu wiązki pytane równolegle (jedno wywołanie na poziom);
  * zabieg może mieć kilku rodziców (jak MeSH w cookbooku — graf rozwinięty w ścieżki): ta sama usługa to ten
    sam WĘZEŁ zabiegu i te same poziomy niżej, niezależnie od dziedziny, przez którą do niego doszła;
  * poziomy 3–5 zależą tylko od zabiegu (drzewo-iloczyn), więc ich pytania idą razem w jednym wywołaniu;
  * „ogolnie” = usługa tego nie mówi; „inne” = mówi coś spoza listy (bez tej opcji wybór wpychał usługę
    w najbliższą opcję — „włosy do ramion” w „długie”).
O „ta sama / podobna / inna” rozstrzyga drzewo; żadnych reguł na nazwach.
"""

from __future__ import annotations

import itertools
import math
from typing import Any

from typesafe_sdk import Choice

from .klasyfikacja import PYTANIA_WSPOLNE
from .kontekst_v12 import POZYCJE
from .schemat import MODEL
from .schemat_v10 import _jednostronne

K = 3
MIN_KRAWEDZ = 0.05
MIN_ROZKLAD = 0.01
MAX_OPCJI = 240
OGOLNIE = "ogolnie"
INNE = "inne"
NIE_DOTYCZY = "—"
NIZEJ = ("metoda", "gdzie", "etap")
NAZWY = ("dziedzina", "zabieg", "metoda", "gdzie i ile", "etap")
POROWNYWANE = frozenset({"zabieg", "pakiet"})
EPS = 1e-9

PYTANIA_NIZEJ = {
    "metoda": "Jaką metodą, techniką, urządzeniem albo preparatem wykonuje się zabieg „{z}” w usłudze `usluga`?",
    "gdzie": "Gdzie i ile obejmuje zabieg „{z}” w usłudze `usluga` — obszar, długość, rozmiar, gęstość, liczba, dla kogo?",
    "etap": "Którą wizytą w cyklu zabiegu „{z}” jest usługa `usluga`?",
}
OPCJE_BRAKU = {
    OGOLNIE: {"what": "usługa tego nie określa", "not_for": "usługa to określa"},
    INNE: {"what": "usługa określa to inaczej niż którakolwiek opcja z listy", "not_for": "pasuje jedna z opcji"},
}


def _opcje(wezly: dict) -> dict:
    opcje = sorted(wezly.items(), key=lambda kv: -kv[1].get("n", 0))[:MAX_OPCJI]
    return {**{k: {"examples": d.get("examples", [])[:3], "inne_nazwy": [s for s in d.get("synonimy", []) if s != k][:5]}
               for k, d in opcje}, **OPCJE_BRAKU}


def zabiegi_dziedziny(drzewo: dict, d: str) -> dict[str, dict]:
    return {g: {"n": w["n"], "examples": w["examples"], "synonimy": w["synonimy"]}
            for g, w in drzewo["zabiegi"].items() if d in w["rodzice"]}


def pytania_1(drzewo: dict) -> dict[str, Any]:
    opis = drzewo.get("dziedziny_opis", {})
    return {
        "pozycja": Choice(instructions="Czym jest pozycja `usluga` w cenniku salonu `salon`? Rozstrzygają nazwa, kategoria, "
                                       "opis i warianty.", criteria=POZYCJE),
        "dziedzina": Choice(
            instructions="Czego dotyczy usługa `usluga` — jakiej części ciała albo obiektu? Rozstrzygają nazwa, kategoria, "
                         "opis, warianty i zabieg wybrany w Booksy.",
            criteria={**{d: {"examples": opis.get(d, {}).get("miejsca", [])[:6], "zabiegi": opis.get(d, {}).get("zabiegi", [])[:6]}
                         for d in drzewo["dziedziny"]}, INNE: {"what": "usługa nie dotyczy żadnej z tych dziedzin"}}),
        **PYTANIA_WSPOLNE,
    }


def pytanie_zabiegu(drzewo: dict, d: str) -> Choice | None:
    zg = zabiegi_dziedziny(drzewo, d)
    if not zg:
        return None
    etyk = {g: drzewo["zabiegi"][g]["etykieta"] for g in zg}
    return Choice(instructions=f"Jaki zabieg wykonuje się w usłudze `usluga` (dziedzina „{d}”)? Rozstrzygają nazwa, "
                               "kategoria, opis, warianty i zabieg wybrany w Booksy.",
                  criteria={**{g: {"nazwa": etyk[g], **v} for g, v in _opcje(zg).items() if g in zg},
                            **OPCJE_BRAKU})


def pytanie_nizej(drzewo: dict, g: str, poz: str) -> Choice | None:
    wezly = drzewo["zabiegi"].get(g, {}).get(poz) or {}
    if not wezly:
        return None
    return Choice(instructions=PYTANIA_NIZEJ[poz].format(z=drzewo["zabiegi"][g]["etykieta"]), criteria=_opcje(wezly))


def _top(prawd: dict[str, float]) -> list[tuple[str, float]]:
    naj = max(prawd, key=prawd.get)
    return [(e, p) for e, p in sorted(prawd.items(), key=lambda kv: -kv[1]) if p >= MIN_KRAWEDZ or e == naj]


def _wynik(prawd: list[float | None]) -> tuple[float, float]:
    decyzje = [p for p in prawd if p is not None]
    log_p = sum(math.log(max(p, EPS)) for p in decyzje)
    return log_p, (math.exp(log_p / len(decyzje)) if decyzje else 1.0)


async def wiazka_v13(client: Any, st: dict, drzewo: dict, tok: list, k: int = K) -> dict:
    """→ {"pozycja", "zestaw", "rozszerzenie", "zabiegi": [[dziedzina, węzeł, p, {poziom: rozkład|None}]],
    "sciezki": [[(dziedzina, węzeł, metoda, gdzie, etap), p, wynik]]}. Trzy wywołania: poziom 1, 2, 3–5."""
    r1 = await client.system_one(st, pytania_1(drzewo), model=MODEL)
    tok[0] += r1.usage.input_tokens or 0
    dziedziny = [(d, p) for d, p in _top(dict(r1.choices["dziedzina"].probabilities)) if d != INNE][:k]
    q2 = {f"z|{d}": q for d, _p in dziedziny if (q := pytanie_zabiegu(drzewo, d)) is not None}
    pary: list[tuple[str, str, float, float | None]] = []
    if q2:
        r2 = await client.system_one(st, q2, model=MODEL)
        tok[0] += r2.usage.input_tokens or 0
        for d, pd in dziedziny:
            a = r2.choices.get(f"z|{d}")
            pary += [(d, g, pd, pg) for g, pg in _top(dict(a.probabilities))] if a else []
    pary = sorted(pary, key=lambda x: -_wynik([x[2], x[3]])[1])[:k]
    q3: dict[str, Choice] = {}
    for _d, g, _pd, _pg in pary:
        if g in drzewo["zabiegi"]:
            for poz in NIZEJ:
                if (q := pytanie_nizej(drzewo, g, poz)) is not None:
                    q3[f"{poz}|{g}"] = q
    r3 = None
    if q3:
        r3 = await client.system_one(st, q3, model=MODEL)
        tok[0] += r3.usage.input_tokens or 0
    zab, kandydaci = [], []
    for d, g, pd, pg in pary:
        rozk: dict[str, dict | None] = {}
        for poz in NIZEJ:
            a = r3.choices.get(f"{poz}|{g}") if r3 else None
            rozk[poz] = {o: round(p, 4) for o, p in a.probabilities.items() if p >= MIN_ROZKLAD} if a else None
        zab.append([d, g, round(pd * (pg or 1.0), 5), rozk])
        poziomy = [_top(rozk[p])[:k] if rozk[p] else [(NIE_DOTYCZY, None)] for p in NIZEJ]
        for komb in itertools.product(*poziomy):
            log_p, wyn = _wynik([pd, pg] + [p for _e, p in komb])
            kandydaci.append(((d, g) + tuple(e for e, _p in komb), log_p, wyn))
    beam = sorted(kandydaci, key=lambda s: -s[2])[:k]
    return {"pozycja": {o: round(p, 4) for o, p in r1.choices["pozycja"].probabilities.items() if p >= MIN_ROZKLAD},
            **{kl: round(r1.nouls[kl].noul, 3) for kl in PYTANIA_WSPOLNE if kl in r1.nouls},
            "zabiegi": zab, "sciezki": [[list(s), round(math.exp(lp), 5), round(w, 5)] for s, lp, w in beam]}


def pozycja(rek: dict) -> str:
    return max(rek["pozycja"], key=rek["pozycja"].get) if rek.get("pozycja") else "zabieg"


def konkretny(x: str) -> bool:
    return x not in (OGOLNIE, INNE)


def porownaj_v13(a: dict | None, b: dict | None, drzewo: dict | None = None) -> tuple[str, str, int]:
    """→ (werdykt, powód, poziom pokrycia 0–5). tozsame | powiazane | niepelne | rozne.

    Ta sama usługa = ten sam węzeł zabiegu i te same metoda, gdzie i ile, etap (najlepsza ścieżka wiązki);
    dziedzina nie musi być ta sama, jeśli zabieg jest wspólny (węzeł z kilkoma rodzicami)."""
    if not a or not b or not a.get("sciezki") or not b.get("sciezki"):
        return "niepelne", "brak destylacji", 0
    pa, pb = pozycja(a), pozycja(b)
    if pa not in POROWNYWANE or pb not in POROWNYWANE:
        return "rozne", f"nie porównujemy: {pa if pa not in POROWNYWANE else pb}", 0
    na, nb = tuple(a["sciezki"][0][0]), tuple(b["sciezki"][0][0])
    wspolny_zabieg = na[1] == nb[1] and konkretny(na[1])
    poziom = 0 if na[0] != nb[0] and not wspolny_zabieg else 1
    if pa != pb:
        return "powiazane", "pakiet i pojedyncza wizyta", poziom
    for klucz, powod in (("zestaw", "zestaw"), ("rozszerzenie", "dodatek")):
        if _jednostronne(a, b, klucz):
            return "powiazane", powod, poziom
    if not (konkretny(na[1]) and konkretny(nb[1])):
        return "niepelne", "zabieg nieustalony", poziom
    if not wspolny_zabieg:
        return "rozne", "inny zabieg", poziom
    for i in range(2, 5):
        x, y = na[i], nb[i]
        # ten sam liść: ta sama opcja, obie usługi tego nie określają („ogolnie”) albo poziom nie dotyczy;
        # „inne” po obu stronach to NIE to samo — dwie różne wartości spoza listy (np. „do ramion” i „do talii”)
        if x == y and x != INNE:
            continue
        if not (konkretny(x) and konkretny(y)):
            return "niepelne", f"{NAZWY[i]} nieustalony" if OGOLNIE in (x, y) else f"{NAZWY[i]} spoza listy", i
        return "powiazane", f"inny poziom: {NAZWY[i]}", i
    return "tozsame", "ta sama ścieżka", 5


__all__ = ["K", "OGOLNIE", "INNE", "NIE_DOTYCZY", "NIZEJ", "wiazka_v13", "porownaj_v13", "pytania_1", "pytanie_zabiegu",
           "pytanie_nizej"]
