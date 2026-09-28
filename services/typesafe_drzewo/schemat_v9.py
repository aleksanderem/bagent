"""Schemat v9 — cechy, po których porównujemy rodzaj zabiegu, bez dziur (bd BEAUTY_AUDIT-asrk).

Pomiar 26.09 (12 losowych salonów, sędzia par jako miernik): taksonomia mówiła „ta sama
usługa” trafnie w 60%. Przyczyna leżała w schemacie, nie w pytaniach do TypeSafe:
cechy rodzaju liczono tylko z jego własnych nazw (cecha ≥ 5% nazw rodzaju), więc
  * rodzaj z kilku nazw miał przypadkowe cechy („depilacja cukrem lub woskiem”, 3 nazwy,
    bez „obszaru” — szyja i wąsik wychodziły jako ta sama usługa),
  * cechę, którą salon podaje rzadko, ale która zmienia usługę (poziom stylisty,
    marka preparatu, technika), rodzaj tracił (160 z 430 rodzajów bez „obszaru”).

Trzy reguły, te same dla każdej branży:
  1. scal_male — rodzaj z mniej niż MIN_RODZAJ nazw, który ma rodzica, przechodzi do
     rodzica: przy 20 nazwach próg 5% to jedna nazwa, więc cechy są przypadkowe;
  2. dziedzicz_cechy — odmiana ma wszystkie cechy przodków (wartość domyślna tylko
     własna: domyślność rodzica nie musi obowiązywać odmiany);
  3. cechy_uniwersalne — każdy rodzaj ma cechy UNIWERSALNE z wartościami z całej swojej
     dziedziny (kolejność wg liczby nazw); nieznana wartość to „inna” i porównanie
     daje „niepełne”, nigdy „ta sama”.
"""

from __future__ import annotations

import copy
from collections import Counter
from typing import Any

MIN_RODZAJ = 20
MAX_WARTOSCI = 240  # limit opcji jednego pytania Choice (API: 255) z zapasem na nie_podano/inna
UNIWERSALNE = ("obszar", "odbiorca", "technika", "preparat_urzadzenie", "poziom_specjalisty")
WERSJA_SCHEMATU = 9


def przodkowie(rodzic: dict[str, str], r: str) -> list[str]:
    out: list[str] = []
    x = r
    while x in rodzic and rodzic[x] not in out and rodzic[x] != r:
        x = rodzic[x]
        out.append(x)
    return out


def scal_male(podzial: dict, min_n: int = MIN_RODZAJ) -> dict:
    p = copy.deepcopy(podzial)
    liczn, rodzic = p["liczn"], p["rodzic"]
    male = {r for r in p["kanoniczne"] if liczn.get(r, 0) < min_n and r in rodzic}

    def cel(r: str) -> str:
        for a in przodkowie(rodzic, r):
            if a not in male:
                return a
        return r

    docel = {r: cel(r) for r in male}
    for r, t in sorted(docel.items(), key=lambda kv: liczn.get(kv[0], 0)):
        if t == r:
            continue
        liczn[t] = liczn.get(t, 0) + liczn.pop(r, 0)
        p["przyklady"][t] = p["przyklady"].get(t, []) + [n for n in p["przyklady"].pop(r, []) if n not in p["przyklady"].get(t, [])]
        p["cechy"].pop(r, None)
    p["kanon"] = {k: docel.get(v, v) for k, v in p["kanon"].items()}
    p["rodzic"] = {dz: docel.get(ro, ro) for dz, ro in rodzic.items() if dz not in docel}
    p["kanoniczne"] = [r for r in p["kanoniczne"] if r not in docel or docel[r] == r]
    p["korzen"] = {r: (przodkowie(p["rodzic"], r) or [r])[-1] for r in p["kanoniczne"]}
    return p


def dziedzicz_cechy(podzial: dict) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in podzial["kanoniczne"]:
        cechy = copy.deepcopy(podzial["cechy"].get(r, {}))
        for a in przodkowie(podzial["rodzic"], r):
            for c, d in podzial["cechy"].get(a, {}).items():
                if c not in cechy:
                    cechy[c] = {"wartosci": list(d.get("wartosci", [])), "domyslna": None, "z_przodka": a}
                else:
                    cechy[c]["wartosci"] = cechy[c]["wartosci"] + [v for v in d.get("wartosci", []) if v not in cechy[c]["wartosci"]]
        out[r] = cechy
    return out


def cechy_uniwersalne(podzial: dict, cechy: dict[str, dict], uniwersalne: tuple[str, ...] = UNIWERSALNE,
                      max_w: int = MAX_WARTOSCI) -> dict[str, dict]:
    korzen = podzial["korzen"]
    dziedziny_korzenia: dict[str, list[str]] = {}
    for b, rs in podzial["branze"].items():
        for k in rs:
            dziedziny_korzenia.setdefault(k, []).append(b)
    w_dziedzinie: dict[tuple[str, str], Counter] = {}
    for q in podzial["kanoniczne"]:
        for b in dziedziny_korzenia.get(korzen.get(q, q), []):
            for c in uniwersalne:
                for v in (cechy.get(q, {}).get(c) or {}).get("wartosci", []):
                    w_dziedzinie.setdefault((b, c), Counter())[v] += podzial["liczn"].get(q, 1)
    out = copy.deepcopy(cechy)
    for r in podzial["kanoniczne"]:
        for c in uniwersalne:
            if c in out.get(r, {}):
                continue
            razem: Counter = Counter()
            for b in dziedziny_korzenia.get(korzen.get(r, r), []):
                razem.update(w_dziedzinie.get((b, c), Counter()))
            wart = [v for v, _ in razem.most_common(max_w)]
            if wart:
                out.setdefault(r, {})[c] = {"wartosci": wart, "domyslna": None, "uniwersalna": True}
    return out


def schemat_v9(podzial: dict, min_n: int = MIN_RODZAJ) -> dict[str, Any]:
    p = scal_male(podzial, min_n)
    p["cechy"] = cechy_uniwersalne(p, dziedzicz_cechy(p))
    p["wersja_schematu"] = WERSJA_SCHEMATU
    return p


__all__ = ["MIN_RODZAJ", "MAX_WARTOSCI", "UNIWERSALNE", "WERSJA_SCHEMATU",
           "przodkowie", "scal_male", "dziedzicz_cechy", "cechy_uniwersalne", "schemat_v9"]
