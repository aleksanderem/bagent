"""Drzewo usług v11 — klasyfikacja jak w cookbooku TypeSafe „hierarchical_classification” (bd BEAUTY_AUDIT-asrk).

Decyzja Alexa 28.09: taksonomia to drzewo, po którym TypeSafe schodzi od góry do liścia.
Kategoriom salonów z Booksy nie ufamy — nakładają się („manicure” w Paznokciach i Salonie
kosmetycznym, ogólny „zabieg” w pięciu), więc górny poziom to GRUPY zdefiniowane tym, co się
robi: podział rozłączny, każda grupa z opisem, wykluczeniem i przykładami (grupy_uslug.py).

Poziomy: grupa → zabieg (rodzaj z taksonomii v9, osadzony w grupie po nazwach usług; to samo
słowo w dwóch grupach to dwa różne węzły: przedłużanie paznokci ≠ przedłużanie rzęs) → odmiana.
Każdy węzeł to jedno pytanie wyboru między bezpośrednimi dziećmi. Gdy nazwa nie wskazuje
zabiegu ani odmiany, model wybiera „ogolnie” — liść na poziomie rodzica.

Wiązka jak w cookbooku: 3 najlepsze ścieżki na KAŻDYM poziomie, pytania o wszystkie ścieżki
jednej usługi w jednym wywołaniu (jedna usługa na wywołanie — decyzja 27.09). Ścieżki
porównuje średnia geometryczna prawdopodobieństw krawędzi (płytkie i głębokie liście
uczciwie); prawdopodobieństwo liścia = iloczyn krawędzi. Krawędź poniżej MIN_KRAWEDZ nie
wchodzi do wiązki — przy 0,05 jej ścieżka ma średnią najwyżej 0,22 (ochrona kosztu).
"""

from __future__ import annotations

import math
from typing import Any

from typesafe_sdk import Choice

from .podzial import _nazwa, jako_schemat
from .schemat import MODEL, porownaj
from .schemat_v10 import _bez_czasu, _jednostronne, _z_liczbami

WERSJA = 11
K = 3
MAX_GLEBOKOSC = 6  # grupa + zabieg + do 4 poziomów odmian (worki rozbite na węzły z nazw usług)
MIN_KRAWEDZ = 0.05
MAX_OPCJI = 240
OGOLNIE = "ogolnie"
EPS = 1e-9

Sciezka = tuple[str, ...]


def dzieci(drzewo: dict, sciezka: Sciezka) -> dict[str, dict]:
    """Bezpośrednie dzieci węzła (bez „ogolnie”); puste = liść. Poniżej zabiegu dzieci to „odmiany”."""
    if not sciezka:
        return drzewo["grupy"]
    if OGOLNIE in sciezka:
        return {}
    wezly = drzewo["zabiegi"].get(sciezka[0], {})
    for etykieta in sciezka[1:]:
        wezly = wezly.get(etykieta, {}).get("odmiany", {})
    return wezly


def pytanie(drzewo: dict, sciezka: Sciezka) -> Choice:
    """Jedno pytanie wyboru o bezpośrednie dzieci węzła."""
    dz = dzieci(drzewo, sciezka)
    if not sciezka:
        return Choice(
            instructions="Do jakiej grupy usług należy usługa `usluga`? Rozstrzyga nazwa, kategoria w cenniku i opis; "
                         "typ salonu to tylko kontekst — salony sprzedają też usługi spoza swojego typu.",
            criteria={g: {"what": d["what"], "not_for": d["not_for"], "examples": d["examples"]} for g, d in dz.items()},
        )
    opcje = sorted(dz.items(), key=lambda kv: -kv[1].get("n", 0))[:MAX_OPCJI]
    if len(sciezka) == 1:
        instr = f"Jaki zabieg z grupy „{sciezka[0]}” wykonuje się w usłudze `usluga`?"
        ogolnie = {"what": f"usługa z grupy „{sciezka[0]}” bez wskazania, który to zabieg",
                   "not_for": "nazwa lub opis wskazują jeden z zabiegów z listy"}
    else:
        instr = f"Którą odmianą zabiegu „{sciezka[-1]}” jest usługa `usluga`?"
        ogolnie = {"what": f"{sciezka[-1]} bez wskazania odmiany",
                   "not_for": "nazwa lub opis wskazują jedną z odmian z listy"}
    return Choice(instructions=instr,
                  criteria={**{k: {"examples": d.get("examples", [])[:3]} for k, d in opcje}, OGOLNIE: ogolnie})


def _rozwin(s: dict, etykieta: str, prawd: dict[str, float]) -> dict:
    decyzja = len(prawd) > 1
    log_p = s["log_p"] + (math.log(max(prawd[etykieta], EPS)) if decyzja else 0.0)
    n = s["decyzje"] + decyzja
    return {"sciezka": s["sciezka"] + (etykieta,), "log_p": log_p, "decyzje": n,
            "wynik": math.exp(log_p / n) if n else 1.0}


async def wiazka(client: Any, st: dict, drzewo: dict, tok: list, k: int = K,
                 grupy: dict[str, float] | None = None) -> list[dict]:
    """Najlepsze k ścieżek od korzenia do liścia (malejąco wg średniej geometrycznej).
    `grupy` = gotowy rozkład pierwszego pytania (to samo pytanie, np. z budowy drzewa) — bez wywołania."""
    beam = [{"sciezka": (), "log_p": 0.0, "decyzje": 0, "wynik": 1.0}]
    for poziom in range(MAX_GLEBOKOSC):
        rozwijane = [s for s in beam if dzieci(drzewo, s["sciezka"])]
        if not rozwijane:
            break
        if poziom == 0 and grupy:
            odp = {"_": grupy}
        else:
            q = {"|".join(s["sciezka"]) or "_": pytanie(drzewo, s["sciezka"]) for s in rozwijane}
            r = await client.system_one(st, q, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            odp = {kl: a.probabilities for kl, a in r.choices.items()}
        nowe = [s for s in beam if not dzieci(drzewo, s["sciezka"])]
        for s in rozwijane:
            prawd = odp["|".join(s["sciezka"]) or "_"]
            naj = max(prawd, key=prawd.get)  # najlepsza krawędź zostaje zawsze, nawet przy płaskim rozkładzie
            nowe += [_rozwin(s, e, prawd) for e, p in prawd.items() if p >= MIN_KRAWEDZ or e == naj]
        beam = sorted(nowe, key=lambda s: -s["wynik"])[:k]
    return beam


def rozklad(beam: list[dict]) -> dict[Sciezka, float]:
    """Liść → prawdopodobieństwo (iloczyn krawędzi); masa spoza wiązki przepada — ostrożnie."""
    return {tuple(s["sciezka"]): math.exp(s["log_p"]) for s in beam}


def konkretny(sciezka: Sciezka) -> bool:
    """Liść wskazuje zabieg (nie tylko grupę)."""
    return len(sciezka) >= 2 and sciezka[1] != OGOLNIE


def p_ten_sam(ra: dict[Sciezka, float], rb: dict[Sciezka, float]) -> float:
    return sum(p * rb[s] for s, p in ra.items() if s in rb and konkretny(s))


def zabieg_liscia(sciezka: Sciezka, rodzaje: dict | None = None) -> str | None:
    """Rodzaj z taksonomii v9 dla cech: najgłębsza etykieta ścieżki, która jest rodzajem v9
    (węzły z rozbitych worków to frazy z nazw — cechy mają po najbliższym przodku)."""
    if not konkretny(sciezka):
        return None
    etykiety = [e for e in sciezka[1:] if e != OGOLNIE]
    if rodzaje is None:
        return etykiety[min(1, len(etykiety) - 1)] if len(etykiety) > 1 else etykiety[0]
    return next((e for e in reversed(etykiety) if e in rodzaje), etykiety[0])


def relacja_drzewa(a: Sciezka, b: Sciezka) -> tuple[str, str]:
    """Relacja dwóch NAJLEPSZYCH liści, gdy drzewo nie daje „ta sama”."""
    if a[:1] != b[:1]:
        return "rozne", "inna grupa"
    if not (konkretny(a) and konkretny(b)):
        return "niepelne", "zabieg nieustalony"
    if a[1] != b[1]:
        return "rozne", "inny zabieg"
    for i in range(2, max(len(a), len(b))):
        oa, ob = (a[i] if len(a) > i else OGOLNIE), (b[i] if len(b) > i else OGOLNIE)
        if oa == ob:
            continue
        if OGOLNIE in (oa, ob):
            return "niepelne", "zabieg ogólny"
        return "powiazane", "inna odmiana"
    return "niepelne", "niepewne drzewo"


def rozklad_rekordu(rek: dict) -> dict[Sciezka, float]:
    return {tuple(s): p for s, p, _w in rek.get("sciezki", [])}


def najlepsza(rek: dict) -> Sciezka:
    return tuple(rek["sciezki"][0][0]) if rek.get("sciezki") else ()


def porownaj_v11(a: dict | None, b: dict | None, podzial_cech: dict, fa: dict | None = None, fb: dict | None = None,
                 _sch: dict | None = None, regula_czasu: bool = False) -> tuple[str, str]:
    """„Ta sama” rozstrzyga drzewo: ten sam konkretny liść z prawdopodobieństwem ≥ 0,5 (punkt
    neutralny tak/nie), potem cechy zabiegu jak w v9. Dodatek po jednej stronie i liczby sztuk
    z nazwy jak w v10. Czas trwania nie rozróżnia usług (decyzja Alexa 28.09) — przełącznik do pomiaru."""
    if not a or not b or not a.get("sciezki") or not b.get("sciezki"):
        return "niepelne", "brak destylacji"
    na, nb = najlepsza(a), najlepsza(b)
    if fa and fb and na[:1] == nb[:1] and _nazwa(fa.get("nazwa")) and _nazwa(fa.get("nazwa")) == _nazwa(fb.get("nazwa")):
        return "tozsame", "identyczna nazwa"
    for klucz, powod in (("zestaw", "zestaw"), ("rozszerzenie", "dodatek")):
        if _jednostronne(a, b, klucz):
            return "powiazane", powod
    ra, rb = rozklad_rekordu(a), rozklad_rekordu(b)
    if p_ten_sam(ra, rb) < 0.5:
        return relacja_drzewa(na, nb)
    wspolny = max((s for s in ra if s in rb and konkretny(s)), key=lambda s: ra[s] * rb[s])
    sch = _sch or jako_schemat(podzial_cech)
    r = zabieg_liscia(wspolny, sch["rodzaje"])
    if r not in sch["rodzaje"]:
        return "tozsame", "ten sam liść drzewa"
    def rek(x: dict, f: dict | None) -> dict:
        cechy = x.get("cechy", {})
        return {**_z_liczbami(x, f), "rodzaj": r, "cechy": {r: cechy.get(r) or next(iter(cechy.values()), {})}}
    ca, cb = (fa, fb) if regula_czasu else (_bez_czasu(fa), _bez_czasu(fb))
    return porownaj(rek(a, fa), rek(b, fb), sch, ca, cb)


__all__ = ["WERSJA", "K", "OGOLNIE", "dzieci", "pytanie", "wiazka", "rozklad", "konkretny", "p_ten_sam",
           "zabieg_liscia", "relacja_drzewa", "rozklad_rekordu", "najlepsza", "porownaj_v11"]
