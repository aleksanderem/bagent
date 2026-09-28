"""Destylacja v8/v9 paczkami — tryb POMIAROWY, NIE domyślny (bd BEAUTY_AUDIT-asrk).

Kilka usług w jednym wywołaniu TypeSafe. KAŻDE pytanie ma pełne opcje z opisami, dokładnie
te same co przy destylacji usługa po usłudze — zmienia się tylko wskazanie usługi
(`uslugi[i]` zamiast `usluga`). Pomiar 27.09 (10 salonów, 4101 par, sędzia jako miernik):
paczka oszczędza tylko ok. 9% tokenów (opcje liczą się przy każdym pytaniu, stan raz na
wywołanie), a gubi ok. 10 pkt tożsamych par. Decyzja Alexa 27.09: JEDNA USŁUGA NA
WYWOŁANIE. Moduł zostaje jako zapis zmierzonej, odrzuconej drogi — nie podpinać.

Kroki jak w podzial.rodzaj_v8 / cechy_v8 / dodatki_v8: dziedzina → grupa zabiegów w BEAM
najbardziej prawdopodobnych dziedzinach → odmiana albo „ogólnie” → cechy rodzaju oraz
zestaw i dodatek w nazwie.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from typesafe_sdk import Choice, Noul

from .klasyfikacja import PYTANIA_WSPOLNE
from .podzial import BEAM, MIN_KRAWEDZ, OGOLNIE, WERSJA, pytanie_dziedzina, pytanie_korzen, pytanie_odmiana
from .schemat import MODEL, NIEUSTALONE, _pytania_cech, liczby

logger = logging.getLogger(__name__)

PACZKA = 5
ROWNOLEGLE = 6
U = "`usluga`"


def _dla(i: int) -> str:
    return f"`uslugi[{i}]`"


def _przenies(q: Any, i: int) -> Any:
    """To samo pytanie z tymi samymi opcjami, wskazujące usługę nr i w paczce."""
    instr = q.instructions.replace(U, _dla(i))
    if isinstance(q, Choice):
        return Choice(instructions=instr, criteria=q.criteria)
    return Noul(instructions=instr, criteria=q.criteria)


async def destyluj_paczke(client: Any, stany: list[dict], podzial: dict, dz: dict[str, list[str]], tok: list,
                          wersja: int | None = None) -> list[dict]:
    """stany: [{"usluga": {...}}] w kolejności; zwraca rekord destylacji na każdą usługę."""
    n = len(stany)
    state = {"uslugi": [s["usluga"] for s in stany]}
    wer = wersja if wersja is not None else podzial.get("wersja_schematu", WERSJA)

    async def pytaj(q: dict) -> Any:
        if not q:
            return None
        r = await client.system_one(state, q, model=MODEL)
        tok[0] += r.usage.input_tokens or 0
        return r

    qd = pytanie_dziedzina(podzial)["dziedzina"]
    r1 = await pytaj({f"d{i}": _przenies(qd, i) for i in range(n)})
    dzd = {i: sorted(r1.choices[f"d{i}"].probabilities.items(), key=lambda kv: -kv[1])[:BEAM] for i in range(n)}

    korzenia: dict[str, Choice] = {}
    q2: dict[str, Any] = {}
    for i in range(n):
        for b, p in dzd[i]:
            if p >= MIN_KRAWEDZ:
                if b not in korzenia:
                    korzenia[b] = pytanie_korzen(podzial, b, dz)
                q2[f"k{i}|{b}"] = _przenies(korzenia[b], i)
    r2 = await pytaj(q2)
    wyniki: list[dict] = []
    for i in range(n):
        sciezki = []
        for b, p in dzd[i]:
            a = r2.choices.get(f"k{i}|{b}") if r2 else None
            if a is not None:
                pr = a.probabilities.get(a.choice, 0.0)
                sciezki.append([b, a.choice, round((p * pr) ** 0.5, 3)])
        sciezki.sort(key=lambda x: (x[1] == "inny", -x[2]))
        korzen = sciezki[0][1] if sciezki else "inny"
        wyniki.append({"rodzaj": korzen, "korzen": korzen, "pewnosc": sciezki[0][2] if sciezki else 0.0,
                       "p_odmiany": None, "sciezki": sciezki[:3]})

    r3 = await pytaj({f"o{i}": _przenies(pytanie_odmiana(podzial, w["korzen"], dz[w["korzen"]]), i)
                      for i, w in enumerate(wyniki) if w["korzen"] in dz})
    for i, w in enumerate(wyniki):
        o = r3.choices.get(f"o{i}") if r3 else None
        if o is not None:
            w["p_odmiany"] = round(o.probabilities.get(o.choice, 0.0), 3)
            # odmiana tylko przy wyraźnej większości — inaczej zostaje poziom ogólny
            if o.choice != OGOLNIE and w["p_odmiany"] >= 0.5:
                w["rodzaj"] = o.choice

    q4: dict[str, Any] = {}
    for i, w in enumerate(wyniki):
        if w["rodzaj"] != "inny":
            for k, q in _pytania_cech(w["rodzaj"], podzial["cechy"].get(w["rodzaj"], {})).items():
                q4[f"c{i}|{k.split('|', 1)[1]}"] = _przenies(q, i)
        for k, q in PYTANIA_WSPOLNE.items():
            q4[f"{k}#{i}"] = _przenies(q, i)
    r4 = await pytaj(q4)

    out: list[dict] = []
    for i, (s, w) in enumerate(zip(stany, wyniki)):
        pref = f"c{i}|"
        cechy = {k[len(pref):]: (a.choice if a.probabilities.get(a.choice, 0.0) >= 0.5 else NIEUSTALONE)
                 for k, a in (r4.choices.items() if r4 else []) if k.startswith(pref)}
        dod = {k: round(r4.nouls[f"{k}#{i}"].noul, 3) for k in PYTANIA_WSPOLNE if r4 and f"{k}#{i}" in r4.nouls}
        u = s["usluga"]
        out.append({"wersja": wer, **w, "cechy": {w["rodzaj"]: cechy}, **dod, **liczby(u.get("nazwa"), u.get("opis"))})
    return out


async def destyluj_paczkami(client: Any, stany: dict[str, dict], podzial: dict, dz: dict[str, list[str]], tok: list,
                            paczka: int = PACZKA, rownolegle: int = ROWNOLEGLE) -> dict[str, dict]:
    """{klucz: stan} → {klucz: rekord}; paczka, która się nie uda, zostaje pusta (log)."""
    klucze = list(stany)
    sem = asyncio.Semaphore(rownolegle)
    out: dict[str, dict] = {}

    async def jedna(ks: list[str]) -> None:
        async with sem:
            try:
                rek = await destyluj_paczke(client, [stany[k] for k in ks], podzial, dz, tok)
            except Exception as e:  # noqa: BLE001 — jedna paczka nie zatrzymuje reszty
                logger.warning("paczka destylacji (%d usług) nie przeszła: %s: %s", len(ks), type(e).__name__, str(e)[:160])
                return
        out.update(zip(ks, rek))

    await asyncio.gather(*[jedna(klucze[i:i + paczka]) for i in range(0, len(klucze), paczka)])
    return out


__all__ = ["PACZKA", "destyluj_paczke", "destyluj_paczkami"]
