"""Drzewo usług v11b: rodzeństwo jako podział — scalanie węzłów, które są tą samą usługą (bd BEAUTY_AUDIT-asrk, 28.09).

Pomiar v11 (28.09): „depilacja laserowa”, „laserem diodowym” i „laserowa Vectus” to trzy węzły
jednej usługi — pewność rozkłada się między nie i drzewo nie mówi „ta sama” (13 z 14 pogorszonych
par depilacji). Rodzeństwo musi być podziałem.

Kandydaci (bez miernika): pary rodzeństwa, które przejście po drzewie myli — oba liście w wiązce
jednej usługi, każdy z p ≥ 0,1, w co najmniej MIN_USLUG usługach (ten sam próg co węzeł drzewa).
Decyzja: pytanie tak/nie TypeSafe na przykładach nazw obu węzłów; scalamy przy p ≥ 0,5 (punkt
neutralny). Sędzia par nie bierze udziału — zostaje miernikiem.

Scalenie nie wymaga ponownego przejścia: prawdopodobieństwa ścieżek scalonych liści się sumują,
więc zapisane wiązki przelicza się na nowe drzewo (przemapuj()).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_scalanie.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.drzewo_uslug import OGOLNIE  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Noul, NoulCriteria, RetryPolicy  # noqa: E402

DANE = Path(__file__).resolve().parent / "dane"
OUT = DANE / "2026-09-28" / "drzewo"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_USLUG = 3
MIN_P = 0.1

PYTANIE = {"ta_sama": Noul(
    instructions="Czy `zabieg_a` i `zabieg_b` z tej samej grupy (`grupa`) to ta sama usługa dla klienta? "
                 "Przykłady to nazwy usług z cenników salonów.",
    criteria=NoulCriteria(
        true="klient dostaje to samo: te same czynności na tym samym obszarze; różni się tylko nazwa, "
             "marka lub typ urządzenia albo sformułowanie",
        false="inna technika, inny preparat, inny obszar albo inny zakres zabiegu",
    ),
)}


def pomylenia(rek: dict) -> Counter:
    myl: Counter = Counter()
    for r in rek.values():
        sc = [(tuple(s), p) for s, p, _w in r.get("sciezki", [])]
        for i, (a, pa) in enumerate(sc):
            for b, pb in sc[i + 1:]:
                if (len(a) == len(b) and a[:-1] == b[:-1] and a[-1] != b[-1] and min(pa, pb) >= MIN_P
                        and OGOLNIE not in (a[-1], b[-1])):
                    myl[(a[:-1], tuple(sorted((a[-1], b[-1]))))] += 1
    return myl


def wezel(drzewo: dict, sciezka: tuple) -> dict:
    return drzewo["zabiegi"][sciezka[0]][sciezka[1]] if len(sciezka) == 2 else \
        drzewo["zabiegi"][sciezka[0]][sciezka[1]]["odmiany"][sciezka[2]]


def scal(drzewo: dict, decyzje: list[tuple[tuple, tuple]]) -> tuple[dict, dict]:
    """Scala B w A (A = liczniejszy); zwraca nowe drzewo i mapę starej ścieżki na nową."""
    import copy
    d = copy.deepcopy(drzewo)
    mapa: dict[tuple, tuple] = {}

    def koniec(s: tuple) -> tuple:
        while s in mapa:
            s = mapa[s]
        return s

    for a, b in decyzje:
        a, b = koniec(a), koniec(b)
        if a == b:
            continue
        wa, wb = wezel(d, a), wezel(d, b)
        if wb["n"] > wa["n"]:
            a, b, wa, wb = b, a, wb, wa
        wa["n"] += wb["n"]
        wa["examples"] = (wa["examples"] + [x for x in wb["examples"] if x not in wa["examples"]])[:3]
        for o, wo in wb.get("odmiany", {}).items():
            wa.setdefault("odmiany", {}).setdefault(o, wo)
            mapa[b + (o,)] = a + (o,)
        rodzic = d["zabiegi"][b[0]] if len(b) == 2 else d["zabiegi"][b[0]][b[1]]["odmiany"]
        rodzic.pop(b[-1])
        mapa[b] = a
    return d, {s: koniec(s) for s in mapa}


def przemapuj(rek: dict, mapa: dict) -> dict:
    """Wiązka na nowym drzewie: ścieżki scalonych liści sumują prawdopodobieństwo."""
    out = {}
    for k, r in rek.items():
        suma: dict[tuple, float] = {}
        for s, p, _w in r.get("sciezki", []):
            s = tuple(s)
            nowa = next((mapa[s[:i]] + s[i:] for i in range(len(s), 0, -1) if s[:i] in mapa), s)
            suma[nowa] = suma.get(nowa, 0.0) + p
        sc = sorted(suma.items(), key=lambda kv: -kv[1])
        rodzaj = r.get("rodzaj")
        if sc:
            naj = sc[0][0]
            rodzaj = (naj[2] if len(naj) > 2 and naj[2] != OGOLNIE else (naj[1] if len(naj) > 1 else "inny"))
        out[k] = {**r, "sciezki": [[list(s), round(p, 5), round(p, 5)] for s, p in sc],
                  "rodzaj": rodzaj, "cechy": {rodzaj: next(iter(r.get("cechy", {}).values()), {})}}
    return out


async def main_async() -> None:
    drzewo = json.loads((OUT / "drzewo_v11.json").read_text(encoding="utf-8"))
    rek = json.loads((OUT / "destylacje_v11.json").read_text(encoding="utf-8"))
    kand = [(rodzic, pary) for (rodzic, pary), n in pomylenia(rek).items() if n >= MIN_USLUG]
    print(f"kandydatów do scalenia: {len(kand)}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok, decyzje, zapis = [0], [], []
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def jedna(rodzic: tuple, x: str, y: str) -> None:
            a, b = rodzic + (x,), rodzic + (y,)
            st = {"grupa": " › ".join(rodzic),
                  "zabieg_a": {"nazwa": x, "przyklady": wezel(drzewo, a)["examples"]},
                  "zabieg_b": {"nazwa": y, "przyklady": wezel(drzewo, b)["examples"]}}
            r = await client.system_one(st, PYTANIE, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            p = round(r.nouls["ta_sama"].noul, 3)
            zapis.append({"rodzic": list(rodzic), "a": x, "b": y, "p": p})
            if p >= 0.5:
                decyzje.append((a, b))
        await asyncio.gather(*[jedna(r, x, y) for r, (x, y) in kand])
    nowe, mapa = scal(drzewo, decyzje)
    nowe["scalenia"] = zapis
    (OUT / "drzewo_v11b.json").write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "destylacje_v11b.json").write_text(json.dumps(przemapuj(rek, mapa), ensure_ascii=False), encoding="utf-8")
    print(f"koszt {tok[0] * CENA_TOK:.4f} USD; scalono {len(decyzje)} z {len(kand)}")
    for z in sorted(zapis, key=lambda z: -z["p"]):
        print(f"  {'SCAL' if z['p'] >= 0.5 else '    '} {z['p']:.2f} | {' › '.join(z['rodzic'])[:32]:<32} | {z['a'][:30]} ↔ {z['b'][:30]}")


if __name__ == "__main__":
    asyncio.run(main_async())
