"""Drzewo usług v11c: rozbicie worków na węzły wybrane z nazw usług (bd BEAUTY_AUDIT-asrk, 28.09).

Pomiar v11: węzeł „klamra ortonyksyjna” zbiera każde leczenie wrastającego paznokcia (podcięcie,
tamponada, opatrunek) — drzewo mówi „ta sama” o różnych zabiegach (52 z 66 pogorszonych par
podologii). Brakuje węzłów, a TypeSafe nie wymyśla etykiet — więc je WYBIERAMY z nazw
(przepis TypeSafe „select instead of generate”):

  1. kod: frazy (1–2 słowa) powtarzające się w nazwach usług liścia — bez słów samej ścieżki,
     w co najmniej MIN_USLUG nazwach (ten sam próg co węzeł drzewa);
  2. TypeSafe, tak/nie per fraza: czy nazywa odrębny zabieg albo schorzenie, które zabieg leczy
     — a nie obszar, markę, liczbę, czas, etap, dodatek czy marketing (te mają cechy i reguły);
  3. liść z ≥ 2 frazami, które przeszły, dostaje je jako dzieci; usługi, w których wiązce jest ten
     liść, idą poziom niżej tym samym pytaniem co przejście po drzewie (wybór dziecka + „ogolnie”).

Reguła wyboru liści jest ta sama dla każdej branży i nie patrzy na miernik (sędzia par).
Frazy pochodzą z nazw zbioru roboczego — wynik na tym zbiorze jest optymistyczny; dowód to
przejście nowym drzewem po nowych salonach.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_worki.py
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.drzewo_uslug import EPS, K, MIN_KRAWEDZ, OGOLNIE, dzieci, pytanie  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, stan  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Noul, NoulCriteria, RetryPolicy  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from drzewo_scalanie import PYTANIE as TA_SAMA  # noqa: E402

DANE = Path(__file__).resolve().parent / "dane"
OUT = DANE / "2026-09-28" / "drzewo"
PARY = OUT / "pary.json"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_USLUG = 3
MAX_FRAZ = 15
ROWNOLEGLE = 30
STOP = frozenset("i w z na do dla oraz bez po od lub ze się the and with pakiet usługa zabieg wizyta cena "
                 "min minut h godz szt os osoba osób gratis promocja nowość".split())

FILTR = {"zabieg": Noul(
    instructions="Czy fraza `fraza` w nazwach usług z węzła `wezel` mówi, JAKI to zabieg albo jakie schorzenie "
                 "zabieg leczy — czyli zmienia, którą usługę klient kupuje? Przykłady to nazwy z cenników.",
    criteria=NoulCriteria(
        true="fraza nazywa odrębny zabieg, technikę zabiegu albo leczone schorzenie (np. tamponada, grzybica, "
             "sombre, drenaż limfatyczny)",
        false="fraza to obszar ciała, marka, liczba, czas trwania, etap (założenie, uzupełnienie), dodatek, "
              "odbiorca, inny osobny zabieg sprzedawany razem albo słowo marketingowe "
              "(np. nogi, Vectus, 60 min, uzupełnienie, dla kobiet, + strzyżenie, gratis)",
    ),
)}


def rdzen(slowo: str) -> str:
    return slowo[:5]


def tokeny(nazwa: str) -> list[str]:
    return [t for t in re.sub(r"[^a-ząćęłńóśźż]+", " ", nazwa.lower()).split() if len(t) >= 3 and t not in STOP]


def frazy(nazwy: list[str], sciezka: tuple) -> list[tuple[str, int, list[str]]]:
    """(fraza, w ilu nazwach, przykłady) — 1–2 słowa spoza etykiet ścieżki, ≥ MIN_USLUG nazw."""
    zakazane = {rdzen(t) for e in sciezka for t in tokeny(e)}
    df: Counter = Counter()
    forma: dict[tuple, Counter] = defaultdict(Counter)
    przyk: dict[tuple, list[str]] = defaultdict(list)
    for n in set(nazwy):
        t = [x for x in tokeny(n) if rdzen(x) not in zakazane]
        klucze = {(rdzen(a),): a for a in t} | {(rdzen(a), rdzen(b)): f"{a} {b}" for a, b in zip(t, t[1:]) if rdzen(a) != rdzen(b)}
        for k, f in klucze.items():
            df[k] += 1
            forma[k][f] += 1
            if len(przyk[k]) < 5:
                przyk[k].append(n)
    kandydaci = {k for k, n in df.items() if n >= MIN_USLUG}
    out = []
    for k, n in df.most_common():
        if n < MIN_USLUG:
            break
        if len(k) == 1 and any(len(k2) == 2 and k[0] in k2 and df[k2] >= 0.8 * n for k2 in kandydaci):
            continue  # wolimy dokładniejszą frazę dwuwyrazową, gdy niesie prawie te same nazwy
        if len(k) == 2 and (k[0],) in kandydaci and (k[1],) in kandydaci:
            continue  # dwie osobne frazy obok siebie = zestaw („henna regulacja”) — łapie go pytanie o zestaw
        out.append((forma[k].most_common(1)[0][0], n, przyk[k]))
    return out[:MAX_FRAZ]


async def main_async() -> None:
    drzewo = json.loads((OUT / "drzewo_v11b.json").read_text(encoding="utf-8"))
    rek = json.loads((OUT / "destylacje_v11b.json").read_text(encoding="utf-8"))
    uslugi: dict[str, dict] = {}
    for p in json.loads(PARY.read_text(encoding="utf-8")):
        uslugi.setdefault(p["ka"], p["a"])
        uslugi.setdefault(p["kb"], p["b"])
    nazwy_liscia: dict[tuple, list[str]] = defaultdict(list)
    for k, r in rek.items():
        if r.get("sciezki") and k in uslugi:
            s = tuple(r["sciezki"][0][0])
            if len(s) >= 2 and OGOLNIE not in s and not dzieci(drzewo, s):
                nazwy_liscia[s].append(uslugi[k]["nazwa"])
    kand = {s: frazy(n, s) for s, n in nazwy_liscia.items() if len(n) >= 2 * MIN_USLUG}
    kand = {s: f for s, f in kand.items() if len(f) >= 2}
    print(f"liści z ≥ 2 frazami-kandydatami: {len(kand)}, fraz: {sum(len(f) for f in kand.values())}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    przeszly: dict[tuple, list] = defaultdict(list)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def filtr(s: tuple, f: str, n: int, przyk: list[str]) -> None:
            async with sem:
                r = await client.system_one({"wezel": " › ".join(s), "fraza": f, "przyklady": przyk}, FILTR, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            if r.nouls["zabieg"].noul >= 0.5:
                przeszly[s].append((f, n, przyk))
        await asyncio.gather(*[filtr(s, f, n, p) for s, fs in kand.items() for f, n, p in fs])
        # nowe dzieci jednego liścia mają być podziałem: synonimy scala to samo pytanie co drzewo_scalanie.py
        async def synonimy(s: tuple, fs: list) -> list:
            rodzic = {f: f for f, _n, _p in fs}
            def koniec(f: str) -> str:
                while rodzic[f] != f:
                    f = rodzic[f]
                return f
            dane = {f: (n, p) for f, n, p in fs}
            pary = [(a, b) for i, (a, _, _) in enumerate(fs) for b, _, _ in fs[i + 1:]]
            async def jedna(a: str, b: str) -> tuple[str, str, float]:
                st = {"grupa": " › ".join(s), "zabieg_a": {"nazwa": a, "przyklady": dane[a][1][:3]},
                      "zabieg_b": {"nazwa": b, "przyklady": dane[b][1][:3]}}
                async with sem:
                    r = await client.system_one(st, TA_SAMA, model=MODEL)
                tok[0] += r.usage.input_tokens or 0
                return a, b, r.nouls["ta_sama"].noul
            for a, b, p in await asyncio.gather(*[jedna(a, b) for a, b in pary]):
                if p >= 0.5:
                    ra, rb = koniec(a), koniec(b)
                    if ra != rb:
                        rodzic[rb if dane[rb][0] <= dane[ra][0] else ra] = ra if dane[rb][0] <= dane[ra][0] else rb
            grupy: dict[str, list] = defaultdict(list)
            for f in dane:
                grupy[koniec(f)].append(f)
            return [(g, sum(dane[f][0] for f in fs2), [x for f in fs2 for x in dane[f][1]][:5]) for g, fs2 in grupy.items()]

        przeszly = {s: await synonimy(s, fs) for s, fs in przeszly.items() if len(fs) >= 2}
        nowe = copy.deepcopy(drzewo)
        rozbite = {s: fs for s, fs in przeszly.items() if len(fs) >= 2}
        for s, fs in rozbite.items():
            wezel = nowe["zabiegi"][s[0]]
            for e in s[1:-1]:
                wezel = wezel[e]["odmiany"]
            wezel[s[-1]].setdefault("odmiany", {}).update(
                {f: {"n": n, "examples": przyk[:3], "odmiany": {}} for f, n, przyk in sorted(fs, key=lambda x: -x[1])})
        print(f"rozbite liście: {len(rozbite)}", flush=True)
        for s, fs in sorted(rozbite.items(), key=lambda kv: -len(nazwy_liscia[kv[0]])):
            print(f"  {' › '.join(s)[:55]:<55} ({len(nazwy_liscia[s])} usł.) → " + ", ".join(f for f, _n, _p in sorted(fs, key=lambda x: -x[1])))
        # poziom niżej dla usług, w których wiązce jest rozbity liść — to samo pytanie co przejście po drzewie
        nowe_rek = {}

        async def zejdz(k: str, r: dict) -> None:
            sc = [(tuple(s), p) for s, p, _w in r.get("sciezki", [])]
            do = [s for s, _p in sc if s in rozbite]
            if not do or k not in uslugi:
                nowe_rek[k] = r
                return
            u = uslugi[k]
            st = stan(u["nazwa"], u.get("kategoria_w_cenniku") or None, u.get("typ_salonu") or None, None, None)
            q = {"|".join(s): pytanie(nowe, s) for s in do}
            async with sem:
                odp = await client.system_one(st, q, model=MODEL)
            tok[0] += odp.usage.input_tokens or 0
            wiazka = []
            for s, p in sc:
                if s not in rozbite:
                    wiazka.append((s, p))
                    continue
                prawd = odp.choices["|".join(s)].probabilities
                naj = max(prawd, key=prawd.get)
                wiazka += [(s + (e,), p * max(pe, EPS)) for e, pe in prawd.items() if pe >= MIN_KRAWEDZ or e == naj]
            wiazka = sorted(wiazka, key=lambda x: -(x[1] ** (1 / max(len(x[0]) - 1, 1))))[:K]
            nowe_rek[k] = {**r, "sciezki": [[list(s), round(p, 5), round(p ** (1 / max(len(s) - 1, 1)), 5)] for s, p in wiazka]}

        await asyncio.gather(*[zejdz(k, r) for k, r in rek.items()])
    (OUT / "drzewo_v11c.json").write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "destylacje_v11c.json").write_text(json.dumps(nowe_rek, ensure_ascii=False), encoding="utf-8")
    print(f"koszt {tok[0] * CENA_TOK:.3f} USD")


if __name__ == "__main__":
    asyncio.run(main_async())
