"""Automatyczny słownik cech usług z destylacji GLM — ten sam mechanizm dla każdej branży.

Wejście: service_taxonomy (334 tys. nazw, otwarte wartości GLM), zrzucone do CSV.
  krok 0 (kod): wartości ponad progiem — co najmniej 1 na 1000 nazw branży (min. 3),
          oś obecna w ≥ 0,5% nazw branży. Ten sam próg co mapa pytań destylacji.
  krok 1 (TypeSafe, wybór): którą cechę z katalogu (osie.py) opisuje wartość —
          z przykładowymi nazwami usług, w których wystąpiła. Porządkuje
          „1:1” zapisane jako metoda, „60 min” jako rozmiar itp.
  krok 2 (TypeSafe, tak/nie): synonimy w obrębie cechy („laser diodowy” =
          „dioda”) i hierarchia rodzajów („manicure hybrydowy” to odmiana
          „manicure”). Kandydaci par z kodu (wspólne trzyliterowe fragmenty
          albo zawieranie słów), decyzja z modelu, próg 0,8 = przykład z docs
          dla kosztownych pomyłek (sklejenie dwóch różnych wartości psuje porównania).
Wynik: slownik.json — cecha → wartość kanoniczna → synonimy, liczność, rodzic.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/slownik_osi.py --csv <taksonomia.csv.gz> --out <katalog> [--krok 1|2]
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"

from services.typesafe_drzewo.osie import CECHY, LICZBOWE  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, RetryPolicy  # noqa: E402

MODEL = "jev-1.13.0"
ROWNOLEGLE = 6
PRZYKLADOW = 4
TAK = 0.8
PARY_NA_WYWOLANIE = 40
MAX_OPCJI = 250  # limit opcji wyboru w API: 255


def norm(v: Any) -> str:
    return " ".join(str(v).lower().split()).strip(" .,;")


def zbierz(plik: Path) -> tuple[dict[tuple[str, str], int], dict[tuple[str, str], list[str]], Counter]:
    """(oś surowa, wartość) → liczność (suma po branżach ponad progiem) i przykładowe nazwy."""
    per: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    przyk: dict[tuple[str, str], list[str]] = defaultdict(list)
    nazw: Counter = Counter()
    for b, n, o in csv.reader(gzip.open(plik, "rt")):
        nazw[b] += 1
        try:
            d = json.loads(o)
        except ValueError:
            continue
        for k, v in d.items():
            for x in (v if isinstance(v, list) else [v]):
                if x in (None, "", []):
                    continue
                key = (k.lower(), norm(x))
                per[b][key[0]][key[1]] += 1
                if len(przyk[key]) < PRZYKLADOW and n not in przyk[key]:
                    przyk[key].append(n)
    wartosci: dict[tuple[str, str], int] = Counter()
    for b, osie in per.items():
        prog = max(3, nazw[b] // 1000)
        for k, c in osie.items():
            if sum(c.values()) < max(10, nazw[b] // 200):
                continue
            for v, m in c.items():
                if m >= prog:
                    wartosci[(k, v)] += m
    return wartosci, przyk, nazw


PYTANIE_CECHA = {
    "cecha": Choice(
        instructions=(
            "Którą cechę usługi z cennika salonu beauty opisuje `wartosc`? "
            "`przyklady_uslug` to nazwy usług, w których ta wartość wystąpiła."
        ),
        criteria={k: {"what": co, "not_for": nie, "examples": ex} for k, (co, nie, ex) in CECHY.items()},
    )
}


async def krok1(client: Any, wartosci: dict, przyk: dict) -> dict[str, dict]:
    sem = asyncio.Semaphore(ROWNOLEGLE)
    out: dict[str, dict] = {}

    async def jedna(k: str, v: str) -> None:
        state = {"wartosc": v, "przyklady_uslug": przyk.get((k, v), [])}
        async with sem:
            try:
                r = await client.system_one(state, PYTANIE_CECHA, model=MODEL)
            except Exception as e:  # noqa: BLE001
                print(f"krok1 {v!r}: {type(e).__name__}: {str(e)[:100]}")
                return
        c = r.choices["cecha"]
        top = sorted(c.probabilities.items(), key=lambda kv: -kv[1])[:3]
        out[f"{k}|{v}"] = {"cecha": c.choice, "pewnosc": round(c.confidence, 3), "top": [[a, round(p, 3)] for a, p in top],
                           "tokeny": r.usage.input_tokens or 0}

    await asyncio.gather(*[jedna(k, v) for (k, v) in wartosci])
    return out


def _trigramy(s: str) -> set[str]:
    s = f"  {s} "
    return {s[i : i + 3] for i in range(len(s) - 2)}


def kandydaci(wart: list[str]) -> list[tuple[str, str]]:
    """Pary do sprawdzenia: podobne literowo albo jedna zawiera słowa drugiej."""
    tri = {v: _trigramy(v) for v in wart}
    slowa = {v: set(v.split()) for v in wart}
    pary = []
    for i, a in enumerate(wart):
        for b in wart[i + 1 :]:
            j = len(tri[a] & tri[b]) / len(tri[a] | tri[b])
            if j >= 0.4 or slowa[a] <= slowa[b] or slowa[b] <= slowa[a]:
                pary.append((a, b))
    return pary


async def krok2(client: Any, grupy: dict[str, list[str]], przyk: dict[str, list[str]]) -> dict[str, list]:
    """cecha → [(a, b, p_synonim, p_a_odmiana_b, p_b_odmiana_a)]."""
    sem = asyncio.Semaphore(ROWNOLEGLE)
    out: dict[str, list] = defaultdict(list)
    tok = [0]

    async def paczka(cecha: str, pary: list[tuple[str, str]]) -> None:
        co = CECHY[cecha][0]
        q: dict[str, Any] = {}
        for i, (a, b) in enumerate(pary):
            rek = {"a": a, "przyklady_a": przyk.get(a, [])[:3], "b": b, "przyklady_b": przyk.get(b, [])[:3]}
            q[f"syn{i}"] = Noul(instructions={**rek, "question": f"Czy w cennikach salonów `a` i `b` oznaczają to samo ({co})?"})
            if cecha == "rodzaj_zabiegu":
                q[f"ab{i}"] = Noul(instructions={**rek, "question": "Czy `a` to odmiana lub szczególny przypadek zabiegu `b`?"})
                q[f"ba{i}"] = Noul(instructions={**rek, "question": "Czy `b` to odmiana lub szczególny przypadek zabiegu `a`?"})
        async with sem:
            try:
                r = await client.system_one({"cecha_uslugi": co}, q, model=MODEL)
            except Exception as e:  # noqa: BLE001
                print(f"krok2 {cecha}: {type(e).__name__}: {str(e)[:100]}")
                return
        tok[0] += r.usage.input_tokens or 0
        n = r.nouls
        for i, (a, b) in enumerate(pary):
            out[cecha].append([a, b, round(n[f"syn{i}"].noul, 3),
                               round(n[f"ab{i}"].noul, 3) if f"ab{i}" in n else None,
                               round(n[f"ba{i}"].noul, 3) if f"ba{i}" in n else None])

    zadania = []
    for cecha, wart in grupy.items():
        pary = kandydaci(wart)
        print(f"  {cecha:<20} wartości {len(wart):>4} par-kandydatów {len(pary):>6}", flush=True)
        for i in range(0, len(pary), PARY_NA_WYWOLANIE):
            zadania.append(paczka(cecha, pary[i : i + PARY_NA_WYWOLANIE]))
    await asyncio.gather(*zadania)
    print(f"krok2 tokeny {tok[0]} ({tok[0] * 0.042 / 1e6:.3f} USD)")
    return out


def zloz(przypis: dict, liczn: dict, relacje: dict) -> dict:
    """Sklejenie synonimów (union-find) i rodzice rodzajów → słownik kanoniczny."""
    wynik: dict[str, dict] = {}
    for cecha, rel in relacje.items():
        rodzic: dict[str, str] = {}

        def find(x: str) -> str:
            while rodzic.get(x, x) != x:
                x = rodzic[x]
            return x

        for a, b, syn, _ab, _ba in rel:
            if syn >= TAK:
                ra, rb = find(a), find(b)
                if ra != rb:
                    # kanoniczna = częstsza
                    if liczn.get(ra, 0) >= liczn.get(rb, 0):
                        rodzic[rb] = ra
                    else:
                        rodzic[ra] = rb
        grupy: dict[str, list[str]] = defaultdict(list)
        for v in {x for x, c in przypis.items() if c == cecha}:
            grupy[find(v)].append(v)
        rodzice: dict[str, str] = {}
        for a, b, syn, ab, ba in rel:
            if syn >= TAK or ab is None:
                continue
            ca, cb = find(a), find(b)
            if ca == cb:
                continue
            if ab >= TAK and (ba or 0) < TAK:
                rodzice[ca] = cb
            elif ba >= TAK and (ab or 0) < TAK:
                rodzice[cb] = ca
        wynik[cecha] = {
            k: {"synonimy": sorted(v), "n": sum(liczn.get(x, 0) for x in v), "rodzic": rodzice.get(k)}
            for k, v in sorted(grupy.items(), key=lambda kv: -sum(liczn.get(x, 0) for x in kv[1]))
        }
    return wynik


async def krok2_kotwice(client: Any, grupy: dict[str, list[str]], liczn: Counter,
                        przyk: dict[str, list[str]]) -> dict[str, dict[str, dict]]:
    """Wartości od najczęstszej: każda WYBIERA z listy dotychczasowych kotwic swojej
    cechy tę, która znaczy to samo (albo „żadna” → sama zostaje kotwicą). Dla
    rodzaju zabiegu w tym samym wywołaniu wybiera też zabieg ogólniejszy, którego
    jest odmianą (drzewo rodzajów). Wybór zamiast par: model porównuje opcje ze
    sobą i nie zależy od tego, czy kod zgadł kandydata po pisowni.
    Próg: wybrana opcja ≥ 0,5 = większość masy prawdopodobieństwa."""
    wynik: dict[str, dict[str, dict]] = {}
    tok = [0]

    async def cecha_run(cecha: str, wart: list[str]) -> None:
        co = CECHY[cecha][0]
        kotwice: list[str] = []
        mapa: dict[str, dict] = {}
        wart = sorted(wart, key=lambda v: -liczn[v])
        for i in range(0, len(wart), ROWNOLEGLE):
            snap = list(kotwice)

            async def jedna(v: str) -> tuple[str, dict]:
                if not snap:
                    return v, {"kotwica": v, "p": 1.0, "rodzic": None}
                opcje = {k: {"examples": przyk.get(k, [])[:2]} for k in snap[-(MAX_OPCJI):]}
                q: dict[str, Any] = {
                    "syn": Choice(
                        instructions=f"Która z wartości oznacza dokładnie to samo co `wartosc` (cecha: {co}) — synonim, inna pisownia, liczba mnoga?",
                        criteria={**opcje, "zadna": {"what": "żadna z wartości nie oznacza tego samego"}},
                    )
                }
                if cecha == "rodzaj_zabiegu":
                    q["rodzic"] = Choice(
                        instructions="Którego ogólniejszego zabiegu odmianą lub szczególnym przypadkiem jest `wartosc`?",
                        criteria={**opcje, "zaden": {"what": "nie jest odmianą żadnego z wymienionych zabiegów"}},
                    )
                r = await client.system_one({"wartosc": v, "przyklady_uslug": przyk.get(v, [])[:3]}, q, model=MODEL)
                tok[0] += r.usage.input_tokens or 0
                sy = r.choices["syn"]
                p_sy = sy.probabilities.get(sy.choice, 0.0)
                kot = sy.choice if sy.choice != "zadna" and p_sy >= 0.5 else v
                rodzic = None
                if "rodzic" in r.choices:
                    ro = r.choices["rodzic"]
                    if ro.choice != "zaden" and ro.probabilities.get(ro.choice, 0.0) >= 0.5 and ro.choice != kot:
                        rodzic = ro.choice
                return v, {"kotwica": kot, "p": round(p_sy, 3), "rodzic": rodzic}

            try:
                wyniki = await asyncio.gather(*[jedna(v) for v in wart[i : i + ROWNOLEGLE]])
            except Exception as e:  # noqa: BLE001
                print(f"krok2 {cecha}: {type(e).__name__}: {str(e)[:100]}")
                continue
            for v, w in wyniki:
                mapa[v] = w
                if w["kotwica"] == v:
                    kotwice.append(v)
        wynik[cecha] = mapa
        print(f"  {cecha:<20} wartości {len(wart):>4} → kotwic {len(kotwice):>4}", flush=True)

    await asyncio.gather(*[cecha_run(c, w) for c, w in grupy.items()])
    print(f"krok2 tokeny {tok[0]} ({tok[0] * 0.042 / 1e6:.3f} USD)")
    return wynik


async def domknij(client: Any, mapy: dict[str, dict[str, dict]], liczn: Counter,
                  przyk: dict[str, list[str]]) -> int:
    """Kotwica wybiera spośród kotwic CZĘSTSZYCH od siebie. Naprawia wartości
    z tej samej równoległej paczki (w pierwszym przebiegu nie widziały się
    nawzajem — pierwsza paczka każdej cechy szła przy pustej liście kotwic)."""
    sem = asyncio.Semaphore(ROWNOLEGLE)
    zmian = [0]
    tok = [0]

    async def jedna(cecha: str, v: str, wyzsze: list[str]) -> None:
        co = CECHY[cecha][0]
        opcje = {k: {"examples": przyk.get(k, [])[:2]} for k in wyzsze[:MAX_OPCJI]}
        q = {"syn": Choice(
            instructions=f"Która z wartości oznacza dokładnie to samo co `wartosc` (cecha: {co}) — synonim, inna pisownia, liczba mnoga?",
            criteria={**opcje, "zadna": {"what": "żadna z wartości nie oznacza tego samego"}},
        )}
        async with sem:
            try:
                r = await client.system_one({"wartosc": v, "przyklady_uslug": przyk.get(v, [])[:3]}, q, model=MODEL)
            except Exception as e:  # noqa: BLE001
                print(f"domknij {v!r}: {type(e).__name__}: {str(e)[:100]}")
                return
        tok[0] += r.usage.input_tokens or 0
        sy = r.choices["syn"]
        if sy.choice != "zadna" and sy.probabilities.get(sy.choice, 0.0) >= 0.5:
            mapy[cecha][v] = {**mapy[cecha][v], "kotwica": sy.choice, "p": round(sy.probabilities[sy.choice], 3), "domkniete": True}
            zmian[0] += 1

    zadania = []
    for cecha, mapa in mapy.items():
        kotwice = sorted((v for v, w in mapa.items() if w["kotwica"] == v), key=lambda v: -liczn[v])
        for i, v in enumerate(kotwice[1:], start=1):
            zadania.append(jedna(cecha, v, kotwice[:i]))
    await asyncio.gather(*zadania)
    print(f"domknięcie: {zmian[0]} sklejeń, {tok[0] * 0.042 / 1e6:.3f} USD", flush=True)
    return zmian[0]


def zloz_kotwice(mapy: dict[str, dict[str, dict]], liczn: Counter) -> dict:
    wynik: dict[str, dict] = {}
    for cecha, mapa in mapy.items():
        grupy: dict[str, list[str]] = defaultdict(list)
        rodzice: dict[str, str] = {}
        for v, w in mapa.items():
            k = w["kotwica"]
            while mapa.get(k, {}).get("kotwica", k) != k:
                k = mapa[k]["kotwica"]
            grupy[k].append(v)
            if w.get("rodzic") and k not in rodzice:
                rodzice[k] = w["rodzic"]
        wynik[cecha] = {
            k: {"synonimy": sorted(v), "n": sum(liczn[x] for x in v), "rodzic": rodzice.get(k)}
            for k, v in sorted(grupy.items(), key=lambda kv: -sum(liczn[x] for x in kv[1]))
        }
    return wynik


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    wartosci, przyk, _ = zbierz(Path(args.csv))
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    p1 = out / "krok1_cechy.json"
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        przyp = json.loads(p1.read_text(encoding="utf-8")) if p1.exists() else {}
        brak = {kv: n for kv, n in wartosci.items() if f"{kv[0]}|{kv[1]}" not in przyp}
        if brak:
            przyp.update(await krok1(client, brak, przyk))
            p1.write_text(json.dumps(przyp, ensure_ascii=False, indent=1), encoding="utf-8")
        tok1 = sum(v["tokeny"] for v in przyp.values())
        print(f"krok1: {len(przyp)} wartości, {tok1 * 0.042 / 1e6:.3f} USD; cechy: "
              + ", ".join(f"{k}={v}" for k, v in Counter(v['cecha'] for v in przyp.values()).most_common()), flush=True)
        if args.krok < 2:
            return
        # wartość → cecha (jedna wartość z kilku osi surowych: cecha najczęstsza)
        liczn: Counter = Counter()
        glosy: dict[str, Counter] = defaultdict(Counter)
        przyk_v: dict[str, list[str]] = defaultdict(list)
        for (k, v), n in wartosci.items():
            c = przyp.get(f"{k}|{v}", {}).get("cecha")
            if not c or c == "nie_cecha":
                continue
            glosy[v][c] += n
            liczn[v] += n
            przyk_v[v] = (przyk_v[v] + przyk.get((k, v), []))[:PRZYKLADOW]
        przypis = {v: c.most_common(1)[0][0] for v, c in glosy.items()}
        grupy: dict[str, list[str]] = defaultdict(list)
        for v, c in przypis.items():
            if c not in LICZBOWE:
                grupy[c].append(v)
        p2 = out / "krok2_kotwice.json"
        mapy = json.loads(p2.read_text(encoding="utf-8")) if p2.exists() else await krok2_kotwice(client, grupy, liczn, przyk_v)
        if not mapy.get("_domkniete"):
            await domknij(client, {k: v for k, v in mapy.items() if not k.startswith("_")}, liczn, przyk_v)
            mapy["_domkniete"] = True
        p2.write_text(json.dumps(mapy, ensure_ascii=False), encoding="utf-8")
        mapy = {k: v for k, v in mapy.items() if not k.startswith("_")}
    slownik = zloz_kotwice(mapy, liczn)
    slownik["_liczbowe"] = {c: sorted(v for v, cc in przypis.items() if cc == c) for c in LICZBOWE}
    (out / "slownik.json").write_text(json.dumps(slownik, ensure_ascii=False, indent=1), encoding="utf-8")
    for cecha, d in slownik.items():
        if cecha.startswith("_"):
            continue
        print(f"{cecha:<20} kanonicznych {len(d):>4}  z rodzicem {sum(1 for x in d.values() if x['rodzic']):>3}  "
              f"np: {', '.join(list(d)[:6])}")


def main() -> None:
    p = argparse.ArgumentParser(description="Automatyczny słownik cech usług")
    p.add_argument("--csv", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--krok", type=int, default=2)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
