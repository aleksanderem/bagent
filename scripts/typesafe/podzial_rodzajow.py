"""Lista rodzajów zabiegów jako PODZIAŁ — automatycznie, tak samo dla każdej branży (bd BEAUTY_AUDIT-qbe4).

Wybór z listy działa tylko, gdy opcje się wykluczają. Schemat v7 (schemat_osi.py)
miał na liście synonimy, rodzaj ogólny obok szczegółowego i zestawy — identyczna
nazwa usługi dostawała różny rodzaj w 10% przypadków (miara bez etykiet:
zgodność rodzaju dla tej samej nazwy w różnych salonach).

Kroki:
  1. zestawy (kod): rodzaj, którego nazwa dzieli się separatorem (",", "+", "/",
     "&", " i ", " z ", " oraz ") na części będące rodzajami z listy → poza listą;
     zestawy obsługuje osobne pytanie przy destylacji;
  2. kandydaci par (kod): pary mylone przy identycznych nazwach (≥ MIN_MYLONYCH
     w destylacjach v7) + pary ogólny/szczegółowy (słowa jednej ⊂ słowa drugiej)
     + relacje rodzic–dziecko ze schematu v7;
  3. relacja (TypeSafe, wybór): ten sam zabieg / a odmianą b / b odmianą a / różne,
     z przykładowymi nazwami usług po obu stronach;
  4. złożenie (kod): „ten sam” z p ≥ 0,8 → sklejenie (pomyłka sklejenia jest
     kosztowna — próg z przykładu dokumentacji); odmiana z p ≥ 0,5 → rodzic
     (skutkiem jest tylko „niepełne” zamiast „różne/ta sama”); cykle przerwane,
     przy kilku rodzicach wygrywa najpewniejszy.

Wynik: podzial.json — kanon (stary rodzaj → kanoniczny), rodzic, korzeń,
zestawy, przykłady nazw, cechy (suma cech sklejonych rodzajów), branże → korzenie.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/podzial_rodzajow.py [--dane <katalog>] [--out <plik>]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from typesafe_sdk import AsyncTypeSafeClient, Choice, RetryPolicy

MODEL = "jev-1.13.0"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
DANE = Path(__file__).resolve().parent / "dane" / "2026-09-25"
MIN_MYLONYCH = 3
TAK_SKLEJ = 0.8
TAK_RODZIC = 0.5
PAR_NA_WYWOLANIE = 25
ROWNOLEGLE = 6
PRZYKLADOW = 4
SEPARATORY = re.compile(r"\s*(?:,|\+|/|&|\bi\b|\bz\b|\boraz\b)\s*")
SLOWA_POMIJANE = {"i", "z", "w", "na", "do", "oraz", "dla", "ze", "-"}


def slowa(s: str) -> set[str]:
    return {w for w in re.split(r"[\s,+/&()-]+", s.lower()) if w and w not in SLOWA_POMIJANE}


def przyklady(destylacje: dict) -> dict[str, list[str]]:
    """rodzaj → najczęstsze nazwy usług, którym destylacja v7 przypisała ten rodzaj."""
    licz: dict[str, Counter] = defaultdict(Counter)
    for k, v in destylacje.items():
        licz[v["rodzaj"]][k.split("|")[0]] += 1
    return {r: [n for n, _ in c.most_common(PRZYKLADOW)] for r, c in licz.items()}


def rdzenie(s: str) -> frozenset[str]:
    """Rdzenie słów (pierwsze 5 liter) — polska odmiana: „modelowaniem” = „modelowanie”."""
    return frozenset(w[:5] for w in slowa(s))


def zestawy(rodzaje: set[str]) -> dict[str, list[str]]:
    po_rdzeniach: dict[frozenset[str], str] = {}
    for r in sorted(rodzaje, key=len):
        po_rdzeniach.setdefault(rdzenie(r), r)
    out = {}
    for r in rodzaje:
        czesci = [c.strip() for c in SEPARATORY.split(r) if c.strip()]
        trafione = [po_rdzeniach.get(rdzenie(c)) for c in czesci]
        if len(czesci) >= 2 and all(t and t != r for t in trafione):
            out[r] = trafione
    return out


def mylone(destylacje: dict) -> Counter:
    grupy: dict[str, list[str]] = defaultdict(list)
    for k, v in destylacje.items():
        grupy[k.split("|")[0]].append(v["rodzaj"])
    c: Counter = Counter()
    for rs in grupy.values():
        for i in range(len(rs)):
            for j in range(i + 1, len(rs)):
                if rs[i] != rs[j] and "inny" not in (rs[i], rs[j]):
                    c[tuple(sorted((rs[i], rs[j])))] += 1
    return c


def kandydaci(rodzaje: set[str], myl: Counter, rodzice: dict[str, str]) -> list[tuple[str, str]]:
    pary = {p for p, n in myl.items() if n >= MIN_MYLONYCH and p[0] in rodzaje and p[1] in rodzaje}
    sl = {r: slowa(r) for r in rodzaje}
    lista = sorted(rodzaje)
    for i, a in enumerate(lista):
        for b in lista[i + 1 :]:
            if sl[a] and sl[b] and (sl[a] < sl[b] or sl[b] < sl[a]):
                pary.add((a, b))
    for dz, ro in rodzice.items():
        if dz in rodzaje and ro in rodzaje and dz != ro:
            pary.add(tuple(sorted((dz, ro))))
    return sorted(pary)


KRYTERIA = {
    "ten_sam": {"what": "ten sam zabieg nazwany inaczej: synonim, skrót, inna pisownia, inna kolejność słów",
                "not_for": "zabieg ogólny i jego odmiana; dwa różne zabiegi"},
    "a_odmiana_b": {"what": "`a` jest odmianą lub szczególnym przypadkiem zabiegu `b` (b jest ogólniejsze)",
                    "not_for": "synonimy; dwa różne zabiegi"},
    "b_odmiana_a": {"what": "`b` jest odmianą lub szczególnym przypadkiem zabiegu `a` (a jest ogólniejsze)",
                    "not_for": "synonimy; dwa różne zabiegi"},
    "rozne": {"what": "dwa różne zabiegi — żaden nie jest odmianą drugiego",
              "not_for": "synonimy; zabieg ogólny i jego odmiana"},
}


async def relacje(client: Any, pary: list[tuple[str, str]], przyk: dict[str, list[str]]) -> tuple[list[dict], int]:
    sem = asyncio.Semaphore(ROWNOLEGLE)
    out: list[dict] = []
    tok = [0]

    async def paczka(ps: list[tuple[str, str]]) -> None:
        q = {
            str(i): Choice(
                instructions={"a": a, "przyklady_uslug_a": przyk.get(a, []), "b": b, "przyklady_uslug_b": przyk.get(b, []),
                              "question": "Jaka jest relacja między zabiegiem `a` i zabiegiem `b` w cennikach salonów beauty?"},
                criteria=KRYTERIA,
            )
            for i, (a, b) in enumerate(ps)
        }
        async with sem:
            try:
                r = await client.system_one({"kontekst": "cenniki salonów beauty w Polsce"}, q, model=MODEL)
            except Exception as e:  # noqa: BLE001
                print(f"relacje: {type(e).__name__}: {str(e)[:120]}")
                return
        tok[0] += r.usage.input_tokens or 0
        for i, (a, b) in enumerate(ps):
            c = r.choices[str(i)]
            out.append({"a": a, "b": b, "relacja": c.choice, "p": round(c.probabilities.get(c.choice, 0.0), 3),
                        "rozklad": {k: round(v, 3) for k, v in c.probabilities.items()}})

    await asyncio.gather(*[paczka(pary[i : i + PAR_NA_WYWOLANIE]) for i in range(0, len(pary), PAR_NA_WYWOLANIE)])
    return out, tok[0]


def zloz(rodzaje: set[str], rel: list[dict], liczn: dict[str, int], zest: dict[str, list[str]]) -> dict[str, Any]:
    ojciec: dict[str, str] = {}

    def find(x: str) -> str:
        while ojciec.get(x, x) != x:
            x = ojciec[x]
        return x

    for e in sorted(rel, key=lambda e: -e["p"]):
        if e["relacja"] == "ten_sam" and e["p"] >= TAK_SKLEJ:
            ra, rb = find(e["a"]), find(e["b"])
            if ra != rb:
                gl, pod = (ra, rb) if liczn.get(ra, 0) >= liczn.get(rb, 0) else (rb, ra)
                ojciec[pod] = gl
    kanon = {r: find(r) for r in rodzaje}
    kandydaci_rodzica: dict[str, list[tuple[float, str]]] = defaultdict(list)
    for e in rel:
        if e["p"] < TAK_RODZIC or e["relacja"] not in ("a_odmiana_b", "b_odmiana_a"):
            continue
        dz, ro = (e["a"], e["b"]) if e["relacja"] == "a_odmiana_b" else (e["b"], e["a"])
        dz, ro = kanon.get(dz, dz), kanon.get(ro, ro)
        if dz != ro:
            kandydaci_rodzica[dz].append((e["p"], ro))
    rodzic: dict[str, str] = {}
    for dz, lst in kandydaci_rodzica.items():
        for p, ro in sorted(lst, reverse=True):
            # przerwij cykl: nie dopinaj, jeśli ro ma już dz wśród przodków
            x, przodkowie = ro, set()
            while x in rodzic and x not in przodkowie:
                przodkowie.add(x)
                x = rodzic[x]
            przodkowie.add(x)
            if dz not in przodkowie:
                rodzic[dz] = ro
                break

    def korzen(x: str) -> str:
        seen = set()
        while x in rodzic and x not in seen:
            seen.add(x)
            x = rodzic[x]
        return x

    kanoniczne = sorted({kanon[r] for r in rodzaje if r not in zest})
    return {"kanon": kanon, "rodzic": rodzic, "korzen": {k: korzen(k) for k in kanoniczne}, "kanoniczne": kanoniczne}


async def main_async(args: argparse.Namespace) -> None:
    dane = Path(args.dane)
    schemat = json.loads((dane / args.schemat).read_text(encoding="utf-8"))
    destylacje = json.loads((dane / "destylacje_v7.json").read_text(encoding="utf-8"))
    rodzaje = set(schemat["rodzaje"])
    liczn = {r: schemat["rodzaje"][r]["n"] for r in rodzaje}
    przyk = przyklady(destylacje)
    slownik = json.loads((dane / "slownik.json").read_text(encoding="utf-8")).get("rodzaj_zabiegu", {})
    for r in rodzaje:  # rodzaj bez destylacji: przykłady z synonimów słownika
        if not przyk.get(r):
            przyk[r] = [s for s in slownik.get(r, {}).get("synonimy", []) if s != r][:PRZYKLADOW]
    zest = zestawy(rodzaje)
    bez_zest = rodzaje - set(zest)
    myl = mylone(destylacje)
    pary = kandydaci(bez_zest, myl, schemat.get("rodzice", {}))
    print(f"rodzajów {len(rodzaje)}, zestawów {len(zest)}, par-kandydatów {len(pary)} "
          f"(mylonych ≥{MIN_MYLONYCH}: {sum(1 for p, n in myl.items() if n >= MIN_MYLONYCH)})", flush=True)
    plik_rel = Path(args.out).with_name("podzial_relacje.json")
    if plik_rel.exists():
        rel = json.loads(plik_rel.read_text(encoding="utf-8"))
        tok = 0
    else:
        api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
        async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
            rel, tok = await relacje(client, pary, przyk)
        plik_rel.write_text(json.dumps(rel, ensure_ascii=False, indent=1), encoding="utf-8")
    wynik = zloz(bez_zest, rel, liczn, zest)
    kanon = wynik["kanon"]
    cechy: dict[str, dict] = {}
    for r in bez_zest:  # cechy kanonicznego = suma cech sklejonych rodzajów (wartości bez powtórzeń)
        k = kanon[r]
        for c, d in schemat["rodzaje"][r]["cechy"].items():
            dst = cechy.setdefault(k, {}).setdefault(c, {"wartosci": [], "domyslna": d.get("domyslna")})
            dst["wartosci"] += [v for v in d.get("wartosci", []) if v not in dst["wartosci"]]
    przyk_k: dict[str, list[str]] = defaultdict(list)
    for r in sorted(bez_zest, key=lambda r: -liczn.get(r, 0)):
        przyk_k[kanon[r]] += [n for n in przyk.get(r, []) if n not in przyk_k[kanon[r]]]
    branze = {b: sorted({wynik["korzen"].get(kanon.get(r, r), kanon.get(r, r)) for r in rs if r in kanon})
              for b, rs in schemat["branze"].items()}
    out = {**wynik, "zestawy": zest, "przyklady": {k: v[:PRZYKLADOW] for k, v in przyk_k.items()},
           "cechy": cechy, "branze": branze, "liczn": {k: sum(liczn[r] for r in bez_zest if kanon[r] == k) for k in wynik["kanoniczne"]}}
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    korzenie = {v for v in wynik["korzen"].values()}
    print(f"relacje: {Counter(e['relacja'] for e in rel)}; koszt {tok * 0.042 / 1e6:.3f} USD")
    print(f"kanonicznych rodzajów {len(wynik['kanoniczne'])} (z {len(bez_zest)}), korzeni {len(korzenie)}, "
          f"z rodzicem {len(wynik['rodzic'])}; korzeni na branżę: "
          + ", ".join(f"{b}={len(k)}" for b, k in sorted(branze.items(), key=lambda kv: -len(kv[1]))[:8]))


def main() -> None:
    p = argparse.ArgumentParser(description="Lista rodzajów jako podział (automatycznie)")
    p.add_argument("--dane", default=str(DANE))
    p.add_argument("--out", default=str(DANE / "podzial.json"))
    p.add_argument("--schemat", default="schemat_v8.json", help="plik schematu w katalogu --dane")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
