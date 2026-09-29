"""Katalog usług, etap 1 — test paczek GLM (abonament Z.ai, 0 USD): czy paczka ofert w jednym wywołaniu
daje te same cechy co jedna oferta na wywołanie, i czy dwa przebiegi są zgodne.

Oferty: ~300 z usług w moich ocenionych parach (7 zbiorów), po równo z branż, warianty z etykietą osobno.
Warianty: p1 (1 na wywołanie), p1b (to samo drugi raz), p12, p20 (losowo), p20z (po 20 wg zabiegu Booksy).
Zabezpieczenia: zła liczba rekordów = odrzut paczki; frazy tylko z własnej oferty; pierwszy 429 zatrzymuje całość.
Wyniki w pamięci (plik na wariant), przebieg wznawialny.

Klucz: zmienna ZAI_API_KEY albo plik ~/.config/zai/api_key.
Użycie: bagent/.venv/bin/python bagent/scripts/katalog/test_paczek.py [--proba 24] [--warianty p1,p12] [--licz]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts")]
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU, Oferta, oferty_z_uslugi, prompt, waliduj  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402
from services.katalog_uslug.podpis import podpis  # noqa: E402

DZ = B / "scripts" / "typesafe" / "dane" / "2026-09-28"
OUT = B / "scripts" / "katalog" / "dane" / "2026-09-29" / "paczki"
ZBIORY = ["v13_sprawdzian", "v14_sprawdzian", *(f"v14_sprawdzian{i}" for i in range(2, 7))]
KLUCZ = Path.home() / ".config" / "zai" / "api_key"
ROZMIAR = {"p1": 1, "p1b": 1, "p12": 12, "p20": 20, "p20z": 20}
ZIARNO = 20260929


def oferty_probki(n: int) -> list[Oferta]:
    per: dict[str, dict[str, Oferta]] = defaultdict(dict)
    for z in ZBIORY:
        us = {int(k): v for k, v in json.loads((DZ / z / "uslugi.json").read_text(encoding="utf-8"))["uslugi"].items()}
        for q in json.loads((DZ / z / "ocena_claude.json").read_text(encoding="utf-8")):
            for strona in ("a", "b"):
                for o in oferty_z_uslugi(us[int(q[strona])]):
                    per[q["branza"]][o.id] = o
    if n <= 0:  # wszystkie oferty z ocenionych par (do oceny podpisu)
        return sorted({o.id: o for br in per.values() for o in br.values()}.values(), key=lambda o: o.id)
    rng = random.Random(ZIARNO)
    na_branze = -(-n // len(per))
    wynik = [o for br in sorted(per) for o in rng.sample(sorted(per[br].values(), key=lambda x: x.id),
                                                          min(na_branze, len(per[br])))]
    return sorted(rng.sample(wynik, min(n, len(wynik))), key=lambda o: o.id)


def paczki(oferty: list[Oferta], wariant: str) -> list[list[Oferta]]:
    k = ROZMIAR[wariant]
    lst = list(oferty)
    if wariant == "p20z":
        lst.sort(key=lambda o: (normalizuj(o.zabieg_booksy), normalizuj(o.kategoria), o.id))
    elif k > 1:
        random.Random(ZIARNO + k).shuffle(lst)
    return [lst[i:i + k] for i in range(0, len(lst), k)]


class Limit(Exception):
    """429 od Z.ai — zatrzymujemy całość (jak LimitDostawcy w taxonomy_backfill.py)."""


async def przebieg(klient, wariant: str, oferty: list[Oferta], rownolegle: int) -> None:
    import openai
    plik = OUT / f"{wariant}.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    sem = asyncio.Semaphore(rownolegle)
    stop = asyncio.Event()

    async def jedna(i: int, p: list[Oferta]) -> None:
        klucz = str(i)
        if klucz in pamiec and pamiec[klucz].get("odp") is not None or stop.is_set():
            return
        async with sem:
            if stop.is_set():
                return
            t0 = time.monotonic()
            odp, blad = None, None
            for _proba in range(2):  # jedno ponowienie: ~2% odpowiedzi to uszkodzony JSON (pomiar 29.09)
                try:
                    odp, blad = await klient.generate_json(prompt(p), max_tokens=16000), None
                    break
                except openai.RateLimitError as e:
                    stop.set()
                    raise Limit(str(e)[:200]) from e
                except Exception as e:  # noqa: BLE001 — błąd paczki zapisany i policzony, nie cichy
                    odp, blad = None, f"{type(e).__name__}: {str(e)[:200]}"
            pamiec[klucz] = {"ids": [o.id for o in p], "odp": odp, "blad": blad, "sekundy": round(time.monotonic() - t0, 1)}
            plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")

    wyniki = await asyncio.gather(*(jedna(i, p) for i, p in enumerate(paczki(oferty, wariant))), return_exceptions=True)
    limity = [w for w in wyniki if isinstance(w, Limit)]
    if limity:
        raise limity[0]


def rekordy(wariant: str, oferty: list[Oferta]) -> tuple[dict[str, dict], Counter]:
    po_id = {o.id: o for o in oferty}
    plik = OUT / f"{wariant}.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    wynik: dict[str, dict] = {}
    stat: Counter = Counter()
    for v in pamiec.values():
        p = [po_id[i] for i in v["ids"] if i in po_id]
        stat["paczek"] += 1
        if v.get("odp") is None:
            stat["paczka: błąd wywołania"] += 1
            continue
        r, bledy = waliduj(p, v["odp"])
        if not r and bledy:
            stat["paczka odrzucona (liczba rekordów)"] += 1
        stat["rekord: błąd id / nazwy / przeciek"] += len(bledy) if r or not bledy else 0
        wynik.update(r)
    stat["ofert z rekordem"] = len(wynik)
    stat["z nieprzypisanym słowem"] = sum(bool(r["nieprzypisane"]) for r in wynik.values())
    return wynik, stat


def podpis_roboczy(r: dict) -> tuple:
    z = normalizuj((r.get("zabieg") or {}).get("fraza") or "")
    return z, r.get("pozycja"), tuple(sorted((c["rola"], normalizuj(c.get("fraza") or "")) for c in r.get("cechy") or []))


def licz(oferty: list[Oferta], warianty: list[str]) -> None:
    rek = {w: rekordy(w, oferty) for w in warianty}
    for w, (_r, st) in rek.items():
        print(f"{w:<5} " + ", ".join(f"{k}: {v}" for k, v in st.items()))
    if "p1" not in rek:
        return
    baza = rek["p1"][0]
    for w in warianty:
        if w == "p1":
            continue
        inne = rek[w][0]
        wspolne = [i for i in baza if i in inne]
        zgodne = sum(podpis_roboczy(baza[i]) == podpis_roboczy(inne[i]) for i in wspolne)
        role = Counter()
        for i in wspolne:
            for rola in ("zabieg", "metoda", "obszar", "rozmiar", "liczba", "dla_kogo", "etap", "sklad", "sesje"):
                a, b = ([podpis_roboczy(x[i])[0]] if rola == "zabieg" else
                        sorted(f for rr, f in podpis_roboczy(x[i])[2] if rr == rola) for x in (baza, inne))
                role[rola] += a == b
        zgodny_podpis = sum(podpis(baza[i]).zbior == podpis(inne[i]).zbior and
                            bool(podpis(baza[i]).blokady) == bool(podpis(inne[i]).blokady) for i in wspolne)
        print(f"{w} vs p1: wspólnych {len(wspolne)}, zgodny PODPIS {zgodny_podpis} "
              f"({zgodny_podpis / max(len(wspolne), 1):.0%}), zgodny surowy rozkład {zgodne} "
              f"({zgodne / max(len(wspolne), 1):.0%}); " + ", ".join(f"{k} {v / max(len(wspolne), 1):.0%}" for k, v in role.items()))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--proba", type=int, default=300, help="ile ofert (najpierw 24 do obejrzenia)")
    ap.add_argument("--warianty", default="p1,p1b,p12,p20,p20z")
    ap.add_argument("--rownolegle", type=int, default=3)
    ap.add_argument("--licz", action="store_true", help="tylko policz z pamięci, bez wywołań")
    a = ap.parse_args()
    global OUT
    OUT = OUT.parent / f"w{WERSJA_PROMPTU}" / (f"paczki_{a.proba}" if a.proba > 0 else "wszystkie")
    OUT.mkdir(parents=True, exist_ok=True)
    oferty = oferty_probki(a.proba)
    warianty = [w for w in a.warianty.split(",") if w in ROZMIAR]
    print(f"ofert {len(oferty)} z {len({o.typ_salonu for o in oferty})} typów salonów; warianty {warianty}; "
          f"wywołań ≤ {sum(len(paczki(oferty, w)) for w in warianty)}", flush=True)
    if not a.licz:
        klucz = (os.environ.get("ZAI_API_KEY") or (KLUCZ.read_text(encoding="utf-8") if KLUCZ.exists() else "")).strip()
        if not klucz:
            sys.exit(f"Brak klucza Z.ai: ustaw ZAI_API_KEY albo zapisz go w {KLUCZ}")
        from taxonomy_backfill import KlientGLM
        klient = KlientGLM(klucz, temperature=0.0)
        for w in warianty:
            try:
                await przebieg(klient, w, oferty, a.rownolegle)
            except Limit as e:
                print(f"STOP — 429 od Z.ai przy {w}: {e}", flush=True)
                break
            print(f"{w}: gotowe", flush=True)
    licz(oferty, warianty)


if __name__ == "__main__":
    asyncio.run(main())
