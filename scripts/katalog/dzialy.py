"""Katalog usług — rozbiór DZIAŁU cennika (decyzja Alexa 1.10, po raporcie 279).

Model (GLM z abonamentu Z.ai, 0 USD) czyta dział w całości — nagłówek, oferty działu, nazwę i typ salonu — i przypisuje
każdej ofercie frazy, które jej dotyczą, także z nagłówka i nazwy salonu. Zastępuje osobny rozkład kategorii i reguły
podpisu o tym, skąd brać słowa (każdy nowy układ cennika wymagał nowej reguły). Małe działy idą razem w jednym
wywołaniu (≤ 12 ofert, jak paczki p12), duży dział jest cięty na kawałki z tym samym nagłówkiem. Wznawialne.

  python scripts/katalog/dzialy.py --wyjscie sprawdzian12                     # rozbiór ofert zbioru (jak --wyciagnij)
  python scripts/katalog/dzialy.py --wyjscie raport_279 --proba 40 --pokaz    # próba do obejrzenia
Podpisy z tego rozbioru w pomiarach: KATALOG_ROZBIOR=dzial przed sprawdzian7.py / zbierz_raport.py.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug.ekstrakcja import WERSJA_DZIALU, Oferta, prompt_dzialu, waliduj  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402

DANE = B / "scripts" / "katalog" / "dane" / "2026-09-29"
KLUCZ = Path.home() / ".config" / "zai" / "api_key"
OFERT_NA_WYWOLANIE = 12  # jak paczki p12 — test paczek 29.09: 12 ofert w wywołaniu bez straty jakości
PLIK = f"d{WERSJA_DZIALU}.json"
Dzial = tuple[str, str, list[Oferta]]  # (nagłówek, nazwa salonu, oferty)


def dzialy(oferty: list[Oferta], salon_of: dict[str, str]) -> list[Dzial]:
    po: dict[tuple[str, str], list[Oferta]] = defaultdict(list)
    for o in sorted(oferty, key=lambda o: o.id):
        po[(salon_of.get(o.id, ""), normalizuj(o.kategoria))].append(o)
    return [(of[0].kategoria, salon, of[i:i + OFERT_NA_WYWOLANIE])
            for (salon, _k), of in sorted(po.items()) for i in range(0, len(of), OFERT_NA_WYWOLANIE)]


def paczki(dz: list[Dzial]) -> list[list[Dzial]]:
    wynik: list[list[Dzial]] = []
    for d in dz:
        if wynik and sum(len(x[2]) for x in wynik[-1]) + len(d[2]) <= OFERT_NA_WYWOLANIE:
            wynik[-1].append(d)
        else:
            wynik.append([d])
    return wynik


def _klucz(p: list[Dzial]) -> str:
    return hashlib.sha1(json.dumps([[n, s, [o.id for o in of]] for n, s, of in p]).encode()).hexdigest()[:16]


async def przebieg(plik: Path, pk: list[list[Dzial]], rownolegle: int) -> None:
    import openai
    from taxonomy_backfill import KlientGLM
    klient = KlientGLM((os.environ.get("ZAI_API_KEY") or KLUCZ.read_text(encoding="utf-8")).strip(), temperature=0.0)
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    sem, stop = asyncio.Semaphore(rownolegle), asyncio.Event()

    async def jedna(p: list[Dzial]) -> None:
        k = _klucz(p)
        if (pamiec.get(k) or {}).get("odp") is not None or stop.is_set():
            return
        async with sem:
            if stop.is_set():
                return
            t0, odp, blad = time.monotonic(), None, None
            for _proba in range(2):  # jedno ponowienie: ~2% odpowiedzi to uszkodzony JSON (pomiar 29.09)
                try:
                    odp, blad = await klient.generate_json(prompt_dzialu(p), max_tokens=16000), None
                    break
                except openai.RateLimitError:
                    stop.set()  # pierwszy 429 zatrzymuje całość (jak test_paczek)
                    raise
                except Exception as e:  # noqa: BLE001 — błąd wywołania zapisany i policzony, nie cichy
                    odp, blad = None, f"{type(e).__name__}: {str(e)[:200]}"
            pamiec[k] = {"dzialy": [[n, s, [o.id for o in of]] for n, s, of in p], "odp": odp, "blad": blad,
                         "sekundy": round(time.monotonic() - t0, 1)}
            plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")

    wyniki = await asyncio.gather(*(jedna(p) for p in pk), return_exceptions=True)
    if bledy := [w for w in wyniki if isinstance(w, BaseException)]:
        raise bledy[0]


def rekordy(plik: Path, oferty: dict[str, Oferta], salon_of: dict[str, str]) -> tuple[dict[str, dict], Counter]:
    """Rekordy z rozbioru działów, walidowane jak paczki p12 (frazy z oferty, nagłówka albo nazwy salonu)."""
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    wynik: dict[str, dict] = {}
    st: Counter = Counter()
    for v in pamiec.values():
        p = [oferty[i] for _n, _s, ids in v["dzialy"] for i in ids if i in oferty]
        st["wywołań"] += 1
        if v.get("odp") is None:
            st["wywołanie: błąd"] += 1
            continue
        r, bledy = waliduj(p, v["odp"], salony={o.id: salon_of.get(o.id, "") for o in p})
        if not r and bledy:
            st["wywołanie odrzucone (liczba rekordów)"] += 1
        st["rekord: błąd id / nazwy / przeciek"] += len(bledy) if r else 0
        wynik.update({k: {**x, "dzial": True} for k, x in r.items()})
    st["ofert z rekordem"] = len(wynik)
    return wynik, st


def _s7(wyjscie: str):
    spec = importlib.util.spec_from_file_location("sprawdzian7", B / "scripts" / "katalog" / "sprawdzian7.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.OUT = DANE / wyjscie
    return m


def _proba(dz: list[Dzial], n: int) -> list[Dzial]:
    rng, wynik = random.Random(20261001), []
    for d in rng.sample(dz, len(dz)):
        if sum(len(x[2]) for x in wynik) + len(d[2]) > n:
            continue
        wynik.append(d)
    return wynik


def pokaz(s7, rek: dict[str, dict], dz: list[Dzial]) -> None:
    """Rekord działu obok słów podpisu: dotychczasowego (rozbiór p12 + reguły) i z rozbioru działu."""
    _pary, _oferty, _rek, x = s7.podpisy()
    s = x["slownik"]
    for naglowek, salon, of in dz:
        print(f"\n[{naglowek}] — {salon}")
        for o in of:
            r = rek.get(o.id)
            if r is None:
                print(f"  {o.nazwa!r} / {o.wariant!r}: BRAK REKORDU")
                continue
            nowy = s7.podpis(r, s, None, x["slowa"], None, x["wyk"])
            stary = x["pod"].get(o.id)
            kont = [f"{c['rola']}:{c['fraza']}({c.get('zrodlo')})" for c in r.get("cechy") or [] if c.get("zrodlo") not in ("nazwa", "wariant")]
            z = r.get("zabieg") or {}
            print(f"  {o.nazwa[:60]!r} / {o.wariant[:25]!r} → zabieg {z.get('fraza')!r}({z.get('zrodlo')}), poz {r.get('pozycja')}"
                  f"{', z kontekstu: ' + '; '.join(kont) if kont else ''}")
            print(f"      było {sorted(stary.zbior) if stary else '—'}\n      jest {sorted(nowy.zbior)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wyjscie", required=True, help="katalog zbioru w dane/2026-09-29/")
    ap.add_argument("--proba", type=int, default=0, help="tylko ~N ofert, całymi działami (do obejrzenia)")
    ap.add_argument("--z-dzialem", default="", help="z --proba: dołącz działy, których nagłówek zawiera ten tekst")
    ap.add_argument("--rownolegle", type=int, default=4)
    ap.add_argument("--pokaz", action="store_true")
    a = ap.parse_args()
    s7 = _s7(a.wyjscie)
    us, _pary, oferty = s7.pary_ofert()
    salon_of = s7.salon_ofert(us, oferty)
    dz = dzialy(s7.do_wyciagniecia(), salon_of)
    plik = s7.OUT / PLIK
    if a.proba:
        wymuszone = [d for d in dz if a.z_dzialem and normalizuj(a.z_dzialem) in normalizuj(d[0])]
        dz = wymuszone + _proba([d for d in dz if d not in wymuszone], a.proba - sum(len(d[2]) for d in wymuszone))
        plik = s7.OUT / f"d{WERSJA_DZIALU}_proba.json"
    pk = paczki(dz)
    print(f"ofert {sum(len(d[2]) for d in dz)}, działów {len(dz)}, wywołań {len(pk)}", flush=True)
    asyncio.run(przebieg(plik, pk, a.rownolegle))
    rek, st = rekordy(plik, oferty, salon_of)
    print("rozbiór działów: " + ", ".join(f"{k}: {v}" for k, v in st.items()), flush=True)
    if a.pokaz:
        pokaz(s7, rek, dz)


if __name__ == "__main__":
    main()
