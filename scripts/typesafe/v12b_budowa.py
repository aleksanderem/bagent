"""Drzewo v12b — etykiety poziomów wybrane przez TypeSafe z fraz każdej usługi, w jej pełnym kontekście.

Po przeglądzie drzewa v12 (28.09): pytanie o rolę frazy bez kontekstu usługi dawało rodzeństwo, które nie było
podziałem („usuwanie” i „brodawek” jako osobne zabiegi), metody z zestawów pod cudzymi zabiegami („pudrowa” pod
regulacją brwi), a jeden poziom „gdzie i ile” mieszał niezależne wymiary („męskie” obok „długie” — usługa bywa
jednym i drugim naraz, więc wybór między nimi nie jest podziałem).

v12b, wzorzec TypeSafe „wybierz, nie generuj” (cookbook pre_parsed_value_extraction):
  A. kod dzieli krótkie pola usługi (nazwa, kategoria w cenniku, zabieg Booksy, etykiety wariantów) na odcinki
     po separatorach („+”, „/”, przecinek, „ i ”, „ z ”, „ na ”…) i słowa;
  B. TypeSafe, jedna usługa na wywołanie, pełny kontekst (także opis i salon): dla KAŻDEGO słowa usługi wybór
     z zamkniętej listy — zabieg, metoda, obszar, wielkość, dla kogo, cel, etap, składnik w cenie, nieistotne;
     niezależne pytania o ten sam stan w jednym wywołaniu; plus tak/nie, czy to zestaw kilku zabiegów (zestawy
     nie budują drzewa — porównuje się je jako zbiory). Pierwsza wersja pytała „która fraza z listy to zabieg /
     metoda / …” (7 pytań) — próba na 40 usługach: model rzadko wybiera „brak”, sam zabieg ląduje w metodzie
     albo celu, ta sama fraza w trzech wymiarach naraz; pytanie o rolę słowa to zwykły wybór z podziału;
  C. kod: sąsiednie słowa odcinka o tej samej roli to jedna fraza („usuwanie” + „brodawek” → „usuwanie brodawek”),
     więc poziomy są rozłączne z definicji — każde słowo ma jedną rolę; zabieg usługi = pierwsza fraza zabiegu
     z nazwy, a gdy nazwa go nie ma — z zabiegu Booksy, kategorii, wariantów; etykieta wchodzi do drzewa, gdy
     wybrały ją usługi ≥ 3 różnych salonów (ten sam próg co wycena); etykieta pod zabiegiem należy do jednego
     wymiaru — tego, w którym wybrało ją najwięcej salonów;
  D. TypeSafe tak/nie: synonimy zabiegów w dziedzinie (każda para zabiegów z ≥ 10 salonów, pozostałe przy wspólnym
     rdzeniu albo podobnym zapisie) i opcji pod zabiegiem (wspólny rdzeń albo podobny zapis); grupy łączą się tylko,
     gdy żadna oceniona para między nimi nie jest „różne”.

Poziom „gdzie i ile” z modelu Alexa to cztery pytania: obszar, wielkość (długość, rozmiar, gęstość, liczba),
dla kogo oraz cel (na jaki problem, po jaki efekt) — cel dopisany, żeby efekty nie udawały zabiegów; do potwierdzenia.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v12b_budowa.py --budzet 1.5 [--limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import json
import re
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v12_budowa as vb  # noqa: E402

from services.typesafe_drzewo.drzewo_v12 import FACETY  # noqa: E402
from services.typesafe_drzewo.grupy_uslug import GRUPY  # noqa: E402
from services.typesafe_drzewo.klasyfikacja import PYTANIA_WSPOLNE  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, RetryPolicy  # noqa: E402

DZ = vb.DZ
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_SALONOW = 3
CZESTE_SALONOW = 10  # synonimy każdej pary tylko dla częstych zabiegów — to ich rozbicie najbardziej obniża pokrycie
MAX_SLOW = 18
PRZYKLADOW = 6
ROWNOLEGLE = 30
TOK_USLUGA = 4200  # szacunek: ~14 słów × pytanie o rolę z opisami ról + stan
ZRODLA = ("nazwa", "zabieg_booksy", "kategoria", "wariant")  # kolejność: skąd brać zabieg usługi
SEPARATOR = re.compile(r"\s[-–—]\s|[+/&,;|()\[\]–—]|\s(?:i|oraz|lub|albo|z|ze|w|we|na|do|dla|od|po|u)\s", re.I)

ROLE_SLOWA: dict[str, dict] = {
    "zabieg": {"what": "co się robi — nazwa zabiegu", "examples": ["strzyżenie", "henna", "depilacja", "masaż", "manicure", "usuwanie", "przedłużanie"]},
    "metoda": {"what": "czym albo jak: technika, urządzenie, preparat, styl", "examples": ["laserowa", "woskiem", "hybrydowy", "kobido", "ipl", "hialuronowym"]},
    "obszar": {"what": "gdzie: część ciała albo to, na czym się pracuje", "examples": ["pachy", "twarzy", "brwi", "paznokci", "włosów", "stóp"]},
    "wielkosc": {"what": "ile: długość, rozmiar, gęstość, liczba, objętość", "examples": ["długie", "krótkie", "2:1", "całe", "mały", "1 ml"]},
    "dla_kogo": {"what": "dla kogo: płeć, wiek, rodzaj klienta albo zwierzęcia", "examples": ["damskie", "męskie", "dziecięce", "york", "kota"]},
    "cel": {"what": "na co: problem albo efekt, który zabieg ma dać", "examples": ["przebarwienia", "trądzik", "brodawki", "ujędrniający", "relaksacyjny"]},
    "etap": {"what": "która wizyta w cyklu zabiegu", "examples": ["uzupełnienie", "założenie", "zdjęcie", "korekta", "kontrola"]},
    "skladnik": {"what": "dodatek w cenie obok zabiegu", "examples": ["maska", "ampułka", "zdobienie", "mycie", "opatrunek"]},
    "nieistotne": {"what": "nic nie mówi o usłudze: marketing, imię, nazwa salonu, słowo ogólne", "examples": ["premium", "promocja", "zabieg", "usługa", "Kasia", "standard"]},
}


def teksty_zrodel(u: dict) -> list[tuple[str, str]]:
    out = [("nazwa", u.get("nazwa") or ""), ("zabieg_booksy", u.get("zabieg_booksy") or ""), ("kategoria", u.get("kategoria") or "")]
    return out + [("wariant", w["label"]) for w in u.get("warianty") or [] if w.get("label")]


def odcinki(u: dict) -> list[tuple[str, list[str]]]:
    """(źródło, słowa odcinka) — separatory rozdzielają odcinki, więc sąsiedztwo nie przechodzi przez „+” czy „ i ”."""
    out = []
    for zr, t in teksty_zrodel(u):
        for kawalek in SEPARATOR.split(f" {t} "):
            tk = vb.tokeny(kawalek)
            if tk:
                out.append((zr, tk))
    return out


def slowa(u: dict) -> list[str]:
    out: list[str] = []
    for _zr, tk in odcinki(u):
        for w in tk:
            if w not in out:
                out.append(w)
    return out[:MAX_SLOW]


def pytania(sl: list[str]) -> dict[str, Any]:
    q: dict[str, Any] = {f"r{i}": Choice(instructions=f"Czym jest słowo „{w}” w usłudze `usluga`? Rozstrzyga cała usługa: "
                                                        "nazwa, kategoria, opis, warianty i zabieg z Booksy.", criteria=ROLE_SLOWA)
                         for i, w in enumerate(sl)}
    return {**q, "zestaw": PYTANIA_WSPOLNE["zestaw"]}


def frazy_rol(u: dict, role: dict[str, list]) -> dict[str, list[tuple[str, str]]]:
    """Sąsiednie słowa odcinka o tej samej roli = jedna fraza. → rola: [(fraza, źródło)]."""
    out: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for zr, tk in odcinki(u):
        biezaca: list[str] = []
        rola_b = None
        for w in tk + [None]:
            r = role[w][0] if w is not None and w in role else None
            if biezaca and r != rola_b:
                if rola_b not in (None, "nieistotne", "skladnik") and (" ".join(biezaca), zr) not in out[rola_b]:
                    out[rola_b].append((" ".join(biezaca), zr))
                biezaca = []
            if r is not None:
                biezaca.append(w)
                rola_b = r
    return out


def wybor_uslugi(u: dict, zapis: dict) -> dict[str, Any]:
    """→ {"zabieg": fraza | None, wymiar: [frazy]}; zabieg = pierwsza fraza zabiegu z nazwy, potem z innych źródeł."""
    fr = frazy_rol(u, zapis["role"])
    zab = next((f for zr in ZRODLA for f, z in fr.get("zabieg", []) if z == zr), None)
    return {"zabieg": zab, **{w: sorted({f for f, _z in fr.get(w, [])}) for w in FACETY}}


def podobne(a: str, b: str) -> bool:
    return bool({t[:5] for t in a.split()} & {t[:5] for t in b.split()}) or difflib.SequenceMatcher(None, a, b).ratio() >= 0.7


async def wybierz(client: Any, uslugi: list[dict], pamiec: dict, tok: list, budzet: float) -> None:
    sem = asyncio.Semaphore(ROWNOLEGLE)
    bledy = [0]

    async def jedna(u: dict) -> None:
        sl = slowa(u)
        if not sl:
            return
        async with sem:
            try:
                r = await client.system_one(stan_v12(u), pytania(sl), model=MODEL)
            except Exception as e:  # noqa: BLE001 — jedna usługa bez wyboru, reszta dalej
                if " 402 " in str(e):
                    raise SystemExit(f"TypeSafe: brak kredytów — {str(e)[:160]}") from e
                bledy[0] += 1
                if bledy[0] <= 5:
                    print(f"  {u['nazwa'][:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
                return
        tok[0] += r.usage.input_tokens or 0
        if tok[0] * CENA_TOK > budzet:
            raise SystemExit("budżet wyczerpany")
        pamiec[str(u["id"])] = {"role": {w: [r.choices[f"r{i}"].choice, round(r.choices[f"r{i}"].probabilities[r.choices[f"r{i}"].choice], 3)]
                                         for i, w in enumerate(sl)},
                                "zestaw": round(r.nouls["zestaw"].noul, 3)}
    await asyncio.gather(*[jedna(u) for u in uslugi if str(u["id"]) not in pamiec])
    print(f"wyborów {len(pamiec)}, błędów {bledy[0]}, koszt {tok[0] * CENA_TOK:.3f} USD", flush=True)


async def scal(pytaj: Any, g: str, rola: str, etykiety: dict[str, dict], kazda_para: bool) -> dict[str, str]:
    """→ etykieta: reprezentant grupy synonimów (najczęstsza). Łączenie jak w klastrowaniu pełnym: grupy
    łączą się tylko, gdy żadna oceniona para między nimi nie jest „różne”."""
    lista = sorted(etykiety, key=lambda e: -etykiety[e]["n"])
    czeste = {e for e in lista if etykiety[e]["n"] >= CZESTE_SALONOW}
    pary = [(a, b) for i, a in enumerate(lista) for b in lista[i + 1:]
            if (kazda_para and a in czeste and b in czeste) or podobne(a, b)]
    werdykt: dict[tuple[str, str], float] = {}

    async def para(a: str, b: str) -> None:
        r = await pytaj({"dziedzina": g, "rola": rola, "fraza_a": {"fraza": a, "przyklady": etykiety[a]["examples"]},
                         "fraza_b": {"fraza": b, "przyklady": etykiety[b]["examples"]}}, vb.TO_SAMO)
        werdykt[(a, b)] = werdykt[(b, a)] = r.nouls["to_samo"].noul
    await asyncio.gather(*[para(a, b) for a, b in pary])
    grupa = {e: frozenset({e}) for e in lista}
    for a, b in sorted(pary, key=lambda p: -werdykt[p]):
        if werdykt[(a, b)] < 0.5:
            break
        ga, gb = grupa[a], grupa[b]
        if ga == gb or any(werdykt.get((x, y), 1.0) < 0.5 for x in ga for y in gb):
            continue
        for x in ga | gb:
            grupa[x] = ga | gb
    return {e: max(sorted(grupa[e]), key=lambda x: etykiety[x]["n"]) for e in lista}


def etykiety_slotu(uslugi: list[dict], wybory: dict, slot: str) -> dict[str, dict]:
    sal: dict[str, set] = defaultdict(set)
    przyk: dict[str, list[str]] = defaultdict(list)
    for u in uslugi:
        w = wybory[str(u["id"])].get(slot)
        for e in ([w] if isinstance(w, str) else w or []):
            if u["booksy_id"] not in sal[e] and len(przyk[e]) < PRZYKLADOW:
                przyk[e].append(f"{u['nazwa']} [{u.get('kategoria') or '—'}]")
            sal[e].add(u["booksy_id"])
    return {e: {"n": len(s), "examples": przyk[e]} for e, s in sal.items() if len(s) >= MIN_SALONOW}


async def main_async(a: argparse.Namespace) -> None:
    uslugi = [json.loads(x) for x in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    p01 = json.loads((DZ / "poziom01.json").read_text(encoding="utf-8"))
    naj = lambda d: max(d, key=d.get)  # noqa: E731
    uslugi = [u for u in uslugi if (r := p01.get(str(u["id"]))) and naj(r["pozycja"]) in vb.POROWNYWANE
              and r["dziedzina"][naj(r["dziedzina"])] >= 0.5]
    if a.limit:
        uslugi = uslugi[: a.limit]
    plik = DZ / "wybory_v12b.json"
    pamiec: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = [u for u in uslugi if str(u["id"]) not in pamiec]
    print(f"usług {len(uslugi)}, do wyboru {len(brak)}, szac. {len(brak) * TOK_USLUGA * CENA_TOK:.2f} USD (budżet {a.budzet})", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        try:
            await wybierz(client, brak, pamiec, tok, a.budzet)
        finally:
            plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
        if a.tylko_wybory:
            return
        sem = asyncio.Semaphore(ROWNOLEGLE)

        async def pytaj(st: dict, q: dict):
            async with sem:
                r = await client.system_one(st, q, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            if tok[0] * CENA_TOK > a.budzet:
                raise SystemExit("budżet wyczerpany")
            return r
        drzewo = await zbuduj(pytaj, uslugi, pamiec, p01)
    (DZ / "drzewo_v12b.json").write_text(json.dumps(drzewo, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt łącznie {tok[0] * CENA_TOK:.3f} USD")
    raport(drzewo)


def _wybory(uslugi: list[dict], pamiec: dict) -> dict[str, dict]:
    """Zestawy odpadają (porównuje się je jako zbiory zabiegów); usługi bez frazy zabiegu też."""
    out = {}
    for u in uslugi:
        zapis = pamiec.get(str(u["id"]))
        if not zapis or zapis["zestaw"] >= 0.5:
            continue
        w = wybor_uslugi(u, zapis)
        if w["zabieg"]:
            out[str(u["id"])] = w
    return out


async def zbuduj(pytaj: Any, uslugi: list[dict], pamiec: dict, p01: dict) -> dict:
    naj = lambda d: max(d, key=d.get)  # noqa: E731
    wybory = _wybory(uslugi, pamiec)
    po_dz: dict[str, list[dict]] = defaultdict(list)
    for u in uslugi:
        if str(u["id"]) in wybory:
            po_dz[naj(p01[str(u["id"])]["dziedzina"])].append(u)
    zabiegi: dict[str, dict] = {}
    for g, us in po_dz.items():
        et_z = etykiety_slotu(us, wybory, "zabieg")
        rep = await scal(pytaj, g, "zabieg", et_z, kazda_para=True)
        grupy: dict[str, list[dict]] = defaultdict(list)
        for u in us:
            z = wybory[str(u["id"])]["zabieg"]
            if z in rep:
                grupy[rep[z]].append(u)
        zg = {}
        for z, uz in grupy.items():
            wezel: dict[str, Any] = {"n": len({u["booksy_id"] for u in uz}),
                                     "synonimy": sorted(e for e, r in rep.items() if r == z),
                                     "examples": [u["nazwa"] for u in uz[:3]]}
            wymiar = {f: etykiety_slotu(uz, wybory, f) for f in FACETY}
            gdzie = {}  # etykieta należy do wymiaru, w którym wybrało ją najwięcej salonów
            for f, ets in wymiar.items():
                for e, d in ets.items():
                    if e not in gdzie or d["n"] > wymiar[gdzie[e]][e]["n"]:
                        gdzie[e] = f
            for f in FACETY:
                ets = {e: d for e, d in wymiar[f].items() if gdzie[e] == f}
                rep_f = await scal(pytaj, g, f, ets, kazda_para=False) if len(ets) > 1 else {e: e for e in ets}
                wezel[f] = {r: {"n": sum(ets[e]["n"] for e in ets if rep_f[e] == r),
                                "synonimy": sorted(e for e in ets if rep_f[e] == r),
                                "examples": ets[r]["examples"][:3]} for r in set(rep_f.values())}
            zg[z] = wezel
        zabiegi[g] = zg
        print(f"  {g}: zabiegów {len(zg)} (etykiet {len(et_z)})", flush=True)
    return {"wersja": "12b", "grupy": GRUPY, "zabiegi": zabiegi}


def raport(drzewo: dict) -> None:
    for g, zg in drzewo["zabiegi"].items():
        print(f"\n{g}: zabiegów {len(zg)}")
        for z, w in sorted(zg.items(), key=lambda kv: -kv[1]["n"])[:8]:
            osie = " | ".join(f"{f}: {', '.join(sorted(w[f], key=lambda e: -w[f][e]['n'])[:5])}" for f in FACETY if w[f])
            print(f"  {z} ({w['n']} sal.; {', '.join(w['synonimy'][:4])}) | {osie}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v12b: etykiety poziomów wybrane z fraz każdej usługi")
    p.add_argument("--budzet", type=float, default=1.5)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--tylko-wybory", action="store_true", help="tylko etap B (wybory), bez budowy drzewa")
    asyncio.run(main_async(p.parse_args()))
