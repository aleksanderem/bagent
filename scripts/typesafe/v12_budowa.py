"""Drzewo v12, etap B–D: opcje poziomów 2–5 wybrane z danych Booksy (bd BEAUTY_AUDIT-asrk, 28.09).

Model tej samej usługi (Alex, 28.09): dziedzina → zabieg → metoda → gdzie i ile → etap, plus skład.
Opcje poziomów pochodzą z danych, nie z ręcznych list (TypeSafe wybiera, nie wymyśla):

  B. kod: frazy 1–3 słów z nazw, etykiet wariantów, kategorii w cenniku i zabiegów Booksy usług
     danej dziedziny, używane przez ≥ MIN_SALONOW różnych salonów (termin rynkowy, nie nazewnictwo
     jednego salonu; ten sam próg co wycena: 3 salony);
  C. TypeSafe, wybór per fraza: czy nazywa zabieg, metodę, zakres (gdzie, ile, dla kogo), etap,
     składnik w cenie, czy nic nie mówi o usłudze;
     fraza, której każde słowo jest osobną frazą z rolą, jest złożeniem („henna brwi” = henna + brwi)
     i odpada — poziomy zostają rozłączne;
  D. TypeSafe, tak/nie per para fraz tej samej roli o wspólnym rdzeniu albo podobnym zapisie:
     czy znaczą to samo — synonimy scalone („przedłużanie” = „przedłużanie rzęs”).
  E. drzewo: zabiegi dziedziny = frazy roli „zabieg”; pod każdym zabiegiem metody, zakresy i etapy
     z fraz, które występują w usługach tego zabiegu w ≥ MIN_SALONOW salonach.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v12_budowa.py --budzet 0.6
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.grupy_uslug import GRUPY  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, NoulCriteria, RetryPolicy  # noqa: E402

DZ = Path(__file__).resolve().parent / "dane" / "2026-09-28" / "v12"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_SALONOW = 3
MAX_FRAZ = 300
PRZYKLADOW = 6
ROWNOLEGLE = 30
POZIOMY = ("metoda", "zakres", "etap")
POROWNYWANE = ("zabieg", "pakiet")

TOKEN = re.compile(r"\d+\s*:\s*\d+|#\s*\d+|\d+\s*-\s*\d+\s*(?:d\b|tyg\w*)|\d+\s*(?:d\b|tyg\w*)|\d+(?:[.,]\d+)?\s*(?:ml|cm|szt|stref\w*|okolic\w*|palc\w*|paznok\w*|os\b|osob\w*|osób)|[a-ząćęłńóśźż]{3,}", re.I)
STOP = frozenset("""dla oraz lub bez przy się ale jak the and with usługa usługi usług zabieg zabiegi zabiegu wizyta
wizyty cena ceny gratis promocja promocji nowość nowosc pakiet pakiety karnet cennik oferta oferty
jest są który która które inne inny min minut minuty godz godzina godziny zabiegów zabiegami""".split())

ROLE = Choice(
    instructions="Co fraza `fraza` mówi o usługach z dziedziny `dziedzina`? Przykłady to usługi z cenników różnych salonów.",
    criteria={
        "zabieg": {"what": "nazywa, co się robi — sam zabieg", "not_for": "technika, obszar, etap, marka",
                   "examples": ["strzyżenie", "henna", "przedłużanie", "depilacja", "masaż", "manicure", "mezoterapia"]},
        "metoda": {"what": "czym albo jaką techniką się to robi: metoda, technika, urządzenie, preparat, styl",
                   "not_for": "sam zabieg, obszar ciała",
                   "examples": ["laserowa", "woskiem", "pudrowa", "hybrydowy", "tajski", "klasyczny", "kwasem hialuronowym"]},
        "zakres": {"what": "gdzie, ile albo dla kogo: obszar ciała, długość, rozmiar, gęstość, liczba, płeć lub wiek klienta",
                   "not_for": "technika, etap wizyty",
                   "examples": ["pachy", "całe nogi", "długie", "2:1", "1 ml", "męskie", "dziecięce"]},
        "etap": {"what": "która wizyta w cyklu zabiegu", "not_for": "sam zabieg",
                 "examples": ["założenie", "uzupełnienie", "zdjęcie", "korekta", "pierwsza wizyta", "kontrola"]},
        "skladnik": {"what": "dodatkowy element w cenie usługi", "not_for": "główny zabieg",
                     "examples": ["maska", "ampułka", "zdobienie", "mycie", "opatrunek", "serum"]},
        "nieistotne": {"what": "nie mówi, jaka to usługa: marketing, nazwa salonu, imię pracownika, słowo ogólne",
                       "not_for": "cokolwiek z pozostałych",
                       "examples": ["promocja", "premium", "nowość", "Kasia", "relaks"]},
    },
)
TO_SAMO = {"to_samo": Noul(
    instructions="Czy frazy `fraza_a` i `fraza_b` znaczą to samo w usługach z dziedziny `dziedzina` "
                 "(ta sama rola: `rola`)? Przykłady to usługi z cenników różnych salonów.",
    criteria=NoulCriteria(true="to ta sama rzecz innymi słowami, w innej formie gramatycznej albo skrócie",
                          false="to różne rzeczy — inna metoda, inny obszar, inny etap albo inny zabieg"),
)}


def teksty(u: dict) -> list[str]:
    t = [u.get("nazwa") or "", u.get("kategoria") or "", u.get("zabieg_booksy") or ""]
    return t + [w["label"] for w in u.get("warianty") or [] if w.get("label")]


def tokeny(tekst: str) -> list[str]:
    return [" ".join(t.lower().split()) for t in TOKEN.findall(tekst) if t.lower() not in STOP]


def frazy_uslugi(u: dict) -> set[tuple[str, ...]]:
    out: set[tuple[str, ...]] = set()
    for t in teksty(u):
        tk = tokeny(t)
        for n in (1, 2, 3):
            out |= {tuple(tk[i:i + n]) for i in range(len(tk) - n + 1)}
    return out


def kandydaci(uslugi: list[dict]) -> tuple[list[tuple], dict, dict]:
    salony: dict[tuple, set] = defaultdict(set)
    przyk: dict[tuple, list[str]] = defaultdict(list)
    for u in uslugi:
        for f in frazy_uslugi(u):
            if u["booksy_id"] not in salony[f] and len(przyk[f]) < PRZYKLADOW:
                przyk[f].append(f"{u['nazwa']} [{u.get('kategoria') or '—'}]")
            salony[f].add(u["booksy_id"])
    lista = sorted((f for f, s in salony.items() if len(s) >= MIN_SALONOW), key=lambda f: -len(salony[f]))[:MAX_FRAZ]
    return lista, salony, przyk


def zlozenia(role: dict[tuple, str]) -> set[tuple]:
    """Fraza, której każde słowo jest osobną frazą z rolą, to złożenie — poziomy mają zostać rozłączne."""
    znaczace = {f[0] for f, r in role.items() if len(f) == 1 and r != "nieistotne"}
    return {f for f in role if len(f) > 1 and all(t in znaczace for t in f)}


def pary_do_scalenia(frazy: list[tuple]) -> list[tuple[tuple, tuple]]:
    out = []
    for i, a in enumerate(frazy):
        for b in frazy[i + 1:]:
            sa, sb = " ".join(a), " ".join(b)
            if {t[:5] for t in a} & {t[:5] for t in b} or difflib.SequenceMatcher(None, sa, sb).ratio() >= 0.7:
                out.append((a, b))
    return out


async def main_async(a: argparse.Namespace) -> None:
    uslugi = [json.loads(x) for x in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    p01 = json.loads((DZ / "poziom01.json").read_text(encoding="utf-8"))
    naj = lambda d: max(d, key=d.get)  # noqa: E731
    po_dz: dict[str, list[dict]] = defaultdict(list)
    for u in uslugi:
        r = p01.get(str(u["id"]))
        if r and naj(r["pozycja"]) in POROWNYWANE and r["dziedzina"][naj(r["dziedzina"])] >= 0.5:
            po_dz[naj(r["dziedzina"])].append(u)
    kand = {g: kandydaci(us) for g, us in po_dz.items()}
    n_fraz = sum(len(k[0]) for k in kand.values())
    print(f"dziedzin {len(kand)}, fraz-kandydatów {n_fraz}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    role: dict[str, dict[tuple, str]] = defaultdict(dict)
    scalone: dict[str, dict[str, list]] = {}
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def pytaj(st: dict, q: dict):
            async with sem:
                r = await client.system_one(st, q, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            if tok[0] * CENA_TOK > a.budzet:
                raise SystemExit("budżet wyczerpany")
            return r

        async def rola(g: str, f: tuple) -> None:
            r = await pytaj({"dziedzina": g, "fraza": " ".join(f), "przyklady": kand[g][2][f]}, {"rola": ROLE})
            role[g][f] = r.choices["rola"].choice
        await asyncio.gather(*[rola(g, f) for g, (lista, _s, _p) in kand.items() for f in lista])
        print(f"role: {dict(Counter(r for rg in role.values() for r in rg.values()))}; koszt {tok[0] * CENA_TOK:.3f} USD", flush=True)

        for g, rg in role.items():
            odpada = zlozenia(rg)
            po_roli: dict[str, list[tuple]] = defaultdict(list)
            for f, r in rg.items():
                if r != "nieistotne" and f not in odpada:
                    po_roli[r].append(f)
            rodzic: dict[tuple, tuple] = {f: f for fs in po_roli.values() for f in fs}

            def koniec(f: tuple) -> tuple:
                while rodzic[f] != f:
                    f = rodzic[f]
                return f

            async def para(rola_: str, x: tuple, y: tuple) -> None:
                r = await pytaj({"dziedzina": g, "rola": rola_, "fraza_a": {"fraza": " ".join(x), "przyklady": kand[g][2][x]},
                                 "fraza_b": {"fraza": " ".join(y), "przyklady": kand[g][2][y]}}, TO_SAMO)
                if r.nouls["to_samo"].noul >= 0.5:
                    kx, ky = koniec(x), koniec(y)
                    if kx != ky:
                        sal = kand[g][1]
                        if len(sal[kx]) >= len(sal[ky]):
                            rodzic[ky] = kx
                        else:
                            rodzic[kx] = ky
            await asyncio.gather(*[para(r, x, y) for r, fs in po_roli.items() for x, y in pary_do_scalenia(fs)])
            grupy: dict[str, dict[tuple, list]] = defaultdict(lambda: defaultdict(list))
            for r, fs in po_roli.items():
                for f in fs:
                    grupy[r][koniec(f)].append(f)
            scalone[g] = {r: [[" ".join(k), sorted({" ".join(x) for x in v})] for k, v in gg.items()] for r, gg in grupy.items()}
    drzewo = zbuduj(po_dz, kand, scalone)
    (DZ / "role.json").write_text(json.dumps({g: {" ".join(f): r for f, r in rg.items()} for g, rg in role.items()}, ensure_ascii=False, indent=1), encoding="utf-8")
    (DZ / "drzewo_v12.json").write_text(json.dumps(drzewo, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt łącznie {tok[0] * CENA_TOK:.3f} USD")
    raport(drzewo)


def zbuduj(po_dz: dict, kand: dict, scalone: dict) -> dict:
    """Pod każdym zabiegiem: metody, zakresy i etapy z fraz obecnych w jego usługach w ≥ MIN_SALONOW salonach."""
    zabiegi: dict[str, dict] = {}
    for g, role_g in scalone.items():
        uslugi = po_dz[g]
        frazy_u = [(u, frazy_uslugi(u)) for u in uslugi]
        zg = {}
        for etykieta, synonimy in role_g.get("zabieg", []):
            syn = {tuple(s.split()) for s in synonimy}
            jego = [(u, fr) for u, fr in frazy_u if fr & syn]
            sal = {u["booksy_id"] for u, _ in jego}
            if len(sal) < MIN_SALONOW:
                continue
            wezel = {"n": len(sal), "synonimy": sorted(synonimy), "examples": [u["nazwa"] for u, _ in jego[:3]]}
            for poz in POZIOMY:
                opcje = {}
                for et2, syn2 in role_g.get(poz, []):
                    s2 = {tuple(s.split()) for s in syn2}
                    z = [(u, fr) for u, fr in jego if fr & s2]
                    if len({u["booksy_id"] for u, _ in z}) >= MIN_SALONOW:
                        opcje[et2] = {"n": len({u["booksy_id"] for u, _ in z}), "synonimy": sorted(syn2),
                                      "examples": [u["nazwa"] for u, _ in z[:3]]}
                wezel[poz] = opcje
            zg[etykieta] = wezel
        zabiegi[g] = zg
    skl = {g: [e for e, _ in r.get("skladnik", [])] for g, r in scalone.items()}
    return {"wersja": 12, "grupy": GRUPY, "zabiegi": zabiegi, "skladniki": skl, "scalone": scalone}


def raport(drzewo: dict) -> None:
    for g, zg in drzewo["zabiegi"].items():
        print(f"\n{g}: zabiegów {len(zg)}")
        for z, w in sorted(zg.items(), key=lambda kv: -kv[1]["n"])[:8]:
            print(f"  {z} ({w['n']} sal.) | metoda: {', '.join(list(w['metoda'])[:5])} | zakres: {', '.join(list(w['zakres'])[:6])} | etap: {', '.join(list(w['etap'])[:4])}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v12: opcje poziomów 2–5 z danych Booksy")
    p.add_argument("--budzet", type=float, default=0.6)
    asyncio.run(main_async(p.parse_args()))
