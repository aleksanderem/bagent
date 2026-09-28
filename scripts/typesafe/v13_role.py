"""Drzewo v13, etap B: rola każdego słowa usługi w jej pełnym kontekście — role zgodne z modelem Alexa (28.09).

Model: dziedzina (czego dotyczy) → zabieg (co się robi) → metoda (czym, jak) → gdzie i ile (obszar, wielkość,
dla kogo) → etap; plus skład. W v12b rola „cel” nie miała miejsca w modelu i była dopisana bez zgody — tu jej
nie ma. Słowa o problemie albo efekcie („brodawek”, „antycellulitowy”) mają trafić tam, gdzie zmieniają usługę:
do zabiegu albo metody (tabela „Odrzucone”: usunięcie „celu” z porównania pogorszyło 205 par, bo cel odróżnia
usługi — informacja nie może przepaść).

Jedna usługa na wywołanie; w tym samym wywołaniu niezależne pytania o ten sam stan: rola każdego słowa (wybór
z zamkniętej listy ról), czym jest pozycja w cenniku (odsiew tego, czego nie porównujemy), tak/nie: zestaw kilku
zabiegów, dodatek w nazwie. Sąsiednie słowa odcinka o tej samej roli łączy kod (v12b_budowa.frazy_rol).
Próba v12b pokazała, że pytanie o rolę słowa działa, a „która fraza z listy” — nie.

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_role.py --limit 40 --budzet 0.05
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_role.py --od 0 --do 10300 --budzet 2.6
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v12b_budowa as vb  # noqa: E402

from services.typesafe_drzewo.klasyfikacja import PYTANIA_WSPOLNE  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import POZYCJE, stan_v12  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, RetryPolicy  # noqa: E402

DZ = vb.DZ
OUT = DZ.parent / "v13"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
ROWNOLEGLE = 30
TOK_USLUGA = 5300  # z próby v12b: 5029 tok. za role słów + pozycja i dwa tak/nie

ROLE_V13: dict[str, dict] = {
    # Przykłady zabiegu i metody wprost z tabeli modelu w dokumencie (Alex, 28.09). Pierwsza próba z własnymi
    # przykładami dawała tej samej nazwie różne role: „mezoterapia” raz zabieg, raz metoda; „henna” jako metoda.
    "zabieg": {"what": "co się robi — sam zabieg, jak przedłużanie rzęs, henna, depilacja, strzyżenie",
               "not_for": "technika albo odmiana zabiegu, problem albo miejsce, którego zabieg dotyczy",
               "examples": ["przedłużanie", "henna", "depilacja", "strzyżenie", "masaż", "manicure"]},
    "metoda": {"what": "czym albo jak wykonuje się zabieg — odmiana: technika, urządzenie, preparat, styl, "
                       "jak laser albo wosk, henna klasyczna albo pudrowa, hybryda albo żel",
               "not_for": "sam zabieg",
               "examples": ["laserowa", "woskiem", "pudrowa", "hybrydowy", "żelowe", "kobido"]},
    "obszar": {"what": "czego dotyczy albo gdzie: część ciała, obiekt, miejsce, także problem, którego dotyczy zabieg",
               "not_for": "sam zabieg, technika",
               "examples": ["twarzy", "włosów", "paznokci", "pachy", "brwi", "zmarszczek", "auto"]},
    "wielkosc": {"what": "ile: długość, rozmiar, gęstość, liczba, objętość",
                 "examples": ["długie", "krótkie", "2:1", "całe", "mały", "1 ml"]},
    "dla_kogo": {"what": "dla kogo: płeć, wiek, rodzaj klienta albo zwierzęcia",
                 "examples": ["damskie", "męskie", "dziecięce", "york", "kota"]},
    "etap": {"what": "która wizyta w cyklu zabiegu", "examples": ["uzupełnienie", "założenie", "zdjęcie", "korekta", "kontrola"]},
    "skladnik": {"what": "dodatek w cenie obok zabiegu", "examples": ["maska", "ampułka", "zdobienie", "mycie", "opatrunek"]},
    "nieistotne": {"what": "nic nie mówi o usłudze: marketing, imię, nazwa salonu, słowo ogólne",
                   "examples": ["premium", "promocja", "zabieg", "usługa", "Kasia", "standard"]},
}


def pytania(sl: list[str]) -> dict[str, Any]:
    q: dict[str, Any] = {f"r{i}": Choice(instructions=f"Czym jest słowo „{w}” w usłudze `usluga`? Rozstrzyga cała usługa: "
                                                        "nazwa, kategoria, opis, warianty i zabieg z Booksy.", criteria=ROLE_V13)
                         for i, w in enumerate(sl)}
    q["pozycja"] = Choice(instructions="Czym jest pozycja `usluga` w cenniku salonu `salon`? Rozstrzygają nazwa, kategoria, "
                                       "opis i warianty.", criteria=POZYCJE)
    return {**q, **PYTANIA_WSPOLNE}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    uslugi = [json.loads(x) for x in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    if a.limit:
        uslugi = random.Random(a.ziarno).sample(uslugi, a.limit)
    else:
        uslugi = uslugi[a.od: a.do or None]
    plik = OUT / (f"role_v13_proba{a.ziarno}.json" if a.limit else "role_v13.json")
    pamiec: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = [u for u in uslugi if str(u["id"]) not in pamiec and vb.slowa(u)]
    szac = len(brak) * TOK_USLUGA * CENA_TOK
    print(f"usług {len(uslugi)}, do zapytania {len(brak)}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz zakres (--od/--do)")
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok, bledy = [0], [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def jedna(u: dict) -> None:
            sl = vb.slowa(u)
            async with sem:
                try:
                    r = await client.system_one(stan_v12(u), pytania(sl), model=MODEL)
                except Exception as e:  # noqa: BLE001 — jedna usługa bez odpowiedzi, reszta dalej
                    if " 402 " in str(e):
                        raise SystemExit(f"TypeSafe: brak kredytów — {str(e)[:160]}") from e
                    bledy[0] += 1
                    if bledy[0] <= 5:
                        print(f"  {u['nazwa'][:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
                    return
            tok[0] += r.usage.input_tokens or 0
            if tok[0] * CENA_TOK > a.budzet:
                raise SystemExit("budżet wyczerpany")
            ch = r.choices
            pamiec[str(u["id"])] = {
                "role": {w: [ch[f"r{i}"].choice, round(ch[f"r{i}"].probabilities[ch[f"r{i}"].choice], 3)] for i, w in enumerate(sl)},
                "pozycja": {o: round(p, 3) for o, p in ch["pozycja"].probabilities.items() if p >= 0.01},
                **{k: round(r.nouls[k].noul, 3) for k in PYTANIA_WSPOLNE if k in r.nouls}}
        try:
            await asyncio.gather(*[jedna(u) for u in brak])
        finally:
            plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    zrob = max(len(brak) - bledy[0], 1)
    print(f"koszt {tok[0] * CENA_TOK:.3f} USD ({tok[0] / zrob:.0f} tok./usługę), błędów {bledy[0]}, zapisanych {len(pamiec)}")
    if a.limit:
        raport_proby(uslugi, pamiec)


def raport_proby(uslugi: list[dict], pamiec: dict) -> None:
    naj = lambda d: max(d, key=d.get)  # noqa: E731
    print("role:", dict(Counter(r for z in pamiec.values() for r, _p in z["role"].values()).most_common()))
    for u in uslugi:
        z = pamiec.get(str(u["id"]))
        if not z:
            continue
        fr = vb.frazy_rol(u, z["role"])
        osie = " | ".join(f"{k}={','.join(sorted({f for f, _ in v}))}" for k, v in fr.items() if v)
        print(f"- {u['nazwa'][:44]!r} [{(u.get('kategoria') or '')[:18]}] {naj(z['pozycja'])} "
              f"zest={z.get('zestaw', 0):.2f} dod={z.get('rozszerzenie', 0):.2f} :: {osie}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v13: role słów zgodne z modelem, pełny kontekst")
    p.add_argument("--budzet", type=float, default=0.05)
    p.add_argument("--limit", type=int, default=0, help="próba: N losowych usług (osobny plik)")
    p.add_argument("--ziarno", type=int, default=7, help="losowanie próby")
    p.add_argument("--od", type=int, default=0)
    p.add_argument("--do", type=int, default=0)
    asyncio.run(main_async(p.parse_args()))
