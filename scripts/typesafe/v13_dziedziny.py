"""Drzewo v13, poziom 1 (dziedzina = czego dotyczy usługa) z danych — próba na rolach słów v12b (bd BEAUTY_AUDIT-asrk).

Model Alexa (28.09): dziedzina = czego dotyczy usługa (paznokcie, włosy, stopy, twarz); zabieg = co się robi.
Jedna zasada podziału na tym poziomie — obiekt usługi, nie czynność. Lista 21 dziedzin pisana ręcznie
(v11–v12b) mieszała obie zasady i rozcięła 68 z 206 zabiegów na kilka gałęzi (np. pedicure: paznokcie 143,
stopy — podologia 56) — to łamało zasady 2 i 5 z cookbooka.

Etykiety z danych: frazy o roli „obszar” z nazw usług (≥ 3 salony, próg jak w wycenie). TypeSafe decyduje:
  a) które etykiety to ten sam obiekt (odmiana, zapis) — pary o wspólnym rdzeniu albo podobnym zapisie;
  b) czy obiekt to DZIEDZINA (cały obszar, którego dotyczy usługa), czy MIEJSCE w obrębie dziedziny — z przykładami
     wprost z tabeli modelu w dokumencie („paznokcie, włosy, stopy, twarz” vs „pachy, całe nogi”), z dowodem:
     najczęstsze zabiegi na obiekcie i przykłady usług (liczy kod);
  c) które dziedziny to to samo innymi słowami (każda para dziedzin; łączenie pełne);
  d) w której dziedzinie leży każde miejsce (wybór spośród dziedzin z danych albo „żadna” → osobna dziedzina).
Pierwsza próba (każda para częstych obiektów: „ta sama dziedzina?”) dała 63 dziedziny — twarz, szyja, dekolt
osobno — bo pytanie o równość części ciała jest ścisłe, a model stawia szyję i pachy na poziomie 4, nie 1.
Nazwa dziedziny = najczęstsza etykieta grupy (wybór z danych, nie nazwa wymyślona).

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_dziedziny.py --budzet 0.2
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v12b_budowa as vb  # noqa: E402

from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, NoulCriteria, RetryPolicy  # noqa: E402

DZ = vb.DZ
OUT = DZ.parent / "v13"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_SALONOW = 3
CZESTE_SALONOW = 10
ROWNOLEGLE = 30

TEN_SAM_OBIEKT = {"to_samo": Noul(
    instructions="Czy `obiekt_a` i `obiekt_b` to ten sam obszar ciała albo obiekt, którego dotyczy usługa — "
                 "tylko inna forma słowa, zapis albo skrót? Przykłady to usługi z cenników różnych salonów.",
    criteria=NoulCriteria(true="ten sam obszar albo obiekt, inna forma słowa lub zapis",
                          false="różne obszary albo obiekty"),
)}
CZY_DZIEDZINA = {"dziedzina": Noul(
    instructions="Czy `obiekt` to dziedzina usług — cały obszar, którego dotyczy usługa, jak paznokcie, włosy, stopy "
                 "albo twarz — czy tylko miejsce w obrębie dziedziny, jak pachy albo całe nogi? Obok: najczęstsze "
                 "zabiegi na tym obiekcie i przykłady usług z cenników różnych salonów.",
    criteria=NoulCriteria(true="dziedzina: cały obszar, którego dotyczą usługi, jak paznokcie, włosy, stopy, twarz",
                          false="miejsce w obrębie dziedziny, jak pachy albo całe nogi"),
)}
TA_SAMA_NAZWA_DZIEDZINY = {"to_samo": Noul(
    instructions="Czy `dziedzina_a` i `dziedzina_b` to ta sama dziedzina usług — ten sam obszar, którego dotyczą "
                 "usługi, tylko innymi słowami? Obok: najczęstsze zabiegi i przykłady usług.",
    criteria=NoulCriteria(true="ta sama dziedzina innymi słowami", false="różne dziedziny"),
)}
TA_SAMA_DZIEDZINA = {"ta_sama": Noul(
    instructions="Czy usługi dotyczące `obiekt_a` i usługi dotyczące `obiekt_b` to ta sama dziedzina usług — "
                 "ta sama część ciała albo obiekt, którego dotyczą, z tymi samymi rodzajami zabiegów? "
                 "Obok każdego obiektu: najczęstsze zabiegi na nim i przykłady usług z cenników różnych salonów.",
    criteria=NoulCriteria(true="jedna dziedzina: ten sam obiekt albo jego część, te same rodzaje zabiegów",
                          false="różne dziedziny: inna część ciała albo obiekt, inne rodzaje zabiegów"),
)}


def obiekty(uslugi: list[dict], pamiec: dict) -> dict[str, dict]:
    """Etykieta obszaru → salony, najczęstsze zabiegi, przykłady (liczy kod)."""
    sal: dict[str, set] = defaultdict(set)
    zab: dict[str, Counter] = defaultdict(Counter)
    przyk: dict[str, list[str]] = defaultdict(list)
    for u in uslugi:
        zapis = pamiec.get(str(u["id"]))
        if not zapis:
            continue
        fr = vb.frazy_rol(u, zapis["role"])
        z = next((f for f, _ in fr.get("zabieg", [])), None)
        for o in {f for f, _ in fr.get("obszar", [])}:
            if u["booksy_id"] not in sal[o] and len(przyk[o]) < 4:
                przyk[o].append(u["nazwa"])
            sal[o].add(u["booksy_id"])
            if z:
                zab[o][z] += 1
    return {o: {"n": len(s), "zabiegi": [z for z, _ in zab[o].most_common(6)], "przyklady": przyk[o]}
            for o, s in sal.items() if len(s) >= MIN_SALONOW}


def podobne(a: str, b: str) -> bool:
    """Kandydaci na odmianę tego samego słowa — decyduje TypeSafe; krótkie słowa odmieniają się w 4 pierwszych literach."""
    return bool({t[:4] for t in a.split()} & {t[:4] for t in b.split()}) or difflib.SequenceMatcher(None, a, b).ratio() >= 0.6


def laczenie_srednie(etykiety: list[str], waga: dict[str, int], werdykt: dict) -> dict[str, str]:
    """Łączenie grup, gdy średnia ocen „to samo” po wszystkich parach między nimi ≥ 0,5 (bardziej tak niż nie).
    Pełne łączenie blokowało grupę jednym „różne” (stomatologia rozbita na 6 dziedzin w próbie 2)."""
    grupy = [frozenset({e}) for e in etykiety]
    while True:
        naj, para = 0.5, None
        for i, a in enumerate(grupy):
            for b in grupy[i + 1:]:
                oc = [werdykt[(x, y)] for x in a for y in b if (x, y) in werdykt]
                if oc and len(oc) == len(a) * len(b) and sum(oc) / len(oc) >= naj:
                    naj, para = sum(oc) / len(oc), (a, b)
        if para is None:
            break
        grupy = [g for g in grupy if g not in para] + [para[0] | para[1]]
    return {e: max(sorted(g), key=lambda x: waga[x]) for g in grupy for e in g}


def laczenie_pelne(etykiety: list[str], waga: dict[str, int], pary: list[tuple[str, str]], werdykt: dict) -> dict[str, str]:
    grupa = {e: frozenset({e}) for e in etykiety}
    for a, b in sorted(pary, key=lambda p: -werdykt[p]):
        if werdykt[(a, b)] < 0.5:
            break
        ga, gb = grupa[a], grupa[b]
        if ga == gb or any(werdykt.get((x, y), 1.0) < 0.5 for x in ga for y in gb):
            continue
        for x in ga | gb:
            grupa[x] = ga | gb
    return {e: max(sorted(grupa[e]), key=lambda x: waga[x]) for e in etykiety}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    uslugi = [json.loads(x) for x in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    pamiec = json.loads((DZ / "wybory_v12b.json").read_text(encoding="utf-8"))
    ob = obiekty(uslugi, pamiec)
    lista = sorted(ob, key=lambda o: -ob[o]["n"])
    pary_syn = [(x, y) for i, x in enumerate(lista) for y in lista[i + 1:] if podobne(x, y)]
    print(f"obiektów {len(lista)}, par do scalenia odmian {len(pary_syn)}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def pytaj(st: dict, q: dict, klucz: str) -> float:
            async with sem:
                r = await client.system_one(st, q, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            if tok[0] * CENA_TOK > a.budzet:
                raise SystemExit("budżet wyczerpany")
            return r.nouls[klucz].noul

        def stan(x: str, y: str) -> dict:
            return {"obiekt_a": {"nazwa": x, **{k: ob[x][k] for k in ("zabiegi", "przyklady")}},
                    "obiekt_b": {"nazwa": y, **{k: ob[y][k] for k in ("zabiegi", "przyklady")}}}

        w_syn: dict = {}

        async def para_syn(x: str, y: str) -> None:
            w_syn[(x, y)] = w_syn[(y, x)] = await pytaj(stan(x, y), TEN_SAM_OBIEKT, "to_samo")
        await asyncio.gather(*[para_syn(x, y) for x, y in pary_syn])
        rep = laczenie_pelne(lista, {o: ob[o]["n"] for o in lista}, pary_syn, w_syn)
        # obiekt po scaleniu odmian: salony sumuje kod, zabiegi i przykłady z reprezentanta
        scal: dict[str, dict] = {}
        for o in lista:
            r = rep[o]
            d = scal.setdefault(r, {"n": 0, "odmiany": [], **{k: ob[r][k] for k in ("zabiegi", "przyklady")}})
            d["n"] += ob[o]["n"]
            d["odmiany"].append(o)
        glowne = sorted(scal, key=lambda o: -scal[o]["n"])
        ob.update({o: {**ob[o], "n": scal[o]["n"]} for o in glowne})

        def opis(o: str) -> dict:
            return {"nazwa": o, **{k: ob[o][k] for k in ("zabiegi", "przyklady")}}

        czy_dz: dict[str, float] = {}

        async def dziedzina(o: str) -> None:
            czy_dz[o] = await pytaj({"obiekt": opis(o)}, CZY_DZIEDZINA, "dziedzina")
        await asyncio.gather(*[dziedzina(o) for o in glowne])
        korzenie = [o for o in glowne if czy_dz[o] >= 0.5]
        pary_k = [(x, y) for i, x in enumerate(korzenie) for y in korzenie[i + 1:]]
        print(f"po scaleniu odmian: {len(glowne)} obiektów; dziedzin wg TypeSafe {len(korzenie)}, par dziedzin {len(pary_k)}; "
              f"koszt dotąd {tok[0] * CENA_TOK:.3f} USD", flush=True)
        w_k: dict = {}

        async def para_k(x: str, y: str) -> None:
            w_k[(x, y)] = w_k[(y, x)] = await pytaj({"dziedzina_a": opis(x), "dziedzina_b": opis(y)}, TA_SAMA_NAZWA_DZIEDZINY, "to_samo")
        await asyncio.gather(*[para_k(x, y) for x, y in pary_k])
        rep_k = laczenie_srednie(korzenie, {o: ob[o]["n"] for o in korzenie}, w_k)
        dziedziny = sorted(set(rep_k.values()), key=lambda o: -ob[o]["n"])
        q_rodzic = {"rodzic": Choice(
            instructions="W której dziedzinie usług leży miejsce `obiekt` — częścią której z tych dziedzin jest?",
            criteria={**{d: {"examples": ob[d]["przyklady"][:3]} for d in dziedziny},
                      "zadna": "żadna z tych dziedzin — to osobny obszar usług"})}
        rodzic: dict[str, str] = {o: rep_k[o] for o in korzenie}

        async def miejsce(o: str) -> None:
            async with sem:
                r = await client.system_one({"obiekt": opis(o)}, q_rodzic, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            rodzic[o] = r.choices["rodzic"].choice
        await asyncio.gather(*[miejsce(o) for o in glowne if o not in rodzic])
    osobne = [o for o, r in rodzic.items() if r == "zadna"]
    for o in osobne:
        rodzic[o] = o
    grupy: dict[str, list[str]] = defaultdict(list)
    for o in glowne:
        grupy[rodzic[o]].append(o)
    wynik = {"dziedziny": {g: {"miejsca": sorted(v, key=lambda o: -ob[o]["n"]), "salony_obiektow": sum(ob[o]["n"] for o in v),
                               "z_pytania": g in korzenie}
                           for g, v in grupy.items()},
             "rodzic": rodzic, "odmiany": {o: scal[o]["odmiany"] for o in glowne},
             "czy_dziedzina": {o: round(p, 3) for o, p in czy_dz.items()},
             "ta_sama_dziedzina": {f"{x} | {y}": round(w_k[(x, y)], 3) for x, y in pary_k},
             "koszt_usd": round(tok[0] * CENA_TOK, 4)}
    (OUT / "dziedziny_proba3.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt {tok[0] * CENA_TOK:.3f} USD; dziedzin {len(grupy)} (z pytania {len(dziedziny)}, osobnych miejsc {len(osobne)})")
    for g, d in sorted(wynik["dziedziny"].items(), key=lambda kv: -kv[1]["salony_obiektow"]):
        print(f"  {g:18} ({d['salony_obiektow']:4} sal.{'' if d['z_pytania'] else ', osobne'}): {', '.join(d['miejsca'][:14])}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v13: dziedziny (czego dotyczy usługa) z danych")
    p.add_argument("--budzet", type=float, default=0.2)
    asyncio.run(main_async(p.parse_args()))
