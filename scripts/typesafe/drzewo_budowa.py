"""Budowa drzewa usług v11 z nazw usług — bez bazy, z plików pomiarów (bd BEAUTY_AUDIT-asrk, 28.09).

Górny poziom: grupy z services/typesafe_drzewo/grupy_uslug.py (tym, co się robi — nie kategorie
Booksy). Każda usługa z próby dostaje grupę pytaniem TypeSafe (to samo pytanie co pierwszy krok
przejścia po drzewie). Pod grupą zabiegi i odmiany z taksonomii v9 — węzeł istnieje tam, gdzie
trafiają nazwy jego usług, więc to samo słowo w dwóch grupach to dwa węzły
(przedłużanie paznokci ≠ przedłużanie rzęs).

Reguły (te same dla każdej grupy):
  * węzeł zabiegu wymaga ≥ MIN_USLUG usług w grupie — przy mniej niż 3 nazwach przykłady
    opcji są przypadkowe (ten sam próg co wartości cech w schemat_osi.py);
  * odmiana zagnieżdża się pod zabiegiem tylko wtedy, gdy sam zabieg ma w tej grupie własne
    usługi — inaczej rodzic jest tylko słowem („zabieg” nad „zabieg podologiczny”) i odmiana
    staje się zabiegiem grupy.

Próba: wszystkie usługi z par pomiarów 27.09 (warianty + diagnoza — do porównania i tak
potrzebne) + do NA_RODZAJ usług każdego rodzaju z sieci 26.09 (22 tys. usług).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_budowa.py --budzet 0.8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.drzewo_uslug import WERSJA, pytanie  # noqa: E402
from services.typesafe_drzewo.grupy_uslug import GRUPY  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL, stan  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DANE = Path(__file__).resolve().parent / "dane"
OUT = DANE / "2026-09-28" / "drzewo"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_GRUPA = 1300
ROWNOLEGLE = 30
NA_RODZAJ = 20
MIN_USLUG = 3
PRZYKLADOW = 3
SEED = 20260928
ZRODLA = [DANE / "2026-09-27" / "warianty" / "destylacje_v9.json", DANE / "2026-09-27" / "diagnoza" / "destylacje_v9.json"]
SIEC = DANE / "2026-09-26" / "siec" / "destylacje.json"


def czesci(klucz: str) -> tuple[str, str | None, str | None]:
    n, k, t = (klucz.split("|") + ["", ""])[:3]
    return n, (k or None), (t or None)


def proba(p9: dict) -> dict[str, str]:
    """klucz usługi → rodzaj v9; wszystkie usługi pomiarów + do NA_RODZAJ z sieci na rodzaj."""
    out: dict[str, str] = {}
    for f in ZRODLA:
        for k, r in json.loads(f.read_text(encoding="utf-8")).items():
            if r.get("rodzaj") and r["rodzaj"] != "inny":
                out[k] = p9["kanon"].get(r["rodzaj"], r["rodzaj"])
    siec = json.loads(SIEC.read_text(encoding="utf-8"))
    po_rodzaju: dict[str, list[str]] = defaultdict(list)
    for k, r in siec.items():
        if k not in out and r.get("rodzaj") and r["rodzaj"] != "inny":
            po_rodzaju[p9["kanon"].get(r["rodzaj"], r["rodzaj"])].append(k)
    rng = random.Random(SEED)
    for rodzaj, ks in sorted(po_rodzaju.items()):
        for k in rng.sample(sorted(ks), min(NA_RODZAJ, len(ks))):
            out[k] = rodzaj
    return out


async def grupy_uslug(uslugi: dict[str, str], pamiec: dict, budzet: float) -> float:
    brak = [k for k in uslugi if k not in pamiec]
    szac = len(brak) * TOK_GRUPA * CENA_TOK
    print(f"usług w próbie {len(uslugi)}, bez grupy {len(brak)}, szac. {szac:.2f} USD (budżet {budzet})", flush=True)
    if szac > budzet:
        raise SystemExit("ponad budżet")
    q = {"_": pytanie({"grupy": GRUPY, "zabiegi": {}}, ())}
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def jedna(k: str) -> None:
            async with sem:
                try:
                    r = await client.system_one(stan(*czesci(k), None, None), q, model=MODEL)
                except Exception as e:  # noqa: BLE001 — jedna usługa bez grupy, reszta dalej
                    print(f"  grupa {k[:40]!r}: {type(e).__name__}: {str(e)[:80]}", flush=True)
                    return
            tok[0] += r.usage.input_tokens or 0
            pamiec[k] = {g: round(p, 4) for g, p in r.choices["_"].probabilities.items()}
        await asyncio.gather(*[jedna(k) for k in brak])
    return tok[0] * CENA_TOK


def zbuduj(uslugi: dict[str, str], pamiec: dict, p9: dict) -> dict:
    """Liczy usługi w (grupa, zabieg, odmiana) i składa drzewo wg reguł z nagłówka."""
    korzen, rodzic = p9["korzen"], p9["rodzic"]
    licz: dict[tuple, Counter] = defaultdict(Counter)  # (grupa, korzeń, odmiana|None) → nazwy
    for k, rodzaj in uslugi.items():
        if k not in pamiec:
            continue
        g = max(pamiec[k], key=pamiec[k].get)
        kz = korzen.get(rodzaj, rodzaj)
        licz[(g, kz, rodzaj if rodzaj in rodzic else None)][czesci(k)[0]] += 1
    zabiegi: dict[str, dict] = defaultdict(dict)
    wlasne = {(g, kz): sum(c.values()) for (g, kz, o), c in licz.items() if o is None}
    for (g, kz, o), c in sorted(licz.items(), key=lambda kv: -sum(kv[1].values())):
        n = sum(c.values())
        if n < MIN_USLUG:
            continue
        przyk = [x for x, _ in c.most_common(PRZYKLADOW)]
        if o is None:
            zabiegi[g].setdefault(kz, {"n": 0, "examples": [], "odmiany": {}})
            zabiegi[g][kz].update(n=zabiegi[g][kz]["n"] + n, examples=przyk)
        elif wlasne.get((g, kz), 0) >= MIN_USLUG:
            zabiegi[g].setdefault(kz, {"n": 0, "examples": [], "odmiany": {}})
            zabiegi[g][kz]["odmiany"][o] = {"n": n, "examples": przyk}
        else:
            zabiegi[g].setdefault(o, {"n": 0, "examples": przyk, "odmiany": {}})
            zabiegi[g][o]["n"] += n
    return {"wersja": WERSJA, "grupy": GRUPY, "zabiegi": dict(zabiegi)}


def raport(drzewo: dict, uslugi: dict[str, str], pamiec: dict) -> None:
    z = drzewo["zabiegi"]
    print(f"\ngrup {len(drzewo['grupy'])}, zabiegów {sum(len(v) for v in z.values())}, "
          f"odmian {sum(len(d['odmiany']) for v in z.values() for d in v.values())}")
    for g in drzewo["grupy"]:
        zg = z.get(g, {})
        print(f"  {g:<36} zabiegów {len(zg):>3} | " + ", ".join(sorted(zg, key=lambda r: -zg[r]['n'])[:8]))
    w_grupach: dict[str, set] = defaultdict(set)
    for g, zg in z.items():
        for r in zg:
            w_grupach[r].add(g)
    print("\nto samo słowo w kilku grupach:", {r: sorted(gs) for r, gs in w_grupach.items() if len(gs) > 1})
    typ_grupa: dict[str, Counter] = defaultdict(Counter)
    for k in uslugi:
        if k in pamiec:
            typ = (czesci(k)[2] or "?").split(", ")[0]
            typ_grupa[typ][max(pamiec[k], key=pamiec[k].get)] += 1
    print("\ntyp salonu → grupy usług (kontrola: salon ≠ usługa):")
    for typ, c in sorted(typ_grupa.items(), key=lambda kv: -sum(kv[1].values()))[:12]:
        print(f"  {typ:<22} {sum(c.values()):>5} | " + ", ".join(f"{g} {n}" for g, n in c.most_common(4)))


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    p9 = json.loads((DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    uslugi = proba(p9)
    plik = OUT / "grupy_uslug.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    koszt = await grupy_uslug(uslugi, pamiec, a.budzet)
    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    drzewo = zbuduj(uslugi, pamiec, p9)
    (OUT / "drzewo_v11.json").write_text(json.dumps(drzewo, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt grup: {koszt:.3f} USD")
    raport(drzewo, uslugi, pamiec)


def main() -> None:
    p = argparse.ArgumentParser(description="Budowa drzewa usług v11 z plików pomiarów")
    p.add_argument("--budzet", type=float, default=0.8, help="USD na pytania o grupę")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
