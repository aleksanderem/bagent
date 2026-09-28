"""Drzewo v12, etap A: pozycja (poziom 0) i dziedzina (poziom 1) dla próbki z pełnym kontekstem.

Jedna usługa na wywołanie (decyzja 27.09), oba pytania w tym samym wywołaniu (niezależne pytania
o ten sam stan). Wynik z prawdopodobieństwami — kolejne etapy i przejście po drzewie z nich korzystają.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v12_poziom01.py --budzet 3.0 [--limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.kontekst_v12 import pytania_0_1, stan_v12  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DZ = Path(__file__).resolve().parent / "dane" / "2026-09-28" / "v12"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_USLUGA = 3600  # szacunek: stan z opisem ~500 + pozycja ~350 + 21 dziedzin z opisami ~2700
ROWNOLEGLE = 30


async def main_async(a: argparse.Namespace) -> None:
    uslugi = [json.loads(line) for line in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    if a.na_salon:  # równa próbka: najwyżej N losowych usług z każdego salonu
        import random
        rng, po_salonie = random.Random(20261001), {}
        for u in uslugi:
            po_salonie.setdefault(u["booksy_id"], []).append(u)
        uslugi = [u for us in po_salonie.values() for u in rng.sample(us, min(a.na_salon, len(us)))]
    if a.limit:
        uslugi = uslugi[: a.limit]
    plik = DZ / "poziom01.json"
    pamiec: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = [u for u in uslugi if str(u["id"]) not in pamiec]
    szac = len(brak) * TOK_USLUGA * CENA_TOK
    print(f"usług {len(uslugi)}, do zapytania {len(brak)}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet — użyj --limit")
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    q = pytania_0_1()
    tok, bledy = [0], [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def jedna(u: dict) -> None:
            async with sem:
                try:
                    r = await client.system_one(stan_v12(u), q, model=MODEL)
                except Exception as e:  # noqa: BLE001 — jedna usługa bez odpowiedzi, reszta dalej
                    bledy[0] += 1
                    if bledy[0] <= 5:
                        print(f"  {u['nazwa'][:40]!r}: {type(e).__name__}: {str(e)[:90]}", flush=True)
                    return
            tok[0] += r.usage.input_tokens or 0
            pamiec[str(u["id"])] = {k: {o: round(p, 4) for o, p in r.choices[k].probabilities.items()} for k in ("pozycja", "dziedzina")}
        await asyncio.gather(*[jedna(u) for u in brak])
    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    naj = lambda d: max(d, key=d.get)  # noqa: E731
    print(f"koszt {tok[0] * CENA_TOK:.3f} USD ({tok[0] / max(len(brak) - bledy[0], 1):.0f} tok./usługę), błędów {bledy[0]}")
    print("pozycja:", dict(Counter(naj(v["pozycja"]) for v in pamiec.values()).most_common()))
    print("dziedzina:", dict(Counter(naj(v["dziedzina"]) for v in pamiec.values()).most_common()))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v12: pozycja i dziedzina z pełnym kontekstem")
    p.add_argument("--budzet", type=float, default=3.0)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--na-salon", type=int, default=0, help="najwyżej N usług z salonu (0 = wszystkie)")
    asyncio.run(main_async(p.parse_args()))
