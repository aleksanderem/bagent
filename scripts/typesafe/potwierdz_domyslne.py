"""Wartość domyślna cechy tylko wtedy, gdy wynika z DEFINICJI rodzaju.

Kandydat z danych (schemat_osi.py: ≥ 80% podających cechę, ≥ 20 usług) bywa
skrzywiony: salony podają cechę głównie, gdy odbiega od normy („uzupełnienie”
rzęs, „hybryda” przy pedicure). TypeSafe (tak/nie, próg 0,8) sprawdza, czy
sam rodzaj zabiegu przesądza o tej wartości: przedłużanie rzęs → obszar rzęsy
(tak), przedłużanie rzęs → etap uzupełnienie (nie). Niepotwierdzone kandydaty
są usuwane — brak podania zostaje wtedy brakiem.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/potwierdz_domyslne.py --schemat <schemat.json>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from typesafe_sdk import AsyncTypeSafeClient, Noul, RetryPolicy

MODEL = "jev-1.13.0"
TAK = 0.8
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"


async def main_async(plik: Path) -> None:
    schemat = json.loads(plik.read_text(encoding="utf-8"))
    kand = [(r, c, d.get("domyslna_kandydat") or d["domyslna"]) for r, x in schemat["rodzaje"].items()
            for c, d in x["cechy"].items() if d.get("domyslna") or d.get("domyslna_kandydat")]
    q = {
        f"{i}": Noul(instructions={
            "zabieg": r, "cecha": c, "wartosc": v,
            # 25.09: „czy KAŻDA usługa z definicji…” odrzucało wszystko (dosłowne
            # czytanie „każda”); to sformułowanie rozdzieliło 4 oczywiste tak
            # (0,80–0,94) od 4 oczywistych nie (0,13–0,34).
            "question": "Czy `wartosc` (cecha: `cecha`) wynika wprost z nazwy zabiegu `zabieg`?",
        })
        for i, (r, c, v) in enumerate(kand)
    }
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        odp = {}
        klucze = list(q)
        for i in range(0, len(klucze), 50):
            r = await client.system_one({"kontekst": "cenniki salonów beauty w Polsce"}, {k: q[k] for k in klucze[i : i + 50]}, model=MODEL)
            odp.update({k: a.noul for k, a in r.nouls.items()})
    tak, nie = [], []
    for i, (r, c, v) in enumerate(kand):
        p = odp.get(str(i), 0.0)
        d = schemat["rodzaje"][r]["cechy"][c]
        d["domyslna_p"] = round(p, 3)
        d["domyslna_kandydat"] = v
        if p >= TAK:
            d["domyslna"] = v
            tak.append((r, c, v))
        else:
            d["domyslna"] = None
            nie.append((r, c, v, round(p, 2)))
    plik.write_text(json.dumps(schemat, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"potwierdzone {len(tak)} z {len(kand)}")
    print("TAK:", tak[:20])
    print("NIE:", nie[:20])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--schemat", required=True)
    asyncio.run(main_async(Path(p.parse_args().schemat)))


if __name__ == "__main__":
    main()
