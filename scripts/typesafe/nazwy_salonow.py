"""Nazwy usług z cenników salonów danej branży (branża SALONU, nie zabiegu).

Po co: service_taxonomy przypisuje nazwę do branży ZABIEGU (kaskada
treatment_branch_map), a weto w wycenie szuka jej po branży SALONU podmiotu.
Salon paznokci sprzedaje też depilację nóg, kosmetyczny — pedicure. Mapa pytań
policzona tylko z service_taxonomy wycinała pytania o usługi, które salony tej
branży realnie mają. Ta próbka daje populację, którą widzi weto.

Tylko odczyt. Losowe salony (ziarno stałe) po primary_category_id, cennik
z najnowszego skanu (SupabaseService.get_competitor_full_data).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/nazwy_salonow.py <plik.tsv>
"""

from __future__ import annotations

import asyncio
import csv
import os
import random
import sys
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from nowa_partia import BRANZE, SEED  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

SALONOW = 100
PACZKA = 20


async def run(out: Path) -> None:
    rng = random.Random(SEED + 1)
    sb = SupabaseService()
    rows: list[tuple[str, str]] = []
    for branch, cat_id in BRANZE.items():
        salons = [
            s["booksy_id"] for s in (
                sb.client.table("salons").select("booksy_id").eq("primary_category_id", cat_id).limit(3000).execute().data or []
            ) if s.get("booksy_id")
        ]
        picked = rng.sample(salons, min(SALONOW, len(salons)))
        n = 0
        for i in range(0, len(picked), PACZKA):
            data = await sb.get_competitor_full_data(picked[i : i + PACZKA])
            for d in data.values():
                for svc in d.get("services") or []:
                    if svc.get("name"):
                        rows.append((branch, svc["name"]))
                        n += 1
        print(f"{branch:<22} salonów {len(picked):>3}, nazw {n:>5}")
    with out.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f, delimiter="\t").writerows(rows)
    print(f"zapisano {len(rows)} nazw → {out}")


if __name__ == "__main__":
    asyncio.run(run(Path(sys.argv[1])))
