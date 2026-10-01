"""Wgranie pamięci katalogu (pliki JSONL z eksport_pamieci.py) do tabel mig 201 na produkcji.

ZAPIS NA PRODUKCJI — tylko po „tak” Alexa i po zastosowaniu migracji 201. Bez `--tak` skrypt jedynie liczy wiersze.
Istniejących wierszy nie nadpisuje (klucz główny = rodzaj, klucz, wersja): pierwszy zapis wygrywa, jak w pamięci.

  python scripts/katalog/wgraj_pamiec.py --z DIR            # liczby, bez zapisu
  python scripts/katalog/wgraj_pamiec.py --z DIR --tak      # zapis paczkami po 500
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B)]
from dotenv import load_dotenv  # noqa: E402

load_dotenv(B / ".env")
from services.supabase import SupabaseService  # noqa: E402

TABELE = {"katalog_rozbior": "rodzaj,klucz,wersja_promptu", "katalog_klasa": "rodzaj,klucz,wersja_pytania",
          "katalog_slowo": "budowa,slowo"}
PACZKA = 500


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--z", type=Path, required=True, help="katalog z plikami JSONL")
    ap.add_argument("--tak", action="store_true", help="zapis na produkcji (po zgodzie Alexa)")
    a = ap.parse_args()
    cli = SupabaseService().client if a.tak else None
    for tabela, klucz in TABELE.items():
        wiersze = [json.loads(x) for x in (a.z / f"{tabela}.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
        print(f"{tabela}: {len(wiersze)} wierszy", flush=True)
        if cli is None:
            continue
        for i in range(0, len(wiersze), PACZKA):
            cli.table(tabela).upsert(wiersze[i:i + PACZKA], on_conflict=klucz, ignore_duplicates=True).execute()
        print(f"  zapisane (istniejące pominięte)", flush=True)
    if cli is None:
        print("bez --tak: nic nie zapisano")


if __name__ == "__main__":
    main()
