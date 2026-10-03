"""Ręczne odświeżenie ofert b-card — to samo, co nocny cron (services/bmatch/odswiez.py).

  .venv/bin/python scripts/bmatch/odswiez.py --sucho [--salonow N]   # tylko policz kolejkę: salony, usługi, braki kart
  .venv/bin/python scripts/bmatch/odswiez.py [--salonow N]           # przelicz i zapisz (bcard_karta/oferta/skan)
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.bmatch import odswiez  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sucho", action="store_true")
    ap.add_argument("--salonow", type=int, default=odswiez.SALONOW)
    a = ap.parse_args()
    print(asyncio.run(odswiez.odswiez(SupabaseService().client, sucho=a.sucho, salonow=a.salonow)), flush=True)


if __name__ == "__main__":
    main()
