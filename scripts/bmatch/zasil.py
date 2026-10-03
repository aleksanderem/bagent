"""Jednorazowe zasilenie tabel b-card (migracja 203) kartami policzonymi w projekcie b-card.

  .venv/bin/python scripts/bmatch/zasil.py --karty ~/projects/b-card/wyniki/karty_prod_v4.jsonl [--sucho] [--salonow N]

1. bcard_karta  ← plik kart (klucz, karta nazwami; obszar domyślny uzupełniony jak przy nauce b-match).
2. bcard_oferta ← usługi aktualnych skanów salonów beauty (odczyt salon_scrapes / salon_scrape_services) z kartą.
--sucho: nic nie zapisuje, liczy tylko pokrycie (ile usług ma kartę) — do sprawdzenia przed zapisem na produkcji.
Wznawialne (upsert). Zapis WYŁĄCZNIE do bcard_karta i bcard_oferta.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import settings  # noqa: E402
from services.bmatch import oferty  # noqa: E402
from services.bmatch.wycena import uzupelnij_obszar  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

BRANZE = ("Barber shop", "Brwi i rzęsy", "Depilacja", "Fryzjer", "Masaż", "Medycyna Estetyczna", "Paznokcie",
          "Podologia", "Salon Kosmetyczny")


def salony_beauty(client) -> list[int]:
    out, od = [], 0
    while True:
        r = (client.table("salon_scrapes").select("booksy_id,business_categories").eq("is_chain_head", True)
             .range(od, od + 999).execute())
        for g in r.data or []:
            if g.get("booksy_id") and any(k.get("name") in BRANZE for k in (g.get("business_categories") or [])):
                out.append(int(g["booksy_id"]))
        if len(r.data or []) < 1000:
            return sorted(set(out))
        od += 1000


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--karty", required=True)
    ap.add_argument("--sucho", action="store_true")
    ap.add_argument("--salonow", type=int, default=0)
    ap.add_argument("--bez-kart", action="store_true", help="pomiń zapis bcard_karta (już zasilone)")
    ap.add_argument("--od", type=int, default=0, help="wznów od N-tego salonu (kolejność stała: rosnące booksy_id)")
    a = ap.parse_args()
    client = SupabaseService().client
    t0 = time.time()
    karty: dict[str, dict] = {}
    for line in open(Path(a.karty).expanduser()):
        x = json.loads(line)
        if x.get("karta"):
            karty[x["klucz"]] = uzupelnij_obszar(x["karta"])
    print(f"karty z pliku: {len(karty)} ({time.time() - t0:.0f} s)", flush=True)
    if not a.sucho and not a.bez_kart:
        wiersze = [{"klucz": k, "karta": v, "wersja_modelu": settings.bcard_wersja,
                    "wersja_slownika": settings.bcard_wersja_slownika} for k, v in karty.items()]
        for i in range(0, len(wiersze), 500):
            client.table("bcard_karta").upsert(wiersze[i:i + 500], on_conflict="klucz").execute()
            if i % 100000 == 0:
                print(f"bcard_karta {i}/{len(wiersze)}", flush=True)
    salony = salony_beauty(client)
    salony = salony[a.od:]
    if a.salonow:
        salony = salony[:a.salonow]
    glowy = oferty.glowy_skanow(client, salony)
    z_karta = bez_karty = 0
    for n, (bid, sid) in enumerate(glowy.items(), 1):
        gotowe, brak = oferty.oferty_salonu(oferty.uslugi_skanu(client, sid), bid, karty)
        z_karta += len(gotowe)
        bez_karty += len(brak)
        if not a.sucho:
            oferty.zapisz(client, bid, gotowe)
        if n % 1000 == 0:
            print(f"salony {n}/{len(glowy)} | ofert z kartą {z_karta}, bez karty {bez_karty} | {time.time() - t0:.0f} s",
                  flush=True)
    print(f"KONIEC: salonów {len(glowy)}, ofert z kartą {z_karta}, bez karty {bez_karty} "
          f"({z_karta / max(1, z_karta + bez_karty):.1%} pokrycia) | {'SUCHO — nic nie zapisano' if a.sucho else 'zapisano'}"
          f" | {time.time() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    main()
