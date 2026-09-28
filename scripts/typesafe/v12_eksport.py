"""Próbka usług z PEŁNYM kontekstem Booksy do budowy drzewa v12 (bd BEAUTY_AUDIT-asrk, 28.09).

Model tej samej usługi (Alex, 28.09): informacja o usłudze jest w pięciu miejscach — kategoria
w cenniku, nazwa, opis, warianty Booksy, zabieg wybrany w Booksy + profil salonu. Do 28.09
TypeSafe dostawał tylko nazwę, kategorię i typ salonu. Ten eksport zbiera wszystkie pięć.

Salony: losowo po NA_KATEGORIE z każdej kategorii Booksy (także spoza 9 głównych), bez salonów
użytych w pomiarach 25–28.09 (w tym 18 odłożonych na sprawdzian). Baza: tylko odczyt.
Wynik: dane/2026-09-28/v12/probka.jsonl — jedna usługa na linię.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v12_eksport.py --na-kategorie 25
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import schemat_v10_sprawdzian as sv  # noqa: E402
import siec_kandydatow as sk  # noqa: E402

from services.supabase import SupabaseService  # noqa: E402

SEED = 20261001
OUT = sk.DANE / "2026-09-28" / "v12"
MIN_USLUG = 10
OPIS_MAX = 800


def odlozone() -> set[int]:
    f = sk.DANE / "2026-09-28" / "sprawdzian_drzewo" / "salony.json"
    return {b for _br, b, _m in json.loads(f.read_text(encoding="utf-8"))} if f.exists() else set()


def warianty(v) -> list[dict]:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return []
    out = []
    for w in v or []:
        if isinstance(w, dict):
            out.append({"label": (w.get("label") or "").strip(), "cena_zl": w.get("price"), "min": w.get("duration")})
    return out


def main(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    kategorie = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    pomin = sv.uzyte() | odlozone()
    rng = random.Random(SEED)
    wybrane: list[tuple[int, str]] = []
    for kid, knazwa in sorted(kategorie.items()):
        rows = (cli.table("salons").select("booksy_id").eq("primary_category_id", kid)
                .order("booksy_id").limit(3000).execute().data or [])
        ids = [r["booksy_id"] for r in rows if r.get("booksy_id") and r["booksy_id"] not in pomin]
        rng.shuffle(ids)
        wybrane += [(i, knazwa) for i in ids[: a.na_kategorie * 2]]  # zapas na salony bez usług
    print(f"kandydatów: {len(wybrane)} z {len(kategorie)} kategorii", flush=True)
    uslugi, salony, na_kat = [], [], {}
    for bid, knazwa in wybrane:
        if na_kat.get(knazwa, 0) >= a.na_kategorie:
            continue
        sc = (cli.table("salon_scrapes").select("id,booksy_id,salon_name,salon_description,primary_category_id,business_categories,scraped_at")
              .eq("booksy_id", bid).order("scraped_at", desc=True).limit(1).execute().data or [])
        if not sc:
            continue
        s = sc[0]
        rows = [r for r in sb._load_services_for_scrape(s["id"]) if r.get("is_active", True)]
        if len(rows) < MIN_USLUG:
            continue
        na_kat[knazwa] = na_kat.get(knazwa, 0) + 1
        bc = s.get("business_categories") or []
        typy = sorted({(c.get("name") if isinstance(c, dict) else kategorie.get(c, "")) or "" for c in bc} - {""})
        salony.append({"booksy_id": bid, "kategoria": knazwa, "nazwa": s.get("salon_name"), "uslug": len(rows)})
        for r in rows:
            uslugi.append({
                "booksy_id": bid, "salon": s.get("salon_name") or "", "typ_salonu": knazwa, "typy_salonu": typy,
                "id": r.get("id"), "kategoria": r.get("category_name") or "", "nazwa": r.get("name") or "",
                "opis": " ".join((r.get("description") or "").split())[:OPIS_MAX],
                "warianty": warianty(r.get("variants")), "zabieg_booksy": r.get("treatment_name") or "",
                "zabieg_booksy_rodzic": r.get("treatment_parent_id"), "zestaw_booksy": r.get("combo_type"),
                "pakiet": bool(r.get("is_package")), "promo": bool(r.get("is_promo")),
                "cena_gr": r.get("price_grosze"), "min": r.get("duration_minutes"),
            })
        if len(salony) % 25 == 0:
            print(f"  salonów {len(salony)}, usług {len(uslugi)}", flush=True)
    with (OUT / "probka.jsonl").open("w", encoding="utf-8") as f:
        for u in uslugi:
            f.write(json.dumps(u, ensure_ascii=False) + "\n")
    (OUT / "salony.json").write_text(json.dumps(salony, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"salonów {len(salony)}, usług {len(uslugi)}; na kategorię: {dict(sorted(na_kat.items(), key=lambda kv: -kv[1]))}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Próbka usług z pełnym kontekstem Booksy (v12)")
    p.add_argument("--na-kategorie", type=int, default=25)
    main(p.parse_args())
