"""Nowa, niewidziana partia par do weryfikacji destylacji (poza holdoutem mig 188).

Poprawki badania v2 powstały z analizy błędów na holdoucie, więc wynik na nim
jest optymistyczny. Ta partia jest świeża: salony spoza holdoutu, 6 branż
holdoutu + 2 branże spoza niego (uniwersalność), kandydaci z TEGO SAMEGO
wyszukiwania co wycena (qdrant_search.search_twins, exact=True), próbkowanie
po pasmach podobieństwa jak w mig 188. Tylko pary, w których obie nazwy mają
destylację GLM w tej branży — porównanie jakości destylacji, nie pokrycia.

Tylko odczyt (Supabase + Qdrant). Wynik lokalnie w JSONL — do etykietowania
przez dwóch niezależnych sędziów. Ziarno stałe: partia jest odtwarzalna.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/nowa_partia.py --out <katalog>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))

# qdrant_search.get_client czyta QDRANT_* z os.environ (na serwerze ładuje je PM2),
# a nie z config.settings — przy uruchomieniu lokalnym trzeba je doczytać z .env.
for _line in (BAGENT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    _k, _, _v = _line.partition("=")
    if _k.strip() in ("QDRANT_URL", "QDRANT_API_KEY") and _v.strip():
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.similarity_pricing.report_pricing import _fetch_subject_embeddings  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

SEED = 20260921
SOURCE = "typesafe-walidacja-2026-09-21"
# nazwa branży (service_taxonomy.branza) -> business_categories.id (primary_category_id)
BRANZE = {
    "Barber shop": 16, "Fryzjer": 6, "Paznokcie": 10, "Salon Kosmetyczny": 7,
    "Tatuaż i Piercing": 17, "Masaż": 4,
    "Medycyna Estetyczna": 11, "Makijaż": 9,  # spoza holdoutu
}
PASMA = [(0.55, 0.68), (0.68, 0.75), (0.75, 0.85), (0.85, 1.01)]
NA_PASMO = 10
SALONOW_NA_BRANZE = 3
USLUG_NA_SALON = 40
PULA_KONKURENTOW = 400


def name_key(name: str | None) -> str:
    return " ".join((name or "").lower().split())


def band(sim: float) -> str:
    for lo, hi in PASMA:
        if lo <= sim < hi:
            return f"{lo:.2f}-{hi:.2f}" if hi <= 1 else f"{lo:.2f}+"
    return "poniżej"


def glm_covered(cli, branch: str, keys: set[str]) -> set[str]:
    found: set[str] = set()
    ks = sorted(keys)
    for i in range(0, len(ks), 40):
        rows = cli.table("service_taxonomy").select("name_key").eq("branza", branch).in_("name_key", ks[i : i + 40]).execute().data or []
        found |= {r["name_key"] for r in rows}
    return found


async def pairs_for_branch(sb: SupabaseService, branch: str, cat_id: int, exclude: set[int], rng: random.Random) -> list[dict[str, Any]]:
    cli = sb.client
    salons = (
        cli.table("salons").select("booksy_id,city").eq("primary_category_id", cat_id)
        .not_.is_("city", "null").limit(2000).execute().data or []
    )
    rng.shuffle(salons)
    pairs: list[dict[str, Any]] = []
    used_cities: set[str] = set()
    chosen = 0
    for s in salons:
        if chosen >= SALONOW_NA_BRANZE:
            break
        bid, city = s["booksy_id"], s["city"]
        if not bid or bid in exclude or city in used_cities:
            continue
        data = (await sb.get_competitor_full_data([bid])).get(bid) or {}
        services = [x for x in (data.get("services") or []) if x.get("id") and x.get("name")]
        if len(services) < 10:
            continue
        services = rng.sample(services, min(USLUG_NA_SALON, len(services)))
        pool = [
            r["booksy_id"] for r in (
                cli.table("salons").select("booksy_id").eq("city", city).neq("booksy_id", bid)
                .limit(PULA_KONKURENTOW).execute().data or []
            ) if r.get("booksy_id")
        ]
        if not pool:
            continue
        ids = [int(x["id"]) for x in services]
        # Jak wycena: wektory podmiotu z Postgresa — Qdrant trzyma tylko chain-head,
        # więc retrieve po id podmiotu bywa pusty (0 par w 4 branżach w 1. próbie).
        # Wektory w tej samej postaci co w wycenie (pgvector jako tekst → lista liczb).
        emb = _fetch_subject_embeddings(sb, ids)
        ids = [i for i in ids if emb.get(i)]  # bez wektora search_twins wysyła puste zapytanie (400)
        if not ids:
            continue
        twins = search_twins(ids, pool, subject_embeddings=emb, limit=10, min_similarity=0.55, exact=True)
        by_id = {int(x["id"]): x for x in services}
        for sid, cands in twins.items():
            subj = by_id.get(int(sid))
            for c in cands:
                sim = float(c.get("similarity") or c.get("score") or 0)
                pairs.append({
                    "branza": branch, "miasto": city,
                    "subject_booksy_id": bid, "subject_name": subj["name"], "subject_category": subj.get("category_name"),
                    "cand_booksy_id": c.get("booksy_id"), "cand_name": c.get("service_name"), "cand_category": c.get("category_name"),
                    "similarity": round(sim, 4), "sim_bin": band(sim),
                })
        used_cities.add(city)
        chosen += 1
    return pairs


async def run(args: argparse.Namespace) -> None:
    rng = random.Random(SEED)
    sb = SupabaseService()
    cli = sb.client
    exclude = {r["subject_booksy_id"] for r in (cli.table("matching_holdout").select("subject_booksy_id").execute().data or [])}
    batch: list[dict[str, Any]] = []
    for branch, cat_id in BRANZE.items():
        pairs = await pairs_for_branch(sb, branch, cat_id, exclude, rng)
        seen: set[tuple[str, str]] = set()
        unique = []
        for p in pairs:
            key = (name_key(p["subject_name"]), name_key(p["cand_name"]))
            if key[0] != key[1] and key not in seen:
                seen.add(key)
                unique.append(p)
        # Bez wymogu pokrycia GLM: w raporcie GLM przy niepokrytej nazwie milczy —
        # to część realnego obrazu. Pokrycie zapisujemy i liczymy oba porównania.
        covered = glm_covered(cli, branch, {name_key(p[f]) for p in unique for f in ("subject_name", "cand_name")})
        picked = []
        for lo, hi in PASMA:
            inb = [p for p in unique if lo <= p["similarity"] < hi]
            picked += rng.sample(inb, min(NA_PASMO, len(inb)))
        for p in picked:
            p["glm_obie_strony"] = name_key(p["subject_name"]) in covered and name_key(p["cand_name"]) in covered
        n_cov = sum(p["glm_obie_strony"] for p in picked)
        print(f"{branch:<22} par z wyszukiwania {len(unique):>5}, wybranych {len(picked):>3}, z GLM po obu stronach {n_cov}")
        batch += picked
    for i, p in enumerate(batch, start=1):
        p["id"] = f"n{i}"
        p["source"] = SOURCE
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "nowa_partia.jsonl").write_text("\n".join(json.dumps(p, ensure_ascii=False) for p in batch) + "\n", encoding="utf-8")
    print(f"razem {len(batch)} par → {out / 'nowa_partia.jsonl'}")


def main() -> None:
    p = argparse.ArgumentParser(description="Nowa partia par do weryfikacji (tylko odczyt)")
    p.add_argument("--out", required=True)
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
