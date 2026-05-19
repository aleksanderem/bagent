#!/usr/bin/env python3
"""Transform user-curated zestawienie CSV → treatment_methods seed format.

Input CSV columns (zestawienie_estetyka_chirurgia_fryzjerstwo.csv):
    Kategoria, Podkategoria, Nazwa, Typ, Producent/Wlasciciel marki,
    Kraj pochodzenia, Krotki opis, Glowne zastosowanie

Output: data/treatment_methods_seed_v2.csv w naszym treatment_methods seed
format (canonical_name, display_name, category, method_type, brand_family,
aliases, body_areas, description, competitor_methods).

Merge strategy: existing data/treatment_methods_seed.csv (113 curated rows)
+ user CSV (594 rows) → deduplicate on canonical_name, prefer user CSV when
duplicate (richer descriptions and brand_family info).

Run:
    .venv/bin/python scripts/transform_user_csv_to_seed.py \\
        --user-csv ~/Downloads/zestawienie_estetyka_chirurgia_fryzjerstwo.csv \\
        --existing-csv data/treatment_methods_seed.csv \\
        --output data/treatment_methods_seed_v2.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from pathlib import Path


# ── Category mapping: (Kategoria, Podkategoria) → my category ──────────
#
# We keep our existing 15 categories + add 2 new ones (chirurgia,
# transplantacja_wlosow). Subcategory information from user CSV is
# preserved in the description prefix.

_PODKATEGORIA_TO_CATEGORY: dict[str, str] = {
    # ── Medycyna estetyczna — lasery ──
    "Laser CO2 frakcyjny": "laser_skin",
    "Laser Er:YAG": "laser_skin",
    "Laser pikosekundowy": "laser_skin",
    "Laser frakcyjny nieablacyjny": "laser_skin",
    "Laser tulowy/Thulium": "laser_skin",
    "Laser hybrydowy": "laser_skin",
    "Laser naczyniowy": "laser_skin",
    "Laser do trądziku": "laser_skin",
    "Laser depilacyjny diodowy": "laser_depilacja",
    "Laser depilacyjny Aleksandryt": "laser_depilacja",
    "Laser Nd:YAG": "laser_depilacja",
    "IPL": "laser_depilacja",
    "Laser ginekologiczny CO2": "laser_skin",
    "Lasery do zylakow": "laser_skin",
    # ── Medycyna estetyczna — RF/HIFU/energy-based ──
    "HIFU": "rf_hifu",
    "Mikroiglowa RF": "rf_hifu",
    "Wielobiegunowa RF": "rf_hifu",
    "EMS HIFEM": "rf_hifu",
    "Kriolipoliza": "rf_hifu",
    "Shockwave akustyczna": "rf_hifu",
    "Endermologia": "rf_hifu",
    "RF ginekologia": "rf_hifu",
    "RF + ultradzwieki": "rf_hifu",
    "Presoterapia": "rf_hifu",
    "Plasma sublimation": "rf_hifu",
    # ── Medycyna estetyczna — peelings + skincare ──
    "Peeling chemiczny": "peeling",
    "Hydra-peeling": "peeling",
    "Kosmeceutyki gabinetowe": "peeling",
    "Dermokosmetyki apteczne": "peeling",
    # ── Medycyna estetyczna — substances / injectables ──
    "Toksyna botulinowa": "substancja",
    "Technika podania toksyny": "substancja",
    "HA - Galderma": "substancja",
    "HA - Allergan": "substancja",
    "HA - Teoxane": "substancja",
    "HA - Vivacy": "substancja",
    "HA - IBSA": "substancja",
    "HA - Merz": "substancja",
    "HA - Filorga": "substancja",
    "HA - Croma": "substancja",
    "HA - Neauvia": "substancja",
    "HA - Adoderm": "substancja",
    "Hialuronidaza": "substancja",
    "Polinukleotydy": "substancja",
    "Skinbooster": "substancja",
    "PRP/PRF": "substancja",
    "Nici PDO": "substancja",
    "Nici PCL": "substancja",
    "CaHA": "substancja",
    "PLLA": "substancja",
    "PCL": "substancja",
    # ── Medycyna estetyczna — mezoterapia & techniki ──
    "Mikroiglowanie": "mezoterapia",
    "Mezoterapia": "mezoterapia",
    "Mezoterapia lipolityczna": "mezoterapia",
    "Mezoterapia trychologiczna": "mezoterapia",
    "No-needle mezoterapia": "mezoterapia",
    "Elektroporacja": "mezoterapia",
    "Karboksyterapia": "mezoterapia",
    "Skleroterapia": "mezoterapia",
    "Skin scanner i diagnostyka": "inny",
    # ── Chirurgia plastyczna ──
    "Hair transplant": "transplantacja_wlosow",
    "Powiekszanie piersi": "chirurgia",
    "Liposukcja chirurgiczna": "chirurgia",
    "Rynoplastyka": "chirurgia",
    "Lifting twarzy": "chirurgia",
    "Konturowanie twarzy": "chirurgia",
    "Lipotransfer": "chirurgia",
    "Powieki": "chirurgia",
    "Otoplastyka": "chirurgia",
    # ── Fryzjerstwo ──
    "Marki profesjonalne": "fryzjer_zabieg",
    "Bond builders/ochrona": "fryzjer_zabieg",
    "Koloryzacja - rozjasnianie": "fryzjer_koloryzacja",
    "Koloryzacja - cala glowa": "fryzjer_koloryzacja",
    "Strzyzenie": "fryzjer_zabieg",
    "Strzyzenie krecone": "fryzjer_zabieg",
    "Strzyzenie meskie": "fryzjer_zabieg",
    "Przedluzanie wlosow": "fryzjer_zabieg",
    "Prostowanie - keratyna": "fryzjer_zabieg",
    "Ondulacja permanentna": "fryzjer_zabieg",
    "Suszarki": "fryzjer_zabieg",
    "Maszynki barberskie": "fryzjer_zabieg",
    "Lokowki/stylery": "fryzjer_zabieg",
    "Prostownice": "fryzjer_zabieg",
    "Trychologia laser": "trychologia",
    "Trychologia urzadzenia": "trychologia",
    # ── Stylizacja brwi i rzes ──
    "Brwi - PMU": "makijaz_permanentny",
    "Brwi - laminacja marki": "brwi_rzesy",
    "Rzesy - przedluzanie": "brwi_rzesy",
    "Rzesy - przedluzanie marki": "brwi_rzesy",
    "Rzesy - lifting marki": "brwi_rzesy",
    "PMU - urzadzenia": "makijaz_permanentny",
    "PMU - pigmenty": "makijaz_permanentny",
    # ── Stylizacja paznokci ──
    "Marki lakiery hybrydowe": "manicure",
    "Marki akryle": "manicure",
    "Marki zel": "manicure",
    "Techniki": "manicure",
    "Frezarki": "manicure",
    # ── Dziedziny pokrewne ──
    "Trychologia preparaty": "trychologia",
    "Mezoterapia hair": "trychologia",
    "LED therapy": "inny",
    "Krioterapia": "inny",
    "Kapsuly SPA": "inny",
    "Podologia urzadzenia": "podologia",
    "Podologia klamry": "podologia",
}

_TYP_TO_METHOD_TYPE: dict[str, str] = {
    "Urzadzenie": "device",
    "Urzadzenia": "device",
    "Urzadzenie/Technika": "device",
    "Marka": "substance",
    "Marka/produkt": "substance",
    "Linia/marka": "substance",
    "Marka/technika": "technique",
    "Technika/marka": "technique",
    "Technika": "technique",
    "Technika/metoda": "technique",
    "Technika (znak towarowy)": "technique",
    "Technika/Technologia": "technique",
    "Technologia": "technique",
}


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _ascii_fold(s: str) -> str:
    """Polish + diacritics → ASCII lowercase."""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return s.lower()


def _slug(name: str) -> str:
    """Create stable snake_case canonical from any display name."""
    s = _ascii_fold(name)
    s = _SLUG_RE.sub("_", s).strip("_")
    return s or "unknown"


def _generate_aliases(name: str, podkategoria: str) -> list[str]:
    """Generate plausible user-facing alias variants from the canonical
    display name. We don't try to be exhaustive — the classifier later
    handles fuzzy matching via embeddings + LLM."""
    aliases: set[str] = set()
    ascii_name = _ascii_fold(name)
    aliases.add(ascii_name)
    # Strip "(...)" parentheticals
    no_paren = re.sub(r"\s*\(.*?\)\s*", " ", ascii_name).strip()
    if no_paren != ascii_name:
        aliases.add(no_paren)
    # Strip non-alphanumeric runs to spaces, dedupe whitespace
    smooth = re.sub(r"[^a-z0-9 ]+", " ", no_paren)
    smooth = re.sub(r"\s+", " ", smooth).strip()
    if smooth and smooth != no_paren:
        aliases.add(smooth)
    # Original casing — Polish UI lookups may match "PRO XN" not "pro xn"
    aliases.add(name.lower())
    aliases.add(name.strip())
    # Drop empty/single-char
    return sorted({a for a in aliases if a and len(a) >= 2})


def _resolve_category(kategoria: str, podkategoria: str) -> str:
    cat = _PODKATEGORIA_TO_CATEGORY.get(podkategoria)
    if cat:
        return cat
    # Fallback by top-level kategoria
    kat_lower = (kategoria or "").lower()
    if "fryzjer" in kat_lower:
        return "fryzjer_zabieg"
    if "manicure" in kat_lower or "paznok" in kat_lower:
        return "manicure"
    if "brwi" in kat_lower or "rzes" in kat_lower:
        return "brwi_rzesy"
    if "chirurg" in kat_lower:
        return "chirurgia"
    if "medycyn" in kat_lower:
        return "inny"  # safer than miscategorizing
    return "inny"


def _resolve_method_type(typ: str) -> str:
    return _TYP_TO_METHOD_TYPE.get(typ.strip(), "technique")


def _build_description(opis: str, zastosowanie: str, podkategoria: str) -> str:
    parts: list[str] = []
    if podkategoria:
        parts.append(f"[{podkategoria}]")
    if opis:
        parts.append(opis.strip())
    if zastosowanie:
        parts.append(f"Zastosowanie: {zastosowanie.strip()}")
    return " ".join(parts)


def _read_existing_seed(path: Path) -> dict[str, dict]:
    """Read our hand-curated treatment_methods_seed.csv and key by
    canonical_name for dedup."""
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cn = (row.get("canonical_name") or "").strip().lower()
            if cn:
                out[cn] = row
    return out


def _transform_user_row(row: dict) -> dict | None:
    name = (row.get("Nazwa") or "").strip()
    if not name:
        return None
    canonical = _slug(name)
    display = name
    podkategoria = (row.get("Podkategoria") or "").strip()
    kategoria = (row.get("Kategoria") or "").strip()
    typ = (row.get("Typ") or "").strip()
    producent = (row.get("Producent/Wlasciciel marki") or "").strip()
    opis = (row.get("Krotki opis") or "").strip()
    zastosowanie = (row.get("Glowne zastosowanie") or "").strip()

    return {
        "canonical_name": canonical,
        "display_name": display,
        "category": _resolve_category(kategoria, podkategoria),
        "method_type": _resolve_method_type(typ),
        "brand_family": _slug(producent) if producent else "",
        "aliases": "|".join(_generate_aliases(name, podkategoria)),
        "body_areas": "",  # Empty — user CSV doesn't carry structured body data
        "description": _build_description(opis, zastosowanie, podkategoria),
        "competitor_methods": "[]",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user-csv", type=Path, required=True,
        help="User-curated source CSV (zestawienie...)",
    )
    parser.add_argument(
        "--existing-csv", type=Path,
        default=Path(__file__).parent.parent / "data" / "treatment_methods_seed.csv",
        help="Existing seed CSV to merge (curated 113 rows).",
    )
    parser.add_argument(
        "--output", type=Path,
        default=Path(__file__).parent.parent / "data" / "treatment_methods_seed_v2.csv",
        help="Output merged seed CSV.",
    )
    args = parser.parse_args()

    if not args.user_csv.exists():
        print(f"User CSV not found: {args.user_csv}", file=sys.stderr)
        return 1

    # Read existing curated seed
    existing_by_cn = _read_existing_seed(args.existing_csv)
    print(f"Existing curated seed: {len(existing_by_cn)} rows")

    # Read user CSV
    user_rows: list[dict] = []
    with args.user_csv.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            user_rows.append(r)
    print(f"User CSV: {len(user_rows)} source rows")

    # Transform user rows
    transformed: list[dict] = []
    skipped_empty = 0
    for r in user_rows:
        t = _transform_user_row(r)
        if t is None:
            skipped_empty += 1
            continue
        transformed.append(t)
    print(f"Transformed: {len(transformed)} (skipped {skipped_empty} empty)")

    # Dedup user-side first (multiple rows may slug to same canonical)
    user_by_cn: dict[str, dict] = {}
    user_dup = 0
    for t in transformed:
        cn = t["canonical_name"]
        if cn in user_by_cn:
            user_dup += 1
            # Merge: combine aliases, prefer earlier
            existing_t = user_by_cn[cn]
            existing_aliases = set(
                a for a in existing_t["aliases"].split("|") if a
            )
            new_aliases = set(a for a in t["aliases"].split("|") if a)
            merged = sorted(existing_aliases | new_aliases)
            existing_t["aliases"] = "|".join(merged)
            continue
        user_by_cn[cn] = t
    if user_dup:
        print(f"  user-side intra-dups merged: {user_dup}")

    # Merge with existing — user CSV wins on conflict (richer descriptions,
    # but we keep our hand-curated competitor_methods / body_areas)
    merged_by_cn: dict[str, dict] = {}
    for cn, existing in existing_by_cn.items():
        merged_by_cn[cn] = dict(existing)
    overlaps = 0
    for cn, user_t in user_by_cn.items():
        if cn in merged_by_cn:
            overlaps += 1
            curated = merged_by_cn[cn]
            # Prefer user description (more detailed); KEEP curated
            # competitor_methods + body_areas; UNION aliases.
            curated_aliases = set(
                a for a in (curated.get("aliases") or "").split("|") if a
            )
            user_aliases = set(
                a for a in (user_t.get("aliases") or "").split("|") if a
            )
            merged_aliases = sorted(curated_aliases | user_aliases)
            curated["aliases"] = "|".join(merged_aliases)
            # User description usually more detailed
            if user_t.get("description"):
                curated["description"] = user_t["description"]
            # Prefer user brand_family when curated had none
            if not curated.get("brand_family") and user_t.get("brand_family"):
                curated["brand_family"] = user_t["brand_family"]
            # method_type: keep curated (more carefully reviewed)
            # category: keep curated (more carefully reviewed)
            continue
        merged_by_cn[cn] = user_t

    print(f"Overlaps merged: {overlaps}")
    print(f"Output total rows: {len(merged_by_cn)}")

    # Stats
    from collections import Counter
    cats = Counter(r["category"] for r in merged_by_cn.values())
    print("\nCategory distribution:")
    for c, n in cats.most_common():
        print(f"  {n:4d}  {c}")

    # Write output
    fieldnames = [
        "canonical_name", "display_name", "category", "method_type",
        "brand_family", "aliases", "body_areas", "description",
        "competitor_methods",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for cn in sorted(merged_by_cn.keys()):
            writer.writerow(merged_by_cn[cn])

    print(f"\nWrote: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
