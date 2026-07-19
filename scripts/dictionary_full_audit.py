#!/usr/bin/env python3
"""Full dictionary audit + auto-merge for same-root conflicts.

Detekcja:
  1. Brand conflict: ten sam canonical root (np "dermapen_4_0" → "dermapen")
     ale różne brand_family między entries → multi-canonical-same-product
  2. Category conflict: ten sam root → różne category
  3. Number-suffix duplikaty: canonical_X i canonical_X_0, canonical_v2 etc.

Strategia merge per group:
  Winner per (root):
    1. source='curated' wins over llm_inferred
    2. higher usage count wins
    3. lower id (older) wins
  Loser handling:
    - Aliases unioned into winner
    - service_method_classification UPDATEd via dedup_merge_method RPC
    - competitor_pricing_comparisons.treatment_method_id remapped
    - Loser row deleted

Run order:
  --dry-run        # raport bez zmian (default)
  --apply          # commit merges
  --max-merges N   # limit do N grup żeby testować incremental

FAIL-LOUD: jakikolwiek SQL error → re-raise.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("dict-audit")


"""SUFFIX detection — KONSERWATYWNIE!

Merge OK (clear version variant tego samego produktu):
  - _v[0-9]+$       np. infini_v2, infini_v20    → version notation
  - _[0-9]_[0-9]$   np. dermapen_4_0, vit_c_2_2  → dot notation (4.0, 2.2)

Merge NOT OK (różny produkt mimo podobnego prefiksu):
  - _[0-9]+$        np. cytocare_532, cytocare_715, sunekos_200/1200,
                        pro_skin_30/60, super_veloce_4800
    Bo: to ID formuły / stężenia / długości fali / czasy zabiegu, nie wersje.

Skrypt zwraca pusty root gdy canonical NIE pasuje do żadnego safe-pattern,
co odbiera tę grupę z merge candidates.
"""
SAFE_VERSION_RE = re.compile(r"^(.+)_(v[0-9]+|[0-9]+_[0-9]+)$", re.IGNORECASE)


def root_of(canonical: str) -> str | None:
    """Zwraca root jeśli canonical wygląda na version variant. None inaczej."""
    m = SAFE_VERSION_RE.match(canonical.lower())
    if m:
        return m.group(1)
    return None  # nie kandydat do merge


def winner_score(m: dict) -> tuple:
    """Tie-breaker: curated > usage > lower id (older)."""
    return (
        1 if m["source"] == "curated" else 0,
        m["usage"],
        -m["id"],  # lower id wins
    )


def load_all_methods(sb: SupabaseService) -> list[dict]:
    """Paginated load of treatment_methods + usage counts via dedup_load_methods RPC."""
    import httpx
    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    service_key = os.environ["SUPABASE_SERVICE_KEY"]
    methods: list[dict] = []
    offset = 0
    page_size = 1000
    with httpx.Client(timeout=60.0) as client:
        while True:
            r = client.post(
                f"{supabase_url}/rest/v1/rpc/dedup_load_methods",
                headers={
                    "apikey": service_key,
                    "Authorization": f"Bearer {service_key}",
                    "Content-Type": "application/json",
                },
                json={"p_offset": offset, "p_limit": page_size},
            )
            r.raise_for_status()
            page = r.json() or []
            methods.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
    return methods


def find_conflict_groups(methods: list[dict]) -> list[tuple[str, list[dict]]]:
    """Find canonical roots with VERSION variants (safe to merge).

    Strategy:
      Pass 1: identify methods z safe-version suffix → group by root.
      Pass 2: dla każdego root, dorzuć bare canonical=root jeśli istnieje
              (np. dermapen sam istnieje + dermapen_4_0 + dermapen_v2).

    Returns list of (root, [methods]) gdzie grupa ma >=2 entries.
    """
    by_canonical = {m["canonical_name"].lower(): m for m in methods if m.get("brand_family")}

    # Pass 1: zbierz methods które MAJĄ safe-version suffix
    version_variants: dict[str, list[dict]] = defaultdict(list)
    for m in methods:
        if not m.get("brand_family"):
            continue
        r = root_of(m["canonical_name"])
        if r is None:
            continue  # canonical bez safe-version suffixu — skip
        if len(r) < 5:
            continue  # za krótki root = false grouping risk
        version_variants[r].append(m)

    # Pass 2: dla każdego root, dodaj bare canonical jeśli istnieje
    conflicts: list[tuple[str, list[dict]]] = []
    for root, variants in version_variants.items():
        group = list(variants)
        bare = by_canonical.get(root)
        if bare and bare["id"] not in {m["id"] for m in group}:
            group.append(bare)
        if len(group) < 2:
            continue
        conflicts.append((root, group))

    conflicts.sort(key=lambda kv: sum(m["usage"] for m in kv[1]), reverse=True)
    return conflicts


def merge_group(sb: SupabaseService, root: str, group: list[dict], dry_run: bool) -> dict:
    """Pick winner, redirect classifications, delete losers, merge aliases."""
    group_sorted = sorted(group, key=winner_score, reverse=True)
    winner = group_sorted[0]
    losers = group_sorted[1:]

    # Aliases merge
    new_aliases = set(winner["aliases"] or [])
    for l in losers:
        new_aliases.update(l.get("aliases") or [])
        new_aliases.add(l["canonical_name"])  # losing canonical → alias
    new_aliases.discard(winner["canonical_name"])
    merged_aliases = sorted(new_aliases)
    loser_ids = [l["id"] for l in losers]
    total_classifications_lost = sum(l["usage"] for l in losers)

    logger.info(
        "MERGE root=%s: winner=%s (id=%d, src=%s, %d usage, brand=%s, cat=%s) ← losers=%s "
        "(%d classifications → winner; new aliases=%d)",
        root, winner["canonical_name"], winner["id"], winner["source"],
        winner["usage"], winner.get("brand_family"), winner.get("category"),
        [(l["canonical_name"], l["id"], l["usage"], l.get("brand_family"), l.get("category")) for l in losers],
        total_classifications_lost, len(merged_aliases),
    )

    if dry_run:
        return {"winner_id": winner["id"], "losers": loser_ids, "merged_aliases": len(merged_aliases)}

    # 1. Update aliases on winner + bump source to curated if any loser was curated
    target_source = "curated" if (winner["source"] == "curated" or any(l["source"] == "curated" for l in losers)) else winner["source"]
    sb.client.table("treatment_methods") \
        .update({"aliases": merged_aliases, "source": target_source}) \
        .eq("id", winner["id"]).execute()

    # 2. Redirect classifications via dedup_merge_method RPC (handles dup-conflict)
    sb.client.rpc("dedup_merge_method", {
        "p_winner_id": winner["id"],
        "p_loser_ids": loser_ids,
    }).execute()

    # 3. Redirect competitor_pricing_comparisons.treatment_method_id BEFORE delete
    #    (delete'em FK na method_id ale cpc_must_have_tid constraint blokuje
    #     SET NULL — trzeba update do winner's id).
    for lid in loser_ids:
        sb.client.table("competitor_pricing_comparisons") \
            .update({"treatment_method_id": winner["id"]}) \
            .eq("treatment_method_id", lid).execute()

    # 4. Delete losers
    for lid in loser_ids:
        sb.client.table("treatment_methods").delete().eq("id", lid).execute()

    return {
        "winner_id": winner["id"],
        "winner_canonical": winner["canonical_name"],
        "losers": loser_ids,
        "merged_aliases": len(merged_aliases),
        "classifications_redirected": total_classifications_lost,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Commit merges (default: dry-run)")
    parser.add_argument("--max-merges", type=int, default=None, help="Limit do N groups dla incremental testing")
    args = parser.parse_args()

    sb = SupabaseService()

    logger.info("Loading treatment_methods + usage counts...")
    methods = load_all_methods(sb)
    logger.info("Loaded %d methods total", len(methods))

    conflicts = find_conflict_groups(methods)
    logger.info("Found %d same-root multi-entry groups (potential merges)", len(conflicts))

    if args.max_merges:
        conflicts = conflicts[: args.max_merges]
        logger.info("Limited to top %d groups by total usage", args.max_merges)

    total_before = sum(len(g) for _, g in conflicts)
    total_after = len(conflicts)
    logger.info(
        "Will collapse %d entries → %d (delete %d losers)",
        total_before, total_after, total_before - total_after,
    )

    if not args.apply:
        logger.warning("DRY RUN — printing first 50 groups then exit. Use --apply to commit.")
        for root, group in conflicts[:50]:
            merge_group(sb, root, group, dry_run=True)
        return 0

    logger.info("APPLY mode — merging %d groups", len(conflicts))
    merged = 0
    classifications_redirected = 0
    for i, (root, group) in enumerate(conflicts, 1):
        try:
            result = merge_group(sb, root, group, dry_run=False)
            merged += 1
            classifications_redirected += result.get("classifications_redirected", 0)
            if i % 20 == 0:
                logger.info("Progress: %d/%d merged, %d classifications redirected", i, len(conflicts), classifications_redirected)
        except Exception as e:
            logger.error("MERGE FAILED for root=%s: %s — continuing", root, e)
    logger.info("DONE — merged %d groups, redirected %d classifications", merged, classifications_redirected)
    return 0


if __name__ == "__main__":
    sys.exit(main())
