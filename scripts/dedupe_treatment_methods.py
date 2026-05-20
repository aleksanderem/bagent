#!/usr/bin/env python3
"""Dedup treatment_methods dictionary.

Problem: LLM klasyfikator (mig 095 → 23K methods) propose'ował literówki
i warianty jako osobne entries (neauvia_hydro_deluxe vs neuvia_hydro_deluxe
vs neauvia_hydro_delux). Słownik puchaty, cross-ref rozszczepiony.

Strategy (3-level clustering):
  1. PHASE 1 — alias overlap: jeśli >= 1 alias współdzielony, merge.
  2. PHASE 2 — Levenshtein <= 2 na canonical_name (per brand_family).
  3. PHASE 3 — embedding cosine > 0.92 (per brand_family scope to
     ograniczyć kombinację).

Winner per cluster:
  curated > llm_inferred z najwięcej usage > most_aliases > shortest_canonical

Merge actions per cluster:
  - UPDATE service_method_classification SET method_id = winner.id
    WHERE method_id IN losers (ON CONFLICT skip duplicates)
  - winner.aliases := winner.aliases ∪ losers.canonical_name ∪ losers.aliases
  - DELETE losers FROM treatment_methods

Run order:
  --dry-run      # print proposed merges, no changes
  --apply        # commit merges

FAIL-LOUD: jakikolwiek SQL/RPC error → re-raise, nie kontynuuj.
"""
from __future__ import annotations

import argparse
import logging
import os
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
logger = logging.getLogger("dedup")


def levenshtein(a: str, b: str) -> int:
    """Standard Levenshtein distance."""
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            ins = curr[j] + 1
            dele = prev[j + 1] + 1
            sub = prev[j] + (ca != cb)
            curr.append(min(ins, dele, sub))
        prev = curr
    return prev[-1]


def normalize(s: str) -> str:
    """Lowercase + strip non-alnum."""
    return "".join(c for c in s.lower() if c.isalnum())


def winner_score(m: dict) -> tuple:
    """Higher score = better winner. Tie-break: curated > usage > aliases > shorter canonical."""
    return (
        1 if m["source"] == "curated" else 0,
        m["usage"],
        len(m["aliases"]),
        -len(m["canonical_name"]),  # shorter wins
    )


def classify_cluster_confidence(cluster: list[dict]) -> str:
    """HIGH-confidence clusters są auto-merge-safe. Zaostrzona definicja:
    HIGH wymaga że WSZYSTKIE canonical_names są SEPARATOR-EQUIVALENT —
    po stripie underscore/space/myślnik dają tę samą bit-stringę.

    Przykłady HIGH:
      - vector_lift + vectorlift     → 'vectorlift' = 'vectorlift'
      - frac_3 + frac3               → 'frac3' = 'frac3'
      - mango_peel + mangopeel       → 'mangopeel' = 'mangopeel'
      - hibiscus_flower + hibiscusflower

    To NIE są HIGH (idą do MEDIUM):
      - cosmelan + cosmelan_2        (Cosmelan 2 to inny produkt)
      - smooth_eye + smoth_eye       (literówka 1-char — wymaga LLM review)
      - colombia + columbia          (literówka — może być variant lub typo)
      - arkana_h + arkana_a          (variant serii — różne produkty)

    Dla literówek typu Lev=1 robimy MEDIUM, nie HIGH — żeby nie skleić
    przypadkiem dwóch realnych wariantów produktu (np. "ii" vs "iii").
    Embedding-cosine promotion z MEDIUM do HIGH zaplanowane w v2.

    MEDIUM: Lev≤2 ale heurystyka nie jest pewna.
    LOW: alias-overlap only, brand-catchall, huge length diff.
    """
    brands = {m.get("brand_family") for m in cluster}
    if None in brands or "" in brands:
        return "low"
    if len(brands) > 1:
        return "low"  # cross-brand never auto-merge (shouldn't happen but safety)
    brand = next(iter(brands))

    # Reject brand-catchall presence — these are heterogeneous "all Brand X
    # treatments" buckets and should NOT swallow specific products.
    for m in cluster:
        norm = normalize(m["canonical_name"])
        if norm == normalize(brand or ""):
            return "low"
        # Also reject if canonical is JUST brand + nothing else
        if norm and brand and norm.startswith(normalize(brand)) and len(norm) - len(normalize(brand)) <= 2:
            return "low"

    norms = [normalize(m["canonical_name"]) for m in cluster]
    lengths = [len(n) for n in norms]
    if max(lengths) - min(lengths) > 6:
        return "low"  # too much length variance

    # HIGH: separator-stripped equality across ALL members
    # `normalize` już strip'uje non-alnum (zwraca tylko a-z0-9), więc
    # `norms` to już forma bez separatorów. Sprawdzenie equality.
    if len(set(norms)) == 1:
        return "high"

    # MEDIUM: pairwise Lev ≤ 2 (literówki 1-2 chars w obrębie alpha)
    max_lev = 0
    for i in range(len(norms)):
        for j in range(i + 1, len(norms)):
            d = levenshtein(norms[i], norms[j])
            if d > max_lev:
                max_lev = d
    if max_lev <= 2:
        return "medium"
    return "low"


def find_clusters(
    methods: list[dict],
    lev_threshold: int = 2,
    include_no_brand: bool = False,
    min_canonical_len: int = 8,
) -> list[list[dict]]:
    """Cluster methods using union-find on (canonical_name Levenshtein) +
    (alias overlap). Group only within same brand_family (NULL = own group).

    include_no_brand=False: skip __nobrand__ entirely (15K+ generic categories,
    O(n^2) intractable, also high false-positive risk — "manicure_klasyczny" vs
    "pedicure_klasyczny" Lev=2 ale to różne usługi).

    min_canonical_len: skip Levenshtein rule dla short names (< 8 znaków po
    normalizacji), tam Lev≤2 = ~25% pairs współdzieli się przez przypadek.
    Krótkie literówki muszą być w aliasach (Rule 2).

    Returns list of clusters of size >= 2 (singletons skipped — nothing to merge).
    """
    # Group by brand_family for tractability — different brands NEVER merge
    by_brand: dict[str, list[dict]] = defaultdict(list)
    for m in methods:
        by_brand[m.get("brand_family") or "__nobrand__"].append(m)

    clusters: list[list[dict]] = []
    sorted_brands = sorted(by_brand.items(), key=lambda kv: len(kv[1]), reverse=True)
    for brand, group in sorted_brands:
        if len(group) < 2:
            continue
        if brand == "__nobrand__" and not include_no_brand:
            logger.info("Skipping __nobrand__ group (%d methods) — use --include-no-brand to enable", len(group))
            continue
        if brand == "__nobrand__":
            logger.warning("Processing __nobrand__ (%d methods) — O(n^2) może być powolne", len(group))
        else:
            logger.info("Clustering brand=%s (%d methods)", brand, len(group))
        # Union-find
        parent = {m["id"]: m["id"] for m in group}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        # Precompute normalized + alias sets
        for m in group:
            m["_norm"] = normalize(m["canonical_name"])
            m["_alias_set"] = {normalize(a) for a in m["aliases"] if isinstance(a, str)}

        # Pairs: O(n^2) per brand_family. Max 58 (Arkana) → 1653 pairs, fine.
        # ZAOSTRZONE REGUŁY (2026-05-20 r10/r11):
        # - skip Lev≤2 dla short canonical names (< min_canonical_len)
        # - tylko 2 reguły, broad alias overlap WYŁĄCZONE
        for i, a in enumerate(group):
            for b in group[i + 1 :]:
                # Rule 1: Levenshtein <= threshold (ONLY dla długich nazw)
                if (
                    min(len(a["_norm"]), len(b["_norm"])) >= min_canonical_len
                    and abs(len(a["_norm"]) - len(b["_norm"])) <= lev_threshold
                ):
                    if levenshtein(a["_norm"], b["_norm"]) <= lev_threshold:
                        union(a["id"], b["id"])
                        continue
                # Rule 2: canonical_name jednego jest ALIASEM drugiego
                # (np. wcześniej zmerge'owany alias zostal — silny signal)
                if a["_norm"] in b["_alias_set"] or b["_norm"] in a["_alias_set"]:
                    union(a["id"], b["id"])

        # Collect clusters
        root_groups: dict[int, list[dict]] = defaultdict(list)
        for m in group:
            root_groups[find(m["id"])].append(m)
        for root, cluster in root_groups.items():
            if len(cluster) >= 2:
                clusters.append(cluster)

    return clusters


def merge_cluster(sb: SupabaseService, cluster: list[dict], dry_run: bool) -> dict:
    """Merge cluster: pick winner, redirect classifications, delete losers."""
    cluster_sorted = sorted(cluster, key=winner_score, reverse=True)
    winner = cluster_sorted[0]
    losers = cluster_sorted[1:]

    # Collect aliases from all losers + their canonical_name (as additional alias)
    new_aliases = set(winner["aliases"])
    for l in losers:
        new_aliases.add(l["canonical_name"])
        new_aliases.update(l["aliases"])
    # Remove winner's own canonical (no self-alias)
    new_aliases.discard(winner["canonical_name"])

    merged_aliases = sorted(new_aliases)
    loser_ids = [l["id"] for l in losers]

    logger.info(
        "MERGE cluster %s (brand=%s): winner=%s (id=%d, src=%s, %d usage, %d aliases) "
        "<- losers=%s (%d aliases lost) ===> new_aliases=%d",
        ", ".join(m["canonical_name"] for m in cluster_sorted),
        winner.get("brand_family") or "no_brand",
        winner["canonical_name"], winner["id"], winner["source"],
        winner["usage"], len(winner["aliases"]),
        [l["canonical_name"] for l in losers],
        sum(len(l["aliases"]) for l in losers),
        len(merged_aliases),
    )

    if dry_run:
        return {"winner": winner["id"], "losers": loser_ids, "new_aliases": len(merged_aliases)}

    # 1. Redirect classifications. UNIQUE(service_id, method_id) — może
    # dać conflict gdy service już ma winner classification. Używam
    # raw SQL z ON CONFLICT DO NOTHING.
    update_sql = (
        "UPDATE service_method_classification "
        "SET method_id = :winner_id "
        "WHERE method_id = ANY(:loser_ids) "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM service_method_classification s2 "
        "  WHERE s2.service_id = service_method_classification.service_id "
        "  AND s2.method_id = :winner_id"
        ")"
    )
    # postgrest doesn't do raw SQL easily — use RPC or direct delete-then-update
    # Workaround: do it in 2 steps:
    #   a) DELETE classifications dla service_id-loser_method gdzie service_id już ma winner_method
    #   b) UPDATE pozostałe loser_method → winner_method
    sb.client.rpc("dedup_merge_method", {
        "p_winner_id": winner["id"],
        "p_loser_ids": loser_ids,
    }).execute()

    # 2. Update winner aliases
    sb.client.table("treatment_methods").update({"aliases": merged_aliases}).eq("id", winner["id"]).execute()

    # 3. Delete losers
    for lid in loser_ids:
        sb.client.table("treatment_methods").delete().eq("id", lid).execute()

    return {"winner": winner["id"], "losers": loser_ids, "new_aliases": len(merged_aliases)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply merges (default: dry-run)")
    parser.add_argument("--limit-clusters", type=int, default=None, help="Test on N clusters first")
    parser.add_argument("--brand", type=str, default=None, help="Restrict to single brand_family")
    parser.add_argument("--include-no-brand", action="store_true",
                        help="Enable clustering on __nobrand__ bucket (15K methods, slow + high FP risk)")
    parser.add_argument("--min-canonical-len", type=int, default=8,
                        help="Min normalized length to enable Levenshtein rule (default 8)")
    args = parser.parse_args()

    sb = SupabaseService()

    # Pull all methods with usage count. Direct httpx call żeby
    # uzyskać Range header support (PostgREST .rpc() builder go nie
    # udostępnia w supabase-py).
    logger.info("Loading treatment_methods + usage counts...")
    import os
    import httpx
    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    service_key = os.environ["SUPABASE_SERVICE_KEY"]
    methods: list[dict] = []
    page_size = 1000
    offset = 0
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
    logger.info("Loaded %d methods (paginated)", len(methods))

    if args.brand:
        methods = [m for m in methods if (m.get("brand_family") or "") == args.brand]
        logger.info("Filtered to brand=%s: %d methods", args.brand, len(methods))

    clusters = find_clusters(
        methods,
        include_no_brand=args.include_no_brand,
        min_canonical_len=args.min_canonical_len,
    )
    logger.info("Found %d clusters with >=2 members", len(clusters))
    if not clusters:
        logger.info("Nothing to merge — exiting.")
        return 0

    # Segreguj na HIGH (auto-apply safe) / MEDIUM / LOW (review queue)
    tiers: dict[str, list[list[dict]]] = {"high": [], "medium": [], "low": []}
    for c in clusters:
        tiers[classify_cluster_confidence(c)].append(c)
    logger.info(
        "Confidence breakdown: HIGH=%d MEDIUM=%d LOW=%d",
        len(tiers["high"]), len(tiers["medium"]), len(tiers["low"]),
    )

    if args.limit_clusters:
        tiers["high"] = tiers["high"][: args.limit_clusters]
        logger.info("Limited HIGH to first %d clusters for test", args.limit_clusters)

    # Persist review queue ZAWSZE (do dry-run + apply)
    import json
    review_path = REPO_ROOT / "tmp_dedup_review_queue.json"
    review_data = {
        "medium": [
            [{"id": m["id"], "canonical_name": m["canonical_name"],
              "brand_family": m.get("brand_family"), "usage": m["usage"],
              "source": m["source"], "alias_count": len(m["aliases"])} for m in c]
            for c in tiers["medium"]
        ],
        "low": [
            [{"id": m["id"], "canonical_name": m["canonical_name"],
              "brand_family": m.get("brand_family"), "usage": m["usage"],
              "source": m["source"], "alias_count": len(m["aliases"])} for m in c]
            for c in tiers["low"]
        ],
    }
    review_path.write_text(json.dumps(review_data, ensure_ascii=False, indent=2))
    logger.info("Wrote MEDIUM/LOW review queue → %s", review_path)

    high_before = sum(len(c) for c in tiers["high"])
    high_after = len(tiers["high"])
    logger.info(
        "HIGH-CONF will collapse %d methods → %d (delete %d)",
        high_before, high_after, high_before - high_after,
    )

    if not args.apply:
        logger.warning("DRY RUN — printing first 30 HIGH-CONF clusters then exit. Use --apply to commit.")
        for c in tiers["high"][:30]:
            merge_cluster(sb, c, dry_run=True)
        logger.warning("DRY RUN — also showing first 10 MEDIUM clusters for review:")
        for c in tiers["medium"][:10]:
            logger.info(
                "  MEDIUM cluster (brand=%s): %s",
                c[0].get("brand_family") or "no_brand",
                ", ".join(m["canonical_name"] for m in c),
            )
        return 0

    logger.info("APPLY mode — merging %d HIGH-CONF clusters", len(tiers["high"]))
    for i, c in enumerate(tiers["high"], 1):
        merge_cluster(sb, c, dry_run=False)
        if i % 50 == 0:
            logger.info("Progress: %d/%d clusters merged", i, len(tiers["high"]))
    logger.info("DONE — merged %d HIGH-CONF clusters, deleted %d duplicate entries. "
                "MEDIUM (%d) + LOW (%d) zostały zapisane do %s do manualnego przeglądu.",
                len(tiers["high"]), high_before - high_after,
                len(tiers["medium"]), len(tiers["low"]), review_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
