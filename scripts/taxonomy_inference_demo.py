"""Demo: hybrid DB-lookup + AI fallback dla taxonomy classification.

Bierze konkretny salon z `convex_audit_id` argumentu (default Floral Nails),
porównuje obecne booksy_treatment_id (z API Booksy) z tym co dałby hot
lookup w `salon_scrape_services` — bierze top tid per zNormalizowanej name
gdy confidence >= threshold i count >= min_n.

Pokazuje ile usług by się zmieniło + których nie da się "naprawić"
deterministycznie (potrzeba AI).

Usage:
    .venv/bin/python -m scripts.taxonomy_inference_demo \\
        --audit-id j57e45tp7cjm8yhprz093ds05d84jd9s
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings
from services.sb_client import make_supabase_client


_STOPWORDS = {
    "z", "i", "na", "do", "w", "od", "po", "dla", "lub", "albo", "oraz",
    "ze", "za", "u", "o", "a", "ten", "ta", "to", "te", "tych",
}


def normalize_name(name: str) -> str:
    """Strip diacritics, lowercase, collapse whitespace, drop punctuation."""
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    no_diac = "".join(c for c in nfkd if not unicodedata.combining(c))
    lower = no_diac.lower()
    cleaned = re.sub(r"[^\w\s]", " ", lower)
    return re.sub(r"\s+", " ", cleaned).strip()


def tokenize(name: str) -> frozenset[str]:
    """Tokenize to significant words: drop stopwords, numbers, and 1-char tokens."""
    norm = normalize_name(name)
    tokens = norm.split()
    return frozenset(
        t for t in tokens
        if len(t) >= 3 and t not in _STOPWORDS and not t.isdigit()
    )


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def build_hot_lookup(client, min_count: int = 3):
    """Build three indices from salon_scrape_services where booksy_treatment_id IS NOT NULL:

    Returns (exact_lookup, token_lookup, parent_to_tokens):
      exact_lookup: normalized_name → (top_tid, confidence, total_n)
      token_lookup: list[(token_set, tid, count)] for unconstrained fuzzy match
      parent_to_tokens: parent_id → list[(token_set, tid, count)] — constrained
                       fuzzy match within a Booksy treatment family
    """
    # Pull all (name, tid) pairs — paginate to handle large datasets.
    print("Building hot lookup from salon_scrape_services...", flush=True)
    all_rows: list[dict] = []
    page = 0
    PAGE_SIZE = 5000
    while True:
        rows = (
            client.table("salon_scrape_services")
            .select("name,booksy_treatment_id,treatment_parent_id")
            .not_.is_("booksy_treatment_id", "null")
            .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
            .execute()
            .data
            or []
        )
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        page += 1
        if page > 200:  # safety: cap at 1M rows
            print(f"  (safety stop at {len(all_rows)} rows)")
            break
    print(f"  loaded {len(all_rows)} services")

    # Group by normalized name → Counter(tid)
    name_to_tids: dict[str, Counter] = defaultdict(Counter)
    # Group by token-set → Counter(tid) (unconstrained)
    tokens_to_tids: dict[frozenset[str], Counter] = defaultdict(Counter)
    # Group by (parent_id, token-set) for parent-constrained match
    parent_tokens_to_tids: dict[tuple[int, frozenset[str]], Counter] = defaultdict(Counter)
    for r in all_rows:
        raw = r.get("name") or ""
        n = normalize_name(raw)
        tid = r.get("booksy_treatment_id")
        parent = r.get("treatment_parent_id")
        if not n or tid is None:
            continue
        name_to_tids[n][tid] += 1
        toks = tokenize(raw)
        if toks:
            tokens_to_tids[toks][tid] += 1
            if parent is not None:
                parent_tokens_to_tids[(parent, toks)][tid] += 1

    # Build exact lookup
    exact_lookup: dict[str, tuple[int, float, int]] = {}
    for n, tids in name_to_tids.items():
        total = sum(tids.values())
        if total < min_count:
            continue
        top_tid, top_n = tids.most_common(1)[0]
        exact_lookup[n] = (top_tid, top_n / total, total)

    # Build token lookup: list of (token_set, tid, count) — sorted by count desc
    token_lookup: list[tuple[frozenset[str], int, int]] = []
    for toks, tids in tokens_to_tids.items():
        for tid, cnt in tids.items():
            if cnt < min_count:
                continue
            token_lookup.append((toks, tid, cnt))
    token_lookup.sort(key=lambda x: -x[2])

    # Build parent-constrained lookup: parent_id → list[(token_set, tid, count)]
    parent_to_tokens: dict[int, list[tuple[frozenset[str], int, int]]] = defaultdict(list)
    for (parent, toks), tids in parent_tokens_to_tids.items():
        for tid, cnt in tids.items():
            if cnt < min_count:
                continue
            parent_to_tokens[parent].append((toks, tid, cnt))
    for p in parent_to_tokens:
        parent_to_tokens[p].sort(key=lambda x: -x[2])

    print(f"  built {len(exact_lookup)} exact entries + {len(token_lookup)} token entries + {len(parent_to_tokens)} parents\n")
    return exact_lookup, token_lookup, parent_to_tokens


def infer_via_tokens(svc_tokens: frozenset[str], token_lookup: list, *,
                     min_jaccard: float = 0.5, top_k_per_tid: int = 20):
    """Find best tid for a service via token-set Jaccard.

    Aggregates votes from top-K most similar entries per tid.
    Returns (tid, confidence, n_neighbors) or None.
    """
    if not svc_tokens:
        return None
    # Score each (toks, tid, count) by Jaccard * sqrt(count) to favor confident matches
    scored: list[tuple[float, int, int, frozenset[str]]] = []
    for toks, tid, cnt in token_lookup:
        j = jaccard(svc_tokens, toks)
        if j < min_jaccard:
            continue
        # weight by similarity AND occurrence count (more popular = more reliable)
        score = j * (1 + (cnt ** 0.5) / 10)
        scored.append((score, tid, cnt, toks))
    if not scored:
        return None
    # Top K → vote on tid weighted by score
    scored.sort(reverse=True)
    top = scored[:top_k_per_tid]
    tid_votes: dict[int, float] = defaultdict(float)
    tid_n: dict[int, int] = defaultdict(int)
    for score, tid, cnt, _ in top:
        tid_votes[tid] += score
        tid_n[tid] += cnt
    total_votes = sum(tid_votes.values())
    if total_votes == 0:
        return None
    top_tid = max(tid_votes, key=tid_votes.get)
    confidence = tid_votes[top_tid] / total_votes
    return (top_tid, confidence, tid_n[top_tid])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-id", required=True)
    ap.add_argument("--min-count", type=int, default=5,
                    help="Min total occurrences for a name to be in the lookup")
    ap.add_argument("--min-confidence", type=float, default=0.5,
                    help="Min confidence (top_tid / total) to apply override")
    args = ap.parse_args()

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # 1. Build hot lookup index (exact + token + parent-constrained)
    exact_lookup, token_lookup, parent_to_tokens = build_hot_lookup(client, min_count=args.min_count)

    # 2. Load subject services
    scrape = (
        client.table("salon_scrapes")
        .select("id,salon_name,total_services")
        .eq("convex_audit_id", args.audit_id)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
        .data[0]
    )
    print(f"Subject: {scrape['salon_name']} (scrape_id={scrape['id']}, total_services={scrape['total_services']})\n")

    svc = (
        client.table("salon_scrape_services")
        .select("name,booksy_treatment_id,treatment_name,treatment_parent_id,price_grosze")
        .eq("scrape_id", scrape["id"])
        .execute()
        .data
        or []
    )

    # 3. Compare per-service
    changed = 0
    unchanged_high_conf = 0
    no_lookup = 0
    kept_due_to_low_conf = 0
    via_tokens = 0
    final_tid_counter: Counter = Counter()

    rows_for_print: list[tuple] = []
    for s in svc:
        original_tid = s["booksy_treatment_id"]
        original_tname = s.get("treatment_name") or ""
        n = normalize_name(s["name"])

        # Step 1: try exact
        match = exact_lookup.get(n)
        source = "exact" if match else None

        # Step 2: parent-constrained token fuzzy match (best signal — same
        # Booksy treatment family, so search space drops from 1000s to ~20 tids)
        parent_id = s.get("treatment_parent_id")
        if match is None and parent_id is not None:
            svc_tokens = tokenize(s["name"])
            family_lookup = parent_to_tokens.get(parent_id, [])
            if family_lookup:
                tok_match = infer_via_tokens(svc_tokens, family_lookup, min_jaccard=0.3)
                if tok_match:
                    match = tok_match
                    source = f"parent({parent_id})"
                    via_tokens += 1

        # Step 3: fallback to unconstrained token fuzzy match (catch-all)
        if match is None:
            svc_tokens = tokenize(s["name"])
            tok_match = infer_via_tokens(svc_tokens, token_lookup, min_jaccard=0.4)
            if tok_match:
                match = tok_match
                source = "tokens"
                via_tokens += 1

        if match is None:
            no_lookup += 1
            final_tid = original_tid
            status = "no-lookup"
            inferred_label = "—"
        else:
            inferred_tid, conf, total = match
            if conf < args.min_confidence:
                kept_due_to_low_conf += 1
                final_tid = original_tid
                status = f"low-conf({source} {conf:.0%},n={total})"
                inferred_label = f"tid={inferred_tid}"
            elif inferred_tid == original_tid:
                unchanged_high_conf += 1
                final_tid = original_tid
                status = f"match({source} {conf:.0%},n={total})"
                inferred_label = f"tid={inferred_tid}"
            else:
                changed += 1
                final_tid = inferred_tid
                status = f"OVERRIDE({source} {conf:.0%},n={total})"
                inferred_label = f"tid={inferred_tid}"

        final_tid_counter[final_tid] += 1
        rows_for_print.append((
            s["name"][:38],
            f"{original_tid} ({original_tname[:18]})",
            inferred_label,
            status,
        ))

    print(f"{'service name':<40}  {'original tid':<30}  {'inferred':<15}  status")
    print("-" * 120)
    for r in rows_for_print:
        print(f"{r[0]:<40}  {r[1]:<30}  {r[2]:<15}  {r[3]}")

    print()
    print("=" * 60)
    print(f"SUMMARY for {scrape['salon_name']}")
    print("=" * 60)
    print(f"  Total services:                 {len(svc)}")
    print(f"  Override (tid changed):         {changed}")
    print(f"  Match (tid unchanged):          {unchanged_high_conf}")
    print(f"  No lookup found:                {no_lookup}")
    print(f"  Kept due to low confidence:     {kept_due_to_low_conf}")
    print()
    print(f"  Distinct tids BEFORE: {len(set(s['booksy_treatment_id'] for s in svc))}")
    print(f"  Distinct tids AFTER:  {len(final_tid_counter)}")
    print()
    print(f"  Final tid distribution (top 10):")
    for tid, n in final_tid_counter.most_common(10):
        tname = next((r[1] for r in rows_for_print if str(tid) in r[1]), f"(tid {tid})")
        print(f"    tid={tid:>5}  n={n:>3}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
