"""Cluster service-name embeddings within each Booksy treatment_id to
discover emergent variants.

Phase 3 of "no comparisons without embeddings" plan. Reads chain-head
services with embeddings + booksy_treatment_id, groups them by tid,
runs HDBSCAN over each group, materializes the resulting clusters as
treatment_variants rows.

Algorithm:
  1. For each treatment_id with >= MIN_SAMPLES_PER_TID chain-head
     services having an embedding, load all (id, name, embedding, price)
     tuples.
  2. L2-normalize embeddings so euclidean distance ≈ cosine distance.
  3. Run HDBSCAN(min_cluster_size=10, min_samples=5, metric='euclidean').
  4. For each cluster (label != -1):
     - Centroid = mean of normalized member embeddings, re-normalized.
     - Representative names = 5 closest to centroid (by cosine).
     - Canonical name = the single closest member name.
     - Price stats: min/p25/median/p75/max from member price_grosze.
  5. Upsert into treatment_variants (parent_treatment_id, cluster_label
     UNIQUE).

Noise points (HDBSCAN label=-1) are not stored as variants. The pipeline
(Phase 5) will fall through to tid-only grouping for services that don't
match any variant centroid via ANN lookup.

Idempotent: re-running rewrites all variants for tids that have enough
data. Old variants get deleted before insert for clean refresh.

Run: cd /home/booksy/webapps/bagent-booksyauditor && .venv/bin/python scripts/cluster_treatment_variants.py
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

import numpy as np
import hdbscan  # type: ignore[import-untyped]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Tids with fewer than this many chain-head services + embeddings get no
# clustering — pricing pipeline falls back to tid-only grouping for them.
MIN_SAMPLES_PER_TID = 50

# HDBSCAN parameters tuned for service-name embedding space:
#   min_cluster_size — smallest variant we accept. 10 members means the
#     pattern is seen in 10+ salons before becoming a recognized variant.
#   min_samples — HDBSCAN's "density required to be a cluster core". Higher
#     = more conservative (more noise points, fewer clusters). 5 is balanced.
MIN_CLUSTER_SIZE = 10
MIN_SAMPLES = 5

# How many representative names to store per variant for UI / QA.
REPRESENTATIVE_N = 5


def _l2_normalize(vec: np.ndarray) -> np.ndarray:
    """Normalize each row to unit length so euclidean dist ≈ cosine dist."""
    norms = np.linalg.norm(vec, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vec / norms


def load_services_for_tid(client: Any, tid: int) -> list[dict[str, Any]]:
    """Pull (id, name, embedding, price_grosze) tuples for chain-head services
    with the given treatment_id that have an embedding.

    Uses a dedicated RPC because raw select via PostgREST returns embeddings
    as JSON arrays — slow for thousands of rows. The RPC streams binary
    vector data more efficiently.
    """
    res = client.rpc(
        "get_chain_head_services_for_tid_clustering",
        {"p_treatment_id": tid},
    ).execute()
    return list(res.data or [])


def cluster_tid(
    tid: int,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Run HDBSCAN on rows, return list of cluster summaries.

    Each summary: {cluster_label, canonical_name, centroid (np.ndarray),
    representative_names, sample_count, price_min, p25, median, p75, max}.
    """
    if len(rows) < MIN_SAMPLES_PER_TID:
        return []

    names = [r["name"] for r in rows]
    prices = np.array(
        [int(r["price_grosze"]) if r.get("price_grosze") is not None else -1
         for r in rows],
        dtype=np.int64,
    )
    # Embeddings come back as Python list-of-floats per row.
    raw_emb = np.array([r["embedding"] for r in rows], dtype=np.float32)
    emb = _l2_normalize(raw_emb)

    # HDBSCAN with euclidean on L2-normalized vectors = cosine clustering.
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=MIN_CLUSTER_SIZE,
        min_samples=MIN_SAMPLES,
        metric="euclidean",
        cluster_selection_method="eom",
        core_dist_n_jobs=-1,
    )
    labels = clusterer.fit_predict(emb)

    out: list[dict[str, Any]] = []
    unique_labels = sorted(set(labels) - {-1})

    for lbl in unique_labels:
        member_idx = np.where(labels == lbl)[0]
        member_emb = emb[member_idx]
        # Centroid = mean of normalized members, re-normalized for consistency.
        centroid = member_emb.mean(axis=0)
        centroid /= max(np.linalg.norm(centroid), 1e-9)

        # Distance from each member to centroid (cosine = 1 - dot for normalized).
        cos_sim = member_emb @ centroid
        # Top representatives (highest similarity to centroid)
        rep_order = np.argsort(-cos_sim)[:REPRESENTATIVE_N]
        representatives = [names[member_idx[i]] for i in rep_order]
        canonical_name = representatives[0]

        member_prices = prices[member_idx]
        priced = member_prices[member_prices >= 0]
        if len(priced) > 0:
            pmin = int(priced.min())
            p25 = int(np.percentile(priced, 25))
            pmed = int(np.percentile(priced, 50))
            p75 = int(np.percentile(priced, 75))
            pmax = int(priced.max())
        else:
            pmin = p25 = pmed = p75 = pmax = None  # type: ignore[assignment]

        out.append({
            "cluster_label": int(lbl),
            "canonical_name": canonical_name,
            "centroid": centroid.tolist(),
            "representative_names": representatives,
            "sample_count": int(len(member_idx)),
            "price_min_grosze": pmin,
            "price_p25_grosze": p25,
            "price_median_grosze": pmed,
            "price_p75_grosze": p75,
            "price_max_grosze": pmax,
        })
    return out


def upsert_variants(
    client: Any,
    tid: int,
    variants: list[dict[str, Any]],
) -> None:
    """Replace all variants for tid with the new clustering result.

    Delete-then-insert is fine here because variant_id references are
    managed in Phase 4 (salon_scrape_services.variant_id can be re-matched
    via match_service_to_variant after refresh).
    """
    # Step 1: delete existing
    client.table("treatment_variants").delete().eq(
        "parent_treatment_id", tid,
    ).execute()
    # Step 2: insert fresh rows
    rows = [
        {
            "parent_treatment_id": tid,
            "cluster_label": v["cluster_label"],
            "canonical_variant_name": v["canonical_name"],
            "centroid_embedding": v["centroid"],
            "representative_names": v["representative_names"],
            "sample_count": v["sample_count"],
            "price_min_grosze": v["price_min_grosze"],
            "price_p25_grosze": v["price_p25_grosze"],
            "price_median_grosze": v["price_median_grosze"],
            "price_p75_grosze": v["price_p75_grosze"],
            "price_max_grosze": v["price_max_grosze"],
        }
        for v in variants
    ]
    if rows:
        client.table("treatment_variants").insert(rows).execute()


def main() -> None:
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Get list of tids with enough chain-head + embedded services to cluster.
    candidates = client.rpc(
        "get_tids_ready_for_clustering",
        {"p_min_samples": MIN_SAMPLES_PER_TID},
    ).execute().data or []

    logger.info("Found %d tids ready for clustering", len(candidates))

    t_start = time.time()
    total_variants = 0
    skipped = 0
    for entry in candidates:
        tid = int(entry["treatment_id"])
        n_services = int(entry.get("service_count") or 0)
        logger.info("[tid=%d] loading %d services...", tid, n_services)
        rows = load_services_for_tid(client, tid)
        if len(rows) < MIN_SAMPLES_PER_TID:
            logger.info("[tid=%d] only %d rows after filtering — skip", tid, len(rows))
            skipped += 1
            continue
        t0 = time.time()
        variants = cluster_tid(tid, rows)
        dt = time.time() - t0
        if not variants:
            logger.info("[tid=%d] no clusters found (HDBSCAN noise-only) — skip", tid)
            skipped += 1
            continue
        upsert_variants(client, tid, variants)
        total_variants += len(variants)
        logger.info(
            "[tid=%d] %d clusters in %.1fs — top: %s",
            tid, len(variants), dt,
            ", ".join(f"{v['canonical_name'][:40]} (n={v['sample_count']})"
                      for v in variants[:3]),
        )

    dt = time.time() - t_start
    logger.info(
        "DONE: %d total variants across %d tids in %.0fs (%d skipped)",
        total_variants, len(candidates) - skipped, dt, skipped,
    )


if __name__ == "__main__":
    main()
