"""Semantic market-context gathering for subject_only pricing rows.

When the subject service has no exact (variant_id, brand) match among
the selected competitors, we surface semantically similar competitor
services via embedding cosine similarity. The fact that
salon_scrape_services already carries an OpenAI text-embedding-3-small
vector per active service (Phase 1 backfill, 100% chain-head coverage)
makes this a single SQL round-trip per subject_only row.

This module REPLACES the previous regex-based brand/method/area
matcher. That approach required maintaining hardcoded patterns for
every possible brand (Red Touch, Thunder, PRIMEX, PRX T33, AQUASHINE,
Estgen, Modelka ONDA, X-Wave) and every treatment-concern phrasing
(usuwanie przebarwien, blizn, rozstepow, cellulitu, zmian skornych) —
brittle, unscalable, and silent on typos like "przebrawień" instead of
"przebarwień". The embedding approach works for any service name the
salon might write, in any language form, with any typo.
"""
from __future__ import annotations

import logging
from typing import Any

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# Threshold tuning notes (empirical, audit 34):
#   - 0.85+ → too strict, most rows empty.
#   - 0.75 → catches "Usuwanie przebrawień" → "Usuwanie przebarwień" 0.82
#     etc., but services with marketing-only language like "Modelka - ONDA"
#     (Beauty4ever's brand-specific name for RF body modeling) get 0
#     because competitors call the same treatment "Modelowanie ciała"
#     which embedding sees as semantically distant.
#   - 0.55 → captures the marketing-named services too (Modelka ONDA →
#     ESTETICAN's "Modelowanie owalu twarzy" 0.464 is borderline; tighter
#     than that and we drop legitimate alternatives). False positives
#     (e.g. Modelka → Modelowanie ust 0.498) get sorted to the bottom by
#     similarity DESC, and UI surfaces the score per row so the user can
#     judge match quality at a glance.
#   - 0.50 → noisy, even unrelated services creep in.
# Threshold can be tightened/loosened at call site for specific routes.
DEFAULT_MIN_SIMILARITY = 0.55
DEFAULT_LIMIT = 20

# 2026-05-17 (Faza 4b) — promote-to-comp-samples threshold. When subject_only
# fallback returns ≥ STRONG_MIN_COUNT services with similarity ≥
# STRONG_MIN_SIMILARITY, the pricing engine treats them as direct market
# comparison samples (computes percentiles, deviation_pct, recommended_action)
# instead of as soft "related" context. Empirical: "Red Touch twarz + szyja
# - PROMOCJA" subject → top embedding matches "RedTouch PRO Twarz+szyja"
# 0.90, "Laser Red Touch twarz-szyja" 0.86 — clearly the same treatment.
# Without this promotion, user sees row as "Brak dokładnego matcha · 20
# semantycznie podobnych" instead of "+75% vs mediana 1700 zł, raise".
STRONG_MIN_SIMILARITY = 0.78
STRONG_MIN_COUNT = 3


async def gather_market_context_samples(
    supabase: SupabaseService,
    subject_service_id: int,
    competitor_booksy_ids: list[int],
    *,
    limit: int = DEFAULT_LIMIT,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
) -> list[dict[str, Any]]:
    """Return semantically similar competitor services for a subject_only row.

    Args:
      supabase: shared SupabaseService.
      subject_service_id: salon_scrape_services.id of the subject service.
        Helper resolves its name_embedding internally — caller doesn't
        need to ship the 1536-dim vector.
      competitor_booksy_ids: scope to chain-head scrapes of these salons
        (typically the 15 selected competitors for the report).
      limit: cap on rows returned (default 30).
      min_similarity: cosine similarity floor [0, 1] (default 0.75).

    Returns:
      List of sample dicts matching competitor_pricing_comparisons.
      related_samples shape, sorted DESC by similarity. Empty when the
      subject service has no name_embedding or no competitor service
      crosses the threshold.

    Sample shape:
      {
        salon_id, salon_name, booksy_id,
        service_id, service_name,
        price_grosze, duration_minutes,
        relation: 'semantic_match',
        similarity: float in [0, 1],
      }
    """
    if subject_service_id is None or not competitor_booksy_ids:
        return []

    try:
        res = supabase.client.rpc(
            "fn_find_related_competitor_services",
            {
                "p_subject_service_id": int(subject_service_id),
                "p_competitor_booksy_ids": list(competitor_booksy_ids),
                "p_limit": int(limit),
                "p_min_similarity": float(min_similarity),
            },
        ).execute()
    except Exception as e:
        logger.warning(
            "gather_market_context_samples RPC failed (svc=%s): %s",
            subject_service_id, e,
        )
        return []

    out: list[dict[str, Any]] = []
    for row in res.data or []:
        price = row.get("price_grosze")
        if price is None:
            continue
        sim = row.get("similarity")
        try:
            sim_val = float(sim) if sim is not None else None
        except (TypeError, ValueError):
            sim_val = None
        out.append({
            "salon_id": row.get("salon_id"),
            "salon_name": row.get("salon_name") or "",
            "booksy_id": row.get("booksy_id"),
            "service_id": row.get("service_id"),
            "service_name": row.get("service_name") or "",
            "price_grosze": int(price),
            "duration_minutes": row.get("duration_minutes"),
            "relation": "semantic_match",
            "similarity": round(sim_val, 4) if sim_val is not None else None,
        })
    return out
