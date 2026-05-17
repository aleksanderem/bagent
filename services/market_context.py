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
#   - 0.85+ → very strict (same words, same intent). Most subject services
#     end up with 0 related, defeats the purpose.
#   - 0.75 → ~10-30 matches per subject service. Top-of-list dominated by
#     semantically real matches ("Usuwanie przebrawień" → "Usuwanie
#     przebarwień" 0.82, "Usuwanie blizn" 0.76). Bottom may include
#     noise ("Usuwanie rzęs" 0.75) — but sorted DESC by similarity, so
#     the user sees real ones first.
#   - 0.70 → noisy.
# Threshold can be lowered/raised at call site for specific routes.
DEFAULT_MIN_SIMILARITY = 0.75
DEFAULT_LIMIT = 30


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
