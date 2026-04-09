"""Deterministic competitor analysis computation (Comp Etap 4).

Entry point: `compute_competitor_analysis(audit_id, tier, selection_mode)`
orchestrates the computation of a complete competitor report from already-
ingested Supabase data. Zero AI calls — pure math over scraped data.

Pipeline steps:
  1. Select competitors via pipelines.competitor_selection (Comp Etap 1)
  2. Create competitor_reports row + persist competitor_matches
  3. Load full subject + competitor data (scrapes, services, reviews,
     top_services, open_hours)
  4. Compute pricing_comparisons per treatment_id (with Versum mapping
     fallback for services with NULL booksy_treatment_id)
  5. Compute service_gaps (missing + unique_usp)
  6. Compute ~28 dimensional_scores with market distribution per dimension
  7. Update competitor_reports.status → 'completed'

AI synthesis (positioning narrative, SWOT, recommendations) is a separate
Etap 5 pipeline that reads the tables populated here.

See docs/plans/2026-04-08-competitor-report-pipeline.md sections
"Pipeline steps (Comp Etap 2-5)" and "Dimensional scores — pełna lista".
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from pipelines.competitor_dimensional_scores import (
    DIMENSION_METADATA,
    compute_all_dimensions_for_salon,
    compute_percentiles,
    compute_subject_percentile,
)
from pipelines.competitor_selection import CompetitorCandidate, select_competitors
from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def compute_competitor_analysis(
    audit_id: str,
    tier: str = "base",
    selection_mode: str = "auto",
    target_count: int = 5,
    on_progress: ProgressCallback | None = None,
    supabase: SupabaseService | None = None,
    convex_user_id: str = "unknown",
) -> int:
    """Compute a full competitor analysis for an audit and persist to Supabase.

    Returns the integer id of the created `competitor_reports` row.

    Args:
        audit_id: convex_audit_id of the subject audit
        tier: 'base' or 'premium' (stored on competitor_reports.tier)
        selection_mode: 'auto' or 'manual' (stored on competitor_reports.selection_mode)
        target_count: how many competitors to select in auto mode (default 5)
        on_progress: optional async progress callback (progress, message)
        supabase: optional SupabaseService instance (for tests). Defaults new.
        convex_user_id: Convex user id to store on the report row. Pipeline
            callers should pass the real user id from the webhook payload.

    Raises:
        RuntimeError: if no competitor candidates could be selected
        ValueError: if the subject audit_id is not found
    """
    progress = on_progress or _noop_progress
    service = supabase or SupabaseService()

    # ── Step 1: Select competitors (Comp Etap 1) ──
    await progress(5, "Selekcja konkurentów...")
    candidates = await select_competitors(
        audit_id, target_count=target_count, mode=selection_mode, supabase=service,
    )
    if not candidates:
        raise RuntimeError(
            f"No competitor candidates selected for audit_id={audit_id!r}"
        )
    logger.info(
        "Etap 4: selected %d competitors for audit=%s",
        len(candidates), audit_id,
    )

    # ── Step 2: Create competitor_reports + matches ──
    await progress(
        15, f"Tworzenie raportu ({len(candidates)} konkurentów)",
    )
    # Load the subject to get the internal salon_id for the FK
    subject_stub = await service.get_subject_salon_for_audit(audit_id)
    if subject_stub is None:
        raise ValueError(f"Subject salon not found for audit_id={audit_id!r}")
    subject_salon_id = subject_stub["salon_id"]

    report_id = await service.create_competitor_report(
        convex_audit_id=audit_id,
        convex_user_id=convex_user_id,
        subject_salon_id=subject_salon_id,
        tier=tier,
        selection_mode=selection_mode,
        competitor_count=len(candidates),
        metadata={
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pipeline": "competitor_analysis",
            "pipeline_version": "etap4",
        },
    )
    logger.info("Etap 4: created competitor_reports id=%s", report_id)

    # Wipe any stale children from a prior re-run before inserting new rows
    await service.delete_competitor_report_children(report_id)

    n_matches = await service.insert_competitor_matches(report_id, candidates)
    logger.info("Etap 4: inserted %d competitor_matches", n_matches)

    # ── Step 3: Load subject + competitors full data ──
    await progress(25, "Ładowanie danych subject + konkurentów...")
    subject_data = await service.get_subject_full_data(audit_id)

    competitor_booksy_ids = [c.booksy_id for c in candidates]
    competitor_data_map = await service.get_competitor_full_data(competitor_booksy_ids)
    # Keep only candidates whose data loaded successfully; align bucket metadata
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]] = []
    for c in candidates:
        data = competitor_data_map.get(c.booksy_id)
        if data is None:
            logger.warning(
                "Competitor booksy_id=%s missing scrape data — dropped from computations",
                c.booksy_id,
            )
            continue
        aligned_competitors.append((c, data))

    if not aligned_competitors:
        logger.error(
            "Etap 4: no competitors with loadable data for report=%s — marking failed",
            report_id,
        )
        await service.update_competitor_report_status(
            report_id, "failed",
            metadata_extras={"error": "no competitor data loaded"},
        )
        raise RuntimeError(
            f"All {len(candidates)} selected competitors have no scrape data"
        )

    # Apply versum_service_mappings to services with NULL booksy_treatment_id
    # BEFORE doing pricing comparisons. Applies to subject AND competitors.
    await progress(35, "Mapowanie Versum → treatment_id...")
    salon_ids_for_mapping: list[int] = [subject_data["salon_id"]]
    for _, cdata in aligned_competitors:
        if cdata.get("salon_id") is not None:
            salon_ids_for_mapping.append(cdata["salon_id"])
    versum_map = await service.get_versum_mappings(salon_ids_for_mapping)
    _apply_versum_mappings(subject_data, versum_map)
    for _, cdata in aligned_competitors:
        _apply_versum_mappings(cdata, versum_map)

    # ── Step 4: Pricing comparisons ──
    await progress(45, "Pricing comparisons per treatment_id...")
    pricing_rows = _compute_pricing_comparisons(
        report_id, subject_data, aligned_competitors,
    )
    n_pricing = await service.insert_competitor_pricing_comparisons(pricing_rows)
    logger.info("Etap 4: inserted %d pricing_comparisons", n_pricing)

    # ── Step 5: Service gaps ──
    await progress(60, "Service gap analysis (missing + unique USP)...")
    gap_rows = _compute_service_gaps(
        report_id, subject_data, aligned_competitors,
    )
    n_gaps = await service.insert_competitor_service_gaps(gap_rows)
    logger.info("Etap 4: inserted %d service_gaps", n_gaps)

    # ── Step 6: Dimensional scores ──
    await progress(70, "Dimensional scores (28 wymiarów)...")
    dim_rows = _compute_dimensional_scores(
        report_id, subject_data, aligned_competitors,
    )
    n_dims = await service.insert_competitor_dimensional_scores(dim_rows)
    logger.info("Etap 4: inserted %d dimensional_scores", n_dims)

    # ── Step 7: Extract active promotions ──
    await progress(85, "Ekstrakcja aktywnych promocji...")
    all_booksy_ids = [subject_data["booksy_id"]] + competitor_booksy_ids
    promo_map = await service.get_active_promotions(all_booksy_ids)
    active_promotions = _build_active_promotions(
        subject_data["booksy_id"], promo_map, candidates,
    )
    n_promos_subject = len(active_promotions.get("subject", []))
    n_promos_competitors = sum(
        len(v) for v in active_promotions.get("competitors", {}).values()
    )
    logger.info(
        "Etap 4: found %d subject promos, %d competitor promos",
        n_promos_subject, n_promos_competitors,
    )

    # ── Step 8: Mark completed ──
    await progress(95, "Finalizacja raportu...")
    await service.update_competitor_report_status(
        report_id,
        "completed",
        metadata_extras={
            "etap4_stats": {
                "competitor_matches": n_matches,
                "pricing_comparisons": n_pricing,
                "service_gaps": n_gaps,
                "dimensional_scores": n_dims,
                "competitors_dropped_no_data": len(candidates) - len(aligned_competitors),
                "promos_subject": n_promos_subject,
                "promos_competitors": n_promos_competitors,
            },
        },
        report_data_extras={"activePromotions": active_promotions},
    )

    await progress(
        100,
        f"Gotowe: {n_matches} matches, {n_pricing} pricing, "
        f"{n_gaps} gaps, {n_dims} dimensions",
    )
    return report_id


# ---------------------------------------------------------------------------
# Versum mapping application
# ---------------------------------------------------------------------------


def _apply_versum_mappings(
    salon_data: dict[str, Any],
    versum_map: dict[tuple[int, int], int],
) -> None:
    """For each service in salon_data.services with NULL booksy_treatment_id,
    try to resolve it via versum_service_mappings. Mutates the services
    in place — services that match get their booksy_treatment_id filled in.

    Salons whose services all have treatment_ids (native Booksy) end up
    with zero mapping applications. This is the graceful-degradation path
    for Versum salons per plan doc section 4.
    """
    salon_id = salon_data.get("salon_id")
    if salon_id is None or not versum_map:
        return
    services = salon_data.get("services") or []
    applied = 0
    for svc in services:
        if svc.get("booksy_treatment_id") is not None:
            continue
        bsid = svc.get("booksy_service_id")
        if bsid is None:
            continue
        tid = versum_map.get((int(salon_id), int(bsid)))
        if tid is not None:
            svc["booksy_treatment_id"] = tid
            svc["_versum_mapped"] = True
            applied += 1
    if applied:
        logger.info(
            "Applied %d Versum mappings for salon_id=%s", applied, salon_id,
        )


# ---------------------------------------------------------------------------
# Pricing comparisons
# ---------------------------------------------------------------------------


def _active_services_with_treatment(
    services: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Return {treatment_id: service_row} for active services with a treatment_id.

    When a salon has multiple services under the same treatment_id, we take
    the one with the LOWEST price_grosze (conservative: picks the base
    variant when variants exist). Services missing price_grosze are kept
    only if no priced service exists for that treatment.
    """
    out: dict[int, dict[str, Any]] = {}
    for svc in services:
        if not svc.get("is_active", True):
            continue
        tid = svc.get("booksy_treatment_id")
        if tid is None:
            continue
        tid = int(tid)
        existing = out.get(tid)
        if existing is None:
            out[tid] = svc
            continue
        # Prefer the one with a price, then the lower price
        existing_price = existing.get("price_grosze")
        new_price = svc.get("price_grosze")
        if existing_price is None and new_price is not None:
            out[tid] = svc
        elif (
            existing_price is not None
            and new_price is not None
            and new_price < existing_price
        ):
            out[tid] = svc
    return out


def _compute_pricing_comparisons(
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Compute per-treatment pricing comparison rows.

    For each treatment_id that the subject offers AND at least 2
    competitors (counts_in_aggregates=True) also offer, compute the
    market distribution and return a dict ready for insertion into
    competitor_pricing_comparisons.
    """
    subject_svcs = _active_services_with_treatment(subject_data.get("services") or [])
    if not subject_svcs:
        logger.info("Etap 4: subject has no services with treatment_id — no pricing comparisons")
        return []

    # Build per-treatment competitor price lists (only for counts_in_aggregates competitors)
    competitor_prices_by_tid: dict[int, list[int]] = {}
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        comp_svcs = _active_services_with_treatment(cdata.get("services") or [])
        for tid, svc in comp_svcs.items():
            price = svc.get("price_grosze")
            if price is None:
                continue
            competitor_prices_by_tid.setdefault(tid, []).append(int(price))

    rows: list[dict[str, Any]] = []
    for tid, subject_svc in subject_svcs.items():
        market_prices = competitor_prices_by_tid.get(tid, [])
        # Need at least 2 competitors with a price for the comparison to be meaningful
        if len(market_prices) < 2:
            continue
        subject_price = subject_svc.get("price_grosze")
        if subject_price is None:
            # Include the row but flag: subject has no price for this treatment
            # Skip for now — no way to compute deviation without a subject price
            continue

        percentiles = compute_percentiles([float(p) for p in market_prices])
        median = percentiles["market_p50"]
        deviation_pct = (
            ((float(subject_price) - median) / median * 100.0) if median > 0 else 0.0
        )
        recommended_action = _classify_pricing_action(deviation_pct)
        subject_pct = compute_subject_percentile(
            float(subject_price), [float(p) for p in market_prices],
        )

        rows.append({
            "report_id": report_id,
            "booksy_treatment_id": tid,
            "treatment_name": (
                subject_svc.get("treatment_name") or subject_svc.get("name") or "Unknown"
            ),
            "treatment_parent_id": subject_svc.get("treatment_parent_id"),
            "subject_price_grosze": int(subject_price),
            "subject_is_from_price": bool(subject_svc.get("is_from_price") or False),
            "subject_duration_minutes": subject_svc.get("duration_minutes"),
            "market_min_grosze": int(percentiles["market_min"]),
            "market_p25_grosze": int(percentiles["market_p25"]),
            "market_median_grosze": int(percentiles["market_p50"]),
            "market_p75_grosze": int(percentiles["market_p75"]),
            "market_max_grosze": int(percentiles["market_max"]),
            "subject_percentile": round(subject_pct, 2),
            "deviation_pct": round(deviation_pct, 2),
            "sample_size": len(market_prices),
            "recommended_action": recommended_action,
        })

    return rows


def _classify_pricing_action(deviation_pct: float) -> str:
    """Classify the recommended pricing action given subject's deviation.

    deviation_pct = (subject - market_median) / market_median * 100.
    - deviation < -15%: subject priced too low, recommend 'raise'
    - deviation > +20%: subject priced too high, recommend 'lower'
    - otherwise: 'hold' (aligned with market)
    """
    if deviation_pct < -15.0:
        return "raise"
    if deviation_pct > 20.0:
        return "lower"
    return "hold"


# ---------------------------------------------------------------------------
# Service gaps
# ---------------------------------------------------------------------------


def _compute_service_gaps(
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Compute service gap rows (missing + unique_usp).

    - 'missing': top 10 treatments that ≥1 counts_in_aggregates competitor
      offers but the subject does not. Popularity score = competitor_count
      weighted by review mentions (limited due to 3-sample review cap).
    - 'unique_usp': up to 5 treatments only the subject offers (no
      counts_in_aggregates competitor has them). Popularity score = 1.0.
    """
    subject_svcs = _active_services_with_treatment(subject_data.get("services") or [])
    subject_tids = set(subject_svcs.keys())

    # Per-treatment stats across competitors
    competitor_counts: dict[int, int] = {}
    competitor_prices: dict[int, list[int]] = {}
    treatment_names: dict[int, str] = {}
    treatment_parents: dict[int, int | None] = {}

    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        comp_svcs = _active_services_with_treatment(cdata.get("services") or [])
        for tid, svc in comp_svcs.items():
            competitor_counts[tid] = competitor_counts.get(tid, 0) + 1
            if tid not in treatment_names:
                treatment_names[tid] = (
                    svc.get("treatment_name") or svc.get("name") or "Unknown"
                )
            if tid not in treatment_parents:
                treatment_parents[tid] = svc.get("treatment_parent_id")
            price = svc.get("price_grosze")
            if price is not None:
                competitor_prices.setdefault(tid, []).append(int(price))

    # Count review mentions per treatment across competitors (review.services
    # is a jsonb array like [{id, name, treatment_id}])
    review_mentions: dict[int, int] = {}
    for _, cdata in aligned_competitors:
        reviews = cdata.get("reviews") or []
        for r in reviews:
            if not isinstance(r, dict):
                continue
            svc_refs = r.get("services") or []
            if not isinstance(svc_refs, list):
                continue
            for ref in svc_refs:
                if not isinstance(ref, dict):
                    continue
                tid = ref.get("treatment_id")
                if isinstance(tid, int):
                    review_mentions[tid] = review_mentions.get(tid, 0) + 1

    # ── Type A: missing (competitors have, subject doesn't) ──
    missing: list[dict[str, Any]] = []
    for tid, count in competitor_counts.items():
        if tid in subject_tids:
            continue
        avg_price_grosze = (
            sum(competitor_prices[tid]) // len(competitor_prices[tid])
            if tid in competitor_prices and competitor_prices[tid]
            else None
        )
        # popularity_score: competitor_count × (review mentions / 100) + competitor_count
        # Weight towards count since reviews are rare (3-sample cap)
        mentions = review_mentions.get(tid, 0)
        popularity = float(count) * (1.0 + mentions / 100.0)
        missing.append({
            "report_id": report_id,
            "gap_type": "missing",
            "booksy_treatment_id": tid,
            "treatment_name": treatment_names.get(tid, "Unknown"),
            "treatment_parent_id": treatment_parents.get(tid),
            "competitor_count": count,
            "avg_price_grosze": avg_price_grosze,
            "popularity_score": round(min(popularity, 999.99), 2),
            "sort_order": 0,  # fixed after sorting
        })
    missing.sort(key=lambda r: (-(r["popularity_score"] or 0), -(r["competitor_count"] or 0)))
    missing = missing[:10]
    for idx, row in enumerate(missing):
        row["sort_order"] = idx

    # ── Type B: unique USPs (subject has, no competitor does) ──
    unique_usps: list[dict[str, Any]] = []
    for tid, svc in subject_svcs.items():
        if tid in competitor_counts:
            continue
        unique_usps.append({
            "report_id": report_id,
            "gap_type": "unique_usp",
            "booksy_treatment_id": tid,
            "treatment_name": (
                svc.get("treatment_name") or svc.get("name") or "Unknown"
            ),
            "treatment_parent_id": svc.get("treatment_parent_id"),
            "competitor_count": 0,
            "avg_price_grosze": svc.get("price_grosze"),
            "popularity_score": 1.0,
            "sort_order": 0,
        })
    # Top 5 by price (higher-priced uniques are typically more valuable USPs)
    unique_usps.sort(
        key=lambda r: -(r["avg_price_grosze"] or 0),
    )
    unique_usps = unique_usps[:5]
    for idx, row in enumerate(unique_usps):
        row["sort_order"] = idx

    return missing + unique_usps


# ---------------------------------------------------------------------------
# Active promotions
# ---------------------------------------------------------------------------


def _build_active_promotions(
    subject_booksy_id: int,
    promo_map: dict[int, list[dict[str, Any]]],
    candidates: list[CompetitorCandidate],
) -> dict[str, Any]:
    """Build the activePromotions dict for report_data.

    Shape:
      {
        "subject": [{serviceName, originalPrice, promoPrice, discountPct}, ...],
        "competitors": {
          "<salon_id>": [{...}, ...],
        }
      }

    Keys competitor entries by the string salon_id (serializable to JSON).
    """
    subject_promos = promo_map.get(subject_booksy_id, [])
    competitor_promos: dict[str, list[dict[str, Any]]] = {}
    for c in candidates:
        promos = promo_map.get(c.booksy_id, [])
        if promos:
            competitor_promos[str(c.booksy_id)] = promos
    return {
        "subject": subject_promos,
        "competitors": competitor_promos,
    }


# ---------------------------------------------------------------------------
# Dimensional scores
# ---------------------------------------------------------------------------


def _compute_dimensional_scores(
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Compute dimensional score rows for every dimension in DIMENSION_METADATA.

    For each dimension:
    1. Compute subject's value (from subject_data)
    2. Compute each aggregable competitor's value
    3. Build the market distribution (p25/p50/p75/min/max)
    4. Compute subject_percentile
    5. Emit a row
    """
    subject_values = compute_all_dimensions_for_salon(subject_data)

    # Per-competitor dimension values, only for counts_in_aggregates
    competitor_values_per_dim: dict[str, list[float]] = {
        dim: [] for _, dim, _, _ in DIMENSION_METADATA
    }
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        cvals = compute_all_dimensions_for_salon(cdata)
        for dim_name, val in cvals.items():
            if dim_name in competitor_values_per_dim:
                competitor_values_per_dim[dim_name].append(val)

    rows: list[dict[str, Any]] = []
    for idx, (category, dim, unit, better_is_higher) in enumerate(DIMENSION_METADATA):
        subject_val = float(subject_values.get(dim, 0.0))
        market_vals = competitor_values_per_dim.get(dim, [])
        percentiles = compute_percentiles(market_vals)
        subject_pct = compute_subject_percentile(subject_val, market_vals)
        rows.append({
            "report_id": report_id,
            "dimension": dim,
            "subject_value": round(subject_val, 2),
            "market_min": round(percentiles["market_min"], 2),
            "market_p25": round(percentiles["market_p25"], 2),
            "market_p50": round(percentiles["market_p50"], 2),
            "market_p75": round(percentiles["market_p75"], 2),
            "market_max": round(percentiles["market_max"], 2),
            "subject_percentile": round(subject_pct, 2),
            "better_is_higher": better_is_higher,
            "unit": unit,
            "category": category,
            "sort_order": idx,
        })
    return rows
