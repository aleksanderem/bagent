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
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from pipelines.competitor_dimensional_scores import (
    DIMENSION_METADATA,
    compute_all_dimensions_for_salon,
    compute_percentiles,
    compute_subject_percentile,
)
from pipelines.competitor_selection import CompetitorCandidate, select_competitors
from services.pricing_verification import (
    VERIFICATION_THRESHOLD_PCT,
    compute_name_embedding_similarity,
    detect_package_keyword,
    should_drop_from_display,
    verify_pricing_comparison,
)
from services.pair_verification import (
    _normalize_pair_name,
    verify_service_pairs,
)
from services.hidden_service_inference import (
    DEFAULT_MIN_CONFIDENCE as HIDDEN_MIN_CONFIDENCE,
    GeminiLLMClient,
    infer_hidden_services_batch,
)
from services.supabase import SupabaseService
from services.taxonomy_inference import infer_and_apply

from config import settings

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

    # LLM-assisted taxonomy inference for services z NULL booksy_treatment_id
    # (po Versum mapping) — Booksy nie skategoryzował tych usług, więc
    # standardowy pipeline by je dropnął. Z LLM-inferred tid (centroidy
    # mv_booksy_treatment_centroids + OpenAI gpt-4o-mini disambiguation)
    # wprowadzamy je do pricing matrix, pricing comparisons i dimensional
    # scores. UI oznacza je badgem "kategoria z AI" przez inferred_treatment_id
    # field (downstream consumer może to forwardować).
    await progress(38, "AI kategoryzacja usług bez taxonomy match...")
    # Outer try/except is deliberate: pipeline-wide robustness — one salon
    # with bad data shouldn't kill the whole audit. The INNER routing logic
    # (_resolve_service_taxonomy) raises hard on every step per directive
    # 2026-05-16 (Bugsink captures), so root causes still surface — only
    # the orchestration boundary swallows.
    try:
        total_overridden = await _apply_llm_taxonomy_to_null_tid_services(
            service, subject_data.get("services") or [], label="subject",
            audit_id=audit_id,
        )
        for _, cdata in aligned_competitors:
            total_overridden += await _apply_llm_taxonomy_to_null_tid_services(
                service, cdata.get("services") or [],
                label=f"competitor booksy_id={cdata.get('booksy_id')}",
                audit_id=audit_id,
            )
        logger.info(
            "Etap 4: taxonomy routing applied to %d NULL-tid services "
            "(subject + %d competitors)",
            total_overridden, len(aligned_competitors),
        )
    except Exception as e:
        logger.warning(
            "Taxonomy routing for matrix expansion failed (%s) — "
            "continuing with raw tids",
            e,
        )

    # Taxonomy inference: correct mis-tagged booksy_treatment_id values for
    # subject + each competitor using crowd lookup (migration 042 — RPC
    # infer_treatment_id pulled from mv_treatment_name_lookup). Many salon
    # owners pick the wrong tid in Booksy panel (e.g. all manicure variants
    # tagged as "Paznokcie żelowe"); this re-anchors them on what the
    # majority of similarly-named services in our DB use. Idempotent —
    # services whose original tid matches inferred get a no-op, and entries
    # with specificity markers (mega/3d/akryl/...) keep their original.
    await progress(40, "Inferencja taxonomy z crowd lookup...")
    try:
        await infer_and_apply(
            service, subject_data.get("services") or [], label="subject",
        )
        for _, cdata in aligned_competitors:
            await infer_and_apply(
                service, cdata.get("services") or [],
                label=f"competitor booksy_id={cdata.get('booksy_id')}",
            )
    except Exception as e:
        # Non-fatal: if RPC missing (migration 042 not yet applied), or any
        # other failure, fall through to raw Booksy tids. Pipeline keeps
        # running with degraded precision rather than blowing up.
        logger.warning(
            "Taxonomy inference failed (continuing with raw tids): %s", e,
        )

    # ── Step 4: Pricing comparisons ──
    await progress(45, "Pricing comparisons per treatment_id...")
    pricing_rows = await _compute_pricing_comparisons(
        service, report_id, subject_data, aligned_competitors,
        audit_id=audit_id,
    )
    n_pricing = await service.insert_competitor_pricing_comparisons(pricing_rows)
    logger.info("Etap 4: inserted %d pricing_comparisons", n_pricing)

    # ── Step 4.5 (Faza 8a 2026-05-17): aggregate LLM-verified matches
    # per competitor + re-bucket. Replaces the composite_score_v2 signal
    # ("they look similar") with ground truth ("they actually offer the
    # same services in the same scope"). Low-verified competitors are
    # marked as 'aspirational' or 'excluded' so the rich UI surfaces a
    # cleaner final list of true competition. ──
    await progress(56, "Re-bucketing konkurentów wg verified matches...")
    await _aggregate_verified_match_counts(service, report_id, pricing_rows)

    # ── Step 5: Service gaps ──
    await progress(60, "Service gap analysis (missing + unique USP)...")
    gap_rows = await _compute_service_gaps(
        service, report_id, subject_data, aligned_competitors,
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

    # ── Step 6.7 (Faza 8b 2026-05-17): package economics analysis. For
    # every subject service that's a package or area-bundle, find the
    # single-session same-area equivalent at the SAME salon and compute
    # discount %. Surfaces as "Uczciwość pakietów" section in the rich
    # UI — flags fake-promo packages where Beauty4ever charges identical
    # per-session price to singles, plus genuine discounts and the
    # rare overpriced bundle. ──
    await progress(78, "Analiza uczciwości pakietów i bundle'i...")
    await _analyze_subject_packages(service, report_id, subject_data)

    # ── Step 6.5: Hidden services detection ──
    # Subject services których nazwa NIE zawiera generycznej nazwy
    # procedury (klient szukający „depilacja laserowa" ich nie znajdzie
    # w wyszukiwarce Booksy), ale opis mówi co to za zabieg. Beauty4ever
    # robi to systematycznie z brand-name'ami (Thunder, Onda, Light&Bright,
    # Plexr, X-Wave, EMBODY, DR CYJ, Red Touch) — usługi technicznie są
    # w cenniku ale niewidoczne dla 95% wyszukiwań. To strzał w stopę
    # właścicielowi salonu.
    await progress(80, "Detekcja ukrytych przed wyszukiwarką usług...")
    hidden_services = _detect_hidden_services(subject_data.get("services") or [])
    if hidden_services:
        logger.info(
            "Etap 4: detected %d hidden services (brand-name only, missing "
            "generic procedure in title — invisible to Booksy search)",
            len(hidden_services),
        )
        # Enrich z taxonomy LLM inference — sugerujemy realną kategorię
        # Booksy zamiast wyniku keyword mapping. POC pokazał: Thunder →
        # "Depilacja ciała" (LLM 0.95), Light&Bright → "Fotoodmładzanie"
        # (LLM 0.88), Modelka-ONDA → "Zabiegi na ciało i modelowanie
        # sylwetki" (LLM 0.95). Keyword mapping nadal jest fallback'iem
        # gdy LLM zwróci unfixable.
        try:
            hidden_services = await _enhance_hidden_services_with_inference(
                hidden_services, service,
            )
            llm_count = sum(
                1 for h in hidden_services if h.get("inference_method") == "llm"
            )
            logger.info(
                "Etap 4: LLM inference applied to %d/%d hidden services",
                llm_count, len(hidden_services),
            )
        except Exception as exc:
            logger.warning(
                "Etap 4: hidden services LLM inference failed (%s) — "
                "falling back to keyword mapping for all",
                exc,
            )

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
        report_data_extras={
            "activePromotions": active_promotions,
            "hiddenServices": hidden_services,
        },
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


# Subject service names that signal a promotional listing — exclude from
# pricing comparisons, otherwise the discounted price compares against
# competitors' full prices and generates false "raise price" recommendations.
# detect_package_keyword from pricing_verification handles pakiet/Nx/×N/N
# zabiegów; this is a focused check for explicit promotion markers.
_PROMO_MARKERS = re.compile(
    r"\b(?:PROMOCJA|PROMOCJI|PROMO|RABAT|AKCJA|OKAZJA|TANIO|TANIEJ|"
    r"WYPRZEDA[ŻZ]\w*|NOWO[ŚS][CĆ]|HAPPY\s+HOUR|WALENTYNK\w*|"
    r"DZIE[ŃN]\s+KOBIET)\b",
    re.IGNORECASE,
)


def _has_promo_marker(name: str) -> bool:
    """True if service name contains an explicit promotion marker (PROMOCJA,
    RABAT, OKAZJA, …). Used by `_active_services_with_variant` to keep
    promotional listings out of price comparisons — they would compare
    a discounted price against competitors' regular cennik and inflate
    'raise price' rekomendacje."""
    if not name:
        return False
    return bool(_PROMO_MARKERS.search(name))


def _duration_bucket(duration_minutes: int | None) -> str:
    """Categoryzuje czas trwania usługi do kompatybilnych bucketów dla
    pricing comparison. Empirycznie (Beauty4ever vs Skin&Body Care 2026-05-16):
    SBC "1 Fokus" 15min za 99 zł porównywane do Beauty4ever single session
    60min za 200 zł powodowało false +100% deviation → drop przez verification.
    Z bucketów porównujemy tylko same-bucket services.

    Buckets:
      short   ≤30min   — trial / single-zone / mini session
      medium  31-90min — standard pojedyncza sesja
      long    >90min   — extended / multi-step zabieg
      unknown NULL     — własny bucket bo NIE wiemy czy to apples vs oranges
    """
    if duration_minutes is None:
        return "unknown"
    if duration_minutes <= 30:
        return "short"
    if duration_minutes <= 90:
        return "medium"
    return "long"


def _active_services_with_variant(
    services: list[dict[str, Any]],
) -> dict[tuple[int, int, str], dict[str, Any]]:
    """Return {(treatment_id, variant_id): service_row} for active services
    that have BOTH a treatment_id AND a variant_id assigned.

    Phase 5 of "no comparisons without embeddings": pricing comparisons
    are grouped by (tid, variant_id) instead of tid alone. This eliminates
    the subgroup confusion that produced false signals like "Botoks raise"
    for a salon whose Botoks listings include a 100 zł brwi-stylizacja+botoks
    mixed in with 600 zł classic 1-okolica botox.

    Services without variant_id (no embedding, no matching variant in
    treatment_variants for their parent tid, or confidence below threshold)
    are dropped. Better zero than false comparisons.

    When a salon has multiple services in the same (tid, variant) bucket
    (e.g. two different price points of "Botoks 1 okolica"), we take the
    one with the LOWEST price_grosze.
    """
    out: dict[tuple[int, int, str], dict[str, Any]] = {}
    skipped_no_variant = 0
    skipped_promo_pakiet = 0
    for svc in services:
        if not svc.get("is_active", True):
            continue
        # Filter subject services that are promotional / multi-pack listings.
        # Empirycznie (report 34): "Dermapen 4 - 1 zabieg - twarz - PROMOCJA"
        # 600 zł vs market full-price 800 zł = -25% — fałszywe „podnieś cenę"
        # bo subject jest już w obniżonej cenie promocyjnej. Tak samo
        # "3x Red Touch", "Onda 4 zabiegi" — pakiety wielokrotne porównane
        # do single zabiegów.
        name = svc.get("name") or ""
        if detect_package_keyword(name) or _has_promo_marker(name) or svc.get("is_promo"):
            skipped_promo_pakiet += 1
            continue
        # IMPORTANT: prefer the raw tid that was active when variant_id was
        # assigned during Krok C backfill — otherwise taxonomy_inference.
        # infer_and_apply (which runs BEFORE pricing comparisons) mutates
        # booksy_treatment_id to a different tid, breaking the (tid,
        # variant_id) key consistency between subject and competitors that
        # had different inference outcomes. The _raw key is set by
        # infer_and_apply when it overrides; absent it, the column was
        # never touched and is the original.
        tid = svc.get("booksy_treatment_id_raw") or svc.get("booksy_treatment_id")
        vid = svc.get("variant_id")
        if tid is None or vid is None:
            skipped_no_variant += 1
            continue
        # 3rd key element: duration bucket (short/medium/long/unknown).
        # User feedback (2026-05-16): SBC "1 Fokus" 15min 99zł porównany do
        # Beauty4ever 60min 300zł = false +200% deviation → drop. Same-duration
        # grouping naprawia: trial pricing matchuje się tylko z trialem,
        # standard session z standard.
        dur_bucket = _duration_bucket(svc.get("duration_minutes"))
        key = (int(tid), int(vid), dur_bucket)
        existing = out.get(key)
        if existing is None:
            out[key] = svc
            continue
        existing_price = existing.get("price_grosze")
        new_price = svc.get("price_grosze")
        if existing_price is None and new_price is not None:
            out[key] = svc
        elif (
            existing_price is not None
            and new_price is not None
            and new_price < existing_price
        ):
            out[key] = svc
    if skipped_promo_pakiet > 0:
        logger.info(
            "_active_services_with_variant: dropped %d promotional / package services "
            "from subject side (mig 064 — prevents false 'raise price' signals)",
            skipped_promo_pakiet,
        )
    if skipped_no_variant > 0:
        logger.info(
            "_active_services_with_variant: dropped %d services without variant_id "
            "(hard gate — Phase 5)", skipped_no_variant,
        )
    return out


def _active_services_with_treatment(
    services: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Return {treatment_id: service_row} for active services with treatment_id
    AND a precomputed name embedding.

    Hard gate from 2026-05-15 (Phase 2 of "no comparisons without embeddings"):
    services without `name_embedding` are silently dropped. The variant
    clustering / hybrid taxonomy match all depend on embeddings, so a service
    without one cannot participate in apples-to-apples pricing comparison —
    we'd be doing a string match in disguise. The embed_chain_heads_priority
    backfill ensures coverage; the inline embedding step in ingest blocks
    promotion of chain heads that fail to embed.

    When a salon has multiple services under the same treatment_id, we take
    the one with the LOWEST price_grosze (conservative: picks the base
    variant when variants exist). Services missing price_grosze are kept
    only if no priced service exists for that treatment.
    """
    out: dict[int, dict[str, Any]] = {}
    skipped_no_embedding = 0
    for svc in services:
        if not svc.get("is_active", True):
            continue
        tid = svc.get("booksy_treatment_id")
        if tid is None:
            continue
        # HARD GATE — require embedding presence. Services without one are
        # excluded from pricing comparisons entirely. Phase 5 will add a
        # second gate on variant_id once treatment_variants table is live.
        if not svc.get("name_embedding") and not svc.get("has_embedding"):
            skipped_no_embedding += 1
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
    if skipped_no_embedding > 0:
        logger.info(
            "_active_services_with_treatment: dropped %d services without embedding "
            "(hard gate — see Phase 2)", skipped_no_embedding,
        )
    return out


async def _compute_pricing_comparisons(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
    *,
    audit_id: str | None = None,
) -> list[dict[str, Any]]:
    """Compute per-variant pricing comparison rows (Phase 5 + mig 064 verify).

    Groups services by (treatment_id, variant_id) tuple instead of plain
    treatment_id. Each comparison row represents a SPECIFIC market segment
    ("Botoks 1 okolica", not just "Botoks"). For each (tid, variant_id)
    pair that the subject offers AND at least 2 competitors also offer,
    compute the market distribution.

    Variant labels come from treatment_variants (migracja 057). Services
    without variant_id are silently dropped — comparable variants by
    definition cannot exist without a variant identity.

    Mig 064 layer: rows with |deviation_pct| > VERIFICATION_THRESHOLD_PCT
    are re-verified against the variant centroid (package keyword regex +
    embedding cosine + optional duration check). Rows flagged as
    package_mismatch / low_name_similarity / duration_mismatch are dropped
    from output; extreme_outlier rows are kept with a flag so UI can warn.
    Each row carries competitor_samples (the per-competitor contributions
    that produced market_min/median/max) for click-expand details in UI.
    """
    subject_svcs = _active_services_with_variant(subject_data.get("services") or [])
    if not subject_svcs:
        logger.info("Etap 4: subject has no services with treatment_id — no pricing comparisons")
        return []

    # Build per-(tid, variant_id) competitor sample lists with full metadata.
    # Each sample preserves salon identity + the offered service so UI can
    # render a per-row drill-down.
    # Key: (tid, variant_id, duration_bucket) — patrz _duration_bucket().
    # Bucket dodany 2026-05-16 żeby trial 15min nie zafałszowywał deviation
    # wobec standard 60min single session.
    competitor_samples_by_variant: dict[
        tuple[int, int, str], list[dict[str, Any]]
    ] = {}
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        comp_scrape = cdata.get("scrape") or {}
        salon_name = comp_scrape.get("salon_name") or ""
        comp_svcs = _active_services_with_variant(cdata.get("services") or [])
        for variant_key, svc in comp_svcs.items():
            price = svc.get("price_grosze")
            if price is None:
                continue
            competitor_samples_by_variant.setdefault(variant_key, []).append({
                "salon_id": cdata.get("salon_id"),
                "salon_name": salon_name,
                "booksy_id": cand.booksy_id,
                "service_id": svc.get("id"),
                "service_name": svc.get("name"),
                "price_grosze": int(price),
                "duration_minutes": svc.get("duration_minutes"),
                # Same variant cluster → similarity 1.0 for UI sort
                # (modal sorts competitor rows by closest-to-subject first).
                "name_similarity": 1.0,
            })

    # Pre-pass: figure out which subject service embeddings + which variant
    # centroids we need to fetch. Only rows with |deviation| > threshold
    # trigger verification, so we filter the fetch set accordingly.
    variant_ids_needing_verify: set[int] = set()
    subject_service_ids_needing_verify: set[int] = set()
    prelim: list[dict[str, Any]] = []
    for variant_key, subject_svc in subject_svcs.items():
        tid, variant_id, _dur_bucket = variant_key
        samples = competitor_samples_by_variant.get(variant_key, [])
        subject_price = subject_svc.get("price_grosze")
        if subject_price is None:
            continue
        # User feedback (2026-05-16): "jeśli u klienta jest jakiś wariant a
        # konkurencja go nie posiada to nie możemy wycinać całego bloku zabiegu —
        # pokazujemy, że wariant istnieje tylko tu i elo, natomiast to nie
        # powinno rzutować na wyświetlenie". Zatem sample=0 dostaje row
        # z NULL market data + recommended_action='subject_only'. UI rysuje
        # kreski w market columns i badge "tylko u Ciebie".
        if len(samples) == 0:
            prelim.append({
                "variant_key": variant_key,
                "tid": tid,
                "variant_id": variant_id,
                "subject_svc": subject_svc,
                "subject_price": subject_price,
                "samples": [],
                "market_prices_f": [],
                "percentiles": None,
                "deviation_pct": None,
                "subject_only": True,
            })
            continue
        market_prices_f = [float(s["price_grosze"]) for s in samples]
        percentiles = compute_percentiles(market_prices_f)
        median = percentiles["market_p50"]
        deviation_pct = (
            ((float(subject_price) - median) / median * 100.0) if median > 0 else 0.0
        )
        prelim.append({
            "variant_key": variant_key,
            "tid": tid,
            "variant_id": variant_id,
            "subject_svc": subject_svc,
            "subject_price": subject_price,
            "samples": samples,
            "market_prices_f": market_prices_f,
            "percentiles": percentiles,
            "deviation_pct": deviation_pct,
            "subject_only": False,
        })
        if abs(deviation_pct) > VERIFICATION_THRESHOLD_PCT:
            variant_ids_needing_verify.add(int(variant_id))
            sid = subject_svc.get("id")
            if sid is not None:
                subject_service_ids_needing_verify.add(int(sid))

    # Batch fetch centroids + subject embeddings only for rows we'll verify.
    variant_centroids = await service.get_variant_centroids(
        list(variant_ids_needing_verify),
    )
    subject_embeddings = await service.get_service_embeddings(
        list(subject_service_ids_needing_verify),
    )

    rows: list[dict[str, Any]] = []
    dropped_counts = {
        "package_mismatch": 0,
        "low_name_similarity": 0,
        "duration_mismatch": 0,
    }
    for item in prelim:
        subject_svc = item["subject_svc"]
        samples = item["samples"]
        deviation_pct = item["deviation_pct"]
        percentiles = item["percentiles"]
        subject_price = item["subject_price"]
        market_prices_f = item["market_prices_f"]
        tid = item["tid"]
        variant_id = item["variant_id"]
        subject_only = item.get("subject_only", False)

        # Subject-only path: usługa istnieje TYLKO u subject, no market median.
        # Emit row z NULL market values, deviation=None, action='subject_only'.
        # UI renderuje kreski + badge "tylko u Ciebie".
        if subject_only:
            subject_dur = subject_svc.get("duration_minutes") or 0
            subject_ppm = (
                round(float(subject_price) / subject_dur, 2)
                if subject_dur > 0 else None
            )
            rows.append({
                "report_id": report_id,
                "comparison_tier": "variant",
                "booksy_treatment_id": tid,
                "variant_id": variant_id,
                "treatment_name": (
                    subject_svc.get("treatment_name") or subject_svc.get("name") or "Unknown"
                ),
                "treatment_parent_id": subject_svc.get("treatment_parent_id"),
                "subject_price_grosze": int(subject_price),
                "subject_is_from_price": bool(subject_svc.get("is_from_price") or False),
                "subject_duration_minutes": subject_svc.get("duration_minutes"),
                "subject_price_per_min_grosze": subject_ppm,
                "market_min_grosze": None,
                "market_p25_grosze": None,
                "market_median_grosze": None,
                "market_p75_grosze": None,
                "market_max_grosze": None,
                "market_price_per_min_grosze_min": None,
                "market_price_per_min_grosze_p25": None,
                "market_price_per_min_grosze_median": None,
                "market_price_per_min_grosze_p75": None,
                "market_price_per_min_grosze_max": None,
                "subject_percentile": None,
                "deviation_pct": None,
                "deviation_pct_per_min": None,
                "sample_size": 0,
                "recommended_action": "subject_only",
                "verification_status": "subject_only",
                "verification_details": None,
                "competitor_samples": [],
            })
            continue

        recommended_action = _classify_pricing_action(deviation_pct)
        subject_pct = compute_subject_percentile(
            float(subject_price), market_prices_f,
        )

        # Verification gate — only when deviation is extreme.
        verification_status = "verified"
        verification_details: dict[str, Any] = {}
        if abs(deviation_pct) > VERIFICATION_THRESHOLD_PCT:
            sid = subject_svc.get("id")
            subject_emb = subject_embeddings.get(int(sid)) if sid is not None else None
            variant_meta = variant_centroids.get(int(variant_id)) or {}
            # Approximate market median duration from samples (best-effort —
            # many salons skip duration_minutes; we filter Nones out).
            durations = [
                float(s["duration_minutes"]) for s in samples
                if s.get("duration_minutes") is not None
            ]
            market_median_duration = (
                sorted(durations)[len(durations) // 2] if durations else None
            )

            verification_status, verification_details = verify_pricing_comparison(
                subject_service={
                    "name": subject_svc.get("name"),
                    "duration_minutes": subject_svc.get("duration_minutes"),
                    "name_embedding": subject_emb,
                },
                variant_centroid_embedding=variant_meta.get("centroid_embedding"),
                variant_canonical_name=variant_meta.get("canonical_variant_name"),
                deviation_pct=deviation_pct,
                market_median_duration=market_median_duration,
            )
            verification_details["samples_count"] = len(samples)

            if should_drop_from_display(verification_status):
                dropped_counts[verification_status] = (
                    dropped_counts.get(verification_status, 0) + 1
                )
                logger.info(
                    "Dropped pricing comparison report=%s tid=%s variant_id=%s "
                    "(%s): subject_name=%r deviation=%.1f%% details=%s",
                    report_id, tid, variant_id, verification_status,
                    (subject_svc.get("name") or "")[:80],
                    deviation_pct, verification_details,
                )
                continue

        # Compute PLN/min metrics (mig 068). Subject + market percentiles
        # liczone z prices ÷ durations gdzie duration > 0.
        subject_dur = subject_svc.get("duration_minutes") or 0
        subject_ppm = (
            round(float(subject_price) / subject_dur, 2)
            if subject_dur > 0 else None
        )
        sample_ppms = [
            float(s["price_grosze"]) / float(s["duration_minutes"])
            for s in samples
            if s.get("price_grosze") is not None
            and s.get("duration_minutes") is not None
            and s.get("duration_minutes") > 0
        ]
        market_ppm_pctiles = (
            compute_percentiles(sample_ppms) if sample_ppms else None
        )
        market_ppm_median = (
            round(market_ppm_pctiles["market_p50"], 2)
            if market_ppm_pctiles else None
        )
        deviation_pct_per_min = (
            round(
                (subject_ppm - market_ppm_median) / market_ppm_median * 100.0,
                2,
            )
            if subject_ppm is not None
            and market_ppm_median is not None
            and market_ppm_median > 0
            else None
        )

        rows.append({
            "report_id": report_id,
            "comparison_tier": "variant",
            "booksy_treatment_id": tid,
            "variant_id": variant_id,
            "treatment_name": (
                subject_svc.get("treatment_name") or subject_svc.get("name") or "Unknown"
            ),
            "treatment_parent_id": subject_svc.get("treatment_parent_id"),
            "subject_price_grosze": int(subject_price),
            "subject_is_from_price": bool(subject_svc.get("is_from_price") or False),
            "subject_duration_minutes": subject_svc.get("duration_minutes"),
            "subject_price_per_min_grosze": subject_ppm,
            "market_min_grosze": int(percentiles["market_min"]),
            "market_p25_grosze": int(percentiles["market_p25"]),
            "market_median_grosze": int(percentiles["market_p50"]),
            "market_p75_grosze": int(percentiles["market_p75"]),
            "market_max_grosze": int(percentiles["market_max"]),
            "market_price_per_min_grosze_min": (
                round(market_ppm_pctiles["market_min"], 2) if market_ppm_pctiles else None
            ),
            "market_price_per_min_grosze_p25": (
                round(market_ppm_pctiles["market_p25"], 2) if market_ppm_pctiles else None
            ),
            "market_price_per_min_grosze_median": market_ppm_median,
            "market_price_per_min_grosze_p75": (
                round(market_ppm_pctiles["market_p75"], 2) if market_ppm_pctiles else None
            ),
            "market_price_per_min_grosze_max": (
                round(market_ppm_pctiles["market_max"], 2) if market_ppm_pctiles else None
            ),
            "subject_percentile": round(subject_pct, 2),
            "deviation_pct": round(deviation_pct, 2),
            "deviation_pct_per_min": deviation_pct_per_min,
            "sample_size": len(samples),
            "recommended_action": recommended_action,
            "verification_status": verification_status,
            "verification_details": verification_details or None,
            "competitor_samples": samples,
        })

    # ── Tier-1: per booksy_treatment_id family-level rows ──
    # Opcja C: gwarantowany overlap dla każdej kategorii w cenniku subject.
    # Aggregate ACROSS variants w jednym tid. UI top-level pokazuje to,
    # drill-down → tier-2 variant rows.
    #
    # Pre-load variant centroids for every variant_id referenced on subject
    # + competitor side. Used as stable reference embeddings in the method-
    # similarity filter inside _compute_treatment_tier_rows.
    variant_ids_to_fetch: set[int] = set()
    for svc in (subject_data.get("services") or []):
        vid = svc.get("variant_id")
        if vid is not None:
            variant_ids_to_fetch.add(int(vid))
    for _cand, _cdata in aligned_competitors:
        for svc in (_cdata.get("services") or []):
            vid = svc.get("variant_id")
            if vid is not None:
                variant_ids_to_fetch.add(int(vid))
    variant_centroids: dict[int, Any] = {}
    if variant_ids_to_fetch:
        try:
            variant_centroids = await service.get_variant_centroids(
                list(variant_ids_to_fetch),
            )
            logger.info(
                "Etap 4 tier-1: loaded %d variant centroids for method filter",
                len(variant_centroids),
            )
        except Exception as e:
            logger.warning(
                "Failed to pre-load variant centroids (%s) — falling back "
                "to service-name embeddings only", e,
            )
    # Faza 7 (2026-05-16) — tier-1 gets an LLM pair-verification gate
    # on top of the embedding method-similarity filter. The function is
    # async so it can await the LLM batch calls + cache RPC roundtrips.
    # `_get_hidden_inference_llm()` is lazy + cached; reuse it instead
    # of spinning up a new client per call.
    tier1_llm_client = _get_hidden_inference_llm()
    tier1_rows = await _compute_treatment_tier_rows(
        report_id, subject_data, aligned_competitors,
        variant_centroids=variant_centroids,
        supabase=service,
        llm_client=tier1_llm_client,
        audit_id=audit_id,
    )
    rows.extend(tier1_rows)

    # ── Tier-3: per (tid + sub_variant_group_id + duration_bucket) ──
    # Najprecyzyjniejszy match cross-salon. "Botoks Twarz" subject vs
    # "Botoks Twarz" competitor — apples-to-apples. Sub_variant_group_id
    # to deterministic cross-salon cluster z mig 071. Wymaga aby salonowy
    # cennik używał natywnej Booksy multi-variant feature; salony z flat
    # listingiem dostają NULL group_id na sub-variantach i pomijamy je
    # w tier-3 (zostają w tier-1/tier-2).
    try:
        tier3_rows = await _compute_sub_variant_tier_rows(
            service, report_id, subject_data, aligned_competitors,
        )
        rows.extend(tier3_rows)
        logger.info(
            "Etap 4 tier-3: emitted %d sub_variant-level rows",
            len(tier3_rows),
        )
    except Exception as e:
        logger.warning(
            "Tier-3 sub-variant pricing failed (%s) — continuing without tier-3",
            e,
        )

    total_dropped = sum(dropped_counts.values())
    if total_dropped > 0:
        logger.info(
            "Pricing verification: dropped %d/%d rows (package=%d, "
            "low_name_sim=%d, duration=%d) for report=%s",
            total_dropped, total_dropped + len(rows),
            dropped_counts["package_mismatch"],
            dropped_counts["low_name_similarity"],
            dropped_counts["duration_mismatch"],
            report_id,
        )

    return rows


# Empirycznie zwalidowane dla report 34 — Thunder IPL (laser depilacja)
# wcześniej dopasowywany do "Depilacja pastą cukrową" przez tid=236 fallback.
# OpenAI text-embedding-3-small daje typowo:
#   cosine ≥ 0.65 dla wariantów tej samej metody ("Botoks 1 okolica" vs "Botoks 2 okolice")
#   cosine 0.45-0.60 dla różnych nazwy/metody w obrębie kategorii
#   cosine < 0.40 dla wyraźnie różnych metod (Thunder IPL vs pasta cukrowa)
# Próg 0.55 odsiewa najgorsze cross-method noise zachowując uczciwe matchy.
_TREATMENT_TIER_MIN_NAME_SIM = 0.55

# Niżej tego progu downgrade'ujemy tier=treatment row do verification_status
# 'low_confidence' — nie pokazujemy bombastycznych deviation% z 1-2 cherry-picked
# konkurentów. Beauty4ever Thunder vs jedna pasta cukrowa = +167% = bullshit.
_TREATMENT_TIER_MIN_SAMPLES = 3

# Faza 7 (2026-05-16) — LLM pair verification thresholds.
# Skip the LLM gate entirely when there's nothing useful to filter; one
# competitor sample either survives method-similarity or it doesn't and
# we won't compute percentiles from <3 samples anyway.
_LLM_VERIFY_MIN_SAMPLES = 2
# If LLM verify rejects so many candidates that fewer than this number
# survive, demote the row to subject_only — better than showing a
# bombastic deviation% off 1-2 cherry-picked LLM-approved samples.
_LLM_VERIFY_LOW_CONFIDENCE_THRESHOLD = 3


def _filter_comp_samples_by_subj_method(
    comp_svc_with_meta: list[tuple[dict[str, Any], Any, int | None]],
    subj_refs: list[tuple[Any, int | None]],
    *,
    min_sim: float = _TREATMENT_TIER_MIN_NAME_SIM,
) -> tuple[list[dict[str, Any]], int]:
    """Keep only competitor services whose reference embedding is similar to
    at least one subject reference within the same tid. Defends against
    same-tid cross-method false pairs (Thunder IPL vs pasta cukrowa under
    tid=236 Depilacja).

    Reference embedding priority per side: variant_centroid (stable cross-
    salon HDBSCAN centroid) when variant_id is known and a centroid exists
    in the lookup table, else the individual service name_embedding. This
    matches the intuition that within the same parent tid the variant
    clustering ALREADY encodes method/scope, and the centroid is less
    noisy than any single branded service name like
    "✦ Thunder - pachy + bikini pełne".

    Fast path: if subject and competitor share the same variant_id, the
    pair auto-passes (centroid-to-itself cosine = 1.0).

    Args:
      comp_svc_with_meta: list of (sample_dict, fallback_embedding, variant_id).
      subj_refs: list of (fallback_embedding, variant_id) for subject
        services in the same tid.

    Returns (kept_samples, n_dropped).
    """
    if not subj_refs:
        # Subject has no references — accept everything, no similarity score.
        out: list[dict[str, Any]] = []
        for s, _, _ in comp_svc_with_meta:
            s["name_similarity"] = None
            out.append(s)
        return out, 0
    subj_vids = {vid for _, vid in subj_refs if vid is not None}
    kept: list[dict[str, Any]] = []
    dropped = 0
    for sample, fallback_emb, comp_vid in comp_svc_with_meta:
        # Fast path 1: identical variant_id → guaranteed same cluster.
        # Similarity is 1.0 (centroid-to-itself) for ranking purposes.
        if comp_vid is not None and comp_vid in subj_vids:
            sample["name_similarity"] = 1.0
            kept.append(sample)
            continue
        # Pick reference embedding for competitor side.
        comp_ref = (
            _CENTROID_LOOKUP_RUNTIME.get(comp_vid)
            if comp_vid is not None and _CENTROID_LOOKUP_RUNTIME
            else None
        ) or fallback_emb
        if comp_ref is None:
            # No embedding at all — keep conservatively, no similarity.
            sample["name_similarity"] = None
            kept.append(sample)
            continue
        max_sim = 0.0
        for fb_emb, subj_vid in subj_refs:
            subj_ref = (
                _CENTROID_LOOKUP_RUNTIME.get(subj_vid)
                if subj_vid is not None and _CENTROID_LOOKUP_RUNTIME
                else None
            ) or fb_emb
            if subj_ref is None:
                continue
            sim = compute_name_embedding_similarity(comp_ref, subj_ref)
            if sim is not None and sim > max_sim:
                max_sim = sim
        if max_sim >= min_sim:
            # Annotate so the rich UI modal can sort competitor rows by
            # closest-to-subject first (default sort) instead of by price.
            sample["name_similarity"] = round(max_sim, 4)
            kept.append(sample)
        else:
            dropped += 1
    return kept, dropped


# Module-level lookup populated by _compute_treatment_tier_rows before the
# filter runs. Holds {variant_id: centroid_embedding_pgvector_string} so the
# filter helper can resolve centroids without async DB calls in its inner
# loop. Reset on each tier-1 invocation.
_CENTROID_LOOKUP_RUNTIME: dict[int, Any] = {}


def _tid_key(svc: dict[str, Any]) -> tuple[str, int] | None:
    """Return the routing key for a service: either a Booksy treatment_id
    (tuple `('booksy', tid)`) OR a synthetic_treatment_id (tuple
    `('synthetic', stid)`) — never both. None when neither exists.

    Priority order:
      1. `booksy_treatment_id_raw` — preserved original Booksy tid even
         when later code mutated `booksy_treatment_id` (e.g. legacy
         taxonomy_inference). Always wins when present.
      2. `booksy_treatment_id` — native or Rule-3 inferred Booksy tid.
      3. `synthetic_treatment_id` — Rules 1/2/4 synthetic anchor.

    This is the canonical aggregation key shared between subject and
    competitor sides of the pricing matrix. Phantom-row bug from
    2026-05-16 was caused by mismatched keys (subject's overwritten
    booksy_treatment_id 7770 vs competitor's NULL); routing via this
    helper makes the key derivation explicit and consistent.
    """
    raw = svc.get("booksy_treatment_id_raw")
    if raw is not None:
        return ("booksy", int(raw))
    btid = svc.get("booksy_treatment_id")
    if btid is not None:
        return ("booksy", int(btid))
    stid = svc.get("synthetic_treatment_id")
    if stid is not None:
        return ("synthetic", int(stid))
    return None


async def _compute_treatment_tier_rows(
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
    *,
    variant_centroids: dict[int, Any] | None = None,
    supabase: SupabaseService | None = None,
    llm_client: GeminiLLMClient | None = None,
    audit_id: str | None = None,
) -> list[dict[str, Any]]:
    """Tier-1 pricing comparison rows aggregated per booksy_treatment_id.

    Każda kategoria w cenniku subject (po LLM inference) dostaje JEDEN row
    z agregowanymi statystykami ACROSS wszystkie warianty:
      - subject prices: min/median/max + median PLN/min
      - market prices: percentiles across competitor services w tym tid
      - sample_size: ilość competitor services (NIE unique salons) w tid
      - deviation_pct + deviation_pct_per_min — PLN/min preferowane bo
        gracefully obsługuje duration variance między variantami

    Filter (consistent z mig 067 stats):
      - active = TRUE
      - duration 5-240 min (no rezerwacje, no kosmetyczne pakiety całodniowe)
      - non-package name (regex pakiet/abonament/karnet/zabiegów)
      - subject_price_grosze IS NOT NULL

    Emituje rows z comparison_tier='treatment', variant_id=NULL,
    recommended_action ∈ {raise, lower, hold, subject_only}.

    Faza 7 (2026-05-16) — LLM pair verification gate. AFTER the
    embedding-based method-similarity filter (`_filter_comp_samples_by_subj_method`)
    each remaining competitor sample is checked by an LLM batch call
    against the subject's canonical service name. Cached in
    `service_pair_verifications` (mig 078 + 079). Same Booksy tid often
    bundles different brands / body parts / intensities under one
    umbrella (e.g. tid=630 mixes PRX, INFINI, peelings of plecy, twarz,
    intymne); the LLM cuts those down to the actually-comparable
    services. Tier=variant + tier=sub_variant skip this gate — their
    cluster_id grouping already enforces method match.
    """
    rows: list[dict[str, Any]] = []

    def _eligible(svc: dict[str, Any]) -> bool:
        if not svc.get("is_active", True):
            return False
        if svc.get("price_grosze") is None or svc.get("price_grosze", 0) <= 0:
            return False
        dur = svc.get("duration_minutes")
        if dur is None or dur < 5 or dur > 240:
            return False
        name = (svc.get("name") or "").lower()
        if any(kw in name for kw in (
            "pakiet", "abonament", "karnet", "voucher", "bon ",
            "x zabieg", "zabiegów",
        )):
            return False
        if detect_package_keyword(svc.get("name") or ""):
            return False
        return True

    # 1. Group subject services by tid_key (Booksy OR synthetic).
    # tid_key = ('booksy', int) | ('synthetic', int). See _tid_key() docstring.
    # Synthetic-keyed groups now flow into tier-1 alongside Booksy-native
    # ones (Faza 2+3+4 refactor 2026-05-16) — display layer uses each row's
    # `booksy_treatment_id` (nullable) or `synthetic_treatment_id` (nullable)
    # to know which catalog the comparison belongs to.
    subject_by_tid: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for svc in subject_data.get("services") or []:
        if not _eligible(svc):
            continue
        key = _tid_key(svc)
        if key is None:
            continue
        subject_by_tid.setdefault(key, []).append(svc)

    if not subject_by_tid:
        return rows

    # 2. Group competitor services by tid_key (z counts_in_aggregates filter).
    # Hold tuples (sample_dict, fallback_embedding, variant_id) — variant_id
    # drives the centroid-based filter, fallback service name embedding
    # catches services that didn't match any variant during Phase 4 backfill.
    competitor_by_tid_raw: dict[
        tuple[str, int],
        list[tuple[dict[str, Any], Any, int | None]],
    ] = {}
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        salon_name = (cdata.get("scrape") or {}).get("salon_name") or ""
        for svc in cdata.get("services") or []:
            if not _eligible(svc):
                continue
            key = _tid_key(svc)
            if key is None:
                continue
            sample = {
                "salon_id": cdata.get("salon_id"),
                "salon_name": salon_name,
                "booksy_id": cand.booksy_id,
                "service_id": svc.get("id"),
                "service_name": svc.get("name"),
                "price_grosze": int(svc["price_grosze"]),
                "duration_minutes": int(svc["duration_minutes"]),
            }
            fb_emb = (
                svc.get("name_embedding") or svc.get("name_embedding_dense")
            )
            v_id = svc.get("variant_id")
            competitor_by_tid_raw.setdefault(key, []).append(
                (sample, fb_emb, int(v_id) if v_id is not None else None)
            )

    # 2a. Method-similarity filter — backed by variant centroids when
    # available. Booksy's tid is method-agnostic (tid=236 "Depilacja"
    # spans laser, IPL, wax, pasta cukrowa with very different price
    # points). The variant clustering layer (HDBSCAN over name embeddings
    # in scripts/cluster_treatment_variants.py) ALREADY separates these
    # into clusters with stable centroids; we use those centroids as the
    # reference vector instead of any single noisy service name embedding
    # like "✦ Thunder - pachy + bikini pełne". Fall back to the raw
    # service name embedding only when variant_id is missing.
    #
    # Setup _CENTROID_LOOKUP_RUNTIME so the filter helper can reach the
    # centroid map without threading it through every call.
    global _CENTROID_LOOKUP_RUNTIME
    _CENTROID_LOOKUP_RUNTIME = {}
    if variant_centroids:
        for vid, info in variant_centroids.items():
            emb = info.get("centroid_embedding") if isinstance(info, dict) else None
            if emb is not None:
                _CENTROID_LOOKUP_RUNTIME[int(vid)] = emb

    competitor_by_tid: dict[tuple[str, int], list[dict[str, Any]]] = {}
    total_dropped = 0
    for key, raw_samples in competitor_by_tid_raw.items():
        subj_refs = [
            (
                s.get("name_embedding") or s.get("name_embedding_dense"),
                int(s["variant_id"]) if s.get("variant_id") is not None else None,
            )
            for s in subject_by_tid.get(key, [])
        ]
        # Filter out empty references (no embedding AND no variant_id) —
        # they don't contribute to similarity computation.
        subj_refs = [r for r in subj_refs if r[0] is not None or r[1] is not None]
        kept, dropped = _filter_comp_samples_by_subj_method(
            raw_samples, subj_refs,
        )
        competitor_by_tid[key] = kept
        total_dropped += dropped
    if total_dropped:
        logger.info(
            "Etap 4 tier-1: method-similarity filter dropped %d competitor "
            "services as cross-method noise (variant-centroid cosine < %.2f)",
            total_dropped, _TREATMENT_TIER_MIN_NAME_SIM,
        )

    # 3. Per tid_key, build aggregate row.
    for key, subj_svcs in subject_by_tid.items():
        tid_kind, tid_value = key
        comp_samples = competitor_by_tid.get(key, [])
        # Min-sample gate. After the method-similarity filter we may end up
        # with 0-2 competitor samples — too few for a credible deviation%
        # ("Thunder vs jedna pasta cukrowa = +167%" is the false-alarm
        # pattern we want to suppress). Treat those rows as subject_only
        # with verification_status='low_confidence' so the UI can hide or
        # demote them without losing the "this service exists" signal.
        if 0 < len(comp_samples) < _TREATMENT_TIER_MIN_SAMPLES:
            logger.info(
                "Etap 4 tier-1: key=%s:%d only %d competitor samples after "
                "method filter — demoting to low_confidence subject_only",
                tid_kind, tid_value, len(comp_samples),
            )
            comp_samples = []

        # Display label — use ORIGINAL service name from cennik (svc["name"]).
        # Faza 2+3+4 refactor (2026-05-16): we stopped overwriting
        # `treatment_name` in `_resolve_service_taxonomy`, so for
        # synthetic-keyed rows there is no Booksy canonical to fall back on.
        # `svc["name"]` is the salon's actual display label and matches what
        # the user sees in their cennik — pricing matrix labels must agree
        # with that, otherwise we get phantom rows like "Tlenoterapia"
        # appearing where the salon has zero services named that.
        service_names = [(s.get("name") or "") for s in subj_svcs]
        if service_names:
            canonical_name = max(set(service_names), key=service_names.count)
        elif tid_kind == "synthetic":
            # Fall back to the synthetic catalog's canonical_name (set by
            # _resolve_service_taxonomy) when subject_svcs is somehow empty.
            canonical_name = (
                subj_svcs[0].get("synthetic_canonical_name")
                if subj_svcs else f"Treatment {tid_value}"
            )
        else:
            canonical_name = f"Treatment {tid_value}"

        # Faza 7 (2026-05-16) — LLM pair verification gate. Embedding
        # cosine accepts same-tid services with overlapping vocabulary
        # but different procedure (Bloomea LIGHTENING peeling vs PRO XN
        # II stopień twarz — both peelings, completely different
        # chemistry/scope). Ask an LLM per-row whether each candidate
        # is actually comparable to the subject for pricing purposes.
        # Cached so future audits don't pay for re-asking the same
        # (subject, competitor, tid) tuple.
        if (
            comp_samples
            and len(comp_samples) >= _LLM_VERIFY_MIN_SAMPLES
            and supabase is not None
            and llm_client is not None
        ):
            booksy_tid_for_verify = (
                tid_value if tid_kind == "booksy" else None
            )
            synthetic_tid_for_verify = (
                tid_value if tid_kind == "synthetic" else None
            )
            candidate_names = [
                (s.get("service_name") or "") for s in comp_samples
            ]
            verified_map = await verify_service_pairs(
                subject_service_name=canonical_name,
                candidate_competitor_names=candidate_names,
                booksy_treatment_id=booksy_tid_for_verify,
                synthetic_treatment_id=synthetic_tid_for_verify,
                supabase=supabase,
                llm_client=llm_client,
                audit_id=audit_id,
            )
            verified_samples: list[dict[str, Any]] = []
            rejected_count = 0
            for sample in comp_samples:
                norm = _normalize_pair_name(sample.get("service_name") or "")
                verdict = verified_map.get(norm, {})
                sample["llm_verified"] = bool(
                    verdict.get("is_comparable", True)
                )
                sample["llm_reasoning"] = verdict.get("reasoning")
                sample["llm_rejection_reason"] = verdict.get(
                    "rejection_reason"
                )
                sample["from_cache"] = bool(verdict.get("from_cache", False))
                if sample["llm_verified"]:
                    verified_samples.append(sample)
                else:
                    rejected_count += 1
            logger.info(
                "Etap 4 tier-1 LLM verify: key=%s:%s kept %d/%d, dropped %d",
                tid_kind, tid_value,
                len(verified_samples), len(comp_samples), rejected_count,
            )
            comp_samples = verified_samples
            # If LLM rejected enough samples that we drop below the
            # statistical-confidence threshold, demote to subject_only —
            # same policy as the embedding method-similarity filter.
            if 0 < len(comp_samples) < _LLM_VERIFY_LOW_CONFIDENCE_THRESHOLD:
                logger.info(
                    "Etap 4 tier-1 LLM verify: key=%s:%s only %d verified "
                    "samples — demoting to low_confidence subject_only",
                    tid_kind, tid_value, len(comp_samples),
                )
                comp_samples = []

        subj_prices = [float(s["price_grosze"]) for s in subj_svcs]
        subj_ppms = [
            float(s["price_grosze"]) / float(s["duration_minutes"])
            for s in subj_svcs
            if s.get("duration_minutes") and s["duration_minutes"] > 0
        ]
        subj_median_price = sorted(subj_prices)[len(subj_prices) // 2]
        subj_median_ppm = (
            round(sorted(subj_ppms)[len(subj_ppms) // 2], 2)
            if subj_ppms else None
        )

        # Anchor columns — one is non-null per row depending on tid_kind.
        # The other stays NULL so the DB CHECK constraint (mig 074) is
        # satisfied and the frontend can branch on `taxonomy_source` to
        # show the right badge.
        emit_booksy_tid = tid_value if tid_kind == "booksy" else None
        emit_synthetic_tid = tid_value if tid_kind == "synthetic" else None
        # taxonomy_source — propagated from any service in the group
        # (they should all share the same source by construction; pick
        # the first non-null for safety).
        emit_taxonomy_source = next(
            (s.get("taxonomy_source") for s in subj_svcs if s.get("taxonomy_source")),
            None,
        )

        # Subject-only at tier-1 = no competitor offers anything in this tid.
        if not comp_samples:
            rows.append({
                "report_id": report_id,
                "comparison_tier": "treatment",
                "booksy_treatment_id": emit_booksy_tid,
                "synthetic_treatment_id": emit_synthetic_tid,
                "taxonomy_source": emit_taxonomy_source,
                "variant_id": None,
                "treatment_name": canonical_name,
                "treatment_parent_id": subj_svcs[0].get("treatment_parent_id"),
                "subject_price_grosze": int(subj_median_price),
                "subject_is_from_price": False,
                "subject_duration_minutes": int(
                    subj_svcs[0].get("duration_minutes") or 0
                ) or None,
                "subject_price_per_min_grosze": subj_median_ppm,
                "market_min_grosze": None,
                "market_p25_grosze": None,
                "market_median_grosze": None,
                "market_p75_grosze": None,
                "market_max_grosze": None,
                "market_price_per_min_grosze_min": None,
                "market_price_per_min_grosze_p25": None,
                "market_price_per_min_grosze_median": None,
                "market_price_per_min_grosze_p75": None,
                "market_price_per_min_grosze_max": None,
                "subject_percentile": None,
                "deviation_pct": None,
                "deviation_pct_per_min": None,
                "sample_size": 0,
                "recommended_action": "subject_only",
                "verification_status": "subject_only",
                "verification_details": None,
                "competitor_samples": [],
            })
            continue

        # Compute market percentiles for both price and PLN/min.
        market_prices = [float(s["price_grosze"]) for s in comp_samples]
        market_ppms = [
            float(s["price_grosze"]) / float(s["duration_minutes"])
            for s in comp_samples
            if s["duration_minutes"] > 0
        ]
        price_pcts = compute_percentiles(market_prices)
        ppm_pcts = compute_percentiles(market_ppms) if market_ppms else None

        # Deviation: prefer PLN/min for tier-1 since variants within tid
        # have different durations — raw price deviation za noisy.
        # Fallback to raw price deviation gdy nie mamy ppm samples.
        dev_per_min = None
        dev_raw = None
        recommended_action = "hold"
        if (
            subj_median_ppm is not None
            and ppm_pcts is not None
            and ppm_pcts["market_p50"] > 0
        ):
            dev_per_min = round(
                (subj_median_ppm - ppm_pcts["market_p50"]) / ppm_pcts["market_p50"]
                * 100.0,
                2,
            )
            recommended_action = _classify_pricing_action(dev_per_min)
        elif price_pcts["market_p50"] > 0:
            dev_raw = round(
                (subj_median_price - price_pcts["market_p50"])
                / price_pcts["market_p50"] * 100.0,
                2,
            )
            recommended_action = _classify_pricing_action(dev_raw)

        if dev_raw is None and price_pcts["market_p50"] > 0:
            dev_raw = round(
                (subj_median_price - price_pcts["market_p50"])
                / price_pcts["market_p50"] * 100.0,
                2,
            )

        subj_pct = compute_subject_percentile(subj_median_price, market_prices)

        rows.append({
            "report_id": report_id,
            "comparison_tier": "treatment",
            "booksy_treatment_id": emit_booksy_tid,
            "synthetic_treatment_id": emit_synthetic_tid,
            "taxonomy_source": emit_taxonomy_source,
            "variant_id": None,
            "treatment_name": canonical_name,
            "treatment_parent_id": subj_svcs[0].get("treatment_parent_id"),
            "subject_price_grosze": int(subj_median_price),
            "subject_is_from_price": False,
            "subject_duration_minutes": int(
                subj_svcs[0].get("duration_minutes") or 0
            ) or None,
            "subject_price_per_min_grosze": subj_median_ppm,
            "market_min_grosze": int(price_pcts["market_min"]),
            "market_p25_grosze": int(price_pcts["market_p25"]),
            "market_median_grosze": int(price_pcts["market_p50"]),
            "market_p75_grosze": int(price_pcts["market_p75"]),
            "market_max_grosze": int(price_pcts["market_max"]),
            "market_price_per_min_grosze_min": (
                round(ppm_pcts["market_min"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_p25": (
                round(ppm_pcts["market_p25"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_median": (
                round(ppm_pcts["market_p50"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_p75": (
                round(ppm_pcts["market_p75"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_max": (
                round(ppm_pcts["market_max"], 2) if ppm_pcts else None
            ),
            "subject_percentile": round(subj_pct, 2),
            "deviation_pct": dev_raw,
            "deviation_pct_per_min": dev_per_min,
            "sample_size": len(comp_samples),
            "recommended_action": recommended_action,
            "verification_status": "verified",  # tier-1 nie używa verification
            "verification_details": {
                "tier": "treatment",
                "subject_variants_in_tid": len(subj_svcs),
                "competitor_services_in_tid": len(comp_samples),
                "unique_competitor_salons": len(
                    set(s["booksy_id"] for s in comp_samples if s.get("booksy_id"))
                ),
            },
            "competitor_samples": comp_samples[:30],  # cap dla payload size
        })

    if rows:
        n_booksy = sum(1 for r in rows if r.get("booksy_treatment_id") is not None)
        n_synthetic = sum(1 for r in rows if r.get("synthetic_treatment_id") is not None)
        logger.info(
            "Etap 4 tier-1: emitted %d treatment-level rows "
            "(%d booksy-keyed, %d synthetic-keyed)",
            len(rows), n_booksy, n_synthetic,
        )
    return rows


async def _compute_sub_variant_tier_rows(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Tier-3 pricing comparison rows per (tid, sub_variant_group_id, duration_bucket).

    Najprecyzyjniejszy match cross-salon. Wykorzystuje natywne Booksy
    multi-variants (mig 069) zlinkowane do cross-salon clusters (mig 071).
    Salons które używają tej feature dostają granular comparison ("Botoks
    Twarz" subject vs "Botoks Twarz" konkurenta), salons z flat listing
    pozostają w tier-1/tier-2.

    Algorytm:
      1. Load sub-variants dla wszystkich subject + competitor services
      2. Group subject sub-variants po (tid, group_id, duration_bucket)
      3. Group competitor sub-variants similarly
      4. Per subject group: znajdź matching competitor samples
      5. Emit row z deviation w cenach + PLN/min
      6. Subject-only path gdy brak overlap
    """
    rows: list[dict[str, Any]] = []

    # 1. Zbierz service IDs z subject + competitors (counts_in_aggregates).
    subject_service_ids: list[int] = []
    for svc in subject_data.get("services") or []:
        sid = svc.get("id")
        if sid is not None:
            subject_service_ids.append(int(sid))

    competitor_service_ids: list[int] = []
    competitor_service_meta: dict[int, dict[str, Any]] = {}
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            continue
        salon_name = (cdata.get("scrape") or {}).get("salon_name") or ""
        for svc in cdata.get("services") or []:
            sid = svc.get("id")
            if sid is None:
                continue
            competitor_service_ids.append(int(sid))
            competitor_service_meta[int(sid)] = {
                "salon_id": cdata.get("salon_id"),
                "salon_name": salon_name,
                "booksy_id": cand.booksy_id,
                "service_name": svc.get("name"),
                "service_tid": (
                    svc.get("booksy_treatment_id_raw")
                    or svc.get("booksy_treatment_id")
                ),
                "is_promo": bool(svc.get("is_promo")),
                "is_active": bool(svc.get("is_active", True)),
            }

    subject_service_meta: dict[int, dict[str, Any]] = {}
    for svc in subject_data.get("services") or []:
        sid = svc.get("id")
        if sid is None:
            continue
        subject_service_meta[int(sid)] = {
            "service_name": svc.get("name"),
            "service_tid": (
                svc.get("booksy_treatment_id_raw")
                or svc.get("booksy_treatment_id")
            ),
            "treatment_name": svc.get("treatment_name"),
            "treatment_parent_id": svc.get("treatment_parent_id"),
            "is_promo": bool(svc.get("is_promo")),
            "is_active": bool(svc.get("is_active", True)),
        }

    if not subject_service_ids:
        return rows

    # 2. Load sub-variants.
    all_service_ids = subject_service_ids + competitor_service_ids
    sub_variants_map = await service.get_sub_variants_for_services(all_service_ids)
    if not sub_variants_map:
        return rows

    # 3. Group subject sub-variants po (tid, group_id, duration_bucket).
    # Key: (tid, sub_variant_group_id, duration_bucket).
    # Value: best (cheapest) sub-variant w bukcie.
    SubKey = tuple[int, int, str]
    subject_subs: dict[SubKey, dict[str, Any]] = {}
    for sid in subject_service_ids:
        svc_meta = subject_service_meta.get(sid) or {}
        if not svc_meta.get("is_active", True):
            continue
        if svc_meta.get("is_promo"):
            continue
        if detect_package_keyword(svc_meta.get("service_name") or ""):
            continue
        tid = svc_meta.get("service_tid")
        if tid is None:
            continue
        for sv in sub_variants_map.get(sid, []):
            gid = sv.get("sub_variant_group_id")
            label = sv.get("label")
            if gid is None or not label:
                continue
            price = sv.get("price_grosze")
            duration = sv.get("duration_minutes")
            if price is None or duration is None or duration < 5 or duration > 240:
                continue
            if price <= 0:
                continue
            bucket = _duration_bucket(duration)
            key = (int(tid), int(gid), bucket)
            existing = subject_subs.get(key)
            if existing is None or price < existing["price_grosze"]:
                subject_subs[key] = {
                    "service_id": sid,
                    "sub_variant_id": sv.get("id"),
                    "sub_variant_label": label,
                    "price_grosze": int(price),
                    "duration_minutes": int(duration),
                    "treatment_name": svc_meta.get("treatment_name"),
                    "treatment_parent_id": svc_meta.get("treatment_parent_id"),
                    "service_name": svc_meta.get("service_name"),
                }

    if not subject_subs:
        return rows

    # 4. Group competitor sub-variants similarly.
    competitor_subs: dict[SubKey, list[dict[str, Any]]] = {}
    for sid in competitor_service_ids:
        meta = competitor_service_meta.get(sid) or {}
        if not meta.get("is_active", True) or meta.get("is_promo"):
            continue
        if detect_package_keyword(meta.get("service_name") or ""):
            continue
        tid = meta.get("service_tid")
        if tid is None:
            continue
        for sv in sub_variants_map.get(sid, []):
            gid = sv.get("sub_variant_group_id")
            label = sv.get("label")
            if gid is None or not label:
                continue
            price = sv.get("price_grosze")
            duration = sv.get("duration_minutes")
            if price is None or duration is None or duration < 5 or duration > 240:
                continue
            if price <= 0:
                continue
            bucket = _duration_bucket(duration)
            key = (int(tid), int(gid), bucket)
            competitor_subs.setdefault(key, []).append({
                "service_id": sid,
                "sub_variant_id": sv.get("id"),
                "sub_variant_label": label,
                "salon_id": meta.get("salon_id"),
                "salon_name": meta.get("salon_name"),
                "booksy_id": meta.get("booksy_id"),
                "service_name": meta.get("service_name"),
                "price_grosze": int(price),
                "duration_minutes": int(duration),
            })

    # 5. Per subject sub-variant key: emit comparison row.
    for key, subj in subject_subs.items():
        tid, gid, bucket = key
        samples = competitor_subs.get(key, [])
        subj_price = subj["price_grosze"]
        subj_dur = subj["duration_minutes"]
        subj_ppm = round(float(subj_price) / float(subj_dur), 2) if subj_dur > 0 else None

        if not samples:
            # Subject-only sub-variant.
            rows.append({
                "report_id": report_id,
                "comparison_tier": "sub_variant",
                "booksy_treatment_id": tid,
                "variant_id": None,
                "sub_variant_group_id": gid,
                "sub_variant_label": subj["sub_variant_label"],
                "treatment_name": (
                    subj.get("treatment_name")
                    or subj.get("service_name") or f"Treatment {tid}"
                ),
                "treatment_parent_id": subj.get("treatment_parent_id"),
                "subject_price_grosze": int(subj_price),
                "subject_is_from_price": False,
                "subject_duration_minutes": int(subj_dur),
                "subject_price_per_min_grosze": subj_ppm,
                "market_min_grosze": None,
                "market_p25_grosze": None,
                "market_median_grosze": None,
                "market_p75_grosze": None,
                "market_max_grosze": None,
                "market_price_per_min_grosze_min": None,
                "market_price_per_min_grosze_p25": None,
                "market_price_per_min_grosze_median": None,
                "market_price_per_min_grosze_p75": None,
                "market_price_per_min_grosze_max": None,
                "subject_percentile": None,
                "deviation_pct": None,
                "deviation_pct_per_min": None,
                "sample_size": 0,
                "recommended_action": "subject_only",
                "verification_status": "subject_only",
                "verification_details": {"tier": "sub_variant"},
                "competitor_samples": [],
            })
            continue

        market_prices = [float(s["price_grosze"]) for s in samples]
        market_ppms = [
            float(s["price_grosze"]) / float(s["duration_minutes"])
            for s in samples
            if s["duration_minutes"] > 0
        ]
        price_pcts = compute_percentiles(market_prices)
        ppm_pcts = compute_percentiles(market_ppms) if market_ppms else None

        dev_per_min = None
        dev_raw = None
        recommended_action = "hold"
        if (
            subj_ppm is not None
            and ppm_pcts is not None
            and ppm_pcts["market_p50"] > 0
        ):
            dev_per_min = round(
                (subj_ppm - ppm_pcts["market_p50"]) / ppm_pcts["market_p50"] * 100.0,
                2,
            )
            recommended_action = _classify_pricing_action(dev_per_min)
        if price_pcts["market_p50"] > 0:
            dev_raw = round(
                (float(subj_price) - price_pcts["market_p50"])
                / price_pcts["market_p50"] * 100.0,
                2,
            )
            if dev_per_min is None:
                recommended_action = _classify_pricing_action(dev_raw)

        subj_pct = compute_subject_percentile(float(subj_price), market_prices)

        rows.append({
            "report_id": report_id,
            "comparison_tier": "sub_variant",
            "booksy_treatment_id": tid,
            "variant_id": None,
            "sub_variant_group_id": gid,
            "sub_variant_label": subj["sub_variant_label"],
            "treatment_name": (
                subj.get("treatment_name")
                or subj.get("service_name") or f"Treatment {tid}"
            ),
            "treatment_parent_id": subj.get("treatment_parent_id"),
            "subject_price_grosze": int(subj_price),
            "subject_is_from_price": False,
            "subject_duration_minutes": int(subj_dur),
            "subject_price_per_min_grosze": subj_ppm,
            "market_min_grosze": int(price_pcts["market_min"]),
            "market_p25_grosze": int(price_pcts["market_p25"]),
            "market_median_grosze": int(price_pcts["market_p50"]),
            "market_p75_grosze": int(price_pcts["market_p75"]),
            "market_max_grosze": int(price_pcts["market_max"]),
            "market_price_per_min_grosze_min": (
                round(ppm_pcts["market_min"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_p25": (
                round(ppm_pcts["market_p25"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_median": (
                round(ppm_pcts["market_p50"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_p75": (
                round(ppm_pcts["market_p75"], 2) if ppm_pcts else None
            ),
            "market_price_per_min_grosze_max": (
                round(ppm_pcts["market_max"], 2) if ppm_pcts else None
            ),
            "subject_percentile": round(subj_pct, 2),
            "deviation_pct": dev_raw,
            "deviation_pct_per_min": dev_per_min,
            "sample_size": len(samples),
            "recommended_action": recommended_action,
            "verification_status": "verified",
            "verification_details": {
                "tier": "sub_variant",
                "duration_bucket": bucket,
                "unique_competitor_salons": len(
                    set(s["booksy_id"] for s in samples if s.get("booksy_id"))
                ),
            },
            "competitor_samples": [
                {
                    "salon_id": s["salon_id"],
                    "salon_name": s["salon_name"],
                    "booksy_id": s["booksy_id"],
                    "service_id": s["service_id"],
                    "service_name": s["service_name"],
                    "sub_variant_label": s["sub_variant_label"],
                    "price_grosze": s["price_grosze"],
                    "duration_minutes": s["duration_minutes"],
                    # Same sub_variant_group → similarity 1.0 for UI sort.
                    "name_similarity": 1.0,
                }
                for s in samples[:30]
            ],
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


async def _compute_service_gaps(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Compute service gap rows (missing + unique_usp).

    - 'missing': top 10 treatments that ≥1 counts_in_aggregates competitor
      offers but the subject does not. Popularity score = competitor_count
      weighted by review mentions (limited due to 3-sample review cap).
    - 'unique_usp': up to 5 treatments only the subject offers, WERYFIKOWANE
      przez embedding similarity vs wszystkie services konkurentów. Subject
      może mieć brand-specific name (Thunder, Onda, Light&Bright) który
      mapuje na inny tid niż konkurenci, ale TO TA SAMA PROCEDURA.
      User insight: \"jeśli to jest nic innego jak depilacja laserowa
      tylko innym urządzeniem, to są kretynami\" — fałszywe USP rujnują
      pozycjonowanie i marketing.
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
    # Pre-filter: subject services których żaden konkurent NIE ma pod tym
    # samym tid. Drugą iteracją weryfikujemy każdy candidate po embedding
    # similarity — jeśli konkurent ma similar service pod innym tid /
    # inną nazwą, to NIE prawdziwy USP.
    usp_candidates: list[tuple[int, dict[str, Any]]] = []
    for tid, svc in subject_svcs.items():
        if tid in competitor_counts:
            continue
        usp_candidates.append((tid, svc))

    # Verify USP candidates by embedding similarity against every competitor
    # service. Threshold 0.80 — same as pricing verification. False USPs
    # (konkurent ma podobną usługę pod inną nazwą / tid) są dropowane, więc
    # właściciel salonu nie zostanie błędnie zachęcony do marketingu fałszywej
    # unikalności.
    verified_usps: list[dict[str, Any]] = []
    if usp_candidates:
        # 1. Subject candidate embeddings
        candidate_service_ids = [
            int(svc["id"]) for _, svc in usp_candidates
            if isinstance(svc.get("id"), (int, str))
        ]
        candidate_embeddings = await service.get_service_embeddings(
            candidate_service_ids,
        )

        # 2. Competitor service embeddings — across all 5 competitors.
        # Każdy może mieć 30-60 services, łącznie 150-300 embeddings.
        competitor_service_ids: list[int] = []
        competitor_service_meta: dict[int, dict[str, Any]] = {}
        for cand, cdata in aligned_competitors:
            comp_scrape = cdata.get("scrape") or {}
            salon_name = comp_scrape.get("salon_name") or f"Salon #{cand.booksy_id}"
            for csvc in (cdata.get("services") or []):
                csvc_id = csvc.get("id")
                if csvc_id is None or not csvc.get("is_active", True):
                    continue
                if not csvc.get("has_embedding"):
                    continue
                competitor_service_ids.append(int(csvc_id))
                competitor_service_meta[int(csvc_id)] = {
                    "name": csvc.get("name"),
                    "salon_name": salon_name,
                    "booksy_id": cand.booksy_id,
                }
        competitor_embeddings = await service.get_service_embeddings(
            competitor_service_ids,
        )

        from services.pricing_verification import (
            NAME_SIMILARITY_THRESHOLD,
            compute_name_embedding_similarity,
        )

        dropped_pseudo = 0
        for tid, svc in usp_candidates:
            svc_id = svc.get("id")
            cand_emb = candidate_embeddings.get(int(svc_id)) if svc_id is not None else None
            if cand_emb is None:
                # Bez embedding nie możemy zweryfikować → keep as USP
                # (zachowawcza decyzja — można dyskutować).
                verified_usps.append({
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
                continue

            # Find max similarity vs competitor services
            max_sim = -1.0
            max_match_meta: dict[str, Any] | None = None
            for comp_id, comp_emb in competitor_embeddings.items():
                sim = compute_name_embedding_similarity(cand_emb, comp_emb)
                if sim is None:
                    continue
                if sim > max_sim:
                    max_sim = sim
                    max_match_meta = competitor_service_meta.get(comp_id)

            if max_sim >= NAME_SIMILARITY_THRESHOLD:
                # Pseudo-USP — konkurent ma similar service. Drop.
                dropped_pseudo += 1
                logger.info(
                    "Dropped pseudo-USP (sim=%.3f >= %.2f): subject=%r → "
                    "competitor=%r (%s, booksy_id=%s)",
                    max_sim, NAME_SIMILARITY_THRESHOLD,
                    (svc.get("name") or "")[:60],
                    (max_match_meta or {}).get("name", "")[:60],
                    (max_match_meta or {}).get("salon_name", "?"),
                    (max_match_meta or {}).get("booksy_id", "?"),
                )
                continue

            verified_usps.append({
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

        if dropped_pseudo > 0:
            logger.info(
                "USP verification: dropped %d pseudo-USPs (similar to competitor "
                "services under different names/tids) — keeps marketing honest",
                dropped_pseudo,
            )

    # Top 5 by price (higher-priced uniques are typically more valuable USPs)
    verified_usps.sort(
        key=lambda r: -(r["avg_price_grosze"] or 0),
    )
    verified_usps = verified_usps[:5]
    for idx, row in enumerate(verified_usps):
        row["sort_order"] = idx

    return missing + verified_usps


# ---------------------------------------------------------------------------
# Active promotions
# ---------------------------------------------------------------------------


# Generic procedure keywords po polsku — words klient wpisuje w wyszukiwarkę
# Booksy gdy szuka konkretnej usługi. Lista nie jest wyczerpująca — pokrywa
# najpopularniejsze procedury. Każde zawiera "luźne dopasowanie" (substring,
# case-insensitive) bo użytkownicy używają form podstawowych lub deklinacji.
# Mapping: substring keyword → human-readable Polish prefix.
# Lewa kolumna = czego szukamy w nazwie/opisie (lowercase, substring).
# Prawa = sugerowany prefix dla naprawionej nazwy usługi.
# Pierwszy match wygrywa, dlatego specyficzne keywords idą PRZED ogólnymi
# (np. "depilacja laserowa" przed samym "laser").
_PROCEDURE_KEYWORD_MAPPING: list[tuple[str, str]] = [
    # ── specyficzne wcześniej ──
    ("depilacja laserowa", "Depilacja laserowa"),
    ("depilacja pastą cukrową", "Depilacja pastą cukrową"),
    ("depilacja woskiem", "Depilacja woskiem"),
    ("depilacj", "Depilacja"),
    ("mezoterapia mikroigłow", "Mezoterapia mikroigłowa"),
    ("mezoterapia igłow", "Mezoterapia igłowa"),
    ("mezoterap", "Mezoterapia"),
    ("oczyszczanie wodorow", "Oczyszczanie wodorowe"),
    ("oczyszczan", "Oczyszczanie twarzy"),
    ("peeling chemiczny", "Peeling chemiczny"),
    ("peeling kawitacyjn", "Peeling kawitacyjny"),
    ("peeling", "Peeling"),
    ("mikrodermabraz", "Mikrodermabrazja"),
    ("kawitacj", "Kawitacja"),
    ("sonoforez", "Sonoforeza"),
    ("dermomasaż", "Dermomasaż"),
    ("dermomasaz", "Dermomasaż"),
    ("radiofrekwencja mikroigłow", "Radiofrekwencja mikroigłowa"),
    ("radiofrekwencj", "Radiofrekwencja"),
    ("fala radiow", "Fala radiowa"),
    ("hifu", "HIFU"),
    ("ipl", "IPL fotoodmładzanie"),
    ("rf ", "Radiofrekwencja"),
    ("fotoodmłodze", "Fotoodmładzanie"),
    ("fotoodmlodze", "Fotoodmładzanie"),
    ("fototerap", "Fototerapia"),
    ("kriolipoliz", "Kriolipoliza"),
    ("botoks", "Botoks"),
    ("wypełniacz", "Wypełniacze"),
    ("wypelniacz", "Wypełniacze"),
    ("kwas hialuron", "Wypełniacze kwasem hialuronowym"),
    ("kwas migdał", "Peeling kwasem migdałowym"),
    ("kwas migda", "Peeling kwasem migdałowym"),
    ("makijaż permanentny", "Makijaż permanentny"),
    ("makijaż", "Makijaż"),
    ("makijaz", "Makijaż"),
    ("henna brwi", "Henna brwi"),
    ("henna", "Henna"),
    ("regulacja brwi", "Regulacja brwi"),
    ("laminacja brwi", "Laminacja brwi"),
    ("brwi", "Stylizacja brwi"),
    ("przedłużanie rzęs", "Przedłużanie rzęs"),
    ("rzęs", "Stylizacja rzęs"),
    ("rzes", "Stylizacja rzęs"),
    ("manicure hybrydow", "Manicure hybrydowy"),
    ("manicure", "Manicure"),
    ("pedicure", "Pedicure"),
    ("paznok", "Paznokcie"),
    ("masaż relaksacyjn", "Masaż relaksacyjny"),
    ("masaż", "Masaż"),
    ("masaz", "Masaż"),
    ("modelowanie sylwetki", "Modelowanie sylwetki"),
    ("modelowani", "Modelowanie sylwetki"),
    ("endermolog", "Endermologia"),
    ("ujędrnia", "Ujędrnianie skóry"),
    ("ujedrnia", "Ujędrnianie skóry"),
    ("vacuu", "Vacuum body"),
    ("lipolu", "Lipoliza"),
    ("trening ems", "Trening EMS"),
    ("ems", "EMS"),
    ("strzyż", "Strzyżenie"),
    ("koloryzacj", "Koloryzacja"),
    ("balayage", "Balayage"),
    ("keratynow", "Keratynowe prostowanie"),
    ("keratyn", "Keratyna"),
    ("fryzjer", "Fryzjerstwo"),
    ("podolog", "Podologia"),
    ("stóp", "Pielęgnacja stóp"),
    ("lifting", "Lifting"),
    ("odmładz", "Odmładzanie skóry"),
    ("odmladz", "Odmładzanie skóry"),
    ("odmłodze", "Odmładzanie skóry"),
    ("odmlodze", "Odmładzanie skóry"),
    ("rozświetl", "Rozświetlenie skóry"),
    ("rozswietl", "Rozświetlenie skóry"),
    ("rozjaśn", "Rozjaśnianie"),
    ("rozjasn", "Rozjaśnianie"),
    ("regenerac", "Regeneracja"),
    ("stymulac kolagen", "Stymulacja kolagenu"),
    ("stymulac", "Stymulacja"),
    ("rewitalizac", "Rewitalizacja"),
    # baseline catch-all — "laser" zostaje na końcu żeby specyficzne
    # "depilacja laserowa" / "fototerapia" złapały się pierwsze
    ("laser", "Laser"),
]

# Plaska lista samych substring keywords używana w nazwie/opisie scan
_GENERIC_PROCEDURE_KEYWORDS = [k for k, _ in _PROCEDURE_KEYWORD_MAPPING]


def _suggested_prefix_for_keyword(matched_kw: str) -> str:
    """Map matched substring → human-readable prefix.

    Pierwszy match wygrywa, idzie po _PROCEDURE_KEYWORD_MAPPING który jest
    posortowany od najbardziej specyficznego do ogólnego.
    """
    for kw, prefix in _PROCEDURE_KEYWORD_MAPPING:
        if kw == matched_kw:
            return prefix
    return matched_kw.capitalize()


def _detect_hidden_services(
    services: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Zwraca services których name NIE ma generic procedure keyword,
    ale description JĄ ma. To są usługi które klient Booksy nie znajdzie
    w wyszukiwarce — bo wyszukiwarka filtruje po name, nie description.

    Empirycznie (Beauty4ever, report 34): "Thunder - pachy + bikini pełne"
    (brand-only name) z opisem 989 znaków otwierającym się "Depilacja
    laserowa Thunder — Kobieta — pachy + bikini pełne — 1 zabieg ⭕Laser
    Thunder…". Description ma "depilacja laserowa" → klient szukający
    tej frazy by zobaczył tę usługę, gdyby była w nazwie. Sam "Thunder"
    w nazwie = niewidoczna.

    Returns list of {service_id, name, description, matched_keyword,
    suggested_prefix, price_grosze} sorted by price desc.
    """
    # Service names that look like consultations / packages / etc. — exclude
    # od hidden services, bo to NIE są zabiegi do których klient szuka
    # generic procedure ("konsultacja", "voucher").
    _EXCLUDE_NAME_PREFIXES = (
        "konsultacja", "konsultacje",
        "voucher", "bon ", "karta podarunkowa", "kart prezent",
        "pakiet ", "abonament", "karnet",
    )

    # Counter-keywords — gdy występują w opisie BLISKO matched_keyword
    # (w pierwszych 300 chars), traktuj match jako fałszywy. Empirycznie
    # (Beauty4ever Red Touch): opis ma „świetnie nadaje się do
    # poprawiania wypełniaczy" → matchował "wypełniacz" mimo że to laser
    # fractional. Counter-keyword "laser" w pierwszej linii unieważnia
    # mniej specyficzny match.
    _LASER_DEVICE_HINTS = ("laser", "fractional", "frakcyjny", "diodow", "nd:yag")

    out: list[dict[str, Any]] = []
    for svc in services:
        if not svc.get("is_active", True):
            continue
        name = (svc.get("name") or "").strip()
        desc = (svc.get("description") or "").strip()
        if not name or len(desc) < 60:
            continue

        # FIX 2: Wyklucz konsultacje / vouchery / pakiety — to nie są
        # zabiegi z procedury do której pasuje generic keyword.
        name_lower = name.lower()
        if any(name_lower.startswith(p) for p in _EXCLUDE_NAME_PREFIXES):
            continue

        desc_lower = desc.lower()

        # Sprawdź czy NAME nie ma żadnego generic keyword
        name_has_keyword = any(kw in name_lower for kw in _GENERIC_PROCEDURE_KEYWORDS)
        if name_has_keyword:
            continue

        # Find ALL matching keywords w description (zbieramy wszystkie żeby
        # wybrać najbardziej discriminative — pierwszy w mappingu, czyli
        # najspecyficzniejszy. Plus context-aware adjustment niżej.)
        matches_with_pos: list[tuple[str, int]] = []
        for kw in _GENERIC_PROCEDURE_KEYWORDS:
            pos = desc_lower.find(kw)
            if pos >= 0:
                matches_with_pos.append((kw, pos))
        if not matches_with_pos:
            continue

        # Choose by mapping order (specificity), nie po position w opisie.
        # _PROCEDURE_KEYWORD_MAPPING jest posortowane od najspecyficzniejszego
        # ("depilacja laserowa") do ogólnego ("laser"). Empirycznie Thunder:
        # opis zaczyna się "⭕Laser Thunder to najmocniejsza..." więc "laser"
        # ma najwcześniejszą position, ALE w opisie też jest "depilacji
        # laserowej" / "depilacja". Position-based wygrywało "laser",
        # mapping-order wygrywa "depilacj" → suggested "Depilacja Thunder...".
        kw_rank = {kw: idx for idx, kw in enumerate(_GENERIC_PROCEDURE_KEYWORDS)}
        matches_with_pos.sort(key=lambda x: kw_rank.get(x[0], 9999))
        chosen_keyword = matches_with_pos[0][0]

        # FIX 1: Context-aware override TYLKO dla mylących non-laser
        # keywords. Empirycznie Red Touch (laser fractional):
        # "świetnie do poprawiania wypełniaczy" — slowo "wypełniacz"
        # występuje w kontekście "różnice od wypełniaczy", nie jako
        # właściwa procedura. Force "laser" tylko gdy chosen to
        # demonstratywnie zły match dla laser device.
        _MISLEADING_FOR_LASER = {
            "wypełniacz", "wypelniacz", "botoks",
            "kwas hialuron", "manicure", "pedicure",
            "makijaż", "makijaz",
        }
        if chosen_keyword in _MISLEADING_FOR_LASER:
            prefix_text = desc_lower[:300]
            if any(h in prefix_text for h in _LASER_DEVICE_HINTS):
                # Find a laser-family match jeśli istnieje
                _LASER_FAMILY = {"laser", "ipl", "depilacja laserowa",
                                 "depilacj", "fototerap", "hifu",
                                 "fotoodmłodze", "fotoodmlodze", "rf "}
                laser_match = next(
                    (kw for kw, _ in matches_with_pos if kw in _LASER_FAMILY),
                    None,
                )
                if laser_match:
                    chosen_keyword = laser_match

        suggested_prefix = _suggested_prefix_for_keyword(chosen_keyword)

        # FIX 3: skróć powtórzenia — jeśli sugerowany prefix duplikuje słowo
        # w aktualnej nazwie (np. "Depilacja Thunder - Depilacja całe ciało"),
        # zostaw tylko prefix + slug usługi. Plus skróć powtórzenia długich
        # fraz dla czytelności.
        suggested_name = _compose_suggested_name(suggested_prefix, name)

        out.append({
            "service_id": svc.get("id"),
            "name": name,
            "matched_keyword": chosen_keyword,
            "suggested_prefix": suggested_prefix,
            "suggested_name": suggested_name,
            "price_grosze": svc.get("price_grosze"),
            "description_preview": desc[:200],
        })

    # Sort by price desc — drogie ukryte usługi to większa strata
    out.sort(key=lambda x: -(x.get("price_grosze") or 0))
    return out


# Lazy-init Gemini Flash client (OpenAI-compat endpoint). Gemini Flash 2.0
# jest preferowany nad MiniMax M2.7 dla taxonomy disambiguation:
#   - szybszy (~500ms vs ~5-15s na MiniMax thinking)
#   - tańszy (~10x niższy koszt input/output)
#   - deterministyczny JSON output (response_format=json_object) — bez
#     thinking blocks zżerających token budget
# Lazy + cached na proces. None gdy GEMINI_API_KEY niepełny (graceful fallback).
_HIDDEN_LLM_CLIENT: GeminiLLMClient | None = None
_HIDDEN_LLM_TRIED = False


def _get_hidden_inference_llm() -> GeminiLLMClient | None:
    """Lazy-init LLM client dla taxonomy disambiguation. Preferuje OpenAI
    (paid, reliable, JSON output), fallback do Gemini (jeśli paid key
    będzie dostępny w przyszłości). None jeśli żaden klucz."""
    global _HIDDEN_LLM_CLIENT, _HIDDEN_LLM_TRIED
    if _HIDDEN_LLM_TRIED:
        return _HIDDEN_LLM_CLIENT
    _HIDDEN_LLM_TRIED = True

    # Preferowana ścieżka: OpenAI gpt-4o-mini (paid, reliable JSON output).
    import os
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if openai_key:
        try:
            _HIDDEN_LLM_CLIENT = GeminiLLMClient(
                api_key=openai_key, model="gpt-4o-mini", provider="openai",
            )
            logger.info(
                "hidden_service_inference: using OpenAI gpt-4o-mini",
            )
            return _HIDDEN_LLM_CLIENT
        except Exception as e:
            logger.warning(
                "hidden_service_inference: failed to init OpenAI client (%s)",
                e,
            )

    # Fallback: Gemini Flash via OpenAI-compat endpoint (jeśli paid key
    # będzie kiedyś dostępny — obecnie free tier ma limit 0).
    if settings.gemini_api_key:
        try:
            _HIDDEN_LLM_CLIENT = GeminiLLMClient(
                api_key=settings.gemini_api_key,
                model="gemini-2.0-flash",
                provider="gemini",
            )
            logger.info(
                "hidden_service_inference: using Gemini Flash 2.0",
            )
            return _HIDDEN_LLM_CLIENT
        except Exception as e:
            logger.warning(
                "hidden_service_inference: failed to init Gemini client (%s)",
                e,
            )

    logger.info(
        "hidden_service_inference: no LLM key configured — "
        "fallback to keyword mapping",
    )
    _HIDDEN_LLM_CLIENT = None
    return _HIDDEN_LLM_CLIENT


async def _enhance_hidden_services_with_inference(
    hidden_services: list[dict[str, Any]],
    supabase: SupabaseService,
) -> list[dict[str, Any]]:
    """For each detected hidden service, run embedding+LLM taxonomy
    inference and override the keyword-derived suggested prefix/name
    with the inferred canonical Booksy category — IF the inference
    method is 'llm' or 'embedding' with confidence >= threshold.

    Mutates each hidden_service dict in-place adding:
      - `inference_method`: 'llm' | 'embedding' | 'rule' | 'unfixable'
      - `inference_confidence`: float 0-1 (None for rule fallback)
      - `inference_reasoning`: str
      - `inferred_tid`: int | None
      - `parent_category`: str | None

    Falls back to the existing keyword-derived values when inference
    returns 'unfixable' or unavailable (no MiniMax key / RPC failure).
    """
    if not hidden_services:
        return hidden_services

    llm = _get_hidden_inference_llm()

    # 1. Batch-load name_embedding for all candidate service ids.
    service_ids = [
        int(h["service_id"]) for h in hidden_services
        if h.get("service_id") is not None
    ]
    if not service_ids:
        # No service ids — can't load embeddings. Mark all as 'rule' fallback.
        for h in hidden_services:
            h.setdefault("inference_method", "rule")
            h.setdefault("inference_confidence", None)
            h.setdefault("inference_reasoning", "Brak service_id — keyword fallback")
            h.setdefault("inferred_tid", None)
            h.setdefault("parent_category", None)
        return hidden_services

    try:
        emb_map = await supabase.get_service_embeddings(service_ids)
    except Exception as e:
        logger.warning(
            "hidden_service_inference: get_service_embeddings failed (%s) — "
            "marking all as rule fallback",
            e,
        )
        emb_map = {}

    # 2. Build candidate inputs for inference. Each needs name, description,
    #    name_embedding.
    candidates_for_inference: list[tuple[int, dict[str, Any]]] = []
    for idx, h in enumerate(hidden_services):
        sid = h.get("service_id")
        if sid is None:
            continue
        emb = emb_map.get(int(sid))
        if not emb:
            continue
        candidates_for_inference.append((idx, {
            "name": h.get("name") or "",
            "description": h.get("description_preview") or "",
            "name_embedding": emb,
        }))

    # 3. Run inference in parallel (semaphore=4 in batch util — gentle on LLM).
    results: list[dict[str, Any]] = []
    if candidates_for_inference and llm is not None:
        results = await infer_hidden_services_batch(
            [c[1] for c in candidates_for_inference],
            supabase,
            llm,
            min_confidence=HIDDEN_MIN_CONFIDENCE,
        )

    # 4. Apply results — override suggested_prefix / suggested_name where
    #    inference succeeded; otherwise keep keyword fallback that already
    #    sits in hidden_services entries.
    result_iter = iter(results) if results else iter(())
    for idx, _ in candidates_for_inference:
        try:
            res = next(result_iter)
        except StopIteration:
            break
        h = hidden_services[idx]
        method = res.get("method")
        if method in ("llm", "embedding") and res.get("inferred_canonical_name"):
            prefix = res["inferred_canonical_name"]
            h["suggested_prefix"] = prefix
            h["suggested_name"] = _compose_suggested_name(prefix, h.get("name") or "")
            h["inference_method"] = method
            h["inference_confidence"] = res.get("confidence")
            h["inference_reasoning"] = res.get("reasoning")
            h["inferred_tid"] = res.get("inferred_tid")
            h["parent_category"] = res.get("inferred_parent_name")
        else:
            # Unfixable — keep keyword-derived prefix/name, but mark explicitly.
            h["inference_method"] = "rule"
            h["inference_confidence"] = None
            h["inference_reasoning"] = res.get("reasoning") or "LLM unfixable, użyto keyword fallback"
            h["inferred_tid"] = None
            h["parent_category"] = None

    # Ensure all hidden services have the inference fields, even if not run.
    for h in hidden_services:
        h.setdefault("inference_method", "rule")
        h.setdefault("inference_confidence", None)
        h.setdefault("inference_reasoning", "")
        h.setdefault("inferred_tid", None)
        h.setdefault("parent_category", None)

    return hidden_services


def _normalize_synthetic(name: str) -> str:
    """Mirror of `fn_synthetic_normalize(TEXT)` (mig 073): lowercase +
    collapse whitespace. Kept in-process for the local dedup check that
    avoids round-tripping every input string through Postgres.
    """
    if not name:
        return ""
    return " ".join(name.lower().split())


async def _resolve_service_taxonomy(
    supabase: SupabaseService,
    services: list[dict[str, Any]],
    label: str = "salon",
    min_confidence: float = HIDDEN_MIN_CONFIDENCE,
    audit_id: str | None = None,
    trace_collector: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Route services with `booksy_treatment_id IS NULL` (post-Versum) through
    a 4-rule decision tree. NEVER overwrites `treatment_name` or
    `booksy_treatment_id` for Rules 1/2/4 — those services keep NULL Booksy
    tid and pick up a `synthetic_treatment_id` instead.

    Rules (checked in order):

    Reguła 2 — salon-defined category. If the service has `category_name`
        (e.g. "Kroplówki" w cenniku Beauty4ever), we treat that as the
        salon's own category label. Normalize, look up / insert in
        `synthetic_treatment_categories` with source='salon_defined'.
        DB partial unique index dedupes across audits.

    Reguła 3 — LLM disambiguation hits an existing Booksy tid. Existing
        `infer_hidden_services_batch` returns inferred_tid + confidence.
        If confidence >= min_confidence: set `booksy_treatment_id` (with
        `_raw` preserving the original NULL) and trigger inline variant
        matching. `treatment_name` is NOT overwritten — display layer
        uses original `svc["name"]`.

    Reguła 4 — embedding inheritance from existing synthetic. Use the
        service's name_embedding to find the top match in
        `synthetic_treatment_categories` (cosine >= 0.85, any source).
        Reuse + bump merged_count.

    Reguła 1 — LLM short generation creates a new synthetic. Last resort.
        Generates a 1-2 word category via OpenAI gpt-4o-mini, embeds it,
        inserts a fresh `llm_generated` row.

    Returns a stats dict: `{rule_2, rule_3, rule_4, rule_1, skipped,
    rule_3_with_variant}`.

    All branches mutate `services` in place (the post-routing state is the
    source of truth for the rest of the pipeline). If `trace_collector` is
    provided, each candidate appends one structured trace entry describing
    the path taken, embedding top-K, LLM responses, and final assignment.
    `dry_run=True` skips DB INSERTs (Rules 1/2) and merged_count bumps
    (Rule 4) — used by the dev `/api/dev/trace-taxonomy` endpoint to debug
    routing without polluting the synthetic catalog.

    Raises on DB / embedding / LLM failures — directive 2026-05-16: no
    graceful try/except in this path; Bugsink captures root causes.
    """
    from services.hidden_service_inference import (
        embed_short_text,
        generate_short_category,
        infer_hidden_services_batch,
    )

    stats = {
        "rule_2": 0,
        "rule_3": 0,
        "rule_4": 0,
        "rule_1": 0,
        "skipped": 0,
        "rule_3_with_variant": 0,
    }

    # ── 0. Identify candidates: NULL booksy_treatment_id + active. ──
    candidates: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in enumerate(services):
        if not svc.get("is_active", True):
            continue
        if svc.get("booksy_treatment_id") is not None:
            continue
        name = (svc.get("name") or "").strip()
        if not name or len(name) < 3:
            continue
        sid = svc.get("id")
        if sid is None:
            continue
        candidates.append((idx, svc))

    if not candidates:
        logger.debug(
            "_resolve_service_taxonomy [%s]: no NULL-tid candidates", label,
        )
        return stats

    logger.info(
        "_resolve_service_taxonomy [%s]: %d NULL-tid candidates entering "
        "4-rule routing (audit_id=%s, dry_run=%s)",
        label, len(candidates), audit_id, dry_run,
    )

    # ── 1. Preload name_embedding for ALL candidates (Rules 3/4 need it,
    #       Rule 1's category embed is generated inline). ──
    service_ids = [int(svc["id"]) for _, svc in candidates]
    emb_map = await supabase.get_service_embeddings(service_ids)
    logger.info(
        "_resolve_service_taxonomy [%s]: loaded %d/%d service embeddings",
        label, len(emb_map), len(service_ids),
    )

    # ── 2. ALL candidates enter Rule 3 first (per user spec 2026-05-16:
    #      "Jeśli mamy dodaną kategorie ręcznie A NIE MA JEJ W SYSTEMIE"
    #      means Rule 2 only fires for services whose category doesn't
    #      resolve to a Booksy tid). Rule 2 becomes a fallback after Rule 3
    #      fails — see the second Rule 2 block AFTER Rule 3 below. ──
    rule_3_4_1_candidates: list[tuple[int, dict[str, Any]]] = list(candidates)

    # ── 3. Rule 3 (LLM disambiguation against Booksy tids). ──
    #     Only services that ALSO have a description >= 30 chars qualify
    #     for the heavy LLM path (matches existing infer_hidden_services_batch
    #     contract). Services without description fall through to Rule 4.
    rule3_eligible: list[tuple[int, dict[str, Any]]] = []
    rule4_only: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in rule_3_4_1_candidates:
        emb = emb_map.get(int(svc["id"]))
        if not emb:
            # No embedding → can't run Rule 3 (which uses centroid match)
            # NOR Rule 4 (which uses synthetic ANN). Push straight to Rule 1.
            rule4_only.append((idx, svc))  # actually rule_1, handled below
            continue
        desc = (svc.get("description") or "").strip()
        if len(desc) >= 30:
            rule3_eligible.append((idx, svc))
        else:
            rule4_only.append((idx, svc))

    llm_client = _get_hidden_inference_llm()
    if rule3_eligible:
        if llm_client is None:
            logger.warning(
                "_resolve_service_taxonomy [%s] rule_3: %d eligible but no "
                "LLM key — pushing all to Rule 4",
                label, len(rule3_eligible),
            )
            rule4_only.extend(rule3_eligible)
            rule3_eligible = []

    rule3_to_rule4_fallback: list[tuple[int, dict[str, Any]]] = []
    if rule3_eligible:
        logger.info(
            "_resolve_service_taxonomy [%s] rule_3: invoking LLM disambiguation "
            "for %d services",
            label, len(rule3_eligible),
        )
        inference_inputs = [
            {
                "name": svc.get("name") or "",
                "description": svc.get("description") or "",
                "name_embedding": emb_map[int(svc["id"])],
                # Pass salon category as a hint to the LLM so it can return
                # tid=null when salon's category doesn't match any Booksy
                # candidate (e.g. "Kroplówki" vs candidates like Tlenoterapia).
                "category_name": svc.get("category_name"),
            }
            for _, svc in rule3_eligible
        ]
        results = await infer_hidden_services_batch(
            inference_inputs, supabase, llm_client,
            min_confidence=min_confidence,
        )

        variant_match_tasks: list[
            tuple[int, dict[str, Any], list[float], int]
        ] = []
        for (idx, svc), res in zip(rule3_eligible, results):
            method = res.get("method")
            inferred_tid = res.get("inferred_tid")
            confidence = float(res.get("confidence") or 0.0)
            logger.info(
                "_resolve_service_taxonomy [%s] rule_3: svc_id=%s name=%r "
                "method=%s tid=%s confidence=%.3f",
                label, svc.get("id"), (svc.get("name") or "")[:60],
                method, inferred_tid, confidence,
            )
            if (
                method in ("llm", "embedding")
                and inferred_tid is not None
                and confidence >= min_confidence
            ):
                # Preserve original NULL under `_raw` so downstream readers
                # that do `_raw or booksy_treatment_id` see NULL (matches
                # existing semantic where _raw wins when present).
                if "booksy_treatment_id_raw" not in svc:
                    svc["booksy_treatment_id_raw"] = svc.get("booksy_treatment_id")
                svc["booksy_treatment_id"] = int(inferred_tid)
                # Do NOT touch svc["treatment_name"] — display uses original
                # svc["name"] so the matrix matches the salon's actual cennik.
                svc["taxonomy_source"] = "booksy_inferred"
                svc["taxonomy_inference_source"] = method
                svc["taxonomy_inference_confidence"] = confidence
                svc["taxonomy_inference_parent"] = res.get("inferred_parent_name")
                stats["rule_3"] += 1
                emb = emb_map.get(int(svc["id"]))
                if emb:
                    variant_match_tasks.append(
                        (idx, svc, emb, int(inferred_tid))
                    )
                if trace_collector is not None:
                    trace_collector.append({
                        "svc_id": svc.get("id"),
                        "svc_name": svc.get("name"),
                        "original_tid": None,
                        "original_category": svc.get("category_name"),
                        "rule": "3",
                        "decision": "matched",
                        "details": {
                            "method": method,
                            "confidence": confidence,
                            "inferred_canonical_name": res.get(
                                "inferred_canonical_name"
                            ),
                            "inferred_parent_name": res.get(
                                "inferred_parent_name"
                            ),
                            "reasoning": res.get("reasoning"),
                            "candidate_count": res.get("candidate_count"),
                        },
                        "embedding_top_k": [],
                        "llm_response": {
                            "tid": inferred_tid,
                            "confidence": confidence,
                            "reasoning": res.get("reasoning"),
                        },
                        "final": {
                            "booksy_tid": int(inferred_tid),
                            "synthetic_tid": None,
                            "taxonomy_source": "booksy_inferred",
                            "treatment_name": svc.get("name"),
                        },
                    })
            else:
                # Rule 3 failed → fall through to Rule 4 for this service.
                logger.info(
                    "_resolve_service_taxonomy [%s] rule_3 → rule_4: svc_id=%s "
                    "name=%r (reason=%s, conf=%.3f)",
                    label, svc.get("id"),
                    (svc.get("name") or "")[:60],
                    method, confidence,
                )
                rule3_to_rule4_fallback.append((idx, svc))
                if trace_collector is not None:
                    trace_collector.append({
                        "svc_id": svc.get("id"),
                        "svc_name": svc.get("name"),
                        "original_tid": None,
                        "original_category": svc.get("category_name"),
                        "rule": "3",
                        "decision": "skipped",
                        "details": {
                            "method": method,
                            "confidence": confidence,
                            "reasoning": res.get("reasoning"),
                            "falls_through_to": "4",
                        },
                        "embedding_top_k": [],
                        "llm_response": {
                            "tid": inferred_tid,
                            "confidence": confidence,
                            "reasoning": res.get("reasoning"),
                        },
                        "final": None,
                    })

        # Inline variant matching for Rule 3 hits (Phase 5 hard gate).
        if variant_match_tasks:
            import asyncio as _asyncio
            sem_vm = _asyncio.Semaphore(8)

            async def _match_variant(
                svc: dict[str, Any], emb: list[float], tid: int,
            ) -> None:
                def _do_call() -> Any:
                    return supabase.client.rpc(
                        "match_service_to_variant",
                        {
                            "p_embedding": emb,
                            "p_parent_treatment_id": tid,
                            "p_min_similarity": 0.55,
                        },
                    ).execute()

                async with sem_vm:
                    res2 = await _asyncio.to_thread(_do_call)
                rows = list(res2.data or [])
                if not rows:
                    return
                row = rows[0]
                vid = row.get("variant_id")
                if vid is None:
                    return
                svc["variant_id"] = int(vid)
                if row.get("canonical_variant_name"):
                    svc["variant_canonical_name"] = row["canonical_variant_name"]

            await _asyncio.gather(*(
                _match_variant(s, e, t)
                for (_, s, e, t) in variant_match_tasks
            ))
            stats["rule_3_with_variant"] = sum(
                1 for (_, s, _, _) in variant_match_tasks
                if s.get("variant_id") is not None
            )

    # ── 3b. Rule 2 (salon-defined synthetic) — runs AFTER Rule 3 so we
    #       only fire it for services whose category did NOT resolve to a
    #       Booksy native tid via LLM disambiguation. Per user spec
    #       2026-05-16: "Jeśli mamy dodaną kategorie ręcznie A NIE MA JEJ
    #       W SYSTEMIE wciągmay ją" — Rule 2 trigger requires both
    #       category_name set AND no Booksy match.
    #
    #       Group by normalized category_name so a single synthetic row
    #       backs every service in the bucket. Services that fell through
    #       Rule 3 here AND lack category_name proceed to Rule 4.
    rule2_queue = rule4_only + rule3_to_rule4_fallback
    rule2_groups: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    rule_4_queue_after_rule2: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in rule2_queue:
        cat = (svc.get("category_name") or "").strip()
        if cat and cat.lower() != "bez kategorii":
            norm = _normalize_synthetic(cat)
            rule2_groups.setdefault(norm, []).append((idx, svc))
        else:
            rule_4_queue_after_rule2.append((idx, svc))

    for norm_name, group in rule2_groups.items():
        canon_choices = [
            (s.get("category_name") or "").strip() for _, s in group
        ]
        canonical = max(set(canon_choices), key=canon_choices.count)
        logger.info(
            "_resolve_service_taxonomy [%s] rule_2: canonical=%r normalized=%r "
            "group_size=%d dry_run=%s",
            label, canonical, norm_name, len(group), dry_run,
        )
        if dry_run:
            syn_id = -1
        else:
            embedding = await embed_short_text(canonical)
            syn_id = await supabase.upsert_synthetic_category_salon_defined(
                normalized_name=norm_name,
                canonical_name=canonical,
                embedding=embedding,
                audit_id=audit_id,
            )
            logger.info(
                "_resolve_service_taxonomy [%s] rule_2: upserted "
                "synthetic_id=%d canonical=%r (audit_id=%s)",
                label, syn_id, canonical, audit_id,
            )

        for idx, svc in group:
            svc["synthetic_treatment_id"] = syn_id
            svc["taxonomy_source"] = "salon_defined"
            svc["synthetic_canonical_name"] = canonical
            stats["rule_2"] += 1
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "2",
                    "decision": "matched",
                    "details": {
                        "normalized_name": norm_name,
                        "canonical_name": canonical,
                        "group_size_in_audit": len(group),
                        "fired_after_rule_3_failed": True,
                    },
                    "embedding_top_k": [],
                    "llm_response": None,
                    "final": {
                        "booksy_tid": None,
                        "synthetic_tid": syn_id,
                        "taxonomy_source": "salon_defined",
                        "treatment_name": svc.get("name"),
                    },
                })

    if stats["rule_2"]:
        logger.info(
            "_resolve_service_taxonomy [%s] rule_2 summary: %d services "
            "across %d unique synthetic categories (post-Rule 3 fallback)",
            label, stats["rule_2"], len(rule2_groups),
        )

    # ── 4. Rule 4 (embedding inheritance from existing synthetic). ──
    #     Queue now contains services that fell through BOTH Rule 3
    #     (no Booksy match) AND Rule 2 (no salon-defined category).
    rule4_queue = rule_4_queue_after_rule2
    rule4_unmatched: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in rule4_queue:
        emb = emb_map.get(int(svc["id"]))
        if not emb:
            # No embedding at all → can only go to Rule 1 if we have a
            # service name (which we do, gated above).
            rule4_unmatched.append((idx, svc))
            continue
        match = await supabase.find_synthetic_category_by_embedding(
            embedding=emb, min_similarity=0.85,
        )
        logger.info(
            "_resolve_service_taxonomy [%s] rule_4: svc_id=%s name=%r "
            "match=%s",
            label, svc.get("id"), (svc.get("name") or "")[:60],
            f"id={match['id']} sim={match['similarity']:.3f} "
            f"canonical={match.get('canonical_name')!r}" if match else "none",
        )
        if match is not None:
            syn_id = int(match["id"])
            svc["synthetic_treatment_id"] = syn_id
            svc["taxonomy_source"] = "inherited"
            svc["synthetic_canonical_name"] = match.get("canonical_name")
            stats["rule_4"] += 1
            if not dry_run:
                await supabase.increment_synthetic_merged_count(syn_id)
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "4",
                    "decision": "inherited",
                    "details": {
                        "synthetic_id": syn_id,
                        "canonical_name": match.get("canonical_name"),
                        "similarity": match["similarity"],
                        "source": match.get("source"),
                    },
                    "embedding_top_k": [{
                        "synthetic_id": syn_id,
                        "canonical_name": match.get("canonical_name"),
                        "similarity": match["similarity"],
                    }],
                    "llm_response": None,
                    "final": {
                        "booksy_tid": None,
                        "synthetic_tid": syn_id,
                        "taxonomy_source": "inherited",
                        "treatment_name": svc.get("name"),
                    },
                })
        else:
            rule4_unmatched.append((idx, svc))

    # ── 5. Rule 1 (LLM short generation creates new synthetic). ──
    if rule4_unmatched and llm_client is None:
        logger.warning(
            "_resolve_service_taxonomy [%s] rule_1: %d services left but no "
            "LLM key — marking as skipped",
            label, len(rule4_unmatched),
        )
        for idx, svc in rule4_unmatched:
            stats["skipped"] += 1
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "1",
                    "decision": "skipped",
                    "details": {"reason": "no LLM client available"},
                    "embedding_top_k": [],
                    "llm_response": None,
                    "final": None,
                })
    else:
        for idx, svc in rule4_unmatched:
            name = (svc.get("name") or "").strip()
            price_pln: float | None = None
            pg = svc.get("price_grosze")
            if pg is not None:
                price_pln = float(pg) / 100.0
            short = await generate_short_category(
                service_name=name,
                price_pln=price_pln,
                duration_min=svc.get("duration_minutes"),
                category_name=svc.get("category_name"),
                llm=llm_client,
            )
            canonical = short["category"]
            longer = short.get("longer_description") or canonical
            logger.info(
                "_resolve_service_taxonomy [%s] rule_1: svc_id=%s name=%r "
                "generated canonical=%r (dry_run=%s)",
                label, svc.get("id"), name[:60], canonical, dry_run,
            )
            if dry_run:
                syn_id = -1
            else:
                # Embed the LONGER description for better ANN inheritance
                # in future Rule 4 lookups (canonical alone is too sparse).
                embedding = await embed_short_text(longer)
                # Dedup ONE more time against existing synthetic catalog
                # using the new embedding — covers the case where LLM
                # generated a phrase that semantically matches an existing
                # row even though Rule 4 (using service-name embedding)
                # missed it.
                existing = await supabase.find_synthetic_category_by_embedding(
                    embedding=embedding, min_similarity=0.85,
                )
                if existing is not None:
                    syn_id = int(existing["id"])
                    await supabase.increment_synthetic_merged_count(syn_id)
                    logger.info(
                        "_resolve_service_taxonomy [%s] rule_1 → inherit: "
                        "svc_id=%s matched existing synthetic_id=%d "
                        "sim=%.3f",
                        label, svc.get("id"), syn_id,
                        existing["similarity"],
                    )
                    canonical = existing.get("canonical_name") or canonical
                else:
                    syn_id = await supabase.upsert_synthetic_category_llm_generated(
                        canonical_name=canonical,
                        embedding=embedding,
                        audit_id=audit_id,
                    )
                    logger.info(
                        "_resolve_service_taxonomy [%s] rule_1: inserted "
                        "synthetic_id=%d canonical=%r",
                        label, syn_id, canonical,
                    )
            svc["synthetic_treatment_id"] = syn_id
            svc["taxonomy_source"] = "llm_generated"
            svc["synthetic_canonical_name"] = canonical
            stats["rule_1"] += 1
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "1",
                    "decision": "generated",
                    "details": {
                        "canonical_name": canonical,
                        "longer_description": longer,
                    },
                    "embedding_top_k": [],
                    "llm_response": short,
                    "final": {
                        "booksy_tid": None,
                        "synthetic_tid": syn_id,
                        "taxonomy_source": "llm_generated",
                        "treatment_name": svc.get("name"),
                    },
                })

    logger.info(
        "_resolve_service_taxonomy [%s] summary: rule_2=%d rule_3=%d "
        "(with_variant=%d) rule_4=%d rule_1=%d skipped=%d total=%d "
        "(dry_run=%s)",
        label, stats["rule_2"], stats["rule_3"], stats["rule_3_with_variant"],
        stats["rule_4"], stats["rule_1"], stats["skipped"], len(candidates),
        dry_run,
    )
    return stats


async def _apply_llm_taxonomy_to_null_tid_services(
    supabase: SupabaseService,
    services: list[dict[str, Any]],
    label: str = "salon",
    min_confidence: float = HIDDEN_MIN_CONFIDENCE,
    audit_id: str | None = None,
) -> int:
    """Backward-compatible shim around `_resolve_service_taxonomy`.

    Old callers used the int return value as a counter of "tid overrides".
    The new router can override booksy_treatment_id (Rule 3) OR attach a
    synthetic_treatment_id (Rules 1/2/4). For continuity we return the sum
    of rule_2 + rule_3 + rule_4 + rule_1 (i.e. every service that exited
    the router with a routing decision attached). Pipelines that need the
    finer breakdown should call `_resolve_service_taxonomy` directly.
    """
    stats = await _resolve_service_taxonomy(
        supabase, services, label=label,
        min_confidence=min_confidence, audit_id=audit_id,
    )
    return stats["rule_2"] + stats["rule_3"] + stats["rule_4"] + stats["rule_1"]


def _compose_suggested_name(prefix: str, current_name: str) -> str:
    """Compose human-readable suggested name from prefix + current name.

    Avoid duplicate words (e.g. "Depilacja Depilacja Thunder"). Trim
    leading symbols (✦, ⭕) from current_name so they don't pollute
    the new suggestion.
    """
    # Strip leading non-word symbols + spaces from current name
    cleaned = current_name.lstrip("✦⭕🔲💎⭐•· -*").strip()
    # Drop hyphen-prefixed leading separators
    if cleaned.startswith("- "):
        cleaned = cleaned[2:].strip()

    # Avoid duplicate first word: if cleaned starts with the same word as prefix,
    # don't double it. E.g. prefix="Depilacja" + cleaned="Depilacja laserowa..."
    # → return cleaned ("Depilacja laserowa..." — opis salonu).
    prefix_first_word = prefix.split()[0].lower() if prefix else ""
    cleaned_first_word = cleaned.split()[0].lower() if cleaned else ""
    if prefix_first_word and prefix_first_word == cleaned_first_word:
        return cleaned

    return f"{prefix} {cleaned}".strip()


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

    Filters entries where promoPrice IS NULL AND discountPct IS NULL — those
    aren't actual promotions, just regular prices. UI labels the field as
    "Active promotions" so passing through regular prices is misleading.
    Filter dropped at this layer so downstream renderers don't need to
    re-implement the check. Empty competitor lists are also dropped
    (don't show an empty "no promotions" key per salon).
    """
    def _is_real_promo(p: dict[str, Any]) -> bool:
        return p.get("promoPrice") is not None or p.get("discountPct") is not None

    subject_promos = [p for p in promo_map.get(subject_booksy_id, []) if _is_real_promo(p)]
    competitor_promos: dict[str, list[dict[str, Any]]] = {}
    for c in candidates:
        promos = [p for p in promo_map.get(c.booksy_id, []) if _is_real_promo(p)]
        if not promos:
            continue
        # Mig 064 follow-up: stamp salonName on every promo entry. UI's
        # mapPromotionsFromBagent needs the salon label, but the keyed
        # `competitors: {<booksy_id>: [...]}` shape used to leave entries
        # anonymous — the adapter then had to look the name up via the
        # competitors map keyed by booksyId, which Convex's CompetitorMatch
        # doesn't expose (only competitorSalonId). Embedding salonName at
        # build time is O(promo) and keeps the snapshot self-contained.
        salon_name = c.name or f"Salon {c.booksy_id}"
        for entry in promos:
            entry.setdefault("salonName", salon_name)
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


# ─────────────────────────────────────────────────────────────────────────────
# Faza 8a — verified-match-count aggregation (2026-05-17)
# ─────────────────────────────────────────────────────────────────────────────

# Re-bucketing thresholds. Counts are over DISTINCT subject-tids where the
# competitor offered at least one LLM-verified comparable service. We do
# not double-count multiple samples from the same competitor under one tid.
_VERIFIED_BUCKET_DIRECT_MIN = 10
_VERIFIED_BUCKET_CLUSTER_MIN = 5
_VERIFIED_BUCKET_ASPIRATIONAL_MIN = 3


async def _aggregate_verified_match_counts(
    service: "SupabaseService",
    report_id: int,
    pricing_rows: list[dict[str, Any]],
) -> dict[int, int]:
    """Aggregate Faza 7 LLM verdicts per competitor for this report and
    re-bucket competitor_matches. Returns {competitor_salon_id:
    verified_match_count} for downstream logging.

    Counts DISTINCT (competitor_salon_id, booksy_treatment_id OR
    synthetic_treatment_id) tuples where any sample for that competitor
    in that tid had llm_verified=true. We don't reward a competitor for
    having three matches under one tid — what matters is how many of
    MY treatment categories they actually offer comparable services in.
    """
    if not pricing_rows:
        return {}

    counts: dict[int, set[tuple[str, int]]] = {}
    for row in pricing_rows:
        tid_kind = "synthetic" if row.get("synthetic_treatment_id") else "booksy"
        tid_value = (
            row.get("synthetic_treatment_id")
            or row.get("booksy_treatment_id")
        )
        if tid_value is None:
            continue
        samples = row.get("competitor_samples") or []
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            # Treat missing llm_verified as True (legacy / tier=variant
            # samples auto-pass). False is only set explicitly by the
            # Faza 7 verifier when LLM rejected.
            verified = sample.get("llm_verified", True)
            if verified is False:
                continue
            comp_salon_id = sample.get("salon_id")
            if comp_salon_id is None:
                continue
            try:
                comp_salon_id = int(comp_salon_id)
            except (TypeError, ValueError):
                continue
            counts.setdefault(comp_salon_id, set()).add(
                (tid_kind, int(tid_value))
            )

    # Convert sets → scalar counts for persistence.
    verified_counts: dict[int, int] = {
        sid: len(tids) for sid, tids in counts.items()
    }

    if not verified_counts:
        logger.warning(
            "Faza 8a: zero verified matches across %d pricing rows — "
            "either LLM verify produced no keepers or competitor_samples "
            "were stripped. Skipping re-bucket.",
            len(pricing_rows),
        )
        return {}

    # Load current bucket assignments so we can persist bucket_pre_verify
    # for audit. Fetch as a single batched call.
    existing_matches = await service.get_competitor_matches(report_id)
    bucket_pre_verify: dict[int, str] = {}
    for m in existing_matches:
        sid = m.get("competitor_salon_id")
        if sid is None:
            continue
        try:
            bucket_pre_verify[int(sid)] = m.get("bucket") or "unknown"
        except (TypeError, ValueError):
            continue

    # Compute new bucket per competitor + collect update batch.
    updates: list[dict[str, Any]] = []
    dropped_low_verified = 0
    for m in existing_matches:
        sid_raw = m.get("competitor_salon_id")
        if sid_raw is None:
            continue
        try:
            sid = int(sid_raw)
        except (TypeError, ValueError):
            continue
        vcount = verified_counts.get(sid, 0)
        if vcount >= _VERIFIED_BUCKET_DIRECT_MIN:
            new_bucket = "direct"
        elif vcount >= _VERIFIED_BUCKET_CLUSTER_MIN:
            new_bucket = "cluster"
        elif vcount >= _VERIFIED_BUCKET_ASPIRATIONAL_MIN:
            new_bucket = "aspirational"
        else:
            new_bucket = "excluded"
            dropped_low_verified += 1
        updates.append({
            "id": m.get("id"),
            "verified_match_count": vcount,
            "bucket_pre_verify": bucket_pre_verify.get(sid),
            "bucket": new_bucket,
            # counts_in_aggregates collapses to True only when the
            # competitor is real enough to feed the pricing matrix.
            # 'excluded' competitors are kept in DB but flagged out of
            # all aggregate stats and out of competitorProfiles.
            "counts_in_aggregates": new_bucket != "excluded",
        })

    # Persist via dedicated method on SupabaseService (PATCH per row).
    await service.update_competitor_matches_verify_buckets(report_id, updates)

    logger.info(
        "Faza 8a: re-bucketed %d competitors (direct=%d, cluster=%d, "
        "aspirational=%d, excluded=%d) using verified_match_count from "
        "%d pricing rows",
        len(updates),
        sum(1 for u in updates if u["bucket"] == "direct"),
        sum(1 for u in updates if u["bucket"] == "cluster"),
        sum(1 for u in updates if u["bucket"] == "aspirational"),
        dropped_low_verified,
        len(pricing_rows),
    )
    return verified_counts


# ─────────────────────────────────────────────────────────────────────────────
# Faza 8b — package economics analysis (2026-05-17)
# ─────────────────────────────────────────────────────────────────────────────

# Heuristic patterns to surface subject packages BEFORE involving LLM.
# Each regex catches a different package marker — pakiety, multiplier
# notation ("3x", "5 +1"), monthly abonament, body-area bundles.
_PACKAGE_HEURISTIC_PATTERNS = [
    re.compile(r"\bpakiet\b", re.IGNORECASE),
    re.compile(r"\babonament\b", re.IGNORECASE),
    re.compile(r"\bkarnet\b", re.IGNORECASE),
    re.compile(r"\bvoucher\b", re.IGNORECASE),
    re.compile(r"\bbon\b\s*\d+", re.IGNORECASE),
    re.compile(r"^\s*\d+\s*x\b", re.IGNORECASE),       # "3x Red Touch"
    re.compile(r"\s\d+\s*x\b", re.IGNORECASE),         # "Red Touch 3x"
    re.compile(r"\b\d+\s*zabieg(?:ów|i|y)?\b", re.IGNORECASE),
    re.compile(r"\b\d+\s*\+\s*\d+\s*zabieg", re.IGNORECASE),  # "5 + 1 zabieg"
    re.compile(r"\b\d+\s*sesj", re.IGNORECASE),
    re.compile(r"\b\d+\s*wizyt", re.IGNORECASE),
]


def _detect_session_count_from_name(name: str) -> int:
    """Best-effort session count extractor. Returns 1 for unrecognised
    patterns so downstream economics math is conservative (assumes one
    session when in doubt, so discount % is computed honestly).
    """
    if not name:
        return 1
    # 5 + 1 zabieg → 6 sessions
    m = re.search(r"\b(\d+)\s*\+\s*(\d+)\s*zabieg", name, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1)) + int(m.group(2))
        except (ValueError, TypeError):
            pass
    # 3x | 5x | 10x | "pakiet 5x" | "Red Touch 3x"
    m = re.search(r"\b(\d+)\s*x\b", name, re.IGNORECASE)
    if m:
        try:
            n = int(m.group(1))
            if 2 <= n <= 30:
                return n
        except (ValueError, TypeError):
            pass
    # "3 zabiegi", "5 zabiegów"
    m = re.search(r"\b(\d+)\s*zabieg(?:ów|i|y)?\b", name, re.IGNORECASE)
    if m:
        try:
            n = int(m.group(1))
            if 2 <= n <= 30:
                return n
        except (ValueError, TypeError):
            pass
    # "5 sesji"
    m = re.search(r"\b(\d+)\s*sesj", name, re.IGNORECASE)
    if m:
        try:
            n = int(m.group(1))
            if 2 <= n <= 30:
                return n
        except (ValueError, TypeError):
            pass
    return 1


def _detect_area_count_from_name(name: str) -> int:
    """Count distinct body-area mentions joined by " + " or "/" — used
    when the package is a bundle (e.g. "twarz + szyja + dekolt" = 3
    areas). Returns 1 when no obvious bundle pattern.
    """
    if not name:
        return 1
    # "twarz + szyja + dekolt"  → 3 segments
    # Conservative: count " + " separators, cap at 5.
    n_plus = name.count(" + ")
    if n_plus >= 1:
        return min(n_plus + 1, 5)
    return 1


def _is_subject_package(svc: dict[str, Any]) -> bool:
    name = svc.get("name") or ""
    for pat in _PACKAGE_HEURISTIC_PATTERNS:
        if pat.search(name):
            return True
    # Bundle of >=2 body areas joined by " + " counts as package for
    # economic analysis (one-shot multi-area discount question).
    if _detect_area_count_from_name(name) >= 2:
        return True
    return False


async def _analyze_subject_packages(
    service: "SupabaseService",
    report_id: int,
    subject_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """Faza 8b: for each subject package, find a matching single at the
    SAME salon and compute discount economics. Persists to
    competitor_reports.package_analysis. Returns the list for logging.

    Matching strategy (no extra LLM calls in this pass — relies on
    existing tid mapping + heuristic):
      1. Identify packages via name heuristics (_is_subject_package).
      2. For each package: search subject_data.services for a single
         with same booksy_treatment_id (or synthetic_tid) AND session
         count = 1 AND area count = 1.
      3. Compute per_session_in_package = package_price / (sessions × areas).
      4. discount_pct = (single - per_session) / single × 100.
      5. Classify verdict per migration 082 enum.

    LLM-judged subject classification can be added later — for now the
    deterministic heuristic catches most of Beauty4ever's 25+ packages.
    """
    services = subject_data.get("services") or []
    if not services:
        return []

    # Index singles by tid_key so we can look up O(1) per package.
    singles_by_key: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for svc in services:
        if not svc.get("is_active", True):
            continue
        if svc.get("price_grosze") in (None, 0):
            continue
        if _is_subject_package(svc):
            continue  # singles are everything NOT a package
        if _detect_session_count_from_name(svc.get("name") or "") != 1:
            continue
        if _detect_area_count_from_name(svc.get("name") or "") != 1:
            continue
        key = _tid_key(svc)
        if key is None:
            continue
        singles_by_key.setdefault(key, []).append(svc)

    analyses: list[dict[str, Any]] = []
    for svc in services:
        if not svc.get("is_active", True):
            continue
        if not _is_subject_package(svc):
            continue
        price = svc.get("price_grosze")
        if price in (None, 0):
            continue
        try:
            price_grosze = int(price)
        except (TypeError, ValueError):
            continue
        name = svc.get("name") or ""
        sessions = _detect_session_count_from_name(name)
        areas = _detect_area_count_from_name(name)
        units = max(sessions * areas, 1)
        per_session_grosze = price_grosze // units

        # Find best single at same salon: same tid_key, closest price.
        key = _tid_key(svc)
        single_match: dict[str, Any] | None = None
        if key is not None:
            candidates = singles_by_key.get(key) or []
            # Pick the single with the closest duration if the package
            # has a per-session duration hint; fall back to median price.
            target_duration = svc.get("duration_minutes")
            if target_duration:
                candidates_sorted = sorted(
                    candidates,
                    key=lambda c: abs(
                        (c.get("duration_minutes") or 0) - target_duration
                    ),
                )
            else:
                # Closest price to per_session estimate.
                candidates_sorted = sorted(
                    candidates,
                    key=lambda c: abs(
                        (c.get("price_grosze") or 0) - per_session_grosze
                    ),
                )
            single_match = candidates_sorted[0] if candidates_sorted else None

        if single_match is None:
            verdict = "no_single_match"
            discount_pct: float | None = None
            reasoning = (
                f"Brak pojedynczego odpowiednika u tego samego salonu "
                f"dla tid={key}. Nie da się obliczyć rzeczywistego rabatu."
            )
            single_price = None
            single_name = None
            single_id = None
        else:
            single_price_g = int(single_match.get("price_grosze") or 0)
            single_id = single_match.get("id")
            single_name = single_match.get("name")
            single_price = single_price_g
            if single_price_g > 0:
                discount_pct = round(
                    (single_price_g - per_session_grosze)
                    / single_price_g * 100.0,
                    1,
                )
            else:
                discount_pct = None
            if discount_pct is None:
                verdict = "no_single_match"
                reasoning = (
                    f"Pojedyncza usługa {single_name!r} ma cenę 0 zł — "
                    f"nie da się obliczyć rabatu."
                )
            elif discount_pct >= 5.0:
                verdict = "fair_discount"
                reasoning = (
                    f"Pakiet daje realny rabat {discount_pct:.1f}% per "
                    f"zabieg/obszar ({per_session_grosze/100:.0f} zł vs "
                    f"{single_price_g/100:.0f} zł single)."
                )
            elif discount_pct <= -5.0:
                verdict = "overpriced"
                reasoning = (
                    f"Pakiet kosztuje WIĘCEJ niż kupowanie pojedynczych "
                    f"({per_session_grosze/100:.0f} zł per unit vs "
                    f"{single_price_g/100:.0f} zł single). Klient straci "
                    f"{-discount_pct:.1f}% kupując pakiet."
                )
            else:
                verdict = "fake_promo"
                reasoning = (
                    f"Brak realnego rabatu w pakiecie — różnica "
                    f"{discount_pct:.1f}% per unit. Klient płaci tę samą "
                    f"cenę co single, bez korzyści."
                )

        analyses.append({
            "package_service_id": svc.get("id"),
            "package_name": name,
            "package_price_grosze": price_grosze,
            "session_count": sessions,
            "area_count": areas,
            "single_service_id": single_id,
            "single_name": single_name,
            "single_price_grosze": single_price,
            "per_session_in_package_grosze": per_session_grosze,
            "discount_pct": discount_pct,
            "verdict": verdict,
            "reasoning": reasoning,
        })

    if analyses:
        await service.persist_competitor_report_package_analysis(
            report_id, analyses,
        )
        logger.info(
            "Faza 8b: analyzed %d subject packages (fair=%d, fake=%d, "
            "overpriced=%d, no_single=%d)",
            len(analyses),
            sum(1 for a in analyses if a["verdict"] == "fair_discount"),
            sum(1 for a in analyses if a["verdict"] == "fake_promo"),
            sum(1 for a in analyses if a["verdict"] == "overpriced"),
            sum(1 for a in analyses if a["verdict"] == "no_single_match"),
        )
    return analyses
