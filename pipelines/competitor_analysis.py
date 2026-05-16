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
    detect_package_keyword,
    should_drop_from_display,
    verify_pricing_comparison,
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
    try:
        total_overridden = await _apply_llm_taxonomy_to_null_tid_services(
            service, subject_data.get("services") or [], label="subject",
        )
        for _, cdata in aligned_competitors:
            total_overridden += await _apply_llm_taxonomy_to_null_tid_services(
                service, cdata.get("services") or [],
                label=f"competitor booksy_id={cdata.get('booksy_id')}",
            )
        logger.info(
            "Etap 4: LLM taxonomy inference applied to %d NULL-tid services "
            "(subject + %d competitors)",
            total_overridden, len(aligned_competitors),
        )
    except Exception as e:
        logger.warning(
            "LLM taxonomy inference for matrix expansion failed (%s) — "
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
    )
    n_pricing = await service.insert_competitor_pricing_comparisons(pricing_rows)
    logger.info("Etap 4: inserted %d pricing_comparisons", n_pricing)

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


def _active_services_with_variant(
    services: list[dict[str, Any]],
) -> dict[tuple[int, int], dict[str, Any]]:
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
    out: dict[tuple[int, int], dict[str, Any]] = {}
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
        key = (int(tid), int(vid))
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
    competitor_samples_by_variant: dict[
        tuple[int, int], list[dict[str, Any]]
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
            })

    # Pre-pass: figure out which subject service embeddings + which variant
    # centroids we need to fetch. Only rows with |deviation| > threshold
    # trigger verification, so we filter the fetch set accordingly.
    variant_ids_needing_verify: set[int] = set()
    subject_service_ids_needing_verify: set[int] = set()
    prelim: list[dict[str, Any]] = []
    for variant_key, subject_svc in subject_svcs.items():
        tid, variant_id = variant_key
        samples = competitor_samples_by_variant.get(variant_key, [])
        # User feedback: \"tutaj powinno być maksymalnie dużo nakładających
        # się + kreski jeśli nie ma\". Zostawiamy gate ≥ 1 (jest jakaś
        # baza porównawcza), ale akceptujemy single-sample rows — mediana
        # = ta jedna cena, deviation realny, UI pokazuje sample=1 jako
        # podpowiedź że to mała próba. Sample=0 (brak overlap'u) trafia
        # do osobnej sekcji \"Twoje unikalne USP\" niżej.
        if len(samples) < 1:
            continue
        subject_price = subject_svc.get("price_grosze")
        if subject_price is None:
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

        rows.append({
            "report_id": report_id,
            "booksy_treatment_id": tid,
            "variant_id": variant_id,
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
            "sample_size": len(samples),
            "recommended_action": recommended_action,
            "verification_status": verification_status,
            "verification_details": verification_details or None,
            "competitor_samples": samples,
        })

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


async def _apply_llm_taxonomy_to_null_tid_services(
    supabase: SupabaseService,
    services: list[dict[str, Any]],
    label: str = "salon",
    min_confidence: float = HIDDEN_MIN_CONFIDENCE,
) -> int:
    """Dla usług z NULL `booksy_treatment_id` (po Versum mapping) — uruchom
    LLM taxonomy inference (centroidy + OpenAI gpt-4o-mini) i jeśli
    confidence ≥ threshold, ustaw `booksy_treatment_id = inferred_tid`
    w-memory. Mutuje `services` in-place.

    Po tym kroku pricing pipeline (`_active_services_with_variant`,
    `_compute_pricing_comparisons`) widzi je jako "skategoryzowane" i
    dorzuca do matrixa. Oryginalne NULL tid jest zachowane w
    `booksy_treatment_id_original`, plus dodawany jest flag
    `taxonomy_inference_source = 'llm'` żeby UI mógł oznaczyć badgem.

    Returns: liczba usług których tid zostało nadpisane.
    """
    from services.hidden_service_inference import infer_hidden_services_batch

    # 1. Identyfikuj kandydatów: NULL tid + jest description (LLM go potrzebuje
    #    żeby wybrać top-30) + serwis aktywny.
    candidates: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in enumerate(services):
        if not svc.get("is_active", True):
            continue
        if svc.get("booksy_treatment_id") is not None:
            continue
        name = (svc.get("name") or "").strip()
        desc = (svc.get("description") or "").strip()
        if not name or len(name) < 3:
            continue
        if len(desc) < 30:
            # Bez opisu LLM ma za mało kontekstu — pomijamy (gracefully).
            continue
        sid = svc.get("id")
        if sid is None:
            continue
        candidates.append((idx, svc))

    if not candidates:
        logger.debug("LLM taxonomy inference [%s]: no NULL-tid candidates", label)
        return 0

    llm = _get_hidden_inference_llm()
    if llm is None:
        logger.info(
            "LLM taxonomy inference [%s]: %d candidates ale brak LLM key — skip",
            label, len(candidates),
        )
        return 0

    # 2. Batch-load embeddings.
    service_ids = [int(svc["id"]) for _, svc in candidates]
    try:
        emb_map = await supabase.get_service_embeddings(service_ids)
    except Exception as e:
        logger.warning(
            "LLM taxonomy inference [%s]: get_service_embeddings failed: %s",
            label, e,
        )
        return 0

    # 3. Build inference inputs (only those z embedding).
    inference_inputs: list[tuple[int, dict[str, Any]]] = []
    for idx, svc in candidates:
        emb = emb_map.get(int(svc["id"]))
        if not emb:
            continue
        inference_inputs.append((idx, {
            "name": svc.get("name") or "",
            "description": svc.get("description") or "",
            "name_embedding": emb,
        }))

    if not inference_inputs:
        return 0

    # 4. Run LLM inference w batch (semaphore=4).
    results = await infer_hidden_services_batch(
        [pair[1] for pair in inference_inputs],
        supabase,
        llm,
        min_confidence=min_confidence,
    )

    # 5. Apply tid override dla services z method='llm' lub 'embedding'
    #    AND confidence ≥ threshold. Plus: spróbuj inline variant matching
    #    używając zaczynalnie zwróconego embedding'u, żeby usługa weszła do
    #    `_active_services_with_variant` (hard gate Phase 5).
    overridden = 0
    overridden_with_variant = 0
    variant_match_tasks: list[tuple[int, dict[str, Any], list[float], int]] = []
    for (idx, inp), res in zip(inference_inputs, results):
        if res.get("method") not in ("llm", "embedding"):
            continue
        inferred_tid = res.get("inferred_tid")
        confidence = res.get("confidence") or 0.0
        if inferred_tid is None or confidence < min_confidence:
            continue
        svc = services[idx]
        # Zachowaj oryginalne wartości dla audytu.
        svc["booksy_treatment_id_original"] = svc.get("booksy_treatment_id")
        svc["treatment_name_original"] = svc.get("treatment_name")
        # Override tid + canonical name (po to żeby downstream pipeline
        # widział spójną etykietę zamiast NULL).
        svc["booksy_treatment_id"] = int(inferred_tid)
        if res.get("inferred_canonical_name"):
            svc["treatment_name"] = res["inferred_canonical_name"]
        # Audit/UI marker.
        svc["taxonomy_inference_source"] = res.get("method", "llm")
        svc["taxonomy_inference_confidence"] = confidence
        svc["taxonomy_inference_parent"] = res.get("inferred_parent_name")
        overridden += 1
        # Kolejka dla inline variant matching (Phase 5 gate).
        emb = inp.get("name_embedding")
        if emb:
            variant_match_tasks.append((idx, svc, emb, int(inferred_tid)))

    # 6. Inline variant matching dla services które dostały inferred tid.
    #    `_active_services_with_variant` wymaga variant_id NOT NULL — bez tego
    #    services są droppowane mimo override'u tid'a. Wywołujemy
    #    `match_service_to_variant` RPC (mig 058) per service z bounded
    #    concurrency.
    import asyncio as _asyncio
    sem_vm = _asyncio.Semaphore(8)

    async def _match_variant(idx: int, svc: dict[str, Any], emb: list[float], tid: int) -> None:
        nonlocal overridden_with_variant

        def _do_call() -> Any:
            return supabase.client.rpc(
                "match_service_to_variant",
                {
                    "p_embedding": emb,
                    "p_parent_treatment_id": tid,
                    "p_min_similarity": 0.55,
                },
            ).execute()

        try:
            async with sem_vm:
                res = await _asyncio.to_thread(_do_call)
        except Exception as e:
            logger.debug(
                "match_service_to_variant failed for service_id=%s tid=%s: %s",
                svc.get("id"), tid, e,
            )
            return
        rows = list(res.data or [])
        if not rows:
            return
        row = rows[0]
        vid = row.get("variant_id")
        if vid is None:
            return
        svc["variant_id"] = int(vid)
        # variant canonical name pomocny dla pricing comparison label'i
        if row.get("canonical_variant_name"):
            svc["variant_canonical_name"] = row["canonical_variant_name"]
        overridden_with_variant += 1

    if variant_match_tasks:
        await _asyncio.gather(*(
            _match_variant(i, s, e, t) for (i, s, e, t) in variant_match_tasks
        ))

    if overridden:
        logger.info(
            "LLM taxonomy inference [%s]: %d/%d NULL-tid services dostały "
            "inferred_tid (confidence ≥ %.2f), z których %d dostało też variant_id "
            "(similarity ≥ 0.55)",
            label, overridden, len(candidates), min_confidence,
            overridden_with_variant,
        )
    return overridden


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
