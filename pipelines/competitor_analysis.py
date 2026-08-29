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
  7. Persist etap4_stats + activePromotions + hiddenServices, status stays
     'processing'

AI synthesis (positioning narrative, SWOT, recommendations) is a separate
Etap 5 pipeline that reads the tables populated here. The terminal
'completed' write happens ONLY after that pipeline (synthesize_competitor_
insights) succeeds — see pipelines/competitor_report.py. BEAUTY_AUDIT-dqir:
this module used to flip status to 'completed' right here, before Etap 5
had even started, so a run that crashed during synthesis left a 'completed'
report with no narrative/SWOT/recommendations. If this function raises
below (or a caller's synthesis step raises), the row is left in
'processing' and the existing self-healing write in
workers/tasks.py::run_competitor_report_task (finally block,
fail_competitor_report_by_audit_id) flips it to 'failed'.

See docs/plans/2026-04-08-competitor-report-pipeline.md sections
"Pipeline steps (Comp Etap 2-5)" and "Dimensional scores — pełna lista".
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import statistics
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from pipelines.competitor_dimensional_scores import (
    DIMENSION_METADATA,
    compute_all_dimensions_for_salon,
    compute_percentiles,
    compute_subject_percentile,
)
from pipelines.competitor_selection import CompetitorCandidate, select_competitors
from services.brand_marker import extract_brand_marker
from services.pipeline_trace import TraceWriter
from services.pricing_verification import (
    compute_name_embedding_similarity,
    detect_package_keyword,
    flag_extreme_deviation,
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
from pipelines.competitor_buckets import (
    BUCKET_MIN_SIMILARITY,
    EMPTY_ASSIGNMENT,
    assign_coverage_buckets,
    coverage_by_salon,
)
from services.similarity_pricing.qdrant_search import search_twins
from services.similarity_pricing.report_pricing import (
    _fetch_subject_embeddings_with_chain_head_fallback,
    compute_pricing_comparisons_v2,
)
from services.supabase import SupabaseService
from services.taxonomy_inference import infer_and_apply

from config import settings

# Faza 8a: max bliźniaków per usługa subjecta w puli wybranych (≤ ~15 salonów,
# liczy się tylko 'czy salon ma odpowiednik', więc limit musi przewyższać liczbę
# salonów × warianty tej samej usługi).
_BUCKET_SEARCH_LIMIT = 600

logger = logging.getLogger(__name__)


ProgressCallback = Callable[[int, str], Awaitable[None]]


# Max równoległych promote-gate LLM verifications (verify_service_pairs).
# Po wyeliminowaniu N+1 RPC (quick 260613-cl2) ~83 sekwencyjnych gate'ów
# stało się wąskim gardłem. asyncio.gather + Semaphore(8) zrównolegla je,
# bezpiecznie vs OpenAI rate limit (verify_service_pairs jest cache'owane).
_PROMOTE_GATE_CONCURRENCY = 8


# Concurrency / RPC latency trace thresholds (quick 260613-m23, P3). Only
# emit a trace when a semaphore acquire or an RPC round-trip exceeds this, so
# the trace isn't flooded on the common fast path — mirrors market_context's
# _RPC_SLOW_MS. 1s headroom over normal acquire (<1ms when uncontended).
_CONCURRENCY_WAIT_SLOW_MS = 1000
_RPC_SLOW_MS = 1000


# Per-salon wall-clock cap for the taxonomy router. One hung LLM call inside
# _apply_llm_taxonomy_to_null_tid_services used to freeze the whole
# asyncio.gather (no timeout) and wedge the job at ~26-38% (quick 260613-kiu —
# prod hang >10min vs ~8.8s normal). 90s is generous headroom over the normal
# pass; env-overridable (the router block re-reads the env at call time so a
# test's monkeypatch.setenv takes effect without re-import).
ROUTER_PER_SALON_TIMEOUT_S = int(os.environ.get("ROUTER_PER_SALON_TIMEOUT_S", "90"))


async def _noop_progress(progress: int, message: str) -> None:
    pass


@asynccontextmanager
async def _phase_timer(tracer: "TraceWriter | None", phase: str):
    """Async context manager recording wall-clock for one pipeline phase.

    Writes a `phase.timer` trace row with `{phase, elapsed_ms}` plus a
    `logger.info("[phase=<name>] <ms>ms")` line so PM2 logs and
    `pipeline_traces` both surface per-phase breakdown.

    Mig 121 / Task 1 of 2026-05-24-pipeline-optimization plan — the 649s
    middle block in profile 2026-05-24-pipeline-profile.md had zero per-
    phase telemetry; this fixes that without touching pipeline semantics.

    Safe when tracer is None (e.g. unit tests) — still emits the log line.
    """
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.info("[phase=%s] %dms", phase, elapsed_ms)
        if tracer is not None:
            try:
                tracer.add(
                    "phase.timer",
                    {"phase": phase, "elapsed_ms": elapsed_ms},
                )
            except Exception:
                # Phase-timer observability MUST NOT crash the pipeline.
                # Log + swallow so the underlying work still ships.
                logger.exception(
                    "phase.timer trace add failed for phase=%s elapsed_ms=%d",
                    phase, elapsed_ms,
                )


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
    must_include_salon_ids: list[int] | None = None,
    job_id: str = "unknown",
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
        must_include_salon_ids: salon_ids the user picked in the frontend
            competitor picker. UNION'd into selection (never a filter) so the
            picks always end up in the candidate set. Also echoed into the
            report metadata as userSelectedSalonIds (durable before synthesis).

    Raises:
        RuntimeError: if no competitor candidates could be selected
        ValueError: if the subject audit_id is not found
    """
    progress = on_progress or _noop_progress
    service = supabase or SupabaseService()

    # ── Pipeline trace writer (mig 094) ──
    # Accumulates intermediate decisions for replay / customer-support /
    # regression detection. Flushed once at the end on the happy path —
    # if the pipeline crashes mid-run, the buffered traces are lost by
    # design (a partial trace would be misleading). Critical traces that
    # must survive a crash should be flushed earlier (see post-selection
    # flush below).
    tracer = TraceWriter(
        client=service.client,
        audit_id=audit_id,
        report_id=None,  # set after create_competitor_report
        pipeline="competitor_analysis",
        job_id=job_id,
    )

    # ── Step 1: Select competitors (Comp Etap 1) ──
    await progress(5, "Selekcja konkurentów...")
    # P2 (quick 260613-m23): selection had no _phase_timer — a black hole in
    # the profile. Wrap the select call (+ its validation) only; progress()
    # stays outside so ordering/semantics are unchanged.
    async with _phase_timer(tracer, "selection"):
        candidates = await select_competitors(
            audit_id,
            target_count=target_count,
            mode=selection_mode,
            supabase=service,
            tracer=tracer,
            must_include_salon_ids=must_include_salon_ids,
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
            # User-picked salon_ids that actually ended up in the candidate
            # set (is_user_selected flag set during the UNION in
            # select_competitors). Durable here even before synthesis runs so
            # the picks survive a synthesis crash. report_data carries the
            # canonical copy as userSelectedCompetitorIds (intersected with
            # the report's competitor set) — see competitor_synthesis.
            "userSelectedSalonIds": [
                c.salon_id for c in candidates if getattr(c, "is_user_selected", False)
            ],
        },
    )
    logger.info("Etap 4: created competitor_reports id=%s", report_id)

    # Now that the report row exists, backfill report_id on every buffered
    # selection trace + future traces. Flush immediately so selection traces
    # are durable even if downstream steps crash (without this, a 5-min
    # pipeline failure loses the entire selection-decision audit trail).
    for row in tracer._buffer:
        row["report_id"] = report_id
    tracer.report_id = report_id
    await tracer.flush()

    # Wipe any stale children from a prior re-run before inserting new rows
    await service.delete_competitor_report_children(report_id)

    n_matches = await service.insert_competitor_matches(report_id, candidates)
    logger.info("Etap 4: inserted %d competitor_matches", n_matches)

    # ── Step 3: Load subject + competitors full data ──
    await progress(25, "Ładowanie danych subject + konkurentów...")
    # P2 (quick 260613-m23): separate timers for the two heavy data loads.
    async with _phase_timer(tracer, "load.subject"):
        subject_data = await service.get_subject_full_data(audit_id)
    # Skąd przyszedł subject i co ma w środku — jedno spojrzenie zamiast
    # śledztwa po bazie (BEAUTY_AUDIT-cbnt). Liczby liczone z pamięci, z tego
    # co get_subject_full_data już przywiozło; zero dodatkowych zapytań.
    # Ta linia opisuje scrape AUDYTOWY — podmianę na chain-head (Etap 4,
    # "scrape-consistency") loguje osobna linia niżej.
    _log_subject_services_fingerprint(subject_data, source="audit scrape")

    competitor_booksy_ids = [c.booksy_id for c in candidates]
    # Keep only candidates whose data loaded successfully; align bucket metadata
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]] = []
    _dropped_booksy_ids: list[int] = []
    async with _phase_timer(tracer, "load.competitors"):
        competitor_data_map = await service.get_competitor_full_data(competitor_booksy_ids)
        for c in candidates:
            data = competitor_data_map.get(c.booksy_id)
            if data is None:
                logger.warning(
                    "Competitor booksy_id=%s missing scrape data — dropped from computations",
                    c.booksy_id,
                )
                _dropped_booksy_ids.append(c.booksy_id)
                continue
            aligned_competitors.append((c, data))

    # P0 silent-fail surfacing (quick 260613-m23): dropping a selected
    # candidate because its scrape never loaded is a silent shrink of the
    # comparison set (the metadata `competitors_dropped_no_data` aggregate
    # existed but never reached the trace). Surface candidate_count vs
    # dropped_count + the dropped ids so the operator can correlate a thin
    # report with missing scrapes. Behaviour unchanged — the drop already
    # happened above.
    if tracer is not None and _dropped_booksy_ids:
        tracer.add(
            "competitors.data_load",
            {
                "candidate_count": len(candidates),
                "dropped_count": len(_dropped_booksy_ids),
                "dropped_booksy_ids": _dropped_booksy_ids[:50],
            },
        )

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
    # P2 (quick 260613-m23): the Versum mapping fetch + apply had no timer.
    async with _phase_timer(tracer, "versum.mapping"):
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
        async with _phase_timer(tracer, "taxonomy.router"):
            # Snapshot LLM token counters so we can persist delta after the
            # full router pass (subject + all competitors). Router shares
            # one GeminiLLMClient via _get_hidden_inference_llm() singleton.
            _router_llm = _get_hidden_inference_llm()
            _pre_router_in = _router_llm.total_input_tokens if _router_llm else 0
            _pre_router_out = _router_llm.total_output_tokens if _router_llm else 0
            _pre_router_calls = _router_llm.total_calls if _router_llm else 0

            # Parallelize per-salon taxonomy routing (subject + 15 competitors).
            # Each salon's _resolve_service_taxonomy is independent:
            # - svc-dict mutations are local (different list per salon)
            # - DB writes are race-safe: Rule 2 via fn_upsert_synthetic_salon_defined
            #   RPC (DB unique index `WHERE source='salon_defined'`), Rule 4 via
            #   atomic increment_synthetic_merged_count, Rule 3 mutates dict only
            #   (no synthetic INSERT). Rule 1 (`upsert_synthetic_category_llm_generated`)
            #   has theoretical race for same-meaning-different-salon canonical names
            #   but rule_1 hits are rare (<7 across 16 salons typically) and the
            #   embedding-dedup catches semantic dupes — acceptable risk.
            # - LLM client is a singleton (_get_hidden_inference_llm) so token
            #   counters aggregate correctly.
            # Inner infer_hidden_services_batch creates its own Semaphore(15)
            # per-call. With outer Semaphore(8) max concurrent gpt-4o-mini calls
            # = 8 × 15 = 120; gpt-4o-mini Tier 2+ allows ~10K RPM so safe.
            import asyncio as _router_asyncio
            import os as _router_os
            _outer_concurrency = int(
                _router_os.environ.get("TAXONOMY_ROUTER_CONCURRENCY", "8")
            )
            _router_sem = _router_asyncio.Semaphore(_outer_concurrency)
            # Per-salon wall-clock cap (quick 260613-kiu). Read at call time
            # (not just module-import) so env override / tests apply without
            # re-import; ROUTER_PER_SALON_TIMEOUT_S module constant is the
            # documented default. One hung LLM call previously froze the whole
            # gather and wedged the job at ~26-38%.
            _per_salon_timeout = int(
                _router_os.environ.get("ROUTER_PER_SALON_TIMEOUT_S", "90")
            )

            async def _route_salon(
                services_list: list[dict[str, Any]], label: str,
            ) -> int:
                # P3 (quick 260613-m23): measure how long this salon waited to
                # acquire the router semaphore. Only traced when slow (>1s) so
                # contention is visible without flooding the trace. The acquire
                # semantics are unchanged — we just time the existing wait.
                _sem_t0 = time.monotonic()
                async with _router_sem:
                    _wait_ms = int((time.monotonic() - _sem_t0) * 1000)
                    if tracer is not None and _wait_ms > _CONCURRENCY_WAIT_SLOW_MS:
                        tracer.add(
                            "concurrency.semaphore_wait",
                            {
                                "gate": "taxonomy_router",
                                "wait_ms": _wait_ms,
                                "label": label,
                            },
                        )
                    return await _router_asyncio.wait_for(
                        _apply_llm_taxonomy_to_null_tid_services(
                            service, services_list, label=label,
                            audit_id=audit_id,
                        ),
                        timeout=_per_salon_timeout,
                    )

            _salon_tasks: list[Awaitable[int]] = [
                _route_salon(
                    subject_data.get("services") or [], label="subject",
                ),
            ]
            for _, cdata in aligned_competitors:
                _salon_tasks.append(
                    _route_salon(
                        cdata.get("services") or [],
                        label=f"competitor booksy_id={cdata.get('booksy_id')}",
                    )
                )
            # return_exceptions=True so one hung/erroring salon (its wait_for
            # raised TimeoutError, or _resolve_service_taxonomy raised) NEVER
            # propagates to the outer except (which re-raises and aborts the
            # whole etap). A skipped salon degrades to 0 routed — its null-tid
            # services simply stay un-routed, identical to the pre-router
            # state, so the pipeline continues safely (quick 260613-kiu).
            _salon_results = await _router_asyncio.gather(
                *_salon_tasks, return_exceptions=True,
            )
            # Build labels from the SAME source/order used for _salon_tasks
            # (subject first, then aligned_competitors) so a skipped salon is
            # logged with its correct label.
            _salon_labels = ["subject"] + [
                f"competitor booksy_id={c.get('booksy_id')}"
                for _, c in aligned_competitors
            ]
            _salon_counts: list[int] = []
            for _label, _res in zip(_salon_labels, _salon_results):
                if isinstance(_res, BaseException):
                    _outcome = (
                        "router_timeout"
                        if isinstance(_res, _router_asyncio.TimeoutError)
                        else "router_error"
                    )
                    logger.warning(
                        "Etap 4: taxonomy router skipped salon (%s): %s — "
                        "null-tid services for this salon stay un-routed "
                        "(safe degradation, pipeline continues)",
                        _label, _outcome,
                    )
                    if tracer is not None:
                        try:
                            tracer.add(
                                "taxonomy.router_skip",
                                {
                                    "label": _label,
                                    "outcome": _outcome,
                                    "error": repr(_res),
                                },
                            )
                        except Exception:
                            logger.exception(
                                "taxonomy.router_skip trace add failed for %s",
                                _label,
                            )
                    _salon_counts.append(0)
                else:
                    _salon_counts.append(_res)
            total_overridden = sum(_salon_counts)
            logger.info(
                "Etap 4: taxonomy routing applied to %d NULL-tid services "
                "(subject + %d competitors, outer_concurrency=%d)",
                total_overridden, len(aligned_competitors), _outer_concurrency,
            )

            if tracer is not None and _router_llm is not None:
                try:
                    delta_in = _router_llm.total_input_tokens - _pre_router_in
                    delta_out = _router_llm.total_output_tokens - _pre_router_out
                    delta_calls = _router_llm.total_calls - _pre_router_calls
                    tracer.add(
                        "agent.tokens",
                        {
                            "step_name": "taxonomy.router",
                            "calls": delta_calls,
                            "model": _router_llm.model,
                            "provider": _router_llm.provider,
                            "input_tokens": delta_in,
                            "output_tokens": delta_out,
                            "services_overridden": total_overridden,
                            "salon_count": 1 + len(aligned_competitors),
                        },
                        tokens_used={
                            "input": delta_in,
                            "output": delta_out,
                            "model": _router_llm.model,
                        },
                    )
                except Exception:
                    logger.exception("agent.tokens trace add failed for taxonomy.router")

        # Stage-5 Pass 5 (2026-05-17): MiniMax M2.7 cross-salon
        # consistency. Groups all services (subject + competitors) by
        # (brand_marker, body_areas) and forces every mixed cluster
        # under ONE authoritative tid_key. Closes the "Thunder Całe
        # ciało 1 zabieg → btid=637, 5+1 zabieg → salon-synthetic"
        # inter-variant inconsistency that the area gate alone cannot
        # resolve (LLM non-determinism between variants of the same
        # treatment).
        async with _phase_timer(tracer, "taxonomy.pass5_consistency"):
            from services.taxonomy_consistency import apply_intra_salon_consistency
            from services.minimax import MiniMaxClient
            if not settings.minimax_api_key:
                raise RuntimeError(
                    "Etap 4 Pass 5: MINIMAX_API_KEY missing — consistency "
                    "layer requires MiniMax M2.7. Do not run pipeline "
                    "without it (silently disabling would defeat the "
                    "user-requested architecture)."
                )
            minimax_client = MiniMaxClient(
                settings.minimax_api_key,
                settings.minimax_base_url,
                settings.minimax_model,
            )
            all_services_cross_salon: list[dict[str, Any]] = list(
                subject_data.get("services") or []
            )
            for _, cdata in aligned_competitors:
                all_services_cross_salon.extend(cdata.get("services") or [])
            consistency_stats = await apply_intra_salon_consistency(
                all_services_cross_salon,
                supabase=service,
                minimax=minimax_client,
                audit_id=audit_id,
                label=f"cross-salon (subject+{len(aligned_competitors)}comp)",
                trace_collector=None,  # consistency-layer trace not surfaced in dev modal yet
                dry_run=False,
                tracer=tracer,
            )
            logger.info(
                "Etap 4 Pass 5: MiniMax consistency — %d clusters total, "
                "%d mixed, %d services rerouted",
                consistency_stats["clusters_total"],
                consistency_stats["clusters_mixed"],
                consistency_stats["rerouted"],
            )
    except Exception:
        # Taxonomy routing is the foundation for every downstream pricing/
        # competitor comparison — silently continuing with raw tids would
        # produce a corrupt report. Surface the failure to Bugsink and
        # abort the etap.
        logger.exception(
            "Taxonomy routing for matrix expansion FAILED — aborting etap "
            "(continuing with raw tids would produce a corrupt report)"
        )
        raise

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
    except Exception:
        # Migration 042 is required in production — if the RPC is missing
        # in dev, fix the migration, don't silently degrade. Surface to
        # Bugsink and abort so reports never ship with raw Booksy tids
        # (which include the salon-owner-picked mistags Phase 8a/8b is
        # supposed to clean up).
        logger.exception(
            "Taxonomy inference (crowd lookup via fn_infer_treatment_id) "
            "FAILED — aborting etap. If RPC is missing, apply migration "
            "042; do not bypass."
        )
        raise

    # ── Scrape-consistency (2026-06-15): use the subject's CHAIN-HEAD scrape
    # services for the PRICING tiers. The audit-triggered scrape loaded above
    # (get_subject_full_data, keyed by convex_audit_id) goes STALE once
    # discovery re-scrapes the salon — a newer scrape becomes the chain head,
    # and the audit scrape's service ids have no classification on it. Every
    # dictionary tier RPC (fn_subject_methods, fn_pricing_samples_structured,
    # fn_compute_method_pricing) keys off the chain head, so with a stale audit
    # the structured/method tiers collapse to subject_only. Swap in the chain
    # head services for pricing; scoring above keeps the audit-time snapshot.
    # No-op when the audit scrape already IS the chain head (fresh audit).
    try:
        _audit_scrape_id = (subject_data.get("scrape") or {}).get("id")
        _ch_id, _ch_services = await service.get_chain_head_services(
            int(subject_data["booksy_id"])
        )
        if _ch_id and _ch_id != _audit_scrape_id and _ch_services:
            # Re-anchor chain-head tids via crowd lookup (same treatment the
            # audit services got). Classification tiers don't need it; the
            # variant/treatment tiers group by tid so it keeps them clean.
            try:
                await infer_and_apply(
                    service, _ch_services, label="subject_chain_head",
                )
            except Exception:
                logger.exception(
                    "scrape-consistency: infer_and_apply on chain-head failed "
                    "— pricing continues with raw tids"
                )
            logger.info(
                "Etap 4 scrape-consistency: audit scrape %s stale → using "
                "chain-head %s for pricing (%d → %d services)",
                _audit_scrape_id, _ch_id,
                len(subject_data.get("services") or []), len(_ch_services),
            )
            if tracer is not None:
                tracer.add("pricing.subject_scrape_swap", {
                    "audit_scrape_id": str(_audit_scrape_id),
                    "chain_head_scrape_id": str(_ch_id),
                    "audit_services": len(subject_data.get("services") or []),
                    "chain_head_services": len(_ch_services),
                })
            subject_data["services"] = _ch_services
    except Exception as _sce:
        logger.warning(
            "Etap 4 scrape-consistency check failed (%s) — keeping audit "
            "scrape services for pricing", _sce,
        )

    # ── Step 4: Pricing comparisons ──
    # T3b 2026-05-19 — single-instance MethodClassifier shared across
    # both tier-1 (treatment) and tier-2 (variant) paths. Warmup loads
    # 689 canonical methods + 1808 alias entries to in-memory index;
    # one round-trip to DB per pipeline instead of per service.
    method_classifier = None
    try:
        from services.method_classifier import MethodClassifier as _MC
        method_classifier = _MC(
            supabase=service,
            llm_client=_get_hidden_inference_llm(),
        )
        await method_classifier.warmup()
        logger.info(
            "Etap 4: MethodClassifier ready (%d methods, %d aliases)",
            len(method_classifier._methods),
            len(method_classifier._alias_index),
        )
    except Exception as _mc_e:
        logger.warning(
            "Etap 4: MethodClassifier warmup failed (%s) — pricing pipeline "
            "will fall back to extract_brand_marker regex on all method gates",
            _mc_e,
        )
        method_classifier = None

    # Dowód wykonania etapu wyceny w TYM przebiegu (BEAUTY_AUDIT-xng).
    # None = etap się nie domknął i nic nie zapisał; int = tyle wierszy zapisał
    # ten run. Tę liczbę czyta bramka przed Step 8. Celowo trzymamy ją w
    # pamięci procesu zamiast pytać competitor_pricing_comparisons — tam mogą
    # leżeć wiersze z POPRZEDNIEGO przebiegu (raport 181: nagłówek 'completed'
    # nad 221 wierszami sprzed doby), więc odczyt z tabeli nie odróżnia
    # "policzone teraz" od "zostało po starym runie".
    n_pricing: int | None = None

    await progress(45, "Pricing comparisons per treatment_id...")
    async with _phase_timer(tracer, "pricing.comparisons"):
        # Early-exit (quick 260613-rne; warunek przepisany 2026-08-23,
        # BEAUTY_AUDIT-cbnt): jeżeli silnik cenowy i tak nie miałby czego
        # policzyć, nie ma po co go budzić — emitujemy wiersze subject_only
        # (jeden per grupa _tid_key, kształt tier-1) i pomijamy ciężkie tiery.
        # Reszta pipeline'u (insert/aggregate/gaps/dims/synthesis) pracuje na
        # pricing_rows niezależnie od tego, która gałąź je wyprodukowała.
        #
        # Dlaczego stary warunek był martwy: pytał `_active_services_with_
        # variant`, czyli o `variant_id`. To wymóg STAREJ, klasyfikacja-first
        # `_compute_pricing_comparisons` (grupuje po (tid, variant_id) i ma
        # własny guard wariantowy w środku — jej ta zmiana nie dotyczy, bo
        # pipeline jej nie woła). Od S0078 w gałęzi `else` stoi
        # `compute_pricing_comparisons_v2`, która słowa `variant_id` nie zna:
        # filtruje subjecta wyłącznie po is_active + price_grosze + id
        # (services/similarity_pricing/report_pricing.py:201-204). Skutek na
        # produkcji (raport 250, subject booksy 98814): 229 usług, 220
        # aktywnych z ceną, ale tylko 25 z wariantem — a że promocje, pakiety
        # i konsultacje bez ceny wypadają w filtrze wariantowym, guard schodził
        # do zera i odcinał 220 dobrych usług od silnika, któremu warianty są
        # niepotrzebne. Klientka dostawała 24 puste wiersze "tylko Ty na
        # rynku" zamiast 203 z cenami rynkowymi.
        #
        # Nowy warunek pyta dokładnie o to, czego wymaga v2 — patrz
        # `_v2_eligible_subject_services`.
        _subject_services = subject_data.get("services") or []
        _subject_qualified = _v2_eligible_subject_services(_subject_services)
        if not _subject_qualified:
            pricing_rows = _emit_subject_only_rows_no_variants(
                report_id, _subject_services,
            )
            pricing_rows = _dedup_pricing_rows(pricing_rows)
            n_pricing = await service.insert_competitor_pricing_comparisons(
                pricing_rows
            )
            logger.info(
                "Etap 4: early-exit subject_only — %d usług, 0 aktywnych z "
                "ceną i id (silnik v2 nie miałby czego liczyć), %d "
                "subject_only rows; tiery variant/treatment/structured/"
                "method/sub_variant pominięte",
                len(_subject_services), n_pricing,
            )
            if tracer is not None:
                tracer.add("pricing.computed", {
                    "pricing_type": "subject_only_early_exit",
                    "row_count": n_pricing,
                    "skip_reason": "subject_has_no_priced_services",
                    "total_services": len(_subject_services),
                    "services_eligible_for_v2": 0,
                    "tiers_skipped": [
                        "variant", "treatment", "structured",
                        "method", "sub_variant",
                    ],
                })
        else:
            # S0078 (2026-06-22) — wpięty silnik similarity-first + test tożsamości
            # (services/similarity_pricing/). Zastępuje stary klasyfikacja-first
            # _compute_pricing_comparisons (tiery variant/method/structured). Stara
            # funkcja pozostaje w pliku (referencja/rollback). Patrz README modułu.
            pricing_rows = await compute_pricing_comparisons_v2(
                service, report_id, subject_data, aligned_competitors,
                audit_id=audit_id,
                tracer=tracer,
                method_classifier=method_classifier,
                llm_client=_get_hidden_inference_llm(),
            )
            pricing_rows = _dedup_pricing_rows(pricing_rows)
            # Flag extreme deviations (2026-06-22): treatment/structured/method/
            # sub_variant tiers don't run the per-variant verify, so dotąd
            # hard-codowały verification_status='verified' — nawet przy +703%
            # (pakiet vs single). Jeden pass podnosi 'verified' → 'extreme_outlier'
            # gdy |deviation| > EXTREME_DEVIATION_PCT, więc UI sygnalizuje
            # "wymaga weryfikacji" zamiast prezentować jako pewne.
            for _r in pricing_rows:
                _r["verification_status"] = flag_extreme_deviation(
                    _r.get("verification_status", "verified"),
                    _r.get("deviation_pct"),
                )
            n_pricing = await service.insert_competitor_pricing_comparisons(
                pricing_rows
            )
            logger.info("Etap 4: inserted %d pricing_comparisons", n_pricing)

    # Flush all method classifications buffered during tier-1/tier-2 runs.
    if method_classifier is not None:
        try:
            _n_classified = await method_classifier.flush_cache_writes()
            logger.info(
                "Etap 4: MethodClassifier flushed %d cache rows "
                "(in-process cache=%d)",
                _n_classified, len(method_classifier._inprocess_cache),
            )
        except Exception as _fe:
            logger.warning("Etap 4: classifier cache flush failed: %s", _fe)

    # ── Step 4.5 (Faza 8a 2026-05-17, przebudowa 2026-08-23 xi18): re-bucket
    # wg POKRYCIA menu subjecta przez każdego wybranego konkurenta (osobne,
    # dokładne wyszukiwanie w puli wybranych). Zastępuje sygnał
    # composite_score_v2 ("wyglądają podobnie") dowodem ("oferują te same
    # usługi"). Słabo pokrywający → 'aspirational'/'excluded'. ──
    await progress(56, "Re-bucketing konkurentów wg pokrycia usług...")
    await _aggregate_verified_match_counts(
        service, report_id, subject_data, aligned_competitors,
    )

    # ── Step 5: Service gaps ──
    await progress(60, "Service gap analysis (missing + unique USP)...")
    async with _phase_timer(tracer, "gaps.compute"):
        gap_rows = await _compute_service_gaps(
            service, report_id, subject_data, aligned_competitors,
            tracer=tracer,
        )
        n_gaps = await service.insert_competitor_service_gaps(gap_rows)
        logger.info("Etap 4: inserted %d service_gaps", n_gaps)

    # ── Step 6: Dimensional scores ──
    await progress(70, "Dimensional scores (28 wymiarów)...")
    async with _phase_timer(tracer, "scoring.dimensional"):
        dim_rows = _compute_dimensional_scores(
            report_id, subject_data, aligned_competitors,
            tracer=tracer,
        )
        n_dims = await service.insert_competitor_dimensional_scores(dim_rows)
        logger.info("Etap 4: inserted %d dimensional_scores", n_dims)

        # Feed the shared deterministic benchmark pool (salon_dimensional_scores,
        # mig 123): every competitor we just scored on the 28-dim axis becomes a
        # data point for fn_market_position. Best-effort + additive — the upsert
        # never raises and this loop must not break the competitor report.
        # Source booksy_id/city/category from the CompetitorCandidate (cand) which
        # carries them directly; scraped_at from cdata's scrape. We include ALL
        # aligned competitors with a booksy_id (not just counts_in_aggregates),
        # since the pool is meant to be a universal market sample.
        _pool_fed = 0
        for cand, cdata in aligned_competitors:
            comp_booksy_id = getattr(cand, "booksy_id", None)
            if not comp_booksy_id:
                continue
            try:
                comp_dims = compute_all_dimensions_for_salon(cdata)
                if not comp_dims:
                    continue
                await service.upsert_salon_dimensional_score(
                    comp_booksy_id, comp_dims,
                    salon_ref_id=getattr(cand, "salon_id", None),
                    city=getattr(cand, "city", None),
                    primary_category_id=getattr(cand, "primary_category_id", None),
                    source="competitor",
                    scraped_at=(cdata.get("scrape") or {}).get("scraped_at"),
                )
                _pool_fed += 1
            except Exception as _pe:  # noqa: BLE001
                logger.warning(
                    "Pool upsert for competitor booksy_id=%s failed: %s",
                    comp_booksy_id, _pe,
                )
        logger.info(
            "Etap 4: fed %d competitor vectors into shared dimensional pool", _pool_fed,
        )

    # ── Step 6.7 (Faza 8b 2026-05-17): package economics analysis. For
    # every subject service that's a package or area-bundle, find the
    # single-session same-area equivalent at the SAME salon and compute
    # discount %. Surfaces as "Uczciwość pakietów" section in the rich
    # UI — flags fake-promo packages where Beauty4ever charges identical
    # per-session price to singles, plus genuine discounts and the
    # rare overpriced bundle. ──
    await progress(78, "Analiza uczciwości pakietów i bundle'i...")
    async with _phase_timer(tracer, "packages.analyze"):
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
    async with _phase_timer(tracer, "hidden_services.enrich"):
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
                    hidden_services, service, tracer=tracer,
                )
                llm_count = sum(
                    1 for h in hidden_services if h.get("inference_method") == "llm"
                )
                logger.info(
                    "Etap 4: LLM inference applied to %d/%d hidden services",
                    llm_count, len(hidden_services),
                )
            except Exception:
                # Hidden-services LLM enrichment is the path that adds
                # inference_method/confidence/inferred_tid fields downstream
                # rendering relies on. Silent keyword fallback corrupts what
                # the user sees in the "Ukryte usługi" section. Bugsink alert.
                logger.exception(
                    "Etap 4: hidden services LLM inference FAILED — aborting "
                    "etap (silent keyword fallback would mislabel inference_method)"
                )
                raise

    # ── Step 7: Extract active promotions ──
    await progress(85, "Ekstrakcja aktywnych promocji...")
    async with _phase_timer(tracer, "promotions.fetch"):
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

    # ── Bramka przed Step 8 (BEAUTY_AUDIT-xng) ──
    # 'completed' wolno postawić TYLKO wtedy, gdy etap wyceny domknął się w tym
    # przebiegu i coś zapisał. Bez tej bramki run, który nic nie policzył,
    # stawiał nagłówek "gotowe" nad zawartością, której sam nie wyprodukował —
    # a `delete_competitor_report_children` połyka błędy kasowania per tabela
    # (services/supabase.py:1330-1341), więc pod tym nagłówkiem mogą wciąż stać
    # wiersze z poprzedniego przebiegu. Klientka widzi raport oznaczony jako
    # gotowy, wypełniony nieaktualnymi cenami.
    if not n_pricing:
        _abort_reason = (
            "pricing stage persisted no rows in this run "
            f"(rows={0 if n_pricing is None else n_pricing}, "
            f"stage_completed={n_pricing is not None}) — refusing to mark "
            "the report completed"
        )
        logger.error("Etap 4: %s (report_id=%s)", _abort_reason, report_id)
        await service.update_competitor_report_status(
            report_id,
            "failed",
            metadata_extras={
                "error": _abort_reason,
                "aborted_stage": "pricing.comparisons",
                "pricing_rows_this_run": 0 if n_pricing is None else n_pricing,
            },
        )
        raise RuntimeError(_abort_reason)

    # ── Step 8: Persist etap4_stats + report_data, status stays 'processing' ──
    # BEAUTY_AUDIT-dqir: this used to write status='completed' right here,
    # before Etap 5 (AI synthesis — positioning narrative, SWOT,
    # recommendations, pipelines/competitor_synthesis.py) had even started.
    # A run that crashed or was killed during synthesis left the row
    # permanently 'completed' with an empty report_data — the client saw a
    # "ready" report missing its narrative. The terminal 'completed' write
    # now happens in pipelines/competitor_report.py, ONLY after
    # synthesize_competitor_insights returns successfully. Passing
    # status="processing" here is a no-op on the status column (the row is
    # already 'processing' since create_competitor_report) — the call exists
    # to persist etap4_stats/activePromotions/hiddenServices without
    # prematurely flipping the terminal flag. If synthesis raises, the row
    # is left 'processing' and workers/tasks.py's existing self-healing
    # write (fail_competitor_report_by_audit_id, guarded on
    # status='processing') flips it to 'failed'.
    await progress(95, "Finalizacja etapu deterministycznego...")
    await service.update_competitor_report_status(
        report_id,
        "processing",
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

    # Final flush — captures any traces added during pricing / scoring /
    # market_context phases past the initial selection flush. NO graceful
    # fail: if this flush errors, the pipeline crashes loudly so the
    # operator sees the missing observability.
    await tracer.flush()

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


def _is_package_service(svc: dict[str, Any], *, name_key: str = "name") -> bool:
    """Czy usługa to pakiet / multi-pack / seria.

    Flaga is_package (mig 136, ustawiana przy ingest) jest źródłem prawdy;
    detect_package_keyword zostaje jako fallback dla wierszy bez flagi
    (sprzed backfillu / brak kolumny w SELECT). Defense-in-depth.
    """
    if svc.get("is_package"):
        return True
    return detect_package_keyword(svc.get(name_key) or "") is not None


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


# S0064 (2026-06-12) — duration buckets mapped to ordinal indexes for the
# promote scope gate. Adjacent buckets (distance 1) still compare —
# 30 vs 45 min is a legit market spread; short (15) vs long (120) is not.
_DURATION_BUCKET_INDEX: dict[str, int] = {"short": 0, "medium": 1, "long": 2}


async def _apply_promote_scope_gates(
    strong: list[dict[str, Any]],
    *,
    subject_name: str,
    subject_duration_minutes: Any,
    subj_brand_markers: set[str] | None,
    booksy_treatment_id: int | None,
    synthetic_treatment_id: int | None,
    supabase: "SupabaseService | None",
    llm_client: Any,
    audit_id: str | None,
    min_count: int,
    min_unique_salons: int,
    tracer: "TraceWriter | None" = None,
) -> dict[str, Any]:
    """Scope gates for the semantic promote path (S0064 audit fixes).

    Pure embedding cosine accepts scope-mismatched samples (Botoks 1 vs 3
    okolice 0.878, depilacja damska vs męska 0.888, manicure vs pedicure
    0.721 — all above the 0.65 promote threshold). Before promoting
    related_samples to direct comp samples, filter by:

      1. brand marker (optional — tier-1 runs its own classifier-aware
         method gate first, so it passes ``subj_brand_markers=None``);
      2. duration bucket distance (>1 bucket apart = different scope;
         pairs where either side lacks duration>0 pass through);
      3. batch LLM pair verification via ``verify_service_pairs`` —
         FAIL-CLOSED: missing llm_client or any verify exception means
         NO promotion (the caller keeps related_samples soft).

    The LLM round-trip is skipped when the post-prefilter sample set
    already fails the >=min_count / >=min_unique_salons gate (the caller
    will demote anyway).

    Returns a dict:
      strong — filtered sample list ([] on verify error → fail-closed),
      brand_dropped / duration_dropped / llm_dropped — drop counters,
      verify_error — str | None (set ⇒ outcome should be
        ``promote_verify_error``).
    """
    info: dict[str, Any] = {
        "brand_dropped": 0,
        "duration_dropped": 0,
        "llm_dropped": 0,
        "verify_error": None,
    }

    # (1) Brand/method gate — same semantics as the tier-1 promote gate:
    # when the subject carries a recognizable marker, samples must carry
    # an intersecting marker; markerless samples drop too.
    if subj_brand_markers:
        kept: list[dict[str, Any]] = []
        for s in strong:
            marker = extract_brand_marker(s.get("service_name") or "")
            if marker and marker in subj_brand_markers:
                kept.append(s)
        info["brand_dropped"] = len(strong) - len(kept)
        strong = kept

    # (2) Duration-bucket prefilter.
    try:
        subj_dur = int(subject_duration_minutes or 0)
    except (TypeError, ValueError):
        subj_dur = 0
    if subj_dur > 0:
        subj_idx = _DURATION_BUCKET_INDEX.get(_duration_bucket(subj_dur))
        if subj_idx is not None:
            kept = []
            for s in strong:
                raw_dur = s.get("duration_minutes")
                try:
                    s_dur = int(raw_dur or 0)
                except (TypeError, ValueError):
                    s_dur = 0
                if s_dur <= 0:
                    # Unknown duration — don't filter the pair.
                    kept.append(s)
                    continue
                s_idx = _DURATION_BUCKET_INDEX.get(_duration_bucket(s_dur))
                if s_idx is None or abs(subj_idx - s_idx) <= 1:
                    kept.append(s)
                else:
                    info["duration_dropped"] += 1
            strong = kept

    # Skip the LLM round-trip when promotion is already impossible.
    unique_salons = {
        s.get("salon_id") for s in strong if s.get("salon_id") is not None
    }
    if len(strong) < min_count or len(unique_salons) < min_unique_salons:
        info["strong"] = strong
        return info

    # (3) Batch LLM pair verification — FAIL-CLOSED.
    if llm_client is None or supabase is None:
        info["verify_error"] = (
            "llm_client unavailable — fail-closed, no promotion"
        )
        strong = []
    else:
        try:
            verified_map = await verify_service_pairs(
                subject_service_name=subject_name,
                candidate_competitor_names=[
                    (s.get("service_name") or "") for s in strong
                ],
                booksy_treatment_id=booksy_treatment_id,
                synthetic_treatment_id=synthetic_treatment_id,
                supabase=supabase,
                llm_client=llm_client,
                audit_id=audit_id,
                tracer=tracer,
            )
        except Exception as e:
            info["verify_error"] = str(e)[:500]
            strong = []
        else:
            kept = []
            for s in strong:
                norm = _normalize_pair_name(s.get("service_name") or "")
                verdict = verified_map.get(norm, {})
                s["llm_verified"] = bool(verdict.get("is_comparable", True))
                s["llm_reasoning"] = verdict.get("reasoning")
                s["llm_rejection_reason"] = verdict.get("rejection_reason")
                if s["llm_verified"]:
                    kept.append(s)
                else:
                    info["llm_dropped"] += 1
            strong = kept

    info["strong"] = strong
    return info


def _log_subject_services_fingerprint(
    subject_data: dict[str, Any], *, source: str,
) -> None:
    """Jedna linia INFO opisująca usługi subjecta: z którego scrape'u przyszły
    i co realnie mają w środku.

    Po co: gdy raport konkurencji wychodzi cienki, pierwsze pytanie brzmi
    „subject wszedł z chain-heada czy ze scrape'u audytowego, i czy jego usługi
    w ogóle nadają się do liczenia cen". Bez tej linii odpowiedź wymagała
    zapytań do bazy po fakcie. Liczymy WYŁĄCZNIE z tego, co już jest w pamięci
    — żadnego dodatkowego ruchu do Supabase.

    Uczciwość zamiast zgadywania: pełnego wektora `name_embedding` nie ma w
    tych wierszach (SupabaseService._load_services_for_scrape świadomie go nie
    ciągnie po drucie), sygnałem obecności embeddingu jest `embedding_applied_
    at`. Gdy kolumny nie ma w danych, logujemy „n/d", a nie zero.
    """
    services = subject_data.get("services") or []
    scrape_id = (subject_data.get("scrape") or {}).get("id") or "n/d"

    def _count(field: str) -> Any:
        if not any(field in svc for svc in services):
            return "n/d"
        return sum(1 for svc in services if svc.get(field) is not None)

    active_priced = sum(
        1 for svc in services
        if svc.get("is_active", True)
        and isinstance(svc.get("price_grosze"), (int, float))
        and svc["price_grosze"] > 0
    )
    logger.info(
        "Etap 4 subject (%s): scrape=%s, usług=%d, z embeddingiem=%s, "
        "z variant_id=%s, aktywnych z ceną=%d, kwalifikuje do silnika v2=%d",
        source, scrape_id, len(services),
        _count("embedding_applied_at"), _count("variant_id"),
        active_priced, len(_v2_eligible_subject_services(services)),
    )


def _v2_eligible_subject_services(
    services: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Usługi subjecta, na których `compute_pricing_comparisons_v2` naprawdę
    ma na czym pracować — kopia JEJ filtru wejściowego, nie żaden nasz wariant.

    Oryginał: services/similarity_pricing/report_pricing.py:201-204
        [s for s in subject_data["services"]
         if s.get("is_active", True) and s.get("price_grosze")
         and s.get("id") is not None]
    Gdy ta lista jest pusta, v2 kończy natychmiast `return []` — i tylko o to
    pyta bramka early-exit w Etapie 4.

    Jedyny świadomy rozjazd z oryginałem: cenę sprawdzamy liczbowo (`> 0`),
    a nie prawdziwościowo, więc ujemna cena odpada u nas, a u v2 by przeszła.
    Kierunek jest bezpieczny — cokolwiek przepuścimy, v2 też przepuści.
    Przypięte w tests/test_competitor_pricing_gate.py.

    UWAGA: to NIE jest `_active_services_with_variant` (niżej). Tamta jest
    filtrem starego, klasyfikacja-first `_compute_pricing_comparisons` i pyta
    dodatkowo o `variant_id` — bo tamten silnik grupuje po (tid, variant_id).
    v2 słowa `variant_id` w ogóle nie zna.
    """
    out: list[dict[str, Any]] = []
    for svc in services:
        if not svc.get("is_active", True):
            continue
        price = svc.get("price_grosze")
        if not isinstance(price, (int, float)) or price <= 0:
            continue
        if svc.get("id") is None:
            continue
        out.append(svc)
    return out


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
        if _is_package_service(svc) or _has_promo_marker(name) or svc.get("is_promo"):
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


async def _compute_method_targeted_pricing(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    *,
    audit_id: str | None = None,
    tracer: TraceWriter | None = None,
    radius_km: float = 16.0,
    min_sample_size: int = 3,
    min_unique_salons: int = 2,
) -> list[dict[str, Any]]:
    """Method-targeted pricing comparison (tier='method').

    For every canonical method that the subject's classified services
    map to (via service_method_classification, mig 095 + backfill),
    compute market pricing percentiles across ALL salons within
    radius_km of subject offering that method — regardless of whether
    those salons were selected as profile-overlap competitors.

    Implementation:
      1. fn_subject_methods(subject_salon_id) — list classified methods
         in subject's cennik
      2. Per method: fn_compute_method_pricing(subject, method, radius)
      3. Emit comparison_tier='method' row with treatment_method_id +
         per-method sample distribution + recommended_action computed
         from subject's median price vs market median

    Gate: skip methods with sample_size < min_sample_size (3) or
    unique_salons < min_unique_salons (2) — too few data points for a
    credible median (one salon's pricing strategy shouldn't dominate).

    FAIL-LOUD: RPC errors propagate to caller. Per-method failures are
    not allowed to silently drop the method from the report.
    """
    subject_salon_id = subject_data.get("salon_id")
    if not subject_salon_id:
        logger.warning(
            "Etap 4 tier-4: subject_data missing salon_id — skipping "
            "method-targeted pricing (audit_id=%s)",
            audit_id,
        )
        if tracer is not None:
            tracer.add(
                "pricing.computed",
                {
                    "pricing_type": "method",
                    "row_count": 0,
                    "skip_reason": "no_subject_salon_id",
                },
            )
        return []

    # 1. List subject's classified methods. RPC returns each canonical
    # method once, with services_count showing how many subject services
    # map to it. Sorted services_count DESC inside the RPC.
    # P3 (quick 260613-m23): time the RPC; trace only when slow (>1s).
    _sm_t0 = time.monotonic()
    methods_res = service.client.rpc(
        "fn_subject_methods",
        {"p_subject_salon_id": int(subject_salon_id)},
    ).execute()
    _sm_dur_ms = int((time.monotonic() - _sm_t0) * 1000)
    if _sm_dur_ms > _RPC_SLOW_MS:
        logger.warning(
            "Etap 4 tier-4: fn_subject_methods slow (salon=%s): %dms",
            subject_salon_id, _sm_dur_ms,
        )
        if tracer is not None:
            tracer.add(
                "rpc.latency",
                {"rpc": "fn_subject_methods", "duration_ms": _sm_dur_ms},
            )
    subject_methods = methods_res.data or []
    if not subject_methods:
        logger.info(
            "Etap 4 tier-4: subject_salon_id=%s has no classified methods "
            "— skipping method-targeted pricing (backfill may be pending)",
            subject_salon_id,
        )
        if tracer is not None:
            tracer.add(
                "pricing.computed",
                {
                    "pricing_type": "method",
                    "row_count": 0,
                    "skip_reason": "no_classified_methods",
                },
            )
        return []

    logger.info(
        "Etap 4 tier-4: subject %s has %d classified methods",
        subject_salon_id, len(subject_methods),
    )

    # 2. Build subject service index per method_id from chain head.
    # Pipeline already loaded subject_data["services"] but classifier
    # cache lives in service_method_classification. Query joint lookup.
    subj_svcs_by_method = await _load_subject_services_by_method(
        service, int(subject_salon_id)
    )

    rows: list[dict[str, Any]] = []
    promoted = 0
    dropped_low_sample = 0
    for m in subject_methods:
        method_id = int(m["method_id"])
        canonical = m["canonical_name"]
        display = m["display_name"]
        category = m["category"]
        method_type = m["method_type"]
        subject_services_for_method = subj_svcs_by_method.get(method_id, [])

        # Call market pricing RPC
        # P3 (quick 260613-m23): time per-method RPC; carry duration into the
        # existing per-method trace below + warn when slow (>1s).
        _mp_t0 = time.monotonic()
        pricing_res = service.client.rpc(
            "fn_compute_method_pricing",
            {
                "p_subject_salon_id": int(subject_salon_id),
                "p_method_id": method_id,
                "p_radius_km": radius_km,
                "p_duration_min": None,
                "p_duration_max": None,
                "p_sample_limit": 30,
            },
        ).execute()
        _mp_dur_ms = int((time.monotonic() - _mp_t0) * 1000)
        if _mp_dur_ms > _RPC_SLOW_MS:
            logger.warning(
                "Etap 4 tier-4: fn_compute_method_pricing slow "
                "(method=%s): %dms", method_id, _mp_dur_ms,
            )
            if tracer is not None:
                tracer.add(
                    "rpc.latency",
                    {
                        "rpc": "fn_compute_method_pricing",
                        "method_id": method_id,
                        "duration_ms": _mp_dur_ms,
                    },
                )
        pricing_data = pricing_res.data or []
        if not pricing_data:
            # No competitors in radius offering this method — emit
            # subject_only row with empty samples so UI shows the gap.
            if subject_services_for_method:
                rows.append(_method_row_subject_only(
                    report_id, method_id, canonical, display, category,
                    method_type, subject_services_for_method, radius_km,
                ))
            continue

        stats = pricing_data[0]
        sample_size = int(stats.get("sample_size") or 0)
        unique_salons = int(stats.get("unique_salons") or 0)

        if tracer is not None:
            tracer.add(
                step="method_targeted.method_pricing",
                data={
                    "method_id": method_id,
                    "canonical_name": canonical,
                    "sample_size": sample_size,
                    "unique_salons": unique_salons,
                    "market_median_grosze": stats.get("market_median_grosze"),
                    "subject_services_count": len(subject_services_for_method),
                },
            )

        # Sample gate
        if sample_size < min_sample_size or unique_salons < min_unique_salons:
            dropped_low_sample += 1
            if subject_services_for_method:
                rows.append(_method_row_subject_only(
                    report_id, method_id, canonical, display, category,
                    method_type, subject_services_for_method, radius_km,
                    reason="low_sample",
                ))
            continue

        # Compute subject's median + range for this method.
        # 2026-05-21 (issue #88): also emit min/max/count so UI can show
        # full price spectrum instead of single representative median.
        # B4E example: 9 Dermapen services 100-2700 zł → showing only
        # the median (1100) misled users into thinking we lost their cennik.
        subject_prices = sorted([
            int(s["price_grosze"])
            for s in subject_services_for_method
            if s.get("price_grosze")
        ])
        if not subject_prices:
            continue
        subject_median = subject_prices[len(subject_prices) // 2]
        subject_min = subject_prices[0]
        subject_max = subject_prices[-1]
        subject_services_count = len(subject_prices)
        market_median = int(stats["market_median_grosze"])

        deviation_pct = round(
            100.0 * (subject_median - market_median) / market_median, 1
        ) if market_median > 0 else 0.0

        # Recommended action thresholds — same convention as tier=treatment
        if abs(deviation_pct) < 10:
            action = "hold"
        elif deviation_pct < 0:
            action = "raise"
        else:
            action = "lower"

        rows.append({
            "report_id": report_id,
            "comparison_tier": "method",
            "treatment_method_id": method_id,
            "booksy_treatment_id": None,
            "synthetic_treatment_id": None,
            "variant_id": None,
            "treatment_name": display,
            "subject_price_grosze": subject_median,
            "subject_is_from_price": False,
            "subject_duration_minutes": _median([
                s.get("duration_minutes") for s in subject_services_for_method
                if s.get("duration_minutes")
            ]),
            "subject_price_per_min_grosze": None,
            "market_min_grosze":    int(stats["market_min_grosze"]),
            "market_p25_grosze":    int(stats["market_p25_grosze"]),
            "market_median_grosze": market_median,
            "market_p75_grosze":    int(stats["market_p75_grosze"]),
            "market_max_grosze":    int(stats["market_max_grosze"]),
            "market_price_per_min_grosze_min":    None,
            "market_price_per_min_grosze_p25":    None,
            "market_price_per_min_grosze_median": None,
            "market_price_per_min_grosze_p75":    None,
            "market_price_per_min_grosze_max":    None,
            "deviation_pct": deviation_pct,
            "deviation_pct_per_min": None,
            "sample_size": sample_size,
            "recommended_action": action,
            "verification_status": "method_targeted",
            "verification_details": {
                "radius_km": radius_km,
                "unique_salons": unique_salons,
                "subject_services_count": subject_services_count,
                "subject_min_grosze": subject_min,
                "subject_max_grosze": subject_max,
                "subject_median_grosze": subject_median,
                "method_category": category,
                "method_type": method_type,
                "avg_duration_minutes": stats.get("avg_duration_minutes"),
            },
            "competitor_samples": stats.get("sample_services") or [],
        })
        promoted += 1

    logger.info(
        "Etap 4 tier-4: emitted %d method-level rows (promoted=%d, "
        "dropped_low_sample=%d, total_subject_methods=%d)",
        len(rows), promoted, dropped_low_sample, len(subject_methods),
    )
    return rows


def _method_row_subject_only(
    report_id: int,
    method_id: int,
    canonical: str,
    display: str,
    category: str,
    method_type: str,
    subject_services: list[dict[str, Any]],
    radius_km: float,
    *,
    reason: str = "no_competitors",
) -> dict[str, Any]:
    """Emit a method-tier row in subject_only mode when no
    cross-salon market data is available (no salons in radius offering
    this method, or sample too small for credible median)."""
    prices = sorted([
        int(s["price_grosze"])
        for s in subject_services
        if s.get("price_grosze")
    ])
    subject_median = prices[len(prices) // 2] if prices else 0
    subject_min = prices[0] if prices else None
    subject_max = prices[-1] if prices else None
    return {
        "report_id": report_id,
        "comparison_tier": "method",
        "treatment_method_id": method_id,
        "booksy_treatment_id": None,
        "synthetic_treatment_id": None,
        "variant_id": None,
        "treatment_name": display,
        "subject_price_grosze": subject_median,
        "subject_is_from_price": False,
        "subject_duration_minutes": _median([
            s.get("duration_minutes") for s in subject_services
            if s.get("duration_minutes")
        ]),
        "subject_price_per_min_grosze": None,
        "market_min_grosze":    None,
        "market_p25_grosze":    None,
        "market_median_grosze": None,
        "market_p75_grosze":    None,
        "market_max_grosze":    None,
        "market_price_per_min_grosze_min":    None,
        "market_price_per_min_grosze_p25":    None,
        "market_price_per_min_grosze_median": None,
        "market_price_per_min_grosze_p75":    None,
        "market_price_per_min_grosze_max":    None,
        "deviation_pct": None,
        "deviation_pct_per_min": None,
        "sample_size": 0,
        "recommended_action": "subject_only",
        "verification_status": "method_targeted_subject_only",
        "verification_details": {
            "radius_km": radius_km,
            "reason": reason,
            "subject_services_count": len(subject_services),
            # 2026-05-21 (issue #88): aggregate spectrum for UI even
            # without market comparison — user still needs to see the
            # range of their own cennik in this method.
            "subject_min_grosze": subject_min,
            "subject_max_grosze": subject_max,
            "subject_median_grosze": subject_median if prices else None,
            "method_category": category,
            "method_type": method_type,
        },
        "competitor_samples": [],
    }


async def _compute_brand_structured_pricing(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[Any],
    *,
    audit_id: str | None = None,
    tracer: TraceWriter | None = None,
    min_direct_sample: int = 3,
) -> list[dict[str, Any]]:
    """Per-subject-service pricing rows using fn_pricing_samples_structured
    (mig 101). The structural query joins service_method_classification +
    treatment_methods.brand_family/category — exactly the columns the user
    asked us to use as PRIMARY filter so that "wpisz Red Touch w wyszukiwarce"
    semantics hold for the comparison engine.

    Per service emits comparison_tier='structured' row. Sample selection:
      - tier 1+2 (same method OR same brand+category) used as direct samples
      - tier 3 (same category, different brand) used as related_samples
        fallback to give user broader market context when direct is thin

    Replaces the cross-brand pollution from tier='treatment' (which filtered
    only by booksy_treatment_id, mixing e.g. Cytocare with Tropokolagen +
    NEAUVIA + mezo dłoni under one umbrella). Frontend dedup
    (adaptToReportData.mapPricingFromBagent) prefers tier='structured' over
    tier='treatment' for the same (booksy_treatment_id, subject_price,
    duration) grouping key.
    """
    if not aligned_competitors:
        if tracer is not None:
            tracer.add(
                "pricing.computed",
                {
                    "pricing_type": "structured",
                    "row_count": 0,
                    "skip_reason": "no_competitors",
                },
            )
        return []
    competitor_booksy_ids = [
        cand.booksy_id for cand, _ in aligned_competitors
        if cand.counts_in_aggregates
    ]
    if not competitor_booksy_ids:
        if tracer is not None:
            tracer.add(
                "pricing.computed",
                {
                    "pricing_type": "structured",
                    "row_count": 0,
                    "skip_reason": "no_counting_competitors",
                },
            )
        return []

    rows: list[dict[str, Any]] = []
    services = subject_data.get("services") or []
    skipped_no_id = 0
    skipped_ineligible = 0
    direct_emitted = 0
    related_only_emitted = 0

    def _service_eligible(svc: dict[str, Any]) -> bool:
        if not svc.get("is_active", True):
            return False
        if svc.get("price_grosze") is None or svc.get("price_grosze", 0) <= 0:
            return False
        dur = svc.get("duration_minutes")
        if dur is None or dur < 5 or dur > 240:
            return False
        name = (svc.get("name") or "").lower()
        # Drop package/abonament names — they bias median upward
        if any(kw in name for kw in (
            "pakiet", "abonament", "karnet", "voucher", "bon ",
            "x zabieg", "zabiegów",
        )):
            return False
        if _is_package_service(svc):
            return False
        return True

    for svc in services:
        sid = svc.get("id")
        if sid is None:
            skipped_no_id += 1
            continue
        if not _service_eligible(svc):
            skipped_ineligible += 1
            continue

        # Call structural RPC (mig 102 — geo radius scope for tier 1+2)
        try:
            res = service.client.rpc(
                "fn_pricing_samples_structured",
                {
                    "p_subject_service_id": int(sid),
                    "p_competitor_booksy_ids": competitor_booksy_ids,
                    "p_limit": 500,
                    "p_radius_km": 16.0,
                },
            ).execute()
            samples = res.data or []
        except Exception as e:
            logger.error(
                "structured tier: fn_pricing_samples_structured FAILED "
                "for service_id=%s: %s — skipping service",
                sid, e,
            )
            continue

        tier1_2 = [s for s in samples if s.get("tier") in (1, 2)]
        tier3 = [s for s in samples if s.get("tier") == 3]

        # Use tier 1+2 as direct samples when present. Tier 3 always goes
        # to related_samples for broader-context fallback.
        # Even a single tier-1 row is useful (Red Touch in Beauty4ever audit
        # has only 1 direct competitor — ESTHETIC&MED 1490 zł — that's still
        # information vs no-data UI). Min_direct_sample gates the percentile
        # computation, not the row emission.

        subj_price = int(svc["price_grosze"])
        subj_dur = svc.get("duration_minutes")
        booksy_tid = svc.get("booksy_treatment_id")
        treatment_name = svc.get("name") or ""

        def _to_sample(s: dict[str, Any]) -> dict[str, Any]:
            return {
                "salon_id": s.get("salon_id"),
                "salon_name": s.get("salon_name"),
                "booksy_id": s.get("booksy_id"),
                "service_id": s.get("service_id"),
                "service_name": s.get("service_name"),
                "price_grosze": int(s["price_grosze"]),
                "duration_minutes": s.get("duration_minutes"),
                "name_similarity": float(s.get("similarity") or 0.0),
                "brand_marker": s.get("brand_family"),
                "method_marker": s.get("method_canonical"),
                "structured_tier": int(s.get("tier") or 0),
            }

        direct_samples = [_to_sample(s) for s in tier1_2]
        related_samples = [
            {**_to_sample(s), "relation": "same_category"} for s in tier3
        ]

        if len(direct_samples) >= min_direct_sample:
            # Compute market stats from direct samples
            prices = sorted([s["price_grosze"] for s in direct_samples])
            n = len(prices)
            market_min = prices[0]
            market_max = prices[-1]
            market_p25 = prices[int(0.25 * (n - 1))]
            market_median = prices[int(0.50 * (n - 1))]
            market_p75 = prices[int(0.75 * (n - 1))]
            cheaper = sum(1 for p in prices if p < subj_price)
            percentile = round(100.0 * cheaper / n, 2)
            deviation = round(
                100.0 * (subj_price - market_median) / market_median, 2
            ) if market_median > 0 else 0.0
            if abs(deviation) < 10:
                action = "hold"
            elif deviation < 0:
                action = "raise"
            else:
                action = "lower"
            row = {
                "report_id": report_id,
                "comparison_tier": "structured",
                "booksy_treatment_id": booksy_tid,
                "synthetic_treatment_id": None,
                "variant_id": None,
                "treatment_method_id": None,
                "treatment_name": treatment_name,
                "subject_price_grosze": subj_price,
                "subject_is_from_price": False,
                "subject_duration_minutes": subj_dur,
                "subject_price_per_min_grosze": (
                    round(subj_price / subj_dur, 2)
                    if subj_dur and subj_dur > 0 else None
                ),
                "market_min_grosze": market_min,
                "market_p25_grosze": market_p25,
                "market_median_grosze": market_median,
                "market_p75_grosze": market_p75,
                "market_max_grosze": market_max,
                "deviation_pct": deviation,
                "subject_percentile": percentile,
                "sample_size": n,
                "recommended_action": action,
                "verification_status": "structured_direct",
                "verification_details": {
                    "direct_tier1_2_count": len(direct_samples),
                    "related_tier3_count": len(related_samples),
                },
                "competitor_samples": direct_samples,
                "related_samples": related_samples,
            }
            rows.append(row)
            direct_emitted += 1
        else:
            # Insufficient direct → emit subject_only with tier 3 as fallback
            row = {
                "report_id": report_id,
                "comparison_tier": "structured",
                "booksy_treatment_id": booksy_tid,
                "synthetic_treatment_id": None,
                "variant_id": None,
                "treatment_method_id": None,
                "treatment_name": treatment_name,
                "subject_price_grosze": subj_price,
                "subject_is_from_price": False,
                "subject_duration_minutes": subj_dur,
                "subject_price_per_min_grosze": (
                    round(subj_price / subj_dur, 2)
                    if subj_dur and subj_dur > 0 else None
                ),
                "market_min_grosze": None,
                "market_p25_grosze": None,
                "market_median_grosze": None,
                "market_p75_grosze": None,
                "market_max_grosze": None,
                "deviation_pct": None,
                "subject_percentile": None,
                "sample_size": len(direct_samples),
                "recommended_action": "subject_only",
                "verification_status": "structured_subject_only",
                "verification_details": {
                    "direct_tier1_2_count": len(direct_samples),
                    "related_tier3_count": len(related_samples),
                    "reason": "insufficient_direct_samples",
                },
                "competitor_samples": direct_samples,  # may have 1-2 tier-1 rows
                "related_samples": related_samples,
            }
            rows.append(row)
            related_only_emitted += 1

    logger.info(
        "Etap 4 structured: emitted %d rows for audit %s (direct=%d, "
        "related_only=%d, skipped_no_id=%d, ineligible=%d)",
        len(rows), audit_id, direct_emitted, related_only_emitted,
        skipped_no_id, skipped_ineligible,
    )
    if tracer is not None:
        tracer.add(step="structured_tier.summary", data={
            "rows_emitted": len(rows),
            "direct_emitted": direct_emitted,
            "related_only_emitted": related_only_emitted,
        })
    return rows


async def _load_subject_services_by_method(
    service: SupabaseService,
    subject_salon_id: int,
) -> dict[int, list[dict[str, Any]]]:
    """Returns {method_id: [services_dict]} for the subject's active
    chain-head services, joined through service_method_classification.
    A service mapped to multiple methods appears under each method_id."""
    # Resolve chain head
    sc_res = (
        service.client.table("salon_scrapes")
        .select("id")
        .eq("salon_ref_id", subject_salon_id)
        .eq("is_chain_head", True)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
    )
    if not sc_res.data:
        return {}
    scrape_id = sc_res.data[0]["id"]

    # Load services + classifications. Two queries: services first,
    # then cache rows for those IDs.
    svc_res = (
        service.client.table("salon_scrape_services")
        # is_package MUSI być w SELECT — _is_package_service (linia ~994) ufa
        # fladze z DB jako źródłu prawdy i tylko fallbackuje na regex nazwy gdy
        # kolumny brak. Bez niej method-tier subject pricing przepuszczał pakiety
        # których nazwa jest spłaszczona (Booksy trzyma "5 zabiegów" w wariancie,
        # nie w name) → fałszywe +400% (np. Presoterapia 700 zł = pakiet 5-zab).
        .select("id, name, price_grosze, duration_minutes, is_package")
        .eq("scrape_id", scrape_id)
        .eq("is_active", True)
        .not_.is_("price_grosze", "null")
        .execute()
    )
    services_by_id = {int(s["id"]): s for s in (svc_res.data or [])}
    if not services_by_id:
        return {}

    cache_res = (
        service.client.table("service_method_classification")
        .select("service_id, method_id")
        .in_("service_id", list(services_by_id.keys()))
        .execute()
    )
    out: dict[int, list[dict[str, Any]]] = {}
    skipped_pkg = 0
    for row in (cache_res.data or []):
        sid = int(row["service_id"])
        mid = int(row["method_id"])
        svc = services_by_id.get(sid)
        if not svc:
            continue
        # Wyklucz pakiety / multi-pack z method-tier subject pricing. Pakiet typu
        # "Pakiet Komfort (bikini gł. + pachy + nogi) x5" (3750 zł = 5 zabiegów
        # łączonej strefy) NIE może udawać pojedynczego zabiegu metody
        # "depilacja nóg" — inaczej subject_median = 3750 zł vs mediana pojedynczych
        # ~128 zł daje fałszywe +2830%. Spójne z _active_services_with_variant
        # (mig 064), które filtruje pakiety na subject-side variant/treatment tierów;
        # method-tier (tier-4 fallback) wcześniej tego NIE robił.
        if _is_package_service(svc):
            skipped_pkg += 1
            continue
        out.setdefault(mid, []).append(svc)
    if skipped_pkg > 0:
        logger.info(
            "_load_subject_services_by_method: dropped %d package services from "
            "method-tier subject pricing (pakiet x5 → fałszywy pojedynczy zabieg)",
            skipped_pkg,
        )
    return out


def _median(values: list) -> int | None:
    """Median of non-null integer-castable values, or None if empty."""
    nums = sorted([int(v) for v in values if v is not None])
    if not nums:
        return None
    return nums[len(nums) // 2]


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


def _emit_subject_only_rows_no_variants(
    report_id: int,
    services: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Subject-only pricing rows for a subject with NO variant_id at all
    (quick 260613-rne early-exit). Groups subject services per _tid_key
    (Booksy OR synthetic), emits one tier-1 row per group, price = group
    median (same as tier-1). No market data, no related_samples (zero RPC) —
    the UI renders "tylko u Ciebie" with dashes instead of an empty section.

    The row shape MUST be identical to _compute_treatment_tier_rows's
    _emit_treatment_subject_only_row (comparison_tier='treatment',
    variant_id=None, recommended_action='subject_only', with
    synthetic_treatment_id + taxonomy_source) so the insert into
    competitor_pricing_comparisons and _dedup_pricing_rows work unchanged.
    Grouping per _tid_key (one row per booksy_tid, or per synthetic_tid +
    treatment_name) lines up with _dedup_pricing_rows.row_key (variant_id
    None, no sub_variant_*, treatment_name in key when tid is NULL).
    """
    # Mirror of _compute_treatment_tier_rows::_eligible (nested → not
    # importable; replicated verbatim).
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
        if _is_package_service(svc):
            return False
        return True

    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for svc in services:
        if not _eligible(svc):
            continue
        key = _tid_key(svc)
        if key is None:
            continue
        groups.setdefault(key, []).append(svc)

    rows: list[dict[str, Any]] = []
    for (kind, tid_int), svcs in groups.items():
        prices = [
            int(s["price_grosze"]) for s in svcs
            if s.get("price_grosze") is not None
        ]
        if not prices:
            continue
        median_price = statistics.median(prices)
        ppms = [
            round(int(s["price_grosze"]) / s["duration_minutes"], 2)
            for s in svcs
            if s.get("price_grosze") is not None
            and s.get("duration_minutes")
        ]
        median_ppm = round(statistics.median(ppms), 2) if ppms else None
        first = svcs[0]
        # taxonomy_source — propagated from any service in the group (they
        # should share the same source by construction; first non-null,
        # else None). Mirrors _compute_treatment_tier_rows ~2936. The column
        # is nullable (mig 074: TEXT + CHECK IN(...) which passes on NULL),
        # so None is a valid value when no service carries a source.
        taxonomy_source = next(
            (s.get("taxonomy_source") for s in svcs if s.get("taxonomy_source")),
            None,
        )
        rows.append({
            "report_id": report_id,
            "comparison_tier": "treatment",
            "booksy_treatment_id": tid_int if kind == "booksy" else None,
            "synthetic_treatment_id": tid_int if kind == "synthetic" else None,
            "taxonomy_source": taxonomy_source,
            "variant_id": None,
            "treatment_name": (
                first.get("treatment_name") or first.get("name") or "Unknown"
            ),
            "treatment_parent_id": first.get("treatment_parent_id"),
            "subject_price_grosze": int(median_price),
            "subject_is_from_price": False,
            "subject_duration_minutes": int(
                first.get("duration_minutes") or 0
            ) or None,
            "subject_price_per_min_grosze": median_ppm,
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
            "related_samples": [],
        })
    return rows


def _dedup_pricing_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse duplicate pricing rows at the same logical key.

    Phase 1b (2026-05-18): _compute_pricing_comparisons / _compute_treatment_tier_rows
    / _compute_sub_variant_tier_rows occasionally emit multiple rows for the
    same (report_id, comparison_tier, booksy_treatment_id, variant_id,
    sub_variant_group_id) tuple. Root cause is that pricing functions iterate
    over subject services without grouping first by (tid, variant_id), so two
    subject services in the same logical bucket each produce their own row
    with different competitor sample sets.

    Tactical fix: deterministic dedup keeping the row with HIGHEST sample_size
    per key (more competitor evidence wins). When sample_size ties, prefer
    rows with non-NULL market_median_grosze (real data over subject_only).

    Proper fix (deferred): refactor _compute_pricing_comparisons to group
    subject services upfront before computing per-key market views.
    """
    if not rows:
        return rows

    def row_key(r: dict[str, Any]) -> tuple:
        return (
            r.get("report_id"),
            r.get("comparison_tier"),
            # S0078 — scope (selected vs local_market) to osobne zakładki, nie duplikaty.
            r.get("comparison_scope"),
            r.get("booksy_treatment_id"),
            r.get("variant_id"),
            r.get("sub_variant_group_id"),
            r.get("sub_variant_label"),
            # S0078 — tier 'identity' jest per-usługa subjectu: różne usługi mogą dzielić
            # booksy_treatment_id (Botox 1/2/3 okolice), więc nazwa + cena je rozróżniają.
            r.get("treatment_name"),
            r.get("subject_price_grosze"),
        )

    def row_quality(r: dict[str, Any]) -> tuple:
        # Higher is better: real market data first, then more samples.
        has_market = 1 if r.get("market_median_grosze") is not None else 0
        sample_size = int(r.get("sample_size") or 0)
        return (has_market, sample_size)

    best: dict[tuple, dict[str, Any]] = {}
    dropped = 0
    for r in rows:
        k = row_key(r)
        if k not in best or row_quality(r) > row_quality(best[k]):
            if k in best:
                dropped += 1
            best[k] = r
        else:
            dropped += 1

    if dropped:
        logger.info(
            "Phase 1b dedup: dropped %d duplicate pricing rows "
            "(kept best per key out of %d)", dropped, len(rows),
        )
    return list(best.values())


# ---------------------------------------------------------------------------
# Service gaps
# ---------------------------------------------------------------------------


# Categories which are too generic / catch-all to use as walk-up evidence.
# `inny` is a literal "miscellaneous" bucket with 12k+ method rows — saying
# subject has "an inny service" tells us nothing about overlap with a
# specific gap candidate. If we walked up on `inny`, virtually every
# legitimate missing gap would be hidden because any salon with a single
# misc service blocks every misc-categorized gap candidate.
#
# Add other catch-all categories here ONLY if downstream debugging shows
# they over-filter. Keep this small — being too permissive defeats the
# walk-up; being too restrictive brings back the false positives the
# walk-up was designed to fix.
_NON_DISCRIMINATIVE_METHOD_CATEGORIES: frozenset[str] = frozenset({"inny"})


async def _resolve_method_categories_for_services(
    service: SupabaseService,
    service_ids: list[int],
    *,
    tracer: "TraceWriter | None" = None,
) -> dict[int, set[str]]:
    """Return {service_id: {treatment_methods.category}} for given service ids.

    Joins `service_method_classification` with `treatment_methods` and
    returns the set of method categories per service. A service can have
    multiple method classifications (multi-procedure rooms — e.g.
    "PRO XN + Dermapen" → [dermapen, pro_xn]), so the value is a set, not
    a single string.

    Categories listed in `_NON_DISCRIMINATIVE_METHOD_CATEGORIES` are
    filtered out — they're catch-all buckets ('inny') that would over-
    filter the gap walk-up logic.

    Returns {} when service_ids is empty or no classifications exist.
    Failures (Supabase down, etc.) log a warning and return {} so the
    caller fails open — preserving the legacy tid-only behavior is safer
    than crashing the whole gap computation.
    """
    if not service_ids:
        return {}
    # Two-step lookup: first method_ids per service, then categories per
    # method_id. We can't single-query because Supabase Python client
    # doesn't expose joins on service_method_classification ↔
    # treatment_methods directly.
    try:
        cls_res = (
            service.client.table("service_method_classification")
            .select("service_id, method_id")
            .in_("service_id", service_ids)
            .execute()
        )
    except Exception as e:
        logger.warning(
            "Failed to load service_method_classification for %d service ids: %s",
            len(service_ids), e,
        )
        # P0 silent-fail surfacing (quick 260613-m23): the gap walk-up fails
        # open to legacy tid-only behaviour on a classification RPC error — a
        # silent loss of method-category filtering. Trace it so the operator
        # can correlate noisy gap rows with a DB blip. Behaviour unchanged.
        if tracer is not None:
            tracer.add(
                "method_categories.rpc_fail",
                {
                    "stage": "service_method_classification",
                    "service_ids_count": len(service_ids),
                    "error": str(e)[:500],
                },
            )
        return {}
    rows = cls_res.data or []
    if not rows:
        return {}
    method_ids = list({int(r["method_id"]) for r in rows if r.get("method_id") is not None})
    if not method_ids:
        return {}
    try:
        tm_res = (
            service.client.table("treatment_methods")
            .select("id, category")
            .in_("id", method_ids)
            .execute()
        )
    except Exception as e:
        logger.warning(
            "Failed to load treatment_methods categories for %d method ids: %s",
            len(method_ids), e,
        )
        if tracer is not None:
            tracer.add(
                "method_categories.rpc_fail",
                {
                    "stage": "treatment_methods",
                    "service_ids_count": len(service_ids),
                    "method_ids_count": len(method_ids),
                    "error": str(e)[:500],
                },
            )
        return {}
    method_to_category: dict[int, str] = {
        int(r["id"]): r["category"]
        for r in (tm_res.data or [])
        if r.get("id") is not None and r.get("category")
    }
    out: dict[int, set[str]] = {}
    for r in rows:
        sid = r.get("service_id")
        mid = r.get("method_id")
        if sid is None or mid is None:
            continue
        category = method_to_category.get(int(mid))
        if not category:
            continue
        if category in _NON_DISCRIMINATIVE_METHOD_CATEGORIES:
            continue
        out.setdefault(int(sid), set()).add(category)
    return out


async def _filter_missing_by_method_category(
    service: SupabaseService,
    missing: list[dict[str, Any]],
    subject_svcs: dict[int, dict[str, Any]],
    competitor_service_ids_by_tid: dict[int, list[int]],
    *,
    tracer: "TraceWriter | None" = None,
) -> list[dict[str, Any]]:
    """Drop missing-gap rows whose method category is already covered by
    the subject.

    Approach:
      1. Resolve `treatment_methods.category` for every subject service
         via `service_method_classification`. Build subject_categories set.
      2. Resolve same categories for the competitor services that carry
         each candidate-missing tid. Build {tid: {category}} map.
      3. Filter: drop rows where tid's categories intersect with
         subject_categories.

    Fail-open contract: if either lookup returns nothing (cold cache,
    Supabase blip, or services with no classification yet), keep the
    row. The legacy tid-only behaviour is the safe fallback — better
    to surface one false positive than to silently hide a real gap.
    """
    if not missing:
        return missing

    subject_service_ids: list[int] = []
    for svc in subject_svcs.values():
        sid = svc.get("id")
        if sid is not None:
            try:
                subject_service_ids.append(int(sid))
            except (TypeError, ValueError):
                continue

    # No subject services with ids → nothing to walk up against, keep
    # the legacy behaviour (return list unchanged).
    if not subject_service_ids:
        return missing

    subject_categories_by_svc = await _resolve_method_categories_for_services(
        service, subject_service_ids, tracer=tracer,
    )
    subject_categories: set[str] = set()
    for cats in subject_categories_by_svc.values():
        subject_categories.update(cats)

    if not subject_categories:
        # Subject is unclassified — can't safely walk up. Keep legacy
        # behaviour.
        logger.info(
            "service_gaps walk-up: subject has no classified services "
            "with non-catch-all categories, skipping walk-up (kept %d "
            "candidate missing rows as-is)", len(missing),
        )
        return missing

    # Collect all competitor service ids across all candidate missing
    # tids in one batch — single Supabase round-trip beats N round-trips.
    candidate_tids = [int(row["booksy_treatment_id"]) for row in missing]
    all_comp_svc_ids: list[int] = []
    for tid in candidate_tids:
        all_comp_svc_ids.extend(competitor_service_ids_by_tid.get(tid, []))
    # Dedup to reduce query payload — same competitor service can appear
    # under one tid only, but defensive against future code paths.
    all_comp_svc_ids = list({int(x) for x in all_comp_svc_ids})

    comp_categories_by_svc = await _resolve_method_categories_for_services(
        service, all_comp_svc_ids, tracer=tracer,
    )

    # Build tid → set[category] by aggregating across the tid's
    # competitor services. Multiple competitor salons may classify the
    # same tid differently (e.g. "Lifting falą radiową" may map to
    # `rf_hifu` in one salon's row and `laser_skin` in another's). Take
    # union — if ANY classification overlaps subject, the subject has
    # equivalent coverage.
    tid_to_categories: dict[int, set[str]] = {}
    for tid in candidate_tids:
        svc_ids = competitor_service_ids_by_tid.get(tid, [])
        cats: set[str] = set()
        for sid in svc_ids:
            cats.update(comp_categories_by_svc.get(int(sid), set()))
        if cats:
            tid_to_categories[tid] = cats

    kept: list[dict[str, Any]] = []
    suppressed: list[tuple[int, str, set[str]]] = []
    for row in missing:
        tid = int(row["booksy_treatment_id"])
        cats = tid_to_categories.get(tid)
        if not cats:
            # Competitor side unclassified for this tid → fail-open,
            # keep the row.
            kept.append(row)
            continue
        overlap = cats & subject_categories
        if overlap:
            suppressed.append((tid, row.get("treatment_name", "Unknown"), overlap))
            continue
        kept.append(row)

    if suppressed:
        logger.info(
            "service_gaps walk-up: suppressed %d missing rows whose "
            "method category is already covered by subject "
            "(subject_categories=%s); examples: %s",
            len(suppressed),
            sorted(subject_categories),
            [(tid, name, sorted(cats)) for tid, name, cats in suppressed[:5]],
        )
    return kept



# ── Weto rodziny nazw dla luk (2026-08-29, raport 250) ──────────────
# Tokeny, które nie identyfikują procedury (obszary ciała, wypełniacze) —
# nie mogą być rdzeniem rodziny ani po stronie luki, ani podmiotu.
_LUKA_STOPWORDY = frozenset({
    # obszary ciała
    "twarz", "twarzy", "cialo", "ciala", "calego", "szyja", "szyi",
    "dekolt", "dekoltu", "dlonie", "dloni", "stopy", "plecy", "plecow",
    "glowa", "glowy", "brzuch", "brzucha", "biust", "biustu", "uda",
    "lydki", "pachy", "ramiona", "posladki", "posladkow", "nogi", "rece",
    "usta", "broda", "kark", "bikini",
    # wypełniacze i modyfikatory bez tożsamości procedury
    "okolica", "okolice", "okolic", "obszar", "obszary", "zabieg",
    "zabiegi", "pakiet", "seria", "premium", "zmarszczek", "zmarszczki",
    # nośniki chemiczne — "kwasem" występuje i w peelingach, i w fillerach;
    # rodziną jest peeling/wypełnianie, nie kwas
    "kwas", "kwasu", "kwasem", "kwasami",
})


def _rdzenie_rodziny_luki(nazwa: str) -> list[str]:
    """Rdzenie (5 znaków) DWÓCH pierwszych znaczących tokenów nazwy.

    Dwa, nie jeden: polskie nazwy zabiegów często zaczynają się od
    przymiotnika ("NIECHIRURGICZNY lifting twarzy") albo ogólnika
    ("PIELĘGNACJA i oczyszczanie twarzy") — pierwsza wersja weta (jeden
    token) przepuściła oba fałsze w regeneracji raportu 250. Próg 4 znaki,
    żeby złapać HIFU/Onda; ł→l PRZED NFKD (U+0142 nie ma dekompozycji).
    Pusta lista, gdy nic znaczącego (fail-open — luka zostaje).
    """
    import unicodedata as _ud
    plain = nazwa.replace("\u0142", "l").replace("\u0141", "L")
    plain = _ud.normalize("NFKD", plain)
    plain = "".join(ch for ch in plain if not _ud.combining(ch)).lower()
    rdzenie: list[str] = []
    for tok in re.findall(r"[a-z]+", plain):
        if len(tok) >= 4 and tok not in _LUKA_STOPWORDY:
            rdzenie.append(tok[:5])
            if len(rdzenie) == 2:
                break
    return rdzenie


def _rdzenie_wszystkich_tokenow(nazwy: list[str]) -> set[str]:
    """Rdzenie (5 znaków) WSZYSTKICH znaczących tokenów listy nazw."""
    import unicodedata as _ud
    rdzenie: set[str] = set()
    for nazwa in nazwy:
        plain = (nazwa or "").replace("\u0142", "l").replace("\u0141", "L")
        plain = _ud.normalize("NFKD", plain)
        plain = "".join(ch for ch in plain if not _ud.combining(ch)).lower()
        for tok in re.findall(r"[a-z]+", plain):
            if len(tok) >= 4 and tok not in _LUKA_STOPWORDY:
                rdzenie.add(tok[:5])
    return rdzenie


async def _compute_service_gaps(
    service: SupabaseService,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[CompetitorCandidate, dict[str, Any]]],
    *,
    tracer: "TraceWriter | None" = None,
) -> list[dict[str, Any]]:
    """Compute service gap rows (missing + unique_usp).

    - 'missing': top 10 treatments that ≥1 counts_in_aggregates competitor
      offers but the subject does not. Popularity score = competitor_count
      weighted by review mentions (limited due to 3-sample review cap).
      Candidates whose method category (`treatment_methods.category` via
      `service_method_classification`) already has equivalent subject
      coverage are filtered out — see "Method-category walk-up" below.
    - 'unique_usp': up to 5 treatments only the subject offers, WERYFIKOWANE
      przez embedding similarity vs wszystkie services konkurentów. Subject
      może mieć brand-specific name (Thunder, Onda, Light&Bright) który
      mapuje na inny tid niż konkurenci, ale TO TA SAMA PROCEDURA.
      User insight: \"jeśli to jest nic innego jak depilacja laserowa
      tylko innym urządzeniem, to są kretynami\" — fałszywe USP rujnują
      pozycjonowanie i marketing.

    Method-category walk-up (2026-05-24, fixes false-positive missing rows):
      Booksy `treatment_id` is a flat taxonomy — "Lifting falą radiową"
      (tid=511) and "Onda" (different tid) are sibling treatments inside
      the same method category `rf_hifu`. A subject with 9 Onda + 4
      fala_radiowa services has FULL coverage of rf_hifu but the legacy
      tid set-difference reports "Lifting falą radiową" as missing. The
      walk-up reads `treatment_methods.category` for every candidate gap
      tid (via the services that carry it in the loaded competitor data)
      and filters out gaps whose category is already covered by ANY
      subject service classified into the same category. Catch-all
      categories like `inny` are excluded from the walk-up — they would
      mass-suppress legitimate gaps.

      Walk-up is fail-open: if the classification cache returns nothing
      (cold cache, Supabase blip, brand-new services), the gap survives.
      Better to surface a real false positive than to silently hide a
      real gap.
    """
    subject_svcs = _active_services_with_treatment(subject_data.get("services") or [])
    subject_tids = set(subject_svcs.keys())

    # Per-treatment stats across competitors
    competitor_counts: dict[int, int] = {}
    competitor_prices: dict[int, list[int]] = {}
    treatment_names: dict[int, str] = {}
    treatment_parents: dict[int, int | None] = {}
    # Service ids per tid — used by the method-category walk-up below to
    # resolve `treatment_methods.category` for each candidate-missing tid
    # through service_method_classification.
    competitor_service_ids_by_tid: dict[int, list[int]] = {}

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
            svc_id = svc.get("id")
            if svc_id is not None:
                competitor_service_ids_by_tid.setdefault(tid, []).append(int(svc_id))

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

    # ── Method-category walk-up (2026-05-24) ─────────────────────────
    # Drop gap rows whose method category is already covered by the
    # subject. Booksy `treatment_id` is brand-flavoured (Lifting falą
    # radiową, Onda, Virtue RF — three sibling tids, same rf_hifu
    # category). The legacy set-difference reported every sibling tid
    # the subject didn't carry as "missing", regardless of whether the
    # subject already had equivalent coverage under a different brand
    # name. The walk-up reuses the existing classification cache —
    # `service_method_classification` → `treatment_methods.category` —
    # to detect this overlap.
    #
    # Done BEFORE the top-10 truncation so legitimate gaps surface into
    # the top 10 instead of being held behind a queue of suppressed
    # false positives.
    if missing:
        missing = await _filter_missing_by_method_category(
            service, missing, subject_svcs, competitor_service_ids_by_tid,
            tracer=tracer,
        )

    # ── Weto rodziny nazw (2026-08-29, raport 250) ──────────────────
    # Walk-up po kategoriach jest ślepy, gdy usługi podmiotu nie mają
    # klasyfikacji (tid=None — VIRTUE RF itp.): raport 250 kazał "dodać"
    # lifting/mezoterapię/peeling salonowi z 5 liftingami i 11
    # mezoterapiami. Siatka bezpieczeństwa: luka odpada, jeśli rdzeń
    # rodziny jej nazwy występuje wśród rdzeni tokenów nazw usług
    # podmiotu (WSZYSTKICH aktywnych, także tych bez tid — to właśnie
    # martwe pole walk-upu). Świadomy koszt: przemilczymy lukę
    # "Depilacja laserowa" u salonu z woskiem — fałszywe "brakuje Ci"
    # jest droższe niż przemilczana wariacja.
    if missing:
        nazwy_podmiotu = [
            str(svc.get("name") or "")
            for svc in (subject_data.get("services") or [])
            if isinstance(svc, dict) and svc.get("is_active", True)
        ]
        rdzenie_podmiotu = _rdzenie_wszystkich_tokenow(nazwy_podmiotu)
        przed_wetem = len(missing)
        missing = [
            g for g in missing
            if not any(
                r in rdzenie_podmiotu
                for r in _rdzenie_rodziny_luki(str(g.get("treatment_name") or ""))
            )
        ]
        if len(missing) != przed_wetem:
            logger.info(
                "service_gaps: weto rodziny nazw odrzucilo %d/%d luk "
                "(podmiot ma usluge tej rodziny pod inna nazwa)",
                przed_wetem - len(missing), przed_wetem,
            )

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
    """Lazy-init LLM client dla taxonomy disambiguation. OpenAI-only —
    Gemini fallback path REMOVED (Google API key was suspended in May
    2026 and user has been on OpenAI exclusively since then; the legacy
    fallback would crash audits silently when Gemini quota errored).

    Returns None ONLY when OPENAI_API_KEY isn't configured, which is
    an environment misconfiguration — downstream callers must treat
    None as a hard error, not a graceful fallback to keyword mapping.
    """
    global _HIDDEN_LLM_CLIENT, _HIDDEN_LLM_TRIED
    if _HIDDEN_LLM_TRIED:
        return _HIDDEN_LLM_CLIENT
    _HIDDEN_LLM_TRIED = True

    import os
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        logger.error(
            "hidden_service_inference: OPENAI_API_KEY not configured. "
            "Pipeline will crash at the first call site that requires "
            "LLM disambiguation — this is an env misconfiguration, NOT a "
            "graceful-fallback condition."
        )
        _HIDDEN_LLM_CLIENT = None
        return _HIDDEN_LLM_CLIENT

    # OpenAI gpt-4o-mini — paid, reliable JSON output, ~$0.0002/call.
    # No try/except wrapper: an init failure is an environment problem
    # that must surface, not be swallowed.
    _HIDDEN_LLM_CLIENT = GeminiLLMClient(
        api_key=openai_key, model="gpt-4o-mini", provider="openai",
    )
    logger.info(
        "hidden_service_inference: using OpenAI gpt-4o-mini",
    )
    return _HIDDEN_LLM_CLIENT


async def _enhance_hidden_services_with_inference(
    hidden_services: list[dict[str, Any]],
    supabase: SupabaseService,
    *,
    tracer: "TraceWriter | None" = None,
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

    # get_service_embeddings is the only data source for the ANN candidate
    # lookup downstream. If it fails (RPC missing, network), the entire
    # inference pass collapses to keyword rules without the user knowing.
    # Let the exception propagate — caller already wraps in Bugsink alert.
    emb_map = await supabase.get_service_embeddings(service_ids)

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
    # Snapshot pre-call token counters so we can compute delta after the
    # batch and persist it as one `agent.tokens` trace row (mig 121).
    pre_input = llm.total_input_tokens if llm is not None else 0
    pre_output = llm.total_output_tokens if llm is not None else 0
    pre_calls = llm.total_calls if llm is not None else 0
    results: list[dict[str, Any]] = []
    if candidates_for_inference and llm is not None:
        results = await infer_hidden_services_batch(
            [c[1] for c in candidates_for_inference],
            supabase,
            llm,
            min_confidence=HIDDEN_MIN_CONFIDENCE,
        )
    if tracer is not None and llm is not None:
        try:
            delta_in = llm.total_input_tokens - pre_input
            delta_out = llm.total_output_tokens - pre_output
            delta_calls = llm.total_calls - pre_calls
            tracer.add(
                "agent.tokens",
                {
                    "step_name": "hidden_services.enrich",
                    "calls": delta_calls,
                    "model": llm.model,
                    "provider": llm.provider,
                    "input_tokens": delta_in,
                    "output_tokens": delta_out,
                    "candidate_count": len(candidates_for_inference),
                },
                tokens_used={
                    "input": delta_in,
                    "output": delta_out,
                    "model": llm.model,
                },
            )
        except Exception:
            logger.exception("agent.tokens trace add failed for hidden_services.enrich")

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

    # Sugestia identyczna z obecną nazwą = zero akcji dla właścicielki —
    # taki wpis w sekcji "ukryte usługi" to szum (audyt raportu 250:
    # "Lipoliza iniekcyjna" radziła zmienić nazwę na nią samą).
    przed_filtrem = len(hidden_services)
    hidden_services = [
        h for h in hidden_services
        if " ".join(str(h.get("suggested_name") or "").split()).lower()
        != " ".join(str(h.get("name") or "").split()).lower()
    ]
    if len(hidden_services) != przed_filtrem:
        logger.info(
            "hidden_services: odrzucono %d wpisow bez realnej zmiany nazwy",
            przed_filtrem - len(hidden_services),
        )

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
        "rule_0": 0,
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

    # ── Rule 0 (Stage-5 commit 2, 2026-05-17): historical anchor replay. ──
    # For each NULL-tid candidate, look up cross-audit anchors keyed by
    # (brand_marker, method_marker, body_area_set). Hits skip Rules
    # 2-4 entirely AND skip Pass 5 LLM cost. Read-only in this commit
    # — empty table = zero hits, zero cost.
    from services.taxonomy_anchors import (
        extract_anchor_key,
        lookup_anchors_bulk,
        apply_anchor_to_service,
    )
    anchor_min_confidence = getattr(
        settings, "taxonomy_anchor_min_confidence", 1,
    )
    candidate_services = [svc for _, svc in candidates]
    anchors_by_key = await lookup_anchors_bulk(
        supabase,
        candidate_services,
        min_confidence_count=anchor_min_confidence,
    )
    anchor_hits: list[tuple[int, dict[str, Any]]] = []
    if anchors_by_key:
        for idx, svc in list(candidates):
            key = extract_anchor_key(svc)
            anchor = anchors_by_key.get(key)
            if anchor is None:
                continue
            if apply_anchor_to_service(svc, anchor):
                stats["rule_0"] += 1
                anchor_hits.append((idx, svc))
                if trace_collector is not None:
                    trace_collector.append({
                        "svc_id": svc.get("id"),
                        "svc_name": svc.get("name"),
                        "original_tid": None,
                        "original_category": svc.get("category_name"),
                        "rule": "0",
                        "decision": "anchor_replay",
                        "details": {
                            "brand_marker": key[0],
                            "method_marker": key[1],
                            "body_area_set": key[2],
                            "tid_kind": anchor["tid_kind"],
                            "booksy_tid": anchor.get("booksy_tid"),
                            "synthetic_tid": anchor.get("synthetic_tid"),
                            "confidence_count": anchor.get("confidence_count"),
                            "status": anchor.get("status"),
                            "reasoning": anchor.get("reasoning"),
                        },
                        "embedding_top_k": [],
                        "llm_response": None,
                        "final": {
                            "booksy_tid": svc.get("booksy_treatment_id"),
                            "synthetic_tid": svc.get("synthetic_treatment_id"),
                            "taxonomy_source": "anchor_replay",
                            "treatment_name": svc.get("name"),
                        },
                    })
        # Remove anchor hits from the queue that goes to Rules 2-4.
        hit_ids = {svc.get("id") for _, svc in anchor_hits}
        candidates = [(i, s) for i, s in candidates if s.get("id") not in hit_ids]
    logger.info(
        "_resolve_service_taxonomy [%s] Rule 0: %d anchor replays "
        "(min_confidence=%d), %d candidates continue to Rules 2-4",
        label, stats["rule_0"], anchor_min_confidence, len(candidates),
    )
    if not candidates:
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
    if rule3_eligible and llm_client is None:
        # Rule 3 (Booksy LLM disambiguation) is REQUIRED when there are
        # eligible services. Silently pushing them all to Rule 4 produces
        # different taxonomy results without any signal in the report —
        # exactly the invisible failure mode we're closing. Bugsink alert.
        raise RuntimeError(
            f"_resolve_service_taxonomy [{label}] rule_3: "
            f"{len(rule3_eligible)} eligible services but no LLM client "
            "(OPENAI_API_KEY + GEMINI_API_KEY both failed init). Fix the "
            "key configuration; do not run pipeline in degraded mode."
        )

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
        # Area-gate Rule 4 too — top-1 embedding match can be wrong-area
        # for multi-area services. Same gate as Rule 3 (pre/post LLM).
        if match is not None:
            from services.body_area_taxonomy import (
                extract_body_areas, areas_compatible,
            )
            svc_areas = extract_body_areas(svc.get("name") or "")
            cand_areas = extract_body_areas(match.get("canonical_name") or "")
            if not areas_compatible(svc_areas, cand_areas):
                logger.info(
                    "_resolve_service_taxonomy [%s] rule_4: area-gate "
                    "REJECTED svc_id=%s svc_areas=%s vs cand=%r areas=%s — "
                    "falling through to Rule 1",
                    label, svc.get("id"), sorted(svc_areas),
                    match.get("canonical_name"), sorted(cand_areas),
                )
                if trace_collector is not None:
                    trace_collector.append({
                        "svc_id": svc.get("id"),
                        "svc_name": svc.get("name"),
                        "original_tid": None,
                        "original_category": svc.get("category_name"),
                        "rule": "4",
                        "decision": "skipped",
                        "details": {
                            "synthetic_id": match["id"],
                            "canonical_name": match.get("canonical_name"),
                            "similarity": match["similarity"],
                            "area_gate_rejection": True,
                            "svc_areas": sorted(svc_areas),
                            "cand_areas": sorted(cand_areas),
                            "falls_through_to": "1",
                        },
                        "embedding_top_k": [],
                        "llm_response": None,
                        "final": None,
                    })
                match = None  # force fall-through to Rule 1
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
        # Rule 1 (synthetic category creation) needs the LLM; without it
        # the services are silently dropped from the matrix entirely.
        # Bugsink alert and abort.
        raise RuntimeError(
            f"_resolve_service_taxonomy [{label}] rule_1: "
            f"{len(rule4_unmatched)} unmatched services but no LLM client. "
            "Silently skipping them would shrink the comparison matrix "
            "without any indication in the report — fix the key config "
            "instead."
        )
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


# Booksy ucina nazwy usług na listingu przy ~50 znakach (patrz
# _recover_full_name / reference_booksy_name_truncation). Sugerowana nazwa
# dłuższa niż limit poniżej i tak nie będzie widoczna w całości — a rada
# "zmień nazwę na 123-znakową" jest bezużyteczna dla właścicielki.
_SUGESTIA_MAX_ZNAKOW = 70


def _compose_suggested_name(prefix: str, current_name: str) -> str:
    """Compose human-readable suggested name from prefix + current name.

    Zasady (2026-08-29, po audycie raportu 250):
      - prefix duplikujący POCZĄTEK nazwy (nie tylko pierwsze słowo) →
        zwróć nazwę bez zmian ("Lipoliza iniekcyjna" + "Lipoliza
        iniekcyjna - ..." dawało sugestię identyczną z oryginałem),
      - całość > _SUGESTIA_MAX_ZNAKOW → utnij OPISOWY ogon nazwy na
        granicy segmentu " - ", a w ostateczności na granicy słowa;
        słowo kluczowe (prefix) zawsze zostaje w całości na początku.
    Trim leading symbols (✦, ⭕) from current_name so they don't pollute
    the new suggestion.
    """
    # Strip leading non-word symbols + spaces from current name
    cleaned = current_name.lstrip("✦⭕🔲💎⭐•· -*").strip()
    # Drop hyphen-prefixed leading separators
    if cleaned.startswith("- "):
        cleaned = cleaned[2:].strip()

    prefix = (prefix or "").strip()
    # Prefix już zawarty na początku nazwy → nazwa jest OK, nic nie doklejaj.
    if prefix and cleaned.lower().startswith(prefix.lower()):
        return cleaned
    # Stara heurystyka: to samo pierwsze słowo → nie dubluj.
    prefix_first_word = prefix.split()[0].lower() if prefix else ""
    cleaned_first_word = cleaned.split()[0].lower() if cleaned else ""
    if prefix_first_word and prefix_first_word == cleaned_first_word:
        return cleaned

    pelna = f"{prefix} {cleaned}".strip()
    if len(pelna) <= _SUGESTIA_MAX_ZNAKOW:
        return pelna

    # Za długo — tnij ogon nazwy segmentami " - " (opisowe dopiski salonu).
    budzet = _SUGESTIA_MAX_ZNAKOW - len(prefix) - 1
    segmenty = cleaned.split(" - ")
    rdzen = segmenty[0]
    for seg in segmenty[1:]:
        if len(rdzen) + 3 + len(seg) > budzet:
            break
        rdzen = f"{rdzen} - {seg}"
    if len(rdzen) > budzet:
        # Nawet pierwszy segment nie mieści się — tnij na granicy słowa.
        rdzen = rdzen[:budzet].rsplit(" ", 1)[0].rstrip(" -,")
    return f"{prefix} {rdzen}".strip()


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
    *,
    tracer: TraceWriter | None = None,
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

    # Per-competitor dimension values, only for counts_in_aggregates.
    # When `tracer` is set, also keep per-competitor breakdown so the trace
    # can answer "which competitors drove this market median".
    competitor_values_per_dim: dict[str, list[float]] = {
        dim: [] for _, dim, _, _ in DIMENSION_METADATA
    }
    competitor_breakdown_per_dim: dict[str, list[dict[str, Any]]] = {
        dim: [] for _, dim, _, _ in DIMENSION_METADATA
    }
    excluded_competitors: list[dict[str, Any]] = []
    for cand, cdata in aligned_competitors:
        if not cand.counts_in_aggregates:
            excluded_competitors.append(
                {
                    "salon_ref_id": cand.salon_id,
                    "name": cand.name,
                    "bucket": cand.bucket,
                    "reason": "counts_in_aggregates=False",
                }
            )
            continue
        cvals = compute_all_dimensions_for_salon(cdata)
        for dim_name, val in cvals.items():
            if dim_name in competitor_values_per_dim:
                competitor_values_per_dim[dim_name].append(val)
                if tracer is not None:
                    competitor_breakdown_per_dim[dim_name].append(
                        {
                            "salon_ref_id": cand.salon_id,
                            "name": cand.name,
                            "bucket": cand.bucket,
                            "value": round(float(val), 2),
                        }
                    )

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
        if tracer is not None:
            # Each dimension trace captures the FULL inputs: subject value,
            # each competitor's contribution, the resulting market percentiles,
            # the subject's percentile rank, plus context (category, unit,
            # better_is_higher) so a reader can interpret without DIMENSION_METADATA.
            breakdown = competitor_breakdown_per_dim.get(dim, [])
            tracer.add(
                step="scoring.dimension_score",
                data={
                    "dimension": dim,
                    "category": category,
                    "unit": unit,
                    "better_is_higher": better_is_higher,
                    "sort_order": idx,
                    "subject_value": round(subject_val, 2),
                    "market_min": round(percentiles["market_min"], 2),
                    "market_p25": round(percentiles["market_p25"], 2),
                    "market_p50": round(percentiles["market_p50"], 2),
                    "market_p75": round(percentiles["market_p75"], 2),
                    "market_max": round(percentiles["market_max"], 2),
                    "subject_percentile": round(subject_pct, 2),
                    "competitor_count": len(market_vals),
                    "competitor_values": [round(v, 2) for v in market_vals],
                    # Per-competitor breakdown (cap 20 for size guard).
                    "competitor_breakdown": breakdown[:20],
                    "excluded_competitors_count": len(excluded_competitors),
                },
            )

    if tracer is not None:
        # Final summary trace: dimensions covered, excluded competitors,
        # subject percentile distribution. Useful for "did anything go
        # weird in scoring" smell-test.
        avg_subject_pct = (
            sum(r["subject_percentile"] for r in rows) / len(rows)
            if rows
            else 0.0
        )
        tracer.add(
            step="scoring.summary",
            data={
                "dimensions_count": len(rows),
                "report_id": report_id,
                "aligned_competitors_count": len(aligned_competitors),
                "competitors_counted_in_aggregates": sum(
                    1 for cand, _ in aligned_competitors if cand.counts_in_aggregates
                ),
                "excluded_competitors": excluded_competitors[:20],
                "subject_percentile_avg": round(avg_subject_pct, 2),
                "subject_percentile_above_75_count": sum(
                    1 for r in rows if r["subject_percentile"] > 75
                ),
                "subject_percentile_below_25_count": sum(
                    1 for r in rows if r["subject_percentile"] < 25
                ),
            },
        )

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Faza 8a — verified-match-count aggregation (2026-05-17)
# ─────────────────────────────────────────────────────────────────────────────

async def _aggregate_verified_match_counts(
    service: "SupabaseService",
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[Any, dict[str, Any]]],
) -> dict[int, int]:
    """Faza 8a: re-bucket konkurentów raportu wg POKRYCIA menu subjecta.

    Osobne wyszukiwanie bliźniaków w puli SAMYCH wybranych konkurentów
    (exact, bez crowd-outu przez 9k salonów z promienia 15 km, który w
    wycenie zostawia wybranym 1–4 wiersze). Koszyk zależy od UDZIAŁU
    pokrytych usług w CAŁYM menu subjecta — patrz
    ``pipelines.competitor_buckets``. Zwraca {competitor_salon_id:
    verified_match_count} (liczba pokrytych usług subjecta).

    Bezpieczniki (błąd w logu, re-bucket POMINIĘTY zamiast degradować
    wszystkich do 'excluded'): brak embeddingów subjecta NIGDZIE — ani na
    audit scrape (stare/świeże scrape'y sprzed inline-embeddingu lub przed
    catch-upem crona), ani na chain-head scrape tego samego salonu, którego
    próbujemy jako fallback (BEAUTY_AUDIT-gqul, patrz
    _fetch_subject_embeddings_with_chain_head_fallback) — zero pokryć u
    wszystkich (Qdrant bez wybranych / awaria searchu), pusty przekrój
    pokrycia z competitor_matches raportu (pomylona przestrzeń ID,
    BEAUTY_AUDIT-hx85).
    """
    subject_services = [
        s for s in (subject_data.get("services") or [])
        if s.get("is_active", True) and s.get("price_grosze") and s.get("id") is not None
    ]
    selected = {
        int(cand.booksy_id): int(cand.salon_id)
        for cand, _ in aligned_competitors
        if getattr(cand, "booksy_id", None) and getattr(cand, "salon_id", None) is not None
    }
    if not subject_services or not selected:
        return {}

    subject_ids = [int(s["id"]) for s in subject_services]
    # Fallback na chain-head scrape TEGO SAMEGO salonu, gdy audit scrape nie ma
    # jeszcze wektorów (świeży audyt przed catch-upem crona itd.) — patrz
    # compute_pricing_comparisons_v2 dla pełnego uzasadnienia (BEAUTY_AUDIT-gqul).
    # Tu, w przeciwieństwie do wyceny, brak wektorów NIGDZIE nie produkuje
    # fałszywie-kompletnego raportu — pomija tylko jeden krok weryfikacji
    # (bucket_pre_verify zostaje), więc log+skip (nie wyjątek) zostaje właściwą
    # reakcją nawet po nieudanym fallbacku.
    subject_services, subject_ids, subject_embeddings = (
        await _fetch_subject_embeddings_with_chain_head_fallback(
            service, subject_services, subject_ids, subject_data.get("booksy_id"),
        )
    )
    if not subject_embeddings:
        logger.error(
            "Faza 8a: 0/%d usług subjecta ma name_embedding (raport %s), i "
            "chain-head fallback też pusty (scrape sprzed inline-embeddingu, "
            "brak chain-head scrape'a, albo błąd ingestu) — pomijam re-bucket.",
            len(subject_ids), report_id,
        )
        return {}

    # Sync klient Qdrant, ~15 s dla 221 usług — poza event loopem.
    clusters = await asyncio.to_thread(
        search_twins,
        subject_ids, list(selected), subject_embeddings=subject_embeddings,
        limit=_BUCKET_SEARCH_LIMIT, min_similarity=BUCKET_MIN_SIMILARITY, exact=True,
    )
    coverage = coverage_by_salon(clusters, selected, BUCKET_MIN_SIMILARITY)
    subject_total = len(subject_embeddings)
    covered_ids = {sid for sid, svcs in coverage.items() if svcs}
    if not covered_ids:
        logger.error(
            "Faza 8a: 0 pokryć u WSZYSTKICH %d wybranych (raport %s, %d usług "
            "subjecta, sim>=%.2f) — wybrani nie są w Qdrant albo search padł. "
            "Pomijam re-bucket zamiast degradować wszystkich do 'excluded'.",
            len(selected), report_id, subject_total, BUCKET_MIN_SIMILARITY,
        )
        return {}
    assignments = assign_coverage_buckets(coverage, subject_total)

    existing_matches = await service.get_competitor_matches(report_id)
    # BEAUTY_AUDIT-ripy: na pierwszym przebiegu Fazy 8a kolumna
    # bucket_pre_verify jest jeszcze NULL, więc 'bucket' faktycznie trzyma
    # przed-weryfikacyjną klasyfikację composite_score_v2 — bierzemy ją.
    # Na KOLEJNYM przebiegu 'bucket' trzyma już wynik POprzedniej weryfikacji,
    # więc gdyby użyć go znów, pierwotny zamiar (opisany w migracji 081 jako
    # "Preserved for audit + debugging") ginie bezpowrotnie. Dlatego wartość
    # już zapisaną w bucket_pre_verify traktujemy jako źródło prawdy i tylko
    # gdy jej brak (pierwszy przebieg) sięgamy po bieżący 'bucket'.
    bucket_pre_verify: dict[int, str] = {}
    for m in existing_matches:
        sid = m.get("competitor_salon_id")
        if sid is None:
            continue
        try:
            sid_int = int(sid)
        except (TypeError, ValueError):
            continue
        bucket_pre_verify[sid_int] = m.get("bucket_pre_verify") or m.get("bucket") or "unknown"

    if bucket_pre_verify and not (covered_ids & bucket_pre_verify.keys()):
        logger.error(
            "Faza 8a: pokrycie (%d salonów) nie przecina się z competitor_matches "
            "raportu %s (%d konkurentów) — podejrzenie pomylonej przestrzeni ID. "
            "Pomijam re-bucket zamiast degradować wszystkich do 'excluded'.",
            len(covered_ids), report_id, len(bucket_pre_verify),
        )
        return {}

    updates: list[dict[str, Any]] = []
    for m in existing_matches:
        sid_raw = m.get("competitor_salon_id")
        if sid_raw is None:
            continue
        try:
            sid = int(sid_raw)
        except (TypeError, ValueError):
            continue
        a = assignments.get(sid, EMPTY_ASSIGNMENT)
        updates.append({
            "id": m.get("id"),
            "verified_match_count": a.covered,
            "bucket_pre_verify": bucket_pre_verify.get(sid),
            "bucket": a.bucket,
            # 'excluded' zostaje w DB, ale wypada z agregatów i competitorProfiles.
            "counts_in_aggregates": a.bucket != "excluded",
        })

    try:
        await service.update_competitor_matches_verify_buckets(report_id, updates)
    except Exception:
        # BEAUTY_AUDIT-63ky: update_competitor_matches_verify_buckets nie
        # łapie wyjątku z PATCH-a PostgREST (docstring: "Raises on first
        # failure — no partial updates") — np. CHECK na kolumnie 'bucket'
        # odrzuca wartość, której produkcyjny schemat jeszcze nie dopuszcza.
        # Bez tego safeguardu wyjątek uciekał z tej funkcji i wysadzał CAŁY
        # raport konkurencji, nie tylko re-bucketing. Traktujemy to jak
        # trzy inne tryby awarii w tej funkcji (brak embeddingów, zero
        # pokryć, przekrój pusty): re-bucket pominięty, raport idzie dalej.
        logger.error(
            "Faza 8a: zapis re-bucketingu odrzucony przez bazę (raport %s, "
            "%d aktualizacji) — pomijam re-bucket, poprzednie koszyki "
            "zostają.",
            report_id, len(updates), exc_info=True,
        )
        return {}

    logger.info(
        "Faza 8a: re-bucketed %d competitors (direct=%d, cluster=%d, "
        "aspirational=%d, excluded=%d) — max pokrycie %d z %d usług subjecta "
        "przy sim>=%.2f, exact search po %d wybranych",
        len(updates),
        sum(1 for u in updates if u["bucket"] == "direct"),
        sum(1 for u in updates if u["bucket"] == "cluster"),
        sum(1 for u in updates if u["bucket"] == "aspirational"),
        sum(1 for u in updates if u["bucket"] == "excluded"),
        max((a.covered for a in assignments.values()), default=0), subject_total,
        BUCKET_MIN_SIMILARITY, len(selected),
    )
    return {sid: a.covered for sid, a in assignments.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Faza 8b — package economics analysis (2026-05-17)
# ─────────────────────────────────────────────────────────────────────────────

# Heuristic patterns to surface subject packages BEFORE involving LLM.
# Each regex catches a different package marker — pakiety, multiplier
# notation ("3x", "5 +1"), monthly abonament, body-area bundles.
# Cosine similarity floor for matching a package to its single-session
# equivalent at the SAME salon. Higher than Faza 7's 0.55 because the
# same-salon space is much narrower — most tid-mates share core
# vocabulary ("Dermapen", "PRO XN", "Thunder"). At 0.55 we kept
# "Dermapen pakiet" vs "EstGen do zabiegu Dermapen" pair (preparation
# add-on) AND "PRO XN I stopień" vs "PRO XN III stopień" pair
# (different intensity tier of the same product line). 0.70 separates
# genuine variants of THE SAME service from cousins / different-tier
# products that share branding tokens.
_SINGLE_MATCH_MIN_SIM = 0.70

# Token-level guard against intensity / stage / tier mismatch. When the
# package name carries a Roman numeral level (I/II/III/IV) or arabic
# "stopień <N>", the single MUST carry the same level OR none.
# Catches "PRO XN I stopień" vs "PRO XN III stopień" pair that the
# embedding cosine accepts because the shared "PRO XN" + "(twarz +
# szyja)" tokens dominate the vector.
_INTENSITY_MARKERS = [
    (re.compile(r"\bstopień\s*([IVX]+|[1-9])\b", re.IGNORECASE), "stopień"),
    (re.compile(r"\b(I{1,3}|IV|V)\b\s*stopień", re.IGNORECASE), "stopień"),
    (re.compile(r"\bbasic\b", re.IGNORECASE), "tier"),
    (re.compile(r"\badvanced\b", re.IGNORECASE), "tier"),
    (re.compile(r"\bvip\b", re.IGNORECASE), "tier"),
    (re.compile(r"\bpremium\b", re.IGNORECASE), "tier"),
]


def _extract_intensity_marker(name: str) -> str | None:
    """Returns a stable string token representing the intensity tier
    detected in `name`, or None when no marker found.
    'stopień I' / 'I stopień' / 'stopień 1' → 'stopień_1'
    'Basic' → 'tier_basic'
    'VIP' → 'tier_vip'
    """
    if not name:
        return None
    nlow = name.lower()
    # Roman/arabic stopień detection
    m = re.search(r"\bstopień\s*([ivx]+|[1-9])\b", nlow)
    if not m:
        m = re.search(r"\b(i{1,3}|iv|v)\b\s*stopień", nlow)
    if m:
        raw = m.group(1)
        # Normalize roman to arabic 1-5
        roman_map = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5"}
        return "stopień_" + roman_map.get(raw, raw)
    for marker in ("basic", "advanced", "vip", "premium"):
        if re.search(rf"\b{marker}\b", nlow):
            return f"tier_{marker}"
    return None

_PACKAGE_HEURISTIC_PATTERNS = [
    re.compile(r"\bpakiet\b", re.IGNORECASE),
    re.compile(r"\babonament\b", re.IGNORECASE),
    re.compile(r"\bkarnet\b", re.IGNORECASE),
    re.compile(r"\bvoucher\b", re.IGNORECASE),
    re.compile(r"\bbon\b\s*\d+", re.IGNORECASE),
    # Quantity-bearing patterns require ≥2 — "1x", "1 zabieg", "1 sesja"
    # describe SINGLES (one body area, one session). Treating them as
    # packages causes false "BRAK REFERENCJI" rows in PackageHonesty
    # (e.g. "Onda 1 zabieg-1 obszar np. podbródek" was being flagged as
    # a package needing a single-service baseline at the same salon).
    re.compile(r"^\s*([2-9]|\d{2,})\s*x\b", re.IGNORECASE),       # "3x Red Touch"
    re.compile(r"\s([2-9]|\d{2,})\s*x\b", re.IGNORECASE),         # "Red Touch 3x"
    re.compile(r"\b([2-9]|\d{2,})\s*zabieg(?:ów|i|y)?\b", re.IGNORECASE),
    re.compile(r"\b\d+\s*\+\s*\d+\s*zabieg", re.IGNORECASE),  # "5 + 1 zabieg" — always ≥2 total
    re.compile(r"\b([2-9]|\d{2,})\s*sesj", re.IGNORECASE),
    re.compile(r"\b([2-9]|\d{2,})\s*wizyt", re.IGNORECASE),
]

_EXPLICIT_PACKAGE_KEYWORDS = re.compile(
    r"\b(?:pakiet|abonament|karnet|voucher|bon)\b", re.IGNORECASE
)


def _detect_session_count_from_name(name: str) -> int:
    """Best-effort session count extractor. Returns 1 for unrecognised
    patterns so downstream economics math is conservative (assumes one
    session when in doubt, so discount % is computed honestly).

    All regex groups capture digits only (`\\d+`) so int() conversion
    cannot fail — any exception in this function is a real bug, not
    something to swallow. NO try/except.
    """
    if not name:
        return 1
    # 5 + 1 zabieg → 6 sessions
    m = re.search(r"\b(\d+)\s*\+\s*(\d+)\s*zabieg", name, re.IGNORECASE)
    if m:
        return int(m.group(1)) + int(m.group(2))
    # 3x | 5x | 10x | "pakiet 5x" | "Red Touch 3x"
    m = re.search(r"\b(\d+)\s*x\b", name, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 2 <= n <= 30:
            return n
    # "3 zabiegi", "5 zabiegów"
    m = re.search(r"\b(\d+)\s*zabieg(?:ów|i|y)?\b", name, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 2 <= n <= 30:
            return n
    # "5 sesji"
    m = re.search(r"\b(\d+)\s*sesj", name, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 2 <= n <= 30:
            return n
    return 1


def _detect_area_count_from_name(name: str) -> int:
    """Count distinct body-area mentions joined by " + " or "/" — used
    when the package is a bundle (e.g. "twarz + szyja + dekolt" = 3
    areas). Also catches explicit "N obszar(y|ów)" markers used by some
    salons instead of bundle notation (e.g. "Onda 1 zabieg- 2 obszary").
    Returns 1 when no obvious bundle pattern.
    """
    if not name:
        return 1
    # "twarz + szyja + dekolt"  → 3 segments
    n_plus = name.count(" + ")
    if n_plus >= 1:
        return min(n_plus + 1, 5)
    # "2 obszary", "3 obszarów", "1 obszar" — explicit count marker.
    # Regex captures digits only, so int() cannot fail — any exception
    # here is a real bug, NOT something to swallow.
    m = re.search(r"\b(\d+)\s*obszar(?:y|ów|u)?\b", name, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 10:
            return n
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
        # Defense in depth: if the heuristic flagged this row as a package
        # but the unit math says 1×1=1 AND there's no explicit
        # pakiet/abonament/karnet/voucher/bon keyword, it's a single being
        # misclassified (e.g. "Onda 1 zabieg-1 obszar"). Skip so it doesn't
        # produce a "BRAK REFERENCJI" row in PackageHonesty — but LOG the
        # skip so silent drops show up in worker logs / Bugsink breadcrumbs.
        if units == 1 and not _EXPLICIT_PACKAGE_KEYWORDS.search(name):
            logger.info(
                "_analyze_subject_packages: skipping svc_id=%s name=%r — "
                "heuristic flagged as package but units=1×1=1 and no "
                "explicit pakiet/abonament/karnet/voucher/bon keyword "
                "(treating as single, indexed in singles_by_key instead)",
                svc.get("id"), name,
            )
            continue
        per_session_grosze = price_grosze // units

        # Find best single at same salon: same tid_key + name embedding
        # cosine to package ≥ _SINGLE_MATCH_MIN_SIM. The embedding gate is
        # essential — without it the deterministic match picks the
        # cheapest service under the same tid (e.g. "EstGen do zabiegu
        # Dermapen na 1 obszar" 150zł as the "single" for "Dermapen 4 -
        # pakiet 3 zabiegów" 1500zł — EstGen is just the mask preparation
        # add-on, not a full Dermapen single). Empirically 0.55 separates
        # genuine variants from add-ons / different-procedure services.
        key = _tid_key(svc)
        single_match: dict[str, Any] | None = None
        pkg_emb = svc.get("name_embedding") or svc.get("name_embedding_dense")
        if key is not None:
            raw_candidates = singles_by_key.get(key) or []
            # Filter by embedding similarity FIRST so we only ever rank
            # genuine variants. Services without embedding fall through
            # (rare — name_embedding is populated at ingest for chain
            # heads). Score each kept candidate so we can use similarity
            # as a tiebreaker against duration / price.
            #
            # Also enforce an intensity-marker match: when the package
            # name says "stopień I" / "Basic" / "VIP", the single must
            # carry the same marker OR none. "PRO XN I stopień" vs
            # "PRO XN III stopień" share too much vocabulary for the
            # cosine to reject them — but they're DIFFERENT product
            # tiers. Same applies to Basic/Advanced/VIP/Premium lines.
            pkg_intensity = _extract_intensity_marker(name)
            filtered_candidates: list[tuple[dict[str, Any], float]] = []
            for cand in raw_candidates:
                cand_intensity = _extract_intensity_marker(
                    cand.get("name") or ""
                )
                if (
                    pkg_intensity
                    and cand_intensity
                    and pkg_intensity != cand_intensity
                ):
                    # Different stopień / tier — definitely not same single.
                    continue
                cand_emb = (
                    cand.get("name_embedding")
                    or cand.get("name_embedding_dense")
                )
                if pkg_emb is None or cand_emb is None:
                    # Conservative: include but with neutral score.
                    filtered_candidates.append((cand, 0.5))
                    continue
                sim = compute_name_embedding_similarity(pkg_emb, cand_emb)
                if sim is None:
                    continue
                if sim < _SINGLE_MATCH_MIN_SIM:
                    continue
                filtered_candidates.append((cand, float(sim)))
            if not filtered_candidates:
                single_match = None
            else:
                # Highest similarity first; break ties by closest
                # duration to the package's per-session duration when
                # provided, else closest price to the per-session
                # estimate.
                target_duration = svc.get("duration_minutes")
                if target_duration:
                    filtered_candidates.sort(
                        key=lambda pair: (
                            -pair[1],
                            abs(
                                (pair[0].get("duration_minutes") or 0)
                                - target_duration
                            ),
                        ),
                    )
                else:
                    filtered_candidates.sort(
                        key=lambda pair: (
                            -pair[1],
                            abs(
                                (pair[0].get("price_grosze") or 0)
                                - per_session_grosze
                            ),
                        ),
                    )
                single_match = filtered_candidates[0][0]

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
            elif discount_pct <= -50.0:
                # Final sanity guard. A discount below -50% almost always
                # signals a misclassified single match — genuine bundles
                # rarely cost twice the single. e.g. "Dermapen pakiet 3
                # zabiegów" 1500zł matched to "EstGen do zabiegu Dermapen"
                # 150zł produces -233%, but EstGen is the mask preparation
                # add-on, not the single-session Dermapen. Demote to
                # no_single_match with diagnostic reasoning rather than
                # surface the bogus -233% to the user.
                verdict = "no_single_match"
                discount_pct = None
                reasoning = (
                    f"Heurystyka znalazła {single_name!r} jako pojedynczy "
                    f"odpowiednik, ale różnica cen jest zbyt skrajna "
                    f"({single_price_g/100:.0f} zł vs {per_session_grosze/100:.0f} "
                    f"zł per zabieg w pakiecie). Najprawdopodobniej to inna "
                    f"usługa (np. preparat/maska/zabieg dodatkowy), nie "
                    f"single-session wersja pakietu. Wymaga manualnej "
                    f"weryfikacji lub LLM-confirm w v2."
                )
                single_id = None
                single_name = None
                single_price = None
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
