"""BAGENT #1 pipeline — report + content optimization.

Single responsibility: diagnose the scraped pricelist and produce content
suggestions (naming, descriptions, SEO keywords, quick wins, transformations).
Writes to Supabase tables: audit_reports, audit_issues, audit_transformations,
audit_seo_keywords, audit_quick_wins.

Does NOT generate a new pricelist (that's BAGENT #2 in cennik.py) and does NOT
fetch competitor context (that's BAGENT #3 in summary.py, post competitor
selection).

Webhook callbacks:
    POST Convex /api/audit/report/progress (during pipeline)
    POST Convex /api/audit/report/complete (on success)
    POST Convex /api/audit/report/fail    (on failure after retries)
"""

from __future__ import annotations

import inspect
import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from services.json_repair import parse_llm_json

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def _fill_prompt(template: str, **kwargs: str) -> str:
    """Replace {var_name} placeholders in prompt template. Always strips {refinement_prefix}."""
    result = template.replace("{refinement_prefix}", "")
    for key, value in kwargs.items():
        result = result.replace(f"{{{key}}}", value)
    return result


def _normalize_item(item: Any) -> dict[str, Any] | None:
    """Normalize a tool call array item — MiniMax sometimes returns strings instead of dicts.

    Stringi przechodzą przez parse_llm_json (naprawa surowych \\n, polskich
    cytatów z prostym zamknięciem itd. — docs/FINDINGS-2026-06-11.md P0-1),
    nie przez gołe json.loads.
    """
    if isinstance(item, dict):
        return item
    if isinstance(item, str):
        parsed = parse_llm_json(item)
        if isinstance(parsed, dict):
            return parsed
    return None


def _normalize_lookup_key(name: str) -> str:
    """Normalize a service name for fuzzy transformation lookup.

    Duplicated from pipelines/cennik.py so report.py can build the same
    {original_name: transformation} map used by the deterministic finalize
    step in BAGENT #2. Strips pricelist row formatting that BAGENT #1
    sometimes accidentally includes in the `before` field
    (e.g. `"Laser CO2" | 1 500,00 zł | 45min |`).
    """
    if not name:
        return ""
    cleaned = name.split(" | ")[0]
    cleaned = cleaned.strip().strip('"').strip("'")
    return cleaned.lower().strip()


def _build_transformation_map(
    transformations: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str]]:
    """Build lookup maps from original name → new name / description.

    Duplicated from pipelines/cennik.py. Returns:
        (name_map, description_map) — both keyed by normalized lookup key
        (lowercase, stripped of pricelist row formatting).
    """
    name_map: dict[str, str] = {}
    desc_map: dict[str, str] = {}
    for t in transformations:
        raw = t.get("before") or t.get("serviceName") or ""
        original = _normalize_lookup_key(raw)
        if not original:
            continue
        ttype = t.get("type", "")
        after = t.get("after", "")
        if ttype == "name" and after:
            name_map[original] = after
        elif ttype == "description" and after:
            desc_map[original] = after
    return name_map, desc_map


def _apply_transformations_to_scraped(
    scraped_data: Any,
    transformations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a plain-dict pricelist with naming + description transformations
    applied mechanically. Used to feed the category restructuring agent a
    pre-cleaned view so it can propose structure on the final content.

    Mirrors the transformation application logic in pipelines/cennik.py
    Step 2 but stripped down — no provenance/canonical taxonomy bookkeeping
    because the report pipeline only needs a textual view of the pricelist
    for the category agent prompt.
    """
    name_map, desc_map = _build_transformation_map(transformations)

    categories_out: list[dict[str, Any]] = []
    for cat in scraped_data.categories:
        services_out: list[dict[str, Any]] = []
        for svc in cat.services:
            key = _normalize_lookup_key(svc.name)
            new_name = name_map.get(key, svc.name)
            new_desc = desc_map.get(key, svc.description)
            services_out.append({
                "name": new_name,
                "price": svc.price,
                "duration": svc.duration,
                "description": new_desc,
            })
        categories_out.append({"name": cat.name, "services": services_out})

    return {
        "salonName": scraped_data.salonName,
        "categories": categories_out,
    }


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


def _make_monotonic_progress(base: ProgressCallback) -> ProgressCallback:
    """Wrap a progress callback so the emitted percent never decreases.

    Phase 1 (scoring) and Phase 2 (agents) each run naming + description
    concurrently via asyncio.gather, with naming on a LOWER sub-range than
    description (naming 30→42, description 56→64). Emitted concurrently the raw
    percent regresses (e.g. 56→38); neither convex (updateProgress patches raw)
    nor the frontend (formatProgress only clamps to [0,100]) guards monotonicity,
    so the bar visibly jumps backward. We clamp the number here while always
    forwarding the MESSAGE, so per-phase status text still updates live.
    """
    state = {"last": 0}

    async def progress(pct: int, message: str) -> None:
        if pct < state["last"]:
            pct = state["last"]
        else:
            state["last"] = pct
        await base(pct, message)

    return progress


def _make_phase2_step_emitter(
    progress: ProgressCallback,
    lo: int = 58,
    hi: int = 74,
    cap: int = 24,
) -> Callable[[str, int, int], Awaitable[None]]:
    """Build a shared per-step progress emitter for the Phase 2 agent loops.

    Both the naming and description agents make many SEQUENTIAL MiniMax tool_use
    calls (~15-35s each; measured 202s/12 steps naming, 386s/11 steps desc). With
    no per-step progress the bar froze at ~40% for ~6 min. This emitter feeds
    both agents' steps into ONE counter that creeps the bar lo→hi across the
    whole phase (asyncio is single-threaded, so the bare += is race-free).
    """
    state = {"steps": 0}

    async def on_agent_step(label: str, step: int, count: int) -> None:
        state["steps"] += 1
        frac = min(state["steps"], cap) / cap
        pct = lo + int(frac * (hi - lo))
        await progress(
            pct,
            f"Agent {label}: krok {step} ({count} partii usług przetworzonych)...",
        )

    return on_agent_step


async def run_audit_pipeline(
    scraped_data: Any,
    audit_id: str,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Full audit analysis pipeline (10 steps)."""
    from agent.runner import run_agent_loop
    from agent.tools import DESCRIPTION_TOOL, NAMING_TOOL
    from config import settings
    from pipelines.helpers import (
        calculate_audit_stats,
        calculate_completeness_score,
        calculate_seo_score,
        calculate_ux_score,
        cap_audit_score,
    )
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    # Clamp progress monotonic (see _make_monotonic_progress): the concurrent
    # naming/description phases would otherwise make the frontend bar jump back.
    progress = _make_monotonic_progress(on_progress or _noop_progress)
    total_services = scraped_data.totalServices
    cat_count = len(scraped_data.categories)

    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()

    # ── Step 1: Stats ──
    logger.info("[%s] Step 1: Calculating statistics...", audit_id)
    await progress(10, f"Obliczanie statystyk ({total_services} usług, {cat_count} kategorii)...")
    t0 = time.time()
    stats = calculate_audit_stats(scraped_data)
    dt = int((time.time() - t0) * 1000)

    desc_count = stats["servicesWithDescription"]
    dur_count = stats["servicesWithDuration"]
    dupes = len(stats["duplicateNames"])
    await progress(15, f"Statystyki: {total_services} usług, {desc_count} z opisem, {dur_count} z czasem, {dupes} duplikatów ({dt}ms)")

    # ── Phase 1 — Diagnostic scoring (PARALLEL) ──
    # Produces issues WITHOUT running transformation agents. We need a stable
    # issue list before dispatching the naming / description agents so each
    # transformation can point back to the issue it resolves (Etap 2
    # traceability).
    import asyncio as _asyncio

    logger.info("[%s] Phase 1: Scoring naming, descriptions, structure in parallel...", audit_id)
    await progress(20, "Faza 1/2 — skorowanie: nazwy + opisy + struktura → MiniMax...")
    t0 = time.time()

    naming_scoring, desc_scoring, structure_result = await _asyncio.gather(
        _score_naming(client, scraped_data, stats, progress),
        _score_descriptions(client, scraped_data, stats, progress),
        _analyze_structure(client, scraped_data, stats),
    )

    dt = int((time.time() - t0) * 1000)
    n_score = naming_scoring["score"]
    d_score = desc_scoring["score"]
    s_score = structure_result["structureScore"]
    p_score = structure_result["pricingScore"]
    qw_count = len(structure_result.get("quickWins", []))
    seo_count = len(structure_result.get("missingSeoKeywords", []))
    await progress(55, (
        f"Skorowanie zakończone ({dt}ms): "
        f"Nazwy {n_score}/20, Opisy {d_score}/20, "
        f"Struktura {s_score}/15, Ceny {p_score}/15, "
        f"{qw_count} QW, {seo_count} SEO"
    ))

    # Assemble all_issues NOW so we can feed them to the agent loops as context.
    # Preserve insertion order within each source so downstream code that filters
    # by dimension gets deterministic indexing; severity sort below reshuffles
    # but we keep the pre-sort order as the source of truth for global indices.
    all_issues = (
        naming_scoring["issues"]
        + desc_scoring["issues"]
        + structure_result.get("issues", [])
    )
    severity_order = {"critical": 0, "major": 1, "minor": 2}
    all_issues.sort(key=lambda i: severity_order.get(i.get("severity", "minor"), 2))

    # ── Phase 2 — Transformation agents (PARALLEL), issues as context ──
    # Naming agent sees issues with dimension in (naming, structure).
    # Descriptions agent sees issues with dimension in (descriptions, seo).
    logger.info("[%s] Phase 2: Naming + descriptions agents with issues context...", audit_id)
    await progress(58, "Faza 2/2 — agenty transformacyjne z kontekstem problemów...")
    t0 = time.time()

    # Per-step progress so the bar creeps 58→74 during the long agent loops
    # instead of freezing at ~40% (see _make_phase2_step_emitter).
    _on_agent_step = _make_phase2_step_emitter(progress)

    naming_result, desc_result = await _asyncio.gather(
        _agent_naming(
            client, scraped_data, all_issues, run_agent_loop, NAMING_TOOL, progress,
            on_step=lambda step, count: _on_agent_step("nazwy", step, count),
        ),
        _agent_descriptions(
            client, scraped_data, all_issues, run_agent_loop, DESCRIPTION_TOOL, progress,
            on_step=lambda step, count: _on_agent_step("opisy", step, count),
        ),
    )

    dt = int((time.time() - t0) * 1000)
    n_transforms = len(naming_result["transformations"])
    d_transforms = len(desc_result["transformations"])
    n_linked = sum(
        1 for t in naming_result["transformations"] if t.get("causedByIssueGlobalIndex") is not None
    )
    d_linked = sum(
        1 for t in desc_result["transformations"] if t.get("causedByIssueGlobalIndex") is not None
    )
    naming_coverage = naming_result.get("coverage", {})
    desc_coverage = desc_result.get("coverage", {})
    await progress(75, (
        f"Agenty zakończone ({dt}ms): "
        f"Nazwy {n_transforms} transform. "
        f"({n_linked} z traceability, {naming_coverage.get('alreadyOptimal', 0)} OK, "
        f"{naming_coverage.get('totalChecked', 0)} sprawdzonych), "
        f"Opisy {d_transforms} transform. "
        f"({d_linked} z traceability, {desc_coverage.get('alreadyOptimal', 0)} OK, "
        f"{desc_coverage.get('totalChecked', 0)} sprawdzonych)"
    ))

    # ── Calculate scores ──
    completeness = calculate_completeness_score(stats)
    seo = calculate_seo_score(structure_result.get("missingSeoKeywords", []), stats)
    ux = calculate_ux_score(stats)

    score_breakdown = {
        "completeness": completeness,
        "naming": naming_scoring["score"],
        "descriptions": desc_scoring["score"],
        "structure": structure_result["structureScore"],
        "pricing": structure_result["pricingScore"],
        "seo": seo,
        "ux": ux,
    }
    total_score = sum(score_breakdown.values())

    critical_count = sum(1 for i in all_issues if i.get("severity") == "critical")
    major_count = sum(1 for i in all_issues if i.get("severity") == "major")
    original_score = total_score
    total_score = cap_audit_score(
        total_score=total_score,
        critical_count=critical_count,
        major_count=major_count,
        all_issues_count=len(all_issues),
    )
    cap_msg = f" (capped from {original_score})" if total_score != original_score else ""
    transformations = naming_result["transformations"] + desc_result["transformations"]

    # ── Phase 2.5: Hidden services detection (Booksy search visibility) ──
    # Salonowo "Thunder całe ciało" jest niewidoczny w wyszukiwarce Booksy
    # bo Booksy ranks po nazwie usługi, nie po opisie ani kategorii.
    # Empirycznie potwierdzone (Beauty4ever, report 34) + 28% populacji
    # dotknięte (15 634 salonów). Z _enhance_hidden_services_with_inference
    # dostajemy LLM-anchored sugestię prefiksu z taksonomii Booksy.
    try:
        # Dedupe po nazwie — jeśli naming agent już zaproponował rename dla
        # tej usługi, pomijamy ją w hidden services (żeby UI nie pokazywało
        # dwóch sprzecznych transformacji dla jednej usługi). Naming agent
        # emituje transformacje bez scrape_service_id więc match po `before`.
        existing_transformed_names = {
            (t.get("before") or t.get("serviceName") or "").strip().lower()
            for t in transformations
            if t.get("type") == "name"
        }
        hidden_transformations, hidden_issue = await _detect_hidden_services_for_audit(
            scraped_data,
            supabase,
            skip_service_names=existing_transformed_names,
        )
        if hidden_transformations:
            # Append as a new global issue + linked transformations.
            issue_global_index = len(all_issues)
            all_issues.append(hidden_issue)
            for t in hidden_transformations:
                t["causedByIssueGlobalIndex"] = issue_global_index
            transformations.extend(hidden_transformations)
            llm_count = sum(
                1 for t in hidden_transformations if t.get("inferenceMethod") == "llm"
            )
            logger.info(
                "[%s] Hidden services: %d services flagged for renaming (%d via LLM)",
                audit_id, len(hidden_transformations), llm_count,
            )
    except Exception as exc:
        logger.warning(
            "[%s] hidden services detection failed (%s) — skipping section",
            audit_id, exc,
        )

    await progress(78, f"Score: {total_score}/100{cap_msg} | {critical_count} critical, {major_count} major | {len(transformations)} transformacji")

    # ── Step 6.5: Category restructuring (moved from BAGENT #2) ──
    # Build a transformed pricelist view by mechanically applying the naming
    # and description transformations on top of the original scrape. Feed that
    # to the category agent so it proposes structure on the cleaned content
    # rather than on raw scraped names. Output is persisted in the report dict
    # as categoryMapping + categoryChanges; cennik pipeline loads them
    # deterministically instead of re-running this agent.
    logger.info("[%s] Step 6.5: Category restructuring...", audit_id)
    await progress(80, "Restrukturyzacja kategorii...")
    t0 = time.time()
    from pipelines.category_restructure import restructure_categories
    transformed_pricelist = _apply_transformations_to_scraped(scraped_data, transformations)
    category_mapping, category_changes, restructure_status = await restructure_categories(
        client=client,
        transformed_pricelist=transformed_pricelist,
        top_issues=all_issues,
        audit_id=audit_id,
        on_progress=progress,
    )
    dt = int((time.time() - t0) * 1000)
    await progress(
        85,
        f"Kategorie gotowe: {len(category_mapping)} mapowań, {len(category_changes)} zmian ({dt}ms)"
        + (" — agent kategorii zawiódł, struktura bez zmian" if restructure_status == "failed" else ""),
    )

    # ── Step 7: Summary ──
    logger.info("[%s] Step 7: Generating summary...", audit_id)
    await progress(86, "Generowanie podsumowania audytu...")
    t0 = time.time()
    summary = await _generate_summary(client, total_score, stats, all_issues, scraped_data.salonName)
    dt = int((time.time() - t0) * 1000)
    await progress(87, f"Podsumowanie wygenerowane ({len(summary)} znaków, {dt}ms)")

    # ── Step 8: Industry benchmarks (WITHOUT competitor context) ──
    # Competitor context moved to BAGENT #3 (summary.py) which runs AFTER the user
    # explicitly picks competitors. BAGENT #1 only pulls city-level benchmarks for
    # industry comparison, nothing salon-specific.
    # Honest market position: score the audited salon on the SAME deterministic
    # 28-dim axis as the universe pool + competitor analysis, upsert it (so every
    # audit feeds the shared pool), then read fn_market_position — the mean of
    # direction-corrected per-dim percentiles vs the local market. No fabricated
    # avg+20 / score/top. Insufficient pool → marketPosition stays None (honest
    # absence, never a fake number). Independent of the pricing matrix.
    logger.info("[%s] Step 8: Market position...", audit_id)
    await progress(88, "Pozycja na tle rynku...")
    from pipelines.competitor_dimensional_scores import compute_all_dimensions_for_salon
    market_position: dict[str, Any] | None = None
    try:
        subject_full = await supabase.get_subject_full_data(audit_id)
        booksy_id = subject_full.get("booksy_id")
        dims = compute_all_dimensions_for_salon(subject_full)
        if booksy_id and dims:
            await supabase.upsert_salon_dimensional_score(
                booksy_id, dims,
                salon_ref_id=(subject_full.get("scrape") or {}).get("salon_ref_id"),
                city=scraped_data.salonCity,
                primary_category_id=scraped_data.primaryCategoryId,
                source="audit",
                scraped_at=(subject_full.get("scrape") or {}).get("scraped_at"),
            )
            market_position = await supabase.get_market_position(booksy_id)
        if market_position and market_position.get("status") == "ok":
            await progress(
                90,
                f"Pozycja rynkowa: {market_position.get('composite')} "
                f"(scope {market_position.get('scope')}, n={market_position.get('sampleSize')})",
            )
        else:
            await progress(90, "Za mało danych rynkowych dla tej okolicy")
    except Exception as e:
        logger.warning("[%s] Market position failed: %s", audit_id, e)
        await progress(90, "Pozycja rynkowa chwilowo niedostępna")

    # ── Step 9: Assemble report ──
    # No salonLocation, competitorContext, or competitors fields — those belong
    # to BAGENT #3 output in audit_summaries.basic_competitor_data.
    # categoryMapping + categoryChanges are the Etap 1 additions — used by
    # BAGENT #2 (cennik) as a deterministic input instead of re-running the
    # category agent loop there.
    await progress(93, "Składanie raportu końcowego...")
    # Ensure quickWins always has content — MiniMax sometimes returns empty.
    # Deterministic fallback: derive from top critical/major issues with actionable fixes.
    quick_wins = structure_result.get("quickWins", [])
    if not quick_wins and all_issues:
        for iss in all_issues:
            if len(quick_wins) >= 5:
                break
            severity = iss.get("severity", "minor")
            if severity in ("critical", "major") and iss.get("fix"):
                quick_wins.append({
                    "action": iss["fix"],
                    "effort": "low" if severity == "critical" else "medium",
                    "impact": "high" if severity == "critical" else "medium",
                    "example": iss.get("example", ""),
                    "affectedServices": iss.get("affectedCount", 0),
                })
        if quick_wins:
            logger.info("[%s] Generated %d deterministic quickWins from issues (MiniMax returned none)", audit_id, len(quick_wins))

    report: dict[str, Any] = {
        "version": "v2", "totalScore": total_score, "scoreBreakdown": score_breakdown,
        "stats": stats, "topIssues": all_issues, "transformations": transformations,
        "missingSeoKeywords": structure_result.get("missingSeoKeywords", []),
        "quickWins": quick_wins,
        "marketPosition": market_position,
        "summary": summary.strip(),
        "categoryMapping": category_mapping,
        "categoryChanges": category_changes,
        # FINDINGS P0-2: odróżnia "agent uznał, że zmian brak" (ok + puste)
        # od "agent się wywalił" (failed + puste) — frontend/cennik mogą
        # zakomunikować degradację zamiast udawać świadomą decyzję.
        "categoryRestructureStatus": restructure_status,
        # FINDINGS P1-7: skąd pochodzi każdy wynik scoringowy —
        # minimax | openai_gpt_4o_mini | degraded_default. degraded_default
        # oznacza, że score to stała zastępcza, nie realna ocena.
        "llmSources": {
            "naming": naming_scoring.get("llmSource", "minimax"),
            "descriptions": desc_scoring.get("llmSource", "minimax"),
            "structure": structure_result.get("llmSource", "minimax"),
        },
        # Etap 3 of Unified Report Pipeline: agent coverage stats. Each agent
        # now evaluates every service and reports either an actual change or
        # alreadyOptimal=true. Frontend renders this as
        # "Agent sprawdził X usług: poprawił Y, uznał Z za optymalne".
        "coverage": {
            "naming": naming_coverage,
            "descriptions": desc_coverage,
        },
    }

    quality = _validate_quality(report, scraped_data)
    if quality["isAcceptable"]:
        await progress(95, f"Walidacja jakości OK (score: {quality['score']:.0%})")
    else:
        failed_names = [c["name"] for c in quality["failedChecks"]]
        await progress(95, f"Walidacja jakości: FAILED ({', '.join(failed_names)})")
        logger.warning("[%s] Quality check failed: %s", audit_id, failed_names)

    await progress(100, f"Raport gotowy! Score: {total_score}/100, {len(all_issues)} problemów, {len(transformations)} transformacji")
    return report


# ── Private step functions ──
# ── Hidden services detection (BAGENT #1 — Booksy search visibility) ──


async def _detect_hidden_services_for_audit(
    scraped_data: Any,
    supabase: Any,
    skip_service_names: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Wykryj usługi w cenniku które są niewidoczne w wyszukiwarce Booksy
    (brand-only names typu "Thunder całe ciało" bez prefiksu typu "Depilacja").

    Reusuje detekcję + LLM inference z `pipelines.competitor_analysis`
    (centroidy `mv_booksy_treatment_centroids` + OpenAI gpt-4o-mini). Z każdej
    wykrytej usługi tworzy:
      - transformation entry: rename do nazwy z prefiksem kategorii Booksy
      - jedno zbiorcze issue (severity major gdy ≥5, inaczej minor)

    Returns (transformations_list, summary_issue).
    `skip_service_ids` pozwala pominąć usługi które już mają transformację
    z naming agent (dedupe — naming agent + hidden services nie powinny
    proponować dwóch przeciwstawnych renamów dla tej samej usługi).
    """
    from pipelines.competitor_analysis import (
        _detect_hidden_services,
        _enhance_hidden_services_with_inference,
    )

    # 1. Flatten scraped_data.categories[].services[] → dict format
    #    kompatybilny z _detect_hidden_services. Skip usługi które naming
    #    agent już renamował (dedup po lowercase name).
    flat: list[dict[str, Any]] = []
    skip_names = skip_service_names or set()
    for cat in scraped_data.categories:
        for svc in cat.services:
            sid = getattr(svc, "scrape_service_id", None)
            if sid is None:
                continue
            if (svc.name or "").strip().lower() in skip_names:
                continue
            flat.append({
                "id": sid,
                "name": svc.name,
                "description": svc.description or "",
                "price_grosze": getattr(svc, "price_grosze", None),
                "booksy_treatment_id": getattr(svc, "booksy_treatment_id", None),
                "is_active": True,
            })

    if not flat:
        return [], {}

    # 2. Detect hidden candidates (brand-only names z generic keyword w opisie).
    hidden = _detect_hidden_services(flat)
    if not hidden:
        return [], {}

    # 3. Enrich z LLM inference (centroidy + OpenAI gpt-4o-mini).
    hidden = await _enhance_hidden_services_with_inference(hidden, supabase)

    # 4. Build transformations — jedna per hidden service.
    transformations: list[dict[str, Any]] = []
    for h in hidden:
        prefix = h.get("suggested_prefix") or ""
        suggested = h.get("suggested_name") or h.get("name", "")
        method = h.get("inference_method") or "rule"
        confidence = h.get("inference_confidence")
        parent = h.get("parent_category")
        reasoning = h.get("inference_reasoning") or ""
        category_note = (
            f"Kategoria Booksy: {prefix}"
            + (f" → {parent}" if parent else "")
            + (f" (pewność {round((confidence or 0) * 100)}%)" if confidence is not None else "")
        )
        city = scraped_data.salonCity or "Warszawa"
        if prefix:
            reason = (
                f"Usługa jest niewidoczna w wyszukiwarce Booksy — klient szukający "
                f"„{prefix.lower()} {city}\" Twojej usługi nie zobaczy. {category_note}."
            )
        else:
            reason = (
                "Usługa nie ma w nazwie generycznej procedury — dodaj prefix kategorii Booksy."
            )
        transformations.append({
            "type": "name",
            "serviceName": h.get("name", ""),
            "before": h.get("name", ""),
            "after": suggested,
            "reason": reason,
            "impactScore": 9,  # High — directly impacts Booksy search visibility
            "scrape_service_id": h.get("service_id"),
            # Extra metadata (consumed by frontend audit results view if available)
            "inferenceMethod": method,
            "inferenceConfidence": confidence,
            "inferredTid": h.get("inferred_tid"),
            "parentCategory": parent,
            "inferenceReasoning": reasoning,
            "booksySearchVisibility": True,  # flag for UI segmentation
        })

    if not transformations:
        return [], {}

    # 5. Summary issue
    total = len(transformations)
    total_value_grosze = sum(
        (h.get("price_grosze") or 0) for h in hidden
    )
    total_value_pln = round(total_value_grosze / 100) if total_value_grosze else 0
    severity = "major" if total >= 5 else "minor"
    issue = {
        "severity": severity,
        "dimension": "naming",
        "issue": (
            f"{total} usług w Twoim cenniku jest niewidocznych w wyszukiwarce Booksy"
        ),
        "impact": (
            "Klient Booksy szuka po nazwie usługi — nazwy typu „Thunder całe ciało” "
            "czy „Modelka - ONDA” są zignorowane przez ranking. "
            f"Wartość ukrytych usług: ~{total_value_pln} zł."
        ),
        "affectedCount": total,
        "example": transformations[0].get("before", ""),
        "fix": (
            "Dla każdej z tych usług dopisz w nazwie prefix z kategorii Booksy "
            "(np. „Depilacja laserowa Thunder całe ciało”). Naprawa zajmuje "
            "1-2 min/usługę w panelu Booksy."
        ),
    }

    return transformations, issue


# ── Traceability helpers ──


def _build_issues_context_for_agent(
    all_issues: list[dict[str, Any]],
    dimensions: tuple[str, ...],
) -> tuple[str, list[int]]:
    """Filter the global issues list by dimension and build the prompt text
    + a filtered-index → global-index map.

    Returns:
        (numbered_text, global_index_by_filtered_index)
          numbered_text: "0. [severity] issue text\n1. [severity] ..."
          global_index_by_filtered_index[i] = position in all_issues of the
            issue rendered as index i in the prompt.

    If no issues match, returns ("(brak zgłoszonych problemów w tej kategorii — "
    "poprawiaj dla ogólnej jakości)", []).
    """
    global_index_by_filtered_index: list[int] = []
    lines: list[str] = []
    for g_idx, iss in enumerate(all_issues):
        if iss.get("dimension") not in dimensions:
            continue
        f_idx = len(global_index_by_filtered_index)
        severity = iss.get("severity", "minor")
        issue_text = iss.get("issue", "")
        lines.append(f"{f_idx}. [{severity}] {issue_text}")
        global_index_by_filtered_index.append(g_idx)

    if not lines:
        return (
            "(brak zgłoszonych problemów w tej kategorii — poprawiaj dla ogólnej jakości)",
            global_index_by_filtered_index,
        )
    return "\n".join(lines), global_index_by_filtered_index


def _resolve_caused_by_issue_index(
    raw_value: Any,
    global_index_map: list[int],
) -> int | None:
    """Translate an agent-reported filtered index into a global index into
    all_issues. Returns None when the value is missing, invalid, or out of
    range — per the plan we silently drop bad values instead of failing.
    """
    if raw_value is None:
        return None
    try:
        idx = int(raw_value)
    except (TypeError, ValueError):
        return None
    if idx < 0 or idx >= len(global_index_map):
        return None
    return global_index_map[idx]


# ── Phase 1: Scoring (produces issues, no transformations) ──


async def _generate_json_resilient(
    client: Any, prompt: str, system: str
) -> tuple[dict[str, Any], str]:
    """MiniMax first, OpenAI gpt-4o-mini second (FINDINGS P1-7).

    Returns (result, source) where source ∈ {minimax, openai_gpt_4o_mini}.
    Raises when BOTH providers fail — caller decides how to degrade.
    """
    try:
        return await client.generate_json(prompt, system=system), "minimax"
    except Exception as e:
        logger.warning(
            "MiniMax generate_json failed (%s) — trying OpenAI fallback", e
        )
        from services.openai_fallback import generate_json_via_openai

        result = await generate_json_via_openai(prompt, system=system)
        return result, "openai_gpt_4o_mini"


async def _score_naming(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    progress: ProgressCallback,
) -> dict[str, Any]:
    """Scoring-only half of the old _analyze_naming.

    Returns {score, issues}. No agent loop, no transformations.
    """
    from pipelines.helpers import build_full_pricelist_text

    pricelist_text = build_full_pricelist_text(scraped_data)
    await progress(22, f"Prompt naming score: {len(pricelist_text)} znaków → MiniMax...")

    naming_prompt = _load_prompt("naming_score.txt")
    if not naming_prompt:
        naming_prompt = (
            "Oceń JAKOŚĆ NAZW usług w poniższym cenniku. "
            "Skala 0-20. Zwróć JSON z polami: score (int), issues (array of objects "
            "z severity, dimension, issue, impact, affectedCount, example, fix).\n\n"
            "CENNIK:\n{pricelist_text}"
        )
    full_prompt = _fill_prompt(naming_prompt, pricelist_text=pricelist_text)

    t0 = time.time()
    try:
        scoring, llm_source = await _generate_json_resilient(
            client, full_prompt, system="Jesteś ekspertem od cenników salonów beauty."
        )
        dt = int((time.time() - t0) * 1000)
        await progress(
            28, f"Naming score otrzymany: {scoring.get('score', '?')}/20 ({dt}ms)"
        )
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Naming scoring failed (both providers): %s", e)
        await progress(28, f"Naming scoring FAILED ({dt}ms): {e}")
        scoring = {"score": 10, "issues": []}
        llm_source = "degraded_default"

    score = min(20, max(0, int(scoring.get("score", 10))))
    # Ensure every naming-scoring issue is tagged with dimension="naming" even
    # if the model forgot — downstream dimension filtering depends on it.
    issues = []
    for iss in scoring.get("issues", []) or []:
        if not isinstance(iss, dict):
            continue
        if "dimension" not in iss or not iss.get("dimension"):
            iss["dimension"] = "naming"
        issues.append(iss)
    return {"score": score, "issues": issues, "llmSource": llm_source}


async def _score_descriptions(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    progress: ProgressCallback,
) -> dict[str, Any]:
    """Scoring-only half of the old _analyze_descriptions."""
    from pipelines.helpers import build_full_pricelist_text

    pricelist_text = build_full_pricelist_text(scraped_data)
    desc_count = stats["servicesWithDescription"]
    total = stats["totalServices"]
    await progress(
        50,
        f"Prompt description score ({desc_count}/{total} z opisem): "
        f"{len(pricelist_text)} znaków → MiniMax...",
    )

    desc_prompt = _load_prompt("descriptions_score.txt")
    if not desc_prompt:
        desc_prompt = (
            "Oceń JAKOŚĆ OPISÓW usług. Skala 0-20. "
            "Zwróć JSON z polami: score (int), issues (array).\n\n"
        )
    full_prompt = (
        _fill_prompt(
            desc_prompt,
            pricelist_text=pricelist_text,
            descriptions_text=pricelist_text,
            total_services=str(total),
            desc_count=str(desc_count),
            desc_percentage=str(round(desc_count / max(total, 1) * 100)),
        )
        if "{" in desc_prompt
        else f"{desc_prompt}\n\nCENNIK:\n{pricelist_text}"
    )

    t0 = time.time()
    try:
        scoring, llm_source = await _generate_json_resilient(
            client, full_prompt, system="Jesteś ekspertem od cenników salonów beauty."
        )
        dt = int((time.time() - t0) * 1000)
        await progress(54, f"Description score: {scoring.get('score', '?')}/20 ({dt}ms)")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Description scoring failed (both providers): %s", e)
        await progress(54, f"Description scoring FAILED ({dt}ms): {e}")
        scoring = {"score": 10, "issues": []}
        llm_source = "degraded_default"

    score = min(20, max(0, int(scoring.get("score", 10))))
    issues = []
    for iss in scoring.get("issues", []) or []:
        if not isinstance(iss, dict):
            continue
        if "dimension" not in iss or not iss.get("dimension"):
            iss["dimension"] = "descriptions"
        issues.append(iss)
    return {"score": score, "issues": issues, "llmSource": llm_source}


# ── Phase 2: Agent loops (consume issues as context, emit transformations) ──


async def _agent_naming(
    client: Any,
    scraped_data: Any,
    all_issues: list[dict[str, Any]],
    run_agent_loop: Any,
    naming_tool: dict[str, Any],
    progress: ProgressCallback,
    on_step: Any = None,
) -> dict[str, Any]:
    """Agent-loop half of the old _analyze_naming.

    Feeds the agent a filtered issues context (dimension in naming/structure)
    so every transformation can point back to an issue via causedByIssueIndex.
    Translates filtered indices to global indices into all_issues before
    returning, so save_report can resolve them to DB IDs.
    """
    from pipelines.helpers import (
        build_full_pricelist_text,
        clean_service_name,
        fix_caps_lock,
        validate_name_transformation,
    )

    pricelist_text = build_full_pricelist_text(scraped_data)

    naming_issues_context, naming_global_map = _build_issues_context_for_agent(
        all_issues, ("naming", "structure")
    )

    naming_agent_prompt = _load_prompt("naming_agent.txt")
    if not naming_agent_prompt:
        naming_agent_prompt = (
            "Popraw nazwy usług w cenniku. Użyj narzędzia submit_naming_results. "
            "Wywołuj wielokrotnie z partiami po 15-20 nazw."
        )

    filled_prompt = _fill_prompt(
        naming_agent_prompt,
        pricelist_text=pricelist_text,
        naming_issues_context=naming_issues_context,
    )
    user_msg = (
        filled_prompt
        if "{pricelist_text}" in naming_agent_prompt
        else f"{filled_prompt}\n\nCENNIK:\n{pricelist_text}"
    )
    await progress(30, "Agent loop: poprawianie nazw usług (tool_use)...")

    transformations: list[dict[str, Any]] = []
    total_checked_count = 0
    optimized_count = 0
    already_optimal_count = 0
    rejected_count = 0
    t0 = time.time()

    async def _step(step: int, count: int) -> None:
        logger.info("[naming] Agent step %d, %d tool calls so far", step, count)
        if on_step is not None:
            r = on_step(step, count)
            if inspect.isawaitable(r):
                await r

    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od nazw usług beauty.",
            user_message=user_msg,
            tools=[naming_tool],
            max_steps=30,
            on_step=_step,
        )
        dt = int((time.time() - t0) * 1000)
        tokens = agent_result.total_input_tokens + agent_result.total_output_tokens
        await progress(
            38,
            f"Agent naming done: {agent_result.total_steps} steps, "
            f"{len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)",
        )

        for tc in agent_result.tool_calls:
            if tc.name == "submit_naming_results":
                raw = tc.input.get("transformations", [])
                for raw_t in raw:
                    t = _normalize_item(raw_t)
                    if t is None:
                        rejected_count += 1
                        logger.warning(
                            "Naming: skipping non-dict item: %s", type(raw_t).__name__
                        )
                        continue
                    original = t.get("name", "")
                    improved_raw = t.get("improved", "")
                    if not original:
                        rejected_count += 1
                        continue
                    total_checked_count += 1
                    already_optimal = bool(t.get("alreadyOptimal"))
                    improved = clean_service_name(improved_raw)
                    improved = fix_caps_lock(improved)

                    # Path 1: explicit "already optimal" verdict — count it,
                    # do NOT create a transformation, do NOT validate.
                    if already_optimal:
                        already_optimal_count += 1
                        continue

                    # Path 2: agent silently proposed the same name without
                    # alreadyOptimal flag. Treat as "no change wanted" — skip.
                    if not improved or original == improved:
                        # Not a rejection, not optimal, just a no-op.
                        continue

                    # Path 3: actual proposed change — validate and persist.
                    if validate_name_transformation(original, improved):
                        global_idx = _resolve_caused_by_issue_index(
                            t.get("causedByIssueIndex"), naming_global_map
                        )
                        transformations.append({
                            "type": "name",
                            "serviceName": original,
                            "before": original,
                            "after": improved,
                            "reason": "Poprawa nazwy usługi",
                            "impactScore": 3,
                            "causedByIssueGlobalIndex": global_idx,
                        })
                        optimized_count += 1
                    else:
                        rejected_count += 1
        await progress(
            42,
            (
                f"Naming: {optimized_count} poprawionych, "
                f"{already_optimal_count} potwierdzonych OK "
                f"({rejected_count} odrzuconych)"
            ),
        )
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Naming agent loop failed: %s", e)
        await progress(42, f"Naming agent FAILED ({dt}ms): {e}")

    return {
        "transformations": transformations,
        "coverage": {
            "totalChecked": total_checked_count,
            "optimized": optimized_count,
            "alreadyOptimal": already_optimal_count,
            "rejected": rejected_count,
        },
    }


async def _agent_descriptions(
    client: Any,
    scraped_data: Any,
    all_issues: list[dict[str, Any]],
    run_agent_loop: Any,
    description_tool: dict[str, Any],
    progress: ProgressCallback,
    on_step: Any = None,
) -> dict[str, Any]:
    """Agent-loop half of the old _analyze_descriptions."""
    from pipelines.helpers import build_full_pricelist_text

    pricelist_text = build_full_pricelist_text(scraped_data)

    desc_issues_context, desc_global_map = _build_issues_context_for_agent(
        all_issues, ("descriptions", "seo")
    )

    desc_agent_prompt = _load_prompt("descriptions_agent.txt")
    if not desc_agent_prompt:
        desc_agent_prompt = (
            "Przepisz opisy usług. Użyj submit_description_results. "
            "Partiami po 15-20."
        )

    filled_prompt = _fill_prompt(
        desc_agent_prompt,
        pricelist_text=pricelist_text,
        descriptions_issues_context=desc_issues_context,
    )
    user_msg = (
        filled_prompt
        if "{pricelist_text}" in desc_agent_prompt
        else f"{filled_prompt}\n\nCENNIK:\n{pricelist_text}"
    )
    await progress(56, "Agent loop: poprawianie opisów usług (tool_use)...")

    # Build a lookup of original descriptions so we can detect
    # alreadyOptimal=false items where the agent didn't actually change
    # anything (no-op) and skip them without creating a transformation.
    original_desc_by_name: dict[str, str] = {}
    for cat in scraped_data.categories:
        for svc in cat.services:
            if svc.name:
                original_desc_by_name[svc.name.strip().lower()] = (
                    svc.description or ""
                )

    transformations: list[dict[str, Any]] = []
    total_checked_count = 0
    optimized_count = 0
    already_optimal_count = 0
    rejected_count = 0
    t0 = time.time()

    async def _step(step: int, count: int) -> None:
        logger.info("[descriptions] Agent step %d, %d tool calls so far", step, count)
        if on_step is not None:
            r = on_step(step, count)
            if inspect.isawaitable(r):
                await r

    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od opisów usług beauty.",
            user_message=user_msg,
            tools=[description_tool],
            max_steps=30,
            on_step=_step,
        )
        dt = int((time.time() - t0) * 1000)
        tokens = agent_result.total_input_tokens + agent_result.total_output_tokens
        await progress(
            62,
            f"Agent descriptions done: {agent_result.total_steps} steps, "
            f"{len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)",
        )

        for tc in agent_result.tool_calls:
            if tc.name == "submit_description_results":
                raw = tc.input.get("transformations", [])
                for raw_t in raw:
                    t = _normalize_item(raw_t)
                    if t is None:
                        rejected_count += 1
                        logger.warning(
                            "Description: skipping non-dict item: %s",
                            type(raw_t).__name__,
                        )
                        continue
                    service_name = t.get("serviceName", "")
                    new_desc = t.get("newDescription", "")
                    if not service_name:
                        rejected_count += 1
                        continue
                    total_checked_count += 1
                    already_optimal = bool(t.get("alreadyOptimal"))

                    # Path 1: explicit "already optimal" verdict.
                    if already_optimal:
                        already_optimal_count += 1
                        continue

                    # Path 2: no description payload — treat as rejected.
                    if not new_desc:
                        rejected_count += 1
                        continue

                    # Path 3: agent returned a description identical to the
                    # original — no-op, skip silently.
                    original_desc = original_desc_by_name.get(
                        service_name.strip().lower(), ""
                    )
                    if (
                        original_desc
                        and new_desc.strip() == original_desc.strip()
                    ):
                        continue

                    # Path 4: actual proposed change — persist.
                    global_idx = _resolve_caused_by_issue_index(
                        t.get("causedByIssueIndex"), desc_global_map
                    )
                    transformations.append({
                        "type": "description",
                        "serviceName": service_name,
                        "before": "",
                        "after": new_desc,
                        "reason": "Nowy/poprawiony opis",
                        "impactScore": 2,
                        "causedByIssueGlobalIndex": global_idx,
                    })
                    optimized_count += 1
        await progress(
            64,
            (
                f"Opisy: {optimized_count} poprawionych, "
                f"{already_optimal_count} potwierdzonych OK "
                f"({rejected_count} odrzuconych)"
            ),
        )
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Description agent loop failed: %s", e)
        await progress(64, f"Description agent FAILED ({dt}ms): {e}")

    return {
        "transformations": transformations,
        "coverage": {
            "totalChecked": total_checked_count,
            "optimized": optimized_count,
            "alreadyOptimal": already_optimal_count,
            "rejected": rejected_count,
        },
    }


async def _analyze_structure(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
) -> dict[str, Any]:
    """Step 6: Structure & pricing analysis."""
    from pipelines.helpers import build_pricelist_summary

    summary_text = build_pricelist_summary(scraped_data)

    structure_prompt = _load_prompt("structure.txt")
    if not structure_prompt:
        structure_prompt = (
            "Oceń STRUKTURĘ (0-15) i CENY (0-15). "
            "Podaj WSZYSTKIE problemy, quick wins, SEO. "
            "Zwróć JSON z polami: structureScore, pricingScore, "
            "issues, quickWins, missingSeoKeywords.\n\n"
        )
    full_prompt = _fill_prompt(structure_prompt, structure_text=summary_text, pricelist_text=summary_text) if "{" in structure_prompt else f"{structure_prompt}\n\nCENNIK:\n{summary_text}"

    stats_context = (
        f"\nSTATYSTYKI:\n"
        f"- Liczba usług: {stats['totalServices']}\n"
        f"- Kategorie: {stats['totalCategories']}\n"
        f"- Średnia usług/kategorię: {stats['avgServicesPerCategory']}\n"
        f"- Duplikaty nazw: {len(stats['duplicateNames'])}\n"
        f"- Puste kategorie: {len(stats['emptyCategories'])}\n"
        f"- Przerośle kategorie (>20): {len(stats['oversizedCategories'])}\n"
    )
    full_prompt += stats_context

    try:
        result, llm_source = await _generate_json_resilient(
            client, full_prompt, system="Jesteś ekspertem od struktury cenników beauty."
        )
    except Exception as e:
        logger.warning("Structure analysis failed (both providers): %s", e)
        result = {}
        llm_source = "degraded_default"

    return {
        "structureScore": min(15, max(0, int(result.get("structureScore", 8)))),
        "pricingScore": min(15, max(0, int(result.get("pricingScore", 8)))),
        "issues": result.get("issues", []),
        "quickWins": result.get("quickWins", []),
        "missingSeoKeywords": result.get("missingSeoKeywords", []),
        "llmSource": llm_source,
    }


async def _generate_summary(
    client: Any,
    total_score: int,
    stats: dict[str, Any],
    issues: list[dict[str, Any]],
    salon_name: str | None,
) -> str:
    """Step 7: Generate human-readable summary."""
    summary_prompt = _load_prompt("summary.txt")
    if not summary_prompt:
        summary_prompt = "Napisz 2-3 zdania podsumowania audytu cennika salonu beauty."

    top_issues_text = "\n".join(
        f"- [{i.get('severity', 'minor')}] {i.get('issue', '')}" for i in issues[:5]
    )

    salon_context = f"Salon: {salon_name or 'Nieznany'}, Wynik: {total_score}/100"
    full_prompt = _fill_prompt(summary_prompt,
        salon_context=salon_context,
        total_score=str(total_score),
        total_services=str(stats['totalServices']),
        total_categories=str(stats['totalCategories']),
        services_with_desc=str(stats.get('servicesWithDescription', 0)),
        issues_summary=top_issues_text,
    ) if "{" in summary_prompt else (
        f"{summary_prompt}\n\n"
        f"SALON: {salon_name or 'Nieznany'}\n"
        f"WYNIK: {total_score}/100\n"
        f"USŁUGI: {stats['totalServices']}\n"
        f"KATEGORIE: {stats['totalCategories']}\n"
        f"GŁÓWNE PROBLEMY:\n{top_issues_text}"
    )

    try:
        return await client.generate_text(full_prompt, system="Jesteś ekspertem od audytów cenników beauty.")
    except Exception as e:
        logger.warning("Summary generation failed: %s — trying OpenAI fallback", e)
        try:
            from services.openai_fallback import generate_text_via_openai

            return await generate_text_via_openai(
                full_prompt, system="Jesteś ekspertem od audytów cenników beauty."
            )
        except Exception as e2:
            logger.warning("Summary OpenAI fallback also failed: %s", e2)
            return f"Cennik salonu {salon_name or 'Nieznany'} uzyskał {total_score}/100 punktów."


def _validate_quality(report: dict[str, Any], scraped_data: Any) -> dict[str, Any]:
    """Quality validation with 7 business checks."""
    checks: list[dict[str, Any]] = []
    total_services = report["stats"].get("totalServices", 0)

    score_ok = 0 <= report["totalScore"] <= 100
    checks.append({"name": "score_range", "weight": 2, "passed": score_ok})

    has_issues = len(report["topIssues"]) > 0 or report["totalScore"] >= 80
    checks.append({"name": "issues_present", "weight": 3, "passed": has_issues})

    trans_count = len(report["transformations"])
    trans_ok = trans_count <= total_services * 3 if total_services > 0 else trans_count == 0
    checks.append({"name": "transformations_reasonable", "weight": 2, "passed": trans_ok})

    breakdown = report["scoreBreakdown"]
    component_sum = sum(breakdown.values())
    sum_ok = abs(component_sum - report["totalScore"]) <= 5
    checks.append({"name": "score_consistency", "weight": 3, "passed": sum_ok})

    summary_ok = len(report.get("summary", "").strip()) > 10
    checks.append({"name": "summary_present", "weight": 1, "passed": summary_ok})

    input_service_count = sum(len(cat.services) for cat in scraped_data.categories)
    stats_ok = report["stats"].get("totalServices", 0) == input_service_count
    checks.append({"name": "stats_match", "weight": 2, "passed": stats_ok})

    critical_count = sum(1 for i in report["topIssues"] if i.get("severity") == "critical")
    inflation_ok = critical_count <= total_services * 0.5 if total_services > 0 else critical_count == 0
    checks.append({"name": "severity_not_inflated", "weight": 2, "passed": inflation_ok})

    total_weight = sum(c["weight"] for c in checks)
    passed_weight = sum(c["weight"] for c in checks if c["passed"])
    quality_score = passed_weight / total_weight if total_weight > 0 else 0

    return {
        "isAcceptable": quality_score >= 0.7,
        "score": quality_score,
        "failedChecks": [c for c in checks if not c["passed"]],
        "allChecks": checks,
    }
