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

import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

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
    """Normalize a tool call array item — MiniMax sometimes returns strings instead of dicts."""
    if isinstance(item, dict):
        return item
    if isinstance(item, str):
        try:
            parsed = json.loads(item)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
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
    )
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
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

    naming_result, desc_result = await _asyncio.gather(
        _agent_naming(client, scraped_data, all_issues, run_agent_loop, NAMING_TOOL, progress),
        _agent_descriptions(client, scraped_data, all_issues, run_agent_loop, DESCRIPTION_TOOL, progress),
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
    if critical_count >= 3 and total_score > 60:
        total_score = 60
    elif critical_count > 0 and total_score > 75:
        total_score = 75
    elif major_count > 0 and total_score > 85:
        total_score = 85
    elif all_issues and total_score > 95:
        total_score = 95

    cap_msg = f" (capped from {original_score})" if total_score != original_score else ""
    transformations = naming_result["transformations"] + desc_result["transformations"]
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
    category_mapping, category_changes = await restructure_categories(
        client=client,
        transformed_pricelist=transformed_pricelist,
        top_issues=all_issues,
        audit_id=audit_id,
        on_progress=progress,
    )
    dt = int((time.time() - t0) * 1000)
    await progress(
        85,
        f"Kategorie gotowe: {len(category_mapping)} mapowań, {len(category_changes)} zmian ({dt}ms)",
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
    logger.info("[%s] Step 8: Industry benchmarks...", audit_id)
    await progress(88, "Pobieranie benchmarków branżowych...")
    industry_comparison: dict[str, Any] = {
        "yourScore": total_score, "industryAverage": 52, "topPerformers": 78,
        "percentile": 50, "sampleSize": 500,
    }
    try:
        # Prefer the structured salon_city field from salon_scrapes
        # (populated by the Booksy parser). Fall back to address parsing
        # for pre-migration audits that only have audit_scraped_data.
        city = scraped_data.salonCity
        if not city and scraped_data.salonAddress:
            # Address format: "street, zip, city, [district]" — second-from-zip
            # is usually city. Simpler heuristic: split on comma, strip, and
            # pick the first segment that looks like a city (no digits).
            parts = [p.strip() for p in scraped_data.salonAddress.split(",")]
            for part in parts:
                if part and not any(ch.isdigit() for ch in part):
                    city = part
                    break
        benchmarks = await supabase.get_benchmarks(
            city=city,
            primary_category_id=scraped_data.primaryCategoryId,
        )
        avg = benchmarks.get("industry_average", 52)
        top = benchmarks.get("top_performers", 78)
        sample = benchmarks.get("sample_size", 500)
        percentile = min(99, max(1, round((total_score / max(top, 1)) * 100)))
        industry_comparison = {
            "yourScore": total_score, "industryAverage": avg, "topPerformers": top,
            "percentile": percentile, "sampleSize": sample,
        }
        await progress(90, f"Benchmarki: avg {avg}, top {top}, percentyl {percentile}")
    except Exception as e:
        logger.warning("[%s] Benchmark fetch failed: %s", audit_id, e)
        await progress(90, f"Błąd pobierania benchmarków: {e}")

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
        "industryComparison": industry_comparison,
        "summary": summary.strip(),
        "categoryMapping": category_mapping,
        "categoryChanges": category_changes,
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
        scoring = await client.generate_json(
            full_prompt, system="Jesteś ekspertem od cenników salonów beauty."
        )
        dt = int((time.time() - t0) * 1000)
        await progress(
            28, f"Naming score otrzymany: {scoring.get('score', '?')}/20 ({dt}ms)"
        )
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Naming scoring failed: %s", e)
        await progress(28, f"Naming scoring FAILED ({dt}ms): {e}")
        scoring = {"score": 10, "issues": []}

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
    return {"score": score, "issues": issues}


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
        scoring = await client.generate_json(
            full_prompt, system="Jesteś ekspertem od cenników salonów beauty."
        )
        dt = int((time.time() - t0) * 1000)
        await progress(54, f"Description score: {scoring.get('score', '?')}/20 ({dt}ms)")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Description scoring failed: %s", e)
        await progress(54, f"Description scoring FAILED ({dt}ms): {e}")
        scoring = {"score": 10, "issues": []}

    score = min(20, max(0, int(scoring.get("score", 10))))
    issues = []
    for iss in scoring.get("issues", []) or []:
        if not isinstance(iss, dict):
            continue
        if "dimension" not in iss or not iss.get("dimension"):
            iss["dimension"] = "descriptions"
        issues.append(iss)
    return {"score": score, "issues": issues}


# ── Phase 2: Agent loops (consume issues as context, emit transformations) ──


async def _agent_naming(
    client: Any,
    scraped_data: Any,
    all_issues: list[dict[str, Any]],
    run_agent_loop: Any,
    naming_tool: dict[str, Any],
    progress: ProgressCallback,
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
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od nazw usług beauty.",
            user_message=user_msg,
            tools=[naming_tool],
            max_steps=30,
            on_step=lambda step, count: logger.info(
                "[naming] Agent step %d, %d tool calls so far", step, count
            ),
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
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od opisów usług beauty.",
            user_message=user_msg,
            tools=[description_tool],
            max_steps=30,
            on_step=lambda step, count: logger.info(
                "[descriptions] Agent step %d, %d tool calls so far", step, count
            ),
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
        result = await client.generate_json(
            full_prompt, system="Jesteś ekspertem od struktury cenników beauty."
        )
    except Exception as e:
        logger.warning("Structure analysis failed: %s", e)
        result = {}

    return {
        "structureScore": min(15, max(0, int(result.get("structureScore", 8)))),
        "pricingScore": min(15, max(0, int(result.get("pricingScore", 8)))),
        "issues": result.get("issues", []),
        "quickWins": result.get("quickWins", []),
        "missingSeoKeywords": result.get("missingSeoKeywords", []),
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
        logger.warning("Summary generation failed: %s", e)
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
