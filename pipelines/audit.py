"""Audit report pipeline -- orchestrates 10-step analysis flow."""

from __future__ import annotations

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

    # ── Step 2-3: Naming ──
    logger.info("[%s] Step 2: Naming scoring...", audit_id)
    await progress(20, "Wysyłanie cennika do AI — ocena nazw...")
    t0 = time.time()
    naming_result = await _analyze_naming(client, scraped_data, stats, run_agent_loop, NAMING_TOOL, progress)
    dt = int((time.time() - t0) * 1000)
    n_score = naming_result["score"]
    n_issues = len(naming_result["issues"])
    n_transforms = len(naming_result["transformations"])
    await progress(45, f"Nazwy: {n_score}/20 pkt, {n_issues} problemów, {n_transforms} transformacji ({dt}ms)")

    # ── Step 4-5: Descriptions ──
    logger.info("[%s] Step 4: Description scoring...", audit_id)
    await progress(48, f"Wysyłanie do AI — ocena opisów ({desc_count}/{total_services} ma opis)...")
    t0 = time.time()
    desc_result = await _analyze_descriptions(client, scraped_data, stats, run_agent_loop, DESCRIPTION_TOOL, progress)
    dt = int((time.time() - t0) * 1000)
    d_score = desc_result["score"]
    d_issues = len(desc_result["issues"])
    d_transforms = len(desc_result["transformations"])
    await progress(65, f"Opisy: {d_score}/20 pkt, {d_issues} problemów, {d_transforms} transformacji ({dt}ms)")

    # ── Step 6: Structure & pricing ──
    logger.info("[%s] Step 6: Structure & pricing...", audit_id)
    await progress(68, "Wysyłanie do AI — analiza struktury i cen...")
    t0 = time.time()
    structure_result = await _analyze_structure(client, scraped_data, stats)
    dt = int((time.time() - t0) * 1000)
    s_score = structure_result["structureScore"]
    p_score = structure_result["pricingScore"]
    qw_count = len(structure_result.get("quickWins", []))
    seo_count = len(structure_result.get("missingSeoKeywords", []))
    await progress(75, f"Struktura: {s_score}/15, Ceny: {p_score}/15, {qw_count} quick wins, {seo_count} SEO ({dt}ms)")

    # ── Calculate scores ──
    completeness = calculate_completeness_score(stats)
    seo = calculate_seo_score(structure_result.get("missingSeoKeywords", []), stats)
    ux = calculate_ux_score(stats)

    score_breakdown = {
        "completeness": completeness,
        "naming": naming_result["score"],
        "descriptions": desc_result["score"],
        "structure": structure_result["structureScore"],
        "pricing": structure_result["pricingScore"],
        "seo": seo,
        "ux": ux,
    }
    total_score = sum(score_breakdown.values())

    all_issues = naming_result["issues"] + desc_result["issues"] + structure_result.get("issues", [])
    severity_order = {"critical": 0, "major": 1, "minor": 2}
    all_issues.sort(key=lambda i: severity_order.get(i.get("severity", "minor"), 2))

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

    # ── Step 7: Summary ──
    logger.info("[%s] Step 7: Generating summary...", audit_id)
    await progress(80, "Generowanie podsumowania audytu...")
    t0 = time.time()
    summary = await _generate_summary(client, total_score, stats, all_issues, scraped_data.salonName)
    dt = int((time.time() - t0) * 1000)
    await progress(84, f"Podsumowanie wygenerowane ({len(summary)} znaków, {dt}ms)")

    # ── Step 8: Competitors ──
    logger.info("[%s] Step 8: Competitor context...", audit_id)
    await progress(86, f"Szukanie konkurencji dla '{scraped_data.salonName or 'Nieznany'}'...")
    salon_location: dict[str, Any] | None = None
    competitor_context: dict[str, Any] | None = None
    competitors: list[dict[str, Any]] | None = None
    industry_comparison: dict[str, Any] = {
        "yourScore": total_score, "industryAverage": 52, "topPerformers": 78,
        "percentile": 50, "sampleSize": 500,
    }

    try:
        location = await supabase.geocode_salon(scraped_data.salonName or "", scraped_data.salonAddress or "")
        if location:
            salon_location = location
            await progress(88, f"Lokalizacja znaleziona: {location['lat']:.4f}, {location['lng']:.4f}")
            all_names = [s.name for cat in scraped_data.categories for s in cat.services]
            raw_competitors = await supabase.get_competitors(location["lat"], location["lng"], 10, all_names)
            if raw_competitors:
                competitors = raw_competitors
                competitor_context = {
                    "competitorsFound": len(raw_competitors), "radiusKm": 10,
                    "pricePositioning": "mid-range", "topServiceGaps": [], "dataFreshness": "recent",
                }
                await progress(89, f"Znaleziono {len(raw_competitors)} konkurentów w promieniu 10km")
            else:
                await progress(89, "Brak konkurentów w promieniu 10km")
        else:
            await progress(89, "Nie znaleziono lokalizacji salonu — pomijam konkurencję")

        city = scraped_data.salonAddress.split(",")[-1].strip() if scraped_data.salonAddress else None
        benchmarks = await supabase.get_benchmarks(city)
        avg = benchmarks.get("industry_average", 52)
        top = benchmarks.get("top_performers", 78)
        sample = benchmarks.get("sample_size", 500)
        percentile = min(99, max(1, round((total_score / max(top, 1)) * 100)))
        industry_comparison = {
            "yourScore": total_score, "industryAverage": avg, "topPerformers": top,
            "percentile": percentile, "sampleSize": sample,
        }
    except Exception as e:
        logger.warning("[%s] Competitor/benchmark context failed: %s", audit_id, e)
        await progress(89, f"Błąd pobierania konkurencji: {e}")

    # ── Step 9-10: Assemble ──
    await progress(92, "Składanie raportu końcowego...")
    report: dict[str, Any] = {
        "version": "v2", "totalScore": total_score, "scoreBreakdown": score_breakdown,
        "stats": stats, "topIssues": all_issues, "transformations": transformations,
        "missingSeoKeywords": structure_result.get("missingSeoKeywords", []),
        "quickWins": structure_result.get("quickWins", []),
        "industryComparison": industry_comparison, "competitorContext": competitor_context,
        "salonLocation": salon_location, "competitors": competitors, "summary": summary.strip(),
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


async def _analyze_naming(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    run_agent_loop: Any,
    naming_tool: dict[str, Any],
    progress: ProgressCallback,
) -> dict[str, Any]:
    """Steps 2-3: Naming score + agent transformations."""
    from pipelines.helpers import (
        build_full_pricelist_text,
        clean_service_name,
        fix_caps_lock,
        validate_name_transformation,
    )

    pricelist_text = build_full_pricelist_text(scraped_data)
    await progress(22, f"Prompt naming score: {len(pricelist_text)} znaków → MiniMax...")

    naming_prompt = _load_prompt("naming_score.txt")
    if not naming_prompt:
        naming_prompt = (
            "Oceń JAKOŚĆ NAZW usług w poniższym cenniku. "
            "Skala 0-20. Zwróć JSON z polami: score (int), issues (array of objects "
            "z severity, dimension, issue, impact, affectedCount, example, fix).\n\n"
        )
    full_prompt = f"{naming_prompt}\n\nCENNIK:\n{pricelist_text}"

    t0 = time.time()
    try:
        scoring = await client.generate_json(full_prompt, system="Jesteś ekspertem od cenników salonów beauty.")
        dt = int((time.time() - t0) * 1000)
        await progress(28, f"Naming score otrzymany: {scoring.get('score', '?')}/20 ({dt}ms)")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Naming scoring failed: %s", e)
        await progress(28, f"Naming scoring FAILED ({dt}ms): {e}")
        scoring = {"score": 10, "issues": []}

    score = min(20, max(0, int(scoring.get("score", 10))))
    issues = scoring.get("issues", [])

    # Agent loop
    naming_agent_prompt = _load_prompt("naming_agent.txt")
    if not naming_agent_prompt:
        naming_agent_prompt = (
            "Popraw nazwy usług w cenniku. Użyj narzędzia submit_naming_results. "
            "Wywołuj wielokrotnie z partiami po 15-20 nazw."
        )
    user_msg = f"{naming_agent_prompt}\n\nCENNIK:\n{pricelist_text}"
    await progress(30, "Agent loop: poprawianie nazw usług (tool_use)...")

    transformations: list[dict[str, Any]] = []
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
        await progress(38, f"Agent naming done: {agent_result.total_steps} steps, {len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)")

        accepted = 0
        rejected = 0
        for tc in agent_result.tool_calls:
            if tc.name == "submit_naming_results":
                raw = tc.input.get("transformations", [])
                for t in raw:
                    original = t.get("name", "")
                    improved = t.get("improved", "")
                    improved = clean_service_name(improved)
                    improved = fix_caps_lock(improved)
                    if original and improved and original != improved:
                        if validate_name_transformation(original, improved):
                            transformations.append({
                                "type": "name", "serviceName": original,
                                "before": original, "after": improved,
                                "reason": "Poprawa nazwy usługi", "impactScore": 3,
                            })
                            accepted += 1
                        else:
                            rejected += 1
        await progress(42, f"Naming: {accepted} zaakceptowanych, {rejected} odrzuconych transformacji")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Naming agent loop failed: %s", e)
        await progress(42, f"Naming agent FAILED ({dt}ms): {e}")

    return {"score": score, "issues": issues, "transformations": transformations}


async def _analyze_descriptions(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    run_agent_loop: Any,
    description_tool: dict[str, Any],
    progress: ProgressCallback,
) -> dict[str, Any]:
    """Steps 4-5: Description score + agent transformations."""
    from pipelines.helpers import build_full_pricelist_text

    pricelist_text = build_full_pricelist_text(scraped_data)
    desc_count = stats["servicesWithDescription"]
    total = stats["totalServices"]
    await progress(50, f"Prompt description score ({desc_count}/{total} z opisem): {len(pricelist_text)} znaków → MiniMax...")

    desc_prompt = _load_prompt("descriptions_score.txt")
    if not desc_prompt:
        desc_prompt = (
            "Oceń JAKOŚĆ OPISÓW usług. Skala 0-20. "
            "Zwróć JSON z polami: score (int), issues (array).\n\n"
        )
    full_prompt = f"{desc_prompt}\n\nCENNIK:\n{pricelist_text}"

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
    issues = scoring.get("issues", [])

    desc_agent_prompt = _load_prompt("descriptions_agent.txt")
    if not desc_agent_prompt:
        desc_agent_prompt = (
            "Przepisz opisy usług. Użyj submit_description_results. "
            "Partiami po 15-20."
        )
    user_msg = f"{desc_agent_prompt}\n\nCENNIK:\n{pricelist_text}"
    await progress(56, "Agent loop: poprawianie opisów usług (tool_use)...")

    transformations: list[dict[str, Any]] = []
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
        await progress(62, f"Agent descriptions done: {agent_result.total_steps} steps, {len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)")

        for tc in agent_result.tool_calls:
            if tc.name == "submit_description_results":
                raw = tc.input.get("transformations", [])
                for t in raw:
                    service_name = t.get("serviceName", "")
                    new_desc = t.get("newDescription", "")
                    if service_name and new_desc:
                        transformations.append({
                            "type": "description", "serviceName": service_name,
                            "before": "", "after": new_desc,
                            "reason": "Nowy/poprawiony opis", "impactScore": 2,
                        })
        await progress(64, f"Opisy: {len(transformations)} nowych/poprawionych opisów")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Description agent loop failed: %s", e)
        await progress(64, f"Description agent FAILED ({dt}ms): {e}")

    return {"score": score, "issues": issues, "transformations": transformations}


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
    full_prompt = f"{structure_prompt}\n\nCENNIK:\n{summary_text}"

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

    full_prompt = (
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
