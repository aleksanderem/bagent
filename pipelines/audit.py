"""Audit report pipeline -- orchestrates 10-step analysis flow."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

# Load prompt templates
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
    """Full audit analysis pipeline.

    Step 1: Calculate stats (pure Python, no AI)
    Step 2: Naming score + issues (single MiniMax call)
    Step 3: Naming transformations (agent loop with tool_use)
    Step 4: Descriptions score + issues (single MiniMax call)
    Step 5: Descriptions transformations (agent loop with tool_use)
    Step 6: Structure & pricing analysis (single MiniMax call)
    Step 7: Summary generation (single MiniMax call)
    Step 8: Competitor context (Supabase queries)
    Step 9: Quality validation + optional refinement
    Step 10: Assemble EnhancedAuditReport
    """
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

    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()

    # Step 1: Calculate stats
    logger.info("[%s] Step 1: Calculating statistics...", audit_id)
    await progress(42, "Obliczanie statystyk cennika...")
    stats = calculate_audit_stats(scraped_data)

    # Step 2-3: Naming analysis
    logger.info("[%s] Step 2-3: Analyzing naming quality...", audit_id)
    await progress(48, "Analiza nazw us\u0142ug...")
    naming_result = await _analyze_naming(client, scraped_data, stats, run_agent_loop, NAMING_TOOL)

    # Step 4-5: Description analysis
    logger.info("[%s] Step 4-5: Analyzing descriptions...", audit_id)
    await progress(58, "Analiza opis\u00f3w us\u0142ug...")
    desc_result = await _analyze_descriptions(client, scraped_data, stats, run_agent_loop, DESCRIPTION_TOOL)

    # Step 6: Structure & pricing
    logger.info("[%s] Step 6: Analyzing structure and pricing...", audit_id)
    await progress(72, "Analiza struktury i cen...")
    structure_result = await _analyze_structure(client, scraped_data, stats)

    # Calculate scores
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

    # Combine issues
    all_issues = naming_result["issues"] + desc_result["issues"] + structure_result.get("issues", [])
    severity_order = {"critical": 0, "major": 1, "minor": 2}
    all_issues.sort(key=lambda i: severity_order.get(i.get("severity", "minor"), 2))

    # Progressive score capping
    critical_count = sum(1 for i in all_issues if i.get("severity") == "critical")
    major_count = sum(1 for i in all_issues if i.get("severity") == "major")
    if critical_count >= 3 and total_score > 60:
        total_score = 60
    elif critical_count > 0 and total_score > 75:
        total_score = 75
    elif major_count > 0 and total_score > 85:
        total_score = 85
    elif all_issues and total_score > 95:
        total_score = 95

    # Combine transformations
    transformations = naming_result["transformations"] + desc_result["transformations"]

    # Step 7: Generate summary
    logger.info("[%s] Step 7: Generating summary...", audit_id)
    await progress(78, "Generowanie podsumowania...")
    summary = await _generate_summary(client, total_score, stats, all_issues, scraped_data.salonName)

    # Step 8: Competitor context
    logger.info("[%s] Step 8: Fetching competitor context...", audit_id)
    await progress(82, "Szukanie konkurencji w okolicy...")
    salon_location: dict[str, Any] | None = None
    competitor_context: dict[str, Any] | None = None
    competitors: list[dict[str, Any]] | None = None
    industry_comparison: dict[str, Any] = {
        "yourScore": total_score,
        "industryAverage": 52,
        "topPerformers": 78,
        "percentile": 50,
        "sampleSize": 500,
    }

    try:
        location = await supabase.geocode_salon(scraped_data.salonName or "", scraped_data.salonAddress or "")
        if location:
            salon_location = location
            all_names = [s.name for cat in scraped_data.categories for s in cat.services]
            raw_competitors = await supabase.get_competitors(location["lat"], location["lng"], 10, all_names)
            if raw_competitors:
                competitors = raw_competitors
                competitor_context = {
                    "competitorsFound": len(raw_competitors),
                    "radiusKm": 10,
                    "pricePositioning": "mid-range",
                    "topServiceGaps": [],
                    "dataFreshness": "recent",
                }

        # Industry benchmarks
        city = scraped_data.salonAddress.split(",")[-1].strip() if scraped_data.salonAddress else None
        benchmarks = await supabase.get_benchmarks(city)
        avg = benchmarks.get("industry_average", 52)
        top = benchmarks.get("top_performers", 78)
        sample = benchmarks.get("sample_size", 500)
        percentile = min(99, max(1, round((total_score / max(top, 1)) * 100)))
        industry_comparison = {
            "yourScore": total_score,
            "industryAverage": avg,
            "topPerformers": top,
            "percentile": percentile,
            "sampleSize": sample,
        }
    except Exception as e:
        logger.warning("[%s] Competitor/benchmark context failed: %s", audit_id, e)

    # Step 9-10: Assemble report
    await progress(90, "Sk\u0142adanie raportu...")
    report: dict[str, Any] = {
        "version": "v2",
        "totalScore": total_score,
        "scoreBreakdown": score_breakdown,
        "stats": stats,
        "topIssues": all_issues,
        "transformations": transformations,
        "missingSeoKeywords": structure_result.get("missingSeoKeywords", []),
        "quickWins": structure_result.get("quickWins", []),
        "industryComparison": industry_comparison,
        "competitorContext": competitor_context,
        "salonLocation": salon_location,
        "competitors": competitors,
        "summary": summary.strip(),
    }

    # Quality validation
    quality = _validate_quality(report, scraped_data)
    if not quality["isAcceptable"]:
        logger.warning(
            "[%s] Quality check failed: %s",
            audit_id,
            [c["name"] for c in quality["failedChecks"]],
        )

    await progress(100, "Raport gotowy!")
    return report


async def _analyze_naming(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    run_agent_loop: Any,
    naming_tool: dict[str, Any],
) -> dict[str, Any]:
    """Steps 2-3: Naming score + agent transformations."""
    from pipelines.helpers import (
        build_full_pricelist_text,
        clean_service_name,
        fix_caps_lock,
        validate_name_transformation,
    )

    pricelist_text = build_full_pricelist_text(scraped_data)

    # Step 2: Get naming score + issues
    naming_prompt = _load_prompt("naming_score.txt")
    if not naming_prompt:
        naming_prompt = (
            "Oce\u0144 JAKO\u015a\u0106 NAZW us\u0142ug w poni\u017cszym cenniku. "
            "Skala 0-20. Zwr\u00f3\u0107 JSON z polami: score (int), issues (array of objects "
            "z severity, dimension, issue, impact, affectedCount, example, fix).\n\n"
        )
    full_prompt = f"{naming_prompt}\n\nCENNIK:\n{pricelist_text}"

    try:
        scoring = await client.generate_json(full_prompt, system="Jeste\u015b ekspertem od cennik\u00f3w salon\u00f3w beauty.")
    except Exception as e:
        logger.warning("Naming scoring failed: %s", e)
        scoring = {"score": 10, "issues": []}

    score = min(20, max(0, int(scoring.get("score", 10))))
    issues = scoring.get("issues", [])

    # Step 3: Agent loop for naming transformations
    naming_agent_prompt = _load_prompt("naming_agent.txt")
    if not naming_agent_prompt:
        naming_agent_prompt = (
            "Popraw nazwy us\u0142ug w cenniku. U\u017cyj narz\u0119dzia submit_naming_results. "
            "Wywo\u0142uj wielokrotnie z partiami po 15-20 nazw."
        )
    user_msg = f"{naming_agent_prompt}\n\nCENNIK:\n{pricelist_text}"

    transformations: list[dict[str, Any]] = []
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jeste\u015b ekspertem od nazw us\u0142ug beauty.",
            user_message=user_msg,
            tools=[naming_tool],
            max_steps=30,
        )
        for tc in agent_result.tool_calls:
            if tc.name == "submit_naming_results":
                raw = tc.input.get("transformations", [])
                for t in raw:
                    original = t.get("name", "")
                    improved = t.get("improved", "")
                    # Apply programmatic fixes first
                    improved = clean_service_name(improved)
                    improved = fix_caps_lock(improved)
                    if original and improved and original != improved:
                        if validate_name_transformation(original, improved):
                            transformations.append(
                                {
                                    "type": "name",
                                    "serviceName": original,
                                    "before": original,
                                    "after": improved,
                                    "reason": "Poprawa nazwy us\u0142ugi",
                                    "impactScore": 3,
                                }
                            )
    except Exception as e:
        logger.warning("Naming agent loop failed: %s", e)

    return {"score": score, "issues": issues, "transformations": transformations}


async def _analyze_descriptions(
    client: Any,
    scraped_data: Any,
    stats: dict[str, Any],
    run_agent_loop: Any,
    description_tool: dict[str, Any],
) -> dict[str, Any]:
    """Steps 4-5: Description score + agent transformations."""
    from pipelines.helpers import build_full_pricelist_text

    pricelist_text = build_full_pricelist_text(scraped_data)

    # Step 4: Get description score + issues
    desc_prompt = _load_prompt("descriptions_score.txt")
    if not desc_prompt:
        desc_prompt = (
            "Oce\u0144 JAKO\u015a\u0106 OPIS\u00d3W us\u0142ug. Skala 0-20. "
            "Zwr\u00f3\u0107 JSON z polami: score (int), issues (array).\n\n"
        )
    full_prompt = f"{desc_prompt}\n\nCENNIK:\n{pricelist_text}"

    try:
        scoring = await client.generate_json(
            full_prompt, system="Jeste\u015b ekspertem od cennik\u00f3w salon\u00f3w beauty."
        )
    except Exception as e:
        logger.warning("Description scoring failed: %s", e)
        scoring = {"score": 10, "issues": []}

    score = min(20, max(0, int(scoring.get("score", 10))))
    issues = scoring.get("issues", [])

    # Step 5: Agent loop for description transformations
    desc_agent_prompt = _load_prompt("descriptions_agent.txt")
    if not desc_agent_prompt:
        desc_agent_prompt = (
            "Przepisz opisy us\u0142ug. U\u017cyj submit_description_results. "
            "Partiami po 15-20."
        )
    user_msg = f"{desc_agent_prompt}\n\nCENNIK:\n{pricelist_text}"

    transformations: list[dict[str, Any]] = []
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jeste\u015b ekspertem od opis\u00f3w us\u0142ug beauty.",
            user_message=user_msg,
            tools=[description_tool],
            max_steps=30,
        )
        for tc in agent_result.tool_calls:
            if tc.name == "submit_description_results":
                raw = tc.input.get("transformations", [])
                for t in raw:
                    service_name = t.get("serviceName", "")
                    new_desc = t.get("newDescription", "")
                    if service_name and new_desc:
                        transformations.append(
                            {
                                "type": "description",
                                "serviceName": service_name,
                                "before": "",
                                "after": new_desc,
                                "reason": "Nowy/poprawiony opis",
                                "impactScore": 2,
                            }
                        )
    except Exception as e:
        logger.warning("Description agent loop failed: %s", e)

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
            "Oce\u0144 STRUKTUR\u0118 (0-15) i CENY (0-15). "
            "Podaj WSZYSTKIE problemy, quick wins, SEO. "
            "Zwr\u00f3\u0107 JSON z polami: structureScore, pricingScore, "
            "issues, quickWins, missingSeoKeywords.\n\n"
        )
    full_prompt = f"{structure_prompt}\n\nCENNIK:\n{summary_text}"

    stats_context = (
        f"\nSTATYSTYKI:\n"
        f"- Liczba us\u0142ug: {stats['totalServices']}\n"
        f"- Kategorie: {stats['totalCategories']}\n"
        f"- \u015arednia us\u0142ug/kategori\u0119: {stats['avgServicesPerCategory']}\n"
        f"- Duplikaty nazw: {len(stats['duplicateNames'])}\n"
        f"- Puste kategorie: {len(stats['emptyCategories'])}\n"
        f"- Przeros\u0142e kategorie (>20): {len(stats['oversizedCategories'])}\n"
    )
    full_prompt += stats_context

    try:
        result = await client.generate_json(
            full_prompt, system="Jeste\u015b ekspertem od struktury cennik\u00f3w beauty."
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
        f"US\u0141UGI: {stats['totalServices']}\n"
        f"KATEGORIE: {stats['totalCategories']}\n"
        f"G\u0141\u00d3WNE PROBLEMY:\n{top_issues_text}"
    )

    try:
        return await client.generate_text(full_prompt, system="Jeste\u015b ekspertem od audyt\u00f3w cennik\u00f3w beauty.")
    except Exception as e:
        logger.warning("Summary generation failed: %s", e)
        return f"Cennik salonu {salon_name or 'Nieznany'} uzyska\u0142 {total_score}/100 punkt\u00f3w."


def _validate_quality(report: dict[str, Any], scraped_data: Any) -> dict[str, Any]:
    """Quality validation with 7 business checks.

    Returns dict with isAcceptable (bool), score (float 0-1), failedChecks (list).
    """
    checks: list[dict[str, Any]] = []
    total_services = report["stats"].get("totalServices", 0)

    # Check 1: Score range (weight: 2)
    score_ok = 0 <= report["totalScore"] <= 100
    checks.append({"name": "score_range", "weight": 2, "passed": score_ok})

    # Check 2: Has issues when score < 80 (weight: 3)
    has_issues = len(report["topIssues"]) > 0 or report["totalScore"] >= 80
    checks.append({"name": "issues_present", "weight": 3, "passed": has_issues})

    # Check 3: Transformations count reasonable (weight: 2)
    trans_count = len(report["transformations"])
    trans_ok = trans_count <= total_services * 3 if total_services > 0 else trans_count == 0
    checks.append({"name": "transformations_reasonable", "weight": 2, "passed": trans_ok})

    # Check 4: Score breakdown sums correctly (weight: 3)
    breakdown = report["scoreBreakdown"]
    component_sum = sum(breakdown.values())
    sum_ok = abs(component_sum - report["totalScore"]) <= 5  # Allow capping delta
    checks.append({"name": "score_consistency", "weight": 3, "passed": sum_ok})

    # Check 5: Summary not empty (weight: 1)
    summary_ok = len(report.get("summary", "").strip()) > 10
    checks.append({"name": "summary_present", "weight": 1, "passed": summary_ok})

    # Check 6: Stats match input data (weight: 2)
    input_service_count = sum(len(cat.services) for cat in scraped_data.categories)
    stats_ok = report["stats"].get("totalServices", 0) == input_service_count
    checks.append({"name": "stats_match", "weight": 2, "passed": stats_ok})

    # Check 7: No critical severity inflation (weight: 2)
    critical_count = sum(1 for i in report["topIssues"] if i.get("severity") == "critical")
    inflation_ok = critical_count <= total_services * 0.5 if total_services > 0 else critical_count == 0
    checks.append({"name": "severity_not_inflated", "weight": 2, "passed": inflation_ok})

    total_weight = sum(c["weight"] for c in checks)
    passed_weight = sum(c["weight"] for c in checks if c["passed"])
    quality_score = passed_weight / total_weight if total_weight > 0 else 0

    failed_checks = [c for c in checks if not c["passed"]]

    return {
        "isAcceptable": quality_score >= 0.7,
        "score": quality_score,
        "failedChecks": failed_checks,
        "allChecks": checks,
    }
