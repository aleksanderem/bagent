"""Competitor report pipeline -- 12-step analysis flow."""

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
    """Replace {var_name} placeholders. Always strips {refinement_prefix}."""
    result = template.replace("{refinement_prefix}", "")
    for key, value in kwargs.items():
        result = result.replace(f"{{{key}}}", value)
    return result


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


def _format_price_comparison(data: list[dict]) -> str:
    """Format price comparison RPC results as readable text for AI context."""
    if not data:
        return "Brak danych porównania cen."
    lines = ["PORÓWNANIE CEN WG KATEGORII:"]
    for row in data:
        cat = row.get("category", "?")
        avg_price = row.get("avg_price", "?")
        subject_avg = row.get("subject_avg_price", "?")
        positioning = row.get("positioning", "?")
        lines.append(f"  {cat}: Salon={subject_avg} zł, Rynek={avg_price} zł, Pozycja={positioning}")
    return "\n".join(lines)


def _format_local_ranking(data: list[dict]) -> str:
    """Format local ranking RPC results as readable text."""
    if not data:
        return "Brak danych rankingu lokalnego."
    row = data[0] if data else {}
    rank = row.get("rank", "?")
    total = row.get("total_in_city", "?")
    city = row.get("city", "?")
    percentile = row.get("percentile", "?")
    return f"RANKING LOKALNY: {rank}/{total} w {city} (percentyl: {percentile}%)"


def _format_gap_matrix(data: list[dict]) -> str:
    """Format service gap matrix as readable text."""
    if not data:
        return "Brak danych luk w usługach."
    lines = ["LUKI W USŁUGACH:"]
    salon_missing = [r for r in data if not r.get("salon_has", True)]
    comp_missing = [r for r in data if not r.get("competitor_has", True)]
    if salon_missing:
        lines.append("  Usługi których salon NIE MA a konkurencja TAK:")
        for r in salon_missing[:15]:
            svc = r.get("service_name", "?")
            count = r.get("competitor_count", 0)
            lines.append(f"    - {svc} (oferuje {count} konkurentów)")
    if comp_missing:
        lines.append("  Usługi które salon MA a konkurencja NIE:")
        for r in comp_missing[:10]:
            svc = r.get("service_name", "?")
            lines.append(f"    - {svc}")
    return "\n".join(lines)


def _format_promotions(data: list[dict]) -> str:
    """Format competitor promotions as readable text."""
    if not data:
        return "Brak danych o promocjach konkurencji."
    lines = ["PROMOCJE KONKURENCJI:"]
    for row in data[:20]:
        salon = row.get("salon_name", "?")
        service = row.get("service_name", "?")
        discount = row.get("discount_percent", 0)
        lines.append(f"  {salon}: {service} (-{discount}%)")
    return "\n".join(lines)


def _format_per_service_pricing(data: list[dict]) -> str:
    """Format per-service pricing comparison as readable text."""
    if not data:
        return "Brak danych porównania cen usług."
    lines = ["PORÓWNANIE CEN USŁUG:"]
    for row in data[:30]:
        service = row.get("service_name", "?")
        subject_price = row.get("subject_price", "?")
        min_price = row.get("min_price", "?")
        max_price = row.get("max_price", "?")
        avg_price = row.get("avg_price", "?")
        percentile = row.get("percentile_position", "?")
        lines.append(
            f"  {service}: Salon={subject_price} zł, Min={min_price}, Max={max_price}, "
            f"Śr={avg_price}, Percentyl={percentile}"
        )
    return "\n".join(lines)


def _format_salon_for_context(salon_data: dict | None, label: str = "SALON") -> str:
    """Format salon data as readable text for AI context."""
    if not salon_data:
        return f"{label}: Brak danych."
    salon = salon_data.get("salon") or {}
    services = salon_data.get("services") or []
    categories = salon_data.get("categories") or []
    name = salon.get("name", "?")
    city = salon.get("city", "?")
    rating = salon.get("reviews_rank", "?")
    review_count = salon.get("reviews_count", 0)
    lines = [
        f"{label}: {name} ({city})",
        f"  Ocena: {rating}/5 ({review_count} opinii)",
        f"  Usługi: {len(services)}, Kategorie: {len(categories)}",
    ]
    if categories:
        lines.append(f"  Kategorie: {', '.join(categories[:10])}")
    return "\n".join(lines)


async def run_competitor_pipeline(
    audit_id: str,
    subject_salon_id: int,
    salon_name: str,
    salon_city: str,
    salon_lat: float | None,
    salon_lng: float | None,
    selected_competitor_ids: list[int],
    services: list[str],
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """12-step competitor report pipeline."""
    from agent.runner import run_agent_loop
    from agent.tools import MARKET_ANALYSIS_TOOL, STRATEGIC_ANALYSIS_TOOL
    from config import settings
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()
    comp_count = len(selected_competitor_ids)

    # ── Step 1: Load subject salon ──
    logger.info("[%s] Step 1: Loading subject salon %d...", audit_id, subject_salon_id)
    await progress(5, f"Ładowanie danych salonu '{salon_name}' (ID: {subject_salon_id})...")
    t0 = time.time()
    subject_data = await supabase.get_salon_with_services(subject_salon_id)
    dt = int((time.time() - t0) * 1000)

    subject_salon = (subject_data or {}).get("salon")
    if subject_salon:
        svc_count = len(subject_data.get("services") or [])
        cat_count = len(subject_data.get("categories") or [])
        await progress(8, f"Salon załadowany: {svc_count} usług, {cat_count} kategorii ({dt}ms)")
        logger.info("[%s] Subject salon loaded: %d services, %d categories", audit_id, svc_count, cat_count)
    else:
        await progress(8, f"Salon nie znaleziony w bazie — używam danych z requestu ({dt}ms)")
        logger.warning("[%s] Subject salon %d not found in Supabase", audit_id, subject_salon_id)

    # ── Step 2: Load competitor salons (PARALLEL) ──
    import asyncio as _asyncio

    logger.info("[%s] Step 2: Loading %d competitor salons in parallel...", audit_id, comp_count)
    await progress(10, f"Ładowanie {comp_count} salonów konkurencji (równolegle)...")
    t0 = time.time()
    comp_results = await _asyncio.gather(
        *(supabase.get_salon_with_services(cid) for cid in selected_competitor_ids),
        return_exceptions=True,
    )
    competitor_salons = [r for r in comp_results if isinstance(r, dict) and r.get("salon")]
    dt = int((time.time() - t0) * 1000)
    loaded_count = len(competitor_salons)
    await progress(18, f"Załadowano {loaded_count}/{comp_count} konkurentów ({dt}ms)")

    # ── Steps 3-7: All RPCs in PARALLEL ──
    logger.info("[%s] Steps 3-7: Running 5 RPCs in parallel...", audit_id)
    await progress(22, "Równoległe zapytania RPC: ceny, ranking, luki, promocje, porównanie...")
    t0 = time.time()

    async def _safe_rpc(name: str, params: dict) -> list[dict]:
        try:
            return await supabase.call_rpc(name, params)
        except Exception as e:
            logger.warning("[%s] RPC %s failed: %s", audit_id, name, e)
            return []

    price_comparison, local_ranking, gap_matrix, promotions, per_service_pricing = await _asyncio.gather(
        _safe_rpc("get_competitor_price_comparison", {"p_salon_ids": selected_competitor_ids, "p_subject_salon_id": subject_salon_id}),
        _safe_rpc("get_local_salon_rank", {"p_salon_id": subject_salon_id}),
        _safe_rpc("get_service_gap_matrix", {"p_subject_salon_id": subject_salon_id, "p_competitor_ids": selected_competitor_ids}),
        _safe_rpc("get_competitor_promotions", {"p_competitor_ids": selected_competitor_ids}),
        _safe_rpc("get_per_service_pricing", {"p_subject_salon_id": subject_salon_id, "p_competitor_ids": selected_competitor_ids}),
    )

    dt = int((time.time() - t0) * 1000)
    ranking_info = local_ranking[0] if local_ranking else {}
    rank_str = f"{ranking_info.get('rank', '?')}/{ranking_info.get('total_in_city', '?')}" if ranking_info else "brak"
    await progress(50, (
        f"RPCs zakończone ({dt}ms): {len(price_comparison)} cen, ranking {rank_str}, "
        f"{len(gap_matrix)} luk, {len(promotions)} promocji, {len(per_service_pricing)} porównań"
    ))

    # ── Step 8: AI Market Analysis (agent loop) ──
    logger.info("[%s] Step 8: AI Market Analysis...", audit_id)
    await progress(52, "AI: Analiza rynkowa (agent loop)...")

    # Build context text from all RPC results
    context_parts = [
        _format_salon_for_context(subject_data, "SALON ANALIZOWANY"),
        "",
    ]
    for i, comp in enumerate(competitor_salons):
        context_parts.append(_format_salon_for_context(comp, f"KONKURENT {i + 1}"))
    context_parts.extend([
        "",
        _format_price_comparison(price_comparison),
        "",
        _format_local_ranking(local_ranking),
        "",
        _format_gap_matrix(gap_matrix),
        "",
        _format_promotions(promotions),
        "",
        _format_per_service_pricing(per_service_pricing),
    ])
    context_text = "\n".join(context_parts)

    market_prompt = _load_prompt("competitor_market.txt")
    if not market_prompt:
        market_prompt = (
            "Przeanalizuj dane rynkowe i zwróć podsumowanie, przewagi, rekomendacje i metryki. "
            "Użyj narzędzia submit_market_analysis."
        )
    user_msg = f"{market_prompt}\n\nDANE:\n{context_text}"

    market_summary: dict[str, Any] = {}
    advantages: list[dict[str, Any]] = []
    recommendations: list[dict[str, Any]] = []
    radar_metrics: list[dict[str, Any]] = []

    t0 = time.time()
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem ds. analizy rynku beauty w Polsce.",
            user_message=user_msg,
            tools=[MARKET_ANALYSIS_TOOL],
            max_steps=5,
            on_step=lambda step, count: logger.info(
                "[market] Agent step %d, %d tool calls so far", step, count
            ),
        )
        dt = int((time.time() - t0) * 1000)
        tokens = agent_result.total_input_tokens + agent_result.total_output_tokens
        await progress(62, f"Market analysis done: {agent_result.total_steps} steps, {len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)")

        for tc in agent_result.tool_calls:
            if tc.name == "submit_market_analysis":
                market_summary = tc.input.get("marketSummary", market_summary)
                advantages = tc.input.get("advantages", advantages)
                recommendations = tc.input.get("recommendations", recommendations)
                radar_metrics = tc.input.get("radarMetrics", radar_metrics)
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("[%s] Market analysis agent failed: %s", audit_id, e)
        await progress(62, f"Market analysis FAILED ({dt}ms): {e}")

    # ── Step 9: AI SWOT Analysis (single JSON call) ──
    logger.info("[%s] Step 9: AI SWOT Analysis...", audit_id)
    await progress(65, "AI: Analiza SWOT...")

    swot_prompt = _load_prompt("competitor_swot.txt")
    if not swot_prompt:
        swot_prompt = (
            "Przygotuj analizę SWOT. Zwróć JSON z strengths, weaknesses, opportunities, threats."
        )

    market_summary_text = ""
    if market_summary:
        headline = market_summary.get("headline", "")
        paragraphs = market_summary.get("paragraphs", [])
        market_summary_text = f"Nagłówek: {headline}\n" + "\n".join(paragraphs)

    advantages_text = "\n".join(
        f"- [{a.get('type', '?')}] {a.get('area', '?')}: {a.get('description', '')}"
        for a in advantages
    ) if advantages else "Brak danych o przewagach."

    swot_full_prompt = (
        f"{swot_prompt}\n\n"
        f"SALON: {salon_name} ({salon_city})\n"
        f"ANALIZA RYNKOWA:\n{market_summary_text}\n\n"
        f"PRZEWAGI:\n{advantages_text}\n\n"
        f"RANKING: {rank_str}\n"
        f"LUKI W USŁUGACH:\n{_format_gap_matrix(gap_matrix)}\n"
    )

    swot: dict[str, Any] = {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []}
    t0 = time.time()
    try:
        swot_result = await client.generate_json(
            swot_full_prompt,
            system="Jesteś ekspertem ds. analizy strategicznej salonów beauty.",
        )
        dt = int((time.time() - t0) * 1000)
        swot = {
            "strengths": swot_result.get("strengths", []),
            "weaknesses": swot_result.get("weaknesses", []),
            "opportunities": swot_result.get("opportunities", []),
            "threats": swot_result.get("threats", []),
        }
        total_items = sum(len(v) for v in swot.values())
        await progress(72, f"SWOT: {total_items} elementów ({dt}ms)")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("[%s] SWOT analysis failed: %s", audit_id, e)
        await progress(72, f"SWOT FAILED ({dt}ms): {e}")

    # ── Step 10: AI Strategic Analysis (agent loop) ──
    logger.info("[%s] Step 10: AI Strategic Analysis...", audit_id)
    await progress(74, "AI: Analiza strategiczna (agent loop)...")

    strategy_prompt = _load_prompt("competitor_strategy.txt")
    if not strategy_prompt:
        strategy_prompt = (
            "Przygotuj analizę strategiczną: nisze rynkowe i plan działania. "
            "Użyj narzędzia submit_strategic_analysis."
        )

    swot_text = (
        f"SWOT:\n"
        f"  Mocne strony: {', '.join(swot['strengths'][:3])}\n"
        f"  Słabości: {', '.join(swot['weaknesses'][:3])}\n"
        f"  Szanse: {', '.join(swot['opportunities'][:3])}\n"
        f"  Zagrożenia: {', '.join(swot['threats'][:3])}\n"
    )

    strategy_user_msg = (
        f"{strategy_prompt}\n\n"
        f"ANALIZA RYNKOWA:\n{market_summary_text}\n\n"
        f"{swot_text}\n"
        f"PRZEWAGI:\n{advantages_text}\n\n"
        f"DANE RYNKOWE:\n{_format_gap_matrix(gap_matrix)}\n"
        f"{_format_promotions(promotions)}\n"
    )

    market_niches: list[dict[str, Any]] = []
    action_plan: list[dict[str, Any]] = []

    t0 = time.time()
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem ds. strategii biznesowej salonów beauty.",
            user_message=strategy_user_msg,
            tools=[STRATEGIC_ANALYSIS_TOOL],
            max_steps=10,
            on_step=lambda step, count: logger.info(
                "[strategy] Agent step %d, %d tool calls so far", step, count
            ),
        )
        dt = int((time.time() - t0) * 1000)
        tokens = agent_result.total_input_tokens + agent_result.total_output_tokens
        await progress(85, f"Strategic analysis done: {agent_result.total_steps} steps, {len(agent_result.tool_calls)} calls, {tokens} tokens ({dt}ms)")

        for tc in agent_result.tool_calls:
            if tc.name == "submit_strategic_analysis":
                niches = tc.input.get("marketNiches", [])
                plan = tc.input.get("actionPlan", [])
                market_niches.extend(niches)
                action_plan.extend(plan)
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("[%s] Strategic analysis agent failed: %s", audit_id, e)
        await progress(85, f"Strategic analysis FAILED ({dt}ms): {e}")

    # ── Step 11: Assemble report ──
    logger.info("[%s] Step 11: Assembling competitor report...", audit_id)
    await progress(90, "Składanie raportu konkurencji...")

    # Build competitor profiles from loaded data
    competitor_profiles: list[dict[str, Any]] = []
    for comp in competitor_salons:
        if not comp:
            continue
        salon_info = (comp.get("salon") or {})
        comp_services = comp.get("services") or []
        competitor_profiles.append({
            "salonId": salon_info.get("id"),
            "name": salon_info.get("name", "?"),
            "city": salon_info.get("city", salon_city),
            "lat": salon_info.get("lat"),
            "lng": salon_info.get("lng"),
            "reviewsRank": salon_info.get("reviews_rank"),
            "reviewsCount": salon_info.get("reviews_count", 0),
            "serviceCount": len(comp_services),
            "categories": comp.get("categories") or [],
        })

    report: dict[str, Any] = {
        "version": "v1",
        "subjectSalonId": subject_salon_id,
        "salonName": salon_name,
        "salonCity": salon_city,
        "salonLat": salon_lat,
        "salonLng": salon_lng,
        "competitorIds": selected_competitor_ids,
        "competitorCount": comp_count,
        "competitorProfiles": competitor_profiles,
        "priceComparison": price_comparison,
        "localRanking": ranking_info,
        "serviceGapMatrix": gap_matrix,
        "promotions": promotions,
        "perServicePricing": per_service_pricing,
        "marketSummary": market_summary,
        "advantages": advantages,
        "recommendations": recommendations,
        "radarMetrics": radar_metrics,
        "swot": swot,
        "marketNiches": market_niches,
        "actionPlan": action_plan,
    }

    await progress(95, f"Raport złożony: {len(competitor_profiles)} profili, {len(recommendations)} rekomendacji, {len(action_plan)} działań")

    # ── Step 12: Return report ──
    logger.info("[%s] Step 12: Report complete.", audit_id)
    await progress(100, f"Raport konkurencji gotowy! {comp_count} konkurentów przeanalizowanych")

    return report
