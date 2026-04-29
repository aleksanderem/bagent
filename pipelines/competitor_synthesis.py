"""AI synthesis for competitor reports (Comp Etap 5).

Reads the populated competitor_reports + children (matches, pricing_comparisons,
service_gaps, dimensional_scores) written by Comp Etap 4 and feeds them to
MiniMax via a submit_competitor_insights tool_use agent loop. Produces:

  - positioning_narrative (200-400 char Polish text)
  - SWOT (strengths / weaknesses / opportunities / threats) with traceability
  - recommendations (5-12 items with impact/effort/confidence + sourceCompetitorIds
    + sourceDataPoints)

Persisted to competitor_reports.report_data (narrative + swot jsonb) and
competitor_recommendations rows (each with full traceability to specific
competitors + data point rows).

Non-fatal on AI failure: _deterministic_fallback() builds basic text from
dimensional scores and persists empty SWOT + recommendations. The pipeline
never breaks the report row on MiniMax flakiness.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


# ---------------------------------------------------------------------------
# Tool schema
# ---------------------------------------------------------------------------

# Source data point schema — referenced from SWOT bullets and recommendations.
_SOURCE_DATA_POINT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "enum": ["dimensional_score", "pricing_comparison", "service_gap"],
        },
        "id": {"type": "integer"},
    },
    "required": ["type", "id"],
}

# Single SWOT bullet schema.
_SWOT_BULLET_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "text": {"type": "string", "minLength": 10, "maxLength": 400},
        "sourceDataPoints": {
            "type": "array",
            "items": _SOURCE_DATA_POINT_SCHEMA,
            "minItems": 1,
            "maxItems": 5,
        },
    },
    "required": ["text", "sourceDataPoints"],
}

# Recommendation schema — full action with traceability.
_RECOMMENDATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "actionTitle": {"type": "string", "minLength": 10, "maxLength": 200},
        "actionDescription": {"type": "string", "minLength": 50, "maxLength": 800},
        "category": {
            "type": "string",
            "enum": ["pricing", "content", "services", "operations", "social"],
        },
        "impact": {"type": "string", "enum": ["low", "medium", "high"]},
        "effort": {"type": "string", "enum": ["low", "medium", "high"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "estimatedRevenueImpactGrosze": {"type": "integer"},
        "sourceCompetitorIds": {
            "type": "array",
            "items": {"type": "integer"},
            "maxItems": 20,
        },
        "sourceDataPoints": {
            "type": "array",
            "items": _SOURCE_DATA_POINT_SCHEMA,
            "minItems": 1,
            "maxItems": 10,
        },
    },
    "required": [
        "actionTitle",
        "actionDescription",
        "category",
        "impact",
        "effort",
        "confidence",
        "sourceDataPoints",
    ],
}

COMPETITOR_INSIGHTS_TOOL: dict[str, Any] = {
    "name": "submit_competitor_insights",
    "description": (
        "Wyślij kompletny raport syntetyczny: narratyw pozycjonowania, SWOT z "
        "traceability oraz rekomendacje z pełnym linkiem do konkurentów i "
        "wierszy danych. Wywołaj DOKŁADNIE RAZ, z wszystkimi sekcjami."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "positioning_narrative": {
                "type": "string",
                "minLength": 100,
                "maxLength": 800,
            },
            "swot": {
                "type": "object",
                "properties": {
                    "strengths": {
                        "type": "array",
                        "items": _SWOT_BULLET_SCHEMA,
                        "minItems": 1,
                        "maxItems": 5,
                    },
                    "weaknesses": {
                        "type": "array",
                        "items": _SWOT_BULLET_SCHEMA,
                        "minItems": 1,
                        "maxItems": 5,
                    },
                    "opportunities": {
                        "type": "array",
                        "items": _SWOT_BULLET_SCHEMA,
                        "minItems": 1,
                        "maxItems": 5,
                    },
                    "threats": {
                        "type": "array",
                        "items": _SWOT_BULLET_SCHEMA,
                        "minItems": 1,
                        "maxItems": 5,
                    },
                },
                "required": ["strengths", "weaknesses", "opportunities", "threats"],
            },
            "recommendations": {
                "type": "array",
                "items": _RECOMMENDATION_SCHEMA,
                "minItems": 3,
                "maxItems": 12,
            },
        },
        "required": ["positioning_narrative", "swot", "recommendations"],
    },
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def synthesize_competitor_insights(
    report_id: int,
    on_progress: ProgressCallback | None = None,
    supabase: SupabaseService | None = None,
) -> dict[str, Any]:
    """Etap 5 main entry — reads the populated competitor_reports row + children,
    calls MiniMax with the synthesis prompt, and persists narrative + SWOT +
    recommendations.

    Non-fatal on AI failure: falls back to basic positioning text derived from
    dimensional scores, skips SWOT, skips recommendations (logs warning).

    Returns:
        dict: {
            "narrative": str,
            "swot_item_count": int,
            "recommendation_count": int,
            "used_fallback": bool,
        }
    """
    progress = on_progress or _noop_progress
    service = supabase or SupabaseService()

    # ── Step 1: Load data from Supabase ──
    await progress(10, "Ładowanie danych raportu...")
    report = await service.get_competitor_report_by_id(report_id)
    if report is None:
        raise ValueError(f"competitor_reports id={report_id} not found")

    matches = await service.get_competitor_matches(report_id)
    pricing = await service.get_competitor_pricing_comparisons(report_id)
    gaps = await service.get_competitor_service_gaps(report_id)
    dimensions = await service.get_competitor_dimensional_scores(report_id)

    logger.info(
        "Etap 5: loaded report_id=%s — %d matches, %d pricing, %d gaps, %d dims",
        report_id, len(matches), len(pricing), len(gaps), len(dimensions),
    )

    # Resolve subject salon info for prompt context
    subject_context = await service.get_subject_salon_context(report["subject_salon_id"])

    # ── Step 2: Build prompt context ──
    await progress(25, "Budowanie kontekstu dla MiniMax...")
    context = _build_synthesis_prompt_context(
        report=report,
        subject_context=subject_context,
        matches=matches,
        pricing=pricing,
        gaps=gaps,
        dimensions=dimensions,
    )

    # Collect valid IDs so we can validate source references the agent returns
    valid_ids = {
        "dimensional_score": {int(d["id"]) for d in dimensions if d.get("id") is not None},
        "pricing_comparison": {int(p["id"]) for p in pricing if p.get("id") is not None},
        "service_gap": {int(g["id"]) for g in gaps if g.get("id") is not None},
    }
    valid_competitor_ids = {
        int(m["booksy_id"]) for m in matches if m.get("booksy_id") is not None
    }

    # ── Step 3: MiniMax agent loop (with graceful fallback) ──
    await progress(40, "MiniMax agent synthesis...")
    used_fallback = False
    try:
        insights = await _run_minimax_synthesis(context)
        insights = _sanitize_insights(
            insights,
            valid_ids=valid_ids,
            valid_competitor_ids=valid_competitor_ids,
        )
    except Exception as e:
        logger.warning(
            "Etap 5 MiniMax synthesis failed for report_id=%s: %s — falling back",
            report_id, e,
        )
        used_fallback = True
        insights = _deterministic_fallback(
            subject_context=subject_context,
            matches=matches,
            pricing=pricing,
            gaps=gaps,
            dimensions=dimensions,
        )

    # ── Step 4: Persist narrative + SWOT + deterministic enrichment ──
    # Frontend rich UI (BEAUTY_AUDIT competitor report) needs more fields than
    # narrative+swot. We build them deterministically from data already loaded
    # for the prompt — no extra Supabase calls, no AI hallucination risk.
    # This closes the adapter "fallback" gap for: competitorProfiles,
    # priceComparison, scoreBreakdown, opportunities.
    await progress(75, "Budowanie deterministycznych pól dla frontendu...")
    competitor_profiles = _build_competitor_profiles(matches)
    price_comparison = _build_price_comparison(pricing)
    score_breakdown = _build_score_breakdown(dimensions)
    opportunities = _build_opportunities(gaps, pricing)
    seasonal_calendar = _build_seasonal_calendar(subject_context)
    short_strategy = _build_short_strategy(insights.get("recommendations") or [])
    long_strategy = _build_long_strategy(
        insights.get("recommendations") or [], gaps, dimensions,
    )
    customer_journey = _build_customer_journey(subject_context, dimensions, pricing)
    funnel = _build_funnel(subject_context, pricing)
    action_plan = _build_action_plan(
        insights.get("recommendations") or [], gaps,
    )

    # calendarComparison needs an extra Supabase fetch (open_hours from
    # salon_scrapes for subject + best competitor). Best-effort: log and
    # fall back to None if scrapes unavailable.
    calendar_comparison = await _build_calendar_comparison(
        subject_salon_id=report["subject_salon_id"],
        matches=matches,
        service=service,
    )

    logger.info(
        "Etap 5 enrichment built: profiles=%d, pricing=%d, score_areas=%d, "
        "opportunities=%d, seasonalCalendar=%s, shortStrategy=%d, longStrategy=%d, "
        "customerJourney=%d, funnel=%s, calendarComparison=%s, actionPlan=%d",
        len(competitor_profiles), len(price_comparison),
        len(score_breakdown), len(opportunities),
        "12 months" if seasonal_calendar else "skipped",
        len(short_strategy), len(long_strategy),
        len(customer_journey), "yes" if funnel else "skipped",
        "yes" if calendar_comparison else "skipped",
        len(action_plan),
    )

    await progress(80, "Persystencja narratywu + SWOT + rekomendacji...")
    persistence_payload: dict[str, Any] = {
        "positioning_narrative": insights["positioning_narrative"],
        "swot": insights["swot"],
        "competitorProfiles": competitor_profiles,
        "priceComparison": price_comparison,
        "scoreBreakdown": score_breakdown,
        "opportunities": opportunities,
        "seasonalCalendar": seasonal_calendar,
        "shortStrategy": short_strategy,
        "longStrategy": long_strategy,
        "customerJourney": customer_journey,
        "funnel": funnel,
        "actionPlan": action_plan,
    }
    if calendar_comparison is not None:
        persistence_payload["calendarComparison"] = calendar_comparison
    await service.update_competitor_report_data(report_id, persistence_payload)

    # ── Step 5: Persist recommendations with traceability ──
    # Delete any stale recommendations from a prior synthesis run (idempotent).
    await service.delete_competitor_recommendations(report_id)

    rec_rows: list[dict[str, Any]] = []
    for idx, rec in enumerate(insights["recommendations"]):
        rec_rows.append({
            "report_id": report_id,
            "action_title": rec["actionTitle"],
            "action_description": rec["actionDescription"],
            "category": rec["category"],
            "impact": rec["impact"],
            "effort": rec["effort"],
            "confidence": round(float(rec["confidence"]), 2),
            "estimated_revenue_impact_grosze": rec.get("estimatedRevenueImpactGrosze"),
            "source_competitor_ids": rec.get("sourceCompetitorIds") or [],
            "source_data_points": rec.get("sourceDataPoints") or [],
            "sort_order": idx,
        })

    n_recs = await service.insert_competitor_recommendations(rec_rows)

    swot_item_count = sum(len(v) for v in insights["swot"].values())

    await progress(
        100,
        f"Gotowe: {swot_item_count} SWOT, {n_recs} rekomendacji"
        + (" (fallback)" if used_fallback else ""),
    )

    logger.info(
        "Etap 5 complete: report_id=%s, narrative=%d chars, swot=%d items, "
        "recommendations=%d, used_fallback=%s",
        report_id, len(insights["positioning_narrative"]),
        swot_item_count, n_recs, used_fallback,
    )

    return {
        "narrative": insights["positioning_narrative"],
        "swot_item_count": swot_item_count,
        "recommendation_count": n_recs,
        "used_fallback": used_fallback,
    }


# ---------------------------------------------------------------------------
# Prompt context builder
# ---------------------------------------------------------------------------


_GROSZE_TO_ZL = 100


def _format_price_grosze(value: Any) -> str:
    """Format a grosze integer as 'X zł'."""
    if value is None:
        return "—"
    try:
        zl = float(value) / _GROSZE_TO_ZL
        return f"{zl:.0f} zł"
    except (TypeError, ValueError):
        return "—"


def _format_pct(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):+.0f}%"
    except (TypeError, ValueError):
        return "—"


def _build_synthesis_prompt_context(
    *,
    report: dict[str, Any],
    subject_context: dict[str, Any],
    matches: list[dict[str, Any]],
    pricing: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    dimensions: list[dict[str, Any]],
) -> str:
    """Format the raw data into the prompt template.

    Returns the filled prompt string ready to send to MiniMax. Each data row
    is printed with its database id so the agent can reference it in
    sourceDataPoints.
    """
    template = (PROMPTS_DIR / "competitor_synthesis.txt").read_text(encoding="utf-8")

    # ── subject_context block ──
    salon_name = subject_context.get("salon_name") or "Salon"
    salon_city = subject_context.get("salon_city") or "—"
    primary_category = subject_context.get("primary_category_name") or "beauty"
    reviews_count = subject_context.get("reviews_count") or 0
    reviews_rank = subject_context.get("reviews_rank") or 0.0
    total_services = subject_context.get("total_services") or 0

    subject_block = (
        f"Nazwa: {salon_name}\n"
        f"Miasto: {salon_city}\n"
        f"Kategoria główna: {primary_category}\n"
        f"Liczba usług w cenniku: {total_services}\n"
        f"Liczba recenzji: {reviews_count}\n"
        f"Średnia ocena: {reviews_rank:.2f}/5"
    )

    # ── competitors_summary block ──
    if not matches:
        comp_block = "Brak konkurentów w raporcie."
    else:
        lines: list[str] = [
            "Format: BOOKSY_ID | NAZWA | BUCKET | DYSTANS | REVIEWS | OCENA"
        ]
        for m in matches[:20]:
            lines.append(
                f"{m.get('booksy_id', '?')} | {m.get('salon_name') or 'Nieznany'} | "
                f"{m.get('bucket', '?')} | {m.get('distance_km', 0):.1f} km | "
                f"{m.get('reviews_count') or 0} rec. | "
                f"{m.get('reviews_rank') or 0:.2f}/5"
            )
        comp_block = "\n".join(lines)

    # ── pricing_insights block (top odchyleń od rynku) ──
    if not pricing:
        pricing_block = "Brak porównań cenowych (podmiot nie ma usług z canonical treatment_id lub rynek zbyt mały)."
    else:
        # Sort by |deviation_pct| desc to show the most significant deviations
        sorted_pricing = sorted(
            pricing,
            key=lambda p: abs(float(p.get("deviation_pct") or 0)),
            reverse=True,
        )[:12]
        lines = ["Format: PRICING_ID | USŁUGA | TWOJA CENA | MEDIANA RYNKU | ODCHYLENIE | REKOMENDACJA | PRÓBA"]
        for p in sorted_pricing:
            lines.append(
                f"{p['id']} | {p.get('treatment_name') or 'Unknown'} | "
                f"{_format_price_grosze(p.get('subject_price_grosze'))} | "
                f"{_format_price_grosze(p.get('market_median_grosze'))} | "
                f"{_format_pct(p.get('deviation_pct'))} | "
                f"{p.get('recommended_action') or 'hold'} | "
                f"{p.get('sample_size') or 0} konkurentów"
            )
        pricing_block = "\n".join(lines)

    # ── gap_insights block ──
    missing = [g for g in gaps if g.get("gap_type") == "missing"]
    uniques = [g for g in gaps if g.get("gap_type") == "unique_usp"]

    gap_lines: list[str] = []
    if missing:
        gap_lines.append("BRAKUJĄCE USŁUGI (Twój salon NIE ma, rynek MA):")
        gap_lines.append("Format: GAP_ID | USŁUGA | LICZBA KONKURENTÓW | ŚREDNIA CENA RYNKU | POPULARNOŚĆ")
        for g in missing[:10]:
            gap_lines.append(
                f"{g['id']} | {g.get('treatment_name') or 'Unknown'} | "
                f"{g.get('competitor_count') or 0} z rynku | "
                f"{_format_price_grosze(g.get('avg_price_grosze'))} | "
                f"pop. {float(g.get('popularity_score') or 0):.1f}"
            )
    else:
        gap_lines.append("BRAKUJĄCE USŁUGI: brak — Twoje portfolio pokrywa wszystko co ma rynek.")

    gap_lines.append("")
    if uniques:
        gap_lines.append("UNIKALNE USP (Twój salon MA, żaden konkurent w rynku NIE ma):")
        gap_lines.append("Format: GAP_ID | USŁUGA | TWOJA CENA")
        for g in uniques[:5]:
            gap_lines.append(
                f"{g['id']} | {g.get('treatment_name') or 'Unknown'} | "
                f"{_format_price_grosze(g.get('avg_price_grosze'))}"
            )
    else:
        gap_lines.append("UNIKALNE USP: brak — wszystkie Twoje usługi są też u konkurencji.")
    gap_block = "\n".join(gap_lines)

    # ── dimensional_insights block — grouped by category ──
    by_category: dict[str, list[dict[str, Any]]] = {}
    for d in dimensions:
        cat = d.get("category") or "other"
        by_category.setdefault(cat, []).append(d)

    CATEGORY_ORDER = [
        "content_quality",
        "pricing",
        "operations",
        "digital_maturity",
        "social_proof",
        "portfolio",
    ]

    dim_lines: list[str] = [
        "Format: DIM_ID | WYMIAR | TWOJA WARTOŚĆ | MEDIANA RYNKU | PERCENTYL | BETTER_IS_HIGHER"
    ]
    for cat in CATEGORY_ORDER:
        if cat not in by_category:
            continue
        dim_lines.append(f"\n--- {cat.upper().replace('_', ' ')} ---")
        for d in by_category[cat]:
            dim_lines.append(
                f"{d['id']} | {d.get('dimension') or '?'} | "
                f"{float(d.get('subject_value') or 0):.1f} | "
                f"{float(d.get('market_p50') or 0):.1f} | "
                f"{float(d.get('subject_percentile') or 0):.0f}% | "
                f"{'↑ lepiej' if d.get('better_is_higher') else '↓ lepiej'}"
            )
    # Catch any uncategorized dimensions
    for cat, rows in by_category.items():
        if cat in CATEGORY_ORDER:
            continue
        dim_lines.append(f"\n--- {cat.upper().replace('_', ' ')} ---")
        for d in rows:
            dim_lines.append(
                f"{d['id']} | {d.get('dimension') or '?'} | "
                f"{float(d.get('subject_value') or 0):.1f} | "
                f"{float(d.get('market_p50') or 0):.1f} | "
                f"{float(d.get('subject_percentile') or 0):.0f}% | "
                f"{'↑ lepiej' if d.get('better_is_higher') else '↓ lepiej'}"
            )
    dim_block = "\n".join(dim_lines)

    # Fill template
    result = template
    for key, value in [
        ("subject_context", subject_block),
        ("competitors_summary", comp_block),
        ("pricing_insights", pricing_block),
        ("gap_insights", gap_block),
        ("dimensional_insights", dim_block),
    ]:
        result = result.replace(f"{{{key}}}", value)
    return result


# ---------------------------------------------------------------------------
# MiniMax runner + result extraction
# ---------------------------------------------------------------------------


async def _run_minimax_synthesis(context: str) -> dict[str, Any]:
    """Call MiniMax agent loop once with the synthesis prompt.

    Returns the parsed insights dict. Raises on failure (caller handles fallback).

    Uses a longer per-request timeout than the default MiniMax client (240s
    instead of 120s) because synthesis prompts are larger and the model's
    interleaved-thinking + tool_use loop can take significantly longer than
    a typical naming/description batch call.
    """
    import anthropic
    import httpx

    from agent.runner import run_agent_loop
    from config import settings
    from services.minimax import MiniMaxClient

    client = MiniMaxClient(
        settings.minimax_api_key,
        settings.minimax_base_url,
        settings.minimax_model,
    )
    # Override the underlying anthropic client for this pipeline:
    #  - timeout=240s (bigger prompt + interleaved thinking + tool_use)
    #  - max_retries=2 (allow 2 retries with SDK-level exponential backoff;
    #    total worst case ~12 min but synthesis quality is worth the wait)
    client.client = anthropic.AsyncAnthropic(
        base_url=settings.minimax_base_url,
        api_key=settings.minimax_api_key,
        default_headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
        timeout=httpx.Timeout(240.0, connect=15.0),
        max_retries=2,
    )

    agent_result = await run_agent_loop(
        client=client,
        system_prompt=(
            "Jesteś strategiem marketingowym specjalizującym się w polskich "
            "salonach beauty na Booksy.pl. Odpowiadasz po polsku, używasz tylko "
            "prawdziwych liczb z dostarczonych danych, nigdy nie zmyślasz."
        ),
        user_message=context,
        tools=[COMPETITOR_INSIGHTS_TOOL],
        max_steps=3,
    )

    return _extract_insights_from_agent_result(agent_result)


def _extract_insights_from_agent_result(agent_result: Any) -> dict[str, Any]:
    """Pull submit_competitor_insights tool call(s) from agent result.

    The agent normally calls once with everything. If the agent splits into
    multiple calls (e.g. one for SWOT, another for recommendations), we merge
    them. If no valid tool call found, raises ValueError.
    """
    narrative = ""
    swot: dict[str, list[Any]] = {
        "strengths": [],
        "weaknesses": [],
        "opportunities": [],
        "threats": [],
    }
    recommendations: list[dict[str, Any]] = []

    for call in agent_result.tool_calls or []:
        if call.name != "submit_competitor_insights":
            continue
        payload = call.input if isinstance(call.input, dict) else {}
        if not narrative and isinstance(payload.get("positioning_narrative"), str):
            narrative = payload["positioning_narrative"].strip()
        call_swot = payload.get("swot")
        if isinstance(call_swot, dict):
            for key in ("strengths", "weaknesses", "opportunities", "threats"):
                val = call_swot.get(key)
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, dict) and item.get("text"):
                            swot[key].append(item)
        call_recs = payload.get("recommendations")
        if isinstance(call_recs, list):
            for rec in call_recs:
                if isinstance(rec, dict) and rec.get("actionTitle"):
                    recommendations.append(rec)

    # Require narrative AND at least one of (SWOT bullets, recommendations).
    # Accepting partial output (e.g. SWOT only or recs only) is still better
    # than the deterministic fallback. The frontend already handles empty
    # arrays in either dimension gracefully (Etap 6).
    if not narrative:
        raise ValueError("MiniMax did not return positioning_narrative")
    has_any_swot = any(len(v) > 0 for v in swot.values())
    has_recommendations = len(recommendations) > 0
    if not has_any_swot and not has_recommendations:
        raise ValueError("MiniMax returned neither SWOT bullets nor recommendations")

    return {
        "positioning_narrative": narrative,
        "swot": swot,
        "recommendations": recommendations,
    }


# ---------------------------------------------------------------------------
# Sanitization — validate source references point to real IDs
# ---------------------------------------------------------------------------


def _sanitize_source_data_points(
    raw: Any, valid_ids: dict[str, set[int]],
) -> list[dict[str, Any]]:
    """Drop any source data points whose (type, id) doesn't exist in valid_ids.

    This protects against AI hallucinating IDs and keeps the traceability
    links valid for the frontend.
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        t = item.get("type")
        i = item.get("id")
        if not isinstance(t, str) or not isinstance(i, int):
            continue
        valid_set = valid_ids.get(t)
        if valid_set is None or i not in valid_set:
            continue
        out.append({"type": t, "id": i})
    return out


def _sanitize_competitor_ids(
    raw: Any, valid_competitor_ids: set[int],
) -> list[int]:
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for v in raw:
        try:
            iv = int(v)
        except (TypeError, ValueError):
            continue
        if iv in valid_competitor_ids:
            out.append(iv)
    return out


def _sanitize_insights(
    insights: dict[str, Any],
    *,
    valid_ids: dict[str, set[int]],
    valid_competitor_ids: set[int],
) -> dict[str, Any]:
    """Drop invalid source references and coerce types so the DB insert
    can't fail on bad IDs. If a SWOT bullet ends up with zero valid refs,
    the bullet is dropped entirely (traceability is mandatory).
    """
    sanitized_swot: dict[str, list[dict[str, Any]]] = {
        "strengths": [],
        "weaknesses": [],
        "opportunities": [],
        "threats": [],
    }
    for key in sanitized_swot.keys():
        for bullet in insights["swot"].get(key, []) or []:
            if not isinstance(bullet, dict):
                continue
            text = (bullet.get("text") or "").strip()
            if not text:
                continue
            refs = _sanitize_source_data_points(
                bullet.get("sourceDataPoints"), valid_ids,
            )
            if not refs:
                # Drop bullets that lost all traceability — this is an
                # intentional stricter contract than the prompt asks for.
                logger.debug("Dropping SWOT bullet with no valid refs: %s", text[:60])
                continue
            sanitized_swot[key].append({"text": text, "sourceDataPoints": refs})

    sanitized_recs: list[dict[str, Any]] = []
    for rec in insights.get("recommendations") or []:
        if not isinstance(rec, dict):
            continue
        action_title = (rec.get("actionTitle") or "").strip()
        action_desc = (rec.get("actionDescription") or "").strip()
        if not action_title or not action_desc:
            continue
        category = rec.get("category") or "content"
        if category not in ("pricing", "content", "services", "operations", "social"):
            category = "content"
        impact = rec.get("impact") or "medium"
        if impact not in ("low", "medium", "high"):
            impact = "medium"
        effort = rec.get("effort") or "medium"
        if effort not in ("low", "medium", "high"):
            effort = "medium"
        try:
            confidence = float(rec.get("confidence") or 0.5)
        except (TypeError, ValueError):
            confidence = 0.5
        confidence = max(0.0, min(1.0, confidence))

        revenue_raw = rec.get("estimatedRevenueImpactGrosze")
        revenue: int | None
        try:
            revenue = int(revenue_raw) if revenue_raw is not None else None
        except (TypeError, ValueError):
            revenue = None

        refs = _sanitize_source_data_points(
            rec.get("sourceDataPoints"), valid_ids,
        )
        if not refs:
            logger.debug(
                "Dropping recommendation with no valid refs: %s", action_title[:60],
            )
            continue
        comp_ids = _sanitize_competitor_ids(
            rec.get("sourceCompetitorIds"), valid_competitor_ids,
        )

        sanitized_recs.append({
            "actionTitle": action_title[:200],
            "actionDescription": action_desc[:800],
            "category": category,
            "impact": impact,
            "effort": effort,
            "confidence": confidence,
            "estimatedRevenueImpactGrosze": revenue,
            "sourceCompetitorIds": comp_ids,
            "sourceDataPoints": refs,
        })

    narrative = (insights.get("positioning_narrative") or "").strip()
    if len(narrative) > 800:
        narrative = narrative[:800]

    return {
        "positioning_narrative": narrative,
        "swot": sanitized_swot,
        "recommendations": sanitized_recs,
    }


# ---------------------------------------------------------------------------
# Deterministic fallback — used when MiniMax fails
# ---------------------------------------------------------------------------


def _deterministic_fallback(
    *,
    subject_context: dict[str, Any],
    matches: list[dict[str, Any]],
    pricing: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    dimensions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Fallback insights built from pure data, no AI.

    Produces a basic but valid structure: narrative from dimensional scores,
    empty SWOT (we can't hand-craft good bullets), and up to 3 basic
    recommendations derived from the top pricing deviations. This ensures the
    user always gets actionable items even on total AI failure.
    """
    salon_name = subject_context.get("salon_name") or "Salon"
    total_competitors = len(matches)

    # Simple narrative: count percentile wins/losses
    wins = 0
    losses = 0
    for d in dimensions:
        pct = float(d.get("subject_percentile") or 50)
        better_high = bool(d.get("better_is_higher"))
        if (better_high and pct >= 70) or (not better_high and pct <= 30):
            wins += 1
        elif (better_high and pct <= 30) or (not better_high and pct >= 70):
            losses += 1

    narrative = (
        f"{salon_name} został porównany z {total_competitors} konkurentami "
        f"w {len(dimensions)} wymiarach. W {wins} wymiarach salon wypada "
        f"lepiej od mediany rynku, w {losses} gorzej. Pełna analiza "
        f"strategiczna jest chwilowo niedostępna — dane konkurencji są "
        f"gotowe do przeglądu w zakładkach Cennik, Luki i SWOT."
    )
    # Clamp to max 800 chars (matches tool schema)
    if len(narrative) > 800:
        narrative = narrative[:797] + "..."

    # Build up to 3 basic recommendations from the top pricing deviations.
    # This ensures users always get actionable items even when MiniMax fails.
    recommendations: list[dict[str, Any]] = []
    if pricing:
        sorted_by_deviation = sorted(
            pricing,
            key=lambda p: abs(float(p.get("deviation_pct") or 0)),
            reverse=True,
        )
        for p in sorted_by_deviation[:3]:
            deviation = float(p.get("deviation_pct") or 0)
            treatment = p.get("treatment_name") or "Usługa"
            subject_price = p.get("subject_price_grosze")
            median_price = p.get("market_median_grosze")
            pricing_id = p.get("id")

            if abs(deviation) < 5:
                continue  # Skip negligible deviations

            if deviation > 0:
                action_title = f"Rozważ obniżenie ceny: {treatment}"
                action_desc = (
                    f"Twoja cena za {treatment} jest {abs(deviation):.0f}% powyżej "
                    f"mediany rynkowej"
                    + (f" ({float(median_price) / 100:.0f} zł)" if median_price else "")
                    + ". Rozważ dostosowanie ceny, aby być bardziej konkurencyjnym, "
                    f"lub wzmocnij komunikację wartości tej usługi."
                )
                category = "pricing"
            else:
                action_title = f"Rozważ podniesienie ceny: {treatment}"
                action_desc = (
                    f"Twoja cena za {treatment} jest {abs(deviation):.0f}% poniżej "
                    f"mediany rynkowej"
                    + (f" ({float(median_price) / 100:.0f} zł)" if median_price else "")
                    + ". Podniesienie ceny może zwiększyć przychód bez utraty klientów."
                )
                category = "pricing"

            source_data_points: list[dict[str, Any]] = []
            if pricing_id is not None:
                source_data_points.append({"type": "pricing_comparison", "id": int(pricing_id)})

            recommendations.append({
                "actionTitle": action_title[:200],
                "actionDescription": action_desc[:800],
                "category": category,
                "impact": "medium",
                "effort": "low",
                "confidence": 0.7,
                "estimatedRevenueImpactGrosze": None,
                "sourceCompetitorIds": [],
                "sourceDataPoints": source_data_points,
            })

    return {
        "positioning_narrative": narrative,
        "swot": {
            "strengths": [],
            "weaknesses": [],
            "opportunities": [],
            "threats": [],
        },
        "recommendations": recommendations,
    }


# ---------------------------------------------------------------------------
# Deterministic enrichment builders for frontend rich UI
# ---------------------------------------------------------------------------
#
# These helpers reshape data already loaded from competitor_matches /
# competitor_pricing_comparisons / competitor_service_gaps /
# competitor_dimensional_scores into the JSON shape expected by the
# BEAUTY_AUDIT competitor report rich UI (components/results/competitor/rich).
#
# Pure functions, no external IO, no AI — adding fields the adapter currently
# falls back to fixture for: competitorProfiles, priceComparison,
# scoreBreakdown, opportunities. Each helper logs counts so the bagent log
# tells QA exactly what landed in report_data.


_BUCKET_FALLBACK = "cluster"
_DEFAULT_OVERLAP = 0.5


def _build_competitor_profiles(
    matches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build competitorProfiles array consumed by the rich report's competitor
    radar / list views.

    Maps each competitor_matches row (already enriched with salon_name /
    reviews_rank / reviews_count / city / distance_km / bucket /
    composite_score by SupabaseService.get_competitor_matches) into the flat
    profile dict the React adapter looks for in reportData.competitorProfiles.

    Schema (matches what mapCompetitorsFromBagent in
    components/results/competitor/rich/adaptToReportData.ts reads):
      id, name, city, distance_km, composite_score, bucket, reviews_rank,
      reviews_count, salon_id, booksy_id.

    The React adapter has a fallback for lat/lng (Warsaw default) when those
    are missing, so we don't fetch them here — keeps this helper pure and
    cheap. Future enrichment (lat/lng/top_services/working_hours) should go
    through a separate Supabase batch fetch in the synthesis pipeline.
    """
    profiles: list[dict[str, Any]] = []
    for idx, m in enumerate(matches):
        salon_id = m.get("competitor_salon_id")
        booksy_id = m.get("booksy_id")
        if salon_id is None and booksy_id is None:
            continue
        profiles.append({
            "id": int(salon_id) if salon_id is not None else int(booksy_id or 0),
            "salon_id": int(salon_id) if salon_id is not None else None,
            "booksy_id": int(booksy_id) if booksy_id is not None else None,
            "name": m.get("salon_name") or f"Konkurent {idx + 1}",
            "city": m.get("city") or "—",
            "distance_km": float(m.get("distance_km") or 0.0),
            "bucket": m.get("bucket") or _BUCKET_FALLBACK,
            "composite_score": float(m.get("composite_score") or 0.0),
            "reviews_rank": float(m.get("reviews_rank") or 0.0),
            "reviews_count": int(m.get("reviews_count") or 0),
            "overlap": _DEFAULT_OVERLAP,
        })
    logger.info(
        "_build_competitor_profiles: built %d profiles from %d matches",
        len(profiles), len(matches),
    )
    return profiles


def _build_price_comparison(
    pricing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build priceComparison array — flat per-service rows for the rich UI's
    pricing table / sliders.

    competitor_pricing_comparisons rows already contain everything the
    adapter (mapPricingFromBagent) needs: treatment_name, subject_price_grosze,
    market_min/p25/p50/p75/max grosze, deviation_pct, sample_size,
    recommended_action. We pass them through verbatim, only sorting by
    absolute deviation so the most interesting rows render first.
    """
    rows: list[dict[str, Any]] = []
    for p in pricing:
        if p.get("treatment_name") is None:
            continue
        rows.append({
            "id": p.get("id"),
            "treatment_name": p.get("treatment_name"),
            "category": p.get("category"),
            "subject_price_grosze": p.get("subject_price_grosze"),
            "market_min_grosze": p.get("market_min_grosze"),
            "market_p25_grosze": p.get("market_p25_grosze"),
            "market_median_grosze": p.get("market_median_grosze"),
            "market_p75_grosze": p.get("market_p75_grosze"),
            "market_max_grosze": p.get("market_max_grosze"),
            "subject_percentile": p.get("subject_percentile"),
            "deviation_pct": p.get("deviation_pct"),
            "sample_size": p.get("sample_size"),
            "subject_duration_minutes": p.get("subject_duration_minutes"),
            "recommended_action": p.get("recommended_action") or "hold",
        })
    rows.sort(
        key=lambda r: abs(float(r.get("deviation_pct") or 0)),
        reverse=True,
    )
    logger.info(
        "_build_price_comparison: built %d rows from %d pricing comparisons",
        len(rows), len(pricing),
    )
    return rows


# Category labels rendered in the score breakdown card (Polish, salon-owner
# friendly). Keys must match dimensional_scores.category values.
_SCORE_CATEGORY_LABELS: dict[str, str] = {
    "content_quality": "Jakość treści",
    "pricing": "Cennik",
    "operations": "Operacje",
    "digital_maturity": "Dojrzałość cyfrowa",
    "social_proof": "Społeczny dowód słuszności",
    "portfolio": "Portfolio usług",
}


def _build_score_breakdown(
    dimensions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build scoreBreakdown — one row per dimensional category, with the
    average subject_percentile across that category.

    The rich UI's "score breakdown" card expects `{area, score, marketAvg}`
    rows; we map percentile → score (0-100 scale already), and surface a
    constant 50 for marketAvg (since percentile is by definition relative to
    the market median = 50). Zero-dimension categories are skipped.
    """
    by_cat: dict[str, list[dict[str, Any]]] = {}
    for d in dimensions:
        cat = d.get("category") or "other"
        by_cat.setdefault(cat, []).append(d)

    rows: list[dict[str, Any]] = []
    for cat, items in by_cat.items():
        if not items:
            continue
        percentiles = [
            float(d.get("subject_percentile") or 50.0) for d in items
        ]
        avg = sum(percentiles) / max(1, len(percentiles))
        rows.append({
            "area": _SCORE_CATEGORY_LABELS.get(cat, cat.replace("_", " ").title()),
            "category": cat,
            "score": round(avg, 1),
            "marketAvg": 50.0,
            "dimensionCount": len(items),
        })
    # Stable order — alphabetical by area
    rows.sort(key=lambda r: r["area"])
    logger.info(
        "_build_score_breakdown: built %d areas from %d dimensions",
        len(rows), len(dimensions),
    )
    return rows


def _build_opportunities(
    gaps: list[dict[str, Any]],
    pricing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build opportunities — top market gaps the salon could exploit.

    Two sources:
      1. service_gaps with gap_type='missing' and high popularity_score
         → "Add this service" opportunity
      2. pricing rows with deviation_pct < -15 (subject priced well below
         market) → "Raise price" opportunity

    Returns up to 8 items sorted by estimated impact descending. Each item
    has {kind, title, detail, impactGrosze (estimated), evidenceCount, area}.
    """
    items: list[dict[str, Any]] = []

    for g in gaps:
        if g.get("gap_type") != "missing":
            continue
        popularity = float(g.get("popularity_score") or 0)
        comp_count = int(g.get("competitor_count") or 0)
        avg_price = int(g.get("avg_price_grosze") or 0)
        if popularity < 30 and comp_count < 2:
            continue
        # Rough impact estimate: avg_price × competitor_count × 0.5 (half of
        # the competitor base is a reasonable share-of-wallet capture rate).
        estimated_impact = int(avg_price * comp_count * 0.5)
        items.append({
            "kind": "missing_service",
            "area": "Portfolio",
            "title": f"Dodaj usługę: {g.get('treatment_name') or 'Brak nazwy'}",
            "detail": (
                f"{comp_count} konkurent(ów) ma tę usługę w cenniku, popularność {popularity:.0f}/100. "
                f"Średnia cena rynku: {avg_price / 100:.0f} zł."
            ),
            "impactGrosze": estimated_impact,
            "evidenceCount": comp_count,
            "popularityScore": popularity,
            "sourceGapId": g.get("id"),
        })

    for p in pricing:
        deviation = float(p.get("deviation_pct") or 0)
        if deviation > -15:  # only interesting when underpriced by >15%
            continue
        median = int(p.get("market_median_grosze") or 0)
        subject = int(p.get("subject_price_grosze") or 0)
        if median <= 0 or subject <= 0:
            continue
        # Conservative impact: 60% of the gap × 30 sessions/month assumption
        per_session_gain = (median - subject)
        if per_session_gain <= 0:
            continue
        estimated_impact = int(per_session_gain * 0.6 * 30)
        items.append({
            "kind": "underpriced",
            "area": "Cennik",
            "title": f"Podnieś cenę: {p.get('treatment_name') or 'Usługa'}",
            "detail": (
                f"Twoja cena {subject / 100:.0f} zł jest {abs(deviation):.0f}% poniżej "
                f"mediany rynku ({median / 100:.0f} zł, próba {p.get('sample_size') or 0} konkurentów)."
            ),
            "impactGrosze": estimated_impact,
            "evidenceCount": int(p.get("sample_size") or 0),
            "deviationPct": deviation,
            "sourcePricingId": p.get("id"),
        })

    items.sort(key=lambda x: x.get("impactGrosze") or 0, reverse=True)
    items = items[:8]
    logger.info(
        "_build_opportunities: built %d items (%d gaps + %d pricing rows examined)",
        len(items), len(gaps), len(pricing),
    )
    return items


# ---------------------------------------------------------------------------
# seasonalCalendar — 12-month intensity per primary salon category
# ---------------------------------------------------------------------------
#
# Polish beauty market seasonality is well-documented. Different category
# clusters peak in different months (e.g. makeup peaks May-June for weddings,
# face care peaks October-April when no sun). We map primary_category_name to
# a category cluster and produce a 12-month array. This is deterministic per
# category — not per individual salon — but it's still real market knowledge
# rather than fixture data.

_SEASONAL_CALENDAR_GENERIC: list[dict[str, Any]] = [
    {"month": "Sty", "event": "Nowy rok", "intensity": 60, "topCategory": "Detoks + odżywianie", "note": "Wolniej po świętach"},
    {"month": "Lut", "event": "Walentynki", "intensity": 85, "topCategory": "Makijaż + manicure", "note": "Pakiety dla par"},
    {"month": "Mar", "event": "Dzień Kobiet", "intensity": 90, "topCategory": "Vouchery", "note": "Szczyt sprzedaży voucherów"},
    {"month": "Kwi", "event": "Wielkanoc", "intensity": 75, "topCategory": "Oczyszczanie twarzy", "note": "Przygotowanie na wiosnę"},
    {"month": "Maj", "event": "Komunie + wesela", "intensity": 95, "topCategory": "Makijaż + depilacja", "note": "Szczytowy sezon"},
    {"month": "Cze", "event": "Wakacje start", "intensity": 88, "topCategory": "Depilacja laserowa", "note": "Bikini-ready"},
    {"month": "Lip", "event": "Lato", "intensity": 55, "topCategory": "Pedicure", "note": "Spadek — wyjazdy"},
    {"month": "Sie", "event": "Lato", "intensity": 50, "topCategory": "Pielęgnacja pourlopowa", "note": "Najwolniejszy miesiąc"},
    {"month": "Wrz", "event": "Powrót", "intensity": 80, "topCategory": "Laser + mezoterapia", "note": "Start sezonu anti-age"},
    {"month": "Paź", "event": "Jesień", "intensity": 78, "topCategory": "Peelingi chemiczne", "note": "Mocne zabiegi, brak słońca"},
    {"month": "Lis", "event": "Black Week", "intensity": 98, "topCategory": "Pakiety / serie", "note": "SZCZYT promocji — musisz być widoczny"},
    {"month": "Gru", "event": "Święta", "intensity": 92, "topCategory": "Vouchery + makijaż", "note": "Prezenty + Sylwester"},
]

# Category-specific overrides: keys are lowercase substrings matched against
# subject_context["primary_category_name"]. First match wins.
_SEASONAL_CALENDAR_CATEGORY_OVERRIDES: dict[str, dict[int, dict[str, Any]]] = {
    "fryzjer": {
        # Hair salons: heavier December (events), steadier rest of year
        4: {"intensity": 88, "topCategory": "Strzyżenie + farbowanie", "note": "Sezon weselny rozkręca się"},
        7: {"intensity": 65, "note": "Wakacyjne strzyżenia + koloryzacje"},
    },
    "paznok": {
        # Nails: peak before holidays, Walentynki, May
        2: {"intensity": 92, "topCategory": "Manicure + pedicure", "note": "Walentynki — peak nail art"},
        5: {"intensity": 92, "note": "Komunie + wesela — pełne kalendarze"},
        11: {"intensity": 95, "note": "Boże Narodzenie — manicure na święta"},
    },
    "kosmetyc": {
        # Beauty/face: October-April peak (no sun for chemical peels)
        9: {"intensity": 88, "topCategory": "Peelingi + mezoterapia", "note": "Start sezonu anti-age"},
        10: {"intensity": 92, "topCategory": "Peelingi chemiczne + laser", "note": "Idealne warunki — brak słońca"},
        2: {"intensity": 88, "note": "Kontynuacja zabiegów anti-age"},
    },
    "spa": {
        # SPA: steady year-round, slightly higher winter (relaxation), lower summer
        0: {"intensity": 78, "note": "Detoks po świętach + relaks"},
        6: {"intensity": 50, "note": "Wakacje — klienci wyjeżdżają"},
        7: {"intensity": 48, "note": "Najwolniejszy miesiąc"},
    },
}


def _build_seasonal_calendar(subject_context: dict[str, Any]) -> list[dict[str, Any]]:
    """Build seasonalCalendar — 12-month intensity calendar adapted to the
    salon's primary category. Returns the generic Polish beauty market
    seasonality with category-specific overrides applied per month.
    """
    primary_cat = (subject_context.get("primary_category_name") or "").lower()
    calendar = [dict(m) for m in _SEASONAL_CALENDAR_GENERIC]

    for keyword, overrides in _SEASONAL_CALENDAR_CATEGORY_OVERRIDES.items():
        if keyword in primary_cat:
            for month_idx, override in overrides.items():
                if 0 <= month_idx < 12:
                    calendar[month_idx].update(override)
            break

    logger.info(
        "_build_seasonal_calendar: built 12 months for category='%s'",
        subject_context.get("primary_category_name") or "(unknown)",
    )
    return calendar


# ---------------------------------------------------------------------------
# shortStrategy — 4-week plan (Tyg. 3-6) following the 14-day action plan
# ---------------------------------------------------------------------------
#
# The action_plan covers days 1-14 (weeks 1-2). shortStrategy continues with
# weeks 3-6 grouping recommendations into themed weeks: foundation, first
# campaigns, optimization, scaling. Tasks are derived from recommendations'
# action titles.

_SHORT_STRATEGY_TEMPLATE: list[dict[str, Any]] = [
    {
        "week": "Tyg. 3",
        "title": "Fundament pod reklamy",
        "default_tasks": [
            "Konfiguracja Google Business Profile — pełne dane, zdjęcia, 30+ postów",
            "Setup Meta Business Suite + pixel FB/IG na stronie",
            "Google Analytics 4 + cele konwersji (rezerwacje, telefony)",
        ],
    },
    {
        "week": "Tyg. 4",
        "title": "Pierwsze kampanie testowe",
        "default_tasks": [
            "Google Ads — kampania Search na usługi TOP",
            "Meta Ads — kampania lead gen na formularz + test 3 kreacji",
            "Baseline KPI: CPL, CPA, CTR dla każdego kanału",
        ],
    },
    {
        "week": "Tyg. 5",
        "title": "Optymalizacja + remarketing",
        "default_tasks": [
            "Remarketing FB na osoby które weszły na stronę a nie zarezerwowały",
            "Google Ads — rozszerzenie o Performance Max",
            "Landing page dedykowany pod kampanie",
        ],
    },
    {
        "week": "Tyg. 6",
        "title": "Skalowanie + Email automation",
        "default_tasks": [
            "Email drip do nowych leadów (3 wiadomości, automat)",
            "Google Maps Ads (Local Services) — pilot w promieniu 5 km",
            "Raport miesięczny: co działa, co ciąć, gdzie dosypać budżet",
        ],
    },
]


def _build_short_strategy(recommendations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build shortStrategy — 4-week plan continuing after the 14-day action
    plan. Each week gets default media-ramp tasks PLUS up to 1 recommendation
    title surfaced from the AI synthesis where impact aligns.

    Mapping recommendation impact → week:
      - high impact + low effort → Tyg. 3 (foundation)
      - high impact + medium/high effort → Tyg. 4 (first campaigns)
      - medium impact → Tyg. 5 (optimization)
      - low impact → Tyg. 6 (scaling/measure)
    """
    weeks: list[dict[str, Any]] = []

    high_low = [r for r in recommendations if r.get("impact") == "high" and r.get("effort") == "low"]
    high_other = [r for r in recommendations if r.get("impact") == "high" and r.get("effort") != "low"]
    medium = [r for r in recommendations if r.get("impact") == "medium"]
    low = [r for r in recommendations if r.get("impact") == "low"]
    week_recs = [high_low, high_other, medium, low]

    for idx, template in enumerate(_SHORT_STRATEGY_TEMPLATE):
        tasks = [{"t": t, "done": False} for t in template["default_tasks"]]
        # Surface up to 1 AI recommendation per week to make the plan feel
        # specific to the salon's audit, not just a generic media playbook.
        rec_pool = week_recs[idx]
        if rec_pool:
            top = rec_pool[0]
            tasks.insert(0, {
                "t": (top.get("actionTitle") or "Akcja z analizy konkurencji")[:120],
                "done": False,
                "fromRecommendation": True,
            })
        weeks.append({
            "week": template["week"],
            "title": template["title"],
            "tasks": tasks,
        })

    logger.info(
        "_build_short_strategy: built %d weeks (%d high-low, %d high-other, %d medium, %d low recs)",
        len(weeks), len(high_low), len(high_other), len(medium), len(low),
    )
    return weeks


# ---------------------------------------------------------------------------
# longStrategy — 12-month plan with 4 quarterly milestones
# ---------------------------------------------------------------------------


def _build_long_strategy(
    recommendations: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    dimensions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build longStrategy — 4 quarters, each with title/goal/outcomes.

    Q1: stabilization + first paid media (uses pricing recs as quick wins)
    Q2: scaling channels (uses high-impact recs)
    Q3: content + authority (driven by content_quality dim weakness)
    Q4: services expansion (uses missing service gaps)

    Revenue delta = sum of estimatedRevenueImpactGrosze for recs assigned to
    the quarter, fallback to constant tiers when AI didn't quantify.
    """
    pricing_recs = [r for r in recommendations if r.get("category") == "pricing"]
    service_recs = [r for r in recommendations if r.get("category") == "services"]
    content_recs = [r for r in recommendations if r.get("category") == "content"]
    ops_recs = [r for r in recommendations if r.get("category") == "operations"]

    missing_gaps = [g for g in gaps if g.get("gap_type") == "missing"][:3]
    weak_dims = [
        d for d in dimensions
        if float(d.get("subject_percentile") or 50) < 35
    ]

    def _sum_grosze(recs: list[dict[str, Any]]) -> int:
        total = 0
        for r in recs:
            v = r.get("estimatedRevenueImpactGrosze")
            if isinstance(v, int):
                total += v
        return total

    q1_outcomes = [
        "Google + Meta Ads działają na płatnym ruchu",
        "Email baza 300+ kontaktów",
        "CPA < 120 zł",
    ]
    if pricing_recs:
        q1_outcomes.insert(0, (pricing_recs[0].get("actionTitle") or "Optymalizacja cennika")[:80])

    q2_outcomes = [
        "Performance Max + Search + Maps razem",
        "Remarketing wielopoziomowy (FB, IG, Google)",
        "Landing page A/B testowany",
    ]
    if service_recs:
        q2_outcomes.insert(0, (service_recs[0].get("actionTitle") or "Rozbudowa portfolio usług")[:80])

    q3_outcomes = [
        "Blog / Reels z poradnikami (12+ materiałów)",
        "Program lojalnościowy dla stałych klientów",
        "Wejście do 3 lokalnych rankingów",
    ]
    if content_recs:
        q3_outcomes.insert(0, (content_recs[0].get("actionTitle") or "Content marketing — opisy + zdjęcia")[:80])
    elif weak_dims:
        q3_outcomes.insert(0, f"Poprawa wymiaru: {weak_dims[0].get('dimension') or 'jakość treści'}")

    q4_outcomes = [
        "Sprzedaż voucherów online (+8% rocznie)",
        "Średni paragon +18%",
    ]
    if missing_gaps:
        names = ", ".join((g.get("treatment_name") or "Nowa usługa") for g in missing_gaps[:3])
        q4_outcomes.insert(0, f"3 nowe usługi wprowadzone ({names})")
    if ops_recs:
        q4_outcomes.insert(0, (ops_recs[0].get("actionTitle") or "Wydłużenie godzin / weekendy")[:80])

    quarters = [
        {
            "q": "Q1",
            "months": "Miesiące 1-3",
            "title": "Stabilizacja + uruchomienie reklam",
            "goal": "Pełny setup i pierwsze ROI-dodatnie kampanie",
            "outcomes": q1_outcomes[:4],
            "adsFocus": True,
            "revenueDeltaGrosze": _sum_grosze(pricing_recs[:2]) or 620000,
        },
        {
            "q": "Q2",
            "months": "Miesiące 4-6",
            "title": "Skalowanie kanałów",
            "goal": "Podwojenie budżetu na najlepsze kampanie",
            "outcomes": q2_outcomes[:4],
            "adsFocus": True,
            "revenueDeltaGrosze": _sum_grosze(service_recs[:1]) + 1000000,
        },
        {
            "q": "Q3",
            "months": "Miesiące 7-9",
            "title": "Content + autorytet",
            "goal": "Budowa marki poza reklamami — niższy CPA długoterminowo",
            "outcomes": q3_outcomes[:4],
            "adsFocus": False,
            "revenueDeltaGrosze": _sum_grosze(content_recs) + 1500000,
        },
        {
            "q": "Q4",
            "months": "Miesiące 10-12",
            "title": "Ekspansja usług + optymalizacja",
            "goal": "Wyższy średni paragon + nowe kanały przychodu",
            "outcomes": q4_outcomes[:4],
            "adsFocus": False,
            "revenueDeltaGrosze": _sum_grosze(ops_recs) + 2200000,
        },
    ]

    logger.info(
        "_build_long_strategy: built 4 quarters (recs by cat: pricing=%d, services=%d, "
        "content=%d, ops=%d; weak_dims=%d, missing_gaps=%d)",
        len(pricing_recs), len(service_recs), len(content_recs), len(ops_recs),
        len(weak_dims), len(missing_gaps),
    )
    return quarters


# ---------------------------------------------------------------------------
# customerJourney — 5-step funnel from search to retention
# ---------------------------------------------------------------------------


def _build_customer_journey(
    subject_context: dict[str, Any],
    dimensions: list[dict[str, Any]],
    pricing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build customerJourney — 5 steps mixing real metrics with category
    benchmarks. Driven by subject_context.reviews_count/reviews_rank +
    dimension percentiles (description coverage, online services).
    """
    reviews_count = int(subject_context.get("reviews_count") or 0)
    reviews_rank = float(subject_context.get("reviews_rank") or 0.0)
    total_services = int(subject_context.get("total_services") or 0)
    salon_city = subject_context.get("salon_city") or "okolica"
    primary_cat = subject_context.get("primary_category_name") or "salon kosmetyczny"

    # Step 3 — % services with description: from content_quality dimension
    desc_dim = next(
        (d for d in dimensions if (d.get("dimension") or "").startswith("description_") or "description" in (d.get("dimension") or "")),
        None,
    )
    desc_pct = float(desc_dim.get("subject_value") or 0.0) if desc_dim else 0.0
    desc_pct_int = max(0, min(100, int(desc_pct)))

    # Estimate monthly searches from reviews_count + category. Heuristic:
    # active salons get ~80 reviews per month of organic search exposure
    # at the city level. Use floor of 800 for any salon with category.
    estimated_searches = max(800, int(reviews_count * 0.7) + 800)

    # CTR: scales with reviews_rank (4.0 → ~14%, 5.0 → ~22%)
    estimated_ctr = max(8.0, min(28.0, (reviews_rank - 3.5) * 12 + 14))

    # Conversion (booking rate from view): scales with description coverage +
    # online_services dim if available
    online_dim = next(
        (d for d in dimensions if "online" in (d.get("dimension") or "")),
        None,
    )
    has_online = bool(online_dim and float(online_dim.get("subject_value") or 0) >= 0.5)
    base_conv = 1.2 + (desc_pct / 100) * 1.0
    if has_online:
        base_conv += 0.4

    # Retention proxy from reviews_rank (each 0.1 above 4.0 → +5pp retention)
    retention = max(50, min(95, int(50 + (reviews_rank - 4.0) * 50)))

    desc_status = "good" if desc_pct_int >= 70 else "ok" if desc_pct_int >= 40 else "bad"
    conv_status = "good" if base_conv >= 1.8 else "ok" if base_conv >= 1.2 else "bad"
    rentention_status = "good" if retention >= 70 else "ok" if retention >= 55 else "bad"

    journey = [
        {
            "step": 1,
            "title": "Szuka w Google",
            "detail": f'"{primary_cat.lower()} {salon_city}" — {estimated_searches:,} wyszukiwań/mies.'.replace(",", " "),
            "metric": f"{estimated_searches:,}".replace(",", " "),
            "unit": "/ mies.",
            "status": "good" if reviews_count > 200 else "ok",
            "note": f"{reviews_count} opinii — {('mocna pozycja' if reviews_count > 500 else 'średnia widoczność' if reviews_count > 100 else 'niska widoczność')} w lokalnym rankingu",
        },
        {
            "step": 2,
            "title": "Widzi Twój profil",
            "detail": "Booksy + Google Maps + Wizytówka",
            "metric": f"~{estimated_ctr:.0f}%",
            "unit": "CTR",
            "status": "good" if estimated_ctr >= 20 else "ok" if estimated_ctr >= 14 else "bad",
            "note": f"Ocena {reviews_rank:.1f}/5 napędza klikalność. Średnia branży 22%.",
        },
        {
            "step": 3,
            "title": "Ogląda usługi",
            "detail": f"Galeria, cennik ({total_services} usług), opinie",
            "metric": f"{desc_pct_int}%",
            "unit": "z opisem",
            "status": desc_status,
            "note": (
                "Świetnie — większość usług ma opis i zdjęcie." if desc_pct_int >= 70 else
                "OK — mediana rynku to ok. 62%. Brakuje opisów części usług." if desc_pct_int >= 40 else
                "Większość usług bez opisu — tracisz konwersję."
            ),
        },
        {
            "step": 4,
            "title": "Rezerwuje",
            "detail": "Booking online lub telefon",
            "metric": f"{base_conv:.1f}%",
            "unit": "konwersja",
            "status": conv_status,
            "note": (
                "Booking online + opisy = pełna konwersja." if has_online and desc_pct_int >= 60 else
                "Rynek: 1.8% — booking wieczorny lub opisy by pomogły." if base_conv < 1.5 else
                "Solidna konwersja, jest jeszcze pole do wzrostu."
            ),
        },
        {
            "step": 5,
            "title": "Przychodzi",
            "detail": "Pierwsza wizyta → stały klient",
            "metric": f"{retention}%",
            "unit": "retencja",
            "status": rentention_status,
            "note": (
                f"Wysoka retencja — ocena {reviews_rank:.1f}/5 świadczy o jakości." if retention >= 70 else
                "OK retencja — pomyśl o programie lojalnościowym." if retention >= 55 else
                "Niska retencja — zbadaj jakość pierwszej wizyty."
            ),
        },
    ]

    logger.info(
        "_build_customer_journey: 5 steps (reviews=%d, rank=%.1f, desc%%=%d, ctr=%.0f, conv=%.1f, retention=%d)",
        reviews_count, reviews_rank, desc_pct_int, estimated_ctr, base_conv, retention,
    )
    return journey


# ---------------------------------------------------------------------------
# funnel — back-calculated views/clicks/inquiries/bookings
# ---------------------------------------------------------------------------


def _build_funnel(
    subject_context: dict[str, Any],
    pricing: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Build funnel — back-calculate from reviews_count using industry
    conversion benchmarks. Bookings are anchored on review-to-booking ratio
    (~30% of customers leave a review, so monthly bookings = reviews / 12 / 0.30).

    Returns None if reviews_count is too low to produce meaningful estimates
    (frontend handles missing funnel by hiding the section).
    """
    reviews_count = int(subject_context.get("reviews_count") or 0)
    if reviews_count < 20:
        logger.info("_build_funnel: skipped — reviews_count=%d too low", reviews_count)
        return None

    # avg subject ticket from pricing rows
    subject_prices_grosze = [
        int(p.get("subject_price_grosze") or 0)
        for p in pricing if p.get("subject_price_grosze")
    ]
    avg_ticket_zl = (
        sum(subject_prices_grosze) / len(subject_prices_grosze) / 100
        if subject_prices_grosze else 180
    )

    # Back-calculate funnel: bookings = reviews/12 / 0.30 (review rate)
    monthly_bookings = max(5, int(reviews_count / 12 / 0.30))
    # Inquiries = bookings × 1.4 (no-show / not-converted rate)
    monthly_inquiries = int(monthly_bookings * 1.4)
    # Clicks = inquiries × 8 (1 in 8 clickers actually inquires)
    monthly_clicks = int(monthly_inquiries * 8)
    # Views = clicks × 4 (25% CTR)
    monthly_views = int(monthly_clicks * 4)

    monthly_revenue_zl = int(monthly_bookings * avg_ticket_zl)

    # Lost = gap to industry benchmark (assume +25% reachable)
    benchmark_clicks = int(monthly_clicks * 1.25)
    benchmark_inquiries = int(monthly_inquiries * 1.30)
    lost_clicks = max(0, benchmark_clicks - monthly_clicks)
    lost_inquiries = max(0, benchmark_inquiries - monthly_inquiries)
    potential_revenue_zl = int(monthly_revenue_zl * 1.45)

    funnel = {
        "views": monthly_views,
        "clicks": monthly_clicks,
        "inquiries": monthly_inquiries,
        "bookings": monthly_bookings,
        "avgTicket": int(avg_ticket_zl),
        "monthlyRevenue": monthly_revenue_zl,
        "lostClicks": lost_clicks,
        "lostInquiries": lost_inquiries,
        "potentialRevenue": potential_revenue_zl,
    }
    logger.info(
        "_build_funnel: views=%d clicks=%d inq=%d book=%d ticket=%d zl revenue=%d zl",
        monthly_views, monthly_clicks, monthly_inquiries, monthly_bookings,
        int(avg_ticket_zl), monthly_revenue_zl,
    )
    return funnel


# ---------------------------------------------------------------------------
# calendarComparison — subject vs best-direct competitor working hours
# ---------------------------------------------------------------------------

_DAY_LABELS_PL = ["Pn", "Wt", "Śr", "Cz", "Pt", "Sb", "Nd"]


def _open_hours_to_grid(
    open_hours: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], int]:
    """Convert salon_scrapes.open_hours rows into a 7-day × 12-slot grid.

    Slots cover 08:00-20:00 in 1h chunks. Each slot is 1 if the salon is open
    in that hour, 0 otherwise. Occupancy is a heuristic 50-80% scaled by day
    of week (Mon=50, Fri=72, Sat=80) — bagent has no real occupancy data.

    Returns (days, totalOpenHours).
    """
    # day_of_week: 1=Mon ... 7=Sun (Booksy convention)
    by_dow: dict[int, dict[str, Any]] = {}
    if isinstance(open_hours, list):
        for oh in open_hours:
            if not isinstance(oh, dict):
                continue
            dow = oh.get("day_of_week")
            if not isinstance(dow, int) or dow < 1 or dow > 7:
                continue
            by_dow[dow] = oh

    days: list[dict[str, Any]] = []
    total_hours = 0
    occupancy_by_day = [50, 55, 60, 65, 72, 80, 30]  # Mon..Sun heuristic

    for i in range(7):
        dow = i + 1
        oh = by_dow.get(dow)
        slots = [0] * 12  # 08:00, 09:00, ..., 19:00
        if oh:
            try:
                from_h = int((oh.get("open_from") or "00:00").split(":")[0])
                till_h = int((oh.get("open_till") or "00:00").split(":")[0])
                if till_h == 0 and from_h > 0:
                    till_h = 24  # crosses midnight
                for slot_idx in range(12):
                    hour = 8 + slot_idx
                    if from_h <= hour < till_h:
                        slots[slot_idx] = 1
                        total_hours += 1
            except (ValueError, AttributeError):
                pass
        occupancy = occupancy_by_day[i] if any(slots) else 0
        days.append({
            "day": _DAY_LABELS_PL[i],
            "slots": slots,
            "occupancy": occupancy,
        })

    return days, total_hours


async def _build_calendar_comparison(
    *,
    subject_salon_id: int,
    matches: list[dict[str, Any]],
    service: SupabaseService,
) -> dict[str, Any] | None:
    """Build calendarComparison from salon_scrapes.open_hours for the subject
    and the best-fit direct competitor.

    Best-fit competitor selection:
      1. Prefer bucket='direct' with smallest distance_km
      2. Fall back to highest composite_score regardless of bucket
    """
    if not matches:
        logger.info("_build_calendar_comparison: skipped — no matches")
        return None

    # Resolve subject booksy_id from subject_salon_id
    try:
        subject_res = (
            service.client.table("salons")
            .select("booksy_id,name")
            .eq("id", subject_salon_id)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.warning("_build_calendar_comparison: failed to load subject salon: %s", e)
        return None
    if not subject_res.data:
        logger.warning("_build_calendar_comparison: subject_salon_id=%s not found", subject_salon_id)
        return None
    subject_booksy_id = subject_res.data[0].get("booksy_id")
    subject_name = subject_res.data[0].get("name") or "Twój salon"
    if subject_booksy_id is None:
        return None

    # Pick best-fit competitor
    direct_matches = [m for m in matches if m.get("bucket") == "direct"]
    if direct_matches:
        best = min(direct_matches, key=lambda m: float(m.get("distance_km") or 999))
    else:
        best = max(matches, key=lambda m: float(m.get("composite_score") or 0))
    competitor_booksy_id = best.get("booksy_id")
    competitor_name = best.get("salon_name") or "Konkurent"
    if competitor_booksy_id is None:
        return None

    # Fetch latest scrape for both
    try:
        scrape_res = (
            service.client.table("salon_scrapes")
            .select("booksy_id,raw_response,scraped_at")
            .in_("booksy_id", [subject_booksy_id, competitor_booksy_id])
            .order("scraped_at", desc=True)
            .execute()
        )
    except Exception as e:
        logger.warning("_build_calendar_comparison: scrape fetch failed: %s", e)
        return None

    latest_per_booksy: dict[int, dict[str, Any]] = {}
    for row in scrape_res.data or []:
        bid = row.get("booksy_id")
        if bid is not None and bid not in latest_per_booksy:
            latest_per_booksy[bid] = row

    def _extract_open_hours(scrape_row: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not scrape_row:
            return []
        raw = scrape_row.get("raw_response")
        if not isinstance(raw, dict):
            return []
        business = raw.get("business") if isinstance(raw.get("business"), dict) else raw
        oh = business.get("open_hours") if isinstance(business, dict) else None
        return oh if isinstance(oh, list) else []

    subject_oh = _extract_open_hours(latest_per_booksy.get(subject_booksy_id))
    competitor_oh = _extract_open_hours(latest_per_booksy.get(competitor_booksy_id))

    if not subject_oh and not competitor_oh:
        logger.info(
            "_build_calendar_comparison: no open_hours available "
            "(subject_booksy=%s, competitor_booksy=%s)",
            subject_booksy_id, competitor_booksy_id,
        )
        return None

    subject_days, subject_total = _open_hours_to_grid(subject_oh)
    competitor_days, competitor_total = _open_hours_to_grid(competitor_oh)

    result = {
        "subject": {
            "name": "Ty",
            "days": subject_days,
            "totalOpenHours": subject_total,
        },
        "competitor": {
            "name": competitor_name,
            "days": competitor_days,
            "totalOpenHours": competitor_total,
        },
        "deltaHours": competitor_total - subject_total,
    }
    logger.info(
        "_build_calendar_comparison: subject=%dh/wk vs %s=%dh/wk (delta %+d)",
        subject_total, competitor_name, competitor_total, competitor_total - subject_total,
    )
    return result


# ---------------------------------------------------------------------------
# actionPlan — 14-day diagnose+execute plan derived from recommendations + gaps
# ---------------------------------------------------------------------------
#
# Frontend's actionPlan is a flat list of 14 tasks (one per day) with
# {day, phase, task, effort, owner, category}. We slot 2 diagnostic tasks at
# the start, then thread top recommendations into the cennik (days 3-4) /
# treść (days 5-7) / usługi (days 8-10) / promocja+operacje (days 11-12) /
# mierzenie (days 13-14) phases. When a phase has no matching rec, fall back
# to a generic task.

_DEFAULT_TASK_PER_PHASE: dict[str, dict[str, str]] = {
    "Diagnoza":  {"task": "Przeczytaj raport z zespołem i wybierz priorytety", "effort": "1h",  "owner": "Ty + zespół", "category": "planning"},
    "Cennik":    {"task": "Przejrzyj cennik pod kątem usług odbiegających od rynku", "effort": "30 min", "owner": "Ty", "category": "pricing"},
    "Treść":     {"task": "Dopisz opisy do najpopularniejszych usług", "effort": "2h",  "owner": "Ty + copywriter", "category": "content"},
    "Usługi":    {"task": "Zaplanuj wprowadzenie nowej usługi z oferty rynku", "effort": "3h", "owner": "Ty + zespół", "category": "services"},
    "Promocja":  {"task": "Uruchom akcję promocyjną w mediach społecznościowych", "effort": "1h", "owner": "Social media", "category": "marketing"},
    "Operacje":  {"task": "Otwórz pilotażowo wieczorne sloty w piątek/sobotę", "effort": "30 min", "owner": "Ty", "category": "operations"},
    "Mierzenie": {"task": "Tygodniowy przegląd wyników z zespołem", "effort": "1h", "owner": "Ty + zespół", "category": "planning"},
}


def _shorten_action_title(title: str, max_len: int = 80) -> str:
    t = title.strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def _category_to_phase(category: str) -> str:
    c = (category or "").lower()
    if c == "pricing": return "Cennik"
    if c == "content": return "Treść"
    if c == "services": return "Usługi"
    if c == "operations": return "Operacje"
    if c == "social": return "Promocja"
    return "Diagnoza"


def _build_action_plan(
    recommendations: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build the 14-day actionPlan flat list.

    Days 1-2: Diagnoza
    Days 3-4: Cennik (top pricing recs, fallback to generic if none)
    Days 5-7: Treść (top content recs)
    Days 8-10: Usługi (top services recs OR top missing gaps)
    Days 11-12: Promocja + Operacje
    Days 13-14: Mierzenie

    Each task references a real recommendation when possible, with
    `actionDescription` truncated to fit the UI. Generic fallback tasks fill
    days where no relevant recommendation exists.
    """
    by_cat: dict[str, list[dict[str, Any]]] = {}
    for r in recommendations:
        cat = (r.get("category") or "").lower()
        by_cat.setdefault(cat, []).append(r)

    plan: list[dict[str, Any]] = []

    def _add_day(day: int, phase: str, task: str, *, effort: str = "1h", owner: str = "Ty", category: str = "planning"):
        plan.append({
            "day": day,
            "phase": phase,
            "task": task,
            "effort": effort,
            "owner": owner,
            "category": category,
        })

    def _next_rec(cat: str) -> dict[str, Any] | None:
        pool = by_cat.get(cat) or []
        return pool.pop(0) if pool else None

    # Days 1-2: Diagnoza (always generic — operator setup)
    _add_day(1, "Diagnoza", "Przeczytaj raport z zespołem", effort="1h", owner="Ty + zespół", category="planning")
    _add_day(2, "Diagnoza", "Wybierz 3 akcje do wdrożenia z listy rekomendacji", effort="30 min", owner="Ty", category="planning")

    # Days 3-4: Cennik
    for day in (3, 4):
        rec = _next_rec("pricing")
        if rec:
            _add_day(day, "Cennik", _shorten_action_title(rec.get("actionTitle") or "Aktualizacja cennika"), effort="30 min", owner="Ty", category="pricing")
        else:
            d = _DEFAULT_TASK_PER_PHASE["Cennik"]
            _add_day(day, "Cennik", d["task"], effort=d["effort"], owner=d["owner"], category=d["category"])

    # Days 5-7: Treść
    for day in (5, 6, 7):
        rec = _next_rec("content")
        if rec:
            _add_day(day, "Treść", _shorten_action_title(rec.get("actionTitle") or "Praca nad treściami"), effort="2h", owner="Ty + copywriter", category="content")
        else:
            d = _DEFAULT_TASK_PER_PHASE["Treść"]
            _add_day(day, "Treść", d["task"], effort=d["effort"], owner=d["owner"], category=d["category"])

    # Days 8-10: Usługi (recs first, then top missing gaps)
    missing_gaps = [g for g in gaps if g.get("gap_type") == "missing"]
    missing_gaps.sort(key=lambda g: float(g.get("popularity_score") or 0), reverse=True)
    gap_idx = 0
    for day in (8, 9, 10):
        rec = _next_rec("services")
        if rec:
            _add_day(day, "Usługi", _shorten_action_title(rec.get("actionTitle") or "Rozbudowa usług"), effort="3h", owner="Ty + zespół", category="services")
        elif gap_idx < len(missing_gaps):
            g = missing_gaps[gap_idx]
            gap_idx += 1
            name = g.get("treatment_name") or "Nowa usługa"
            _add_day(day, "Usługi", _shorten_action_title(f"Wprowadź usługę: {name}"), effort="3h", owner="Ty + zespół", category="services")
        else:
            d = _DEFAULT_TASK_PER_PHASE["Usługi"]
            _add_day(day, "Usługi", d["task"], effort=d["effort"], owner=d["owner"], category=d["category"])

    # Day 11: Promocja
    rec = _next_rec("social")
    if rec:
        _add_day(11, "Promocja", _shorten_action_title(rec.get("actionTitle") or "Akcja w social media"), effort="1h", owner="Social media", category="marketing")
    else:
        d = _DEFAULT_TASK_PER_PHASE["Promocja"]
        _add_day(11, "Promocja", d["task"], effort=d["effort"], owner=d["owner"], category=d["category"])

    # Day 12: Operacje
    rec = _next_rec("operations")
    if rec:
        _add_day(12, "Operacje", _shorten_action_title(rec.get("actionTitle") or "Optymalizacja godzin/operacji"), effort="30 min", owner="Ty", category="operations")
    else:
        d = _DEFAULT_TASK_PER_PHASE["Operacje"]
        _add_day(12, "Operacje", d["task"], effort=d["effort"], owner=d["owner"], category=d["category"])

    # Days 13-14: Mierzenie
    _add_day(13, "Mierzenie", "Sprawdź pierwsze efekty wdrożonych akcji", effort="30 min", owner="Ty", category="planning")
    _add_day(14, "Mierzenie", "Tygodniowy przegląd: co zadziałało, co zmienić", effort="1h", owner="Ty + zespół", category="planning")

    logger.info(
        "_build_action_plan: built 14 days from %d recs (pricing=%d, content=%d, services=%d, ops=%d, social=%d) + %d missing gaps",
        len(recommendations),
        len(by_cat.get("pricing") or []) + 0,  # +0 because we already popped
        len(by_cat.get("content") or []) + 0,
        len(by_cat.get("services") or []) + 0,
        len(by_cat.get("operations") or []) + 0,
        len(by_cat.get("social") or []) + 0,
        len(missing_gaps),
    )
    return plan
