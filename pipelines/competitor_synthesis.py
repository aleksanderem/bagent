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

    logger.info(
        "Etap 5 enrichment built: profiles=%d, pricing=%d, score_areas=%d, opportunities=%d",
        len(competitor_profiles), len(price_comparison),
        len(score_breakdown), len(opportunities),
    )

    await progress(80, "Persystencja narratywu + SWOT + rekomendacji...")
    await service.update_competitor_report_data(
        report_id,
        {
            "positioning_narrative": insights["positioning_narrative"],
            "swot": insights["swot"],
            "competitorProfiles": competitor_profiles,
            "priceComparison": price_comparison,
            "scoreBreakdown": score_breakdown,
            "opportunities": opportunities,
        },
    )

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
