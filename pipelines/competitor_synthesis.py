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

    # ── Step 4: Persist narrative + SWOT into competitor_reports.report_data ──
    await progress(80, "Persystencja narratywu + SWOT + rekomendacji...")
    await service.update_competitor_report_data(
        report_id,
        {
            "positioning_narrative": insights["positioning_narrative"],
            "swot": insights["swot"],
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
    #  - max_retries=0 (the SDK default of 2 means up to 12 minutes of retries
    #    on slow responses; we prefer the deterministic fallback to kick in
    #    quickly so the report row stays useful)
    client.client = anthropic.AsyncAnthropic(
        base_url=settings.minimax_base_url,
        api_key=settings.minimax_api_key,
        default_headers={"Authorization": f"Bearer {settings.minimax_api_key}"},
        timeout=httpx.Timeout(240.0, connect=15.0),
        max_retries=0,
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
    empty SWOT (we can't hand-craft good bullets), empty recommendations. The
    report row stays functional — the frontend renders placeholder content
    with a "AI synthesis temporarily unavailable" hint.
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

    return {
        "positioning_narrative": narrative,
        "swot": {
            "strengths": [],
            "weaknesses": [],
            "opportunities": [],
            "threats": [],
        },
        "recommendations": [],
    }
