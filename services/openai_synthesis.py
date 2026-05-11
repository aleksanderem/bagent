"""OpenAI synthesis fallback for competitor reports — used when MiniMax
M2.7 fails after retries.

Why OpenAI as fallback (not Claude / Gemini):
- We already have OPENAI_API_KEY (added for embeddings)
- gpt-4o-mini supports Structured Outputs (strict JSON schema) — no tool
  loop needed, single API call returns validated JSON
- Latency ~5-10s vs MiniMax 60-600s with retries
- Cost ~$0.001 per synthesis (negligible vs the price of failed report)

Quality trade-off:
- MiniMax M2.7 with tool_use + interleaved thinking produces higher
  quality narratives + better traceability (sourceDataPoints linking)
- gpt-4o-mini single-shot is less nuanced but still way better than
  the deterministic template fallback (which has no LLM input at all)

This fallback is invoked only when MiniMax raises an exception. Caller
(competitor_synthesis.py) tries MiniMax first → OpenAI fallback → final
deterministic template.

Returns the same insights dict shape as MiniMax:
    {
        "positioning_narrative": str,
        "swot": {strengths, weaknesses, opportunities, threats: list[{text, sourceDataPoints}]},
        "recommendations": list[{actionTitle, actionDescription, category,
                                  impact, effort, confidence, sourceCompetitorIds,
                                  sourceDataPoints, estimatedRevenueImpactGrosze}]
    }
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


_OAI_CLIENT = None


def _get_openai_client():
    """Lazily instantiate AsyncOpenAI. Returns None if key missing/unavailable."""
    global _OAI_CLIENT
    if _OAI_CLIENT is None:
        if not os.getenv("OPENAI_API_KEY"):
            logger.warning("OPENAI_API_KEY missing — synthesis fallback unavailable")
            _OAI_CLIENT = False
            return None
        try:
            from openai import AsyncOpenAI
            _OAI_CLIENT = AsyncOpenAI()
        except Exception as e:
            logger.warning("OpenAI client init failed: %s", e)
            _OAI_CLIENT = False
            return None
    return _OAI_CLIENT if _OAI_CLIENT else None


# JSON schema mirroring COMPETITOR_INSIGHTS_TOOL's input_schema. Kept inline
# (not imported) to avoid circular imports with competitor_synthesis. Source
# field is `text` (the bullet sentence) — no sourceDataPoints required from
# the fallback (it's a nice-to-have we leave empty rather than hallucinate).
_INSIGHTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "positioning_narrative": {
            "type": "string",
            "description": (
                "Narratyw pozycjonowania salonu vs rynek: 200-700 znaków, "
                "po polsku, używa konkretnych liczb z kontekstu."
            ),
        },
        "swot": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "strengths": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
                "weaknesses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
                "opportunities": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
                "threats": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
            },
            "required": ["strengths", "weaknesses", "opportunities", "threats"],
        },
        "recommendations": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "actionTitle": {"type": "string"},
                    "actionDescription": {"type": "string"},
                    "category": {
                        "type": "string",
                        "enum": ["pricing", "services", "content", "operations", "social"],
                    },
                    "impact": {"type": "string", "enum": ["low", "medium", "high"]},
                    "effort": {"type": "string", "enum": ["low", "medium", "high"]},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "actionTitle",
                    "actionDescription",
                    "category",
                    "impact",
                    "effort",
                    "confidence",
                ],
            },
        },
    },
    "required": ["positioning_narrative", "swot", "recommendations"],
}


_SYSTEM_PROMPT = (
    "Jesteś strategiem marketingowym specjalizującym się w polskich salonach "
    "beauty na Booksy.pl. Odpowiadasz po polsku, używasz TYLKO prawdziwych liczb "
    "z dostarczonych danych, nigdy nie zmyślasz. Generujesz konkretne, "
    "wykonalne wnioski — nie ogólniki. Każdy bullet SWOT i każda rekomendacja "
    "ma wskazywać konkretny dataset (cena, gap, dimensional score) opisowo."
)


async def synthesize_via_openai(context: str) -> dict[str, Any]:
    """Call OpenAI gpt-4o-mini with Structured Outputs to produce insights
    in the same shape as MiniMax.

    Raises on missing API key or API failure (caller falls back to deterministic).
    Adds empty `sourceDataPoints` to SWOT bullets and empty `sourceCompetitorIds`
    / `sourceDataPoints` to recommendations so the downstream sanitizer doesn't
    complain about missing keys.
    """
    client = _get_openai_client()
    if client is None:
        raise RuntimeError("OpenAI client unavailable (no OPENAI_API_KEY)")

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        max_tokens=4000,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "competitor_insights",
                "strict": True,
                "schema": _INSIGHTS_SCHEMA,
            },
        },
    )
    raw = response.choices[0].message.content or "{}"
    parsed = json.loads(raw)

    # Normalize to MiniMax dict shape — fill sourceDataPoints/sourceCompetitorIds
    # as empty lists so _sanitize_insights doesn't drop bullets.
    for key in ("strengths", "weaknesses", "opportunities", "threats"):
        for bullet in parsed.get("swot", {}).get(key, []) or []:
            bullet.setdefault("sourceDataPoints", [])
    for rec in parsed.get("recommendations", []) or []:
        rec.setdefault("sourceCompetitorIds", [])
        rec.setdefault("sourceDataPoints", [])
        rec.setdefault("estimatedRevenueImpactGrosze", None)

    logger.info(
        "OpenAI fallback synthesis: narrative=%d chars, swot=%d items, recs=%d",
        len(parsed.get("positioning_narrative", "")),
        sum(len(parsed.get("swot", {}).get(k, [])) for k in
            ("strengths", "weaknesses", "opportunities", "threats")),
        len(parsed.get("recommendations", [])),
    )
    return parsed
