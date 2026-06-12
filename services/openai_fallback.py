"""Generic OpenAI fallback for MiniMax failures (FINDINGS P1-7).

Competitor synthesis has had a dedicated OpenAI fallback (openai_synthesis.py)
since the start; the audit report pipeline (BAGENT #1) had none — a MiniMax
outage degraded scoring to constants ({score: 10}) with no trace in the
report. This module provides generic JSON/text generation on gpt-4o-mini so
report.py can fall back before degrading.

Reuses the lazy client from openai_synthesis (single place that knows about
OPENAI_API_KEY). Raises RuntimeError when the key/client is unavailable —
callers treat that the same as any other fallback failure.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

_FALLBACK_MODEL = "gpt-4o-mini"


async def generate_json_via_openai(
    prompt: str,
    system: str = "",
    max_tokens: int = 4096,
) -> dict[str, Any]:
    """JSON-mode chat completion. Returns the parsed dict or raises."""
    from services.openai_synthesis import _get_openai_client

    client = _get_openai_client()
    if client is None:
        raise RuntimeError("OpenAI fallback unavailable (no OPENAI_API_KEY)")

    # json_object mode requires the word "JSON" somewhere in the messages.
    system_msg = f"{system} Odpowiadaj wyłącznie poprawnym JSON.".strip()
    response = await client.chat.completions.create(
        model=_FALLBACK_MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        max_tokens=max_tokens,
        temperature=0.3,
    )
    text = response.choices[0].message.content or ""
    return json.loads(text)


async def generate_text_via_openai(
    prompt: str,
    system: str = "",
    max_tokens: int = 2048,
) -> str:
    """Plain-text chat completion. Returns the text or raises."""
    from services.openai_synthesis import _get_openai_client

    client = _get_openai_client()
    if client is None:
        raise RuntimeError("OpenAI fallback unavailable (no OPENAI_API_KEY)")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = await client.chat.completions.create(
        model=_FALLBACK_MODEL,
        messages=messages,
        max_tokens=max_tokens,
        temperature=0.7,
    )
    return (response.choices[0].message.content or "").strip()
