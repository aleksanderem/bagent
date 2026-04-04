"""Category proposal AI — generates category restructure suggestions."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def generate_category_proposal_text(
    client: Any,
    prompt: str,
) -> str:
    """Send category prompt to AI, return raw text response.

    The prompt is built by the Convex caller with full context (category summary,
    services preview, keyword data, audit issues). We just run the AI call.
    """
    response = await client.create_message(
        system="Jesteś ekspertem od struktury cenników beauty na Booksy.pl.",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4000,
        temperature=0.4,
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    logger.info("Category proposal generated: %d chars", len(text))
    return text


async def generate_keyword_suggestions_text(
    client: Any,
    prompt: str,
) -> str:
    """Send keyword suggestion prompt to AI, return raw text response."""
    response = await client.create_message(
        system="Jesteś ekspertem SEO dla salonów beauty.",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
        temperature=0.5,
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    return text
