"""Versum mapping suggestions — AI-powered Booksy treatment matching via MiniMax.

Takes a list of Versum salon services (name + description + category) and a
Booksy treatment taxonomy (treatmentId + treatmentName + parentCategoryName +
occurrenceCount), then uses MiniMax M2.7 tool_use to suggest the most likely
Booksy treatment match for each service.

Services are batched into groups of 25 per MiniMax call to stay well within
token limits. Each batch produces a list of suggestions with confidence 0-1.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Callable

from agent.runner import run_agent_loop
from config import settings
from services.minimax import MiniMaxClient

logger = logging.getLogger(__name__)

# --- Tool definition ---

SUGGEST_MAPPINGS_TOOL: dict = {
    "name": "submit_mapping_suggestions",
    "description": (
        "Wyślij sugerowane dopasowania usług salonu do kategorii Booksy. "
        "Wywołuj wielokrotnie — za każdym razem z kolejną partią usług. "
        "MUSISZ dopasować KAŻDĄ usługę z listy."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "serviceIndex": {
                            "type": "integer",
                            "description": "Indeks usługi z listy (0-indexed)",
                        },
                        "treatmentId": {
                            "type": "integer",
                            "description": "ID kategorii Booksy z listy KATEGORIE BOOKSY",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Pewność dopasowania 0.0-1.0 (1.0 = idealne dopasowanie)",
                        },
                        "reason": {
                            "type": "string",
                            "description": "Krótkie uzasadnienie dopasowania (max 50 znaków)",
                        },
                    },
                    "required": ["serviceIndex", "treatmentId", "confidence", "reason"],
                },
            },
        },
        "required": ["mappings"],
    },
}

# --- Prompt ---

SYSTEM_PROMPT = (
    "Jesteś ekspertem od kategoryzacji usług w salonach beauty na platformie Booksy.pl. "
    "Twoim zadaniem jest dopasowanie usług salonu do najbliższej kategorii Booksy. "
    "Dopasowuj na podstawie nazwy usługi, opisu i kategorii salonu. "
    "Jeśli usługa nie pasuje do żadnej kategorii, przypisz najbliższą i daj niską pewność (0.1-0.3). "
    "Używaj narzędzia submit_mapping_suggestions aby przesłać dopasowania."
)

BATCH_SIZE = 25


def _build_user_message(
    services: list[dict],
    taxonomy: list[dict],
    batch_offset: int,
) -> str:
    """Build the user prompt for a single batch of services."""
    lines = [
        "Dopasuj poniższe usługi salonu beauty do kategorii Booksy.",
        "Dla każdej usługi wywołaj narzędzie submit_mapping_suggestions z:",
        "- serviceIndex: indeks usługi z listy poniżej",
        "- treatmentId: ID kategorii z listy KATEGORIE BOOKSY",
        "- confidence: 0.0-1.0",
        "- reason: krótkie uzasadnienie",
        "",
        "USŁUGI SALONU:",
    ]

    for i, svc in enumerate(services):
        idx = batch_offset + i
        name = svc.get("name", "?")
        cat = svc.get("categoryName", "")
        desc = svc.get("description", "")
        parts = [f'{idx}. "{name}"']
        if cat:
            parts.append(f"(kat: {cat})")
        if desc:
            # Truncate long descriptions
            short_desc = desc[:100] + "..." if len(desc) > 100 else desc
            parts.append(f"— {short_desc}")
        lines.append(" ".join(parts))

    lines.append("")
    lines.append("KATEGORIE BOOKSY (id: nazwa [popularność]):")

    # Show top 150 taxonomy entries (sorted by occurrence) to keep prompt size manageable
    for entry in taxonomy[:150]:
        tid = entry.get("treatmentId", 0)
        tname = entry.get("treatmentName", "?")
        count = entry.get("occurrenceCount", 0)
        lines.append(f"  {tid}: {tname} [{count}×]")

    return "\n".join(lines)


async def suggest_versum_mappings(
    services: list[dict],
    taxonomy: list[dict],
    on_progress: Callable[[int, str], Any] | None = None,
) -> list[dict]:
    """Run AI-powered mapping suggestion for a list of Versum services.

    Args:
        services: list of {name, description?, categoryName?}
        taxonomy: list of {treatmentId, treatmentName, parentCategoryName?, occurrenceCount}
        on_progress: optional callback(pct, message) for reporting batch progress

    Returns:
        list of {serviceIndex, treatmentId, treatmentName, confidence, reason}
    """
    if not services:
        return []

    if not taxonomy:
        logger.warning("[VersumSuggest] Empty taxonomy — cannot suggest mappings")
        return []

    client = MiniMaxClient(
        settings.minimax_api_key,
        settings.minimax_base_url,
        settings.minimax_model,
    )

    # Build a treatmentId -> treatmentName lookup
    tid_to_name: dict[int, str] = {}
    for entry in taxonomy:
        tid_to_name[entry["treatmentId"]] = entry.get("treatmentName", f"#{entry['treatmentId']}")

    # Valid treatment IDs for validation
    valid_tids = set(tid_to_name.keys())

    total_services = len(services)
    num_batches = math.ceil(total_services / BATCH_SIZE)
    all_suggestions: list[dict] = []

    logger.info(
        "[VersumSuggest] Processing %d services in %d batches (batch size %d)",
        total_services, num_batches, BATCH_SIZE,
    )

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, total_services)
        batch = services[start:end]

        logger.info(
            "[VersumSuggest] Batch %d/%d: services %d-%d",
            batch_idx + 1, num_batches, start, end - 1,
        )

        if on_progress:
            pct = int((batch_idx / num_batches) * 100)
            on_progress(pct, f"Batch {batch_idx + 1}/{num_batches}")

        user_message = _build_user_message(batch, taxonomy, batch_offset=start)

        try:
            result = await run_agent_loop(
                client=client,
                system_prompt=SYSTEM_PROMPT,
                user_message=user_message,
                tools=[SUGGEST_MAPPINGS_TOOL],
                max_steps=10,
            )

            # Extract suggestions from tool calls
            batch_suggestions: list[dict] = []
            for call in result.tool_calls:
                if call.name == "submit_mapping_suggestions":
                    mappings = call.input.get("mappings", [])
                    for m in mappings:
                        service_index = m.get("serviceIndex")
                        treatment_id = m.get("treatmentId")
                        confidence = m.get("confidence", 0.5)
                        reason = m.get("reason", "")

                        # Validate
                        if service_index is None or treatment_id is None:
                            continue
                        if treatment_id not in valid_tids:
                            logger.warning(
                                "[VersumSuggest] Invalid treatmentId %d for service %d — skipping",
                                treatment_id, service_index,
                            )
                            continue

                        # Clamp confidence
                        confidence = max(0.0, min(1.0, float(confidence)))

                        batch_suggestions.append({
                            "serviceIndex": service_index,
                            "treatmentId": treatment_id,
                            "treatmentName": tid_to_name.get(treatment_id, f"#{treatment_id}"),
                            "confidence": round(confidence, 2),
                            "reason": str(reason)[:100],
                        })

            logger.info(
                "[VersumSuggest] Batch %d/%d: %d suggestions, %d input tokens, %d output tokens",
                batch_idx + 1, num_batches,
                len(batch_suggestions),
                result.total_input_tokens,
                result.total_output_tokens,
            )
            all_suggestions.extend(batch_suggestions)

        except Exception as e:
            logger.error(
                "[VersumSuggest] Batch %d/%d failed: %s",
                batch_idx + 1, num_batches, e,
                exc_info=True,
            )
            # Continue with remaining batches — partial results are better than none

    logger.info(
        "[VersumSuggest] Done: %d suggestions for %d services",
        len(all_suggestions), total_services,
    )
    return all_suggestions
