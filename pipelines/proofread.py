"""Polish orthography proofreading for service descriptions.

Two-layer fix that runs INSIDE BAGENT #2 (cennik pipeline) AFTER name+
description maps from BAGENT #1 are applied:

  Layer A — deterministic punctuation pass (`fix_punctuation` in helpers.py):
    catches " .", ".text", "..", trailing-no-period, capitalisation,
    runaway whitespace. Pure regex, idempotent, no AI cost.

  Layer B — batched AI orthography pass (this module):
    sends 1..N descriptions per AI call asking the model to ONLY fix typos
    (literówki) and Polish spelling errors (ortografia) without changing
    meaning, style, vocabulary, or structure. Validated character-distance
    rejects responses that drift too far from the input.

Layer A always runs (cheap). Layer B is opt-in via `proofread_with_ai`
flag because it adds ~1 AI roundtrip per ~20 descriptions and (for
salons with 200 services) can add a noticeable few seconds to cennik.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# How many descriptions go in a single AI call. Trade-off: bigger batches
# cut per-description latency but raise the chance of model returning a
# malformed JSON. 20 is a safe sweet spot empirically.
PROOFREAD_BATCH_SIZE = 20

# Max allowed character-distance ratio between original and fixed
# description. Catches model going off-rails (e.g. summarising or
# rewriting). 0.30 = up to 30% character-level drift allowed (typos
# typically <5%; we leave headroom for diacritic restorations).
MAX_DRIFT_RATIO = 0.30


_SYSTEM_PROMPT = (
    "Jesteś korektorem polskiej ortografii i interpunkcji. "
    "Twoim JEDYNYM zadaniem jest poprawić literówki i błędy ortograficzne "
    "w opisach usług kosmetycznych. "
    "ABSOLUTNIE ZACHOWAJ: znaczenie, styl, słownictwo, długość, układ, "
    "branżowy język (manicure hybrydowy, kwas hialuronowy, brow lamination), "
    "własne imiona i nazwy marek (Booksy, Versum, Dermalogica), "
    "polskie znaki diakrytyczne. "
    "DOZWOLONE poprawki: ó/u, rz/ż, ch/h, h/ø, podwójne litery, brakujące/nadmiarowe "
    "spacje wokół interpunkcji, brakująca kropka na końcu zdania. "
    "ZABRONIONE: parafraza, skracanie, dodawanie informacji, zmiana stylu, "
    "tłumaczenie obcych słów, edycja cen lub czasów."
)


def _char_distance_ratio(a: str, b: str) -> float:
    """Quick character-distance heuristic without full Levenshtein —
    Compares length and unique-char overlap. Good enough to flag a model
    that paraphrased instead of just fixing typos."""
    if not a or not b:
        return 1.0 if a != b else 0.0
    set_a = set(a.lower())
    set_b = set(b.lower())
    union = set_a | set_b
    intersection = set_a & set_b
    char_overlap = len(intersection) / len(union) if union else 1.0
    len_diff = abs(len(a) - len(b)) / max(len(a), len(b))
    # Combined: low char_overlap OR big len_diff = high drift
    return max(len_diff, 1.0 - char_overlap)


async def proofread_descriptions_batch(
    items: list[dict[str, Any]],
    ai_client: Any,
) -> list[str]:
    """Send one batch of descriptions to AI for typo+ortho fixes.

    Args:
        items: list of {"id": int, "text": str} — the id is opaque, AI
            echoes it back so the caller can map results to source.
        ai_client: anything with an async `generate_json(prompt, system,
            max_tokens)` method (e.g. MiniMaxClient).

    Returns:
        list of corrected strings in the SAME ORDER as `items`. If a
        single item drifts beyond MAX_DRIFT_RATIO or is missing from the
        AI response, the original text is returned for that slot — the
        guarantee is "never make it worse than input".
    """
    if not items:
        return []

    user_prompt = (
        "Popraw literówki i błędy ortograficzne w poniższych opisach usług. "
        "Zwróć WYŁĄCZNIE JSON: tablicę obiektów z polami `id` (liczba) i "
        "`text` (poprawiony opis), w tej samej kolejności co wejście. "
        "Bez markdown, bez komentarzy.\n\n"
        "WEJŚCIE:\n" + json.dumps(items, ensure_ascii=False, indent=2)
    )

    try:
        response = await ai_client.generate_json(
            prompt=user_prompt,
            system=_SYSTEM_PROMPT,
            max_tokens=8192,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("[proofread] AI call failed, returning originals: %s", e)
        return [it["text"] for it in items]

    # Response can be either a bare list or {"items": [...]} or {"results": [...]}.
    rows: list[dict[str, Any]] = []
    if isinstance(response, list):
        rows = response
    elif isinstance(response, dict):
        for key in ("items", "results", "data", "descriptions"):
            if isinstance(response.get(key), list):
                rows = response[key]
                break

    by_id: dict[int, str] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        rid = r.get("id")
        rtext = r.get("text")
        if not isinstance(rid, int) or not isinstance(rtext, str):
            continue
        by_id[rid] = rtext

    out: list[str] = []
    drift_rejected = 0
    for it in items:
        original = it["text"]
        corrected = by_id.get(it["id"])
        if corrected is None:
            out.append(original)
            continue
        drift = _char_distance_ratio(original, corrected)
        if drift > MAX_DRIFT_RATIO:
            drift_rejected += 1
            logger.info(
                "[proofread] rejected high-drift correction (%.2f) for id=%d",
                drift, it["id"],
            )
            out.append(original)
            continue
        out.append(corrected)

    if drift_rejected:
        logger.info("[proofread] %d/%d corrections rejected (drift > %.2f)",
                    drift_rejected, len(items), MAX_DRIFT_RATIO)
    return out


async def proofread_descriptions(
    descriptions: list[str],
    ai_client: Any,
    batch_size: int = PROOFREAD_BATCH_SIZE,
) -> list[str]:
    """Proofread a flat list of descriptions in batches of `batch_size`.

    Returns a list of the same length with corrections applied (or
    originals when AI failed / drift exceeded threshold).
    """
    if not descriptions:
        return []

    out: list[str] = [""] * len(descriptions)
    for batch_start in range(0, len(descriptions), batch_size):
        batch = descriptions[batch_start : batch_start + batch_size]
        items = [{"id": i, "text": t} for i, t in enumerate(batch)]
        corrected = await proofread_descriptions_batch(items, ai_client)
        for i, fixed in enumerate(corrected):
            out[batch_start + i] = fixed

    return out
