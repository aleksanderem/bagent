"""Taxonomy inference — corrects mis-tagged booksy_treatment_id values
using crowd lookup from `mv_treatment_name_lookup` Supabase materialized view.

Why this exists
---------------
Booksy salon owners pick `treatment_id` manually when setting up services
in Booksy panel. Many of them either pick the first thing in the list or
just keep their default — so the same service name ("Manicure hybrydowy")
gets tagged with very different tids across salons. This destroys our
ability to do apples-to-apples competitor pricing/gap comparison.

The materialized view `mv_treatment_name_lookup` (see migration 042)
aggregates all (name_norm, parent_id) → top tid pairs across hundreds of
thousands of scraped services. When a salon's tagging is noisy but the
majority of similarly-named services from other salons converge on one
tid, that tid is our inferred ground truth.

Architecture
------------
- `infer_treatment_id(name, parent_hint)` → SQL RPC defined in migration 042
- `enrich_services_with_inference(services)` → batch wrapper that calls
  the RPC for each service and populates `inferred_treatment_id` /
  `inference_source` / `inference_confidence` fields on the dicts.
- `apply_inference_overrides(services, min_confidence)` → mutates the
  service dicts so downstream pipeline aggregation uses inferred tids
  instead of raw Booksy tids. Original tid stays in `booksy_treatment_id_raw`
  for audit / debugging.

When NOT to override
--------------------
- If inferred tid == original tid → no-op (most common case)
- If confidence < min_confidence (default 0.6) → keep original
- If lookup returned no match → keep original
- If original name contains discriminator keywords (mega, 3d, akrylowy,
  etc.) AND original tid is in the same parent family → keep original
  (specificity preservation — avoids regressing precise tagging to
  pop-vote majority)

Used by `pipelines/competitor_analysis.py` after loading subject + competitor
full data, before pricing comparison / service gaps / dimensional scores
aggregations.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from collections.abc import Iterable
from typing import Any

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# Keywords that indicate a service is MORE specific than the average crowd
# tagging — if Booksy already tagged it with one of the specific tids
# (e.g. tid=708 Rzęsy Mega Volume), we should NOT regress to the majority
# tid (e.g. tid=265 Przedłużanie rzęs) just because more salons use the
# less specific name.
_SPECIFICITY_KEYWORDS = frozenset({
    "mega", "volume", "wolume",
    "3d", "4d", "5d", "6d", "10d",
    "akrylowy", "akrylowa", "akrylowe", "akryl",
    "japoński", "japonski", "japońska",
    "tytanowy", "tytanowa",
    "podologiczny", "podologiczna",
    "permanentny", "permanentna",
    "hybrydowy", "hybrydowa",
    "rekonstrukcja",
})


_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_name(name: str) -> str:
    """Lowercase + collapse whitespace + strip leading/trailing spaces.

    Mirrors the SQL normalization in mv_treatment_name_lookup so lookup
    keys match. We do NOT strip diacritics here — the view doesn't either.
    """
    if not name:
        return ""
    return _WHITESPACE_RE.sub(" ", name.strip().lower())


def _specificity_tokens(name: str) -> set[str]:
    """Return discriminator keywords present in `name`. Empty set if none."""
    if not name:
        return set()
    nfkd = unicodedata.normalize("NFKD", name.lower())
    no_diac = "".join(c for c in nfkd if not unicodedata.combining(c))
    tokens = set(_WHITESPACE_RE.split(no_diac))
    return tokens & _SPECIFICITY_KEYWORDS


async def _call_infer_rpc(
    supabase: SupabaseService,
    name: str,
    parent_hint: int | None,
) -> dict[str, Any] | None:
    """Wrap the `match_treatment_by_name` Postgres RPC (canonical-name
    pg_trgm matching from migration 043). Returns None on no match or
    any error (caller treats as 'no inference').

    The RPC matches a service name against canonical Booksy taxonomy
    (mv_booksy_treatments — 368 rows). Parent_hint boosts intra-family
    matches 1.3x but doesn't hard-exclude — useful for salons that
    tagged the wrong tid (e.g. Floral putting brwi services under
    paznokcie family); the match can still hop to the correct family.

    Returns dict with keys:
      - inferred_tid: int
      - confidence: float (trigram similarity score, possibly boosted)
      - sample_n: int (constant 1 — kept for API compat with old crowd RPC)
      - match_source: str ('trigram_parent_boost' | 'trigram')
    """
    try:
        def _do_call() -> Any:
            return (
                supabase.client.rpc(
                    "match_treatment_by_name",
                    {"p_name": name, "p_parent_hint": parent_hint},
                )
                .execute()
            )

        res = await asyncio.to_thread(_do_call)
        rows = res.data or []
        if not rows:
            return None
        row = rows[0] if isinstance(rows, list) else rows
        tid = row.get("inferred_tid")
        if tid is None:
            return None
        return {
            "inferred_tid": int(tid),
            "confidence": float(row.get("score") or 0.0),
            "sample_n": 1,
            "match_source": row.get("source") or "trigram",
        }
    except Exception as e:
        logger.debug("match_treatment_by_name RPC failed for name=%r: %s", name, e)
        return None


async def enrich_services_with_inference(
    supabase: SupabaseService,
    services: list[dict[str, Any]],
    *,
    name_key: str = "name",
    parent_key: str = "treatment_parent_id",
    concurrency: int = 8,
) -> list[dict[str, Any]]:
    """Look up inferred treatment_id for each service via the SQL RPC.

    Mutates each service dict in-place adding fields:
      - `inferred_treatment_id` (int | None)
      - `inference_confidence` (float | None)
      - `inference_sample_n` (int | None)
      - `inference_source` (str | None) — 'parent_constrained' | 'unconstrained' | None

    `services` is also returned for chainability.

    Concurrency: bounded by semaphore (Supabase RPC is light but doing
    400 sequential calls per pipeline run adds 1-2s — parallelization
    helps without overloading PostgREST).
    """
    if not services:
        return services

    sem = asyncio.Semaphore(concurrency)

    async def _one(svc: dict[str, Any]) -> None:
        name = svc.get(name_key) or ""
        parent = svc.get(parent_key)
        if not name or len(name.strip()) < 3:
            return
        async with sem:
            result = await _call_infer_rpc(supabase, name, parent)
        if result:
            svc["inferred_treatment_id"] = result["inferred_tid"]
            svc["inference_confidence"] = result["confidence"]
            svc["inference_sample_n"] = result["sample_n"]
            svc["inference_source"] = result["match_source"]
        else:
            svc["inferred_treatment_id"] = None
            svc["inference_confidence"] = None
            svc["inference_sample_n"] = None
            svc["inference_source"] = None

    await asyncio.gather(*(_one(s) for s in services))
    return services


def apply_inference_overrides(
    services: list[dict[str, Any]],
    *,
    min_confidence: float = 0.30,
    min_sample_n: int = 1,
    preserve_specificity: bool = True,
    raw_tid_key: str = "booksy_treatment_id",
    final_tid_key: str = "booksy_treatment_id",
    name_key: str = "name",
) -> dict[str, int]:
    """For each service, decide whether inferred tid should replace the
    original Booksy tid. Mutates services in-place: writes the chosen tid
    into `final_tid_key` and preserves the original under
    `booksy_treatment_id_raw`.

    Returns stats dict: { 'overridden': N, 'kept_original': N,
                          'no_inference': N, 'low_confidence': N,
                          'specificity_preserved': N }

    Default behavior: override iff
      - inference exists and confidence >= min_confidence
      - sample_n >= min_sample_n
      - inferred tid != original tid
      - original name doesn't contain specificity markers when
        preserve_specificity=True
    """
    stats = {
        "overridden": 0,
        "kept_original": 0,
        "no_inference": 0,
        "low_confidence": 0,
        "specificity_preserved": 0,
    }

    for svc in services:
        original_tid = svc.get(raw_tid_key)
        # Preserve raw value for audit even if we mutate final
        if "booksy_treatment_id_raw" not in svc:
            svc["booksy_treatment_id_raw"] = original_tid

        inferred = svc.get("inferred_treatment_id")
        conf = svc.get("inference_confidence") or 0.0
        sample_n = svc.get("inference_sample_n") or 0
        name = svc.get(name_key) or ""

        if inferred is None:
            stats["no_inference"] += 1
            continue
        if conf < min_confidence or sample_n < min_sample_n:
            stats["low_confidence"] += 1
            continue
        if inferred == original_tid:
            stats["kept_original"] += 1
            continue

        if preserve_specificity:
            # Preserve original tid only if BOTH the service name AND the
            # original treatment_name share a specificity marker — this means
            # the salon owner picked a precise tid intentionally, and crowd
            # majority would regress to a less-specific tid. If the keyword
            # is only in service name (not in original tid's canonical name),
            # the original tag is just generically wrong → override.
            svc_markers = _specificity_tokens(name)
            if svc_markers:
                original_treatment = svc.get("treatment_name") or ""
                original_markers = _specificity_tokens(original_treatment)
                if svc_markers & original_markers:
                    stats["specificity_preserved"] += 1
                    continue

        # Apply override
        svc[final_tid_key] = inferred
        svc["inference_applied"] = True
        stats["overridden"] += 1

    return stats


async def infer_and_apply(
    supabase: SupabaseService,
    services: list[dict[str, Any]],
    *,
    min_confidence: float = 0.30,
    min_sample_n: int = 1,
    preserve_specificity: bool = True,
    label: str = "services",
) -> dict[str, int]:
    """One-shot helper: enrich + apply overrides + log summary.

    Returns stats dict from apply_inference_overrides.
    """
    if not services:
        return {"overridden": 0, "kept_original": 0, "no_inference": 0,
                "low_confidence": 0, "specificity_preserved": 0}

    await enrich_services_with_inference(supabase, services)
    stats = apply_inference_overrides(
        services,
        min_confidence=min_confidence,
        min_sample_n=min_sample_n,
        preserve_specificity=preserve_specificity,
    )
    logger.info(
        "taxonomy_inference for %s: %d total, %d overridden, %d kept_original, "
        "%d no_inference, %d low_confidence, %d specificity_preserved",
        label, len(services),
        stats["overridden"], stats["kept_original"],
        stats["no_inference"], stats["low_confidence"],
        stats["specificity_preserved"],
    )
    return stats
