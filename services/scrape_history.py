"""Helpers for salon_scrapes delta-encoded snapshot chains.

The migration 047 introduced reverse-delta storage for raw_response on the
salon_scrapes table. This module centralizes:

* canonicalisation + hashing of the raw business payload for change detection
* JSON Patch (RFC 6902) generation for compact reverse deltas
* chain reconstruction from a delta row back to a base row

The write path (``scripts/ingest_salon_jsons._insert_scrape``) imports
``compute_content_hash`` and ``compute_reverse_patch``. Reader code that needs
historical state imports ``reconstruct_raw_response``.

Volatile field stripping
------------------------
Booksy's raw response carries values that fluctuate every crawl without
representing a meaningful change to the salon's offer:

* ``reviews_count`` / ``reviews_rank`` — re-computed every time a new review
  lands; the new review itself is captured separately in ``salon_reviews``
  and in the underlying ``reviews`` array, so we don't lose signal by
  ignoring the scalar count.
* ``last_updated`` / ``synced_at`` / ``cached_at`` — Booksy bookkeeping
  timestamps; they bump even when no business data changed.
* ``popularity_*`` / ``trending_score`` — analytics knobs that drift
  continuously.

The strip list is conservative — when in doubt we keep the field, so the hash
is slightly noisier than optimal but never silently swallows a real change.

Checkpoint policy
-----------------
``CHECKPOINT_EVERY = 30`` matches the schema-level expectation. After 30
deltas accumulate on a chain, the next change writes a fresh base instead
of converting the previous head into a delta. This caps reconstruction
depth and creates recovery anchors.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger("scrape_history")

# Stable across the codebase — bumped together with migration 047 if we ever
# need to flush. Keep in sync with the documentation in the migration file.
CHECKPOINT_EVERY: int = 30

# Top-level keys inside ``business`` that we strip before hashing. The strip is
# applied at every depth where the key appears, except where doing so would
# meaningfully alter semantics (e.g. we keep the ``reviews`` array; only the
# scalar count is dropped).
_VOLATILE_BUSINESS_KEYS: frozenset[str] = frozenset(
    {
        "reviews_count",
        "reviews_rank",
        "last_updated",
        "synced_at",
        "cached_at",
        "popularity_score",
        "trending_score",
        "view_count",
        "booking_count",
    }
)


def _strip_volatile(node: Any) -> Any:
    """Recursively drop volatile keys. Pure function — does not mutate input."""
    if isinstance(node, dict):
        return {k: _strip_volatile(v) for k, v in node.items() if k not in _VOLATILE_BUSINESS_KEYS}
    if isinstance(node, list):
        return [_strip_volatile(v) for v in node]
    return node


def canonical_json(payload: Any) -> str:
    """Stable JSON text — sorted keys, compact separators, NaN-safe.

    The output is deterministic for any equal payload regardless of key
    insertion order, which is what we need for content hashing.
    """
    return json.dumps(
        _strip_volatile(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def compute_content_hash(payload: Any) -> str:
    """SHA-256 hex digest of the stripped + canonicalized payload."""
    canon = canonical_json(payload)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def compute_reverse_patch(new_payload: Any, old_payload: Any) -> list[dict[str, Any]]:
    """RFC 6902 patch s.t. ``apply(new_payload, patch) == old_payload``.

    We use jsonpatch's ``make_patch`` which produces an optimized series of
    add/remove/replace/move operations. The patch is stored on the OLD row
    after that row gets converted to a delta — applying the patch to the
    NEW row's raw_response reconstructs what the OLD row originally held.

    Note that volatile-stripped equality means new and old MAY hash the same
    even when raw_response differs (just on volatile fields). The caller must
    short-circuit unchanged crawls BEFORE asking for a reverse patch — calling
    this with hash-equal but byte-different payloads would produce a small,
    pointless patch.
    """
    # Local import keeps the module import-light when only hashing is needed.
    import jsonpatch  # type: ignore[import-untyped]

    patch = jsonpatch.JsonPatch.from_diff(new_payload, old_payload)
    return list(patch)


def apply_patch(target: Any, ops: list[dict[str, Any]]) -> Any:
    """Apply RFC 6902 ops to target. Returns the patched object (new instance).

    Raises jsonpatch.JsonPatchException on corrupt ops — callers should treat
    that as chain corruption and surface a loud error.
    """
    import jsonpatch  # type: ignore[import-untyped]

    return jsonpatch.JsonPatch(ops).apply(target)


def reconstruct_raw_response(client: Any, scrape_id: str) -> dict[str, Any] | None:
    """Walk the chain from ``scrape_id`` forward (newer direction) to a base
    row, then unwind back applying delta_ops to reconstruct the requested
    snapshot's raw_response.

    Returns ``None`` if the row is missing or the chain is corrupted.

    Implementation notes
    --------------------
    The Postgres-side ``scrape_chain_trail`` RPC returns the chain in the
    correct walk order (target → newer → ... → base). We reverse it locally so
    that the base sits at index 0 and each subsequent step is a delta to be
    applied. The accumulator starts with the base's raw_response.
    """
    res = client.rpc("scrape_chain_trail", {"p_scrape_id": scrape_id}).execute()
    trail = res.data or []
    if not trail:
        return None

    # The RPC returns rows in walk order — target first, base last. Reverse
    # so we can start with the base and apply deltas back toward the target.
    trail = list(reversed(trail))
    base = trail[0]
    if base.get("snapshot_kind") != "base":
        logger.warning(
            "reconstruct_raw_response: chain trail for scrape_id=%s does not terminate in a base row",
            scrape_id,
        )
        return None

    current = base.get("raw_response")
    if current is None:
        return None

    for step in trail[1:]:
        ops = step.get("delta_ops")
        if not isinstance(ops, list):
            logger.error(
                "reconstruct_raw_response: corrupt delta at step %s for scrape_id=%s",
                step.get("scrape_id"),
                scrape_id,
            )
            return None
        try:
            current = apply_patch(current, ops)
        except Exception:  # noqa: BLE001
            logger.exception(
                "reconstruct_raw_response: failed to apply patch at step %s",
                step.get("scrape_id"),
            )
            return None

    return current


__all__ = [
    "CHECKPOINT_EVERY",
    "apply_patch",
    "canonical_json",
    "compute_content_hash",
    "compute_reverse_patch",
    "reconstruct_raw_response",
]
