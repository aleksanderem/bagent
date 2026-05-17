"""Stage-5 commit 2: Rule 0 taxonomy anchors lookup.

Cross-audit memory of Pass 5 (MiniMax M2.7) routing decisions. Each
audit's authoritative `(brand_marker, method_marker, body_area_set)
→ tid_key` mappings are persisted in
`taxonomy_consistency_anchors` (migration 084). Subsequent audits
short-circuit the 4-rule routing + Pass 5 LLM cost for clusters
with ≥`min_confidence_count` historical confirmations.

Commit 2 is READ-ONLY: lookup wired in as Rule 0, but Pass 5 does
NOT yet write back. With an empty anchors table this is a no-op
(zero hits). Write-back follows in commit 3 after observing Pass 5
quality on 2-3 production audits.
"""

from __future__ import annotations

import logging
from typing import Any

from services.body_area_taxonomy import extract_body_areas
from services.brand_marker import extract_brand_marker
from services.method_marker import extract_method_marker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key extraction
# ---------------------------------------------------------------------------

def extract_anchor_key(
    svc: dict[str, Any],
) -> tuple[str | None, str, str]:
    """Return canonical (brand_marker, method_marker, body_area_set)
    tuple for a service. `body_area_set` is a sorted comma-joined
    string for deterministic DB lookup. brand_marker is None for
    generic services. method_marker is always a tag ('generic' for
    unrecognized).
    """
    name = svc.get("name") or ""
    category = svc.get("category_name") or ""
    brand = extract_brand_marker(name, category)
    method = extract_method_marker(name, category)
    areas = extract_body_areas(name)
    body_area_set = ",".join(sorted(areas)) if areas else ""
    return (brand, method, body_area_set)


# ---------------------------------------------------------------------------
# Bulk lookup
# ---------------------------------------------------------------------------

async def lookup_anchors_bulk(
    supabase,
    services: list[dict[str, Any]],
    *,
    min_confidence_count: int = 1,
) -> dict[tuple[str | None, str, str], dict[str, Any]]:
    """Bulk-lookup anchors for a list of services. Returns dict keyed
    by `(brand_marker, method_marker, body_area_set)` with anchor
    decision dicts as values:

      {
        "tid_kind": "booksy" | "synthetic",
        "booksy_tid": int | None,
        "synthetic_tid": int | None,
        "canonical_name": str | None,
        "confidence_count": int,
        "status": "active" | "overridden",
        "reasoning": str | None,
      }

    Only anchors with `confidence_count >= min_confidence_count` are
    returned. status='disputed' anchors are NOT returned (RPC filters).

    Empty table = empty dict. No fallback, no LLM.
    """
    # Build unique key set (multiple services often share a key)
    keys_seen: set[tuple[str | None, str, str]] = set()
    keys_payload: list[dict[str, Any]] = []
    for svc in services:
        if not svc.get("is_active", True):
            continue
        if svc.get("booksy_treatment_id") is not None:
            # Already has a tid — Rule 0 only runs for NULL-tid services.
            continue
        key = extract_anchor_key(svc)
        if key in keys_seen:
            continue
        keys_seen.add(key)
        keys_payload.append({
            "brand_marker": key[0],
            "method_marker": key[1],
            "body_area_set": key[2],
        })

    if not keys_payload:
        return {}

    logger.info(
        "lookup_anchors_bulk: querying anchors for %d unique keys",
        len(keys_payload),
    )

    # Call RPC. SupabaseService exposes the underlying client at .client.
    import json
    res = supabase.client.rpc(
        "fn_taxonomy_anchor_lookup_bulk",
        {"p_keys": keys_payload},
    ).execute()
    rows = res.data or []
    out: dict[tuple[str | None, str, str], dict[str, Any]] = {}
    for row in rows:
        if int(row.get("confidence_count") or 0) < min_confidence_count:
            continue
        key = (
            row.get("brand_marker"),
            row.get("method_marker"),
            row.get("body_area_set"),
        )
        out[key] = {
            "tid_kind": row.get("effective_tid_kind"),
            "booksy_tid": row.get("effective_booksy_tid"),
            "synthetic_tid": row.get("effective_synthetic_tid"),
            "canonical_name": row.get("effective_canonical_name"),
            "confidence_count": int(row.get("confidence_count") or 0),
            "status": row.get("status"),
            "reasoning": row.get("last_minimax_reasoning"),
        }
    logger.info(
        "lookup_anchors_bulk: found %d active/overridden anchors with "
        "confidence_count>=%d (queried %d keys)",
        len(out), min_confidence_count, len(keys_payload),
    )
    return out


# ---------------------------------------------------------------------------
# Rule 0 application
# ---------------------------------------------------------------------------

def apply_anchor_to_service(
    svc: dict[str, Any],
    anchor: dict[str, Any],
) -> bool:
    """Mutate svc dict in place with the anchor's routing decision.
    Returns True if anchor was applied, False otherwise.
    """
    tid_kind = anchor.get("tid_kind")
    if tid_kind == "booksy":
        btid = anchor.get("booksy_tid")
        if btid is None:
            return False
        if "booksy_treatment_id_raw" not in svc:
            svc["booksy_treatment_id_raw"] = svc.get("booksy_treatment_id")
        svc["booksy_treatment_id"] = int(btid)
        svc["synthetic_treatment_id"] = None
        svc["taxonomy_source"] = "anchor_replay"
        svc["anchor_confidence_count"] = anchor.get("confidence_count")
        svc["anchor_reasoning"] = anchor.get("reasoning")
        return True
    if tid_kind == "synthetic":
        syn_id = anchor.get("synthetic_tid")
        if syn_id is None:
            return False
        svc["booksy_treatment_id"] = svc.get("booksy_treatment_id_raw")
        svc["synthetic_treatment_id"] = int(syn_id)
        svc["synthetic_canonical_name"] = anchor.get("canonical_name")
        svc["taxonomy_source"] = "anchor_replay"
        svc["anchor_confidence_count"] = anchor.get("confidence_count")
        svc["anchor_reasoning"] = anchor.get("reasoning")
        return True
    return False
