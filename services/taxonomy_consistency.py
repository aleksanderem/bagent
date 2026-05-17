"""Stage-5 Pass 5: intra-salon taxonomy consistency via MiniMax M2.7.

Uses tool_use API (not generate_json) because MiniMax M2.7 with
interleaved thinking emits markdown reasoning before the JSON answer
when called via plain text completion — `generate_json` then fails
to parse "Let me analyze each cluster..." preamble. Tool_use forces
structured output by definition.

After the 4-rule routing finishes, services from the SAME salon that
share (brand_marker, body_area_set) MUST resolve to the same
taxonomy decision. Otherwise:
  - "Thunder Całe ciało 1 zabieg" → booksy_tid=637 Depilacja ciała
  - "Thunder Całe ciało 5 + 1 zabieg" → salon-defined synthetic
end up in DIFFERENT tid_key buckets, splitting one logical service
into two rows of the pricing matrix and breaking Faza 8a/8b matching.

This module:
  1. Groups resolved services by (brand_marker, sorted_area_set).
  2. Identifies "mixed" clusters where members hold different
     final tid_keys, or any member is Rule-3-unfixable (per advisor
     trigger expansion 2026-05-17).
  3. Batches all mixed clusters into ONE MiniMax M2.7 prompt asking
     for a single authoritative decision per cluster.
  4. Re-routes every member of each cluster to the verdict.

MiniMax is chosen over gpt-4o-mini (currently in Rule 3) for the
heavier consistency reasoning — thinking blocks + larger context
window pay off when the prompt holds 10-30 services and their
current decisions side-by-side.

NO graceful fallbacks. MiniMax error = raise (Bugsink alerts).
"""

from __future__ import annotations

import logging
from typing import Any

from services.body_area_taxonomy import extract_body_areas
from services.brand_marker import extract_brand_marker
from services.method_marker import extract_method_marker
from services.minimax import MiniMaxClient, with_retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cluster building
# ---------------------------------------------------------------------------

def _resolved_tid_key(svc: dict[str, Any]) -> tuple[str, int | None]:
    """Canonical decision key for cluster membership comparison.

    Returns ('booksy', tid) for Rule 3 hits, ('synthetic', syn_id) for
    Rule 1/2/4 hits, ('unresolved', None) for services that didn't get
    routed (Rule 3 unfixable + no Rule 4 match + no Rule 1).
    """
    btid = svc.get("booksy_treatment_id")
    stid = svc.get("synthetic_treatment_id")
    if btid is not None:
        return ("booksy", int(btid))
    if stid is not None:
        return ("synthetic", int(stid))
    return ("unresolved", None)


def build_clusters(
    services: list[dict[str, Any]],
) -> dict[tuple[str | None, str, tuple[str, ...]], list[dict[str, Any]]]:
    """Group services by (brand_marker, method_marker, sorted_area_set).

    Three-axis key: brand (device/marka), method (laser/wax/cukrowa/
    rf/...), areas (body parts). This separates Thunder pachy (laser)
    from Wax pachy (cukrowa) at clustering stage so MiniMax cannot
    collapse them into one decision.
    """
    clusters: dict[
        tuple[str | None, str, tuple[str, ...]], list[dict[str, Any]]
    ] = {}
    for svc in services:
        if not svc.get("is_active", True):
            continue
        name = svc.get("name") or ""
        category = svc.get("category_name") or ""
        brand = extract_brand_marker(name, category)
        method = extract_method_marker(name, category)
        areas = extract_body_areas(name)
        key = (brand, method, tuple(sorted(areas)))
        clusters.setdefault(key, []).append(svc)
    return clusters


def find_mixed_clusters(
    clusters: dict[tuple[str | None, str, tuple[str, ...]], list[dict[str, Any]]],
) -> list[tuple[tuple[str | None, str, tuple[str, ...]], list[dict[str, Any]]]]:
    """Pick out clusters that need MiniMax consistency resolution.

    A cluster qualifies when ANY of:
      (a) ≥2 distinct (rule + tid_key) outcomes among members
      (b) any member is unresolved (Rule-3-unfixable, no Rule 4 match)
      (c) cluster has ≥2 members AND brand_marker is not None
          (per advisor trigger expansion 2026-05-17 — unanimous-wrong
          path: if all 4 Thunder Całe ciało went to btid=637 but
          should have been synthetic, the cluster is unanimous and
          would otherwise be skipped; brand-marker presence forces
          review).

    Singletons (cluster size 1) are skipped — no consistency to
    enforce.
    """
    out: list[tuple[tuple[str | None, str, tuple[str, ...]], list[dict[str, Any]]]] = []
    for key, members in clusters.items():
        brand, method, areas = key
        if len(members) < 2:
            continue
        if brand is None and not areas:
            # Generic services without brand + area — likely the
            # well-matched bulk; skip to avoid noise.
            continue
        decisions = {_resolved_tid_key(s) for s in members}
        has_unresolved = any(d[0] == "unresolved" for d in decisions)
        mixed = len(decisions) >= 2
        brand_present = brand is not None
        if mixed or has_unresolved or brand_present:
            out.append((key, members))
    return out


# ---------------------------------------------------------------------------
# MiniMax prompt assembly + parsing
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "Jesteś ekspertem taxonomii usług w polskich salonach beauty "
    "(Booksy.com). Dostajesz LISTĘ klastrów usług — każdy klaster "
    "ma TRZY deterministyczne atrybuty: `brand_marker` (Thunder/Onda/"
    "Dermapen/None), `method_marker` (laser/wax/cukrowa/rf/hifu/hifem/"
    "mezoterapia/microneedling/hydrafacial/kroplowka/manicure/generic) "
    "oraz `body_areas` (zestaw okolic ciała). Wszystkie usługi w "
    "klastrze pochodzą z TEGO SAMEGO salonu i mają TĘ SAMĄ trójkę "
    "(brand, method, areas).\n\n"
    "Twoim zadaniem jest WYBRAĆ JEDNĄ autorytatywną decyzję dla całego "
    "klastra. Każda decyzja MUSI być jedną z dwóch:\n"
    "  1. `booksy_tid` — istniejący tid w taksonomii Booksy z listy "
    "      kandydatów. Booksy tid MUSI pasować JEDNOCZEŚNIE:\n"
    "        (a) metoda — `method_marker` klastra musi być zgodny ze "
    "            znaczeniem canonical_name kandydata (laser ≠ wosk ≠ "
    "            pasta cukrowa ≠ RF ≠ HiFU ≠ mezoterapia). Jeśli "
    "            method_marker='laser' a kandydat to 'Depilacja "
    "            woskiem' lub 'Depilacja pastą cukrową' → REJECT.\n"
    "        (b) okolica — body_areas klastra musi się pokrywać z "
    "            okolicą sugerowaną przez canonical_name.\n"
    "  2. `salon_synthetic` — gdy żaden Booksy tid nie spełnia OBU "
    "      warunków (a) i (b), klaster jest brand- lub method-specific "
    "      i potrzebuje własnej kategorii.\n\n"
    "ZASADY DODATKOWE:\n"
    "  - Konsekwencja > precyzja. Wszystkie usługi w klastrze TRAFIAJĄ "
    "    POD JEDEN tid. Nie dziel klastra.\n"
    "  - 'Depilacja laserowa' (tid generyczny method=laser, area=brak) "
    "    JEST poprawny dla klastra `method=laser` gdy brak bardziej "
    "    specyficznego area-matching tid. NIE jest poprawny dla "
    "    method=wax/cukrowa/inne.\n"
    "  - Bardziej specyficzny tid > generyczny: 'Depilacja ciała' tid=637 "
    "    > 'Depilacja laserowa' tid=240 dla method=laser + area=cale_cialo.\n"
    "  - Method mismatch JEST powodem do wyboru salon_synthetic NAWET "
    "    gdy area się pokrywa. Lepszy synthetic niż wrong-method tid."
)


def _format_cluster_for_prompt(
    cluster_id: int,
    key: tuple[str | None, str, tuple[str, ...]],
    members: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    brand, method, areas = key
    lines: list[str] = [
        f"### KLASTER #{cluster_id}",
        f"brand_marker: {brand or '(brak)'}",
        f"method_marker: {method}  (laser/wax/cukrowa/rf/hifu/hifem/"
        f"mezoterapia/microneedling/hydrafacial/kroplowka/manicure/generic)",
        f"body_areas: {list(areas) or '(brak)'}",
        f"members ({len(members)}):",
    ]
    for s in members:
        decision = _resolved_tid_key(s)
        decision_str = (
            f"booksy_tid={decision[1]}" if decision[0] == "booksy"
            else f"synthetic_tid={decision[1]}" if decision[0] == "synthetic"
            else "unresolved"
        )
        lines.append(
            f"  - svc_id={s.get('id')} name={s.get('name')!r} "
            f"category={s.get('category_name')!r} "
            f"current={decision_str}"
        )
    lines.append("kandydaci Booksy (area-compatible, top-15):")
    for c in candidates[:15]:
        lines.append(
            f"  - tid={c['tid']} canonical={c['canonical_name']!r} "
            f"parent={c.get('parent_canonical_name')!r}"
        )
    return "\n".join(lines)


def _build_user_prompt(
    cluster_payloads: list[tuple[
        int,
        tuple[str | None, str, tuple[str, ...]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]],
) -> str:
    parts: list[str] = [
        "Przeanalizuj poniższe klastry i zwróć decyzję dla każdego.",
        "",
    ]
    for cid, key, members, cands in cluster_payloads:
        parts.append(_format_cluster_for_prompt(cid, key, members, cands))
        parts.append("")
    parts.append(
        "Zwróć JSON w formacie:\n"
        "{\n"
        '  "decisions": [\n'
        '    {"cluster_id": 1, "type": "booksy_tid", "tid": <int>, '
        '"reasoning": "..."},\n'
        '    {"cluster_id": 2, "type": "salon_synthetic", '
        '"canonical_name": "<nazwa>", "reasoning": "..."}\n'
        "  ]\n"
        "}\n"
        "Dla każdego klastra w JSON MUSI być jedna decyzja. "
        "Cluster_id 1..N w kolejności podanej powyżej."
    )
    return "\n".join(parts)


_TAXONOMY_DECISIONS_TOOL: dict[str, Any] = {
    "name": "submit_taxonomy_decisions",
    "description": (
        "Submit ONE authoritative routing decision per provided cluster. "
        "Every cluster_id from the input MUST appear in the decisions array. "
        "For type='booksy_tid' you MUST provide `tid` (integer from candidates). "
        "For type='salon_synthetic' you MUST provide `canonical_name` "
        "(human-readable 2-6 word Polish category name, e.g. "
        "'Modelowanie sylwetki Onda RF' or 'Thunder depilacja laserowa "
        "całe ciało'). canonical_name MUST never be empty for synthetic."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "integer"},
                        "type": {
                            "type": "string",
                            "enum": ["booksy_tid", "salon_synthetic"],
                        },
                        # Both keys are always present in the schema —
                        # model fills the one matching `type`. We
                        # enforce non-empty canonical_name for synthetic
                        # at the validation layer.
                        "tid": {"type": "integer"},
                        "canonical_name": {"type": "string", "minLength": 1},
                        "reasoning": {"type": "string", "minLength": 1},
                    },
                    "required": ["cluster_id", "type", "canonical_name", "reasoning"],
                },
            },
        },
        "required": ["decisions"],
    },
}


def _synthesize_fallback_canonical(
    key: tuple[str | None, str, tuple[str, ...]],
) -> str:
    """Generate a deterministic fallback canonical_name from cluster
    key. Used when LLM returns synthetic decision without
    canonical_name (schema violation) so the pipeline does NOT crash
    on a recoverable bug — but logs it as a defect.

    Format: "Brand method area1, area2, area3" or omit empty components.
    """
    brand, method, areas = key
    parts: list[str] = []
    if brand:
        parts.append(brand.capitalize())
    if method and method != "generic":
        parts.append(method)
    if areas:
        parts.append(", ".join(areas))
    if not parts:
        parts.append("Usługa salonu")
    return " ".join(parts)


def _extract_tool_decisions(msg, expected_count: int) -> list[dict[str, Any]]:
    """Pull `submit_taxonomy_decisions` tool_use input from a MiniMax
    Message response. Raises on missing/malformed tool call — no
    graceful fallback.
    """
    if msg is None:
        raise RuntimeError("MiniMax returned None message")
    decisions: list[dict[str, Any]] = []
    for block in msg.content:
        # anthropic.types.ToolUseBlock has .type == "tool_use"
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_taxonomy_decisions":
            payload = block.input
            if isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
                decisions.extend(payload["decisions"])
    if not decisions:
        raise RuntimeError(
            f"MiniMax tool_use returned no submit_taxonomy_decisions calls "
            f"(stop_reason={getattr(msg, 'stop_reason', '?')}, "
            f"content_types=" + ", ".join(
                getattr(b, "type", "?") for b in msg.content
            ) + ")"
        )
    return decisions


def _validate_decisions(
    decisions: list[dict[str, Any]], expected_count: int,
) -> list[dict[str, Any]]:
    """Validate parsed decisions against schema. Raise on any
    deviation — no graceful fallbacks (Bugsink alert)."""
    if len(decisions) != expected_count:
        raise RuntimeError(
            f"MiniMax returned {len(decisions)} decisions, expected "
            f"{expected_count}"
        )
    for i, d in enumerate(decisions):
        if not isinstance(d, dict):
            raise RuntimeError(f"Decision {i} not a dict: {d!r}")
        if d.get("type") not in ("booksy_tid", "salon_synthetic"):
            raise RuntimeError(
                f"Decision {i} has invalid type={d.get('type')!r}; "
                "expected 'booksy_tid' or 'salon_synthetic'"
            )
        if d["type"] == "booksy_tid":
            if not isinstance(d.get("tid"), int):
                raise RuntimeError(
                    f"Decision {i} type=booksy_tid but tid is not int: "
                    f"{d.get('tid')!r}"
                )
        # NOTE: salon_synthetic canonical_name validation is now done
        # later in apply_intra_salon_consistency where a deterministic
        # fallback can be substituted on LLM schema violation
        # (logger.error so Bugsink still sees the regression).
    return decisions


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

async def apply_intra_salon_consistency(
    services: list[dict[str, Any]],
    *,
    supabase,
    minimax: MiniMaxClient,
    audit_id: str | None,
    label: str,
    trace_collector: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Run consistency pass over already-routed services. Mutates svc
    dicts in place when re-routing happens. Returns stats dict.
    """
    clusters = build_clusters(services)
    if not clusters:
        return {"clusters_total": 0, "clusters_mixed": 0, "rerouted": 0}

    mixed = find_mixed_clusters(clusters)
    if not mixed:
        logger.info(
            "apply_intra_salon_consistency [%s]: %d clusters, none mixed",
            label, len(clusters),
        )
        return {
            "clusters_total": len(clusters),
            "clusters_mixed": 0,
            "rerouted": 0,
        }

    logger.info(
        "apply_intra_salon_consistency [%s]: %d clusters total, %d "
        "mixed/uncertain, sending to MiniMax",
        label, len(clusters), len(mixed),
    )

    # Gather area-compatible candidates per cluster. We reuse the
    # match_taxonomy_candidates RPC: take the FIRST member's embedding
    # (all cluster members share brand+area so candidates overlap
    # heavily) and apply the body-area filter again.
    from services.body_area_taxonomy import filter_candidates_by_area
    from services.hidden_service_inference import match_taxonomy_candidates

    cluster_payloads: list[tuple[
        int,
        tuple[str | None, str, tuple[str, ...]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]] = []
    for cid, (key, members) in enumerate(mixed, start=1):
        ref_svc = members[0]
        emb = ref_svc.get("name_embedding")
        candidates: list[dict[str, Any]] = []
        if emb:
            raw_candidates = await match_taxonomy_candidates(
                supabase, emb, top_k=30,
            )
            filtered, _, _ = filter_candidates_by_area(
                ref_svc.get("name") or "", raw_candidates,
            )
            candidates = filtered
        cluster_payloads.append((cid, key, members, candidates))

    # Batch all clusters into ONE MiniMax call.
    user_prompt = _build_user_prompt(cluster_payloads)
    logger.info(
        "apply_intra_salon_consistency [%s]: MiniMax prompt %d chars, "
        "%d clusters",
        label, len(user_prompt), len(cluster_payloads),
    )

    # Pass 5 LLM provider switch (2026-05-17): MiniMax M2.7 with
    # interleaved thinking exhausts max_tokens on thinking blocks
    # before producing the tool call when the prompt has 25+ mixed
    # clusters + method gate rules. OpenAI gpt-4o reliably produces
    # function calls under the same prompt. Switch via env
    # TAXONOMY_PASS5_PROVIDER (default 'openai').
    import os
    provider = os.environ.get("TAXONOMY_PASS5_PROVIDER", "openai").lower()
    if provider == "openai":
        from config import settings as _settings
        from services.openai_taxonomy_client import OpenAITaxonomyClient
        if not _settings.openai_api_key and not os.environ.get("OPENAI_API_KEY"):
            raise RuntimeError(
                "TAXONOMY_PASS5_PROVIDER=openai but OPENAI_API_KEY is "
                "not set"
            )
        oai_key = _settings.openai_api_key or os.environ["OPENAI_API_KEY"]
        oai_model = os.environ.get("TAXONOMY_PASS5_OPENAI_MODEL", "gpt-4o")
        oai_client = OpenAITaxonomyClient(api_key=oai_key, model=oai_model)
        logger.info(
            "apply_intra_salon_consistency [%s]: provider=openai model=%s",
            label, oai_model,
        )
        async def _call_oai() -> list[dict[str, Any]]:
            return await oai_client.call_decisions_tool(
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                tool_schema=_TAXONOMY_DECISIONS_TOOL,
                max_tokens=16384,
            )
        raw_decisions = await with_retry(_call_oai, max_attempts=2, base_delay=2.0)
    else:
        # Legacy MiniMax path (kept for A/B). Use tool_use API to
        # force structured output.
        logger.info(
            "apply_intra_salon_consistency [%s]: provider=minimax model=%s",
            label, minimax.model,
        )
        async def _call_mm():
            return await minimax.create_message(
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
                tools=[_TAXONOMY_DECISIONS_TOOL],
                max_tokens=32768,
                temperature=0.2,
            )
        msg = await with_retry(_call_mm, max_attempts=2, base_delay=2.0)
        raw_decisions = _extract_tool_decisions(
            msg, expected_count=len(cluster_payloads),
        )
    decisions = _validate_decisions(raw_decisions, expected_count=len(cluster_payloads))

    # MiniMax/OpenAI may return decisions out of order — index by cluster_id.
    decisions_by_cid: dict[int, dict[str, Any]] = {}
    for d in decisions:
        cid_raw = d.get("cluster_id")
        if not isinstance(cid_raw, int):
            raise RuntimeError(
                f"Decision missing cluster_id or non-int: {d!r}"
            )
        decisions_by_cid[cid_raw] = d
    rerouted = 0
    for cid, key, members, _cands in cluster_payloads:
        decision = decisions_by_cid.get(cid)
        if decision is None:
            raise RuntimeError(
                f"LLM skipped cluster_id={cid} — every cluster MUST "
                f"have a decision"
            )
        # If LLM returned salon_synthetic without canonical_name
        # (schema violation), generate a deterministic fallback from
        # the cluster key instead of crashing. Log so the regression
        # is visible in Bugsink breadcrumbs.
        if decision.get("type") == "salon_synthetic":
            cn = decision.get("canonical_name")
            if not isinstance(cn, str) or not cn.strip():
                fallback = _synthesize_fallback_canonical(key)
                logger.error(
                    "LLM returned salon_synthetic for cluster_id=%d "
                    "without canonical_name — using deterministic "
                    "fallback %r (LLM schema violation, see Bugsink)",
                    cid, fallback,
                )
                decision["canonical_name"] = fallback
        rerouted += await _apply_decision(
            cid=cid, key=key, members=members, decision=decision,
            supabase=supabase, audit_id=audit_id,
            trace_collector=trace_collector, dry_run=dry_run,
            label=label,
        )

    return {
        "clusters_total": len(clusters),
        "clusters_mixed": len(mixed),
        "rerouted": rerouted,
    }


async def _apply_decision(
    *,
    cid: int,
    key: tuple[str | None, str, tuple[str, ...]],
    members: list[dict[str, Any]],
    decision: dict[str, Any],
    supabase,
    audit_id: str | None,
    trace_collector: list[dict[str, Any]] | None,
    dry_run: bool,
    label: str,
) -> int:
    """Apply one cluster decision to every member. Returns number of
    services rerouted (i.e. whose final tid_key changed)."""
    brand, method, areas = key
    rerouted_count = 0

    if decision["type"] == "booksy_tid":
        target_btid = int(decision["tid"])
        for svc in members:
            old_key = _resolved_tid_key(svc)
            new_key = ("booksy", target_btid)
            if old_key == new_key:
                continue
            if "booksy_treatment_id_raw" not in svc:
                svc["booksy_treatment_id_raw"] = svc.get("booksy_treatment_id")
            svc["booksy_treatment_id"] = target_btid
            svc["synthetic_treatment_id"] = None
            svc["taxonomy_source"] = "minimax_consistency"
            svc["minimax_cluster_id"] = cid
            svc["minimax_reasoning"] = decision.get("reasoning", "")
            rerouted_count += 1
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "5",
                    "decision": "rerouted_by_minimax",
                    "details": {
                        "cluster_id": cid,
                        "brand_marker": brand,
                        "method_marker": method,
                        "areas": list(areas),
                        "old_decision": list(old_key),
                        "new_decision": ["booksy", target_btid],
                        "reasoning": decision.get("reasoning", ""),
                    },
                    "embedding_top_k": [],
                    "llm_response": decision,
                    "final": {
                        "booksy_tid": target_btid,
                        "synthetic_tid": None,
                        "taxonomy_source": "minimax_consistency",
                        "treatment_name": svc.get("name"),
                    },
                })
    else:
        # salon_synthetic: upsert ONE canonical for the whole cluster
        canonical = decision["canonical_name"].strip()
        if dry_run:
            syn_id = -1
        else:
            from services.hidden_service_inference import embed_short_text
            embedding = await embed_short_text(canonical)
            syn_id = await supabase.upsert_synthetic_category_salon_defined(
                normalized_name=canonical.lower(),
                canonical_name=canonical,
                embedding=embedding,
                audit_id=audit_id,
            )
        for svc in members:
            old_key = _resolved_tid_key(svc)
            new_key = ("synthetic", syn_id)
            if old_key == new_key:
                continue
            svc["booksy_treatment_id"] = svc.get("booksy_treatment_id_raw")
            svc["synthetic_treatment_id"] = syn_id
            svc["synthetic_canonical_name"] = canonical
            svc["taxonomy_source"] = "minimax_consistency"
            svc["minimax_cluster_id"] = cid
            svc["minimax_reasoning"] = decision.get("reasoning", "")
            rerouted_count += 1
            if trace_collector is not None:
                trace_collector.append({
                    "svc_id": svc.get("id"),
                    "svc_name": svc.get("name"),
                    "original_tid": None,
                    "original_category": svc.get("category_name"),
                    "rule": "5",
                    "decision": "rerouted_by_minimax",
                    "details": {
                        "cluster_id": cid,
                        "brand_marker": brand,
                        "method_marker": method,
                        "areas": list(areas),
                        "old_decision": list(old_key),
                        "new_decision": ["synthetic", syn_id],
                        "canonical_name": canonical,
                        "reasoning": decision.get("reasoning", ""),
                    },
                    "embedding_top_k": [],
                    "llm_response": decision,
                    "final": {
                        "booksy_tid": None,
                        "synthetic_tid": syn_id,
                        "taxonomy_source": "minimax_consistency",
                        "treatment_name": svc.get("name"),
                    },
                })
    logger.info(
        "apply_intra_salon_consistency [%s] cluster #%d "
        "(brand=%s method=%s areas=%s size=%d) decision=%s rerouted=%d",
        label, cid, brand, method, areas, len(members), decision["type"],
        rerouted_count,
    )

    # Stage-5 commit 3b (2026-05-17): write-back to anchors table.
    # Persist every Pass 5 cluster decision so future audits read it
    # via Rule 0. Skipped in dry_run (dev modal trace) — we don't
    # pollute the anchor catalog with debug runs.
    if not dry_run and audit_id:
        try:
            body_area_set = ",".join(areas) if areas else ""
            if decision["type"] == "booksy_tid":
                supabase.client.rpc("fn_taxonomy_anchor_upsert", {
                    "p_brand_marker": brand,
                    "p_method_marker": method,
                    "p_body_area_set": body_area_set,
                    "p_audit_id": audit_id,
                    "p_tid_kind": "booksy",
                    "p_booksy_tid": int(decision["tid"]),
                    "p_synthetic_tid": None,
                    "p_synthetic_canonical_name": None,
                    "p_reasoning": decision.get("reasoning", ""),
                    "p_cluster_size": len(members),
                }).execute()
            else:
                # syn_id was assigned above in non-dry-run branch.
                supabase.client.rpc("fn_taxonomy_anchor_upsert", {
                    "p_brand_marker": brand,
                    "p_method_marker": method,
                    "p_body_area_set": body_area_set,
                    "p_audit_id": audit_id,
                    "p_tid_kind": "synthetic",
                    "p_booksy_tid": None,
                    "p_synthetic_tid": syn_id if syn_id and syn_id > 0 else None,
                    "p_synthetic_canonical_name": canonical,
                    "p_reasoning": decision.get("reasoning", ""),
                    "p_cluster_size": len(members),
                }).execute()
            logger.info(
                "apply_intra_salon_consistency [%s] cluster #%d: "
                "anchor upserted (brand=%s method=%s area_set=%r)",
                label, cid, brand, method, body_area_set,
            )
        except Exception:
            # Anchor write-back failure must NOT silently swallow.
            # Surface to Bugsink so we know if RPC drifted.
            logger.exception(
                "apply_intra_salon_consistency [%s] cluster #%d: "
                "anchor write-back FAILED (continuing pipeline — anchor "
                "is opportunistic cache, not authoritative)",
                label, cid,
            )
            raise

    return rerouted_count
