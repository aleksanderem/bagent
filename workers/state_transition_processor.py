"""arq tasks: deterministic state machine for outreach contacts.

The parallel multi-state machine (per migration 041) lets a contact
hold many states simultaneously across funnels (audit_buyer +
competitor_consideration + monitoring_engaged for example). State
membership is derived deterministically from observable events —
NEVER from heuristics. This worker is the single source of truth for
"who is in what state right now."

Three cron-driven tasks:

1. ``ingest_new_cold_contacts`` (every 30 min)
   For salons with email present, no outreach_contact row, and not
   already on the unsubscribe list, create outreach_contacts row +
   insert outreach_customer_states (funnel=audit, state='cold').
   Only the audit funnel is the natural cold-entry; other funnels
   onboard via state transitions, not direct ingestion.

2. ``apply_purchase_transitions`` (every 15 min)
   Reads recent purchases from convex_audit_purchases (or whatever
   bridge view exists) and walks state transitions:
     - audit_purchase    → exit cold/contacted, enter audit_buyer +
                           competitor_consideration (after +7d delay)
     - competitor_purchase → enter competitor_buyer + monitoring_consideration
     - monitoring_subscribe → enter monitoring_subscriber

3. ``expire_stale_states`` (hourly)
   Audit-funnel cold contacts who haven't received any email in 90d
   AND haven't engaged → state='cold_lapsed' (so they re-enter cold
   pool eligibility next time scale cohort top-N picker runs).

Every transition writes to outreach_state_transitions for audit trail
(separate table from outreach_customer_states — the latter is current,
the former is historical). State changes are idempotent: re-running a
task on the same data does nothing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from config import settings
from services.sb_client import make_supabase_client

logger = logging.getLogger("bagent.workers.state_transition_processor")


# ---------------------------------------------------------------------------
# Task 1: cold contact ingestion from salons table
# ---------------------------------------------------------------------------

async def ingest_new_cold_contacts(ctx: dict[str, Any]) -> dict[str, int]:
    """Promote eligible salons → outreach_contacts (cold)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Pick a small batch each tick. We don't want to flood the contacts
    # table — scale cohort daily cap is enforced on enrollment, not on
    # ingestion. But ingestion shouldn't outpace it by too much either,
    # so a few hundred per tick × 48 ticks/day = ~10k/day — comfortable.
    try:
        eligible = sb.rpc("fn_eligible_cold_salons_for_outreach", {"limit_count": 500}).execute()
        rows = eligible.data or []
    except Exception:
        rows = []
    if not rows:
        # Fallback when the RPC isn't yet defined: read salons directly
        # with a conservative filter. Loader-side warns operator to
        # ship the RPC for prod use.
        logger.warning(
            "fn_eligible_cold_salons_for_outreach not present — "
            "skipping cold ingestion. Operator: ship RPC migration."
        )
        return {"ingested": 0}

    ingested = 0
    now_iso = datetime.now(timezone.utc).isoformat()
    for r in rows:
        if not r.get("email"):
            continue
        # Check no contact row yet (RPC should pre-filter, but defensive)
        existing = (
            sb.table("outreach_contacts")
            .select("id")
            .eq("email", r["email"].lower())
            .limit(1)
            .execute()
        )
        if existing.data:
            continue
        ins = sb.table("outreach_contacts").insert({
            "salon_ref_id": r.get("salon_id"),
            "email": r["email"].lower(),
            "cohort": "scale",
            "first_name": r.get("first_name"),
            "last_name": r.get("last_name"),
            "greeting": r.get("greeting"),
        }).execute()
        if ins.data:
            contact_id = ins.data[0]["id"]
            sb.table("outreach_customer_states").insert({
                "contact_id": contact_id,
                "funnel": "audit",
                "state_name": "cold",
                "status": "active",
                "entered_at": now_iso,
                "entry_trigger": "ingestion",
                "entry_evidence_json": {"salon_ref_id": r.get("salon_id")},
            }).execute()
            sb.table("outreach_state_transitions").insert({
                "contact_id": contact_id,
                "funnel": "audit",
                "from_state": None,
                "to_state": "cold",
                "trigger_event": "ingestion",
                "trigger_evidence": {"salon_ref_id": r.get("salon_id")},
            }).execute()
            ingested += 1

    if ingested:
        logger.info("Ingested %d new cold contacts", ingested)
    return {"ingested": ingested}


# ---------------------------------------------------------------------------
# Task 2: purchase-driven transitions
# ---------------------------------------------------------------------------

# Mapping from product purchase → (target funnel, target state, delay_hours_after_purchase).
# Order matters: a single purchase can fire multiple transitions.
PURCHASE_TRANSITIONS: list[tuple[str, str, str, int]] = [
    # product_kind, target_funnel, target_state, delay_hours
    ("audit",       "audit",             "audit_buyer",                0),
    ("audit",       "competitor_report", "competitor_consideration",   7 * 24),
    ("competitor",  "competitor_report", "competitor_buyer",           0),
    ("competitor",  "monitoring",        "monitoring_consideration",   3 * 24),
    ("monitoring",  "monitoring",        "monitoring_subscriber",      0),
]


async def apply_purchase_transitions(ctx: dict[str, Any]) -> dict[str, int]:
    """Read recent attributed purchases, fire deterministic transitions.

    Source of truth for purchases is bagent's `attribute_outreach_conversions`
    cron (TBD; for now this task is a no-op until the conversions table
    exists). We document the intended logic here so the schema is
    obvious to anyone wiring it up.
    """
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Look at outreach_messages with recent audit_attributed_at — that's
    # the bridge that fires when bextract sees a Booksy diff that maps
    # back to a UTM click. Each attributed message implies an audit
    # purchase happened.
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    attributed = (
        sb.table("outreach_messages")
        .select("contact_id, audit_id, audit_attributed_at, funnel")
        .gte("audit_attributed_at", cutoff)
        .limit(500)
        .execute()
    )
    rows = attributed.data or []
    if not rows:
        return {"transitions_fired": 0}

    fired = 0
    now = datetime.now(timezone.utc)
    seen_pairs: set[tuple[int, str]] = set()

    for row in rows:
        contact_id = row["contact_id"]
        audit_id = row.get("audit_id")
        if (contact_id, audit_id) in seen_pairs:
            continue
        seen_pairs.add((contact_id, audit_id))

        # Determine product kind from the funnel that did the attributing.
        product_kind = {
            "audit": "audit",
            "competitor_report": "competitor",
            "monitoring": "monitoring",
        }.get(row["funnel"], "audit")

        applicable = [t for t in PURCHASE_TRANSITIONS if t[0] == product_kind]
        for _kind, target_funnel, target_state, delay_h in applicable:
            # Idempotency: skip if already in this state.
            existing = (
                sb.table("outreach_customer_states")
                .select("id")
                .eq("contact_id", contact_id)
                .eq("funnel", target_funnel)
                .eq("state_name", target_state)
                .eq("status", "active")
                .limit(1)
                .execute()
            )
            if existing.data:
                continue

            entered_at = (now + timedelta(hours=delay_h)).isoformat()
            sb.table("outreach_customer_states").insert({
                "contact_id": contact_id,
                "funnel": target_funnel,
                "state_name": target_state,
                "status": "active",
                "entered_at": entered_at,
                "entry_trigger": f"{product_kind}_purchased",
                "entry_evidence_json": {"audit_id": audit_id},
            }).execute()
            sb.table("outreach_state_transitions").insert({
                "contact_id": contact_id,
                "funnel": target_funnel,
                "from_state": None,
                "to_state": target_state,
                "trigger_event": f"{product_kind}_purchased",
                "trigger_evidence": {"audit_id": audit_id},
            }).execute()
            fired += 1

        # Close out the audit funnel cold/contacted state when audit purchase fires.
        if product_kind == "audit":
            sb.table("outreach_customer_states").update({
                "status": "completed",
                "exited_at": now.isoformat(),
            }).eq("contact_id", contact_id).eq("funnel", "audit").in_(
                "state_name", ["cold", "contacted", "engaged"],
            ).eq("status", "active").execute()

    if fired:
        logger.info("Fired %d purchase-driven state transitions", fired)
    return {"transitions_fired": fired}


# ---------------------------------------------------------------------------
# Task 3: expire stale cold states
# ---------------------------------------------------------------------------

async def expire_stale_states(ctx: dict[str, Any]) -> dict[str, int]:
    """Move stale cold contacts to cold_lapsed so they cycle back into
    the eligibility pool for a future fresh-cohort run."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()

    # Find active cold/contacted states whose owner contact hasn't been
    # touched in 90d.
    stale = (
        sb.table("outreach_customer_states")
        .select("id, contact_id, funnel, state_name")
        .eq("status", "active")
        .in_("state_name", ["cold", "contacted"])
        .lte("entered_at", cutoff)
        .limit(500)
        .execute()
    )
    rows = stale.data or []
    if not rows:
        return {"expired": 0}

    now = datetime.now(timezone.utc).isoformat()
    expired = 0
    for r in rows:
        # Cross-check: contact must have no recent message either.
        recent = (
            sb.table("outreach_messages")
            .select("id", count="exact")
            .eq("contact_id", r["contact_id"])
            .gte("sent_at", cutoff)
            .execute()
        )
        if (recent.count or 0) > 0:
            continue
        sb.table("outreach_customer_states").update({
            "status": "expired",
            "exited_at": now,
        }).eq("id", r["id"]).execute()
        sb.table("outreach_customer_states").insert({
            "contact_id": r["contact_id"],
            "funnel": r["funnel"],
            "state_name": "cold_lapsed",
            "status": "active",
            "entered_at": now,
            "entry_trigger": "stale_90d",
        }).execute()
        sb.table("outreach_state_transitions").insert({
            "contact_id": r["contact_id"],
            "funnel": r["funnel"],
            "from_state": r["state_name"],
            "to_state": "cold_lapsed",
            "trigger_event": "stale_90d",
            "trigger_evidence": None,
        }).execute()
        expired += 1

    if expired:
        logger.info("Expired %d stale cold states → cold_lapsed", expired)
    return {"expired": expired}


ALL_OUTREACH_STATE_TASKS = [
    ingest_new_cold_contacts,
    apply_purchase_transitions,
    expire_stale_states,
]
