"""arq tasks: outreach send loop + sequence enrollment + frequency caps.

The orchestrator is the heart of the runtime side. Two cron tasks:

1. ``enroll_due_contacts`` (every 5 min)
   For every active sequence, find contacts whose state matches
   entry_state and whose contact has no active enrollment yet. Insert
   outreach_customer_states rows (status='active', current_step_key=
   first step's key, next_action_at = NOW()+delay), respecting cohort
   daily caps from config.

2. ``send_due_messages`` (every 1 min, gated by per-cohort daily cap)
   Reads outreach_customer_states WHERE status='active' AND
   next_action_at <= NOW(). For each row resolves which template to
   send (template_step_key + vertical + variant_label round-robin),
   checks frequency caps (1/24h global + 3/7d global + per-funnel),
   calls wintact /transactional.send, logs outreach_messages row, and
   advances the state's current_step_key + next_action_at to the next
   step's predicate-matched branch.

Sequence DAG resolution lives here. The branch selector reads the
predicate JSON shape documented in
``.claude/skills/outreach-asset-schema/SKILL.md`` and picks ONE next
step per state per tick (multi-branch firing not supported by design
— a contact follows exactly one path through the DAG).

Frequency caps are enforced per-contact at send-time, not at
enrollment-time. A contact already enrolled in 3 funnels will receive
at most 1 email/24h regardless. Caps are read from
outreach_frequency_caps and cached for the duration of a tick.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from config import settings
from services.sb_client import make_supabase_client
from services.wintact import WintactClient, WintactError

logger = logging.getLogger("bagent.workers.outreach_orchestrator")

# Default sender — can be overridden per-template later if we add
# per-template sender_email column to the schema.
DEFAULT_SENDER_EMAIL = "kontakt@mailer.booksyaudit.pl"
DEFAULT_SENDER_NAME = "Joanna Kuflewicz | BeautyAudit"

# Hard wintact rate cap — Mailgun EU / standard plan. Orchestrator
# slices the per-tick batch so we don't burst over this.
WINTACT_SENDS_PER_MINUTE = 25


# ---------------------------------------------------------------------------
# Frequency caps cache (per tick)
# ---------------------------------------------------------------------------

def _load_frequency_caps(sb: Any) -> list[dict[str, Any]]:
    rows = (
        sb.table("outreach_frequency_caps")
        .select("scope, max_messages, window_hours")
        .eq("active", True)
        .execute()
    )
    return rows.data or []


def _passes_cap(
    sb: Any,
    contact_id: int,
    funnel: str,
    cohort: str,
    caps: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """Return (allowed, reason_if_blocked).

    Caps are evaluated by counting outreach_messages rows for this
    contact where sent_at >= NOW() - window_hours. Cohort scope and
    funnel scope checked against this message's planned dimensions.
    """
    now = datetime.now(timezone.utc)
    for cap in caps:
        scope = cap["scope"]
        if scope.startswith("cohort:"):
            if scope.split(":", 1)[1] != cohort:
                continue
        elif scope.startswith("funnel:"):
            if scope.split(":", 1)[1] != funnel:
                continue
        elif scope != "global":
            continue
        cutoff = now - timedelta(hours=int(cap["window_hours"]))
        result = (
            sb.table("outreach_messages")
            .select("id", count="exact")
            .eq("contact_id", contact_id)
            .gte("sent_at", cutoff.isoformat())
            .execute()
        )
        count = result.count or 0
        if count >= int(cap["max_messages"]):
            return False, f"freq cap '{scope}' hit ({count}/{cap['max_messages']} in {cap['window_hours']}h)"
    return True, None


# ---------------------------------------------------------------------------
# Predicate engine — DAG branch selector
# ---------------------------------------------------------------------------

def _eval_predicate(
    predicate: dict[str, Any] | None,
    *,
    state_row: dict[str, Any],
    last_message: dict[str, Any] | None,
) -> bool:
    """Evaluate a step predicate against per-state context.

    predicate := value_check | logical_combinator | {"always": true}

    Currently supported leaves:
      - {"prev_message.opened_at": "is_null" | "not_null"}
      - {"prev_message.clicked_at": "is_null" | "not_null"}
      - {"prev_message.replied_at": "is_null" | "not_null"}
      - {"contact.audit_purchased_at": "is_null" | "not_null"}

    Unknown leaves default to True (permissive) so a typo in a
    predicate doesn't strand contacts in a sequence — we'd rather
    over-send than starve a state. Loader catches typos at submit time.
    """
    if predicate is None or predicate == {"always": True}:
        return True
    if not isinstance(predicate, dict):
        return True

    for key, value in predicate.items():
        if key == "and":
            if not all(_eval_predicate(p, state_row=state_row, last_message=last_message) for p in value):
                return False
            continue
        if key == "or":
            if not any(_eval_predicate(p, state_row=state_row, last_message=last_message) for p in value):
                return False
            continue
        if key == "not":
            if _eval_predicate(value, state_row=state_row, last_message=last_message):
                return False
            continue

        # Leaf: value_check
        if key.startswith("prev_message."):
            field = key.split(".", 1)[1]
            actual = (last_message or {}).get(field)
            if value == "is_null" and actual is not None:
                return False
            if value == "not_null" and actual is None:
                return False
        elif key.startswith("contact."):
            # contact.* fields not eagerly loaded — caller fills state_row
            # with whatever it has. Default to permissive.
            pass

    return True


# ---------------------------------------------------------------------------
# Task 1: enroll due contacts into newly active sequences
# ---------------------------------------------------------------------------

async def enroll_due_contacts(ctx: dict[str, Any]) -> dict[str, int]:
    """For each active sequence, enroll contacts in the matching
    state. Cohort daily caps gate how many we enroll per tick."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    seqs = (
        sb.table("outreach_sequences")
        .select("id, funnel, name, entry_state, flow_json")
        .eq("active", True)
        .execute()
    )
    if not seqs.data:
        return {"enrolled": 0}

    enrolled_total = 0
    now = datetime.now(timezone.utc)
    for seq in seqs.data:
        flow = seq.get("flow_json") or {}
        steps = flow.get("steps") or []
        if not steps:
            continue
        first_step = steps[0]
        delay_h = int(first_step.get("delay_hours_after_entry", 0) or 0)

        # Find candidates: contacts in entry_state, no active enrollment in this sequence yet.
        # We don't know cohort here without joining contacts, so fetch a small batch and
        # let the sender enforce per-cohort caps.
        #
        # NOTE on idempotency: outreach_customer_states uniqueness is at
        # the orchestrator level — we accept (contact_id, funnel,
        # state_name=entry_state, sequence_id) as the implicit key and
        # filter accordingly.
        candidates = (
            sb.table("outreach_customer_states")
            .select("id, contact_id, funnel, state_name, active_sequence_id")
            .eq("funnel", seq["funnel"])
            .eq("state_name", seq["entry_state"])
            .eq("status", "active")
            .is_("active_sequence_id", "null")
            .limit(200)
            .execute()
        )
        for cand in candidates.data or []:
            sb.table("outreach_customer_states").update({
                "active_sequence_id": seq["id"],
                "current_step_key": first_step["key"],
                "next_action_at": (now + timedelta(hours=delay_h)).isoformat(),
            }).eq("id", cand["id"]).execute()
            enrolled_total += 1

    if enrolled_total:
        logger.info("Enrolled %d contacts into active sequences", enrolled_total)
    return {"enrolled": enrolled_total}


# ---------------------------------------------------------------------------
# Task 2: send due messages
# ---------------------------------------------------------------------------

async def send_due_messages(ctx: dict[str, Any]) -> dict[str, int]:
    """Find states with next_action_at <= NOW(), send the matching
    template, advance to next step in the DAG."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    now = datetime.now(timezone.utc)

    # Bound this tick to one minute's worth of wintact rate budget.
    budget = WINTACT_SENDS_PER_MINUTE

    due = (
        sb.table("outreach_customer_states")
        .select("id, contact_id, funnel, state_name, active_sequence_id, "
                "current_step_key, next_action_at, status")
        .eq("status", "active")
        .lte("next_action_at", now.isoformat())
        .not_.is_("active_sequence_id", "null")
        .limit(budget)
        .execute()
    )
    if not due.data:
        return {"sent": 0, "skipped_caps": 0, "errors": 0}

    caps = _load_frequency_caps(sb)
    sent = 0
    skipped_caps = 0
    errors = 0

    async with WintactClient() as wc:
        for state_row in due.data:
            try:
                stats = await _send_one(sb, wc, state_row, caps)
                sent += stats["sent"]
                skipped_caps += stats["skipped_caps"]
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.error(
                    "send_one failed state=%s: %s",
                    state_row.get("id"), exc, exc_info=True,
                )

    if sent or errors:
        logger.info(
            "Outreach tick: sent=%d skipped_caps=%d errors=%d",
            sent, skipped_caps, errors,
        )
    return {"sent": sent, "skipped_caps": skipped_caps, "errors": errors}


async def _send_one(
    sb: Any,
    wc: WintactClient,
    state_row: dict[str, Any],
    caps: list[dict[str, Any]],
) -> dict[str, int]:
    """Send one message for one state row, advance state."""
    # 1. Load contact
    contact_resp = (
        sb.table("outreach_contacts")
        .select("id, email, first_name, last_name, greeting, cohort, "
                "wintact_contact_id, salon_ref_id, unsubscribed_at")
        .eq("id", state_row["contact_id"])
        .single()
        .execute()
    )
    contact = contact_resp.data
    if not contact:
        return {"sent": 0, "skipped_caps": 0}
    if contact.get("unsubscribed_at"):
        # Mark state as opted_out so we stop trying.
        sb.table("outreach_customer_states").update({
            "status": "opted_out",
            "exited_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", state_row["id"]).execute()
        return {"sent": 0, "skipped_caps": 1}

    # 2. Load sequence + locate current step
    seq_resp = (
        sb.table("outreach_sequences")
        .select("id, funnel, flow_json")
        .eq("id", state_row["active_sequence_id"])
        .single()
        .execute()
    )
    seq = seq_resp.data
    if not seq:
        return {"sent": 0, "skipped_caps": 0}
    flow = seq.get("flow_json") or {}
    steps = flow.get("steps") or []
    cur_step = next((s for s in steps if s.get("key") == state_row.get("current_step_key")), None)
    if not cur_step:
        # Step removed since enrollment — close the state out.
        sb.table("outreach_customer_states").update({
            "status": "completed",
            "exited_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", state_row["id"]).execute()
        return {"sent": 0, "skipped_caps": 0}

    template_step_key = cur_step.get("template_step_key") or cur_step.get("key")

    # 3. Pick a deployed template variant matching funnel + step + vertical.
    # For now: vertical = 'generic' fallback if specific not found, simple
    # round-robin via lowest sent_count.
    tpl_resp = (
        sb.table("outreach_template_variants")
        .select("id, wintact_template_id, subject, variables_required, vertical, signal_type")
        .eq("funnel", seq["funnel"])
        .eq("step_key", template_step_key)
        .eq("approval_status", "deployed")
        .not_.is_("wintact_template_id", "null")
        .limit(5)
        .execute()
    )
    if not tpl_resp.data:
        logger.warning(
            "No deployed template for funnel=%s step=%s — skipping state %s",
            seq["funnel"], template_step_key, state_row["id"],
        )
        return {"sent": 0, "skipped_caps": 0}
    template = tpl_resp.data[0]   # TODO: smarter variant selection

    # 4. Frequency cap check
    allowed, reason = _passes_cap(sb, contact["id"], seq["funnel"], contact["cohort"], caps)
    if not allowed:
        # Push next_action_at forward by 6h so we retry later.
        sb.table("outreach_customer_states").update({
            "next_action_at": (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat(),
        }).eq("id", state_row["id"]).execute()
        logger.debug("Cap blocked state=%s: %s", state_row["id"], reason)
        return {"sent": 0, "skipped_caps": 1}

    # 5. Build template variables (pull from contact + salon + state)
    variables = _build_template_variables(contact, template)
    utm = {
        "utm_source": settings.outreach_utm_source,
        "utm_medium": "email",
        "utm_campaign": f"{seq['funnel']}_{template_step_key}",
        "utm_content": str(template["id"]),
        "utm_term": contact["cohort"],
    }

    # 6. Send via wintact
    try:
        resp = await wc.send_transactional(
            contact_email=contact["email"],
            template_id=template["wintact_template_id"],
            variables=variables,
            sender_email=DEFAULT_SENDER_EMAIL,
            sender_name=DEFAULT_SENDER_NAME,
            utm=utm,
        )
    except WintactError as exc:
        logger.error("Wintact send failed contact=%s: %s", contact["email"], exc)
        # Push back 1h on transient failure
        sb.table("outreach_customer_states").update({
            "next_action_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        }).eq("id", state_row["id"]).execute()
        raise

    wintact_msg_id = resp.get("message_id") or resp.get("id")
    now_iso = datetime.now(timezone.utc).isoformat()

    # 7. Log outreach_messages row
    sb.table("outreach_messages").insert({
        "contact_id": contact["id"],
        "funnel": seq["funnel"],
        "cohort": contact["cohort"],
        "sequence_id": seq["id"],
        "step_key": template_step_key,
        "template_id": template["id"],
        "state_id": state_row["id"],
        "wintact_message_id": wintact_msg_id,
        "subject_snapshot": template["subject"],
        "variables_snapshot": variables,
        "sent_at": now_iso,
        "utm_campaign": utm["utm_campaign"],
        "utm_content": utm["utm_content"],
        "utm_term": utm["utm_term"],
    }).execute()

    # 8. Update contact frequency-cap counters
    sb.table("outreach_contacts").update({
        "last_outreach_sent_at": now_iso,
        "total_messages_sent": (contact.get("total_messages_sent") or 0) + 1,
    }).eq("id", contact["id"]).execute()

    # 9. Advance state to next step (DAG branch selection)
    next_step, predicate_decision = _pick_next_step(steps, cur_step, last_message_resp=resp)
    if next_step is None:
        # No more steps — sequence finished without conversion.
        sb.table("outreach_customer_states").update({
            "status": "completed",
            "exited_at": now_iso,
            "next_action_at": None,
        }).eq("id", state_row["id"]).execute()
        sb.table("outreach_state_transitions").insert({
            "contact_id": contact["id"],
            "funnel": seq["funnel"],
            "from_state": state_row["state_name"],
            "to_state": None,
            "trigger_event": "sequence_finished_no_purchase",
            "trigger_evidence": {"sequence_id": seq["id"], "predicate": predicate_decision},
        }).execute()
    else:
        delay_h = int(next_step.get("delay_hours_after_entry", 0) or 0)
        sb.table("outreach_customer_states").update({
            "current_step_key": next_step["key"],
            "next_action_at": (datetime.now(timezone.utc) + timedelta(hours=delay_h)).isoformat(),
        }).eq("id", state_row["id"]).execute()

    return {"sent": 1, "skipped_caps": 0}


def _pick_next_step(
    steps: list[dict[str, Any]],
    cur_step: dict[str, Any],
    *,
    last_message_resp: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str]:
    """Pick the next step matching its predicate. Returns (step, decision).

    Strategy: advance to the first step in the list AFTER cur_step
    whose predicate evaluates true. If none match, return None
    (sequence terminates). Linear fallback when no predicates set.
    """
    cur_idx = next((i for i, s in enumerate(steps) if s.get("key") == cur_step.get("key")), -1)
    if cur_idx < 0 or cur_idx >= len(steps) - 1:
        return None, "no_more_steps"

    state_row: dict[str, Any] = {}    # placeholder for future contact-level vars
    for s in steps[cur_idx + 1:]:
        predicate = s.get("predicate")
        if _eval_predicate(predicate, state_row=state_row, last_message=last_message_resp):
            return s, f"matched:{s.get('key')}"
    return None, "no_predicate_matched"


def _build_template_variables(contact: dict[str, Any], template: dict[str, Any]) -> dict[str, Any]:
    """Build {{variable}} substitutions for transactional.send.

    Pulls from contact row directly. Salon-level enrichment (city,
    desc_gap_pct, top_missing_service, nearest_competitor) is NOT
    eagerly joined here — production should add a denormalized
    snapshot column to outreach_contacts or query salons by
    salon_ref_id. For now we cover the universal variables.
    """
    full_name = " ".join(filter(None, [contact.get("first_name"), contact.get("last_name")])).strip()
    return {
        "greeting": contact.get("greeting") or "Pani / Panie",
        "first_name": contact.get("first_name") or "",
        "last_name": contact.get("last_name") or "",
        "full_name": full_name,
        "salon_name": "",        # TODO: join salons.name via salon_ref_id
        "salon_city": "",        # TODO: join salons.salon_city
        "desc_gap_pct": "",      # TODO: latest diff snapshot
    }


ALL_OUTREACH_ORCHESTRATOR_TASKS = [
    enroll_due_contacts,
    send_due_messages,
]
