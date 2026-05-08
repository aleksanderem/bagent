"""arq tasks: ship approved outreach assets to wintact.io.

Two cron-driven tasks:

1. ``deploy_approved_templates`` (every 2 min)
   Reads outreach_template_variants WHERE approval_status='approved'
   AND wintact_template_id IS NULL. For each row, calls
   wintact /templates.create with subject + body, then records the
   returned wintact_template_id and flips approval_status to
   'deployed'. Idempotent (the wintact_template_id IS NULL guard).

2. ``deploy_approved_segments`` (every 5 min)
   Reads outreach_audience_segments WHERE approval_status='approved'
   AND wintact_list_id IS NULL. For each row, creates a wintact list
   (private), records wintact_list_id, flips status to 'deployed'.
   Population (which contacts go in the list) is handled by the
   orchestrator on the day of send — segments are passive list
   buckets here.

3. ``activate_approved_sequences`` (every 5 min)
   Reads outreach_sequences WHERE approval_status='approved' AND
   active=false. Flips active=true + records activated_at. The
   orchestrator picks up active sequences and starts enrolling
   contacts whose state matches entry_state.

NIC nie jest pchane do wintact bez approval_status='approved' — that
gate lives in Convex/admin UI; the loader writes 'pending_review' and
operator approves via /admin/outreach/approval-queue.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from config import settings
from services.sb_client import make_supabase_client
from services.wintact import WintactClient, WintactError

logger = logging.getLogger("bagent.workers.outreach_deployer")


# ---------------------------------------------------------------------------
# Task 1: templates → wintact
# ---------------------------------------------------------------------------

async def deploy_approved_templates(ctx: dict[str, Any]) -> dict[str, int]:
    """Push every approved-but-not-yet-deployed template to wintact."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table("outreach_template_variants")
        .select("id, funnel, step_key, vertical, variant_label, "
                "subject, body_md, body_html, preview_text")
        .eq("approval_status", "approved")
        .is_("wintact_template_id", "null")
        .limit(50)
        .execute()
    )
    if not rows.data:
        return {"deployed": 0, "errors": 0}

    deployed = 0
    errors = 0
    async with WintactClient() as wc:
        for row in rows.data:
            try:
                # Use body_html if pre-rendered, else fall back to markdown.
                body = row.get("body_html") or row.get("body_md") or ""
                name = (
                    f"{row['funnel']}_{row['step_key']}_"
                    f"{row['vertical']}_{row.get('variant_label', 'A')}"
                )
                resp = await wc.create_template(
                    name=name,
                    subject=row["subject"],
                    body=body,
                    preview_text=row.get("preview_text"),
                )
                wintact_id = resp.get("id") or resp.get("template_id")
                if not wintact_id:
                    raise WintactError(
                        f"wintact /templates.create returned no id: {resp}"
                    )
                sb.table("outreach_template_variants").update({
                    "wintact_template_id": wintact_id,
                    "approval_status": "deployed",
                    "deployed_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", row["id"]).execute()
                deployed += 1
                logger.info(
                    "Deployed template %s → wintact id %s",
                    name, wintact_id,
                )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.error(
                    "Failed to deploy template %s: %s",
                    row.get("id"), exc, exc_info=True,
                )

    return {"deployed": deployed, "errors": errors}


# ---------------------------------------------------------------------------
# Task 2: segments → wintact lists
# ---------------------------------------------------------------------------

async def deploy_approved_segments(ctx: dict[str, Any]) -> dict[str, int]:
    """Create wintact lists for approved segments. Population deferred
    to orchestrator (lists are buckets, not snapshots)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table("outreach_audience_segments")
        .select("id, funnel, name, description, cohort")
        .eq("approval_status", "approved")
        .is_("wintact_list_id", "null")
        .limit(20)
        .execute()
    )
    if not rows.data:
        return {"deployed": 0, "errors": 0}

    deployed = 0
    errors = 0
    async with WintactClient() as wc:
        for row in rows.data:
            try:
                list_name = f"{row['funnel']}__{row['cohort']}__{row['name']}"
                resp = await wc.create_list(
                    name=list_name,
                    description=row.get("description") or f"Auto-deployed segment {row['name']}",
                    is_public=False,
                )
                wintact_list_id = resp.get("id") or resp.get("list_id")
                if not wintact_list_id:
                    raise WintactError(
                        f"wintact /lists.create returned no id: {resp}"
                    )
                sb.table("outreach_audience_segments").update({
                    "wintact_list_id": wintact_list_id,
                    "approval_status": "deployed",
                    "deployed_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", row["id"]).execute()
                deployed += 1
                logger.info(
                    "Deployed segment %s → wintact list %s",
                    list_name, wintact_list_id,
                )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                logger.error(
                    "Failed to deploy segment %s: %s",
                    row.get("id"), exc, exc_info=True,
                )

    return {"deployed": deployed, "errors": errors}


# ---------------------------------------------------------------------------
# Task 3: sequences → activate (no wintact call, just flag)
# ---------------------------------------------------------------------------

async def activate_approved_sequences(ctx: dict[str, Any]) -> dict[str, int]:
    """Flip approved sequences to active=true so the orchestrator picks
    them up. Sequences are owned by bagent (we don't push the DAG to
    wintact — wintact only sees the per-message send)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table("outreach_sequences")
        .select("id, funnel, name, entry_state")
        .eq("approval_status", "approved")
        .eq("active", False)
        .limit(20)
        .execute()
    )
    if not rows.data:
        return {"activated": 0}

    now = datetime.now(timezone.utc).isoformat()
    activated = 0
    for row in rows.data:
        sb.table("outreach_sequences").update({
            "active": True,
            "activated_at": now,
            "approval_status": "deployed",
        }).eq("id", row["id"]).execute()
        activated += 1
        logger.info(
            "Activated sequence %s/%s (entry_state=%s)",
            row["funnel"], row["name"], row["entry_state"],
        )
    return {"activated": activated}


ALL_OUTREACH_DEPLOYER_TASKS = [
    deploy_approved_templates,
    deploy_approved_segments,
    activate_approved_sequences,
]
