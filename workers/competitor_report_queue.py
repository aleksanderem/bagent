"""beads BEAUTY_AUDIT-1mb — competitor report queue drain + reap.

Competitor reports are the heaviest job we run (multiple OpenAI + MiniMax LLM
calls, ~10-26 min each). Previously the endpoint enqueued them DIRECTLY to arq
with no cap, so firing N reports at once launched N concurrent heavy LLM jobs →
OpenAI quota spikes + worker-pool contention.

This module drains ``competitor_report_queue`` (migration 131) with a GLOBAL
concurrency cap — the one difference from the scrape lane
(``workers/scrape_refresh.py``). The cap is enforced DB-side inside
``claim_competitor_report_jobs(p_cap, p_worker_id)`` which only returns
``GREATEST(0, p_cap - running)`` rows, so the global running total can never
exceed the cap even under concurrent ticks.

Two arq entrypoints are registered (in :data:`ALL_COMPETITOR_QUEUE_TASKS`):

* :func:`drain_competitor_report_queue` — claims up to (cap - running) due jobs
  and enqueues one ``run_competitor_report_task`` per claimed row. Each enqueue
  reuses ``arq_job_id`` as arq's ``_job_id`` (so the frontend's existing jobId
  polling keeps working) and threads ``_queue_id`` into the payload (so the task
  can release its slot via ``complete_competitor_report_job``).
* :func:`reap_stuck_competitor_report_jobs` — requeues zombie jobs whose worker
  died (locked > 1h), via ``reap_stuck_competitor_report_jobs()``.

The drain is fired by an arq cron once a minute (per-second granularity is
pointless — the cap, not the tick frequency, gates throughput).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from supabase import Client

from services.sb_client import make_supabase_client

from config import settings

logger = logging.getLogger("bagent.workers.competitor_report_queue")


def _read_cap() -> int:
    """Global concurrency cap for competitor reports, floored at 1.

    Read fresh at drain time so an env change after a worker restart takes
    effect. A bad (non-numeric) env value falls back to 4 rather than crashing
    worker boot.
    """
    try:
        cap = int(os.environ.get("COMPETITOR_REPORT_MAX_CONCURRENT", "4"))
    except ValueError:
        return 4
    return max(1, cap)


# Sane default + export for the registry/test. The drain calls _read_cap()
# directly so a post-restart env change is honored without re-import.
COMPETITOR_REPORT_MAX_CONCURRENT = _read_cap()


_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


# ---------------------------------------------------------------------------
# arq tasks
# ---------------------------------------------------------------------------


async def drain_competitor_report_queue(ctx: dict[str, Any]) -> dict[str, int]:
    """Claim up to (cap - running) due reports and enqueue one task each.

    The claim RPC enforces the cap DB-side, so every claimed row is enqueued.
    Returns counts: {claimed, enqueued}.
    """
    cap = _read_cap()
    worker_id = settings.scrape_worker_id
    client = _get_client()
    res = client.rpc(
        "claim_competitor_report_jobs",
        {"p_cap": cap, "p_worker_id": worker_id},
    ).execute()
    jobs = res.data or []
    if not jobs:
        return {"claimed": 0, "enqueued": 0}

    redis = ctx["redis"]
    enqueued = 0
    for job in jobs:
        # camelCase keys MUST match run_competitor_report_task's reads
        # (request["auditId"], request.get("selectionMode"), ...).
        payload = {
            "auditId": job["audit_id"],
            "userId": job["user_id"],
            "tier": job.get("tier", "base"),
            "selectionMode": job.get("selection_mode", "auto"),
            "targetCount": job.get("target_count", 5),
            # Webhook callback routing (migration 148). claim_competitor_report_jobs
            # RETURNS SETOF competitor_report_queue, so the new column rides along
            # in the claimed row with no claim-function change. None for rows
            # enqueued before the migration / by an old Convex → task falls back
            # to the global settings.convex_url.
            "convexSiteUrl": job.get("convex_site_url"),
            "_queue_id": job["id"],
        }
        # _job_id=arq_job_id so the frontend's jobId polling keeps working.
        # _queue_name="arq:reports" routes the task to the dedicated nice'd
        # bagent-report-worker (ReportWorkerSettings), isolated from the scrape
        # pump (2026-06-15). MUST match ReportWorkerSettings.queue_name.
        await redis.enqueue_job(
            "run_competitor_report_task",
            payload,
            _job_id=job["arq_job_id"],
            _queue_name="arq:reports",
        )
        enqueued += 1

    logger.info(
        "[competitor_report_queue] drain cap=%d claimed=%d enqueued=%d",
        cap, len(jobs), enqueued,
    )
    return {"claimed": len(jobs), "enqueued": enqueued}


async def reap_stuck_competitor_report_jobs(ctx: dict[str, Any]) -> dict[str, int]:
    """Requeue jobs whose worker died (locked > 1h) back to 'queued'."""
    client = _get_client()
    res = client.rpc("reap_stuck_competitor_report_jobs", {}).execute()
    reaped = int(res.data or 0)
    if reaped > 0:
        logger.warning("[competitor_report_queue] reaped %d stuck jobs", reaped)
    return {"reaped": reaped}


# All competitor-report-queue tasks for registration in WorkerSettings.
ALL_COMPETITOR_QUEUE_TASKS = [
    drain_competitor_report_queue,
    reap_stuck_competitor_report_jobs,
]
