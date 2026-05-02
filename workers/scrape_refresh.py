"""Issue #23 — scrape orchestrator worker tasks.

Workers are arq tasks that:
  1. Claim a batch of due jobs from ``salon_refresh_queue`` via the
     ``claim_salon_refresh_jobs()`` SQL helper (SELECT FOR UPDATE SKIP
     LOCKED so multiple worker processes scale linearly).
  2. For each claimed booksy_id, fetch fresh JSON from bextract and
     persist it via :func:`bagent.ingestion.fetch_and_persist_salon`.
  3. Update queue rows: ``status='done'`` on success, retry-with-backoff
     on transient failure (max 5 attempts), ``status='failed'`` on
     terminal failure.
  4. If the salon is in an active competitor_matches row (tier-1
     subject), trigger a competitor_report refresh by calling our own
     bagent ``/api/competitor/report/refresh`` endpoint.

Two arq entrypoints are registered:

* :func:`drain_scrape_queue` — single batch of N jobs. The worker pool
  cron kicks this every minute so each tick processes a small batch.
  Decoupling from "long-running consumer" patterns means a worker that
  crashes mid-batch leaks at most ``CLAIM_BATCH_SIZE`` running jobs,
  reaped by the zombie sweep.
* :func:`schedule_refresh_cron` — calls
  :func:`bagent.scheduler.schedule_due_refreshes` once an hour to top up
  the queue.
* :func:`reap_stuck_jobs` — calls ``reap_stuck_salon_refresh_jobs()``
  every 10 minutes.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from supabase import Client, ClientOptions, create_client

from config import settings
from ingestion import LiveIngestError, fetch_and_persist_salon

logger = logging.getLogger("bagent.workers.scrape_refresh")

# Per-tick batch size. Lowered from 10 to 5 because each salon scrape
# triggers ~3-5 Booksy requests inside bextract (business + services +
# reviews + top_services + gallery), and 10 jobs × 4 reqs in a one-
# minute burst was hitting Booksy's 429 rate limit.
# 5 per tick × 60 ticks/h = 300 salons/h sustained, low bursts.
CLAIM_BATCH_SIZE = 5

# Sleep between salon fetches inside a single drain tick. Spreads
# Booksy traffic so a slow tick doesn't pile up requests at the start.
INTER_FETCH_SLEEP_SEC = 1.0


_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
            options=ClientOptions(schema="public"),
        )
    return _supabase_client


# ---------------------------------------------------------------------------
# competitor refresh trigger (tier 1 only)
# ---------------------------------------------------------------------------


async def _maybe_trigger_competitor_refresh(booksy_id: int) -> None:
    """If `booksy_id` corresponds to the subject of an active competitor
    report, POST to bagent's own /api/competitor/report/refresh.

    No-op when no active report exists. Errors are logged but do not
    propagate — a failed refresh shouldn't fail the scrape job, the
    fresh salon_scrapes row is still useful on its own.
    """
    client = _get_client()
    salon_lookup = (
        client.table("salons")
        .select("id")
        .eq("booksy_id", booksy_id)
        .maybe_single()
        .execute()
    )
    salon_row = getattr(salon_lookup, "data", None)
    if not salon_row or "id" not in salon_row:
        return
    salon_id = salon_row["id"]

    report_lookup = (
        client.table("competitor_reports")
        .select("id, convex_audit_id, convex_user_id, tier")
        .eq("subject_salon_id", salon_id)
        .eq("tier", "premium")
        .order("created_at", desc=True)
        .limit(1)
        .maybe_single()
        .execute()
    )
    report = getattr(report_lookup, "data", None)
    if not report:
        return

    bagent_url = "http://127.0.0.1:3001"  # local self-call; or settings.bagent_self_url
    payload = {
        "reportId": report["id"],
        "auditId": report.get("convex_audit_id"),
        "userId": report.get("convex_user_id"),
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as ac:
            r = await ac.post(
                f"{bagent_url}/api/competitor/report/refresh",
                headers={"x-api-key": settings.api_key},
                json=payload,
            )
            if r.status_code >= 300:
                logger.warning(
                    "[scrape_refresh] competitor refresh trigger %s for report %s",
                    r.status_code, report["id"],
                )
    except httpx.HTTPError as e:
        logger.warning(
            "[scrape_refresh] competitor refresh trigger raised %s for report %s",
            e, report["id"],
        )


# ---------------------------------------------------------------------------
# arq tasks
# ---------------------------------------------------------------------------


async def drain_scrape_queue(ctx: dict[str, Any]) -> dict[str, int]:
    """Claim CLAIM_BATCH_SIZE due jobs and process them serially.

    Returns counts: {claimed, succeeded, requeued, failed, skipped}.
    """
    worker_id = settings.scrape_worker_id
    client = _get_client()
    res = client.rpc(
        "claim_salon_refresh_jobs",
        {"p_limit": CLAIM_BATCH_SIZE, "p_worker_id": worker_id},
    ).execute()
    jobs = res.data or []
    if not jobs:
        return {"claimed": 0, "succeeded": 0, "requeued": 0, "failed": 0, "skipped": 0}

    counts = {"claimed": len(jobs), "succeeded": 0, "requeued": 0, "failed": 0, "skipped": 0}

    for idx, job in enumerate(jobs):
        # Politeness delay: give Booksy ~1s between salon fetches so a
        # full batch (5 salons * ~4 inner Booksy reqs = 20 reqs) is
        # spread over ~5s instead of bursting in <1s.
        if idx > 0:
            await asyncio.sleep(INTER_FETCH_SLEEP_SEC)
        job_id = job["id"]
        booksy_id = int(job["booksy_id"])
        tier = int(job.get("tier", 3))
        try:
            result = await fetch_and_persist_salon(
                booksy_id, batch_tag=f"orchestrator:tier{tier}"
            )
            if result.skipped:
                counts["skipped"] += 1
            else:
                counts["succeeded"] += 1
                # Tier 1 only: cascade refresh into competitor report.
                if tier == 1:
                    await _maybe_trigger_competitor_refresh(booksy_id)
            client.rpc(
                "complete_salon_refresh_job",
                {"p_id": job_id, "p_error": None},
            ).execute()
        except LiveIngestError as e:
            error_text = str(e)[:500]
            outcome = client.rpc(
                "complete_salon_refresh_job",
                {"p_id": job_id, "p_error": error_text},
            ).execute()
            if (outcome.data or "") == "failed":
                counts["failed"] += 1
            else:
                counts["requeued"] += 1
            logger.warning(
                "[scrape_refresh] booksy_id=%s job=%s failed: %s",
                booksy_id, job_id, error_text,
            )
        except Exception as e:  # noqa: BLE001 - safety net
            error_text = f"unexpected {type(e).__name__}: {str(e)[:400]}"
            client.rpc(
                "complete_salon_refresh_job",
                {"p_id": job_id, "p_error": error_text},
            ).execute()
            counts["requeued"] += 1
            logger.exception(
                "[scrape_refresh] unexpected failure for booksy_id=%s",
                booksy_id,
            )

    logger.info(
        "[scrape_refresh] drain summary worker=%s claimed=%d ok=%d skipped=%d requeued=%d failed=%d",
        worker_id, counts["claimed"], counts["succeeded"],
        counts["skipped"], counts["requeued"], counts["failed"],
    )
    return counts


async def schedule_refresh_cron(ctx: dict[str, Any]) -> dict[str, int]:
    """Top up the queue with newly-due jobs across all 3 tiers."""
    # Import locally so test environments without scheduler deps still
    # import workers.scrape_refresh cleanly.
    from scheduler import schedule_due_refreshes

    result = await asyncio.to_thread(schedule_due_refreshes)
    # Issue #25 — heartbeat to Healthchecks so we know the cron is alive
    await _healthcheck_ping("HC_PING_SCRAPE_ORCHESTRATOR")
    return {
        "tier1_enqueued": result.enqueued.get(1, 0),
        "tier2_enqueued": result.enqueued.get(2, 0),
        "tier3_enqueued": result.enqueued.get(3, 0),
        "suppressed_dupes": result.suppressed_dupes,
        "skipped_no_booksy_id": result.skipped_no_booksy_id,
    }


async def reap_stuck_jobs(ctx: dict[str, Any]) -> dict[str, int]:
    """Mark jobs whose worker died (locked > 30min) back to 'queued'."""
    client = _get_client()
    res = client.rpc("reap_stuck_salon_refresh_jobs", {}).execute()
    reaped = int(res.data or 0)
    if reaped > 0:
        logger.warning("[scrape_refresh] reaped %d stuck jobs", reaped)
    return {"reaped": reaped}


# ---------------------------------------------------------------------------
# Issue #25 — Healthchecks ping helper
# ---------------------------------------------------------------------------


async def _healthcheck_ping(env_var_name: str, fail: bool = False) -> None:
    """Fire-and-forget GET to a Healthchecks ping URL.

    Reads the URL from env var ``env_var_name``; no-ops when missing so
    the orchestrator works without observability wired up. Always
    swallows exceptions — a healthcheck ping failure must never break
    the actual job.
    """
    import os
    url = os.environ.get(env_var_name)
    if not url:
        return
    if fail:
        url = url.rstrip("/") + "/fail"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.get(url)
    except Exception as e:  # noqa: BLE001
        logger.warning("[scrape_refresh] healthcheck ping failed: %s", e)


# All scrape-orchestrator tasks for registration in WorkerSettings.
ALL_SCRAPE_TASKS = [drain_scrape_queue, schedule_refresh_cron, reap_stuck_jobs]
