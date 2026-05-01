"""arq task definitions — production pipelines migrated from server.py
FastAPI BackgroundTasks (issue #21).

Each task here corresponds to one `run_*_job` function in server.py. The
old functions kept their own JobStore Job + log handler + Convex webhook
sequence. In the worker process we drop the JobStore tracking (workers and
the web process are SEPARATE processes — they can't share the in-memory
JobStore singleton) but preserve every other production-critical
behavior:

  - Convex webhooks (progress / complete / fail) — unchanged contract,
    same payload shapes, same try/except tolerance for webhook failures
  - Cancel-check pattern via Redis key `bagent:cancel:{job_id}` set by
    the FastAPI cancel endpoint — task's on_progress callback polls this
    key and raises CancelledError if found
  - Same pipeline arguments and result handling
  - Same Supabase save calls (or pipeline-internal save when applicable)

Dashboard regression: the web process's JobStore no longer sees live
progress for these tasks (it only records "queued" at enqueue time). The
production-critical surface — frontend reading audit progress through
Convex — is unchanged. Dashboard live updates can be added back later
via Redis pub/sub if needed; for now arq tasks are observable via PM2
logs (`pm2 logs bagent-worker`) and the worker logs are forwarded to
Bugsink (issue #24).

Argument convention: every task receives `ctx: dict` (arq context with
'redis' and 'job_id' keys) and a single `request: dict` payload. The
dict is reconstructed into the original Pydantic model inside the task
because arq does not natively serialize Pydantic v2 models — passing
`.model_dump()` is the idiomatic pattern (see
https://arq-docs.helpmanual.io/#argument-serialization).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from arq.connections import ArqRedis

logger = logging.getLogger("bagent.workers.tasks")


CANCEL_KEY_PREFIX = "bagent:cancel:"
CANCEL_KEY_TTL = 3600  # 1h — long enough that even slow audits can be cancelled mid-run


class _CancelledByUser(Exception):
    """Raised by on_progress when the cancel flag is set in Redis.

    Caught locally inside each task and reported as a graceful cancel
    via Convex *_fail webhook. Distinct from arq's own CancelledError
    so we don't conflate user cancellation with worker shutdown.
    """


async def _check_cancel(ctx: dict[str, Any]) -> bool:
    """Poll Redis for the cancel flag set by the cancel endpoint.

    Returns True if cancel was requested. On any Redis error we return
    False — better to let the task complete than crash on a transient
    Redis hiccup mid-job.
    """
    redis: ArqRedis = ctx["redis"]
    job_id = ctx.get("job_id", "?")
    try:
        flag = await redis.get(f"{CANCEL_KEY_PREFIX}{job_id}")
        return flag is not None
    except Exception as e:  # noqa: BLE001
        logger.warning("cancel-check Redis read failed for %s: %s", job_id, e)
        return False


async def set_cancel_flag(redis: ArqRedis, job_id: str) -> None:
    """Set the Redis cancel flag — called from the FastAPI cancel endpoint."""
    await redis.set(f"{CANCEL_KEY_PREFIX}{job_id}", "1", ex=CANCEL_KEY_TTL)


async def clear_cancel_flag(redis: ArqRedis, job_id: str) -> None:
    """Cleanup helper — task should clear after handling cancel."""
    try:
        await redis.delete(f"{CANCEL_KEY_PREFIX}{job_id}")
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Task #1: BAGENT #1 — audit report
# ---------------------------------------------------------------------------

async def run_report_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """BAGENT #1 — content optimization pipeline (audit report).

    request payload (matches ReportRequest):
        auditId: str
        userId: str
        sourceUrl: str | None
        scrapedData: dict | None  # inline payload for dev/testing
    """
    from models.scraped_data import ScrapedData
    from pipelines.report import run_audit_pipeline
    from services.convex import ConvexClient
    from services.supabase import SupabaseService

    job_id = ctx.get("job_id", "?")
    audit_id = request["auditId"]
    user_id = request["userId"]
    source_url = request.get("sourceUrl") or ""
    inline_scrape = request.get("scrapedData")

    logger.info("[%s] run_report_task started for audit_id=%s", job_id, audit_id)
    convex = ConvexClient()

    try:
        # ── Load scraped data: prefer inline payload, fall back to Supabase ──
        if inline_scrape:
            raw_scraped = inline_scrape
        else:
            supabase = SupabaseService()
            raw_scraped = await supabase.get_scraped_data(audit_id)
            if not raw_scraped:
                raise RuntimeError(
                    f"No audit_scraped_data for {audit_id} and no inline payload"
                )
        scraped_data = ScrapedData(**raw_scraped)

        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)
            try:
                await convex.report_progress(audit_id, progress, message)
            except Exception as e:  # noqa: BLE001
                logger.warning("[%s] convex.report_progress failed: %s", job_id, e)

        report = await run_audit_pipeline(scraped_data, audit_id, on_progress)

        supabase = SupabaseService()
        await supabase.save_report(
            convex_audit_id=audit_id,
            convex_user_id=user_id,
            report=report,
            salon_name=scraped_data.salonName or "",
            salon_address=scraped_data.salonAddress or "",
            source_url=source_url,
        )

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        try:
            await convex.report_complete(
                audit_id=audit_id,
                user_id=user_id,
                overall_score=report.get("totalScore", 0),
                report_stats=report_stats,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] convex.report_complete failed (job still done): %s", job_id, e)

        logger.info("[%s] run_report_task completed", job_id)
        return {"audit_id": audit_id, "score": report.get("totalScore", 0), "stats": report_stats}

    except _CancelledByUser:
        logger.info("[%s] run_report_task cancelled by user", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        try:
            await convex.report_fail(audit_id, "Cancelled by user")
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception as e:
        logger.error("[%s] run_report_task failed: %s", job_id, e, exc_info=True)
        try:
            await convex.report_fail(audit_id, str(e))
        except Exception:  # noqa: BLE001
            pass
        raise


# ---------------------------------------------------------------------------
# Task #2: free_report — frozen BAGENT #1 snapshot
# ---------------------------------------------------------------------------

async def run_free_report_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """Free-tier mirror of run_report_task using pipelines.free_report.

    Intentional duplication of run_report_task control flow — keeps the
    frozen free-tier path stable when run_report_task evolves. Same
    Convex webhooks (report/* — Convex doesn't currently distinguish tiers).
    """
    from models.scraped_data import ScrapedData
    from pipelines.free_report import run_free_report_pipeline
    from services.convex import ConvexClient
    from services.supabase import SupabaseService

    job_id = ctx.get("job_id", "?")
    audit_id = request["auditId"]
    user_id = request["userId"]
    source_url = request.get("sourceUrl") or ""
    inline_scrape = request.get("scrapedData")

    logger.info("[%s] run_free_report_task started for audit_id=%s", job_id, audit_id)
    convex = ConvexClient()

    try:
        if inline_scrape:
            raw_scraped = inline_scrape
        else:
            supabase = SupabaseService()
            raw_scraped = await supabase.get_scraped_data(audit_id)
            if not raw_scraped:
                raise RuntimeError(
                    f"No audit_scraped_data for {audit_id} and no inline payload"
                )
        scraped_data = ScrapedData(**raw_scraped)

        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)
            try:
                await convex.report_progress(audit_id, progress, message)
            except Exception as e:  # noqa: BLE001
                logger.warning("[%s] convex.report_progress failed: %s", job_id, e)

        report = await run_free_report_pipeline(scraped_data, audit_id, on_progress)

        supabase = SupabaseService()
        await supabase.save_report(
            convex_audit_id=audit_id,
            convex_user_id=user_id,
            report=report,
            salon_name=scraped_data.salonName or "",
            salon_address=scraped_data.salonAddress or "",
            source_url=source_url,
        )

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        try:
            await convex.report_complete(
                audit_id=audit_id,
                user_id=user_id,
                overall_score=report.get("totalScore", 0),
                report_stats=report_stats,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] convex.report_complete failed: %s", job_id, e)

        logger.info("[%s] run_free_report_task completed", job_id)
        return {"audit_id": audit_id, "score": report.get("totalScore", 0), "stats": report_stats}

    except _CancelledByUser:
        logger.info("[%s] run_free_report_task cancelled by user", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        try:
            await convex.report_fail(audit_id, "Cancelled by user")
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception as e:
        logger.error("[%s] run_free_report_task failed: %s", job_id, e, exc_info=True)
        try:
            await convex.report_fail(audit_id, str(e))
        except Exception:  # noqa: BLE001
            pass
        raise


# ---------------------------------------------------------------------------
# Task #3: BAGENT #2 — cennik (deterministic, no AI)
# ---------------------------------------------------------------------------

async def run_cennik_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """BAGENT #2 — deterministic optimized pricelist generation.

    Pipeline saves to Supabase internally (optimized_pricelists +
    optimized_services). We just propagate progress/complete/fail webhooks.
    """
    from pipelines.cennik import run_cennik_pipeline
    from services.convex import ConvexClient

    job_id = ctx.get("job_id", "?")
    audit_id = request["auditId"]

    logger.info("[%s] run_cennik_task started for audit_id=%s", job_id, audit_id)
    convex = ConvexClient()

    try:
        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)
            try:
                await convex.cennik_progress(audit_id, progress, message)
            except Exception as e:  # noqa: BLE001
                logger.warning("[%s] convex.cennik_progress failed: %s", job_id, e)

        result = await run_cennik_pipeline(audit_id=audit_id, on_progress=on_progress)

        try:
            await convex.cennik_complete(
                audit_id,
                category_proposal=result.get("category_proposal"),
                stats=result.get("stats"),
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] convex.cennik_complete failed: %s", job_id, e)

        logger.info("[%s] run_cennik_task completed", job_id)
        return result

    except _CancelledByUser:
        logger.info("[%s] run_cennik_task cancelled by user", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        try:
            await convex.cennik_fail(audit_id, "Cancelled by user")
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception as e:
        logger.error("[%s] run_cennik_task failed: %s", job_id, e, exc_info=True)
        try:
            await convex.cennik_fail(audit_id, str(e))
        except Exception:  # noqa: BLE001
            pass
        raise


# ---------------------------------------------------------------------------
# Task #4: BAGENT #3 — summary
# ---------------------------------------------------------------------------

async def run_summary_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """BAGENT #3 — audit summary with basic competitor preview.

    Pipeline saves to audit_summaries internally. Webhook is one-arg
    (audit_id only) — Convex queries summary table by audit_id.
    """
    from pipelines.summary import run_summary_pipeline
    from services.convex import ConvexClient

    job_id = ctx.get("job_id", "?")
    audit_id = request["auditId"]
    user_id = request["userId"]
    selected_competitor_ids = request.get("selectedCompetitorIds", [])

    logger.info("[%s] run_summary_task started for audit_id=%s competitors=%d",
                job_id, audit_id, len(selected_competitor_ids))
    convex = ConvexClient()

    try:
        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)
            try:
                await convex.summary_progress(audit_id, progress, message)
            except Exception as e:  # noqa: BLE001
                logger.warning("[%s] convex.summary_progress failed: %s", job_id, e)

        result = await run_summary_pipeline(
            audit_id=audit_id,
            user_id=user_id,
            selected_competitor_ids=selected_competitor_ids,
            on_progress=on_progress,
        )

        try:
            await convex.summary_complete(audit_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] convex.summary_complete failed: %s", job_id, e)

        logger.info("[%s] run_summary_task completed", job_id)
        return result

    except _CancelledByUser:
        logger.info("[%s] run_summary_task cancelled by user", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        try:
            await convex.summary_fail(audit_id, "Cancelled by user")
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception as e:
        logger.error("[%s] run_summary_task failed: %s", job_id, e, exc_info=True)
        try:
            await convex.summary_fail(audit_id, str(e))
        except Exception:  # noqa: BLE001
            pass
        raise


# ---------------------------------------------------------------------------
# Task #5: Comp Etap 4+5 — competitor report
# ---------------------------------------------------------------------------

async def run_competitor_report_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """BAGENT #4 — competitor report (selection + analysis + AI synthesis).

    Pipeline saves to competitor_reports + 5 child tables internally.
    Result dict includes report_id + stats for the Convex webhook.
    """
    from pipelines.competitor_report import run_competitor_report_pipeline
    from services.convex import ConvexClient

    job_id = ctx.get("job_id", "?")
    audit_id = request["auditId"]
    user_id = request["userId"]
    tier = request.get("tier", "base")
    selection_mode = request.get("selectionMode", "auto")
    target_count = request.get("targetCount", 5)

    logger.info("[%s] run_competitor_report_task started audit_id=%s tier=%s",
                job_id, audit_id, tier)
    convex = ConvexClient()

    try:
        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)
            try:
                await convex.competitor_report_progress(audit_id, progress, message)
            except Exception as e:  # noqa: BLE001
                logger.warning("[%s] convex.competitor_report_progress failed: %s", job_id, e)

        result = await run_competitor_report_pipeline(
            audit_id=audit_id,
            tier=tier,
            selection_mode=selection_mode,
            target_count=target_count,
            convex_user_id=user_id,
            on_progress=on_progress,
        )

        report_stats = {
            "swotItemCount": result.get("swot_item_count", 0),
            "recommendationCount": result.get("recommendation_count", 0),
            "usedFallback": result.get("used_fallback", False),
        }
        try:
            await convex.competitor_report_complete(
                audit_id=audit_id,
                user_id=user_id,
                tier=tier,
                report_id=result.get("report_id"),
                report_stats=report_stats,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[%s] convex.competitor_report_complete failed: %s", job_id, e)

        logger.info("[%s] run_competitor_report_task completed report_id=%s",
                    job_id, result.get("report_id"))
        return result

    except _CancelledByUser:
        logger.info("[%s] run_competitor_report_task cancelled by user", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        try:
            await convex.competitor_report_fail(audit_id, "Cancelled by user")
        except Exception:  # noqa: BLE001
            pass
        raise
    except Exception as e:
        logger.error("[%s] run_competitor_report_task failed: %s", job_id, e, exc_info=True)
        try:
            await convex.competitor_report_fail(audit_id, str(e))
        except Exception:  # noqa: BLE001
            pass
        raise


# ---------------------------------------------------------------------------
# Task #6: Comp Etap 7 — competitor refresh (premium tier)
# ---------------------------------------------------------------------------

async def run_competitor_refresh_task(ctx: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
    """Comp Etap 7 — premium competitor report refresh.

    No Convex webhooks — refresh is a background snapshot task driven by
    Convex cron. Logs only. Idempotency check (skip-if-already-running)
    happens at endpoint level before enqueue, not here.
    """
    from pipelines.competitor_report_refresh import run_refresh

    job_id = ctx.get("job_id", "?")
    report_id = request["reportId"]

    logger.info("[%s] run_competitor_refresh_task started report_id=%s", job_id, report_id)

    try:
        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)

        result = await run_refresh(report_id=report_id, on_progress=on_progress)
        logger.info(
            "[%s] run_competitor_refresh_task completed snapshot=%s changes=%d",
            job_id, result.get("snapshot_date"), result.get("significant_change_count", 0),
        )
        return result

    except _CancelledByUser:
        logger.info("[%s] run_competitor_refresh_task cancelled", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        raise
    except Exception as e:
        logger.error("[%s] run_competitor_refresh_task failed: %s", job_id, e, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Task #7: Versum mapping suggestions
# ---------------------------------------------------------------------------

async def run_versum_suggest_task(ctx: dict[str, Any], request: dict[str, Any]) -> list[dict[str, Any]]:
    """Versum POS service mapping suggestions.

    Returns a list of suggestion dicts (not saved anywhere — frontend polls
    via GET /api/versum/.../result which reads arq job result). No Convex
    webhooks. No cancel mechanism inside pipeline; cancel-check still happens
    via on_progress because we wrap suggestions in a wrapper progress reporter.
    """
    from pipelines.versum_suggest import suggest_versum_mappings

    job_id = ctx.get("job_id", "?")
    services = request.get("services", [])
    taxonomy = request.get("taxonomy", [])

    logger.info("[%s] run_versum_suggest_task started services=%d taxonomy=%d",
                job_id, len(services), len(taxonomy))

    try:
        async def on_progress(progress: int, message: str) -> None:
            if await _check_cancel(ctx):
                raise _CancelledByUser("cancel flag set")
            logger.info("[%s] %d%% %s", job_id, progress, message)

        result = await suggest_versum_mappings(
            services=services,
            taxonomy=taxonomy,
            on_progress=on_progress,
        )
        logger.info("[%s] run_versum_suggest_task completed suggestions=%d", job_id, len(result))
        return result

    except _CancelledByUser:
        logger.info("[%s] run_versum_suggest_task cancelled", job_id)
        await clear_cancel_flag(ctx["redis"], job_id)
        raise
    except Exception as e:
        logger.error("[%s] run_versum_suggest_task failed: %s", job_id, e, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Public task registry — imported by workers/main.py WorkerSettings.functions
# ---------------------------------------------------------------------------

ALL_TASKS = [
    run_report_task,
    run_free_report_task,
    run_cennik_task,
    run_summary_task,
    run_competitor_report_task,
    run_competitor_refresh_task,
    run_versum_suggest_task,
]
