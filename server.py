"""FastAPI app entry point for bagent — Beauty Audit AI Analyzer."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import uuid
from pathlib import Path

from arq.connections import ArqRedis
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

# Issue #24 — load .env into os.environ FIRST so observability.py can
# read BUGSINK_DSN_BAGENT before sentry_sdk.init runs. pydantic-settings
# only reads .env, it doesn't merge into os.environ, so we use dotenv.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # noqa: BLE001
    pass

from config import settings
from job_store import Job, JobStore
from observability import init_for_server as init_sentry
from workers import get_redis_pool, set_cancel_flag

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Initialize Bugsink/Sentry. No-op when BUGSINK_DSN_BAGENT is missing.
init_sentry()


# ---------------------------------------------------------------------------
# FastAPI lifespan: open one shared ArqRedis pool at startup, close on
# shutdown. Endpoints enqueue jobs via app.state.arq. The actual migration
# of background_tasks.add_task → app.state.arq.enqueue_job happens in
# issue #21 — for now the pool is just open and ready, no callers yet.
#
# If Redis is unreachable at startup we log a warning and continue —
# bagent must remain serviceable for non-queue endpoints (sync /api/ai/text,
# /api/embeddings, dashboard) even when Redis is down. The /api/health
# endpoint reports the actual state.
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    arq_pool: ArqRedis | None = None
    try:
        arq_pool = await get_redis_pool()
        await arq_pool.ping()
        logger.info("arq Redis pool connected at startup")
    except Exception as e:
        logger.warning("arq Redis pool failed to connect at startup: %s", e)
        arq_pool = None
    fastapi_app.state.arq = arq_pool
    try:
        yield
    finally:
        if arq_pool is not None:
            try:
                await arq_pool.close()
                logger.info("arq Redis pool closed")
            except Exception as e:
                logger.warning("arq Redis pool close raised: %s", e)


app = FastAPI(title="bagent — Beauty Audit AI Analyzer", lifespan=lifespan)

# CORS — narrowly scoped to dev modal endpoints (/api/dev/*) so frontend
# running on localhost:3000/3010 can call PipelineTraceModal endpoints
# without hitting browser preflight failures. Production frontend
# (booksyaudit.pl) calls the same endpoints from same-origin via Convex
# action layer, so this middleware only affects browser-direct calls.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3010",
        "https://localhost:3000",
        "https://localhost:3010",
        "https://booksyaudit.pl",
        "https://www.booksyaudit.pl",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
    max_age=600,
)

store = JobStore()

TEMPLATES_DIR = Path(__file__).parent / "templates"
_dashboard_html: str | None = None


def _get_dashboard_html() -> str:
    global _dashboard_html
    if _dashboard_html is None:
        _dashboard_html = (TEMPLATES_DIR / "dashboard.html").read_text(encoding="utf-8")
    return _dashboard_html


# --- Request/Response models ---


# Legacy AnalyzeRequest, CompetitorRequest, OptimizationRequest, KeywordRequest
# removed in 3-BAGENT migration. Use ReportRequest/CennikRequest/SummaryRequest
# below which hit /api/audit/report, /api/audit/cennik, /api/audit/summary.


class AiTextRequest(BaseModel):
    """Generic AI text generation — send prompt, get text back."""
    prompt: str
    maxTokens: int = 4000
    temperature: float = 0.4


class AnalyzeResponse(BaseModel):
    jobId: str
    status: str = "accepted"


# 3-bagent migration request models
class ReportRequest(BaseModel):
    """BAGENT #1 — report + content optimization.

    Loads scraped data from Supabase audit_scraped_data by auditId, so the
    HTTP payload stays small. For dev/testing, scrapedData can be inlined.
    """
    auditId: str
    userId: str
    sourceUrl: str | None = None
    scrapedData: dict | None = None  # optional inline payload for dev testing


class FreeReportRequest(ReportRequest):
    """Frozen free-tier snapshot of ReportRequest — same shape, distinct
    class so OpenAPI/logs clearly attribute calls to /api/audit/free_report.

    See pipelines/free_report.py for the frozen pipeline and
    docs/free_report.md for the stability contract.
    """
    pass


class CennikRequest(BaseModel):
    """BAGENT #2 — generate new pricelist from report + original scrape."""
    auditId: str
    userId: str


class SummaryRequest(BaseModel):
    """BAGENT #3 — audit summary with basic competitor preview."""
    auditId: str
    userId: str
    selectedCompetitorIds: list[int] = []


class CompetitorRefreshRequest(BaseModel):
    """Comp Etap 7 — refresh an existing premium competitor report.

    Triggered by the Convex cron `refresh-premium-competitor-reports`
    every 6 hours for each report where tier='premium' and
    next_refresh_at has passed. The pipeline re-reads the latest
    `salon_scrapes` for each competitor, computes a new key_metrics
    snapshot, appends a row to `competitor_report_snapshots`, detects
    price changes vs the previous snapshot, and updates
    `competitor_reports.next_refresh_at` based on refresh_schedule.
    """
    reportId: int
    auditId: str
    userId: str


class CompetitorReportRequest(BaseModel):
    """Comp Etap 2 — generate a new competitor report for an audit.

    Runs the full competitor report pipeline: selection (Etap 1) +
    deterministic analysis (Etap 4) + AI synthesis (Etap 5) + persistence.
    """
    auditId: str
    userId: str
    tier: str = "base"  # "base" | "premium"
    selectionMode: str = "auto"  # "auto" | "manual"
    # 2026-05-17 — bumped from 5 to 15. profile_overlap_sim OR-gate added
    # 15 more salons to the qualifying pool; 5 was too aggressive a cap
    # given Faza 8a verification trims another ~40-60% post-selection.
    targetCount: int = 15
    selectedCompetitorBooksyIds: list[int] | None = None  # for manual mode
    # 2026-05-29 — user picks from the frontend competitor picker. These are
    # Supabase salons.id (internal salon_id PK), NOT booksy_id — same id-space
    # as SummaryRequest.selectedCompetitorIds. Forwarded as a UNION into the
    # deterministic selection (must-include, never a filter) so per-competitor
    # detail can be surfaced for these while analytics stay computed from the
    # full deterministic sample.
    selectedCompetitorIds: list[int] | None = None  # salon_id space


class VersumSuggestRequest(BaseModel):
    """AI-powered Versum service mapping suggestions.

    Takes salon services (name + description + category) and the Booksy
    treatment taxonomy, returns suggested treatmentId matches with confidence.
    Async endpoint — returns 202 + jobId, poll for results via
    GET /api/versum/suggest-mappings/{jobId}/result.
    """
    services: list[dict]  # [{name, description?, categoryName?}]
    taxonomy: list[dict]  # [{treatmentId, treatmentName, parentCategoryName?, occurrenceCount}]


# --- Auth ---


def verify_api_key(x_api_key: str = Header(...)) -> None:
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


# --- Endpoints ---


# Legacy /api/analyze endpoint removed in 3-BAGENT migration.
# Convex now posts directly to /api/audit/report → /api/audit/cennik → /api/audit/summary.


# /api/competitor endpoint REMOVED in 3-bagent migration.
# The full SWOT competitor pipeline moved to bagent/pipelines/_premium/competitor.py
# and will be reactivated as a paid Premium Competitor Analysis product in a
# future sprint. See bagent/pipelines/_premium/README.md for reactivation steps.
# Basic competitor preview (top 3-5 salons, no SWOT) is now produced by BAGENT #3
# (pipelines/summary.py) as part of the Podsumowanie tab.


# Legacy /api/optimize + 4-phase endpoints removed in 3-BAGENT migration.
# Use /api/audit/report → /api/audit/cennik → /api/audit/summary pipeline.


# --- arq enqueue helper (issue #21) -----------------------------------------
# Every pipeline endpoint follows the same pattern:
#   1. Generate job_id, create JobStore entry (for /api/jobs listing only —
#      worker doesn't update the JobStore live, see workers/tasks.py docstring)
#   2. Enqueue task in arq with the same job_id (so JobStore + arq stay aligned)
#   3. Return 202 with jobId
#
# If the arq pool failed to connect at startup (state.arq is None), every
# enqueue endpoint must fail HARD with 503 — the request must NOT be silently
# accepted and lost. This is the explicit follow-up from python-reviewer's
# code review on #20.

async def _enqueue_pipeline(
    *,
    task_name: str,
    job_id: str,
    request_payload: dict,
) -> None:
    """Enqueue a pipeline task in arq with our chosen job_id.

    Raises HTTPException(503) if the pool is unavailable OR if the actual
    enqueue raises a connection error (Redis went down post-startup).
    Logs warning if enqueue returns None (job_id collision — shouldn't
    happen with UUID4).

    Two failure modes for 503:
    1. `pool is None` — startup couldn't connect, lifespan kept us serving
       non-queue endpoints but enqueue can't proceed.
    2. `pool.enqueue_job(...)` raises — Redis died after startup. Pool object
       still exists but underlying connection is broken. We don't try to
       reconnect here; the next pm2 restart of bagent-booksyauditor will
       fix it (or operator restarts redis-bagent and pm2 picks up).
    """
    pool = getattr(app.state, "arq", None)
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="job queue unavailable (Redis disconnected at startup) — retry shortly",
        )

    try:
        arq_job = await pool.enqueue_job(
            task_name,
            request_payload,
            _job_id=job_id,
        )
    except Exception as e:  # noqa: BLE001
        # Redis connectivity lost mid-flight, pool unhealthy, etc.
        logger.error("arq enqueue_job failed for %s job_id=%s: %s", task_name, job_id, e)
        raise HTTPException(
            status_code=503,
            detail=f"job queue unavailable ({type(e).__name__}) — retry shortly",
        ) from e

    if arq_job is None:
        # arq returns None when a job with the same _job_id is already in
        # the queue. With UUID4 this is effectively impossible, but log
        # defensively so we'd notice a regression.
        logger.warning("arq enqueue_job returned None for %s job_id=%s — duplicate?", task_name, job_id)
        raise HTTPException(status_code=409, detail="duplicate job_id")


# --- 3-bagent migration endpoints ---


@app.post(
    "/api/audit/report", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_report(request: ReportRequest) -> AnalyzeResponse:
    """BAGENT #1 — report + content optimization.

    Enqueues an arq task and returns 202 immediately. Worker runs the
    pipeline; progress flows to Convex via /api/audit/report/progress
    webhooks; completion via .../complete; failure via .../fail.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "report",
        "userId": request.userId,
        "sourceUrl": request.sourceUrl,
    })
    await _enqueue_pipeline(
        task_name="run_report_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/audit/free_report", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_free_report(request: FreeReportRequest) -> AnalyzeResponse:
    """Frozen free-tier snapshot of BAGENT #1 — report + content optimization.

    Enqueues run_free_report_task which runs pipelines/free_report.py
    (byte-compatible mirror of pipelines/report.py at commit 47220b7).
    Reuses /api/audit/report/* Convex webhooks. See docs/free_report.md.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "free_report",
        "userId": request.userId,
        "sourceUrl": request.sourceUrl,
    })
    await _enqueue_pipeline(
        task_name="run_free_report_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/audit/cennik", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_cennik(request: CennikRequest) -> AnalyzeResponse:
    """BAGENT #2 — new pricelist generation.

    Triggered by Convex after BAGENT #1 finishes. Loads report and scraped
    data from Supabase, applies transformations, restructures categories,
    writes optimized_pricelists + categoryProposals payload.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "cennik",
        "userId": request.userId,
    })
    await _enqueue_pipeline(
        task_name="run_cennik_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/audit/summary", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_summary(request: SummaryRequest) -> AnalyzeResponse:
    """BAGENT #3 — audit summary with basic competitor preview.

    Triggered by Convex after user confirms competitor selection. Aggregates
    all prior audit data plus competitor info and writes audit_summaries.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "summary",
        "userId": request.userId,
        "competitorCount": len(request.selectedCompetitorIds),
    })
    await _enqueue_pipeline(
        task_name="run_summary_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/competitor/report", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_competitor_report(request: CompetitorReportRequest) -> AnalyzeResponse:
    """Comp Etap 2 — generate a full competitor report for an audit.

    Runs selection (Etap 1) + deterministic analysis (Etap 4) + AI synthesis
    (Etap 5). Webhooks to Convex for progress / complete / fail.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "competitor_report",
        "userId": request.userId,
        "tier": request.tier,
        "selectionMode": request.selectionMode,
        "targetCount": request.targetCount,
    })
    await _enqueue_pipeline(
        task_name="run_competitor_report_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/competitor/report/refresh", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_competitor_refresh(request: CompetitorRefreshRequest) -> AnalyzeResponse:
    """Comp Etap 7 — refresh an existing premium competitor report.

    Idempotent: if a refresh job is already running for the same reportId
    in our local JobStore (recorded at enqueue time), this request returns
    that job's id instead of enqueuing a duplicate. Worker would dedupe
    too if a duplicate slipped through (arq returns None on _job_id reuse),
    but the local check avoids round-tripping to Redis for hot reportIds.
    """
    for job in store.list_jobs():
        if (
            job.status in ("queued", "running")
            and isinstance(job.meta, dict)
            and job.meta.get("type") == "competitor_refresh"
            and job.meta.get("reportId") == request.reportId
        ):
            logger.info(
                "Skipping competitor refresh for report %s — already in flight as job %s",
                request.reportId, job.job_id,
            )
            return AnalyzeResponse(jobId=job.job_id, status="already_running")

    job_id = str(uuid.uuid4())

    # FINDINGS P1-1: lock idempotencji w Redisie (SET NX). JobStore jest
    # in-memory i per-proces — check wyżej ma race między check a enqueue
    # i gubi stan po restarcie FastAPI. TTL 15 min: dłużej niż realny
    # refresh, krócej niż odstęp między cronowymi refreshami. Wartość locka
    # = job_id, więc duplikat dostaje id joba będącego w locie. Awaria
    # Redisa degraduje do dotychczasowego zachowania (sam JobStore check).
    pool = getattr(app.state, "arq", None)
    if pool is not None:
        lock_key = f"bagent:refresh-lock:{request.reportId}"
        try:
            acquired = await pool.set(lock_key, job_id, nx=True, ex=900)
            if not acquired:
                holder = await pool.get(lock_key)
                holder_id = (
                    holder.decode() if isinstance(holder, bytes) else str(holder or "")
                )
                logger.info(
                    "Skipping competitor refresh for report %s — Redis lock held by job %s",
                    request.reportId, holder_id or "unknown",
                )
                return AnalyzeResponse(
                    jobId=holder_id or "unknown", status="already_running"
                )
        except Exception as e:
            logger.warning(
                "Redis idempotency lock failed (%s) — JobStore check only", e
            )

    store.create_job(job_id, request.auditId, meta={
        "type": "competitor_refresh",
        "reportId": request.reportId,
        "userId": request.userId,
    })
    try:
        await _enqueue_pipeline(
            task_name="run_competitor_refresh_task",
            job_id=job_id,
            request_payload=request.model_dump(),
        )
    except Exception:
        # Zwolnij lock przy nieudanym enqueue (np. 503), żeby retry klienta
        # nie czekał na TTL. Best-effort — TTL i tak posprząta.
        if pool is not None:
            try:
                await pool.delete(f"bagent:refresh-lock:{request.reportId}")
            except Exception:
                pass
        raise
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/versum/suggest-mappings",
    status_code=202,
    response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def suggest_versum_mappings(request: VersumSuggestRequest) -> AnalyzeResponse:
    """AI-powered Versum service mapping suggestions via MiniMax.

    Async endpoint — processes services in batches of 25 in a worker task.
    Poll /api/versum/suggest-mappings/{job_id}/result for progress and
    results when complete.
    """
    if not request.services:
        # Empty input: create a completed job immediately with empty result.
        # No worker enqueue needed — JobStore alone serves this case via
        # the /result endpoint reading job.result_data.
        job_id = str(uuid.uuid4())
        job = store.create_job(job_id, f"versum-0-services", meta={"type": "versum_suggest"})
        job.result_data = {"suggestions": [], "serviceCount": 0, "suggestionCount": 0}
        job.mark_running()
        job.mark_completed()
        store.notify_status(job)
        return AnalyzeResponse(jobId=job_id)

    job_id = str(uuid.uuid4())
    store.create_job(job_id, f"versum-{len(request.services)}-services", meta={
        "type": "versum_suggest",
        "serviceCount": len(request.services),
    })
    await _enqueue_pipeline(
        task_name="run_versum_suggest_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
    return AnalyzeResponse(jobId=job_id)


async def _run_versum_suggest_job(job_id: str, request: VersumSuggestRequest) -> None:
    """Background task for versum suggest-mappings."""
    job = store.get_job(job_id)
    if not job:
        return
    job.mark_running()
    store.notify_status(job)

    try:
        from pipelines.versum_suggest import suggest_versum_mappings as _suggest

        suggestions = await _suggest(
            request.services,
            request.taxonomy,
            on_progress=lambda pct, msg: _update_versum_progress(job, pct, msg),
        )
        job.result_data = {
            "suggestions": suggestions,
            "serviceCount": len(request.services),
            "suggestionCount": len(suggestions),
        }
        job.mark_completed()
    except Exception as e:
        logger.error("Versum suggest-mappings job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(f"AI suggestion failed: {str(e)[:200]}")
    store.notify_status(job)


def _update_versum_progress(job: Job, pct: int, msg: str) -> None:
    """Helper to push progress updates for a versum suggest job."""
    job.add_log("info", msg, progress=pct)
    store.notify_progress(job)


@app.get(
    "/api/versum/suggest-mappings/{job_id}/result",
    dependencies=[Depends(verify_api_key)],
)
async def get_versum_suggest_result(job_id: str) -> dict:
    """Poll for versum suggest-mappings job status and results."""
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status == "running" or job.status == "queued":
        return {
            "status": job.status,
            "progress": job.progress,
            "progressMessage": job.progress_message,
        }
    if job.status == "completed":
        return {
            "status": "completed",
            "data": job.result_data,
        }
    # failed / cancelled
    return {
        "status": job.status,
        "error": job.error,
    }


@app.get("/api/jobs")
async def list_jobs() -> list[dict]:
    return [j.to_summary() for j in store.list_jobs()]


@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_summary()


@app.get("/api/jobs/{job_id}/logs")
async def get_job_logs(job_id: str) -> dict:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str) -> dict:
    """Request graceful cancellation of a running pipeline job.

    Cancel mechanism (after #21 arq migration):
    1. Set Redis key `bagent:cancel:{job_id}` with 1h TTL — this is the
       authoritative signal across worker processes.
    2. Also call job.request_cancel() in local JobStore — preserves /api/jobs
       list semantics for the dashboard.

    The arq worker task polls the Redis key in its on_progress callback
    and raises CancelledError on next progress event. There is no
    immediate kill — workers finish whatever they're doing and check at
    the next checkpoint (typically 1-5 seconds for active pipelines).
    """
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("running", "queued"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel job with status '{job.status}'",
        )

    # Set Redis cancel flag — workers across processes check this.
    pool = getattr(app.state, "arq", None)
    if pool is not None:
        try:
            await set_cancel_flag(pool, job_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to set Redis cancel flag for %s: %s", job_id, e)
            # Don't fail the request — local JobStore cancel still applies
            # and the job may complete or fail naturally before next progress.

    # Local JobStore cancellation (legacy path — still useful for SSE
    # dashboard which subscribes to JobStore events).
    job.request_cancel()
    store.notify_progress(job)
    return {"jobId": job_id, "status": "cancel_requested"}


@app.get("/api/events")
async def sse_events() -> EventSourceResponse:
    queue = store.subscribe()

    async def event_generator():
        try:
            while True:
                event = await queue.get()
                yield {"event": event.get("type", "update"), "data": json.dumps(event)}
        except asyncio.CancelledError:
            pass
        finally:
            store.unsubscribe(queue)

    return EventSourceResponse(event_generator())


@app.get("/api/health")
async def health() -> dict:
    running = sum(1 for j in store.list_jobs() if j.status == "running")
    total = len(store.list_jobs())

    # Redis health: ping the arq pool stored in lifespan. Three states:
    # - "ok"        → pool exists, ping returned successfully
    # - "down"      → pool exists but ping raised (transient or permanent)
    # - "disabled"  → no pool attached (startup failed earlier)
    arq_pool: ArqRedis | None = getattr(app.state, "arq", None)
    if arq_pool is None:
        redis_status = "disabled"
    else:
        try:
            await arq_pool.ping()
            redis_status = "ok"
        except Exception:
            redis_status = "down"

    overall = "ok" if redis_status in ("ok", "disabled") else "degraded"
    return {
        "status": overall,
        "jobs_running": running,
        "jobs_total": total,
        "redis": redis_status,
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(_get_dashboard_html())


# --- Staff data endpoints (read-only, RPC wrappers from migration 053) ---
# Thin wrappers around fn_salon_team_summary / fn_competitor_recent_changes /
# fn_staff_migrations_for_salon. Used by:
#   - Audit "Twój zespół" section (via Convex action, but bagent endpoint
#     gives a programmatic surface for cron jobs and external tools)
#   - Pulse alert cron (when launched): poll each subscribed competitor's
#     recent-changes + migrations-for-salon, write to competitorMonitoringAlerts
#   - Future n8n / external integrations
# All gated by API key. Data itself is from publicly visible Booksy profiles,
# but the aggregated/scored output is a product output we don't want public.


@app.get("/api/staff/team-summary/{booksy_id}", dependencies=[Depends(verify_api_key)])
async def staff_team_summary(booksy_id: int) -> dict:
    """Single-row team summary for one salon (current state + 30d events +
    city rotation benchmarks). Returns 404 if the salon has no chain head
    in salon_scrapes."""
    from services.sb_client import make_supabase_client

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    res = sb.rpc("fn_salon_team_summary", {"p_booksy_id": booksy_id}).execute()
    rows = res.data or []
    if not rows:
        raise HTTPException(status_code=404, detail="No team summary for booksy_id")
    return {"booksy_id": booksy_id, "summary": rows[0]}


@app.get(
    "/api/competitor/profile/{salon_id}",
    dependencies=[Depends(verify_api_key)],
)
async def competitor_profile(salon_id: int) -> dict:
    """Full profile snapshot for ONE competitor salon — header photos
    (cover/logo/thumbnail), salon meta, and the COMPLETE active services
    pricelist. Drives the modal that opens from the competitor table row.

    `salon_id` is the internal `salons.id` (= `salon_scrapes.salon_ref_id`)
    which is what the rich-report data emits as `competitor.id`.
    """
    from services.sb_client import make_supabase_client

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    # Latest chain-head scrape for this salon. Chain head = canonical
    # current-state row; delta rows are skipped (would mix in old data).
    scrape_res = (
        sb.table("salon_scrapes")
        .select(
            "id, booksy_id, salon_name, salon_city, salon_address, "
            "salon_lat, salon_lng, salon_logo_url, cover_photo_url, "
            "thumbnail_photo_url, reviews_rank, reviews_count, "
            "business_categories, booksy_url, total_services, "
            "active_services_count, scraped_at"
        )
        .eq("salon_ref_id", salon_id)
        .eq("is_chain_head", True)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
    )
    if not scrape_res.data:
        raise HTTPException(
            status_code=404,
            detail=f"No chain-head scrape for salon_ref_id={salon_id}",
        )
    s = scrape_res.data[0]

    # Full active pricelist for this scrape. is_active = TRUE filters out
    # legacy duplicate rows (pre-dedup-trigger residue) and soft-deleted
    # services. Order by category then name for predictable group rendering.
    services_res = (
        sb.table("salon_scrape_services")
        .select(
            "name, category, price_grosze, duration_minutes, "
            "booksy_service_id, currency"
        )
        .eq("scrape_id", s["id"])
        .eq("is_active", True)
        .order("category", desc=False)
        .order("name", desc=False)
        .limit(500)
        .execute()
    )
    services = services_res.data or []

    return {
        "salon": {
            "id": salon_id,
            "booksyId": s.get("booksy_id"),
            "name": s.get("salon_name"),
            "city": s.get("salon_city"),
            "address": s.get("salon_address"),
            "lat": s.get("salon_lat"),
            "lng": s.get("salon_lng"),
            "coverPhotoUrl": s.get("cover_photo_url"),
            "logoPhotoUrl": s.get("salon_logo_url"),
            "thumbnailPhotoUrl": s.get("thumbnail_photo_url"),
            "reviewsRank": s.get("reviews_rank"),
            "reviewsCount": s.get("reviews_count"),
            "businessCategories": s.get("business_categories") or [],
            "booksyUrl": s.get("booksy_url"),
            "totalServices": s.get("total_services"),
            "activeServicesCount": s.get("active_services_count"),
            "scrapedAt": s.get("scraped_at"),
        },
        "services": [
            {
                "name": svc.get("name"),
                "category": svc.get("category"),
                "priceGrosze": svc.get("price_grosze"),
                "durationMinutes": svc.get("duration_minutes"),
                "booksyServiceId": svc.get("booksy_service_id"),
                "currency": svc.get("currency") or "PLN",
            }
            for svc in services
        ],
        "serviceCount": len(services),
    }


@app.get(
    "/api/competitor/methods_for_audit/{audit_id}",
    dependencies=[Depends(verify_api_key)],
)
async def competitor_methods_for_audit(audit_id: str) -> dict:
    """Returns methods present in subject's classified pricelist for
    UI dropdown in method-targeted competitor search mode (mig 097
    fn_subject_methods). Each entry: canonical method + display name +
    category + services_count (how many of subject's services map to
    this method) + sample service names for context.

    FAIL-LOUD: missing audit / missing subject raises HTTPException."""
    from services.sb_client import make_supabase_client

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    # Resolve subject salon
    rep = sb.table("competitor_reports") \
        .select("subject_salon_id") \
        .eq("convex_audit_id", audit_id).limit(1).execute()
    if not rep.data:
        raise HTTPException(status_code=404, detail=f"No competitor_report for audit_id={audit_id}")
    subject_salon_id = int(rep.data[0]["subject_salon_id"])

    methods_res = sb.rpc(
        "fn_subject_methods", {"p_subject_salon_id": subject_salon_id},
    ).execute()
    methods = methods_res.data or []

    return {
        "audit_id": audit_id,
        "subject_salon_id": subject_salon_id,
        "methods_count": len(methods),
        "methods": methods,
    }


class MethodTargetedRequest(BaseModel):
    """Args for method-targeted competitor search endpoint."""
    auditId: str
    methodIds: list[int]
    radiusKm: float = 16.0
    limit: int = 30


@app.post(
    "/api/competitor/salons_offering_methods",
    dependencies=[Depends(verify_api_key)],
)
async def competitor_salons_offering_methods(req: MethodTargetedRequest) -> dict:
    """Lists salons within radius offering specified canonical methods
    (mig 097 fn_find_salons_offering_method). Powers the map + table
    in method-targeted UI tab — user picks "Red Touch" + "Thunder" from
    dropdown, this returns all salons in 16km offering either, sorted
    by methods matched DESC then services count DESC then distance.

    FAIL-LOUD: bad audit_id raises 404, RPC errors propagate as 500."""
    from services.sb_client import make_supabase_client

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    rep = sb.table("competitor_reports") \
        .select("subject_salon_id") \
        .eq("convex_audit_id", req.auditId).limit(1).execute()
    if not rep.data:
        raise HTTPException(status_code=404, detail=f"No competitor_report for audit_id={req.auditId}")
    subject_salon_id = int(rep.data[0]["subject_salon_id"])

    salons_res = sb.rpc(
        "fn_find_salons_offering_method",
        {
            "p_subject_salon_id": subject_salon_id,
            "p_method_ids": req.methodIds,
            "p_radius_km": req.radiusKm,
            "p_limit": req.limit,
        },
    ).execute()
    salons = salons_res.data or []

    return {
        "audit_id": req.auditId,
        "subject_salon_id": subject_salon_id,
        "method_ids": req.methodIds,
        "radius_km": req.radiusKm,
        "salons_count": len(salons),
        "salons": salons,
    }


class MethodPricingRequest(BaseModel):
    """Args for method-level market pricing endpoint."""
    auditId: str
    methodId: int
    radiusKm: float = 16.0
    durationMin: int | None = None
    durationMax: int | None = None


@app.post(
    "/api/competitor/method_pricing",
    dependencies=[Depends(verify_api_key)],
)
async def competitor_method_pricing(req: MethodPricingRequest) -> dict:
    """Computes market price distribution for ONE canonical method
    across all salons within radius (mig 097 fn_compute_method_pricing).
    Used by UI when user clicks a method in their cennik — get the
    market percentile/median + a sample of competing services.

    FAIL-LOUD: bad audit_id raises 404."""
    from services.sb_client import make_supabase_client

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    rep = sb.table("competitor_reports") \
        .select("subject_salon_id") \
        .eq("convex_audit_id", req.auditId).limit(1).execute()
    if not rep.data:
        raise HTTPException(status_code=404, detail=f"No competitor_report for audit_id={req.auditId}")
    subject_salon_id = int(rep.data[0]["subject_salon_id"])

    pricing_res = sb.rpc(
        "fn_compute_method_pricing",
        {
            "p_subject_salon_id": subject_salon_id,
            "p_method_id": req.methodId,
            "p_radius_km": req.radiusKm,
            "p_duration_min": req.durationMin,
            "p_duration_max": req.durationMax,
            "p_sample_limit": 30,
        },
    ).execute()
    pricing = pricing_res.data or []
    return {
        "audit_id": req.auditId,
        "method_id": req.methodId,
        "radius_km": req.radiusKm,
        "pricing": pricing[0] if pricing else None,
    }


@app.get(
    "/api/staff/competitor-changes/{booksy_id}",
    dependencies=[Depends(verify_api_key)],
)
async def staff_competitor_changes(booksy_id: int, hours: int = 24) -> dict:
    """Recent staff events at ONE salon over the last `hours`. Drives the
    "konkurent stracił N osób w 24h" Pulse alert. hours clamped to [1, 720]."""
    from services.sb_client import make_supabase_client

    hours = max(1, min(720, hours))
    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    res = sb.rpc(
        "fn_competitor_recent_changes",
        {"p_booksy_id": booksy_id, "p_hours": hours},
    ).execute()
    rows = res.data or []
    counts = {
        "joined": sum(1 for r in rows if r.get("event") == "joined"),
        "left": sum(1 for r in rows if r.get("event") == "left"),
        "position_changed": sum(1 for r in rows if r.get("event") == "position_changed"),
        "renamed": sum(1 for r in rows if r.get("event") == "renamed"),
        "score_jump_up": sum(
            1 for r in rows
            if r.get("event") == "score_jump" and (r.get("score_delta") or 0) > 0
        ),
        "score_jump_down": sum(
            1 for r in rows
            if r.get("event") == "score_jump" and (r.get("score_delta") or 0) < 0
        ),
    }
    return {
        "booksy_id": booksy_id,
        "hours": hours,
        "total_events": len(rows),
        "counts": counts,
        "events": rows,
    }


@app.get("/api/staff/migrations/{booksy_id}", dependencies=[Depends(verify_api_key)])
async def staff_migrations(
    booksy_id: int,
    days: int = 30,
    direction: str = "both",
    min_confidence: float = 0.5,
) -> dict:
    """Cross-salon staff migrations involving this booksy_id. direction:
    'from' (this salon LOST staff), 'to' (this salon GAINED), 'both'."""
    from services.sb_client import make_supabase_client

    if direction not in {"from", "to", "both"}:
        raise HTTPException(status_code=400, detail="direction must be from|to|both")
    days = max(1, min(365, days))
    min_confidence = max(0.0, min(1.0, min_confidence))

    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )
    res = sb.rpc(
        "fn_staff_migrations_for_salon",
        {
            "p_booksy_id": booksy_id,
            "p_days": days,
            "p_direction": direction,
            "p_min_confidence": min_confidence,
        },
    ).execute()
    rows = res.data or []
    return {
        "booksy_id": booksy_id,
        "days": days,
        "direction": direction,
        "min_confidence": min_confidence,
        "total": len(rows),
        "migrations": rows,
    }


# --- Dev/debug endpoints (taxonomy routing trace, synthetic catalog) ---
# Disabled in production unless BAGENT_DEV_ENDPOINTS env flag is set —
# bagent.booksyaudit.pl is a public PM2 surface so we hard-gate the route
# bodies. No auth header is required so local debugging is friction-free
# (set BAGENT_DEV_ENDPOINTS=1 in dev shell only).


class TraceTaxonomyRequest(BaseModel):
    """Either pass `services` inline (list of salon_scrape_services-shaped
    dicts) or `audit_id` to load subject services from Supabase.
    `dry_run` (default True) skips writes to synthetic_treatment_categories
    so debugging doesn't pollute the catalog.
    """
    services: list[dict] | None = None
    audit_id: str | None = None
    dry_run: bool = True
    verbose: bool = True


def _ensure_dev_endpoints_enabled() -> None:
    import os
    if not os.environ.get("BAGENT_DEV_ENDPOINTS"):
        raise HTTPException(
            status_code=404,
            detail=(
                "Dev endpoints disabled. Set BAGENT_DEV_ENDPOINTS=1 in env "
                "to enable."
            ),
        )


@app.post("/api/dev/trace-taxonomy")
async def dev_trace_taxonomy(request: TraceTaxonomyRequest) -> dict:
    """Run `_resolve_service_taxonomy` over a list of services and return a
    structured trace of every routing decision. Used by the rich frontend's
    debug modal to inspect why a given service ended up where it did.

    Returns:
      {
        "stats": {rule_2, rule_3, rule_4, rule_1, skipped, rule_3_with_variant},
        "trace": [...],  # one entry per candidate, see _resolve_service_taxonomy
        "services_input": int,
        "candidates": int,
        "dry_run": bool,
      }
    """
    _ensure_dev_endpoints_enabled()

    from pipelines.competitor_analysis import _resolve_service_taxonomy
    from services.supabase import SupabaseService

    if request.services is None and request.audit_id is None:
        raise HTTPException(
            status_code=400,
            detail="Provide either `services` (inline list) or `audit_id`.",
        )

    sb = SupabaseService()
    services_list: list[dict]
    label = "dev-trace"

    if request.services is not None:
        services_list = list(request.services)
    else:
        # Load subject services from Supabase. We grab scrape + services
        # via the existing helper that the pipeline uses.
        subject = await sb.get_subject_full_data(request.audit_id)
        services_list = list(subject.get("services") or [])
        label = f"audit={request.audit_id}"

    services_input = len(services_list)
    trace: list[dict] = []
    stats = await _resolve_service_taxonomy(
        supabase=sb,
        services=services_list,
        label=label,
        audit_id=request.audit_id,
        trace_collector=trace,
        dry_run=request.dry_run,
    )

    # Stage-5 Pass 5 (2026-05-17): MiniMax consistency layer. Forces
    # services in the same (brand, area) cluster onto ONE tid_key. The
    # dev modal needs to see Pass 5 emissions to verify the consistency
    # fix is actually firing. We run this only for SUBJECT services
    # here (dev endpoint doesn't load competitors); full cross-salon
    # consistency happens in `_etap_4_competitor_analysis` during real
    # audit runs.
    from services.taxonomy_consistency import apply_intra_salon_consistency
    from services.minimax import MiniMaxClient
    from config import settings as _settings
    if not _settings.minimax_api_key:
        raise HTTPException(
            status_code=500,
            detail=(
                "Pass 5 requires MINIMAX_API_KEY — not configured. Set "
                "it before calling /api/dev/trace-taxonomy."
            ),
        )
    minimax_for_consistency = MiniMaxClient(
        _settings.minimax_api_key,
        _settings.minimax_base_url,
        _settings.minimax_model,
    )
    consistency_stats = await apply_intra_salon_consistency(
        services_list,
        supabase=sb,
        minimax=minimax_for_consistency,
        audit_id=request.audit_id,
        label=f"dev-trace {label}",
        trace_collector=trace,
        dry_run=request.dry_run,
    )

    # Count candidates the way _resolve_service_taxonomy does (NULL tid +
    # active + non-empty name) so the UI can show "n of N services routed".
    candidates = sum(
        1 for s in services_list
        if s.get("is_active", True)
        and s.get("booksy_treatment_id") is None
        and (s.get("name") or "").strip()
        and len((s.get("name") or "").strip()) >= 3
        and s.get("id") is not None
    )

    # Normalize internal trace_collector emissions into the public
    # ServiceTrace[] contract the modal was coded against. Internal
    # `trace[]` is a flat list of (svc, rule_outcome) dicts; the public
    # shape is one ServiceTrace per service with a `steps[]` array of
    # rule attempts plus a derived `success` flag.
    SUCCESS_DECISIONS = {"matched", "kept", "inherited", "generated"}
    by_svc: dict[Any, dict[str, Any]] = {}
    order: list[Any] = []
    for entry_idx, entry in enumerate(trace):
        svc_id = entry.get("svc_id")
        if svc_id is None:
            # Internal _resolve_service_taxonomy.trace_collector MUST emit
            # svc_id on every record. Missing svc_id is an emitter bug; do
            # not fabricate a key — surface it so Bugsink catches it.
            raise RuntimeError(
                f"trace_collector emitted entry without svc_id at index "
                f"{entry_idx}: keys={sorted(entry.keys())!r}"
            )
        key = svc_id
        if key not in by_svc:
            order.append(key)
            by_svc[key] = {
                "svc_id": svc_id,
                "svc_name": entry.get("svc_name"),
                "original_tid": entry.get("original_tid"),
                "original_category": entry.get("original_category"),
                "steps": [],
                "success": False,
                "error": entry.get("error"),
                "final": None,
            }
        st = by_svc[key]
        raw_rule = entry.get("rule")
        if raw_rule is None:
            raise RuntimeError(
                f"trace_collector entry svc_id={svc_id} missing required "
                f"`rule` field: keys={sorted(entry.keys())!r}"
            )
        # _resolve_service_taxonomy emits rule as "0" (anchor replay,
        # Stage-5 commit 2), "2"/"3"/"4"/"1" (4-rule), or "5" (Pass 5
        # MiniMax consistency). Anything else is an emitter bug; let
        # int() raise and Bugsink capture.
        rule_num = int(raw_rule)
        if rule_num not in (0, 1, 2, 3, 4, 5):
            raise RuntimeError(
                f"trace_collector entry svc_id={svc_id} emitted invalid "
                f"rule value {raw_rule!r} (expected 0/1/2/3/4/5)"
            )
        step = {
            "rule": rule_num,
            "decision": entry.get("decision"),
            "details": entry.get("details"),
            "embedding_top_k": entry.get("embedding_top_k"),
            "llm_response": entry.get("llm_response"),
            "final": entry.get("final"),
        }
        st["steps"].append(step)
        if entry.get("decision") in SUCCESS_DECISIONS:
            st["success"] = True
        if entry.get("final") is not None:
            st["final"] = entry.get("final")
        if entry.get("error"):
            st["error"] = entry.get("error")
    traces_normalized = [by_svc[k] for k in order]

    return {
        "stats": stats,
        "consistency": consistency_stats,
        "traces": traces_normalized,
        "services_input": services_input,
        "candidates": candidates,
        "dry_run": request.dry_run,
        "audit_id": request.audit_id,
    }


@app.get("/api/dev/synthetic-categories")
async def dev_list_synthetic_categories(limit: int = 500) -> dict:
    """Read-only listing of `synthetic_treatment_categories` for the dev
    modal. Returns latest `limit` rows (DESC by created_at).
    """
    _ensure_dev_endpoints_enabled()

    from services.supabase import SupabaseService

    sb = SupabaseService()
    rows = await sb.list_synthetic_categories(limit=max(1, min(2000, limit)))
    return {
        "total": len(rows),
        "limit": limit,
        "rows": rows,
    }


# --- Synchronous AI endpoints (no background job, direct response) ---


# Legacy /api/keywords endpoint removed in 3-BAGENT migration.
# SEO keywords come from BAGENT #1 (audit_reports.missingSeoKeywords) now.


@app.post("/api/ai/text", dependencies=[Depends(verify_api_key)])
async def generate_ai_text(request: AiTextRequest) -> dict:
    """Generic AI text generation. Sends prompt to MiniMax, returns raw text.

    Used by Convex for category proposals, keyword suggestions, and any other
    AI text generation that doesn't need a full pipeline.
    """
    from config import settings
    from services.minimax import MiniMaxClient

    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    response = await client.create_message(
        system="Jesteś ekspertem od salonów beauty na Booksy.pl.",
        messages=[{"role": "user", "content": request.prompt}],
        max_tokens=request.maxTokens,
        temperature=request.temperature,
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    return {"text": text, "inputTokens": response.usage.input_tokens, "outputTokens": response.usage.output_tokens}


class EmbeddingRequest(BaseModel):
    texts: list[str]


# Legacy OptimizeSeoRequest/OptimizeContentRequest/OptimizeCategoriesRequest/
# OptimizeFinalizeAsyncRequest removed in 3-BAGENT migration.


@app.post("/api/embeddings", dependencies=[Depends(verify_api_key)])
async def generate_embeddings(request: EmbeddingRequest) -> dict:
    """Generate Gemini embeddings for a list of texts."""
    import httpx

    from config import settings

    if not request.texts:
        return {"embeddings": []}

    gemini_key = settings.gemini_api_key
    if not gemini_key:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY not configured on server")

    all_embeddings: list[list[float]] = []
    for i in range(0, len(request.texts), 100):
        batch = request.texts[i:i + 100]
        requests_body = [
            {"model": "models/gemini-embedding-001", "content": {"parts": [{"text": t}]}, "taskType": "SEMANTIC_SIMILARITY"}
            for t in batch
        ]
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents?key={gemini_key}",
                json={"requests": requests_body},
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Gemini API error {resp.status_code}: {resp.text[:200]}")
            result = resp.json()
            for emb in result.get("embeddings", []):
                all_embeddings.append(emb["values"])

    return {"embeddings": all_embeddings}


# Legacy /api/optimize/{seo,content,categories,finalize} endpoints removed
# in 3-BAGENT migration. pipelines/optimize_phases.py is no longer imported
# and can be deleted in a follow-up cleanup.


# --- Background job ---


_STEP_PATTERN = re.compile(r"\[.+?\]\s*(Step\s+\d+[-\d]*):?\s*(.*)", re.IGNORECASE)


class _JobLogHandler(logging.Handler):
    """Captures pipeline log messages and writes them to the Job's log entries."""

    def __init__(self, job, job_store: JobStore) -> None:
        super().__init__()
        self.job = job
        self.job_store = job_store

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        m = _STEP_PATTERN.search(msg)
        step_name = m.group(1).strip() if m else None
        clean_msg = m.group(2).strip() if m else msg
        level = "error" if record.levelno >= logging.ERROR else "warning" if record.levelno >= logging.WARNING else "info"
        self.job.add_log(level, clean_msg, step=step_name)
        self.job_store.notify_progress(self.job)


# Legacy run_analysis_job + run_optimization_job + run_finalize_job +
# run_competitor_job removed in 3-BAGENT migration. The full competitor
# pipeline lives in pipelines/_premium/competitor.py as cold code for a
# future sprint. Report / cennik / summary jobs live below.


# ---------------------------------------------------------------------------
# 3-bagent migration background jobs
# ---------------------------------------------------------------------------


async def run_report_job(job_id: str, request: ReportRequest) -> None:
    """BAGENT #1 background job — report + content optimization.

    Either uses inline scrapedData from the request (for dev/testing) or
    loads it from Supabase audit_scraped_data. Runs pipelines/report.py,
    saves to audit_reports + children, and reports success/failure to Convex.
    """
    from services.convex import ConvexClient
    from services.supabase import SupabaseService

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.report")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from models.scraped_data import ScrapedData
        from pipelines.report import run_audit_pipeline

        # Load scraped data — prefer inline payload (dev), fall back to Supabase.
        if request.scrapedData:
            raw_scraped = request.scrapedData
        else:
            supabase = SupabaseService()
            raw_scraped = await supabase.get_scraped_data(request.auditId)
            if not raw_scraped:
                raise RuntimeError(
                    f"No audit_scraped_data for {request.auditId} and no inline payload"
                )
        scraped_data = ScrapedData(**raw_scraped)

        job.add_log("info", f"Parsed: {scraped_data.totalServices} services, {len(scraped_data.categories)} categories")
        store.notify_progress(job)

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.report_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex report_progress failed: %s", e)

        report = await run_audit_pipeline(scraped_data, request.auditId, on_progress)

        supabase = SupabaseService()
        await supabase.save_report(
            convex_audit_id=request.auditId,
            convex_user_id=request.userId,
            report=report,
            salon_name=scraped_data.salonName or "",
            salon_address=scraped_data.salonAddress or "",
            source_url=request.sourceUrl or "",
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Report job %s completed", job_id)

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        try:
            await convex.report_complete(
                audit_id=request.auditId,
                user_id=request.userId,
                overall_score=report.get("totalScore", 0),
                report_stats=report_stats,
            )
        except Exception as e:
            logger.warning("report_complete webhook failed (job still completed): %s", e)

    except CancelledError:
        logger.info("Report job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.report_fail(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Report job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.report_fail(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_free_report_job(job_id: str, request: FreeReportRequest) -> None:
    """Free-tier background job — frozen BAGENT #1 snapshot.

    Duplicates run_report_job's control flow intentionally rather than
    extracting shared code, so the frozen free-tier path cannot drift when
    run_report_job is refactored for Etap 1-3 of the unified report plan.

    Loads the same scraped data, calls pipelines.free_report (not .report),
    writes to the same audit_reports tables, and fires the same Convex
    report/progress/complete/fail webhooks. Convex cannot currently tell
    the difference — the tier split will be introduced on the Convex side
    once there is a real free-tier product surface.
    """
    from services.convex import ConvexClient
    from services.supabase import SupabaseService

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.free_report")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from models.scraped_data import ScrapedData
        from pipelines.free_report import run_free_report_pipeline

        # Load scraped data — prefer inline payload (dev), fall back to Supabase.
        if request.scrapedData:
            raw_scraped = request.scrapedData
        else:
            supabase = SupabaseService()
            raw_scraped = await supabase.get_scraped_data(request.auditId)
            if not raw_scraped:
                raise RuntimeError(
                    f"No audit_scraped_data for {request.auditId} and no inline payload"
                )
        scraped_data = ScrapedData(**raw_scraped)

        job.add_log("info", f"Parsed: {scraped_data.totalServices} services, {len(scraped_data.categories)} categories")
        store.notify_progress(job)

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.report_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex report_progress failed: %s", e)

        report = await run_free_report_pipeline(scraped_data, request.auditId, on_progress)

        supabase = SupabaseService()
        await supabase.save_report(
            convex_audit_id=request.auditId,
            convex_user_id=request.userId,
            report=report,
            salon_name=scraped_data.salonName or "",
            salon_address=scraped_data.salonAddress or "",
            source_url=request.sourceUrl or "",
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Free-report job %s completed", job_id)

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        try:
            await convex.report_complete(
                audit_id=request.auditId,
                user_id=request.userId,
                overall_score=report.get("totalScore", 0),
                report_stats=report_stats,
            )
        except Exception as e:
            logger.warning("report_complete webhook failed (free job still completed): %s", e)

    except CancelledError:
        logger.info("Free-report job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.report_fail(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Free-report job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.report_fail(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_cennik_job(job_id: str, request: CennikRequest) -> None:
    """BAGENT #2 background job — new pricelist generation."""
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.cennik")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from pipelines.cennik import run_cennik_pipeline

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.cennik_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex cennik_progress failed: %s", e)

        result = await run_cennik_pipeline(
            audit_id=request.auditId,
            on_progress=on_progress,
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Cennik job %s completed", job_id)

        try:
            await convex.cennik_complete(
                audit_id=request.auditId,
                category_proposal=result["category_proposal"],
                stats=result["stats"],
            )
        except Exception as e:
            logger.warning("cennik_complete webhook failed (job still completed): %s", e)

    except CancelledError:
        logger.info("Cennik job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.cennik_fail(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Cennik job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.cennik_fail(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_summary_job(job_id: str, request: SummaryRequest) -> None:
    """BAGENT #3 background job — audit summary with basic competitor preview."""
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.summary")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from pipelines.summary import run_summary_pipeline

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.summary_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex summary_progress failed: %s", e)

        await run_summary_pipeline(
            audit_id=request.auditId,
            user_id=request.userId,
            selected_competitor_ids=request.selectedCompetitorIds,
            on_progress=on_progress,
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Summary job %s completed", job_id)

        try:
            await convex.summary_complete(audit_id=request.auditId)
        except Exception as e:
            logger.warning("summary_complete webhook failed (job still completed): %s", e)

    except CancelledError:
        logger.info("Summary job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.summary_fail(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Summary job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.summary_fail(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_competitor_refresh_job(
    job_id: str, request: CompetitorRefreshRequest
) -> None:
    """Comp Etap 7 background job — refresh an existing premium competitor report.

    Loads the report + matches, calls pipelines.competitor_report_refresh:
    run_refresh which re-reads latest salon_scrapes for each competitor,
    appends a snapshot row, detects price changes, and updates
    next_refresh_at. Failures are best-effort — no user-facing webhook yet
    (premium subscription is not live).
    """
    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    pipeline_logger = logging.getLogger("pipelines.competitor_report_refresh")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from pipelines.competitor_report_refresh import run_refresh

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)

        result = await run_refresh(
            report_id=request.reportId,
            on_progress=on_progress,
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info(
            "Competitor refresh job %s completed: report=%s, snapshot_date=%s, "
            "next_refresh_at=%s, significant_changes=%s",
            job_id, request.reportId, result.get("snapshot_date"),
            result.get("next_refresh_at"), result.get("significant_change_count", 0),
        )

    except CancelledError:
        logger.info("Competitor refresh job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
    except Exception as e:
        logger.error(
            "Competitor refresh job %s failed: %s", job_id, e, exc_info=True,
        )
        job.mark_failed(str(e))
        store.notify_status(job)
    finally:
        pipeline_logger.removeHandler(handler)


async def run_competitor_report_job(
    job_id: str, request: CompetitorReportRequest,
) -> None:
    """Comp Etap 2 background job — full competitor report pipeline.

    Chains Etap 4 (compute_competitor_analysis) + Etap 5
    (synthesize_competitor_insights) via pipelines.competitor_report.
    Progress and completion are reported to Convex via the /api/competitor/
    report/{progress,complete,fail} webhooks.
    """
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    # Attach a log handler for both competitor_analysis and competitor_synthesis
    analysis_logger = logging.getLogger("pipelines.competitor_analysis")
    synthesis_logger = logging.getLogger("pipelines.competitor_synthesis")
    report_logger = logging.getLogger("pipelines.competitor_report")
    handler = _JobLogHandler(job, store)
    analysis_logger.addHandler(handler)
    synthesis_logger.addHandler(handler)
    report_logger.addHandler(handler)

    class CancelledError(Exception):
        pass

    try:
        from pipelines.competitor_report import run_competitor_report_pipeline

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.competitor_report_progress(
                    request.auditId, progress, message,
                )
            except Exception as e:
                logger.warning("Convex competitor_report_progress failed: %s", e)

        result = await run_competitor_report_pipeline(
            audit_id=request.auditId,
            tier=request.tier,
            selection_mode=request.selectionMode,
            target_count=request.targetCount,
            convex_user_id=request.userId,
            on_progress=on_progress,
            must_include_salon_ids=request.selectedCompetitorIds,
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info(
            "Competitor report job %s completed: report_id=%s, "
            "swot=%s, recs=%s, fallback=%s",
            job_id, result["report_id"],
            result["swot_item_count"], result["recommendation_count"],
            result["used_fallback"],
        )

        stats = {
            "reportId": result["report_id"],
            "swotItemCount": result["swot_item_count"],
            "recommendationCount": result["recommendation_count"],
            "usedFallback": result["used_fallback"],
        }
        try:
            await convex.competitor_report_complete(
                audit_id=request.auditId,
                report_id=result["report_id"],
                stats=stats,
            )
        except Exception as e:
            logger.warning(
                "competitor_report_complete webhook failed (job still completed): %s",
                e,
            )

    except CancelledError:
        logger.info("Competitor report job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.competitor_report_fail(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error(
            "Competitor report job %s failed: %s", job_id, e, exc_info=True,
        )
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.competitor_report_fail(request.auditId, str(e))
        except Exception:
            pass
    finally:
        analysis_logger.removeHandler(handler)
        synthesis_logger.removeHandler(handler)
        report_logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# Meta Ads attribution: UTM-tagged redirect endpoint
# ---------------------------------------------------------------------------
#
# Every Meta ad's destination URL points here, not directly to Booksy.
# Format: https://api.booksyaudit.pl/r/<booksy_id>?utm_source=meta&...
# Endpoint logs the click to ad_clicks (with hashed IP for privacy/dedup),
# then 302-redirects to the actual booksy.com URL with all UTMs preserved.
#
# This is the only way to close the attribution loop — Meta only knows
# clicks happened, but doesn't know whether a Booksy booking followed.
# By logging click → matching against later salon_scrapes diffs we get
# real "ad click → booking" attribution.

from fastapi import Request
from fastapi.responses import RedirectResponse


@app.get("/r/{booksy_id}")
async def attribution_redirect(
    booksy_id: int,
    request: Request,
    bg: BackgroundTasks,
):
    """UTM-tagged redirect → Booksy. Logs click event, 302's to real URL.

    URL params (passed through to Booksy):
      utm_source=meta, utm_medium=cpc, utm_campaign=<meta_campaign_id>,
      utm_content=<adset_id>, utm_term=<ad_id>, ba_audit=<audit_id>

    Anything not utm_* is also passed through so future channels (Google,
    TikTok) work without code changes."""
    import hashlib
    from datetime import datetime, timezone
    from urllib.parse import urlencode

    qp = dict(request.query_params)

    # Privacy-preserving IP hash with daily-rotating salt — enables
    # within-day dedup without storing raw PII.
    client_ip = request.client.host if request.client else ""
    daily_salt = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    ip_hash = hashlib.sha256(f"{client_ip}|{daily_salt}".encode()).hexdigest()[:32]

    # Construct destination URL — Booksy salon profile by booksy_id
    booksy_url = f"https://booksy.com/pl-pl/{booksy_id}"
    if qp:
        booksy_url = f"{booksy_url}?{urlencode(qp)}"

    async def log_click() -> None:
        try:
            from services.sb_client import make_supabase_client
            client = make_supabase_client(
                settings.supabase_url, settings.supabase_service_key,
            )
            client.table("ad_clicks").insert({
                "ip_hash": ip_hash,
                "user_agent": request.headers.get("user-agent", "")[:500],
                "utm_source": qp.get("utm_source"),
                "utm_medium": qp.get("utm_medium"),
                "utm_campaign": qp.get("utm_campaign"),
                "utm_content": qp.get("utm_content"),
                "utm_term": qp.get("utm_term"),
                "ba_audit": qp.get("ba_audit"),
                "booksy_id": booksy_id,
                "destination_url": booksy_url,
            }).execute()
        except Exception as e:  # noqa: BLE001
            logger.warning("ad_clicks insert failed (non-blocking): %s", e)

    bg.add_task(log_click)
    return RedirectResponse(url=booksy_url, status_code=302)


# ---------------------------------------------------------------------------
# Iter 8 — Outreach automation: wintact webhook handler
# ---------------------------------------------------------------------------
#
# Wintact pushes message lifecycle events here:
#   email.delivered, email.opened, email.clicked, email.bounced,
#   email.complained, email.unsubscribed
#
# We persist the raw payload to outreach_wintact_events for audit, then
# update the corresponding outreach_messages.{opened_at,clicked_at,...}
# fields. The transactional.send call records wintact_message_id at
# send time so this handler is a simple lookup-and-patch.
#
# Auth model: wintact does NOT support Bearer auth on its outgoing
# webhooks. We rely on a shared secret in the URL path component
# (settings.wintact_webhook_secret). This is the same pattern the
# Stripe webhook uses elsewhere in the stack.

@app.post("/api/wintact/webhook/{secret}")
async def wintact_webhook(
    secret: str,
    request: Request,
    bg: BackgroundTasks,
) -> dict:
    """Receive wintact lifecycle events, fan out to outreach_messages.

    Path:        /api/wintact/webhook/<secret>
    Auth:        path-segment shared secret (settings.api_key reused as
                 default — operator should set a dedicated value).
    Body:        wintact event JSON, shape per /webhooks.eventTypes.
    Side effect: row in outreach_wintact_events + patch on
                 outreach_messages.<opened|clicked|...>_at.
    """
    expected = getattr(settings, "wintact_webhook_secret", "") or settings.api_key
    if not expected or secret != expected:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    payload = await request.json()
    bg.add_task(_process_wintact_event, payload)
    return {"ok": True}


async def _process_wintact_event(payload: dict) -> None:
    """Persist raw event + patch outreach_messages columns."""
    from datetime import datetime, timezone

    from services.sb_client import make_supabase_client

    try:
        sb = make_supabase_client(
            settings.supabase_url, settings.supabase_service_key,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Wintact webhook: SB client failed: %s", exc)
        return

    event_type = (payload.get("type") or payload.get("event") or "").lower()
    wintact_message_id = (
        payload.get("message_id")
        or payload.get("data", {}).get("message_id")
        or payload.get("id")
    )
    contact_email = (
        payload.get("contact_email")
        or payload.get("data", {}).get("contact_email")
        or payload.get("email")
        or ""
    ).lower()

    # 1. Raw audit log — always recorded so we can replay if mapping fails.
    try:
        sb.table("outreach_wintact_events").insert({
            "wintact_event_type": event_type,
            "wintact_message_id": wintact_message_id,
            "contact_email_hash": (
                __import__("hashlib").sha256(contact_email.encode()).hexdigest()
                if contact_email else None
            ),
            "raw_payload": payload,
        }).execute()
    except Exception as exc:  # noqa: BLE001
        logger.error("Wintact webhook: audit insert failed: %s", exc)

    # 2. Patch outreach_messages with the lifecycle timestamp.
    if not wintact_message_id:
        return

    field_map = {
        "email.delivered":    "delivered_at",
        "email.opened":       "opened_at",
        "email.clicked":      "clicked_at",
        "email.replied":      "replied_at",
        "email.bounced":      "bounced_at",
        "email.complained":   "complained_at",
        "email.unsubscribed": "unsubscribed_at",
        "email.failed":       "failed_at",
    }
    column = field_map.get(event_type)
    if not column:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    update: dict = {column: now_iso}
    if event_type == "email.bounced":
        update["bounce_type"] = (payload.get("bounce_type")
                                 or payload.get("data", {}).get("bounce_type")
                                 or "soft")
    if event_type == "email.failed":
        reason = (payload.get("reason")
                  or payload.get("data", {}).get("reason")
                  or "")
        update["failure_reason"] = reason[:500]

    try:
        sb.table("outreach_messages").update(update).eq(
            "wintact_message_id", wintact_message_id,
        ).execute()
    except Exception as exc:  # noqa: BLE001
        logger.error("Wintact webhook: outreach_messages patch failed: %s", exc)
        return

    # FUNNEL_AUDIT R6: klik/odpowiedź to zdarzenia lejka — jednolity strumień
    # w funnel_events (idempotentnie po message id; zapis best-effort).
    if event_type in ("email.clicked", "email.replied"):
        from services.funnel_events import record_funnel_event

        record_funnel_event(
            sb,
            event_type="outreach_click" if event_type == "email.clicked" else "outreach_reply",
            source="outreach",
            dedupe_key=f"{event_type}:{wintact_message_id}",
            metadata={"wintact_message_id": wintact_message_id},
        )

    # 3. Cascading state effects: unsubscribe / complaint must close out
    # all active states for this contact across funnels.
    if event_type in ("email.unsubscribed", "email.complained"):
        try:
            row = (
                sb.table("outreach_messages")
                .select("contact_id")
                .eq("wintact_message_id", wintact_message_id)
                .single()
                .execute()
            ).data
            if row and row.get("contact_id"):
                contact_id = row["contact_id"]
                sb.table("outreach_contacts").update({
                    "unsubscribed_at": now_iso,
                    "unsubscribe_reason": event_type,
                }).eq("id", contact_id).execute()
                sb.table("outreach_customer_states").update({
                    "status": "opted_out",
                    "exited_at": now_iso,
                }).eq("contact_id", contact_id).eq("status", "active").execute()
                sb.table("outreach_state_transitions").insert({
                    "contact_id": contact_id,
                    "funnel": "audit",
                    "from_state": None,
                    "to_state": "opted_out",
                    "trigger_event": event_type,
                }).execute()
        except Exception as exc:  # noqa: BLE001
            logger.error("Wintact webhook: opt-out cascade failed: %s", exc)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
