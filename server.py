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
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from config import settings
from job_store import Job, JobStore
from workers import get_redis_pool, set_cancel_flag

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
    targetCount: int = 5
    selectedCompetitorBooksyIds: list[int] | None = None  # for manual mode


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

    Raises HTTPException(503) if the pool is unavailable. Logs warning if
    enqueue returns None (job_id collision — shouldn't happen with UUID).
    """
    pool = getattr(app.state, "arq", None)
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="job queue unavailable (Redis disconnected) — retry shortly",
        )
    arq_job = await pool.enqueue_job(
        task_name,
        request_payload,
        _job_id=job_id,
    )
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
    store.create_job(job_id, request.auditId, meta={
        "type": "competitor_refresh",
        "reportId": request.reportId,
        "userId": request.userId,
    })
    await _enqueue_pipeline(
        task_name="run_competitor_refresh_task",
        job_id=job_id,
        request_payload=request.model_dump(),
    )
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
