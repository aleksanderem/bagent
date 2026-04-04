"""FastAPI app entry point for bagent — Beauty Audit AI Analyzer."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from pathlib import Path

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from config import settings
from job_store import JobStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="bagent — Beauty Audit AI Analyzer")

store = JobStore()

TEMPLATES_DIR = Path(__file__).parent / "templates"
_dashboard_html: str | None = None


def _get_dashboard_html() -> str:
    global _dashboard_html
    if _dashboard_html is None:
        _dashboard_html = (TEMPLATES_DIR / "dashboard.html").read_text(encoding="utf-8")
    return _dashboard_html


# --- Request/Response models ---


class AnalyzeRequest(BaseModel):
    auditId: str
    userId: str
    sourceUrl: str | None = None
    scrapedData: dict


class CompetitorRequest(BaseModel):
    auditId: str
    userId: str
    subjectSalonId: int
    salonName: str
    salonCity: str
    salonLat: float | None = None
    salonLng: float | None = None
    selectedCompetitorIds: list[int]
    services: list[str] = []


class OptimizationRequest(BaseModel):
    auditId: str
    userId: str
    pricelistId: str
    jobId: str
    scrapedData: dict
    auditReport: dict
    selectedOptions: list[str]
    promptTemplates: dict | None = None


class KeywordRequest(BaseModel):
    auditId: str
    scrapedData: dict


class AiTextRequest(BaseModel):
    """Generic AI text generation — send prompt, get text back."""
    prompt: str
    maxTokens: int = 4000
    temperature: float = 0.4


class AnalyzeResponse(BaseModel):
    jobId: str
    status: str = "accepted"


# --- Auth ---


def verify_api_key(x_api_key: str = Header(...)) -> None:
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


# --- Endpoints ---


@app.post("/api/analyze", status_code=202, response_model=AnalyzeResponse, dependencies=[Depends(verify_api_key)])
async def start_analysis(
    request: AnalyzeRequest,
    background_tasks: BackgroundTasks,
) -> AnalyzeResponse:
    job_id = str(uuid.uuid4())
    service_count = request.scrapedData.get("totalServices", 0)
    salon_name = request.scrapedData.get("salonName", "")
    store.create_job(job_id, request.auditId, meta={
        "userId": request.userId,
        "sourceUrl": request.sourceUrl,
        "salonName": salon_name,
        "totalServices": service_count,
    })
    background_tasks.add_task(run_analysis_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


@app.post("/api/competitor", status_code=202, response_model=AnalyzeResponse, dependencies=[Depends(verify_api_key)])
async def start_competitor_report(
    request: CompetitorRequest,
    background_tasks: BackgroundTasks,
) -> AnalyzeResponse:
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "competitor",
        "userId": request.userId,
        "salonName": request.salonName,
        "salonCity": request.salonCity,
        "competitorCount": len(request.selectedCompetitorIds),
    })
    background_tasks.add_task(run_competitor_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


@app.post("/api/optimize", status_code=202, response_model=AnalyzeResponse, dependencies=[Depends(verify_api_key)])
async def start_optimization(
    request: OptimizationRequest,
    background_tasks: BackgroundTasks,
) -> AnalyzeResponse:
    job_id = str(uuid.uuid4())
    service_count = request.scrapedData.get("totalServices", 0)
    salon_name = request.scrapedData.get("salonName", "")
    store.create_job(job_id, request.auditId, meta={
        "type": "optimization",
        "userId": request.userId,
        "salonName": salon_name,
        "totalServices": service_count,
        "selectedOptions": request.selectedOptions,
    })
    background_tasks.add_task(run_optimization_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


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
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "running":
        raise HTTPException(status_code=400, detail=f"Cannot cancel job with status '{job.status}'")
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
    return {"status": "ok", "jobs_running": running, "jobs_total": total}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return HTMLResponse(_get_dashboard_html())


# --- Synchronous AI endpoints (no background job, direct response) ---


@app.post("/api/keywords", dependencies=[Depends(verify_api_key)])
async def generate_keywords(request: KeywordRequest) -> dict:
    """Keyword report: rule-based extraction + AI suggestions. Returns full report synchronously."""
    from pipelines.keywords import run_keyword_pipeline

    result = await run_keyword_pipeline(
        scraped_data=request.scrapedData,
        audit_id=request.auditId,
    )
    return result


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


@app.post("/api/embeddings", dependencies=[Depends(verify_api_key)])
async def generate_embeddings(request: dict = {}) -> dict:
    """Generate Gemini embeddings for a list of texts.

    Request body: { texts: string[], model?: string }
    Returns: { embeddings: number[][] }
    """
    import httpx as _httpx
    from config import settings

    texts = request.get("texts", [])
    if not texts:
        return {"embeddings": []}

    api_key = settings.minimax_api_key  # Reuse configured key or add GEMINI_API_KEY to settings
    gemini_key = getattr(settings, "gemini_api_key", "") or api_key

    # Gemini batchEmbedContents — up to 100 texts per request
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), 100):
        batch = texts[i:i + 100]
        requests_body = [
            {"model": "models/gemini-embedding-001", "content": {"parts": [{"text": t}]}, "taskType": "SEMANTIC_SIMILARITY"}
            for t in batch
        ]
        async with _httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents?key={gemini_key}",
                json={"requests": requests_body},
            )
            resp.raise_for_status()
            result = resp.json()
            for emb in result.get("embeddings", []):
                all_embeddings.append(emb["values"])

    return {"embeddings": all_embeddings}


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


async def run_analysis_job(job_id: str, request: AnalyzeRequest) -> None:
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()

    # Attach log handler to pipeline logger
    pipeline_logger = logging.getLogger("pipelines.audit")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    try:
        from models.scraped_data import ScrapedData
        from pipelines.audit import run_audit_pipeline

        job.add_log("info", "Parsing scraped data...")
        store.notify_progress(job)
        scraped_data = ScrapedData(**request.scrapedData)
        job.add_log("info", f"Parsed: {scraped_data.totalServices} services, {len(scraped_data.categories)} categories")
        store.notify_progress(job)

        class CancelledError(Exception):
            pass

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.update_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex progress webhook failed: %s", e)

        report = await run_audit_pipeline(scraped_data, request.auditId, on_progress)

        from services.supabase import SupabaseService

        supabase = SupabaseService()
        await supabase.save_report(
            convex_audit_id=request.auditId,
            convex_user_id=request.userId,
            report=report,
            salon_name=scraped_data.salonName or "",
            salon_address=scraped_data.salonAddress or "",
            source_url=request.sourceUrl or "",
        )

        # Pipeline + save succeeded — mark job completed regardless of webhook outcome
        job.mark_completed()
        store.notify_status(job)
        logger.info("Job %s completed successfully (report saved)", job_id)

        # Notify Convex — best-effort, job is already completed
        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        try:
            await convex.complete_audit(request.auditId, report.get("totalScore", 0), report_stats)
        except Exception as e:
            logger.warning("Job %s: Convex complete_audit webhook failed (job still completed): %s", job_id, e)

    except CancelledError:
        logger.info("Job %s cancelled by user", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.fail_audit(request.auditId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.fail_audit(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_competitor_job(job_id: str, request: CompetitorRequest) -> None:
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.competitor")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    try:
        from pipelines.competitor import run_competitor_pipeline

        class CancelledError(Exception):
            pass

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.update_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex progress webhook failed: %s", e)

        report = await run_competitor_pipeline(
            audit_id=request.auditId,
            subject_salon_id=request.subjectSalonId,
            salon_name=request.salonName,
            salon_city=request.salonCity,
            salon_lat=request.salonLat,
            salon_lng=request.salonLng,
            selected_competitor_ids=request.selectedCompetitorIds,
            services=request.services,
            on_progress=on_progress,
        )

        from services.supabase import SupabaseService

        supabase = SupabaseService()
        await supabase.save_competitor_report(
            convex_audit_id=request.auditId,
            convex_user_id=request.userId,
            subject_booksy_id=request.subjectSalonId,
            report_data=report,
            competitor_count=len(request.selectedCompetitorIds),
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Competitor job %s completed", job_id)

        try:
            await convex.complete_audit(request.auditId, 0, {"type": "competitor"})
        except Exception as e:
            logger.warning("Competitor job %s: Convex webhook failed: %s", job_id, e)

    except CancelledError:
        logger.info("Competitor job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
    except Exception as e:
        logger.error("Competitor job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.fail_audit(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_optimization_job(job_id: str, request: OptimizationRequest) -> None:
    from services.convex import ConvexClient

    job = store.get_job(job_id)
    if job is None:
        return
    job.mark_running()
    store.notify_status(job)

    convex = ConvexClient()
    pipeline_logger = logging.getLogger("pipelines.optimization")
    handler = _JobLogHandler(job, store)
    pipeline_logger.addHandler(handler)

    try:
        from models.scraped_data import ScrapedData
        from pipelines.optimization import run_optimization_pipeline

        scraped_data = ScrapedData(**request.scrapedData)

        class CancelledError(Exception):
            pass

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            try:
                await convex.update_progress(request.auditId, progress, message)
            except Exception as e:
                logger.warning("Convex progress webhook failed: %s", e)

        result = await run_optimization_pipeline(
            scraped_data=scraped_data,
            audit_report=request.auditReport,
            selected_options=request.selectedOptions,
            audit_id=request.auditId,
            on_progress=on_progress,
        )

        from services.supabase import SupabaseService

        supabase = SupabaseService()
        await supabase.save_optimized_pricelist(
            convex_audit_id=request.auditId,
            optimization_data=result,
            salon_name=scraped_data.salonName or "",
        )

        job.mark_completed()
        store.notify_status(job)
        logger.info("Optimization job %s completed", job_id)

        try:
            await convex.complete_audit(request.auditId, 0, {"type": "optimization", "jobId": request.jobId})
        except Exception as e:
            logger.warning("Optimization job %s: Convex webhook failed: %s", job_id, e)

    except CancelledError:
        logger.info("Optimization job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
    except Exception as e:
        logger.error("Optimization job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.fail_audit(request.auditId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
