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

        scraped_data = ScrapedData(**request.scrapedData)

        async def on_progress(progress: int, message: str) -> None:
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)
            await convex.update_progress(request.auditId, progress, message)

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

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        await convex.complete_audit(request.auditId, report.get("totalScore", 0), report_stats)

        job.mark_completed()
        store.notify_status(job)
        logger.info("Job %s completed successfully", job_id)

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
