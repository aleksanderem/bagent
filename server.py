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


# CompetitorRequest REMOVED — the full competitor pipeline moved to
# pipelines/_premium/competitor.py. Will be reintroduced in a future sprint
# as part of the Premium Competitor Analysis product.


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


class CennikRequest(BaseModel):
    """BAGENT #2 — generate new pricelist from report + original scrape."""
    auditId: str
    userId: str


class SummaryRequest(BaseModel):
    """BAGENT #3 — audit summary with basic competitor preview."""
    auditId: str
    userId: str
    selectedCompetitorIds: list[int] = []


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


# /api/competitor endpoint REMOVED in 3-bagent migration.
# The full SWOT competitor pipeline moved to bagent/pipelines/_premium/competitor.py
# and will be reactivated as a paid Premium Competitor Analysis product in a
# future sprint. See bagent/pipelines/_premium/README.md for reactivation steps.
# Basic competitor preview (top 3-5 salons, no SWOT) is now produced by BAGENT #3
# (pipelines/summary.py) as part of the Podsumowanie tab.


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


# --- 3-bagent migration endpoints ---


@app.post(
    "/api/audit/report", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_report(
    request: ReportRequest, background_tasks: BackgroundTasks
) -> AnalyzeResponse:
    """BAGENT #1 — report + content optimization.

    Runs the report pipeline in the background and returns immediately with a
    job ID. Progress updates go to Convex via /api/audit/report/progress;
    completion via /api/audit/report/complete; failure via .../fail.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "report",
        "userId": request.userId,
        "sourceUrl": request.sourceUrl,
    })
    background_tasks.add_task(run_report_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/audit/cennik", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_cennik(
    request: CennikRequest, background_tasks: BackgroundTasks
) -> AnalyzeResponse:
    """BAGENT #2 — new pricelist generation.

    Automatically triggered by Convex after BAGENT #1 finishes. Loads report
    and scraped data from Supabase, applies transformations, restructures
    categories, writes optimized_pricelists + categoryProposals payload.
    """
    job_id = str(uuid.uuid4())
    store.create_job(job_id, request.auditId, meta={
        "type": "cennik",
        "userId": request.userId,
    })
    background_tasks.add_task(run_cennik_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


@app.post(
    "/api/audit/summary", status_code=202, response_model=AnalyzeResponse,
    dependencies=[Depends(verify_api_key)],
)
async def start_summary(
    request: SummaryRequest, background_tasks: BackgroundTasks
) -> AnalyzeResponse:
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
    background_tasks.add_task(run_summary_job, job_id, request)
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


class EmbeddingRequest(BaseModel):
    texts: list[str]


class OptimizeSeoRequest(BaseModel):
    auditId: str


class OptimizeContentRequest(BaseModel):
    auditId: str
    pricelist: dict


class OptimizeCategoriesRequest(BaseModel):
    auditId: str
    pricelist: dict


class OptimizeFinalizeAsyncRequest(BaseModel):
    auditId: str
    jobId: str
    pricelist: dict


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


@app.post("/api/optimize/seo", dependencies=[Depends(verify_api_key)])
async def optimize_seo(request: OptimizeSeoRequest) -> dict:
    """Phase 1: Inject missing SEO keywords into service names."""
    from pipelines.optimize_phases import run_phase1_seo
    return await run_phase1_seo(audit_id=request.auditId)


@app.post("/api/optimize/content", dependencies=[Depends(verify_api_key)])
async def optimize_content(request: OptimizeContentRequest) -> dict:
    """Phase 2: Rewrite names + descriptions (copywriting, benefit language)."""
    from pipelines.optimize_phases import run_phase2_content
    return await run_phase2_content(audit_id=request.auditId, pricelist=request.pricelist)


@app.post("/api/optimize/categories", dependencies=[Depends(verify_api_key)])
async def optimize_categories(request: OptimizeCategoriesRequest) -> dict:
    """Phase 3: Reorganize categories based on optimized content."""
    from pipelines.optimize_phases import run_phase3_categories
    return await run_phase3_categories(audit_id=request.auditId, pricelist=request.pricelist)


@app.post("/api/optimize/finalize", status_code=202, response_model=AnalyzeResponse, dependencies=[Depends(verify_api_key)])
async def optimize_finalize_async(
    request: OptimizeFinalizeAsyncRequest,
    background_tasks: BackgroundTasks,
) -> AnalyzeResponse:
    """Phase 4 (fire-and-forget): Apply programmatic fixes, generate diff, save to Supabase."""
    background_tasks.add_task(run_finalize_job, request.jobId, request.auditId, request.pricelist)
    return AnalyzeResponse(jobId=request.jobId)


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

    class CancelledError(Exception):
        pass

    try:
        from models.scraped_data import ScrapedData
        from pipelines.audit import run_audit_pipeline

        job.add_log("info", "Parsing scraped data...")
        store.notify_progress(job)
        scraped_data = ScrapedData(**request.scrapedData)
        job.add_log("info", f"Parsed: {scraped_data.totalServices} services, {len(scraped_data.categories)} categories")
        store.notify_progress(job)

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


# run_competitor_job REMOVED — the full competitor pipeline moved to
# pipelines/_premium/competitor.py as cold code for the next sprint.
# Basic competitor preview is now produced by run_summary_job below
# (BAGENT #3, pipelines/summary.py).


async def run_optimization_job(job_id: str, request: OptimizationRequest) -> None:
    """Run optimization phases 1-3, reporting progress to Convex after each phase.

    Phase 4 (finalize) is NOT run here — Convex triggers it later via /api/optimize/finalize.
    """
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

    class CancelledError(Exception):
        pass

    try:
        from pipelines.optimize_phases import (
            run_phase1_seo,
            run_phase2_content,
            run_phase3_categories,
        )

        async def on_progress(progress: int, message: str) -> None:
            if job.cancel_requested:
                raise CancelledError("Job cancelled by user")
            job.add_log("info", message, progress=progress)
            store.notify_progress(job)

        # Phase 1: SEO
        await on_progress(5, "Faza 1: SEO — wstrzykiwanie słów kluczowych...")
        phase1 = await run_phase1_seo(audit_id=request.auditId, on_progress=on_progress)
        await convex.complete_optimization_phase(
            job_id=request.jobId,
            phase="phase1_seo",
            output_json=json.dumps(phase1, ensure_ascii=False),
            progress=25,
        )

        # Phase 2: Content
        await on_progress(30, "Faza 2: Treści — nazwy i opisy usług...")
        phase2 = await run_phase2_content(
            audit_id=request.auditId,
            pricelist=phase1["pricelist"],
            on_progress=on_progress,
        )
        await convex.complete_optimization_phase(
            job_id=request.jobId,
            phase="phase2_content",
            output_json=json.dumps(phase2, ensure_ascii=False),
            progress=50,
        )

        # Phase 3: Categories
        await on_progress(55, "Faza 3: Kategorie — reorganizacja struktury...")
        phase3 = await run_phase3_categories(
            audit_id=request.auditId,
            pricelist=phase2["pricelist"],
            on_progress=on_progress,
        )
        await convex.complete_optimization_phase(
            job_id=request.jobId,
            phase="phase3_categories",
            output_json=json.dumps(phase3, ensure_ascii=False),
            progress=75,
        )

        # Done — phase 4 will be triggered by Convex via /api/optimize/finalize
        job.mark_completed()
        store.notify_status(job)
        logger.info("Optimization job %s completed (phases 1-3)", job_id)

    except CancelledError:
        logger.info("Optimization job %s cancelled", job_id)
        job.mark_cancelled()
        store.notify_status(job)
        try:
            await convex.fail_optimization(request.jobId, "Cancelled by user")
        except Exception:
            pass
    except Exception as e:
        logger.error("Optimization job %s failed: %s", job_id, e, exc_info=True)
        job.mark_failed(str(e))
        store.notify_status(job)
        try:
            await convex.fail_optimization(request.jobId, str(e))
        except Exception:
            pass
    finally:
        pipeline_logger.removeHandler(handler)


async def run_finalize_job(job_id: str, audit_id: str, pricelist: dict) -> None:
    """Run optimization phase 4 (deterministic, no AI) and report back to Convex.

    LEGACY 4-phase optimization pipeline. The 3-BAGENT migration replaces
    this with cennik.py which owns optimized_pricelists/categories/services.
    This function no longer writes to Supabase normalized tables — it only
    runs the deterministic finalize transforms (clean names, detect promos,
    detect duplicates) and returns the result to Convex via webhook.

    Writing to Supabase was REMOVED because this legacy path was overwriting
    the normalized tree with an empty payload, wiping cennik's output. The
    legacy optimizationJobs table still gets its completion callback so the
    old flow terminates cleanly while 3-BAGENT owns the source-of-truth data.
    """
    from services.convex import ConvexClient

    convex = ConvexClient()
    try:
        from pipelines.optimize_phases import run_phase4_finalize
        from services.supabase import SupabaseService

        supabase = SupabaseService()
        original = await supabase.get_scraped_data(audit_id)

        result = run_phase4_finalize(
            audit_id=audit_id,
            pricelist=pricelist,
            original_pricelist=original,
        )

        # NOTE: save_optimized_pricelist() call REMOVED — the 3-BAGENT
        # cennik pipeline owns the optimized_pricelists/categories/services
        # tree. Calling it here with the legacy `result` shape produces
        # an empty children array and wipes the normalized data.

        await convex.complete_optimization(
            job_id=job_id,
            output_pricing_data_json=json.dumps(result.get("finalPricelist", result), ensure_ascii=False),
            optimization_result_json=json.dumps(result, ensure_ascii=False),
        )
        logger.info("Finalize job %s completed (no Supabase write — owned by cennik)", job_id)
    except Exception as e:
        logger.error("Finalize job %s failed: %s", job_id, e, exc_info=True)
        try:
            await convex.fail_optimization(job_id, str(e))
        except Exception:
            pass


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
