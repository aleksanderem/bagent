"""FastAPI app entry point for bagent — Beauty Audit AI Analyzer."""

import logging
import uuid

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="bagent — Beauty Audit AI Analyzer")

jobs: dict[str, dict] = {}


class AnalyzeRequest(BaseModel):
    auditId: str
    userId: str
    sourceUrl: str | None = None
    scrapedData: dict  # Raw dict, pipeline converts to ScrapedData


class AnalyzeResponse(BaseModel):
    jobId: str
    status: str = "accepted"


class JobStatus(BaseModel):
    jobId: str
    status: str  # "running", "completed", "failed"
    error: str | None = None


def verify_api_key(x_api_key: str = Header(...)) -> None:
    """Verify x-api-key header matches configured API key."""
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.post("/api/analyze", status_code=202, response_model=AnalyzeResponse, dependencies=[Depends(verify_api_key)])
async def start_analysis(
    request: AnalyzeRequest,
    background_tasks: BackgroundTasks,
) -> AnalyzeResponse:
    """Start async audit analysis. Returns immediately with job ID."""
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "running", "auditId": request.auditId}

    background_tasks.add_task(run_analysis_job, job_id, request)
    return AnalyzeResponse(jobId=job_id)


@app.get("/api/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str) -> JobStatus:
    """Check job status."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    return JobStatus(jobId=job_id, status=job["status"], error=job.get("error"))


@app.get("/api/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


async def run_analysis_job(job_id: str, request: AnalyzeRequest) -> None:
    """Background task that runs the full audit pipeline."""
    from services.convex import ConvexClient
    from services.supabase import SupabaseService

    convex = ConvexClient()

    try:
        from models.scraped_data import ScrapedData
        from pipelines.audit import run_audit_pipeline

        scraped_data = ScrapedData(**request.scrapedData)

        async def on_progress(progress: int, message: str) -> None:
            await convex.update_progress(request.auditId, progress, message)

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

        report_stats = {
            "totalServices": report.get("stats", {}).get("totalServices", 0),
            "totalTransformations": len(report.get("transformations", [])),
            "totalIssues": len(report.get("topIssues", [])),
        }
        await convex.complete_audit(request.auditId, report.get("totalScore", 0), report_stats)

        jobs[job_id] = {"status": "completed", "auditId": request.auditId}
        logger.info(f"Job {job_id} completed successfully")

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}", exc_info=True)
        jobs[job_id] = {"status": "failed", "auditId": request.auditId, "error": str(e)}
        try:
            await convex.fail_audit(request.auditId, str(e))
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
