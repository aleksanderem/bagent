"""Convex HTTP webhook client for progress updates and audit completion."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from config import settings

logger = logging.getLogger(__name__)


class ConvexClient:
    """Client for calling Convex HTTP endpoints (webhooks)."""

    def __init__(self) -> None:
        self.base_url = settings.convex_url
        self.api_key = settings.api_key

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
        }

    async def update_progress(self, audit_id: str, progress: int, message: str) -> None:
        """Call Convex HTTP endpoint to update audit progress.

        POST /api/audit/progress {auditId, progress, progressMessage}

        Best-effort: does NOT raise on failure.
        """
        url = f"{self.base_url}/api/audit/progress"
        payload = {
            "auditId": audit_id,
            "progress": progress,
            "progressMessage": message,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"Progress updated: {audit_id} -> {progress}% ({message})")
        except Exception as e:
            logger.warning(f"Failed to update progress for {audit_id}: {e}")

    async def complete_audit(self, audit_id: str, score: int, report_stats: dict[str, Any]) -> None:
        """Call Convex HTTP endpoint to mark audit as completed.

        POST /api/audit/complete {auditId, overallScore, reportStats}

        Critical: RAISES on failure.
        """
        url = f"{self.base_url}/api/audit/complete"
        payload = {
            "auditId": audit_id,
            "overallScore": score,
            "reportStats": report_stats,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"Audit completed: {audit_id} with score {score}")
        except Exception as e:
            logger.error(f"Failed to complete audit {audit_id}: {e}")
            raise

    async def fail_audit(self, audit_id: str, error_message: str) -> None:
        """Call Convex HTTP endpoint to mark audit as failed.

        POST /api/audit/fail {auditId, errorMessage}

        Best-effort: does NOT raise on failure.
        """
        url = f"{self.base_url}/api/audit/fail"
        payload = {
            "auditId": audit_id,
            "errorMessage": error_message,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"Audit failed: {audit_id} ({error_message})")
        except Exception as e:
            logger.error(f"Failed to mark audit as failed {audit_id}: {e}")

    # --- Optimization webhooks ---

    async def complete_optimization_phase(
        self, job_id: str, phase: str, output_json: str, progress: int,
    ) -> None:
        """Report optimization phase completion to Convex.

        POST /api/optimization/phase-complete {jobId, phase, outputJson, progress}

        Best-effort: does NOT raise on failure.
        """
        url = f"{self.base_url}/api/optimization/phase-complete"
        payload = {
            "jobId": job_id,
            "phase": phase,
            "outputJson": output_json,
            "progress": progress,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"Optimization phase {phase} completed: {job_id} ({progress}%)")
        except Exception as e:
            logger.warning(f"Failed to report optimization phase {phase}: {e}")

    async def complete_optimization(
        self, job_id: str, output_pricing_data_json: str, optimization_result_json: str,
    ) -> None:
        """Report optimization completion to Convex.

        POST /api/optimization/complete {jobId, outputPricingDataJson, optimizationResultJson}

        Critical: RAISES on failure.
        """
        url = f"{self.base_url}/api/optimization/complete"
        payload = {
            "jobId": job_id,
            "outputPricingDataJson": output_pricing_data_json,
            "optimizationResultJson": optimization_result_json,
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload, headers=self._headers())
            response.raise_for_status()
        logger.info(f"Optimization completed: {job_id}")

    async def fail_optimization(self, job_id: str, error_message: str) -> None:
        """Report optimization failure to Convex.

        POST /api/optimization/fail {jobId, errorMessage}

        Best-effort: does NOT raise on failure.
        """
        url = f"{self.base_url}/api/optimization/fail"
        payload = {
            "jobId": job_id,
            "errorMessage": error_message,
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=self._headers())
                response.raise_for_status()
                logger.info(f"Optimization failed: {job_id} ({error_message})")
        except Exception as e:
            logger.warning(f"Failed to report optimization failure: {e}")
