"""Structured job tracking with logging, timing, and SSE notifications."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class LogEntry:
    timestamp: float
    level: Literal["info", "warning", "error"]
    message: str
    step: str | None = None
    progress: int | None = None
    data: dict[str, Any] | None = None


@dataclass
class Job:
    job_id: str
    audit_id: str
    status: Literal["queued", "running", "completed", "failed", "cancelled"] = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    current_step: str | None = None
    progress: int = 0
    progress_message: str = ""
    error: str | None = None
    logs: list[LogEntry] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)
    _cancel_requested: bool = field(default=False, repr=False)

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_requested

    def request_cancel(self) -> None:
        self._cancel_requested = True
        self.add_log("warning", "Cancellation requested")

    def mark_cancelled(self) -> None:
        self.status = "cancelled"
        self.completed_at = time.time()
        self.add_log("warning", "Job cancelled")

    def add_log(
        self,
        level: Literal["info", "warning", "error"],
        message: str,
        step: str | None = None,
        progress: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        entry = LogEntry(
            timestamp=time.time(),
            level=level,
            message=message,
            step=step,
            progress=progress,
            data=data,
        )
        self.logs.append(entry)
        if progress is not None:
            self.progress = progress
        if message:
            self.progress_message = message
        if step:
            # Close previous step timing
            if self.current_step and self.current_step != step and self.current_step in self.steps:
                prev = self.steps[self.current_step]
                if prev.get("completed_at") is None:
                    prev["completed_at"] = time.time()
                    prev["duration_ms"] = int((prev["completed_at"] - prev["started_at"]) * 1000)
            self.current_step = step
            if step not in self.steps:
                self.steps[step] = {"started_at": time.time(), "completed_at": None, "duration_ms": None}

    def mark_running(self) -> None:
        self.status = "running"
        self.started_at = time.time()
        self.add_log("info", "Job started")

    def mark_completed(self) -> None:
        self.status = "completed"
        self.completed_at = time.time()
        # Close last step
        if self.current_step and self.current_step in self.steps:
            s = self.steps[self.current_step]
            if s.get("completed_at") is None:
                s["completed_at"] = time.time()
                s["duration_ms"] = int((s["completed_at"] - s["started_at"]) * 1000)
        self.progress = 100
        elapsed = int((self.completed_at - (self.started_at or self.created_at)) * 1000)
        self.add_log("info", f"Job completed in {elapsed}ms")

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self.completed_at = time.time()
        self.error = error
        self.add_log("error", f"Job failed: {error}")

    def to_summary(self) -> dict[str, Any]:
        return {
            "jobId": self.job_id,
            "auditId": self.audit_id,
            "status": self.status,
            "progress": self.progress,
            "progressMessage": self.progress_message,
            "currentStep": self.current_step,
            "createdAt": self.created_at,
            "startedAt": self.started_at,
            "completedAt": self.completed_at,
            "error": self.error,
            "logCount": len(self.logs),
            "meta": self.meta,
            "steps": self.steps,
        }

    def to_dict(self) -> dict[str, Any]:
        result = self.to_summary()
        result["logs"] = [
            {
                "timestamp": e.timestamp,
                "level": e.level,
                "message": e.message,
                "step": e.step,
                "progress": e.progress,
                "data": e.data,
            }
            for e in self.logs
        ]
        return result


class JobStore:
    def __init__(self, max_jobs: int = 50) -> None:
        self._jobs: dict[str, Job] = {}
        self._max_jobs = max_jobs
        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []

    def create_job(self, job_id: str, audit_id: str, meta: dict[str, Any] | None = None) -> Job:
        job = Job(job_id=job_id, audit_id=audit_id, meta=meta or {})
        self._jobs[job_id] = job
        self._evict()
        self._notify({"type": "job_created", "jobId": job_id, "auditId": audit_id})
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        jobs = list(self._jobs.values())
        # Running first, then by created_at descending
        jobs.sort(key=lambda j: (j.status != "running", -j.created_at))
        return jobs

    def notify_progress(self, job: Job) -> None:
        self._notify({
            "type": "job_progress",
            "jobId": job.job_id,
            "status": job.status,
            "progress": job.progress,
            "progressMessage": job.progress_message,
            "currentStep": job.current_step,
        })

    def notify_status(self, job: Job) -> None:
        self._notify({
            "type": f"job_{job.status}",
            "jobId": job.job_id,
            "status": job.status,
            "progress": job.progress,
            "error": job.error,
        })

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def _notify(self, event: dict[str, Any]) -> None:
        for q in self._subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop events for slow clients

    def _evict(self) -> None:
        if len(self._jobs) <= self._max_jobs:
            return
        completed = [
            j for j in self._jobs.values() if j.status in ("completed", "failed", "cancelled")
        ]
        completed.sort(key=lambda j: j.created_at)
        while len(self._jobs) > self._max_jobs and completed:
            old = completed.pop(0)
            del self._jobs[old.job_id]
