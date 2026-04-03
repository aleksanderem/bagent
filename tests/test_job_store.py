"""Tests for job_store.py — JobStore, Job, LogEntry."""

import asyncio
import json
import time

import pytest

from job_store import Job, JobStore, LogEntry


class TestLogEntry:
    def test_creation(self):
        entry = LogEntry(timestamp=time.time(), level="info", message="test")
        assert entry.level == "info"
        assert entry.step is None


class TestJob:
    def test_initial_state(self):
        job = Job(job_id="j1", audit_id="a1")
        assert job.status == "queued"
        assert job.progress == 0
        assert job.logs == []

    def test_add_log_updates_progress(self):
        job = Job(job_id="j1", audit_id="a1")
        job.add_log("info", "Working...", progress=42)
        assert job.progress == 42
        assert job.progress_message == "Working..."
        assert len(job.logs) == 1

    def test_add_log_tracks_steps(self):
        job = Job(job_id="j1", audit_id="a1")
        job.add_log("info", "Naming", step="Step 2")
        assert job.current_step == "Step 2"
        assert "Step 2" in job.steps
        job.add_log("info", "Descriptions", step="Step 4")
        assert job.current_step == "Step 4"
        # Step 2 should be closed
        assert job.steps["Step 2"]["completed_at"] is not None

    def test_mark_running(self):
        job = Job(job_id="j1", audit_id="a1")
        job.mark_running()
        assert job.status == "running"
        assert job.started_at is not None
        assert len(job.logs) == 1

    def test_mark_completed(self):
        job = Job(job_id="j1", audit_id="a1")
        job.mark_running()
        job.mark_completed()
        assert job.status == "completed"
        assert job.completed_at is not None
        assert job.progress == 100

    def test_mark_failed(self):
        job = Job(job_id="j1", audit_id="a1")
        job.mark_running()
        job.mark_failed("boom")
        assert job.status == "failed"
        assert job.error == "boom"

    def test_to_summary(self):
        job = Job(job_id="j1", audit_id="a1", meta={"salon": "Test"})
        summary = job.to_summary()
        assert summary["jobId"] == "j1"
        assert summary["auditId"] == "a1"
        assert summary["meta"]["salon"] == "Test"
        assert "logs" not in summary

    def test_to_dict_includes_logs(self):
        job = Job(job_id="j1", audit_id="a1")
        job.add_log("info", "hello")
        d = job.to_dict()
        assert len(d["logs"]) == 1
        assert d["logs"][0]["message"] == "hello"

    def test_to_dict_serializable(self):
        job = Job(job_id="j1", audit_id="a1")
        job.mark_running()
        job.add_log("info", "step", step="Step 1", progress=50, data={"score": 15})
        job.mark_completed()
        result = json.dumps(job.to_dict())
        assert isinstance(result, str)


class TestJobStore:
    def test_create_job(self):
        s = JobStore()
        job = s.create_job("j1", "a1", {"key": "val"})
        assert job.job_id == "j1"
        assert s.get_job("j1") is job

    def test_get_job_missing(self):
        s = JobStore()
        assert s.get_job("nonexistent") is None

    def test_list_jobs_running_first(self):
        s = JobStore()
        j1 = s.create_job("j1", "a1")
        j2 = s.create_job("j2", "a2")
        j3 = s.create_job("j3", "a3")
        j1.mark_running()
        j1.mark_completed()
        j2.mark_running()
        # j3 stays queued
        listed = s.list_jobs()
        assert listed[0].job_id == "j2"  # running first

    def test_eviction_removes_oldest_completed(self):
        s = JobStore(max_jobs=5)
        for i in range(7):
            j = s.create_job(f"j{i}", f"a{i}")
            j.mark_running()
            j.mark_completed()
        assert len(s.list_jobs()) == 5

    def test_eviction_preserves_running(self):
        s = JobStore(max_jobs=3)
        j0 = s.create_job("j0", "a0")
        j0.mark_running()  # running — should not be evicted
        for i in range(1, 5):
            j = s.create_job(f"j{i}", f"a{i}")
            j.mark_running()
            j.mark_completed()
        # j0 is running, should still be there
        assert s.get_job("j0") is not None

    @pytest.mark.asyncio
    async def test_subscribe_receives_events(self):
        s = JobStore()
        q = s.subscribe()
        s.create_job("j1", "a1")
        event = q.get_nowait()
        assert event["type"] == "job_created"
        assert event["jobId"] == "j1"
        s.unsubscribe(q)

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_events(self):
        s = JobStore()
        q = s.subscribe()
        s.unsubscribe(q)
        s.create_job("j1", "a1")
        assert q.empty()

    @pytest.mark.asyncio
    async def test_notify_drops_on_full_queue(self):
        s = JobStore()
        q = s.subscribe()
        # Fill the queue
        for i in range(100):
            s.create_job(f"j{i}", f"a{i}")
        # Next event should not raise
        s.create_job("overflow", "a_overflow")
        s.unsubscribe(q)
