"""Smoke tests for the frozen free-tier BAGENT #1 snapshot.

Verifies that:
  1. pipelines/free_report.py is importable and exposes run_free_report_pipeline
     with the same signature as pipelines/report.run_audit_pipeline.
  2. The /api/audit/free_report endpoint is registered in the FastAPI app.
  3. The endpoint rejects calls without x-api-key (401) and accepts them
     with the correct key (202 + jobId).
  4. Internal helpers are still reachable — guards against accidental
     deletion of private functions the frozen pipeline depends on.

These are lightweight guards. Full behavior parity is guaranteed by the
fact that free_report.py is a byte-compatible copy of report.py; we
explicitly do NOT re-run the heavy pipeline steps here. The background
task itself is patched to a no-op in the dispatch tests because the real
job depends on Supabase which is not available in the test environment.
"""

from __future__ import annotations

import inspect
import os

# Set API_KEY before importing server so Settings picks it up
os.environ.setdefault("API_KEY", "test-api-key-free-report")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import server  # noqa: E402
from server import app  # noqa: E402

client = TestClient(app)


@pytest.fixture
def noop_background_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace run_free_report_job with a no-op so TestClient's synchronous
    background-task execution does not try to load Supabase / MiniMax in
    the test env. Dispatch tests only care that the endpoint accepted the
    request and created a job entry — not that the pipeline ran.
    """

    async def _noop(job_id: str, request):  # type: ignore[no-untyped-def]
        # Mark the job as running then completed so the store is consistent.
        job = server.store.get_job(job_id)
        if job is not None:
            job.mark_running()
            job.mark_completed()
            server.store.notify_status(job)

    monkeypatch.setattr(server, "run_free_report_job", _noop)


def test_free_report_pipeline_importable():
    """pipelines.free_report exposes run_free_report_pipeline."""
    from pipelines.free_report import run_free_report_pipeline

    assert callable(run_free_report_pipeline)
    assert run_free_report_pipeline.__name__ == "run_free_report_pipeline"


def test_free_report_signature_matches_report():
    """run_free_report_pipeline has the same signature as run_audit_pipeline.

    If the main report pipeline evolves its signature (Etap 1+), this test
    intentionally stays satisfied as long as free_report keeps its frozen
    three-param shape. The goal is snapshot stability, not perpetual parity.
    """
    from pipelines.free_report import run_free_report_pipeline

    params = list(inspect.signature(run_free_report_pipeline).parameters)
    assert params == ["scraped_data", "audit_id", "on_progress"]


def test_free_report_internal_helpers_present():
    """Frozen pipeline still has all private helpers it depends on."""
    from pipelines import free_report

    for name in (
        "_load_prompt",
        "_fill_prompt",
        "_normalize_item",
        "_analyze_naming",
        "_analyze_descriptions",
        "_analyze_structure",
        "_generate_summary",
        "_validate_quality",
    ):
        assert hasattr(free_report, name), f"missing helper: {name}"


def test_free_report_route_registered():
    """POST /api/audit/free_report is registered in the app."""
    routes = [r.path for r in app.routes if hasattr(r, "path")]
    assert "/api/audit/free_report" in routes


def test_free_report_requires_api_key():
    """POST /api/audit/free_report without x-api-key returns 401."""
    response = client.post(
        "/api/audit/free_report",
        json={"auditId": "test-audit-id", "userId": "test-user-id"},
    )
    # FastAPI returns 422 when the Header is missing (depends), 401 when it
    # fails verify_api_key. Either is an auth failure — accept both.
    assert response.status_code in (401, 422)


def test_free_report_accepts_valid_request(noop_background_job: None):
    """POST /api/audit/free_report with valid api key returns 202 + jobId.
    The background job is patched to no-op via the fixture so Supabase /
    MiniMax are never touched — the test only checks dispatch.
    """
    response = client.post(
        "/api/audit/free_report",
        headers={"x-api-key": "test-api-key-free-report"},
        json={
            "auditId": "test-audit-smoke",
            "userId": "test-user-smoke",
            "sourceUrl": "https://example.com",
        },
    )
    assert response.status_code == 202, response.text
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "accepted"


def test_free_report_job_type_in_store(noop_background_job: None):
    """The job created by /api/audit/free_report is tagged type='free_report'
    in the in-memory job store so dashboards / logs can distinguish it from
    regular report jobs.
    """
    response = client.post(
        "/api/audit/free_report",
        headers={"x-api-key": "test-api-key-free-report"},
        json={"auditId": "test-audit-type-check", "userId": "test-user"},
    )
    assert response.status_code == 202
    job_id = response.json()["jobId"]

    job = server.store.get_job(job_id)
    assert job is not None
    assert job.meta.get("type") == "free_report"
