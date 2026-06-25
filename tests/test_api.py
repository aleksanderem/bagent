"""API endpoint tests for bagent FastAPI server."""

import os

# Set API_KEY before importing server so Settings picks it up
os.environ["API_KEY"] = "test-api-key-12345"

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from server import app

client = TestClient(app)


def test_health_endpoint():
    """GET /api/health returns 200 OK with job counts."""
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "jobs_running" in data
    assert "jobs_total" in data


# Legacy /api/analyze endpoint tests removed in 3-BAGENT migration.
# The report/cennik/summary endpoints are tested separately in
# tests/test_3bagent_endpoints.py (if exists) or via E2E smoke tests.


def test_get_job_not_found():
    """GET /api/jobs/nonexistent returns 404."""
    response = client.get("/api/jobs/nonexistent-id")
    assert response.status_code == 404


def test_dashboard_serves_html():
    """GET /dashboard returns HTML page."""
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "bagent Monitor" in response.text


def test_sse_endpoint_exists():
    """The /api/events route is registered."""
    routes = [r.path for r in app.routes]
    assert "/api/events" in routes


# --- competitor report → queue (beads BEAUTY_AUDIT-1mb) ---


def _make_enqueue_sb(returned_id):
    """Mock supabase client whose enqueue RPC returns `returned_id`.

    Chain: make_supabase_client(...).rpc(name, params).execute().data
    """
    from unittest.mock import MagicMock

    execute_result = MagicMock()
    execute_result.data = returned_id
    rpc_chain = MagicMock()
    rpc_chain.execute = MagicMock(return_value=execute_result)
    sb = MagicMock()
    sb.rpc = MagicMock(return_value=rpc_chain)
    return sb


def test_competitor_report_enqueues_into_queue_not_arq():
    """POST /api/competitor/report inserts into the queue via RPC and does NOT
    enqueue directly to arq. jobId == the arq_job_id passed to the queue."""
    from unittest.mock import MagicMock

    from config import settings  # read the live api_key (order-independent)

    sb = _make_enqueue_sb(99)  # queue id
    arq_mock = MagicMock()
    arq_mock.enqueue_job = AsyncMock()

    with (
        patch("services.sb_client.make_supabase_client", return_value=sb),
        patch.object(app.state, "arq", arq_mock, create=True),
    ):
        response = client.post(
            "/api/competitor/report",
            json={"auditId": "a", "userId": "u"},
            headers={"x-api-key": settings.api_key},
        )

    assert response.status_code == 202
    body = response.json()
    assert body["jobId"]

    sb.rpc.assert_called_once()
    name, params = sb.rpc.call_args.args
    assert name == "enqueue_competitor_report"
    assert params["p_audit_id"] == "a"
    assert params["p_user_id"] == "u"
    assert params["p_arq_job_id"] == body["jobId"]
    assert "p_tier" in params
    assert "p_selection_mode" in params
    assert "p_target_count" in params
    # Webhook callback routing (migration 148): absent in the request body →
    # None forwarded to the RPC → queue row stores NULL → worker falls back to
    # the global settings.convex_url.
    assert params["p_convex_site_url"] is None

    # The report path must NOT enqueue directly to arq anymore.
    arq_mock.enqueue_job.assert_not_awaited()


def test_competitor_report_forwards_convex_site_url():
    """The originating Convex deployment's .convex.site URL (migration 148) is
    threaded into the enqueue RPC so the worker can webhook back to the SAME
    deployment that started the job (dev vs prod), not the global CONVEX_URL."""
    from config import settings

    sb = _make_enqueue_sb(101)

    with patch("services.sb_client.make_supabase_client", return_value=sb):
        response = client.post(
            "/api/competitor/report",
            json={
                "auditId": "a",
                "userId": "u",
                "convexSiteUrl": "https://reliable-scorpion-10.convex.site",
            },
            headers={"x-api-key": settings.api_key},
        )

    assert response.status_code == 202
    _, params = sb.rpc.call_args.args
    assert (
        params["p_convex_site_url"]
        == "https://reliable-scorpion-10.convex.site"
    )


def test_competitor_report_dedup_returns_already_queued():
    """When the enqueue RPC returns data=None (active row exists), the endpoint
    signals dedup with status='already_queued'."""
    from config import settings  # read the live api_key (order-independent)

    sb = _make_enqueue_sb(None)  # dedup guard fired

    with patch("services.sb_client.make_supabase_client", return_value=sb):
        response = client.post(
            "/api/competitor/report",
            json={"auditId": "a", "userId": "u"},
            headers={"x-api-key": settings.api_key},
        )

    assert response.status_code == 202
    assert response.json()["status"] == "already_queued"
