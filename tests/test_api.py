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
