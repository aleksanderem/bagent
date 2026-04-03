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


def test_analyze_returns_202(sample_scraped_data):
    """POST /api/analyze with valid data returns 202 Accepted with jobId."""
    with patch("server.run_analysis_job", new_callable=AsyncMock):
        response = client.post(
            "/api/analyze",
            json={"auditId": "test-audit-1", "userId": "test-user-1", "scrapedData": sample_scraped_data},
            headers={"x-api-key": "test-api-key-12345"},
        )
    assert response.status_code == 202
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "accepted"


def test_analyze_requires_api_key(sample_scraped_data):
    """POST /api/analyze without x-api-key returns 422."""
    response = client.post(
        "/api/analyze",
        json={"auditId": "test-audit-1", "userId": "test-user-1", "scrapedData": sample_scraped_data},
    )
    assert response.status_code == 422


def test_analyze_validates_input():
    """POST /api/analyze with missing fields returns 422."""
    response = client.post(
        "/api/analyze",
        json={"auditId": "test-audit-1"},  # Missing userId and scrapedData
        headers={"x-api-key": "test-api-key-12345"},
    )
    assert response.status_code == 422


def test_list_jobs(sample_scraped_data):
    """GET /api/jobs returns list including created jobs."""
    with patch("server.run_analysis_job", new_callable=AsyncMock):
        client.post(
            "/api/analyze",
            json={"auditId": "list-test", "userId": "u1", "scrapedData": sample_scraped_data},
            headers={"x-api-key": "test-api-key-12345"},
        )
    response = client.get("/api/jobs")
    assert response.status_code == 200
    jobs = response.json()
    assert any(j["auditId"] == "list-test" for j in jobs)


def test_get_job_logs(sample_scraped_data):
    """GET /api/jobs/{id}/logs returns job with logs array."""
    with patch("server.run_analysis_job", new_callable=AsyncMock):
        resp = client.post(
            "/api/analyze",
            json={"auditId": "logs-test", "userId": "u1", "scrapedData": sample_scraped_data},
            headers={"x-api-key": "test-api-key-12345"},
        )
    job_id = resp.json()["jobId"]
    response = client.get(f"/api/jobs/{job_id}/logs")
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data


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
