"""API endpoint tests for bagent FastAPI server."""

import os

# Set API_KEY before importing server so Settings picks it up
os.environ["API_KEY"] = "test-api-key-12345"

from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from server import app

client = TestClient(app)


def test_health_endpoint():
    """GET /api/health returns 200 OK."""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


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
