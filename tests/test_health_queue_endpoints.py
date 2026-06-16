"""Tests for the admin Health-tab bagent endpoints (Faza 2):
  - server._enqueue_pipeline stashes retry meta on the JobStore job
  - POST /api/jobs/{id}/retry re-enqueues a report-lane job (or 400)
  - GET /api/queues reports Redis-truth queue depth + worker liveness

Handlers are exercised directly (async) with a mocked arq pool, mirroring
test_report_queue_routing.py — no TestClient / api-key plumbing needed.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException


@pytest.mark.asyncio
async def test_enqueue_pipeline_stashes_retry_meta():
    import server

    server.store.create_job("job-retry-1", "audit-1", meta={"type": "report"})
    pool = MagicMock()
    pool.enqueue_job = AsyncMock(return_value=MagicMock())
    prev = getattr(server.app.state, "arq", None)
    server.app.state.arq = pool
    try:
        await server._enqueue_pipeline(
            task_name="run_report_task",
            job_id="job-retry-1",
            request_payload={"auditId": "audit-1", "userId": "u1"},
        )
    finally:
        server.app.state.arq = prev

    job = server.store.get_job("job-retry-1")
    assert job is not None
    assert job.meta["_retry"] == {
        "task_name": "run_report_task",
        "payload": {"auditId": "audit-1", "userId": "u1"},
    }


@pytest.mark.asyncio
async def test_retry_job_reenqueues_with_same_task_and_payload():
    import server

    server.store.create_job("job-old", "audit-9", meta={"type": "cennik"})
    job = server.store.get_job("job-old")
    job.meta["_retry"] = {
        "task_name": "run_cennik_task",
        "payload": {"auditId": "audit-9"},
    }

    pool = MagicMock()
    pool.enqueue_job = AsyncMock(return_value=MagicMock())
    prev = getattr(server.app.state, "arq", None)
    server.app.state.arq = pool
    try:
        result = await server.retry_job("job-old")
    finally:
        server.app.state.arq = prev

    assert result["retriedFrom"] == "job-old"
    assert result["status"] == "queued"
    assert result["jobId"] != "job-old"

    pool.enqueue_job.assert_awaited_once()
    args, kwargs = pool.enqueue_job.await_args
    assert args[0] == "run_cennik_task"
    assert kwargs["_queue_name"] == "arq:reports"


@pytest.mark.asyncio
async def test_retry_job_400_when_not_retryable():
    import server

    server.store.create_job("job-noretry", "audit-x", meta={"type": "report"})
    with pytest.raises(HTTPException) as exc:
        await server.retry_job("job-noretry")
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_retry_job_404_when_missing():
    import server

    with pytest.raises(HTTPException) as exc:
        await server.retry_job("does-not-exist")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_list_queues_reports_depth_and_worker_liveness():
    import server

    pool = MagicMock()
    pool.zcard = AsyncMock(side_effect=[7, 3])  # arq:queue, arq:reports
    pool.get = AsyncMock(side_effect=[b"healthy j_complete=10", None])
    pool.ping = AsyncMock(return_value=True)
    prev = getattr(server.app.state, "arq", None)
    server.app.state.arq = pool
    try:
        result = await server.list_queues()
    finally:
        server.app.state.arq = prev

    assert result["redisOk"] is True
    queues = {q["name"]: q for q in result["queues"]}
    assert queues["arq:queue"]["depth"] == 7
    assert queues["arq:queue"]["workerAlive"] is True
    assert "healthy" in queues["arq:queue"]["health"]
    assert queues["arq:reports"]["depth"] == 3
    assert queues["arq:reports"]["workerAlive"] is False
