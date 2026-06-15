"""Routing tests for the dedicated report worker (kierunek C).

User-facing report/LLM jobs (audit, free report, cennik, summary, versum,
competitor refresh) are routed off the scrape worker onto the "arq:reports"
queue so they no longer contend with the discovery pump.

The critical invariant: server._enqueue_pipeline must enqueue to the SAME queue
that ReportWorkerSettings consumes. A mismatch = jobs land in a queue no worker
drains (stuck forever) — exactly the failure this suite guards against.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


def test_enqueue_queue_matches_report_worker_queue():
    """The queue the API enqueues to == the queue the report worker consumes."""
    import server
    from workers.main import ReportWorkerSettings

    assert server.REPORT_QUEUE_NAME == ReportWorkerSettings.queue_name == "arq:reports"


def test_report_worker_registers_all_user_facing_tasks():
    """Report worker can execute every task routed to arq:reports."""
    from workers.main import ReportWorkerSettings
    from workers.tasks import ALL_TASKS

    names = {getattr(f, "__name__", "") for f in ReportWorkerSettings.functions}
    for t in ALL_TASKS:
        assert t.__name__ in names, f"{t.__name__} not registered on report worker"
    # The audit job specifically — the one that was queued behind the pump.
    assert "run_report_task" in names


def test_scrape_worker_on_default_queue_keeps_tasks_for_inflight_drain():
    """Scrape worker stays on arq:queue and keeps report tasks registered too, so
    in-flight jobs drain gracefully across a deploy (routing, not
    deregistration, is what isolates the load)."""
    from workers.main import WorkerSettings

    assert WorkerSettings.queue_name == "arq:queue"
    names = {getattr(f, "__name__", "") for f in WorkerSettings.functions}
    assert "run_report_task" in names


@pytest.mark.asyncio
async def test_enqueue_pipeline_routes_to_report_queue():
    """_enqueue_pipeline passes _queue_name='arq:reports' (+ the chosen job_id)."""
    import server

    pool = MagicMock()
    pool.enqueue_job = AsyncMock(return_value=MagicMock())  # non-None == success
    prev = getattr(server.app.state, "arq", None)
    server.app.state.arq = pool
    try:
        await server._enqueue_pipeline(
            task_name="run_report_task",
            job_id="job-123",
            request_payload={"auditId": "a1", "userId": "u1"},
        )
    finally:
        server.app.state.arq = prev

    pool.enqueue_job.assert_awaited_once()
    args, kwargs = pool.enqueue_job.await_args
    assert args[0] == "run_report_task"
    assert kwargs["_job_id"] == "job-123"
    assert kwargs["_queue_name"] == "arq:reports"
