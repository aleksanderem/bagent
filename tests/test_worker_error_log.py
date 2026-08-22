"""bagent_error_log central writer (BEAUTY_AUDIT-n3zw).

The table was empty since creation because no code path wrote to it on a
real job failure. These tests pin the contract: a wrapped arq task that
raises MUST produce exactly one insert into `bagent_error_log`, the
exception MUST propagate unchanged, and a broken logger MUST NOT mask the
original error. Zero live DB — the Supabase client is injected.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from workers import error_log
from workers.error_log import (
    build_error_row,
    classify_error,
    log_task_errors,
    wrap_all,
)


@pytest.fixture
def captured_client():
    rows: list[dict] = []
    client = MagicMock()

    def _insert(row):  # noqa: ANN001
        rows.append(dict(row))
        return MagicMock()

    client.table.return_value.insert.side_effect = _insert
    error_log.set_client_factory(lambda: client)
    yield client, rows
    error_log.set_client_factory(None)


CTX = {"job_id": "job-123", "job_try": 2, "redis": MagicMock()}
REQUEST = {"auditId": "k97abc", "userId": "user_1", "sourceUrl": "https://booksy.com/x"}


async def test_failed_task_writes_one_row_and_reraises(captured_client):
    client, rows = captured_client

    @log_task_errors
    async def run_report_task(ctx, request):  # noqa: ANN001
        raise RuntimeError("MiniMax exploded")

    with pytest.raises(RuntimeError, match="MiniMax exploded"):
        await run_report_task(CTX, REQUEST)

    client.table.assert_called_once_with("bagent_error_log")
    assert len(rows) == 1
    row = rows[0]
    assert row["convex_audit_id"] == "k97abc"
    assert row["convex_user_id"] == "user_1"
    assert row["bagent_pipeline"] == "report"
    assert row["attempt_number"] == 2
    assert row["error_message"].startswith("RuntimeError: MiniMax exploded")
    assert "RuntimeError" in row["stack_trace"]
    assert row["request_payload"]["task"] == "run_report_task"
    assert row["request_payload"]["job_id"] == "job-123"
    assert row["request_payload"]["args"][0]["auditId"] == "k97abc"


async def test_success_writes_nothing(captured_client):
    client, rows = captured_client

    @log_task_errors
    async def ok(ctx, request):  # noqa: ANN001
        return {"fine": True}

    assert await ok(CTX, REQUEST) == {"fine": True}
    assert rows == []
    client.table.assert_not_called()


async def test_user_cancel_is_not_an_error(captured_client):
    _, rows = captured_client

    class _CancelledByUser(Exception):
        pass

    @log_task_errors
    async def run_cennik_task(ctx, request):  # noqa: ANN001
        raise _CancelledByUser("cancel flag set")

    with pytest.raises(_CancelledByUser):
        await run_cennik_task(CTX, REQUEST)
    assert rows == []


async def test_broken_logger_does_not_mask_original_error():
    client = MagicMock()
    client.table.return_value.insert.return_value.execute.side_effect = Exception("RLS denied")
    error_log.set_client_factory(lambda: client)
    try:

        @log_task_errors
        async def run_summary_task(ctx, request):  # noqa: ANN001
            raise ValueError("original")

        with pytest.raises(ValueError, match="original"):
            await run_summary_task(CTX, REQUEST)
    finally:
        error_log.set_client_factory(None)


def test_wrap_all_keeps_arq_registration_names():
    async def drain_scrape_queue(ctx):  # noqa: ANN001
        return None

    [wrapped] = wrap_all([drain_scrape_queue])
    assert wrapped.__qualname__ == drain_scrape_queue.__qualname__
    assert wrapped.__name__ == "drain_scrape_queue"


def test_row_without_request_dict_uses_placeholder_and_task_name():
    row = build_error_row("drain_scrape_queue", RuntimeError("boom"), ctx={"job_try": 7})
    assert row["convex_audit_id"] == "(none)"
    assert row["bagent_pipeline"] == "drain_scrape_queue"
    assert row["attempt_number"] == 3  # clamped to CHECK range 1..3
    assert row["request_payload"]["task"] == "drain_scrape_queue"


@pytest.mark.parametrize(
    "exc, expected",
    [
        (TimeoutError("t"), "timeout"),
        (ValueError("v"), "validation_error"),
        (RuntimeError("r"), "unknown"),
    ],
)
def test_classify_error(exc, expected):
    assert classify_error(exc) == expected


# --- FAIL fixes (weryfikator 2026-08-22): crons, CancelledError, write timeout ---


def test_wrap_crons_wraps_both_registration_styles():
    """arq registers cron_jobs separately from `functions`; both the string
    path style and the direct coroutine style must end up wrapped, with the
    cron name and schedule untouched."""
    from arq import cron

    from workers.error_log import wrap_crons

    async def direct_cron(ctx):  # noqa: ANN001
        return None

    crons = [
        cron("workers.main.report_worker_heartbeat", minute={0, 30}),
        cron(direct_cron, minute={5}, name="cron:direct"),
    ]
    wrapped = wrap_crons(crons)

    assert [c.name for c in wrapped] == [
        "cron:workers.main.report_worker_heartbeat",
        "cron:direct",
    ]
    assert all(hasattr(c.coroutine, "__wrapped__") for c in wrapped)
    assert wrapped[0].minute == {0, 30} and wrapped[1].minute == {5}
    assert wrapped[1].coroutine.__wrapped__ is direct_cron
    # originals not mutated
    assert not hasattr(crons[1].coroutine, "__wrapped__")


def test_worker_settings_crons_are_all_wrapped():
    from workers.main import ReportWorkerSettings, WorkerSettings

    for settings in (WorkerSettings, ReportWorkerSettings):
        assert settings.cron_jobs, "arq cron import must succeed in tests"
        assert all(hasattr(c.coroutine, "__wrapped__") for c in settings.cron_jobs)


async def test_wrapped_cron_failure_writes_row(captured_client):
    from arq import cron

    from workers.error_log import wrap_crons

    async def reap_stuck_jobs(ctx):  # noqa: ANN001
        raise RuntimeError("reaper boom")

    [cj] = wrap_crons([cron(reap_stuck_jobs, minute={2})])
    _, rows = captured_client
    with pytest.raises(RuntimeError, match="reaper boom"):
        await cj.coroutine(CTX)
    assert len(rows) == 1
    assert rows[0]["bagent_pipeline"] == "reap_stuck_jobs"
    assert rows[0]["convex_audit_id"] == "(none)"


async def test_cancelled_error_logged_as_timeout_and_reraised(captured_client):
    """arq implements job_timeout with asyncio.wait_for -> the task body sees
    asyncio.CancelledError (a BaseException). It must be recorded as
    error_type=timeout and propagate unchanged."""
    import asyncio

    _, rows = captured_client

    @log_task_errors
    async def run_report_task(ctx, request):  # noqa: ANN001
        await asyncio.sleep(60)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(run_report_task(CTX, REQUEST), timeout=0.01)

    assert len(rows) == 1
    assert rows[0]["error_type"] == "timeout"
    assert rows[0]["error_message"].startswith("CancelledError")


async def test_direct_cancelled_error_is_reraised_not_swallowed(captured_client):
    import asyncio

    _, rows = captured_client

    @log_task_errors
    async def run_cennik_task(ctx, request):  # noqa: ANN001
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await run_cennik_task(CTX, REQUEST)
    assert rows[0]["error_type"] == "timeout"


async def test_hanging_supabase_write_does_not_block_job(monkeypatch):
    """A stuck insert must be abandoned after WRITE_TIMEOUT_S; the original
    exception still propagates and the job finishes promptly."""
    import asyncio
    import threading
    import time

    release = threading.Event()

    class _HangingExec:
        def execute(self):
            release.wait(timeout=10)  # hangs far longer than WRITE_TIMEOUT_S

    client = MagicMock()
    client.table.return_value.insert.return_value = _HangingExec()
    error_log.set_client_factory(lambda: client)
    monkeypatch.setattr(error_log, "WRITE_TIMEOUT_S", 0.05)
    try:

        @log_task_errors
        async def run_summary_task(ctx, request):  # noqa: ANN001
            raise ValueError("original")

        start = time.monotonic()
        with pytest.raises(ValueError, match="original"):
            await run_summary_task(CTX, REQUEST)
        assert time.monotonic() - start < 1.0
    finally:
        release.set()
        error_log.set_client_factory(None)


def test_log_task_errors_is_idempotent():
    async def t(ctx):  # noqa: ANN001
        return None

    once = log_task_errors(t)
    assert log_task_errors(once) is once
