"""Unhandled FastAPI route exceptions land in bagent_error_log (BEAUTY_AUDIT-mol-cdi).

`log_task_errors` only wraps arq tasks, so it runs in the worker processes.
The uvicorn process serving these routes is a different pm2 process and had no
writer at all — a route that blew up produced a bare 500 and nothing in
Supabase. These tests pin the HTTP half of the contract:

  * a route raising ValueError → 500 AND exactly one row whose
    `bagent_pipeline` is `http:` + the route PATTERN (never the concrete id,
    which would explode into one pipeline value per request),
  * an HTTPException(400) → 400 and zero rows (it is handled by
    ExceptionMiddleware and never reaches our handler),
  * a Supabase insert that dies → the route still answers 500,
  * an exception already recorded deeper in the stack → not counted twice.

Zero live DB: the Supabase client is injected via `error_log.set_client_factory`,
exactly as tests/test_worker_error_log.py does.

`raise_server_exceptions=False` is required: ServerErrorMiddleware re-raises
after our handler returns, and the default TestClient would surface that
exception to the test instead of the 500 a real HTTP client receives.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

# Match tests/test_api.py — Settings reads API_KEY at server import time.
os.environ.setdefault("API_KEY", "test-api-key-12345")

from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from server import app  # noqa: E402
from workers import error_log  # noqa: E402

BOOM_PATTERN = "/__errlog_test/boom/{thing_id}"
HTTP_400_PATTERN = "/__errlog_test/client_error/{thing_id}"


@pytest.fixture
def captured_client():
    """Injected Supabase client that records inserted rows instead of writing."""
    rows: list[dict] = []
    client = MagicMock()

    def _insert(row):  # noqa: ANN001
        rows.append(dict(row))
        return MagicMock()

    client.table.return_value.insert.side_effect = _insert
    error_log.set_client_factory(lambda: client)
    yield client, rows
    error_log.set_client_factory(None)


@pytest.fixture
def temp_routes():
    """Register throwaway routes on the REAL app, then remove them.

    Testing the real `server.app` is the point — it proves the handler is
    actually registered there and that FastAPI puts the APIRoute in the scope.
    """
    added = []

    def _add(path, endpoint):  # noqa: ANN001
        app.add_api_route(path, endpoint, methods=["GET"])
        added.append(app.router.routes[-1])

    yield _add

    for route in added:
        app.router.routes.remove(route)


@pytest.fixture
def http_client():
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


async def _raise_value_error(thing_id: str):
    raise ValueError(f"boom for {thing_id}")


async def _raise_http_400(thing_id: str):
    raise HTTPException(status_code=400, detail="nie ten identyfikator")


def test_unhandled_route_error_returns_500_and_logs_route_pattern(
    captured_client, temp_routes, http_client
):
    """A route raising ValueError answers 500 and writes exactly one row whose
    bagent_pipeline is `http:` + the route pattern, not the concrete URL."""
    _, rows = captured_client
    temp_routes(BOOM_PATTERN, _raise_value_error)

    response = http_client.get("/__errlog_test/boom/12345")

    assert response.status_code == 500

    assert len(rows) == 1, f"expected exactly one bagent_error_log row, got {rows}"
    row = rows[0]
    assert row["bagent_pipeline"].startswith("http:")
    # The PATTERN, so /boom/12345 and /boom/67890 share one pipeline value.
    assert row["bagent_pipeline"] == f"http:{BOOM_PATTERN}"
    assert "12345" not in row["bagent_pipeline"]
    assert row["error_type"] == "validation_error"
    assert row["error_message"].startswith("ValueError: boom for 12345")
    assert "ValueError" in row["stack_trace"]
    # The concrete URL is still recoverable from the payload, just not from
    # the grouping key.
    assert row["request_payload"]["kwargs"]["path"] == "/__errlog_test/boom/12345"
    assert row["request_payload"]["kwargs"]["method"] == "GET"


def test_two_ids_on_one_route_share_a_single_pipeline_value(
    captured_client, temp_routes, http_client
):
    """The whole reason for using the pattern: N requests to N ids must not
    create N distinct bagent_pipeline values."""
    _, rows = captured_client
    temp_routes(BOOM_PATTERN, _raise_value_error)

    http_client.get("/__errlog_test/boom/aaa")
    http_client.get("/__errlog_test/boom/bbb")

    assert len(rows) == 2
    assert {r["bagent_pipeline"] for r in rows} == {f"http:{BOOM_PATTERN}"}


def test_http_exception_400_is_not_logged(captured_client, temp_routes, http_client):
    """HTTPException is handled by ExceptionMiddleware and never reaches the
    Exception handler, so 4xx needs no filtering and writes nothing."""
    client, rows = captured_client
    temp_routes(HTTP_400_PATTERN, _raise_http_400)

    response = http_client.get("/__errlog_test/client_error/12345")

    assert response.status_code == 400
    assert response.json()["detail"] == "nie ten identyfikator"
    assert rows == []
    client.table.assert_not_called()


def test_supabase_write_failure_still_returns_500(temp_routes, http_client):
    """A dead Supabase insert must not blow up the error handler — the route
    still answers 500 instead of the connection dropping."""
    client = MagicMock()
    client.table.return_value.insert.return_value.execute.side_effect = Exception(
        "RLS denied"
    )
    error_log.set_client_factory(lambda: client)
    try:
        temp_routes(BOOM_PATTERN, _raise_value_error)
        response = http_client.get("/__errlog_test/boom/12345")
        assert response.status_code == 500
    finally:
        error_log.set_client_factory(None)


def test_record_task_error_itself_raising_still_returns_500(
    monkeypatch, temp_routes, http_client
):
    """Belt and braces: even if the writer raises before touching Supabase,
    the handler's own guard keeps the 500 response intact."""

    async def _explode(*_args, **_kwargs):
        raise RuntimeError("logger itself is broken")

    monkeypatch.setattr(error_log, "record_task_error", _explode)

    temp_routes(BOOM_PATTERN, _raise_value_error)
    response = http_client.get("/__errlog_test/boom/12345")

    assert response.status_code == 500


def test_already_logged_exception_is_not_written_twice(
    captured_client, temp_routes, http_client
):
    """The arq wrapper marks exceptions it recorded; the HTTP handler must
    honour that mark rather than inserting a duplicate row."""
    _, rows = captured_client

    async def _raise_pre_logged(thing_id: str):  # noqa: ANN001
        exc = ValueError("already recorded downstream")
        exc._bagent_error_logged = True
        raise exc

    temp_routes(BOOM_PATTERN, _raise_pre_logged)

    response = http_client.get("/__errlog_test/boom/12345")

    assert response.status_code == 500
    assert rows == []


async def test_record_task_error_marks_the_exception_on_success(captured_client):
    """Other half of the anti-duplicate contract: a successful write stamps the
    exception so a later handler skips it."""
    from workers.error_log import record_task_error

    _, rows = captured_client
    exc = RuntimeError("boom")

    assert await record_task_error("run_report_task", exc) is True
    assert len(rows) == 1
    assert getattr(exc, "_bagent_error_logged", False) is True
