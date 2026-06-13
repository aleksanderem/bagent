"""job_id threading tests (quick 260613-m23 Task 3, P1).

job_id is injected CENTRALLY in TraceWriter.add — every row carries it without
touching the hundreds of tracer.add callsites. `pipeline_traces` (mig 094 +
121) has NO `job_id` column, so the id rides inside trace_data as `_job_id`
(locked-design forbids applying a migration). The threading path is:
  ctx['job_id'] → run_competitor_report_task → run_competitor_report_pipeline
  → compute_competitor_analysis + synthesize_competitor_insights → TraceWriter

Backward compatible: every new param defaults to "unknown"/None, so existing
callsites keep working.

Zero live DB — supabase client / insert mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.pipeline_trace import TraceWriter


def _mock_client_capturing_insert() -> tuple[MagicMock, list]:
    """Return (client, captured_rows). client.table(...).insert(rows).execute()
    appends the rows list to captured_rows."""
    captured: list = []
    client = MagicMock()

    def _insert(rows):  # noqa: ANN001
        # Capture a COPY — TraceWriter.flush() clears self._buffer (the same
        # list object) right after inserting, so a reference would read empty.
        captured.append([dict(r) for r in rows])
        exec_mock = MagicMock()
        exec_mock.execute.return_value = MagicMock()
        return exec_mock

    client.table.return_value.insert.side_effect = _insert
    return client, captured


# ---------------------------------------------------------------------------
# Test 1 — TraceWriter injects job_id into every row (trace_data._job_id)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracewriter_injects_job_id_into_trace_data() -> None:
    client, captured = _mock_client_capturing_insert()
    writer = TraceWriter(
        client=client, audit_id="a", report_id=1,
        pipeline="competitor_analysis", job_id="job-xyz",
    )
    writer.add("step.one", {"k": "v"})
    writer.add("step.two", {"n": 2})
    await writer.flush()

    assert len(captured) == 1
    rows = captured[0]
    assert len(rows) == 2
    for row in rows:
        # No job_id column exists → it rides inside trace_data.
        assert "job_id" not in row
        assert row["trace_data"]["_job_id"] == "job-xyz"
    # Original payload keys preserved.
    assert rows[0]["trace_data"]["k"] == "v"
    assert rows[1]["trace_data"]["n"] == 2


@pytest.mark.asyncio
async def test_tracewriter_does_not_mutate_caller_data() -> None:
    """Immutability: add() must copy data before injecting _job_id, never
    mutate the caller's dict."""
    client, captured = _mock_client_capturing_insert()
    writer = TraceWriter(
        client=client, audit_id="a", report_id=1,
        pipeline="p", job_id="job-1",
    )
    original = {"k": "v"}
    writer.add("step", original)
    assert "_job_id" not in original  # caller dict untouched


# ---------------------------------------------------------------------------
# Test 2 — backward compatible: no job_id → no _job_id injected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracewriter_backward_compat_no_job_id() -> None:
    client, captured = _mock_client_capturing_insert()
    writer = TraceWriter(
        client=client, audit_id="a", report_id=1, pipeline="p",
    )
    writer.add("step", {"k": "v"})
    await writer.flush()

    rows = captured[0]
    assert "job_id" not in rows[0]
    assert "_job_id" not in rows[0]["trace_data"]
    assert rows[0]["trace_data"] == {"k": "v"}


# ---------------------------------------------------------------------------
# Test 3 — compute_competitor_analysis passes job_id to TraceWriter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compute_passes_job_id_to_tracewriter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipelines.competitor_analysis as ca

    captured_job_ids: list = []

    real_init = TraceWriter.__init__

    def _spy_init(self, *args, **kwargs):  # noqa: ANN002, ANN003
        captured_job_ids.append(kwargs.get("job_id"))
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(ca.TraceWriter, "__init__", _spy_init)

    # Make selection raise immediately so we don't run the whole pipeline —
    # the TraceWriter is constructed BEFORE selection, so job_id is captured.
    async def _boom(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("stop after tracer construction")

    monkeypatch.setattr(ca, "select_competitors", _boom)

    mock = AsyncMock()
    mock.client = MagicMock()

    with pytest.raises(RuntimeError, match="stop after tracer"):
        await ca.compute_competitor_analysis(
            audit_id="a", job_id="job-1", supabase=mock,
        )

    assert "job-1" in captured_job_ids


# ---------------------------------------------------------------------------
# Test 4 — run_competitor_report_pipeline threads job_id to both stages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_threads_job_id_to_compute_and_synthesis() -> None:
    import pipelines.competitor_report as cr

    mock_compute = AsyncMock(return_value=42)
    mock_synth = AsyncMock(return_value={
        "narrative": "n", "swot_item_count": 0,
        "recommendation_count": 0, "used_fallback": False,
    })

    with patch.object(cr, "compute_competitor_analysis", mock_compute), \
         patch.object(cr, "synthesize_competitor_insights", mock_synth):
        await cr.run_competitor_report_pipeline(
            audit_id="a", job_id="job-2", supabase=MagicMock(),
        )

    assert mock_compute.await_args.kwargs["job_id"] == "job-2"
    assert mock_synth.await_args.kwargs["job_id"] == "job-2"


# ---------------------------------------------------------------------------
# Test 6 — synthesize_competitor_insights logs with [job_id] prefix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_synthesis_logs_job_id_prefix(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    import pipelines.competitor_synthesis as cs

    report = {
        "id": 27, "convex_audit_id": "audit_j", "subject_salon_id": 500,
        "tier": "base", "selection_mode": "auto", "report_data": {},
        "status": "processing",
    }
    mock = MagicMock()
    mock.get_competitor_report_by_id = AsyncMock(return_value=report)
    mock.get_competitor_matches = AsyncMock(return_value=[])
    mock.get_competitor_pricing_comparisons = AsyncMock(return_value=[])
    mock.get_competitor_service_gaps = AsyncMock(return_value=[])
    mock.get_competitor_dimensional_scores = AsyncMock(return_value=[])
    mock.get_subject_salon_context = AsyncMock(return_value={
        "salon_id": 500, "salon_name": "Beauty4ever", "salon_city": "Warszawa",
        "primary_category_name": "X", "reviews_count": 10, "reviews_rank": 4.0,
        "total_services": 10,
    })
    mock.update_competitor_report_data = AsyncMock(return_value=None)
    mock.delete_competitor_recommendations = AsyncMock(return_value=None)
    mock.insert_competitor_recommendations = AsyncMock(return_value=0)

    canned = {
        "positioning_narrative": "Beauty4ever " + "x" * 60,
        "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
        "recommendations": [],
    }

    with caplog.at_level(logging.INFO, logger="pipelines.competitor_synthesis"), \
         patch.object(cs, "_run_minimax_synthesis", new=AsyncMock(return_value=canned)):
        await cs.synthesize_competitor_insights(
            report_id=27, supabase=mock, job_id="job-3",
        )

    assert any("[job-3]" in r.getMessage() for r in caplog.records), (
        "expected at least one synthesis log line to carry the [job-3] prefix"
    )
