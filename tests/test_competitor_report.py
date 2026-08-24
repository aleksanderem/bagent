"""Tests for the top-level competitor report pipeline orchestration
(pipelines/competitor_report.py — chains Comp Etap 4 + Comp Etap 5).

Covers BEAUTY_AUDIT-dqir: competitor_reports.status='completed' must be
written AFTER Etap 5 (AI synthesis — positioning narrative, SWOT,
recommendations) has already returned, never before it and never instead
of it.

Before the fix, pipelines/competitor_analysis.py (Etap 4) wrote 'completed'
at its own Step 8, while Etap 5 (pipelines/competitor_synthesis.py) ran
strictly afterwards and never touched the status column at all (it only
writes report_data via update_competitor_report_data). A run whose
synthesis step crashed or was killed left the row permanently 'completed'
with an empty/partial report_data — the client saw a "ready" report with
no narrative. The fix moves the terminal write to this module, after
synthesize_competitor_insights succeeds; Etap 4 alone now leaves the row
'processing' (see tests/test_competitor_analysis.py::
test_compute_competitor_analysis_end_to_end_with_mocks for that half of
the contract).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from pipelines.competitor_report import run_competitor_report_pipeline


def _mock_supabase() -> AsyncMock:
    mock = AsyncMock()
    mock.update_competitor_report_status = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_completed_is_written_after_synthesis_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: status flips to 'completed' exactly once, and only AFTER
    synthesize_competitor_insights has already returned — never before
    (i.e. never as part of / immediately after compute_competitor_analysis).
    """
    call_order: list[str] = []

    async def _fake_compute(*args, **kwargs):  # noqa: ANN001, ANN002
        call_order.append("compute_competitor_analysis")
        return 42  # report_id

    async def _fake_synthesize(*args, **kwargs):  # noqa: ANN001, ANN002
        call_order.append("synthesize_competitor_insights")
        return {
            "narrative": "Testowy narratyw.",
            "swot_item_count": 4,
            "recommendation_count": 2,
            "used_fallback": False,
        }

    monkeypatch.setattr(
        "pipelines.competitor_report.compute_competitor_analysis", _fake_compute,
    )
    monkeypatch.setattr(
        "pipelines.competitor_report.synthesize_competitor_insights", _fake_synthesize,
    )

    mock_supabase = _mock_supabase()

    async def _status_spy(*args, **kwargs):  # noqa: ANN001, ANN002
        call_order.append(f"update_competitor_report_status:{args[1]}")

    mock_supabase.update_competitor_report_status.side_effect = _status_spy

    result = await run_competitor_report_pipeline(
        audit_id="test_audit",
        supabase=mock_supabase,
    )

    assert result["report_id"] == 42
    assert result["narrative"] == "Testowy narratyw."
    assert call_order == [
        "compute_competitor_analysis",
        "synthesize_competitor_insights",
        "update_competitor_report_status:completed",
    ], "'completed' must be written strictly after synthesis, never before it"

    mock_supabase.update_competitor_report_status.assert_called_once_with(
        42, "completed",
    )


@pytest.mark.asyncio
async def test_synthesis_failure_never_marks_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Etap 5 raises (MiniMax + OpenAI + deterministic fallback all
    failed, or the job got killed mid-synthesis), the exception propagates
    out of run_competitor_report_pipeline and update_competitor_report_status
    is NEVER called — so the row is never flipped to 'completed' by this
    layer, and is left 'processing'.

    This test pins the contract at THIS layer only: no premature/silent
    'completed' on a synthesis failure. Flipping the row to 'failed' is a
    separate, already-existing responsibility one layer up — see
    workers/tasks.py::run_competitor_report_task's `except Exception` /
    `except asyncio.CancelledError` branches and the guaranteed synchronous
    write in the `finally` block
    (SupabaseService.fail_competitor_report_by_audit_id, guarded on
    status='processing' so it can never clobber an already-completed
    report). That mechanism already handles ANY exception raised out of
    run_competitor_report_pipeline, generic or cancellation — it needed no
    change for this fix to work; it just needed the row to still be
    'processing' when synthesis fails, which is what this fix guarantees.
    """
    async def _fake_compute(*args, **kwargs):  # noqa: ANN001, ANN002
        return 42

    async def _fake_synthesize(*args, **kwargs):  # noqa: ANN001, ANN002
        raise RuntimeError("MiniMax + OpenAI + deterministic fallback all failed")

    monkeypatch.setattr(
        "pipelines.competitor_report.compute_competitor_analysis", _fake_compute,
    )
    monkeypatch.setattr(
        "pipelines.competitor_report.synthesize_competitor_insights", _fake_synthesize,
    )

    mock_supabase = _mock_supabase()

    with pytest.raises(RuntimeError, match="all failed"):
        await run_competitor_report_pipeline(
            audit_id="test_audit",
            supabase=mock_supabase,
        )

    mock_supabase.update_competitor_report_status.assert_not_called()


@pytest.mark.asyncio
async def test_compute_failure_never_marks_completed_and_synthesis_not_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Etap 4 itself raises (e.g. the pricing-gate from
    BEAUTY_AUDIT-xng), synthesis must never start and 'completed' must
    never be written. compute_competitor_analysis is responsible for its
    own 'failed' write in that case (unchanged by this fix — see
    pipelines/competitor_analysis.py's pricing gate)."""
    async def _fake_compute(*args, **kwargs):  # noqa: ANN001, ANN002
        raise RuntimeError("pricing stage persisted no rows in this run")

    synth_called = False

    async def _fake_synthesize(*args, **kwargs):  # noqa: ANN001, ANN002
        nonlocal synth_called
        synth_called = True
        raise AssertionError("synthesis must not run when Etap 4 raised")

    monkeypatch.setattr(
        "pipelines.competitor_report.compute_competitor_analysis", _fake_compute,
    )
    monkeypatch.setattr(
        "pipelines.competitor_report.synthesize_competitor_insights", _fake_synthesize,
    )

    mock_supabase = _mock_supabase()

    with pytest.raises(RuntimeError, match="pricing stage persisted no rows"):
        await run_competitor_report_pipeline(
            audit_id="test_audit",
            supabase=mock_supabase,
        )

    assert synth_called is False
    mock_supabase.update_competitor_report_status.assert_not_called()
