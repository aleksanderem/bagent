"""Rozróżnienie: CancelledError z restartu/abortu vs prawdziwy timeout arq."""

from __future__ import annotations

import asyncio

from workers.error_log import DEFAULT_JOB_TIMEOUT_S, build_error_row, cancellation_note


def test_cancellation_note_distinguishes_interruption_from_timeout():
    exc = asyncio.CancelledError()
    assert cancellation_note(exc, 12.4) == "przerwane po 12 s (restart/abort workera)"
    assert cancellation_note(exc, DEFAULT_JOB_TIMEOUT_S - 1) == f"timeout po {DEFAULT_JOB_TIMEOUT_S - 1} s"
    assert cancellation_note(RuntimeError("x"), 5) is None


def test_build_error_row_uses_note_as_message():
    row = build_error_row("discovery_pump_step", asyncio.CancelledError(), ctx={"job_try": 1}, note="przerwane po 3 s (restart/abort workera)")
    assert row["error_type"] == "timeout", "CHECK w mig 014 — typ zostaje"
    assert row["error_message"] == "CancelledError: przerwane po 3 s (restart/abort workera)"
    plain = build_error_row("x", RuntimeError("boom"), ctx={})
    assert plain["error_message"] == "RuntimeError: boom"
