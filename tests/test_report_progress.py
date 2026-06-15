"""Unit tests for report.py progress helpers.

Covers the two fixes for the "audit hangs at 40%" symptom:
  * _make_monotonic_progress — concurrent naming/description phases would emit a
    regressing percent (56→38); the wrapper clamps it so the frontend bar never
    jumps backward, while always forwarding the message.
  * _make_phase2_step_emitter — a shared per-step counter that creeps the bar
    lo→hi during the long agent loops so it no longer freezes mid-pipeline.
"""

import pytest

from pipelines.report import _make_monotonic_progress, _make_phase2_step_emitter


@pytest.mark.asyncio
async def test_monotonic_progress_never_decreases():
    """Regressing percents are clamped to the last max; messages pass through."""
    seen: list[tuple[int, str]] = []

    async def base(pct: int, msg: str) -> None:
        seen.append((pct, msg))

    progress = _make_monotonic_progress(base)
    # Mirrors the real concurrent emission order from Phase 2.
    await progress(30, "naming start")
    await progress(56, "desc start")
    await progress(38, "naming done")        # regress → clamp
    await progress(62, "desc done")
    await progress(42, "naming processed")   # regress → clamp

    assert [s[0] for s in seen] == [30, 56, 56, 62, 62]
    # Messages are always forwarded verbatim, even when the number is held.
    assert [s[1] for s in seen] == [
        "naming start",
        "desc start",
        "naming done",
        "desc done",
        "naming processed",
    ]


@pytest.mark.asyncio
async def test_monotonic_progress_passes_increasing_values():
    """A normal monotonic sequence is forwarded unchanged."""
    seen: list[int] = []

    async def base(pct: int, msg: str) -> None:
        seen.append(pct)

    progress = _make_monotonic_progress(base)
    for v in (10, 20, 55, 75, 100):
        await progress(v, "x")

    assert seen == [10, 20, 55, 75, 100]


@pytest.mark.asyncio
async def test_phase2_emitter_shared_counter_is_monotonic_and_bounded():
    """Two interleaved agents share one counter; output is non-decreasing in-range."""
    seen: list[tuple[int, str]] = []

    async def base(pct: int, msg: str) -> None:
        seen.append((pct, msg))

    emit = _make_phase2_step_emitter(base, lo=58, hi=74, cap=24)
    # Interleave like asyncio.gather would.
    await emit("nazwy", 1, 1)
    await emit("opisy", 1, 1)
    await emit("nazwy", 2, 2)
    await emit("opisy", 2, 2)

    pcts = [s[0] for s in seen]
    assert pcts == sorted(pcts)                 # monotonic non-decreasing
    assert all(58 <= p <= 74 for p in pcts)     # within reserved range
    assert pcts[-1] > pcts[0]                    # shared counter advances
    assert "nazwy" in seen[0][1]
    assert "opisy" in seen[1][1]


@pytest.mark.asyncio
async def test_phase2_emitter_caps_at_hi():
    """Beyond `cap` combined steps the percent saturates at `hi` (never past it)."""
    seen: list[int] = []

    async def base(pct: int, msg: str) -> None:
        seen.append(pct)

    emit = _make_phase2_step_emitter(base, lo=58, hi=74, cap=4)
    for i in range(10):
        await emit("nazwy", i + 1, i + 1)

    assert max(seen) == 74
    assert seen[-1] == 74
    assert seen == sorted(seen)
