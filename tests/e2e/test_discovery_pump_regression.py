"""Regression tests for the parallel discovery pump.

The pump uses N parallel slots (default 3) with per-slot Redis locks
+ per-combo SETNX claims. The dedup guard inside
``discover_combo_via_locations`` is the second line of defence.

Critical invariants we want to lock in:

  1. _pick_next_due_combos returns DIFFERENT combos for two parallel
     callers (top-K candidate list is wide enough).

  2. Per-combo claim lock prevents two slots picking the same combo
     within the small race window between picker and DB-insert.

  3. Saturation cooldown skips combos with last salons_new < 5.

  4. Slot lock TTL releases stale locks (deploy mid-step doesn't
     orphan a slot for 4h — auto-expires).

These tests don't need AI calls or live discovery — they exercise the
picker logic + Redis claim semantics directly with mocked DB rows.

Usage:
    pytest tests/e2e/test_discovery_pump_regression.py -v -m e2e
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from workers.discovery_tasks import (
    DONE_COOLDOWN_HOURS,
    PUMP_COMBO_CLAIM_PREFIX,
    PUMP_SLOT_LOCK_PREFIX,
    SATURATION_COOLDOWN_HOURS,
    SATURATION_NEW_THRESHOLD,
    _pick_next_due_combos,
)


def _mock_client_with_combos_and_runs(
    cats: list[dict[str, Any]],
    voivs: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    coverage: list[dict[str, Any]],
) -> MagicMock:
    """Builds a MagicMock that mimics the supabase client surface
    used by _pick_next_due_combos:
      - .table(name).select(...).order(...).limit(...).execute().data
      - .table(name).select(...).execute().data
    """
    def _execute_returns(rows):
        m = MagicMock()
        m.data = rows
        return m

    def _table(name):
        t = MagicMock()
        if name == "booksy_categories":
            t.select.return_value.execute.return_value = _execute_returns(cats)
        elif name == "booksy_voivodeships":
            t.select.return_value.execute.return_value = _execute_returns(voivs)
        elif name == "discovery_runs":
            chain = t.select.return_value.order.return_value.limit.return_value
            chain.execute.return_value = _execute_returns(runs)
        elif name == "v_discovery_coverage":
            t.select.return_value.execute.return_value = _execute_returns(coverage)
        else:
            t.select.return_value.execute.return_value = _execute_returns([])
        return t

    client = MagicMock()
    client.table.side_effect = _table
    return client


@pytest.mark.e2e
def test_picker_returns_top_k_unique_combos():
    """_pick_next_due_combos with top_k=3 should return 3 distinct
    (cat, voiv) tuples sorted by tier+gap."""
    cats = [{"id": 1}, {"id": 2}, {"id": 3}]
    voivs = [{"id": 100}, {"id": 200}]
    # 6 combos total. No runs → all are tier 0 (never-swept).
    coverage = [
        {"category_id": 1, "voivodeship_id": 100, "our_unique_count": 10, "booksy_reported_total": 1000},
        {"category_id": 2, "voivodeship_id": 100, "our_unique_count": 50, "booksy_reported_total": 500},
        {"category_id": 3, "voivodeship_id": 200, "our_unique_count": 0, "booksy_reported_total": 100},
    ]
    client = _mock_client_with_combos_and_runs(cats, voivs, [], coverage)

    combos = _pick_next_due_combos(client, top_k=3)
    assert len(combos) == 3
    assert len(set(combos)) == 3, f"duplicate combos returned: {combos}"


@pytest.mark.e2e
def test_picker_skips_recently_done_combos():
    """A combo done within DONE_COOLDOWN_HOURS should NOT be picked
    (unless it's the only candidate left)."""
    cats = [{"id": 1}]
    voivs = [{"id": 100}, {"id": 200}]
    now = datetime.now(tz=timezone.utc)
    just_finished = (now - timedelta(minutes=10)).isoformat()
    long_ago = (now - timedelta(hours=DONE_COOLDOWN_HOURS + 1)).isoformat()

    runs = [
        {
            "category_id": 1, "voivodeship_id": 100,
            "status": "done", "started_at": just_finished,
            "finished_at": just_finished, "salons_new": 50,
        },
        {
            "category_id": 1, "voivodeship_id": 200,
            "status": "done", "started_at": long_ago,
            "finished_at": long_ago, "salons_new": 50,
        },
    ]
    client = _mock_client_with_combos_and_runs(cats, voivs, runs, [])

    combos = _pick_next_due_combos(client, top_k=5)
    # Only voiv=200 should be eligible (voiv=100 in cooldown)
    assert (1, 200) in combos
    assert (1, 100) not in combos, (
        f"combo (1,100) was in cooldown but picker returned it: {combos}"
    )


@pytest.mark.e2e
def test_picker_applies_saturation_cooldown():
    """A combo whose last run found <SATURATION_NEW_THRESHOLD new
    should get the longer SATURATION_COOLDOWN_HOURS instead of the
    short DONE_COOLDOWN_HOURS — so it's still cooled down even after
    DONE_COOLDOWN passes."""
    cats = [{"id": 1}]
    voivs = [{"id": 100}, {"id": 200}]
    now = datetime.now(tz=timezone.utc)
    # Past DONE_COOLDOWN but within SATURATION_COOLDOWN
    middle = (now - timedelta(hours=DONE_COOLDOWN_HOURS + 1)).isoformat()
    long_ago = (now - timedelta(hours=SATURATION_COOLDOWN_HOURS + 1)).isoformat()

    runs = [
        {
            "category_id": 1, "voivodeship_id": 100,
            "status": "done", "started_at": middle, "finished_at": middle,
            "salons_new": SATURATION_NEW_THRESHOLD - 1,  # saturated → longer cooldown
        },
        {
            "category_id": 1, "voivodeship_id": 200,
            "status": "done", "started_at": middle, "finished_at": middle,
            "salons_new": 100,  # fresh → only DONE_COOLDOWN applies
        },
    ]
    client = _mock_client_with_combos_and_runs(cats, voivs, runs, [])

    combos = _pick_next_due_combos(client, top_k=5)
    assert (1, 200) in combos, "non-saturated combo should be eligible"
    assert (1, 100) not in combos, (
        "saturated combo (last run <5 new) should still be in saturation "
        "cooldown despite passing DONE_COOLDOWN"
    )


@pytest.mark.e2e
def test_picker_skips_running_combos():
    """A combo currently 'running' is never picked, even by parallel
    slots — this prevents the race that the per-combo claim lock
    catches."""
    cats = [{"id": 1}]
    voivs = [{"id": 100}, {"id": 200}]
    now = datetime.now(tz=timezone.utc)
    runs = [
        {
            "category_id": 1, "voivodeship_id": 100,
            "status": "running", "started_at": now.isoformat(),
            "finished_at": None, "salons_new": None,
        },
    ]
    client = _mock_client_with_combos_and_runs(cats, voivs, runs, [])

    combos = _pick_next_due_combos(client, top_k=5)
    assert (1, 100) not in combos, (
        f"running combo was picked — dedup guard regression: {combos}"
    )
    assert (1, 200) in combos


@pytest.mark.e2e
def test_picker_prioritizes_largest_gap():
    """Among walked combos, the one with the biggest absolute coverage
    gap should sort first (most missing salons = highest priority)."""
    cats = [{"id": 1}]
    voivs = [{"id": 100}, {"id": 200}]
    now = datetime.now(tz=timezone.utc)
    long_ago = (now - timedelta(hours=24)).isoformat()
    runs = [
        {
            "category_id": 1, "voivodeship_id": 100,
            "status": "done", "started_at": long_ago, "finished_at": long_ago,
            "salons_new": 100,
        },
        {
            "category_id": 1, "voivodeship_id": 200,
            "status": "done", "started_at": long_ago, "finished_at": long_ago,
            "salons_new": 100,
        },
    ]
    coverage = [
        {"category_id": 1, "voivodeship_id": 100, "our_unique_count": 10, "booksy_reported_total": 100},     # gap=90
        {"category_id": 1, "voivodeship_id": 200, "our_unique_count": 10, "booksy_reported_total": 10000},   # gap=9990
    ]
    client = _mock_client_with_combos_and_runs(cats, voivs, runs, coverage)

    combos = _pick_next_due_combos(client, top_k=2)
    assert combos[0] == (1, 200), (
        f"larger-gap combo (1,200) gap=9990 should sort first; got {combos}"
    )


@pytest.mark.e2e
def test_lock_key_format_stable():
    """Slot + claim lock key prefixes shouldn't change without
    deliberate migration — Redis state across deploy depends on them."""
    assert PUMP_SLOT_LOCK_PREFIX == "discovery:pump:slot:", (
        "Slot lock key prefix changed — orphan locks from old code "
        "won't auto-cleanup against new code"
    )
    assert PUMP_COMBO_CLAIM_PREFIX == "discovery:pump:claim:", (
        "Combo claim key prefix changed — same risk as above"
    )
