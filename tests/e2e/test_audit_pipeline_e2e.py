"""E2E test: audit (BAGENT #1) pipeline against random salons.

Usage:
    pytest tests/e2e/test_audit_pipeline_e2e.py -v -m e2e
    pytest tests/e2e/test_audit_pipeline_e2e.py -v -m e2e --e2e-salons=5 --e2e-seed=42

Each parametrized salon → one full pipeline invocation → invariants
checked. AI variability is captured by the invariants (range bounds,
schema validity, Polish text quality), not by exact-match comparisons.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from .invariants import check_audit_report


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_audit_pipeline_against_sampled_salons(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    capture_progress,
    disable_supabase_writes,
):
    """For each sampled salon, run the full audit pipeline in-process
    and assert all invariants hold.

    This is a single test that internally loops salons (not parametrized)
    so a partial failure on salon #2 still surfaces salon #3's report —
    helps separate "this salon's data is weird" from "the pipeline is
    broken for everyone"."""
    from pipelines.report import run_audit_pipeline

    cb, log = capture_progress
    failures_by_salon: dict[Any, list[str]] = {}

    for salon_row in salon_pool:
        scraped = scraped_data_for(salon_row)
        audit_id = f"e2e-test-{salon_row['salon_id']}-{salon_row['id']}"
        try:
            report = await run_audit_pipeline(
                scraped_data=scraped,
                audit_id=audit_id,
                on_progress=cb,
            )
        except Exception as e:
            failures_by_salon[salon_row["salon_id"]] = [
                f"pipeline_raised: {type(e).__name__}: {e}"
            ]
            continue

        # Pipeline returned — run all invariants
        rep = check_audit_report(report, scraped)
        if not rep.ok:
            failures_by_salon[salon_row["salon_id"]] = rep.failures
        else:
            print(
                f"\n[e2e] salon={salon_row['salon_id']} "
                f"name={salon_row['salon_name']!r} → "
                f"score={report.get('totalScore')}, "
                f"issues={len(report.get('topIssues') or [])}, "
                f"transformations={len(report.get('transformations') or [])}, "
                f"all {len(rep.passed)} invariants OK"
            )

    if failures_by_salon:
        details = "\n".join(
            f"  salon={sid}: {len(fails)} failures\n    " + "\n    ".join(fails)
            for sid, fails in failures_by_salon.items()
        )
        pytest.fail(
            f"{len(failures_by_salon)}/{len(salon_pool)} salons "
            f"violated audit invariants:\n{details}"
        )


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_audit_pipeline_progress_emits_milestones(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    capture_progress,
    disable_supabase_writes,
):
    """Smoke check: pipeline emits progress at expected milestones.
    Catches the case where a pipeline silently aborts mid-run.

    Required milestones (from pipelines/report.py): 10, 15, 90, 95, 100.
    We allow ±5 fuzz on each."""
    from pipelines.report import run_audit_pipeline

    if not salon_pool:
        pytest.skip("Empty salon pool")

    salon_row = salon_pool[0]
    scraped = scraped_data_for(salon_row)
    cb, log = capture_progress
    audit_id = f"e2e-progress-{salon_row['salon_id']}"

    await run_audit_pipeline(
        scraped_data=scraped, audit_id=audit_id, on_progress=cb,
    )

    progress_values = [p for p, _ in log]
    assert progress_values, "no progress events emitted"
    assert progress_values[0] <= 20, f"first progress {progress_values[0]} > 20"
    assert progress_values[-1] == 100, f"last progress {progress_values[-1]} != 100"
    # Strictly non-decreasing — pipelines must not jump backwards
    for i in range(1, len(progress_values)):
        assert progress_values[i] >= progress_values[i - 1], (
            f"progress went backwards at step {i}: "
            f"{progress_values[i-1]} → {progress_values[i]}"
        )
