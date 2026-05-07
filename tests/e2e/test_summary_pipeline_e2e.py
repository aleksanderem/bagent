"""E2E test: summary (BAGENT #3) pipeline against random salons.

Summary takes audit_id + user_id + competitor_ids and produces a
narrative summary with headline/subheadline/ctaText. We mock Supabase
reads with synthetic audit_report + competitor list so the pipeline
runs in-process without DB writes.

Usage:
    pytest tests/e2e/test_summary_pipeline_e2e.py -v -m e2e
"""

from __future__ import annotations

from typing import Any

import pytest

from .invariants import check_summary_output
from .pipeline_runners import run_summary


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_summary_against_sampled_salons(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    read_only_supabase,
    monkeypatch,
):
    failures_by_salon: dict[Any, list[str]] = {}

    for salon_row in salon_pool:
        scraped = scraped_data_for(salon_row)
        try:
            output = await run_summary(scraped, monkeypatch=monkeypatch)
        except Exception as e:
            failures_by_salon[salon_row["salon_id"]] = [
                f"pipeline_raised: {type(e).__name__}: {e}"
            ]
            continue

        rep = check_summary_output(output, scraped)
        if not rep.ok:
            failures_by_salon[salon_row["salon_id"]] = rep.failures
        else:
            print(
                f"\n[e2e summary] salon={salon_row['salon_id']} → "
                f"all {len(rep.passed)} invariants OK"
            )

    if failures_by_salon:
        details = "\n".join(
            f"  salon={sid}: {len(fails)} failures\n    " + "\n    ".join(fails)
            for sid, fails in failures_by_salon.items()
        )
        pytest.fail(
            f"{len(failures_by_salon)}/{len(salon_pool)} salons "
            f"violated summary invariants:\n{details}"
        )
