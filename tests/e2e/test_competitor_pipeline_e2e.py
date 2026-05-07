"""E2E test: competitor_report (BAGENT #4) pipeline against random salons.

Competitor report is a 9-step pipeline producing dimensional scores +
synthesis + traceability artifacts. Heavy AI cost — keep --e2e-salons
low (1-2) for routine runs.

Usage:
    pytest tests/e2e/test_competitor_pipeline_e2e.py -v -m e2e --e2e-salons=1
"""

from __future__ import annotations

from typing import Any

import pytest

from .invariants import check_competitor_report_output
from .pipeline_runners import run_competitor_report


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.asyncio
async def test_competitor_report_against_sampled_salons(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    read_only_supabase,
    monkeypatch,
):
    failures_by_salon: dict[Any, list[str]] = {}

    # Limit to first salon — competitor pipeline is the heaviest
    # (multiple AI passes per dimension × 28 dimensions × N competitors)
    for salon_row in salon_pool[:1]:
        scraped = scraped_data_for(salon_row)
        try:
            output = await run_competitor_report(scraped, monkeypatch=monkeypatch)
        except Exception as e:
            failures_by_salon[salon_row["salon_id"]] = [
                f"pipeline_raised: {type(e).__name__}: {e}"
            ]
            continue

        rep = check_competitor_report_output(output, scraped)
        if not rep.ok:
            failures_by_salon[salon_row["salon_id"]] = rep.failures
        else:
            print(
                f"\n[e2e competitor] salon={salon_row['salon_id']} → "
                f"all {len(rep.passed)} invariants OK"
            )

    if failures_by_salon:
        details = "\n".join(
            f"  salon={sid}: {len(fails)} failures\n    " + "\n    ".join(fails)
            for sid, fails in failures_by_salon.items()
        )
        pytest.fail(
            f"{len(failures_by_salon)} salons violated competitor "
            f"invariants:\n{details}"
        )
