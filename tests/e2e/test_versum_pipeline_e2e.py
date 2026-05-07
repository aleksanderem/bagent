"""E2E test: versum_suggest pipeline against random salons.

Versum mapping takes service list + taxonomy and emits per-service
treatment suggestions with confidence scores. No DB reads — full
in-process invocation.

Usage:
    pytest tests/e2e/test_versum_pipeline_e2e.py -v -m e2e
"""

from __future__ import annotations

from typing import Any

import pytest

from .invariants import check_versum_suggest_output
from .pipeline_runners import run_versum_suggest


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_versum_suggest_against_sampled_salons(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
):
    failures_by_salon: dict[Any, list[str]] = {}

    for salon_row in salon_pool:
        scraped = scraped_data_for(salon_row)
        try:
            output = await run_versum_suggest(scraped)
        except Exception as e:
            failures_by_salon[salon_row["salon_id"]] = [
                f"pipeline_raised: {type(e).__name__}: {e}"
            ]
            continue

        rep = check_versum_suggest_output(output, scraped)
        if not rep.ok:
            failures_by_salon[salon_row["salon_id"]] = rep.failures
        else:
            print(
                f"\n[e2e versum] salon={salon_row['salon_id']} → "
                f"all {len(rep.passed)} invariants OK"
            )

    if failures_by_salon:
        details = "\n".join(
            f"  salon={sid}: {len(fails)} failures\n    " + "\n    ".join(fails)
            for sid, fails in failures_by_salon.items()
        )
        pytest.fail(
            f"{len(failures_by_salon)}/{len(salon_pool)} salons "
            f"violated versum invariants:\n{details}"
        )
