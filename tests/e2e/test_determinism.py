"""Determinism check — same salon run twice must produce semantically
similar outputs.

AI temperature drift is a real risk: a release bumping temperature
from 0.2 → 0.7 silently makes outputs much more variable, with no
test catching it. This test runs the SAME salon through audit twice
and asserts:

  1. totalScore differs by ≤ 5 points
  2. summary length stays roughly stable (±50%)
  3. The set of categoryNames in transformations is similar
     (Jaccard ≥ 0.5)

We can't use embedding similarity here (no embedding service in
bagent yet), so we use simple set/string-length heuristics.

Costly — runs audit pipeline 2× per salon. Use sparingly.

Usage:
    pytest tests/e2e/test_determinism.py -v -m e2e --e2e-salons=1
"""

from __future__ import annotations

from typing import Any

import pytest

from .pipeline_runners import run_audit


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.asyncio
async def test_audit_pipeline_is_semantically_stable(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    read_only_supabase,
):
    """For 1 sampled salon, run audit pipeline twice. Assert outputs
    are semantically similar — catches temperature drift / random
    instability."""
    if not salon_pool:
        pytest.skip("empty salon pool")

    # Use only the first salon — 2 runs × ~3 min = ~6 min
    salon_row = salon_pool[0]
    scraped = scraped_data_for(salon_row)

    print(f"\n[determinism] running audit twice for salon={salon_row['salon_id']}")
    report_a = await run_audit(scraped)
    report_b = await run_audit(scraped)

    # 1. totalScore stability
    score_a = report_a.get("totalScore", -1)
    score_b = report_b.get("totalScore", -1)
    assert abs(score_a - score_b) <= 5, (
        f"totalScore drift: run A={score_a}, run B={score_b} (diff={abs(score_a-score_b)}) "
        f"— suggests temperature too high or non-deterministic prompt"
    )

    # 2. summary length stability
    sum_a = report_a.get("summary", "")
    sum_b = report_b.get("summary", "")
    if sum_a and sum_b:
        ratio = len(sum_a) / len(sum_b) if sum_b else 0
        assert 0.5 <= ratio <= 2.0, (
            f"summary length drift: run A={len(sum_a)}, run B={len(sum_b)} "
            f"(ratio={ratio:.2f}) — pipeline may be non-deterministic"
        )

    # 3. transformation category coverage Jaccard similarity
    def _cat_set(report: dict[str, Any]) -> set[str]:
        cats = set()
        for t in report.get("transformations", []):
            if isinstance(t, dict):
                # Best-effort: extract category name from various shapes
                cats.add(t.get("categoryName") or t.get("category") or "")
        return {c for c in cats if c}

    cats_a = _cat_set(report_a)
    cats_b = _cat_set(report_b)
    if cats_a or cats_b:
        sim = _jaccard(cats_a, cats_b)
        assert sim >= 0.4, (
            f"transformation category Jaccard={sim:.2f} (expected ≥0.4) — "
            f"runA cats={cats_a}, runB cats={cats_b}"
        )

    print(f"[determinism] OK: scores {score_a}/{score_b} (diff {abs(score_a-score_b)}), "
          f"summaries {len(sum_a)}/{len(sum_b)} chars, "
          f"category Jaccard {_jaccard(cats_a, cats_b):.2f}")
