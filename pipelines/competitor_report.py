"""Top-level competitor report pipeline (Comp Etap 2 wiring).

Chains Comp Etap 4 (compute_competitor_analysis — deterministic data) with
Comp Etap 5 (synthesize_competitor_insights — MiniMax AI synthesis) into a
single entry point that the server.py background job calls.

Progress scaling:
  - Etap 4 runs from 0 to 70% (compute + persistence of matches/pricing/gaps/dims)
  - Etap 5 runs from 70% to 100% (AI narrative + SWOT + recommendations)

Returns a summary dict with report_id + stats used by the completion webhook.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from pipelines.competitor_analysis import compute_competitor_analysis
from pipelines.competitor_synthesis import synthesize_competitor_insights
from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


async def run_competitor_report_pipeline(
    audit_id: str,
    tier: str = "base",
    selection_mode: str = "auto",
    target_count: int = 5,
    convex_user_id: str = "unknown",
    on_progress: ProgressCallback | None = None,
    supabase: SupabaseService | None = None,
) -> dict[str, Any]:
    """Top-level competitor report pipeline.

    Steps:
      0-70%: compute_competitor_analysis (selection + persistence + computation)
      70-100%: synthesize_competitor_insights (AI narrative + SWOT + recommendations)

    Args:
        audit_id: convex_audit_id of the subject audit
        tier: 'base' or 'premium'
        selection_mode: 'auto' or 'manual'
        target_count: how many competitors to select in auto mode
        convex_user_id: Convex user id for the report row
        on_progress: async progress callback (0-100, message)
        supabase: optional SupabaseService instance (for tests)

    Returns:
        {
            "report_id": int,
            "narrative": str,
            "swot_item_count": int,
            "recommendation_count": int,
            "used_fallback": bool,
        }

    Raises:
        RuntimeError: if competitor selection fails or compute fails
        ValueError: if subject audit not found
    """
    progress = on_progress or _noop_progress
    service = supabase or SupabaseService()

    # Scale Etap 4 progress to 0-70
    async def compute_progress(p: int, m: str) -> None:
        await progress(int(p * 0.70), m)

    logger.info(
        "Starting competitor report pipeline for audit_id=%s tier=%s",
        audit_id, tier,
    )

    report_id = await compute_competitor_analysis(
        audit_id=audit_id,
        tier=tier,
        selection_mode=selection_mode,
        target_count=target_count,
        on_progress=compute_progress,
        supabase=service,
        convex_user_id=convex_user_id,
    )
    logger.info("Etap 4 complete — report_id=%s, starting Etap 5", report_id)

    # Scale Etap 5 progress to 70-100
    async def synth_progress(p: int, m: str) -> None:
        await progress(70 + int(p * 0.30), m)

    synth_result = await synthesize_competitor_insights(
        report_id=report_id,
        on_progress=synth_progress,
        supabase=service,
    )

    return {
        "report_id": report_id,
        "narrative": synth_result["narrative"],
        "swot_item_count": synth_result["swot_item_count"],
        "recommendation_count": synth_result["recommendation_count"],
        "used_fallback": synth_result["used_fallback"],
    }
