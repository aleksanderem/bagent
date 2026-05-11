"""Run the full competitor report pipeline (Etap 4 + Etap 5) in-process
against PRODUCTION Supabase data for a given convex_audit_id.

Mirrors what workers.tasks.run_competitor_report_task does, but without
Redis/arq: instantiates SupabaseService directly and calls
run_competitor_report_pipeline().

Usage:
    cd /Users/alex/Desktop/MOJE_PROJEKTY/bagent
    .venv/bin/python -m scripts.run_competitor_report_local \\
        --audit-id j57e45tp7cjm8yhprz093ds05d84jd9s \\
        --tier base \\
        --target-count 5

Reads Supabase URL + service key from .env (config.settings.*).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import settings  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("comp-report-local")


async def _print_progress(progress: int, message: str) -> None:
    bar = "█" * (progress // 5) + "·" * (20 - progress // 5)
    print(f"  [{bar}] {progress:>3}% — {message}", flush=True)


async def main_async(args: argparse.Namespace) -> int:
    from pipelines.competitor_report import run_competitor_report_pipeline

    print(f"=== Competitor report pipeline ===")
    print(f"  audit_id   = {args.audit_id}")
    print(f"  tier       = {args.tier}")
    print(f"  selection  = {args.selection_mode}")
    print(f"  target #   = {args.target_count}")
    print(f"  supabase   = {settings.supabase_url}")
    print(f"  minimax    = {'configured' if settings.minimax_api_key else 'MISSING (synthesis will fallback)'}")
    print()

    supabase = SupabaseService()

    # Sanity check: subject scrape exists for this audit
    subject = await supabase.get_subject_salon_for_audit(args.audit_id)
    if subject is None:
        print(f"ERROR: no salon_scrapes row found for audit_id={args.audit_id}")
        return 2
    print(f"Subject salon:")
    print(f"  name       = {subject.get('name')}")
    print(f"  city       = {subject.get('city')}")
    print(f"  booksy_id  = {subject.get('booksy_id')}")
    print(f"  lat,lng    = {subject.get('salon_lat')}, {subject.get('salon_lng')}")
    print(f"  partner    = {subject.get('partner_system')}")
    print()

    t0 = time.time()
    print("Running pipeline...")
    try:
        result = await run_competitor_report_pipeline(
            audit_id=args.audit_id,
            tier=args.tier,
            selection_mode=args.selection_mode,
            target_count=args.target_count,
            convex_user_id=args.convex_user_id,
            on_progress=_print_progress,
            supabase=supabase,
        )
    except Exception as e:
        print(f"\nFAILED after {time.time() - t0:.1f}s: {type(e).__name__}: {e}")
        logger.exception("pipeline failed")
        return 3

    dt = time.time() - t0
    print()
    print(f"=== DONE in {dt:.1f}s ===")
    print(f"  report_id           = {result['report_id']}")
    print(f"  narrative chars     = {len(result.get('narrative') or '')}")
    print(f"  swot items          = {result.get('swot_item_count')}")
    print(f"  recommendations     = {result.get('recommendation_count')}")
    print(f"  used_fallback (AI)  = {result.get('used_fallback')}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-id", required=True)
    ap.add_argument("--tier", default="base", choices=["base", "premium"])
    ap.add_argument("--selection-mode", default="auto", choices=["auto", "manual"])
    ap.add_argument("--target-count", type=int, default=5)
    ap.add_argument("--convex-user-id", default="local-test-user")
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
