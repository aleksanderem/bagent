"""Load test: N concurrent audit report pipelines sharing the MiniMax-M2.7
provider_slot cap (PR#41). Verifies no 429 cascade / degradation at the target
concurrency WITHOUT polluting prod data — it runs `run_audit_pipeline` directly
(in-memory report dict) and SKIPS the Supabase save + Convex webhooks that
`workers.tasks.run_report_task` does.

Reuses an existing audit's scraped data (already in Supabase) as the payload, so
no scrape is triggered. Run on tytan against the bagent venv:

    cd /home/booksy/webapps/bagent-booksyauditor
    set -a && source .env && set +a
    .venv/bin/python scripts/loadtest_report_concurrency.py \
        --audit-id j57eb6wsb60k103b5pch82xnd988qk0a --concurrency 10

Output: per-run wall time, success/failure, and any 429 / rate-limit errors seen.
NOTE: Step 8 (market position) does an idempotent upsert of the salon's
dimensional score — re-running it is a no-op overwrite, not new data.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any


async def _one_run(idx: int, scraped: Any) -> dict[str, Any]:
    from pipelines.report import run_audit_pipeline

    t0 = time.perf_counter()
    err: str | None = None
    score: int | None = None
    try:
        # Distinct synthetic audit_id per run for log correlation only — the
        # pipeline uses it for log lines, not for any write here.
        report = await run_audit_pipeline(scraped, f"loadtest-{idx}", None)
        score = report.get("totalScore")
    except Exception as e:  # noqa: BLE001
        err = f"{type(e).__name__}: {e}"
    dt = time.perf_counter() - t0
    return {"idx": idx, "seconds": round(dt, 1), "score": score, "error": err}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-id", required=True, help="existing audit whose scraped data to reuse")
    ap.add_argument("--concurrency", type=int, default=10)
    args = ap.parse_args()

    from config import settings
    from models.scraped_data import ScrapedData
    from services.supabase import SupabaseService

    supabase = SupabaseService()
    raw = await supabase.get_scraped_data(args.audit_id)
    if not raw:
        raise SystemExit(f"No scraped data for audit {args.audit_id}")
    scraped = ScrapedData(**raw)

    cap = settings.__dict__.get("LLM_MAX_CONCURRENCY_MINIMAX_M2_7", "env-default(4)")
    print(
        f"Load test: {args.concurrency} concurrent run_audit_pipeline | "
        f"salon='{scraped.salonName}' services={scraped.totalServices} | "
        f"MiniMax cap={cap}"
    )

    t0 = time.perf_counter()
    results = await asyncio.gather(
        *[_one_run(i, scraped) for i in range(args.concurrency)]
    )
    total = time.perf_counter() - t0

    ok = [r for r in results if r["error"] is None]
    failed = [r for r in results if r["error"] is not None]
    rate_limited = [r for r in failed if "429" in (r["error"] or "") or "rate" in (r["error"] or "").lower()]

    print("\n=== per-run ===")
    for r in sorted(results, key=lambda x: x["idx"]):
        print(f"  run {r['idx']:2d}: {r['seconds']:7.1f}s  score={r['score']}  {r['error'] or 'OK'}")

    secs = sorted(r["seconds"] for r in ok)
    print("\n=== summary ===")
    print(f"  concurrency      : {args.concurrency}")
    print(f"  wall clock total : {total:.1f}s")
    print(f"  ok / failed      : {len(ok)} / {len(failed)}")
    print(f"  429 / rate-limit : {len(rate_limited)}")
    if secs:
        print(f"  per-run min/median/max : {secs[0]:.1f}s / {secs[len(secs)//2]:.1f}s / {secs[-1]:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
