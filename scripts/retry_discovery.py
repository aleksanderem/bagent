"""Manual re-trigger for a failed discovery_runs combo.

Usage (from /home/booksy/webapps/bagent-booksyauditor on tytan):

    # by category slug + voivodeship slug (most readable)
    /home/booksy/.local/bin/uv run python -m scripts.retry_discovery \\
        --slug trening-i-dieta --voiv mazowieckie

    # by ids (when you already have them from the dashboard)
    /home/booksy/.local/bin/uv run python -m scripts.retry_discovery \\
        --cat 3 --voiv-id 6

    # retry every failed combo that auto-retry would consider
    # (same SQL helper, no cap)
    /home/booksy/.local/bin/uv run python -m scripts.retry_discovery --all-eligible

The script enqueues a `discover_combo_task` into the same arq queue
the auto-retry uses, so behaviour is identical — it just bypasses the
cool-down and the per-tick cap.

Safety: refuses to enqueue if the latest discovery_runs row for the
combo is currently 'running' (use --force to override). Logs each
enqueue and exits non-zero on resolver miss.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Optional

from arq.connections import create_pool
from supabase import Client

from services.sb_client import make_supabase_client

from config import settings
from workers.main import redis_settings


def _client() -> Client:
    return make_supabase_client(
        settings.supabase_url,
        settings.supabase_service_key,
    )


def _resolve_category(c: Client, *, slug: Optional[str], cat_id: Optional[int]) -> int:
    if cat_id is not None:
        return int(cat_id)
    if slug is None:
        raise SystemExit("Provide either --cat <id> or --slug <category-slug>")
    res = c.table("booksy_categories").select("id").eq("slug", slug).execute()
    if not res.data:
        raise SystemExit(f"category slug not found: {slug!r}")
    return int(res.data[0]["id"])


def _resolve_voivodeship(c: Client, *, name: Optional[str], voiv_id: Optional[int]) -> int:
    if voiv_id is not None:
        return int(voiv_id)
    if name is None:
        raise SystemExit("Provide either --voiv-id <id> or --voiv <name>")
    res = c.table("booksy_voivodeships").select("id").eq("name", name).execute()
    if not res.data:
        # try slug too
        res = c.table("booksy_voivodeships").select("id").eq("slug", name).execute()
    if not res.data:
        raise SystemExit(f"voivodeship not found by name or slug: {name!r}")
    return int(res.data[0]["id"])


def _is_running(c: Client, cat: int, voiv: int) -> bool:
    res = (
        c.table("discovery_runs")
        .select("status")
        .eq("category_id", cat)
        .eq("voivodeship_id", voiv)
        .order("started_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    return bool(res and res[0].get("status") == "running")


async def _enqueue_one(pool, cat: int, voiv: int) -> str:
    job = await pool.enqueue_job("discover_combo_task", cat, voiv)
    return job.job_id if job else "?"


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cat", type=int, help="booksy_categories.id")
    p.add_argument("--slug", help="booksy_categories.slug (e.g. trening-i-dieta)")
    p.add_argument("--voiv-id", type=int, help="booksy_voivodeships.id")
    p.add_argument("--voiv", help="booksy_voivodeships.name OR slug (e.g. mazowieckie)")
    p.add_argument(
        "--all-eligible",
        action="store_true",
        help="Retry every combo list_failed_discovery_runs_for_retry() returns "
             "(same filter as auto-retry, no per-tick cap).",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Enqueue even if latest run is currently 'running' (you'll get a "
             "concurrent discovery_runs row — only use if you know the running "
             "row is a zombie).",
    )
    args = p.parse_args()

    c = _client()
    pool = await create_pool(redis_settings)

    if args.all_eligible:
        res = c.rpc(
            "list_failed_discovery_runs_for_retry",
            {"p_cooldown_min": 0, "p_max_age_hours": 720, "p_limit": 1000},
        ).execute()
        rows = res.data or []
        if not rows:
            print("No eligible failed combos. (Either none failed transiently, or "
                  "all were retried already.)")
            return 0
        print(f"Enqueueing {len(rows)} eligible failed combos:")
        ok = 0
        for r in rows:
            cat = int(r["category_id"])
            voiv = int(r["voivodeship_id"])
            if not args.force and _is_running(c, cat, voiv):
                print(f"  cat={cat} voiv={voiv}  SKIP (running)")
                continue
            jid = await _enqueue_one(pool, cat, voiv)
            print(f"  cat={cat} voiv={voiv}  job={jid}  was: {(r.get('error') or '')[:60]}")
            ok += 1
        print(f"Enqueued {ok}/{len(rows)}.")
        return 0

    cat = _resolve_category(c, slug=args.slug, cat_id=args.cat)
    voiv = _resolve_voivodeship(c, name=args.voiv, voiv_id=args.voiv_id)

    if not args.force and _is_running(c, cat, voiv):
        print(f"Latest run for cat={cat} voiv={voiv} is currently 'running'. "
              f"Refusing to enqueue (use --force if you know it's a zombie).",
              file=sys.stderr)
        return 2

    jid = await _enqueue_one(pool, cat, voiv)
    print(f"Enqueued discover_combo_task(cat={cat}, voiv={voiv})  job_id={jid}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
