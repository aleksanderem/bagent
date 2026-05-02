"""Issue #34 — arq tasks for Booksy salon discovery.

Two task types registered:

* ``discover_combo_task`` — single (category_id, voivodeship_id) sweep.
  Used both by the weekly fan-out cron and by manual smoke tests.
* ``discovery_full_sweep_cron`` — kicks off one full pass through all
  22 categories × 16 voivodeships every Sunday at 03:00 UTC. Each
  combo is scheduled as a separate task so a single bad combo doesn't
  hold up the others.
* ``enqueue_discovered_to_refresh_queue`` — runs hourly: pushes new
  discoveries from ``discovered_salons`` into ``salon_refresh_queue``
  via the SQL helper, in batches of 1000.
"""

from __future__ import annotations

import logging
from typing import Any

from arq.connections import ArqRedis

from supabase import Client, ClientOptions, create_client

from config import settings
from discovery import discover_combo

logger = logging.getLogger("bagent.workers.discovery_tasks")

_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
            options=ClientOptions(schema="public"),
        )
    return _supabase_client


async def discover_combo_task(
    ctx: dict[str, Any],
    category_id: int,
    voivodeship_id: int,
) -> dict[str, Any]:
    result = await discover_combo(int(category_id), int(voivodeship_id))
    return {
        "category_id": result.category_id,
        "voivodeship_id": result.voivodeship_id,
        "bboxes_walked": result.bboxes_walked,
        "salons_found": result.salons_found,
        "salons_new": result.salons_new,
        "total_count_hint": result.total_count_hint,
        "duration_sec": result.duration_sec,
        "error": result.error,
    }


async def discovery_full_sweep_cron(ctx: dict[str, Any]) -> dict[str, int]:
    """Run all (category, voivodeship) combos sequentially in this task.

    Earlier version fanned out 352 sub-tasks to arq, but bagent-worker
    drains 20 in parallel which immediately tripped Booksy's 429 rate
    limit. Doing it sequentially in one long-running task gives us
    deterministic spacing (with the per-call sleep + backoff inside
    discover.py) and naturally skips combos already covered by a fresh
    UPSERT during this run.
    """
    from discovery import discover_all
    results = await discover_all()
    total_new = sum(r.salons_new for r in results)
    total_found = sum(r.salons_found for r in results)
    failed = sum(1 for r in results if r.error)
    return {
        "combos_run": len(results),
        "salons_found": total_found,
        "salons_new": total_new,
        "failed_combos": failed,
    }


async def enqueue_discovered_to_refresh_queue(ctx: dict[str, Any]) -> dict[str, int]:
    """Hourly: bulk-enqueue new discoveries into salon_refresh_queue
    (tier 3, low priority). The SQL helper marks each row enqueued_at
    so we don't re-enqueue. Cap per tick at 1000 so we never blow up
    the queue with a single sweep's output."""
    client = _get_client()
    res = client.rpc("enqueue_discovered_salons", {"p_limit": 1000}).execute()
    enqueued = int(res.data or 0)
    if enqueued > 0:
        logger.info("[discovery] enqueued %d new discoveries into salon_refresh_queue", enqueued)
    return {"enqueued": enqueued}


ALL_DISCOVERY_TASKS = [
    discover_combo_task,
    discovery_full_sweep_cron,
    enqueue_discovered_to_refresh_queue,
]
