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
    """Fan out one task per (category, voivodeship). Idempotent: each
    combo task UPSERTs by booksy_id so re-runs just refresh last_seen_at."""
    client = _get_client()
    cats = client.table("booksy_categories").select("id").execute().data or []
    voivs = client.table("booksy_voivodeships").select("id").execute().data or []

    pool: ArqRedis = ctx["redis"]
    queued = 0
    for cat in cats:
        for voiv in voivs:
            await pool.enqueue_job(
                "discover_combo_task",
                int(cat["id"]),
                int(voiv["id"]),
            )
            queued += 1
    logger.info(
        "[discovery] weekly sweep queued %d combos (%d cats x %d voivs)",
        queued, len(cats), len(voivs),
    )
    return {"queued_combos": queued, "categories": len(cats), "voivodeships": len(voivs)}


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
