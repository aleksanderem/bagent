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
    """LEGACY: run all 352 combos sequentially in one long task.
    Mostly superseded by ``discovery_pump_step`` which drains one combo
    at a time + self-reschedules. Kept as manual escape hatch.
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


# ---------------------------------------------------------------------------
# Continuous discovery pump
# ---------------------------------------------------------------------------
#
# One self-rescheduling task picks the least-recently-swept (category,
# voivodeship) combo, runs discover_combo, then re-enqueues itself with
# a delay. Loop runs forever, naturally cycles every ~6-12h depending
# on bbox depth, and survives worker restart cleanly (arq picks up the
# pending re-enqueue from Redis).
#
# Bootstrap: enqueue ``discovery_pump_step`` once after deploy.
# Self-healing: a Redis lock prevents two pumps from running in
# parallel (which would happen if a bootstrap is mistakenly fired
# while a step is mid-flight).

PUMP_LOCK_KEY = "discovery:pump:active"
PUMP_IDLE_DELAY_SEC = 60        # delay between combos when work exists
PUMP_EMPTY_DELAY_SEC = 600      # delay when no due combo is found
# Lock TTL needs to outlive the longest single sweep. mazowieckie x
# dense category (Trening i Dieta, Fryzjer, Salon kosmetyczny) can
# probe 500+ bboxes at ~1.5s each. 4h is comfortable headroom.
PUMP_LOCK_TTL_SEC = 4 * 60 * 60
PUMP_TASK_TIMEOUT_SEC = 4 * 60 * 60  # arq per-job timeout for the pump itself


def _pick_next_due_combo(client: Client) -> tuple[int, int] | None:
    """Return (category_id, voivodeship_id) of the least-recently-swept
    combo. NEVER-swept combos win first. Combos with status='running'
    in the latest discovery_runs row are skipped (zombies/concurrent)."""
    cats = client.table("booksy_categories").select("id").execute().data or []
    voivs = client.table("booksy_voivodeships").select("id").execute().data or []
    combos = {(int(c["id"]), int(v["id"])) for c in cats for v in voivs}
    if not combos:
        return None

    runs = (
        client.table("discovery_runs")
        .select("category_id,voivodeship_id,status,started_at")
        .order("started_at", desc=True)
        .limit(5000)
        .execute()
        .data
        or []
    )
    latest_per_combo: dict[tuple[int, int], dict[str, Any]] = {}
    for r in runs:
        key = (int(r["category_id"]), int(r["voivodeship_id"]))
        if key not in latest_per_combo:
            latest_per_combo[key] = r

    candidates: list[tuple[Any, tuple[int, int]]] = []
    for combo in combos:
        latest = latest_per_combo.get(combo)
        if latest is None:
            candidates.append(("", combo))  # NEVER-swept sorts first
        elif latest.get("status") == "running":
            continue
        else:
            candidates.append((latest.get("started_at") or "0", combo))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


async def discovery_pump_step(ctx: dict[str, Any]) -> dict[str, Any]:
    """One-combo discovery step + self-reschedule. Holds Redis lock
    so concurrent pumps can't double-fire."""
    pool: ArqRedis = ctx["redis"]
    locked = await pool.set(PUMP_LOCK_KEY, "1", ex=PUMP_LOCK_TTL_SEC, nx=True)
    if not locked:
        logger.info("[discovery] pump step: another step holds the lock, skipping")
        return {"skipped": True}

    outcome: dict[str, Any]
    try:
        client = _get_client()
        combo = _pick_next_due_combo(client)
        if combo is None:
            delay = PUMP_EMPTY_DELAY_SEC
            outcome = {"swept": False, "reason": "no_due_combos", "next_step_in_sec": delay}
        else:
            cat_id, voiv_id = combo
            logger.info("[discovery] pump: sweeping cat=%s voiv=%s", cat_id, voiv_id)
            from discovery import discover_combo
            result = await discover_combo(cat_id, voiv_id)
            delay = PUMP_IDLE_DELAY_SEC
            outcome = {
                "swept": True,
                "category_id": cat_id,
                "voivodeship_id": voiv_id,
                "salons_found": result.salons_found,
                "salons_new": result.salons_new,
                "bboxes_walked": result.bboxes_walked,
                "duration_sec": int(result.duration_sec),
                "error": result.error,
                "next_step_in_sec": delay,
            }
    finally:
        await pool.delete(PUMP_LOCK_KEY)

    await pool.enqueue_job(
        "discovery_pump_step",
        _defer_by=delay,
        _job_timeout=PUMP_TASK_TIMEOUT_SEC,
    )
    return outcome


async def bootstrap_discovery_pump(ctx: dict[str, Any]) -> dict[str, Any]:
    """One-shot kick to start the pump loop. Idempotent — fires a single
    enqueue with a 2s defer. If the loop is already running, the lock
    inside ``discovery_pump_step`` ensures no double-pump."""
    pool: ArqRedis = ctx["redis"]
    await pool.enqueue_job(
        "discovery_pump_step",
        _defer_by=2,
        _job_timeout=PUMP_TASK_TIMEOUT_SEC,
    )
    return {"bootstrapped": True}


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


async def reap_stuck_discovery_runs(ctx: dict[str, Any]) -> dict[str, int]:
    """Mark discovery_runs rows still 'running' after 4h as 'failed'.
    arq SIGINT during a worker restart kills the task before its
    try/finally can update the row, leaving zombies. Hourly cron
    cleans them up."""
    client = _get_client()
    res = client.rpc("reap_stuck_discovery_runs", {}).execute()
    reaped = int(res.data or 0)
    if reaped > 0:
        logger.warning("[discovery] reaped %d stuck discovery_runs rows", reaped)
    return {"reaped": reaped}


ALL_DISCOVERY_TASKS = [
    discover_combo_task,
    discovery_full_sweep_cron,        # legacy / manual escape hatch
    enqueue_discovered_to_refresh_queue,
    reap_stuck_discovery_runs,
    discovery_pump_step,              # the loop
    bootstrap_discovery_pump,         # kicks the loop
]
