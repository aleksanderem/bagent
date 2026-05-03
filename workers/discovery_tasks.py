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
# Delay between combos when work exists. Step 2: 60→15→5. The pump
# runs serially so this is pure inter-step idle and there's no
# downside to dropping it close to zero — discover_combo itself
# dominates wall clock. 5s gives a tiny breather between bursts of
# tiny rural combos so we don't slam Booksy with back-to-back
# zero-second turnarounds.
PUMP_IDLE_DELAY_SEC = 5
PUMP_EMPTY_DELAY_SEC = 600      # delay when no due combo is found
# Lock TTL needs to outlive the longest single sweep. mazowieckie x
# dense category (Trening i Dieta, Fryzjer, Salon kosmetyczny) can
# probe 500+ bboxes at ~1.5s each. 4h is comfortable headroom.
PUMP_LOCK_TTL_SEC = 4 * 60 * 60
PUMP_TASK_TIMEOUT_SEC = 4 * 60 * 60  # arq per-job timeout for the pump itself


# Cooldowns for the pump's combo selection. Tuned after first full
# 368-combo sweep finished in <24h: with 24h cooldown the entire
# pump went idle waiting for cooldowns to expire, scrape orchestrator
# starved (queue empty for hours).
#
# Dropped to 4h — 368 combos × ~75s saturated-rewalk avg = ~7.7h
# per cycle, so ~3 full cycles/day. Saturation early-stop (25
# zero-new bboxes) makes re-walks cheap because most salons already
# in DB; per-cycle cost is dominated by genuinely-new salons we're
# catching from Booksy (the whole point of frequent re-walks).
#
# FAILED_COOLDOWN_HOURS: short so auto_retry_failed_discovery_runs
# (cron :15/:45) gets first crack but pump can step in if retry
# misses something.
DONE_COOLDOWN_HOURS = 4
FAILED_COOLDOWN_HOURS = 1


def _pick_next_due_combo(client: Client) -> tuple[int, int] | None:
    """Return (category_id, voivodeship_id) of the next combo due for
    discovery, with cooldown logic to avoid re-walking the same combo
    every pump cycle.

    Priority order:
      1. NEVER-swept combos (no discovery_runs row at all).
      2. Combos whose latest run is past cooldown — sorted by oldest
         started_at first. 'done' combos cool down for
         DONE_COOLDOWN_HOURS, 'failed' for FAILED_COOLDOWN_HOURS.
      3. (skipped) Combos currently 'running' (let them finish).
      4. (skipped) Combos in cooldown window."""
    from datetime import datetime, timedelta, timezone
    now = datetime.now(tz=timezone.utc)
    done_cutoff = now - timedelta(hours=DONE_COOLDOWN_HOURS)
    failed_cutoff = now - timedelta(hours=FAILED_COOLDOWN_HOURS)

    cats = client.table("booksy_categories").select("id").execute().data or []
    voivs = client.table("booksy_voivodeships").select("id").execute().data or []
    combos = {(int(c["id"]), int(v["id"])) for c in cats for v in voivs}
    if not combos:
        return None

    # Pull both started_at and finished_at — we cool down based on
    # finished_at (when the run ENDED) not started_at.
    runs = (
        client.table("discovery_runs")
        .select("category_id,voivodeship_id,status,started_at,finished_at")
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

    def _parse_iso(ts: str | None) -> datetime | None:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    candidates: list[tuple[Any, tuple[int, int]]] = []
    skipped_cooldown = 0
    for combo in combos:
        latest = latest_per_combo.get(combo)
        if latest is None:
            candidates.append(("", combo))  # NEVER-swept sorts first
            continue
        status = latest.get("status")
        if status == "running":
            continue  # in flight
        finished_at = _parse_iso(latest.get("finished_at")) or _parse_iso(latest.get("started_at"))
        if finished_at is not None:
            if status == "done" and finished_at >= done_cutoff:
                skipped_cooldown += 1
                continue
            if status == "failed" and finished_at >= failed_cutoff:
                skipped_cooldown += 1
                continue
        candidates.append((latest.get("started_at") or "0", combo))

    if not candidates:
        if skipped_cooldown > 0:
            logger.info(
                "[discovery] no due combo (%d in cooldown — done<%dh or failed<%dh)",
                skipped_cooldown, DONE_COOLDOWN_HOURS, FAILED_COOLDOWN_HOURS,
            )
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

    # arq has no per-job timeout override; the global
    # WorkerSettings.job_timeout (4h) covers us.
    await pool.enqueue_job("discovery_pump_step", _defer_by=delay)
    return outcome


async def bootstrap_discovery_pump(ctx: dict[str, Any]) -> dict[str, Any]:
    """One-shot kick to start the pump loop. Idempotent — fires a single
    enqueue with a 2s defer. If the loop is already running, the lock
    inside ``discovery_pump_step`` ensures no double-pump.

    Also opportunistically reaps the most-recent generation of zombies
    (rows still 'running' but with no progress in the last 5 min). The
    hourly :30 cron handles the long-tail (>4h) case; this catches the
    common pump_step-killed-by-deploy case where a row sat for ~minutes
    after a worker restart and the pump can't pick its combo because
    _pick_next_due_combo skips 'running' rows.
    """
    pool: ArqRedis = ctx["redis"]
    client = _get_client()
    # Reap rows still 'running' from >5 min ago AND with bboxes_walked=0
    # (nothing flushed yet → almost certainly killed before first probe).
    try:
        client.rpc("reap_stuck_discovery_runs_recent", {}).execute()
    except Exception:
        # Helper not present in older deploys — fall through silently.
        pass
    await pool.enqueue_job("discovery_pump_step", _defer_by=2)
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


async def auto_retry_failed_discovery_runs(ctx: dict[str, Any]) -> dict[str, Any]:
    """Auto-recovery for transient discovery failures.

    Pulls candidates from the SQL helper ``list_failed_discovery_runs_for_retry``
    (latest run per combo is 'failed' with a known transient error
    pattern, finished_at past the cool-down). Re-enqueues each as a
    ``discover_combo_task`` so the failed combo gets another shot
    without waiting for the natural pump rotation (~6-12h).

    Race protection: before enqueueing, double-checks via Supabase
    that no NEW 'running' row was inserted for the combo since the
    helper read the snapshot. Combos already running on the pump
    or a prior retry are skipped.

    Cron: every 30 min at :45 (off-peak from bootstrap :00/:30 and
    reap :30, leaving 15-min spacing).
    """
    pool: ArqRedis = ctx["redis"]
    client = _get_client()

    try:
        res = client.rpc(
            "list_failed_discovery_runs_for_retry",
            {"p_cooldown_min": 5, "p_max_age_hours": 24, "p_limit": 5},
        ).execute()
        candidates = res.data or []
    except Exception as e:  # noqa: BLE001
        logger.warning("[discovery] auto-retry helper call failed: %s", e)
        return {"enqueued": 0, "candidates": 0, "error": str(e)}

    enqueued = 0
    skipped_running = 0
    for c in candidates:
        cat = int(c["category_id"])
        voiv = int(c["voivodeship_id"])
        # Race re-check: if a newer run is now 'running' (e.g. pump
        # picked the combo while we listed), skip — let it run.
        latest = (
            client.table("discovery_runs")
            .select("status")
            .eq("category_id", cat)
            .eq("voivodeship_id", voiv)
            .order("started_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
        if latest and latest[0].get("status") == "running":
            skipped_running += 1
            continue
        await pool.enqueue_job("discover_combo_task", cat, voiv)
        enqueued += 1
        logger.info(
            "[discovery] auto-retry enqueued cat=%s voiv=%s (was: %s)",
            cat, voiv, c.get("error", "")[:60],
        )

    if enqueued or skipped_running:
        logger.info(
            "[discovery] auto-retry: enqueued=%d skipped_running=%d total_candidates=%d",
            enqueued, skipped_running, len(candidates),
        )
    return {
        "enqueued": enqueued,
        "skipped_running": skipped_running,
        "candidates": len(candidates),
    }


ALL_DISCOVERY_TASKS = [
    discover_combo_task,
    discovery_full_sweep_cron,        # legacy / manual escape hatch
    enqueue_discovered_to_refresh_queue,
    reap_stuck_discovery_runs,
    discovery_pump_step,              # the loop
    bootstrap_discovery_pump,         # kicks the loop
    auto_retry_failed_discovery_runs, # transient-failure auto-recovery
]
