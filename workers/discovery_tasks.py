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
import os
from typing import Any

from arq.connections import ArqRedis

from supabase import Client

from services.sb_client import make_supabase_client

from config import settings
from discovery import discover_combo

logger = logging.getLogger("bagent.workers.discovery_tasks")

_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


async def discover_combo_task(
    ctx: dict[str, Any],
    category_id: int,
    voivodeship_id: int,
) -> dict[str, Any]:
    # Switched to location-hierarchy walker — see commit 5ed9f1f and
    # discovery/locations.py for the rationale (bbox quad-tree caps at
    # ~0.5% coverage on dense urban combos because Booksy returns the
    # same top-N for every nearby bbox).
    from discovery.locations import discover_combo_via_locations
    result = await discover_combo_via_locations(int(category_id), int(voivodeship_id))
    return {
        "category_id": result.category_id,
        "voivodeship_id": result.voivodeship_id,
        "locations_walked": result.locations_walked,
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

# --- Parallel pump configuration -------------------------------------------
#
# The pump used to be strictly sequential — single Redis lock
# `discovery:pump:active` enforced one combo at a time. With 368 combos
# and ~30 min/walk, throughput peaked at ~3-4 combos/h. After all combos
# walked once (we're now in re-walk mode) the long tail of low-coverage
# combos took weeks to converge.
#
# Now: N parallel slots (default 3, tunable via MAX_PARALLEL_PUMPS env).
# Each slot holds its own lock `discovery:pump:slot:{i}` and runs
# discovery_pump_step independently. A per-combo Redis claim
# (`discovery:pump:claim:{cat}:{voiv}`) prevents two slots from
# accidentally racing on the same combo before the DB-level dedup guard
# in `discover_combo_via_locations` kicks in.
#
# 3 parallel slots × ~20-30 min/combo = ~6-9 combos/h, ~3× current
# throughput. Bextract API/proxies handle 2.5 req/s sustained per
# walker so 3× = 7.5 req/s — still under Booksy's observed ~10 req/s
# soft limit. Bump to 5+ if Bextract scales.
MAX_PARALLEL_PUMPS = int(os.environ.get("MAX_PARALLEL_PUMPS", "3"))

# Per-slot lock key prefix. {i} ranges over 0..MAX_PARALLEL_PUMPS-1.
PUMP_SLOT_LOCK_PREFIX = "discovery:pump:slot:"
# Per-combo claim prefix. Set on pick, deleted on finish. TTL longer
# than the worst-case walk so an orphaned claim from a crashed worker
# auto-expires.
PUMP_COMBO_CLAIM_PREFIX = "discovery:pump:claim:"

# Delay between combos when work exists. Step 2: 60→15→5. Each slot
# self-paces so this is pure intra-slot idle.
PUMP_IDLE_DELAY_SEC = 5
PUMP_EMPTY_DELAY_SEC = 600      # delay when no due combo is found
# Lock TTL needs to outlive the longest single sweep. mazowieckie x
# dense category (Trening i Dieta, Fryzjer, Salon kosmetyczny) can
# probe 500+ bboxes at ~1.5s each. 4h is comfortable headroom.
PUMP_LOCK_TTL_SEC = 4 * 60 * 60
PUMP_COMBO_CLAIM_TTL_SEC = 4 * 60 * 60  # match slot TTL — claim auto-clears if pump crashes
PUMP_TASK_TIMEOUT_SEC = 4 * 60 * 60  # arq per-job timeout for the pump itself

# Backward compat: legacy single-lock key. Kept so existing watchdog
# crons that probe this don't false-alarm during rollout. The pump no
# longer reads/writes it; slot locks above are the source of truth.
PUMP_LOCK_KEY = "discovery:pump:active"


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

# Saturation-aware cooldown — when the last walk found very few new
# salons, the combo is essentially saturated against canonical_children
# walking. Re-walking it every 4h wastes pump cycles that should go to
# bigger gaps. Combos under SATURATION_NEW_THRESHOLD jump to longer
# cooldown.
#
# Empirical: a combo at 99%+ coverage finds 0-2 new per walk; the
# whole walk is ~99.8% duplicates. With 158 combos in the 1-30% bucket
# (still 100k missing salons), pump time is much better spent on
# those gaps than re-walking saturated ones.
#
# Threshold of 5 new is conservative — gives a combo a chance to find
# legitimately new salons that were just added on Booksy, while
# stepping out of the way once it's clearly saturated.
SATURATION_NEW_THRESHOLD = 5
SATURATION_COOLDOWN_HOURS = 24


def _pick_next_due_combos(
    client: Client, top_k: int = 10
) -> list[tuple[int, int]]:
    """Return up to ``top_k`` (category_id, voivodeship_id) tuples of
    combos due for discovery, ordered by priority. Caller (parallel
    pump) iterates these and SETNX-claims the first available one.

    Returns a LIST (was: single combo) so parallel pumps each pick a
    different combo from the same priority-sorted snapshot, falling
    back to lower-priority items if the top-priority combo is already
    claimed by another slot.

    Sort key (smaller wins, picked first):
      Tier 0: NEVER-swept combos (no discovery_runs row).
      Tier 1: Combos with biggest absolute coverage gap
              (booksy_reported_total - our_unique_count). A run that's
              missing 7,000 salons trumps one missing 200, regardless
              of when it last finished.
      Tier 2: For combos without a known booksy_reported_total (or with
              equal gaps), fall back to oldest started_at.
      Skipped: currently 'running', or in cooldown window. Saturated
              combos (last run found <SATURATION_NEW_THRESHOLD new) get
              SATURATION_COOLDOWN_HOURS instead of DONE_COOLDOWN_HOURS.

    Saturation cooldown pushes already-converged combos out of the
    way so parallel pumps spend cycles on the long tail of 1-30%
    combos instead of burning runs on 99%-saturated ones."""
    from datetime import datetime, timedelta, timezone
    now = datetime.now(tz=timezone.utc)
    done_cutoff = now - timedelta(hours=DONE_COOLDOWN_HOURS)
    failed_cutoff = now - timedelta(hours=FAILED_COOLDOWN_HOURS)
    saturation_cutoff = now - timedelta(hours=SATURATION_COOLDOWN_HOURS)

    cats = client.table("booksy_categories").select("id").execute().data or []
    voivs = client.table("booksy_voivodeships").select("id").execute().data or []
    combos = {(int(c["id"]), int(v["id"])) for c in cats for v in voivs}
    if not combos:
        return []

    # Pull started_at, finished_at, and salons_new — we cool down
    # based on finished_at, and apply saturation cooldown when
    # latest.salons_new < SATURATION_NEW_THRESHOLD.
    runs = (
        client.table("discovery_runs")
        .select("category_id,voivodeship_id,status,started_at,finished_at,salons_new")
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

    # Pull coverage gap snapshot — used for absolute-priority sort.
    # `v_discovery_coverage` already exists (BEAUTY_AUDIT migration 028).
    gap_per_combo: dict[tuple[int, int], int] = {}
    try:
        cov = (
            client.table("v_discovery_coverage")
            .select("category_id,voivodeship_id,our_unique_count,booksy_reported_total")
            .execute()
            .data
            or []
        )
        for row in cov:
            cid = row.get("category_id")
            vid = row.get("voivodeship_id")
            ours = row.get("our_unique_count") or 0
            theirs = row.get("booksy_reported_total")
            if cid is None or vid is None or theirs is None:
                continue
            gap = max(0, int(theirs) - int(ours))
            gap_per_combo[(int(cid), int(vid))] = gap
    except Exception as e:  # noqa: BLE001
        logger.warning("[discovery] coverage view unavailable, falling back to age sort: %s", e)

    def _parse_iso(ts: str | None) -> datetime | None:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    # Sort key shape: (tier, neg_gap, started_at_str)
    #   tier 0 = never-swept (highest priority), tier 1 = walked
    #   neg_gap = -gap so larger gap sorts first
    #   started_at = secondary tiebreaker (older first)
    candidates: list[tuple[tuple[int, int, str], tuple[int, int]]] = []
    skipped_cooldown = 0
    skipped_saturated = 0
    for combo in combos:
        latest = latest_per_combo.get(combo)
        if latest is None:
            candidates.append(((0, 0, ""), combo))
            continue
        status = latest.get("status")
        if status == "running":
            continue
        finished_at = _parse_iso(latest.get("finished_at")) or _parse_iso(latest.get("started_at"))
        if finished_at is not None:
            if status == "done":
                # Saturation-aware cooldown: combos whose last run found
                # <SATURATION_NEW_THRESHOLD new salons get the longer
                # SATURATION_COOLDOWN_HOURS so the pump doesn't burn
                # cycles re-walking already-converged combos.
                salons_new = latest.get("salons_new")
                is_saturated = (
                    salons_new is not None
                    and int(salons_new) < SATURATION_NEW_THRESHOLD
                )
                effective_cutoff = saturation_cutoff if is_saturated else done_cutoff
                if finished_at >= effective_cutoff:
                    if is_saturated:
                        skipped_saturated += 1
                    else:
                        skipped_cooldown += 1
                    continue
            if status == "failed" and finished_at >= failed_cutoff:
                skipped_cooldown += 1
                continue
        gap = gap_per_combo.get(combo, 0)
        # Tier 1 + biggest gap first (negative for ascending sort).
        # started_at as final tiebreaker (oldest first).
        candidates.append(((1, -gap, latest.get("started_at") or "0"), combo))

    if not candidates:
        if skipped_cooldown or skipped_saturated:
            logger.info(
                "[discovery] no due combo (%d in cooldown done<%dh/failed<%dh, "
                "%d saturated <%d new in last walk → cooldown<%dh)",
                skipped_cooldown, DONE_COOLDOWN_HOURS, FAILED_COOLDOWN_HOURS,
                skipped_saturated, SATURATION_NEW_THRESHOLD, SATURATION_COOLDOWN_HOURS,
            )
        return []
    candidates.sort(key=lambda x: x[0])
    return [c[1] for c in candidates[:top_k]]


def _pick_next_due_combo(client: Client) -> tuple[int, int] | None:
    """Backward-compat shim. Returns first candidate or None.
    New code paths should call ``_pick_next_due_combos`` directly to
    enable parallel-pump SETNX-based combo claiming."""
    combos = _pick_next_due_combos(client, top_k=1)
    return combos[0] if combos else None


async def discovery_pump_step(
    ctx: dict[str, Any], slot: int = 0
) -> dict[str, Any]:
    """One-combo discovery step + self-reschedule. Each slot holds
    its own Redis lock (`discovery:pump:slot:{slot}`) so up to
    MAX_PARALLEL_PUMPS slots can run concurrently. Per-combo SETNX
    claim (`discovery:pump:claim:{cat}:{voiv}`) prevents two slots
    from racing on the same combo before the in-process dedup guard
    in `discover_combo_via_locations` (5-min freshness check on
    running rows) catches it.

    On any exit path (success, no-due-combos, exception) the slot
    re-enqueues itself with the same slot id so the parallel fan-out
    persists across worker restarts (modulo arq job persistence)."""
    pool: ArqRedis = ctx["redis"]
    slot_key = f"{PUMP_SLOT_LOCK_PREFIX}{slot}"
    locked = await pool.set(slot_key, "1", ex=PUMP_LOCK_TTL_SEC, nx=True)
    if not locked:
        logger.info(
            "[discovery] pump slot=%d: another step already holds this slot, skipping",
            slot,
        )
        return {"skipped": True, "slot": slot}

    outcome: dict[str, Any]
    claimed_combo: tuple[int, int] | None = None
    delay = PUMP_IDLE_DELAY_SEC
    try:
        client = _get_client()
        # Pick a list of top-K candidates and SETNX-claim the first
        # one not already locked by another slot. This decorrelates
        # parallel slots cleanly without a coordinator. Top-K depth
        # of MAX_PARALLEL_PUMPS*4 leaves ample headroom even when
        # multiple slots all happen to picker-tick within the same
        # second.
        candidates = _pick_next_due_combos(
            client, top_k=max(MAX_PARALLEL_PUMPS * 4, 10)
        )
        combo: tuple[int, int] | None = None
        for cand in candidates:
            cat, voiv = cand
            claim_key = f"{PUMP_COMBO_CLAIM_PREFIX}{cat}:{voiv}"
            claimed = await pool.set(
                claim_key, str(slot), ex=PUMP_COMBO_CLAIM_TTL_SEC, nx=True
            )
            if claimed:
                combo = cand
                claimed_combo = cand
                break
            else:
                logger.info(
                    "[discovery] pump slot=%d: combo cat=%s voiv=%s already "
                    "claimed by another slot, trying next candidate",
                    slot, cat, voiv,
                )

        if combo is None:
            delay = PUMP_EMPTY_DELAY_SEC
            outcome = {
                "slot": slot,
                "swept": False,
                "reason": "no_due_combos_or_all_claimed",
                "candidates_seen": len(candidates),
                "next_step_in_sec": delay,
            }
        else:
            cat_id, voiv_id = combo
            logger.info(
                "[discovery] pump slot=%d: sweeping cat=%s voiv=%s (location-walk)",
                slot, cat_id, voiv_id,
            )
            # Switched from discover_combo (bbox quad-tree) to
            # discover_combo_via_locations (canonical hierarchy walker).
            # Empirical: bbox approach hit 0.5% coverage on dense urban
            # combos. Hierarchy walker uses Booksy's per-location top-100
            # listing for each canonical_children sublocation (Warsaw alone
            # has 194 districts) — projected 50-100% coverage.
            from discovery.locations import discover_combo_via_locations
            result = await discover_combo_via_locations(cat_id, voiv_id)
            outcome = {
                "slot": slot,
                "swept": True,
                "category_id": cat_id,
                "voivodeship_id": voiv_id,
                "salons_found": result.salons_found,
                "salons_new": result.salons_new,
                "new_mappings": result.new_mappings,
                "locations_walked": result.locations_walked,
                "duration_sec": int(result.duration_sec),
                "error": result.error,
                "next_step_in_sec": delay,
            }
    finally:
        # Always release this slot's lock and any per-combo claim,
        # even on exception, so the next pump_step can pick up.
        await pool.delete(slot_key)
        if claimed_combo is not None:
            cat, voiv = claimed_combo
            await pool.delete(f"{PUMP_COMBO_CLAIM_PREFIX}{cat}:{voiv}")

    # Re-enqueue this same slot — keeps the parallel fan-out stable
    # across self-rescheduling. arq has no per-job timeout override;
    # the global WorkerSettings.job_timeout (4h) covers us.
    await pool.enqueue_job("discovery_pump_step", slot, _defer_by=delay)
    return outcome


async def bootstrap_discovery_pump(ctx: dict[str, Any]) -> dict[str, Any]:
    """Idempotent watchdog: fires MAX_PARALLEL_PUMPS pump_step jobs
    (one per slot). Slots already running (lock held) will exit
    cleanly on entry — no double-pump risk.

    Also opportunistically reaps the most-recent generation of zombies
    (rows still 'running' but with no progress in the last 5 min). The
    hourly :30 cron handles the long-tail (>4h) case; this catches the
    common pump_step-killed-by-deploy case where a row sat for ~minutes
    after a worker restart and the picker can't see its combo because
    _pick_next_due_combos skips 'running' rows.
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
    # Fan out one pump_step per slot. Tiny per-slot stagger so they
    # don't all hit the picker in the same millisecond — reduces the
    # window where two slots could read the same set of candidates.
    for s in range(MAX_PARALLEL_PUMPS):
        await pool.enqueue_job("discovery_pump_step", s, _defer_by=2 + s)
    return {"bootstrapped": True, "slots": MAX_PARALLEL_PUMPS}


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
