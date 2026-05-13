"""SLO probes — proactive semantic correctness checks for continuous pipelines.

Liveness checks (HC pings from inside crons) answer "did the cron run".
These probes answer the harder question: "is the data actually flowing
correctly?". Each probe runs a Supabase query, applies a threshold,
and pings Healthchecks with success / `/fail` depending on the answer.

The probes are deliberately separate from the work crons they observe —
a working cron that produces no useful output is a class of bug that
plain liveness checks miss entirely.

Architecture
------------
* One probe = one Python ``async def`` returning ``ProbeResult(ok, detail)``.
* Each probe has its own ``HC_PING_SLO_*`` env var holding the ping URL.
* Each probe is registered as its own arq cron in ``workers/main.py``
  so they fire at different cadences appropriate to their semantics
  (scrape pipeline every 30 min, daily-volume probes every 6 hours).
* On exception: probe wrapper catches, logs, sends ``/fail`` ping with
  the error message as body. Probe failures NEVER crash the worker.

Adding a new probe
------------------
1. Write ``async def probe_<name>(client) -> ProbeResult`` here.
2. Add wrapper cron task ``async def run_<name>(ctx)`` calling the probe
   + ping helper. Export from ``ALL_SLO_TASKS``.
3. Create Healthchecks check via API, copy ping URL.
4. Add ``HC_PING_SLO_<NAME>=...`` to tytan ``.env``.
5. Register arq cron entry in ``workers/main.py`` with appropriate
   schedule + matching probe wrapper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx

from services.healthcheck import ping
from services.sb_client import make_supabase_client
from config import settings

logger = logging.getLogger("slo_probes")


@dataclass
class ProbeResult:
    """Outcome of a single SLO probe.

    ok        : True when the metric is within expected bounds.
    detail    : One-line human description. Sent as HC ping body so the
                Healthchecks event log shows the actual measured value.
    """
    ok: bool
    detail: str


# ---------------------------------------------------------------------------
# Individual probes
# ---------------------------------------------------------------------------


async def probe_scrape_pipeline_progressing(client) -> ProbeResult:
    """Queue should be either small (drained quickly) or actively draining.

    PASS condition (either):
      - queued depth < 2000 (catches up)
      - >= 30 jobs done in last 30 min (actively draining)

    FAIL: queue stuck at high depth with no drain progress (worker dead,
    bextract unavailable, or queue logic broken).
    """
    queued = (
        client.table("salon_refresh_queue")
        .select("id", count="exact")
        .eq("status", "queued")
        .execute()
    )
    queued_count = queued.count or 0

    done_recent = (
        client.table("salon_refresh_queue")
        .select("id", count="exact")
        .eq("status", "done")
        .gte("created_at", _iso_ago(seconds=1800))
        .execute()
    )
    done_count = done_recent.count or 0

    ok = queued_count < 2000 or done_count >= 30
    return ProbeResult(
        ok=ok,
        detail=f"queued={queued_count} done_last_30min={done_count}",
    )


async def probe_chain_heads_growing(client) -> ProbeResult:
    """New chain heads in salon_scrapes should grow as the scrape pipeline
    re-checks salons. Healthy production rate is ~10–500/h depending on
    how much real change Booksy publishes plus the first-cycle re-scrapes.

    PASS: >= 5 new chain heads in last hour.
    FAIL: 0 in last hour AND > 0 expected (scheduler+worker both alive).

    The 5/h threshold tolerates quiet periods (e.g. weekend nights with
    low review activity); a true outage would show 0 for many consecutive
    hours and HC grace (3h) absorbs the rest.
    """
    res = (
        client.table("salon_scrapes")
        .select("id", count="exact")
        .not_.is_("content_hash", "null")
        .gte("scraped_at", _iso_ago(seconds=3600))
        .execute()
    )
    n = res.count or 0
    return ProbeResult(ok=n >= 5, detail=f"new_chain_heads_last_hour={n}")


async def probe_reviews_ingesting(client) -> ProbeResult:
    """Reviews flow from Booksy via every scrape's UPSERT (idempotent on
    booksy_review_id). Quiet day expect ~50 new reviews across the catalog,
    busy day ~500. Probe fires every 6h so threshold = 12 / 6h = quiet
    day equivalent.

    PASS: >= 12 new reviews in last 24h.
    FAIL: < 12 in 24h → either Booksy stopped publishing OR our scrape
    path stopped writing reviews.
    """
    res = (
        client.table("salon_reviews")
        .select("id", count="exact")
        .gte("scraped_at", _iso_ago(seconds=86400))
        .execute()
    )
    n = res.count or 0
    return ProbeResult(ok=n >= 12, detail=f"new_reviews_last_24h={n}")


async def probe_discovery_active(client) -> ProbeResult:
    """discovery_pump_step finds new (booksy_id, category_id, voivodeship)
    triplets and writes them to discovered_salon_categories. Quiet runs
    (saturated category) write nothing; outright stalled pump writes
    nothing for many consecutive hours.

    PASS: >= 50 new triplets in last 6h (probe interval).
    FAIL: 0 for 6h → discovery pump stalled.

    Table is keyed by (booksy_id, category_id, voivodeship_id) composite PK,
    no `id` column. The freshness column is `first_seen_at`.
    """
    res = (
        client.table("discovered_salon_categories")
        .select("booksy_id", count="exact")
        .gte("first_seen_at", _iso_ago(seconds=21600))
        .execute()
    )
    n = res.count or 0
    return ProbeResult(ok=n >= 50, detail=f"new_discovered_last_6h={n}")


async def probe_storage_budget(client) -> ProbeResult:
    """Post-dedup, salon_scrapes growth should be modest. Pre-dedup it grew
    ~500 MB-1 GB/day. Steady-state target: < 100 MB/day. Probe samples
    the table size at 6h intervals via pg_total_relation_size.

    PASS: salon_scrapes total_size < 10 GB (heap + TOAST + indexes).
    NOTE: For now this is an absolute-ceiling probe rather than rate-of-
    change. Adding a historical samples table is future work; the
    ceiling here is sized so a runaway growth scenario alerts well
    before disk pressure.
    """
    res = (
        client.table("v_scrape_dedup_stats")
        .select("*")
        .execute()
    )
    row = (res.data or [{}])[0]
    table_size_text = row.get("table_size_with_toast_and_indexes") or row.get("table_size") or "?"

    # Parse pretty-size into MB (best effort)
    mb = _parse_pretty_size_to_mb(table_size_text)
    # Hard ceiling 10 GB for salon_scrapes total (heap + TOAST + indexes).
    # Pre-dedup peak was 1.5 GB; under dedup we expect to stay < 5 GB
    # even after 1 year of steady-state writes.
    ceiling_mb = 10_240
    return ProbeResult(
        ok=mb is None or mb < ceiling_mb,
        detail=f"salon_scrapes_total_size={table_size_text}",
    )


async def probe_logflare_bounded(client) -> ProbeResult:
    """The cloudflare.logs.prod table (token 547f1556) grew to 111 GB
    before we truncated it on 2026-05-13 and set up daily retention.
    Healthy post-retention size: < 25 GB (7 days × ~3 GB/day from
    Kong access logs of discovery_pump polling).

    PASS: cloudflare logs table < 30 GB.
    FAIL: > 30 GB → retention job missed a day or growth rate spiked.

    Implementation note: we can't query _supabase.public schema via
    PostgREST (no access). Instead, we expose this metric via a SECURITY
    DEFINER function in the main `postgres` DB that reads pg_class
    across DBs is also tricky — easier path is for the daily retention
    cron itself to write its result into a small ops table that this
    probe reads. For initial deploy we'll mark this probe as a no-op
    until that wiring lands (see TODO).
    """
    # TODO(monitoring): add ops.log_retention_history table written by
    # the systemd retention job; query latest row here. Until then,
    # always pass and let the retention's own HC ping cover the alarm.
    return ProbeResult(ok=True, detail="not_yet_implemented_see_TODO")


# ---------------------------------------------------------------------------
# Cron wrappers — each registered in workers/main.py
# ---------------------------------------------------------------------------


PROBE_REGISTRY: dict[str, tuple[Callable[[Any], Awaitable[ProbeResult]], str]] = {
    "scrape_pipeline_progressing": (probe_scrape_pipeline_progressing, "HC_PING_SLO_SCRAPE_PIPELINE"),
    "chain_heads_growing":         (probe_chain_heads_growing,         "HC_PING_SLO_CHAIN_HEADS"),
    "reviews_ingesting":           (probe_reviews_ingesting,           "HC_PING_SLO_REVIEWS"),
    "discovery_active":            (probe_discovery_active,            "HC_PING_SLO_DISCOVERY"),
    "storage_budget":              (probe_storage_budget,              "HC_PING_SLO_STORAGE"),
    "logflare_bounded":            (probe_logflare_bounded,            "HC_PING_SLO_LOGFLARE"),
}


async def _run_probe(name: str, ctx: dict[str, Any]) -> str:
    """Generic dispatcher used by every cron wrapper below. Handles client
    creation, error trapping, and HC ping."""
    probe_fn, env_var = PROBE_REGISTRY[name]
    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    try:
        result = await probe_fn(client)
        if result.ok:
            await _ping_with_body(env_var, result.detail, fail=False)
            logger.info("[slo] %s OK %s", name, result.detail)
            return f"ok: {result.detail}"
        else:
            await _ping_with_body(env_var, result.detail, fail=True)
            logger.warning("[slo] %s FAIL %s", name, result.detail)
            return f"fail: {result.detail}"
    except Exception as e:  # noqa: BLE001
        msg = f"{type(e).__name__}: {e}"
        await _ping_with_body(env_var, msg, fail=True)
        logger.exception("[slo] %s exception", name)
        return f"exception: {msg}"


async def _ping_with_body(env_var: str, body: str, *, fail: bool) -> None:
    """Like services.healthcheck.ping but sends a body so the HC event log
    shows the measured value (queue depth, row count, etc.)."""
    import os
    url = os.environ.get(env_var)
    if not url:
        return
    if fail:
        url = url.rstrip("/") + "/fail"
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            await http.post(url, data=body.encode("utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning("[slo] healthcheck ping (%s) failed: %s", env_var, e)


# Exposed cron tasks — arq needs each as a top-level coroutine.

async def slo_scrape_pipeline_progressing(ctx):
    return await _run_probe("scrape_pipeline_progressing", ctx)


async def slo_chain_heads_growing(ctx):
    return await _run_probe("chain_heads_growing", ctx)


async def slo_reviews_ingesting(ctx):
    return await _run_probe("reviews_ingesting", ctx)


async def slo_discovery_active(ctx):
    return await _run_probe("discovery_active", ctx)


async def slo_storage_budget(ctx):
    return await _run_probe("storage_budget", ctx)


async def slo_logflare_bounded(ctx):
    return await _run_probe("logflare_bounded", ctx)


ALL_SLO_TASKS = [
    slo_scrape_pipeline_progressing,
    slo_chain_heads_growing,
    slo_reviews_ingesting,
    slo_discovery_active,
    slo_storage_budget,
    slo_logflare_bounded,
]


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def _iso_ago(*, seconds: int) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _parse_pretty_size_to_mb(s: str | None) -> float | None:
    """Parse Postgres pg_size_pretty output like '1234 MB', '5 GB', '700 kB'
    into MB float. Returns None on parse failure."""
    if not s:
        return None
    try:
        parts = s.strip().split()
        val = float(parts[0])
        unit = parts[1].lower() if len(parts) > 1 else "b"
        if unit.startswith("k"):
            return val / 1024.0
        if unit.startswith("m"):
            return val
        if unit.startswith("g"):
            return val * 1024.0
        if unit.startswith("t"):
            return val * 1024.0 * 1024.0
        # bytes
        return val / (1024.0 * 1024.0)
    except (ValueError, IndexError):
        return None
