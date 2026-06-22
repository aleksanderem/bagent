"""arq Worker entrypoint — `arq workers.WorkerSettings` runs this.

Foundation work for issue #20 (Iter 1.1 Redis + arq job queue setup).
This file does NOT yet migrate the 7 production pipelines from
server.py BackgroundTasks. That happens in #21. Here we set up:

  - RedisSettings (from config.Settings — host/port/password/database)
  - on_startup / on_shutdown hooks for shared singletons
  - One smoke_test task so we can prove enqueue → execute end-to-end
  - get_redis_pool() helper for the FastAPI side to enqueue jobs

PM2 ecosystem entry runs this as `arq workers.WorkerSettings`. The web
process (uvicorn server:app) and worker process share Redis but are
separate OS processes — code changes for either deploy independently.
"""

from __future__ import annotations

import logging
from typing import Any

from arq.connections import ArqRedis, RedisSettings, create_pool

from config import settings

logger = logging.getLogger("bagent.workers")


# ---------------------------------------------------------------------------
# Redis connection settings
# ---------------------------------------------------------------------------
# Single source of truth for the entire bagent process tree (web + worker).
# Both the FastAPI app (when enqueuing) and the arq worker (when polling)
# import this. Read settings.redis_* — set via env vars on tytan, default to
# localhost in dev.

redis_settings = RedisSettings(
    host=settings.redis_host,
    port=settings.redis_port,
    password=settings.redis_password or None,
    database=settings.redis_database,
    conn_timeout=5,
    conn_retries=3,
)


# ---------------------------------------------------------------------------
# Lazy pool accessor for the FastAPI side
# ---------------------------------------------------------------------------
# server.py initializes one ArqRedis pool at startup (in lifespan) and uses
# it from endpoints to enqueue jobs. This helper wraps create_pool with our
# settings so callers don't need to import RedisSettings.

async def get_redis_pool() -> ArqRedis:
    """Create a fresh ArqRedis connection pool. Caller owns lifecycle.

    Use this from FastAPI lifespan startup to obtain the pool, store on
    app.state.arq, and close in lifespan shutdown.
    """
    return await create_pool(redis_settings)


# ---------------------------------------------------------------------------
# Lifecycle hooks (per worker process)
# ---------------------------------------------------------------------------
# arq calls on_startup once when the worker boots, on_shutdown once on
# graceful exit. Use these to instantiate SHARED singletons (Supabase client,
# MiniMax client) and stash them in ctx — every task receives ctx as first
# argument.
#
# IMPORTANT: existing pipelines in bagent/pipelines/ instantiate clients
# fresh per job (see server.py run_*_job functions). We can preserve that
# pattern in #21 OR move to ctx-based shared clients. For the foundation
# (this PR), we keep on_startup minimal — just log and verify connectivity.

async def startup(ctx: dict[str, Any]) -> None:
    # Issue #24 — load .env into os.environ first so observability.py
    # can see BUGSINK_DSN_*. pydantic-settings reads .env separately
    # but doesn't merge into os.environ.
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:  # noqa: BLE001
        pass

    # Sentry/Bugsink init for the worker process. No-op when DSN env
    # var is missing. Done first so import-time failures in tasks register.
    try:
        from observability import init_for_worker
        init_for_worker()
    except Exception as e:  # noqa: BLE001
        logger.warning("Sentry init for worker failed: %s", e)

    # Observability — arq's CLI does NOT accept --log-level (verified
    # 2026-05-24, crashloops with "No such option"), so we set up
    # Python logging programmatically here at worker startup. This
    # makes `logger.info(...)` from pipelines/competitor_analysis.py +
    # services/* (where the per-phase markers + Etap 4 progress live)
    # reach PM2 logs. See 2026-05-24-pipeline-profile.md for why this
    # is needed: 649s of pipeline silence at WARNING-default.
    #
    # CRITICAL: setLevel alone is not enough — Python's root logger
    # ships with zero handlers, so any record below WARNING gets
    # silently dropped even if the logger level is INFO. We must
    # install a handler via basicConfig (idempotent if already set,
    # force=True overrides arq's own initialization which sets the
    # arq.worker logger to INFO but leaves root at WARNING).
    # PM2 prepends its own timestamp on every log line — keep our
    # format short to avoid double timestamps making PM2 logs unreadable.
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s %(levelname)s %(message)s",
        force=True,
    )
    logging.getLogger("pipelines").setLevel(logging.INFO)
    logging.getLogger("services").setLevel(logging.INFO)
    logging.getLogger("agent").setLevel(logging.INFO)
    logging.getLogger("bagent.workers").setLevel(logging.INFO)

    logger.info("arq worker starting up")
    logger.info(
        "redis target host=%s port=%s database=%s password=%s",
        settings.redis_host, settings.redis_port, settings.redis_database,
        "set" if settings.redis_password else "none",
    )
    # Sanity check: we can ping our own Redis connection (ctx['redis'] is the
    # ArqRedis instance arq attaches to ctx).
    redis: ArqRedis = ctx["redis"]
    pong = await redis.ping()
    logger.info("redis PING → %s", pong)

    # FINDINGS P1-10: współdzielone klienty w ctx + fail-fast na env.
    # Dotąd SupabaseService/MiniMaxClient powstawały per task, więc literówka
    # w SUPABASE_URL/kluczu wybuchała dopiero W ŚRODKU pierwszego joba.
    # Konstrukcja tutaj = crashloop przy bootcie (widoczny w PM2) zamiast
    # cichej porażki klienta w produkcie. Taski biorą ctx["supabase"]
    # z fallbackiem na świeżą instancję (testy wołają je z pustym ctx).
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    ctx["supabase"] = SupabaseService()
    ctx["minimax"] = MiniMaxClient(
        settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model
    )
    if not settings.minimax_api_key:
        raise RuntimeError("MINIMAX_API_KEY is empty — worker would fail every AI job")
    logger.info("shared clients ready: supabase + minimax (%s)", settings.minimax_model)


async def shutdown(ctx: dict[str, Any]) -> None:
    logger.info("arq worker shutting down")


# ---------------------------------------------------------------------------
# Worker heartbeats — Healthchecks ping every 5 minutes (issue: monitoring)
# ---------------------------------------------------------------------------
# Standalone heartbeat crons so Healthchecks alerts when a worker process dies
# entirely. Each named cron has its own check; these specifically prove a
# worker is alive between scheduled jobs.
#
# We run TWO physically separate worker processes (since 2026-06-15), so we
# need TWO independent heartbeats — one is NOT a proxy for the other:
#   * worker_heartbeat        → scrape worker  (WorkerSettings / arq:queue)
#   * report_worker_heartbeat → report worker  (ReportWorkerSettings / arq:reports)
# The report worker now processes every audit + on-demand report; if it died
# while the scrape worker kept pinging, monitoring would stay green while all
# audits silently stalled. Hence the dedicated report-worker heartbeat below.

async def worker_heartbeat(ctx: dict[str, Any]) -> str:
    """Ping the bagent-worker-heartbeat Healthchecks endpoint (SCRAPE worker).

    Fired every 5 minutes by the scrape worker's arq scheduler (SCRAPE_CRONS).
    Proves ONLY the scrape worker (WorkerSettings, arq:queue) is alive — it says
    nothing about the report worker, which has its own report_worker_heartbeat.
    If the scrape worker process is killed, this stops pinging and Healthchecks
    alerts after the grace window (10 minutes for this check).
    """
    from services.healthcheck import ping
    await ping("HC_PING_BAGENT_WORKER_HEARTBEAT")
    return "ok"


async def report_worker_heartbeat(ctx: dict[str, Any]) -> str:
    """Ping the bagent-report-worker-heartbeat Healthchecks endpoint.

    Fired every 5 minutes by the report worker's arq scheduler (REPORT_CRONS).
    The report worker (ReportWorkerSettings, arq:reports) processes ALL
    user-facing report jobs — audits (run_report_task), free report, cennik,
    summary, versum, competitor reports. The scrape worker's heartbeat proves
    nothing about THIS process, so without this independent ping a dead report
    worker would stall every audit while Healthchecks stayed green. If this
    process is killed, the ping stops and Healthchecks alerts after the grace
    window (10 minutes for this check).
    """
    from services.healthcheck import ping
    await ping("HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT")
    return "ok"


# ---------------------------------------------------------------------------
# Smoke test task — proof that enqueue → execute works
# ---------------------------------------------------------------------------
# Real production tasks (audit, competitor_report, etc) come in #21. This
# one exists so we can verify the queue works end-to-end before touching
# any pipeline code.

async def smoke_test(ctx: dict[str, Any], message: str = "hello") -> dict[str, Any]:
    """Trivial task: log message, return ack with job_id and timing.

    Enqueue:
        pool = await get_redis_pool()
        job = await pool.enqueue_job("smoke_test", "hello from FastAPI")
        result = await job.result(timeout=10)
        # result == {"ack": True, "echo": "hello from FastAPI", "job_id": ...}
    """
    job_id = ctx.get("job_id", "?")
    job_try = ctx.get("job_try", 1)
    logger.info("smoke_test executed: message=%r job_id=%s try=%d", message, job_id, job_try)
    return {"ack": True, "echo": message, "job_id": job_id, "try": job_try}


# ---------------------------------------------------------------------------
# WorkerSettings — what `arq workers.WorkerSettings` runs
# ---------------------------------------------------------------------------
# functions: list of every task this worker can execute. As we migrate
# pipelines in #21, this grows.
# max_jobs: concurrent tasks per worker process. IO-bound (most of our load
# is HTTP to MiniMax), so 20 is a reasonable starting value. Adjust based on
# memory pressure observed in production.
# job_timeout: 4 hours — the GLOBAL arq envelope (arq has no per-job
# override). Sized for worst-case discovery_pump_step (dense category
# quad-tree over mazowieckie); competitor (~5min) / audit (~15min) /
# optimization (~10min) pipelines stay far under it and enforce their own
# shorter timeouts via wait_for / httpx. See the job_timeout assignment
# below for the full rationale.
# keep_result: 24h — long enough that frontend polling for status after a
# slow job still works, short enough that result store doesn't bloat.

from .tasks import ALL_TASKS
# Issue #23 — scrape orchestrator tasks (queue drain + scheduler + reaper).
from .scrape_refresh import ALL_SCRAPE_TASKS
# beads BEAUTY_AUDIT-1mb — competitor report queue (capped drain + reaper).
from .competitor_report_queue import ALL_COMPETITOR_QUEUE_TASKS
# Issue #34 — Booksy listing discovery tasks.
from .discovery_tasks import ALL_DISCOVERY_TASKS
# Meta Ads automation — push approved drafts, daily metrics fetch, attribution diff.
from .campaign_tasks import ALL_CAMPAIGN_TASKS
# Iter 8 — outreach automation (wintact deployment + send loop + state machine).
from .outreach_deployer import ALL_OUTREACH_DEPLOYER_TASKS
from .outreach_orchestrator import ALL_OUTREACH_ORCHESTRATOR_TASKS
from .state_transition_processor import ALL_OUTREACH_STATE_TASKS
# Taxonomy maintenance — nightly mv refresh + service embedding + inference backfill.
from .taxonomy_refresh import ALL_TAXONOMY_TASKS
# Staff identity links — nightly cross-salon migration detection (migration 052).
from .staff_identity_refresh import ALL_STAFF_IDENTITY_TASKS
# SLO probes — proactive semantic correctness checks (not plain liveness).
from .slo_probes import ALL_SLO_TASKS

# arq cron import is gated to keep imports cheap when only running tests.
try:  # pragma: no cover
    from arq import cron
    SCRAPE_CRONS = [
        # Drain every 15s (4 ticks/min). Smaller batches but more
        # frequent gives much smoother rate vs the previous 12-batch
        # / 30s pattern that produced bimodal "0 then 24/min" peaks
        # in Grafana. Throughput ceiling unchanged: 6 × 4 × 60 = 1440
        # salons/h, realistic ~1100/h. Per-tick budget ~12s, fits
        # comfortably in the 15s window.
        cron(
            "workers.scrape_refresh.drain_scrape_queue",
            minute={i for i in range(0, 60)},
            second={0, 15, 30, 45},
        ),
        # Top up the queue once an hour at minute 5 (stagger off other crons).
        cron("workers.scrape_refresh.schedule_refresh_cron", minute={5}),
        # Reap stuck jobs every 10 minutes.
        cron("workers.scrape_refresh.reap_stuck_jobs", minute={i for i in range(2, 60, 10)}),
        # beads BEAUTY_AUDIT-1mb — competitor report queue drain + zombie reap
        # MOVED to ReportWorkerSettings (2026-06-15): report processing +
        # classification now run on a dedicated, nice'd bagent-report-worker
        # off the "arq:reports" queue so a heavy report/backfill load can't
        # starve the scrape pump (and vice versa). The drain cron lives with
        # that worker and enqueues run_competitor_report_task to its own queue.
        # Issue #34 — every 30s: bulk-enqueue newly discovered salons.
        # Synced with the drain cadence so a discovery dump at any
        # given second waits at most ~30s before reaching the queue
        # (vs ~60s with per-minute enqueue). SQL helper is cheap.
        cron(
            "workers.discovery_tasks.enqueue_discovered_to_refresh_queue",
            minute={i for i in range(0, 60)},
            second={0, 30},
        ),
        # Issue #34 — every hour at :30: reap discovery_runs rows still
        # 'running' after 4h (worker SIGINT during restart leaves zombies).
        cron(
            "workers.discovery_tasks.reap_stuck_discovery_runs",
            minute={30},
        ),
        # Issue #34 — every 30 min: bootstrap watchdog. Fires a pump
        # bootstrap which is no-op if a step is already in-flight (lock
        # in Redis). Catches the case where the pump loop gets
        # interrupted (worker crash mid-step) and the re-enqueue never
        # happened.
        cron(
            "workers.discovery_tasks.bootstrap_discovery_pump",
            minute={0, 30},
        ),
        # Issue #34 — every 30 min at :15 and :45: auto-retry failed
        # combos with transient errors (timeout/restart/HTTP-flake).
        # Cap=5 per tick, cooldown=5min, max-age=24h. Permanent
        # failures (non-transient errors) skipped — they need a human.
        cron(
            "workers.discovery_tasks.auto_retry_failed_discovery_runs",
            minute={15, 45},
        ),
        # Meta Ads — push approved campaign drafts to Meta API every 5 min.
        # Idempotent (Convex query filters out already-pushed). Fast path
        # so client sees campaign go from 'approved' to 'paused on Meta'
        # within minutes, not hours.
        # DISABLED 2026-05-20 — legacy ConvexClient.run_query usage (campaign_tasks unused in prod):
        # cron(
        #     "workers.campaign_tasks.push_approved_campaigns",
        #     minute={i for i in range(0, 60, 5)},
        # ),
        # Meta Ads — daily metrics fetch at 02:00 UTC. Runs once per day
        # for all active campaigns, pulls yesterday's insights via Meta
        # Marketing API, upserts into Convex campaign_daily_metrics.
        # DISABLED 2026-05-20 — same as above:
        # cron(
        #     "workers.campaign_tasks.fetch_daily_metrics",
        #     hour={2}, minute={0},
        # ),
        # Meta Ads — booking attribution diff every 30 min. Reads
        # v_salon_scrape_pairs (migration 039) for new diffs, matches
        # against ad_clicks (migration 040) within 7-day window, records
        # ad_booking_attributions + mirrors to Convex events.
        # DISABLED 2026-05-20 — same as above:
        # cron(
        #     "workers.campaign_tasks.attribute_bookings",
        #     minute={0, 30},
        # ),
        # Iter 8 — outreach deployer: ship approved templates and
        # segments to wintact every 2 min. Idempotent (skip rows whose
        # wintact_*_id is already set).
        cron(
            "workers.outreach_deployer.deploy_approved_templates",
            minute={i for i in range(0, 60, 2)},
        ),
        cron(
            "workers.outreach_deployer.deploy_approved_segments",
            minute={i for i in range(0, 60, 5)},
        ),
        cron(
            "workers.outreach_deployer.activate_approved_sequences",
            minute={i for i in range(0, 60, 5)},
        ),
        # Iter 8 — outreach orchestrator: enrol every 5 min, send loop
        # every minute (capped at 25 sends per minute by wintact rate
        # limit; orchestrator slices accordingly).
        cron(
            "workers.outreach_orchestrator.enroll_due_contacts",
            minute={i for i in range(0, 60, 5)},
        ),
        cron(
            "workers.outreach_orchestrator.send_due_messages",
            minute={i for i in range(0, 60)},
        ),
        # Iter 8 — state machine: cold ingestion every 30 min,
        # purchase-driven transitions every 15 min, stale expiry hourly.
        cron(
            "workers.state_transition_processor.ingest_new_cold_contacts",
            minute={0, 30},
        ),
        cron(
            "workers.state_transition_processor.apply_purchase_transitions",
            minute={i for i in range(0, 60, 15)},
        ),
        cron(
            "workers.state_transition_processor.expire_stale_states",
            minute={45},
        ),
        # ── Taxonomy maintenance (nightly cascade, ~30min total budget) ──
        # 03:00 — refresh materialized views (mv_booksy_treatments +
        #         mv_treatment_name_lookup) so new treatments from yesterday's
        #         scrapes show up in the canonical taxonomy. ~5-15s.
        cron(
            "workers.taxonomy_refresh.refresh_taxonomy_views",
            hour={3}, minute={0},
        ),
        # 03:15 — embed new services that landed today via OpenAI
        #         text-embedding-3-small. Capped 50k/night (~$0.015 budget).
        cron(
            "workers.taxonomy_refresh.embed_new_services",
            hour={3}, minute={15},
        ),
        # 03:30 — refresh inferred_treatment_id: mark stale rows (>7d) +
        #         drain backfill until exhausted or 200k cap hit. Catches
        #         new scrapes + taxonomy evolution. ~10-15 min budget.
        cron(
            "workers.taxonomy_refresh.refresh_inferred_treatments",
            hour={3}, minute={30},
        ),
        # 03:35 — backfill variant_id for chain-head services (S0078, the
        #         MISSING nightly job). variant_id is written ONLY by the mig-127
        #         RPCs (backfill_service_variants / _untagged); nothing in bagent
        #         called them, so fresh scrapes kept variant_id NULL since
        #         ~2026-06-13 and the competitor pricing pipeline early-exited to
        #         empty (competitor_analysis.py ~679). Drains in-tid (0.55) then
        #         untagged (0.70) until exhausted or cap. Runs after inferred
        #         (03:30) and before focus (04:00), which reads variant_id.
        cron(
            "workers.taxonomy_refresh.refresh_service_variants",
            hour={3}, minute={35},
        ),
        # 03:45 — recompute staff_identity_links over last 90 days. The
        #         Postgres RPC scans v_salon_staff_events, pairs lefts↔
        #         joins by normalized name + geo proximity + time window,
        #         scores confidence, upserts. ~1s on current data; capped
        #         at 5min via per-function statement_timeout.
        cron(
            "workers.staff_identity_refresh.refresh_staff_identity_links",
            hour={3}, minute={45},
        ),
        # 04:00 — refresh salon focus distributions + portfolio embeddings.
        #         Picks up salons with NULL focus_computed_at (new chain heads
        #         via mig 063 trigger) or older than 14 days. Cap 5000/night
        #         (~10 min @ 10/s). Used by competitor_selection v2 for
        #         focus-weighted matching.
        cron(
            "workers.taxonomy_refresh.refresh_salon_focus_distributions",
            hour={4}, minute={0},
        ),
        # Worker heartbeat — every 5 minutes pings the Healthchecks
        # bagent-worker-heartbeat URL so HC alerts when the process dies.
        # All other named crons have their own checks; this one specifically
        # proves the worker process itself is alive between scheduled jobs.
        cron(
            "workers.main.worker_heartbeat",
            minute={i for i in range(0, 60, 5)},
        ),

        # ── SLO probes — semantic-correctness checks for data flow ────────
        # Each probe queries Supabase, applies a threshold, pings its own
        # Healthchecks endpoint with /success or /fail. See workers/slo_probes.py
        # for what each probe measures.

        # Scrape pipeline progressing — every 30 min (matches probe grace 90 min).
        cron(
            "workers.slo_probes.slo_scrape_pipeline_progressing",
            minute={0, 30},
        ),
        # Chain heads growing — every 1h on :07 (staggered off other crons).
        cron(
            "workers.slo_probes.slo_chain_heads_growing",
            minute={7},
        ),
        # Reviews ingesting — every 6h on :12 (daily-volume probe).
        cron(
            "workers.slo_probes.slo_reviews_ingesting",
            hour={2, 8, 14, 20}, minute={12},
        ),
        # Discovery active — every 4h on :17.
        cron(
            "workers.slo_probes.slo_discovery_active",
            hour={1, 5, 9, 13, 17, 21}, minute={17},
        ),
        # Storage budget — every 6h on :22.
        cron(
            "workers.slo_probes.slo_storage_budget",
            hour={3, 9, 15, 21}, minute={22},
        ),
        # Logflare bounded — every 6h on :27.
        cron(
            "workers.slo_probes.slo_logflare_bounded",
            hour={4, 10, 16, 22}, minute={27},
        ),
    ]
    # Competitor report queue: drain + zombie reap. These run on the
    # dedicated bagent-report-worker (ReportWorkerSettings), NOT the scrape
    # worker, so report/classification load is isolated from the scrape pump
    # (2026-06-15). The drain enqueues run_competitor_report_task to the
    # "arq:reports" queue, which only the report worker consumes.
    REPORT_CRONS = [
        cron(
            "workers.competitor_report_queue.drain_competitor_report_queue",
            minute={i for i in range(0, 60)},
        ),
        cron(
            "workers.competitor_report_queue.reap_stuck_competitor_report_jobs",
            minute={i for i in range(8, 60, 10)},
        ),
        # Report worker heartbeat — every 5 minutes pings the Healthchecks
        # bagent-report-worker-heartbeat URL so HC alerts when THIS process
        # dies. MUST live in REPORT_CRONS (ReportWorkerSettings.cron_jobs):
        # registering report_worker_heartbeat in functions does NOT fire it —
        # arq only runs crons listed in cron_jobs. Since 2026-06-15 the report
        # worker drains every audit + on-demand report, so a dead report worker
        # stalls all audits while the scrape worker's heartbeat stays green.
        cron(
            "workers.main.report_worker_heartbeat",
            minute={i for i in range(0, 60, 5)},
        ),
    ]
except Exception:  # noqa: BLE001
    SCRAPE_CRONS = []
    REPORT_CRONS = []


class WorkerSettings:
    # Production tasks from tasks.py + smoke_test for liveness verification.
    functions = [
        smoke_test,
        worker_heartbeat,
        *ALL_TASKS,
        *ALL_SCRAPE_TASKS,
        *ALL_COMPETITOR_QUEUE_TASKS,
        *ALL_DISCOVERY_TASKS,
        *ALL_CAMPAIGN_TASKS,
        *ALL_OUTREACH_DEPLOYER_TASKS,
        *ALL_OUTREACH_ORCHESTRATOR_TASKS,
        *ALL_OUTREACH_STATE_TASKS,
        *ALL_TAXONOMY_TASKS,
        *ALL_STAFF_IDENTITY_TASKS,
        *ALL_SLO_TASKS,
    ]
    cron_jobs = SCRAPE_CRONS
    redis_settings = redis_settings
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 20
    # 4h — covers worst-case discovery_pump_step (mazowieckie x dense
    # category quad-tree). Audit + competitor pipelines never approach
    # this; their own logic enforces shorter timeouts via wait_for /
    # httpx timeout. Per-job overrides aren't supported by arq, so we
    # raise the global to the tallest envelope.
    job_timeout = 4 * 60 * 60
    keep_result = 86400  # 24 hours
    max_tries = 3
    # TODO(future): namespace by environment (e.g. "arq:prod:queue") so
    # accidental shared Redis between dev/staging/prod doesn't cause workers
    # to steal each other's jobs. For now we use the arq default since we
    # only have one environment running — production on tytan.
    queue_name = "arq:queue"


class ReportWorkerSettings:
    """Dedicated arq worker for ALL user-facing report/LLM generation, isolated
    from the scrape pump. Runs as a separate, nice'd PM2 process
    (bagent-report-worker) on its OWN queue ("arq:reports"), so heavy report /
    classification load can't starve the scrape worker — and the scrape pump
    can't starve report latency (the stuck-worker incident where reports cycled
    >1h under shared load).

    Consumes "arq:reports" for:
      * competitor reports — enqueued by the report-queue drain cron (which also
        lives here, moved off SCRAPE_CRONS), globally capped at
        COMPETITOR_REPORT_MAX_CONCURRENT (4);
      * every other on-demand report job — audit (run_report_task), free report,
        cennik, summary, versum suggest, competitor refresh — routed here by
        server._enqueue_pipeline (2026-06-15). Previously these ran on the scrape
        worker and contended with the discovery pump (the audit-hangs-at-40%
        latency tail); now scraping runs alone on WorkerSettings.

    functions registers *ALL_TASKS so this worker can execute every report task
    enqueued to its queue. They remain registered on WorkerSettings too (belt +
    suspenders for graceful drain of in-flight jobs across a deploy — routing,
    not deregistration, is what isolates the load). Shares Redis + Supabase with
    the scrape worker — DB contention is mitigated, not eliminated.
    """
    functions = [
        smoke_test,
        # This worker fires its OWN heartbeat (report_worker_heartbeat via
        # REPORT_CRONS), NOT the scrape worker's. The scrape heartbeat stays
        # scoped to WorkerSettings so the two liveness signals don't conflate.
        report_worker_heartbeat,
        *ALL_TASKS,
        *ALL_COMPETITOR_QUEUE_TASKS,
    ]
    cron_jobs = REPORT_CRONS
    redis_settings = redis_settings
    on_startup = startup
    on_shutdown = shutdown
    # Reports are heavy. Competitor reports self-cap at
    # COMPETITOR_REPORT_MAX_CONCURRENT (4) via the drain; audits + the other
    # on-demand jobs enqueue UNCAPPED and each audit fans out several concurrent
    # MiniMax calls that bypass the global per-model limiter, so total concurrency
    # here is the only ceiling. Kept at 6 (≈4 capped competitor reports + ~2
    # audits) rather than raised — a prior shared-worker incident traced
    # degradation to event-loop saturation, not pure IO-wait, so we do NOT raise
    # concurrency on this workload without a load test. The 50-concurrent goal is
    # a queue/horizontal-scaling concern, not a max_jobs knob on one process.
    max_jobs = 6
    job_timeout = 4 * 60 * 60
    keep_result = 86400  # 24 hours
    max_tries = 3
    queue_name = "arq:reports"
