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


async def shutdown(ctx: dict[str, Any]) -> None:
    logger.info("arq worker shutting down")


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
# job_timeout: 30 minutes — competitor reports take ~5min, audit ~15min,
# leaving ample headroom. Anything taking >30min is stuck and should fail.
# keep_result: 24h — long enough that frontend polling for status after a
# slow job still works, short enough that result store doesn't bloat.

class WorkerSettings:
    functions = [smoke_test]
    redis_settings = redis_settings
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 20
    job_timeout = 1800  # 30 minutes
    keep_result = 86400  # 24 hours
    max_tries = 3
    # TODO(#21): namespace by environment (e.g. "arq:prod:queue") so
    # accidental shared Redis between dev/staging/prod doesn't cause workers
    # to steal each other's jobs. For now we use the arq default since we
    # only have one environment running — production on tytan.
    queue_name = "arq:queue"
