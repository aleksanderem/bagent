"""arq worker pool — persistent task queue for bagent pipelines.

This module provides a Redis-backed task queue (via arq) that replaces the
in-memory FastAPI BackgroundTasks model used previously in `server.py`. The
purpose is durability: when a deploy of the bagent process happens (pm2
restart), in-flight jobs in BackgroundTasks die silently, leaving customers
with stuck "generating..." states forever. With arq, jobs are persisted to
Redis at enqueue time and survive worker restarts (configurable retry +
checkpoint patterns).

Iteration #20 (this PR) sets up the foundation only — Redis connection,
WorkerSettings, lifecycle hooks, an example task, and Redis health check.
The actual migration of all 7 background_tasks endpoints in server.py
happens in iteration #21.
"""

from .main import (
    WorkerSettings,
    get_redis_pool,
    redis_settings,
    smoke_test,
)
from .tasks import (
    ALL_TASKS,
    clear_cancel_flag,
    run_cennik_task,
    run_competitor_refresh_task,
    run_competitor_report_task,
    run_free_report_task,
    run_report_task,
    run_summary_task,
    run_versum_suggest_task,
    set_cancel_flag,
)

__all__ = [
    "WorkerSettings",
    "get_redis_pool",
    "redis_settings",
    "smoke_test",
    "ALL_TASKS",
    "set_cancel_flag",
    "clear_cancel_flag",
    "run_report_task",
    "run_free_report_task",
    "run_cennik_task",
    "run_summary_task",
    "run_competitor_report_task",
    "run_competitor_refresh_task",
    "run_versum_suggest_task",
]
