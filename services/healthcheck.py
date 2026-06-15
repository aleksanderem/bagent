"""Shared Healthchecks.io / self-hosted healthchecks ping helper.

Originally lived inside ``workers/scrape_refresh.py`` (issue #25) — promoted
to a shared service module so taxonomy/discovery/outreach crons can ping
their own checks without duplicating the helper.

Usage
-----

In .env (one line per check):

    HC_PING_TAXONOMY_VIEWS_REFRESH=https://healthchecks.booksyaudit.pl/ping/<uuid>
    HC_PING_BAGENT_WORKER_HEARTBEAT=https://healthchecks.booksyaudit.pl/ping/<uuid>
    HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT=https://healthchecks.booksyaudit.pl/ping/<uuid>

Two separate worker-liveness checks (since 2026-06-15 there are two worker
processes): HC_PING_BAGENT_WORKER_HEARTBEAT is pinged only by the scrape worker
(WorkerSettings / arq:queue) via worker_heartbeat, and
HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT only by the report worker
(ReportWorkerSettings / arq:reports — drains every audit + on-demand report) via
report_worker_heartbeat. Each needs its own Healthchecks check (~10 min grace);
one going green tells you nothing about the other.

In an arq cron task:

    from services.healthcheck import ping

    async def my_cron(ctx):
        try:
            result = do_work()
            await ping("HC_PING_TAXONOMY_VIEWS_REFRESH")
            return result
        except Exception:
            await ping("HC_PING_TAXONOMY_VIEWS_REFRESH", fail=True)
            raise

Design notes
------------
* Fire-and-forget: a healthcheck ping failure must NEVER propagate to the
  cron itself. Healthchecks already alerts on missing pings.
* No-op when env var unset: lets local dev work without HC wiring.
* HEAD request with a short 5s timeout — Healthchecks accepts any method.
* Optional ``fail=True`` appends ``/fail`` to mark the check as down
  (used in except blocks).

Operational reference
---------------------
Self-hosted instance: https://healthchecks.booksyaudit.pl/
Ping host:            https://healthchecks.booksyaudit.pl/ping/
Project API key:      see Healthchecks admin (project "BeautyAudit")
"""

from __future__ import annotations

import logging
import os

import httpx

logger = logging.getLogger("services.healthcheck")


async def ping(env_var_name: str, *, fail: bool = False) -> None:
    """Fire-and-forget Healthchecks ping.

    Parameters
    ----------
    env_var_name : str
        Name of the env var holding the full ping URL (e.g. ``HC_PING_FOO``).
        When unset or empty, the function is a no-op — local dev / fresh
        deploys work without observability wired up.
    fail : bool, default False
        When True, hits ``<ping_url>/fail`` to mark the check as down.
        Use inside ``except`` handlers so a failing cron actively alerts
        instead of silently missing its next expected ping.
    """
    url = os.environ.get(env_var_name)
    if not url:
        return
    if fail:
        url = url.rstrip("/") + "/fail"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.get(url)
    except Exception as e:  # noqa: BLE001
        logger.warning("healthcheck ping (%s) failed: %s", env_var_name, e)


__all__ = ["ping"]
