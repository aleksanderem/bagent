"""Liveness-heartbeat tests for the dedicated report worker.

Since 2026-06-15 the report worker (ReportWorkerSettings, PM2 process
bagent-report-worker) processes ALL user-facing report jobs — audits
(run_report_task), free report, cennik, summary, versum, competitor reports —
on its own "arq:reports" queue. The scrape worker's heartbeat
(worker_heartbeat → HC_PING_BAGENT_WORKER_HEARTBEAT) says NOTHING about whether
the report worker is alive. Without an independent heartbeat the report worker
could die while the scrape worker keeps pinging green, and every audit would
silently stall behind a dead consumer.

These tests pin the invariant: the report worker fires its OWN heartbeat
(report_worker_heartbeat → HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT) and the
scrape heartbeat stays scoped to the scrape worker only.

Test isolation note (mirrors test_workers.py): all imports happen inside test
bodies and no os.environ mutation happens at module-collection time, so
importing `workers`/`config` here does not pin settings.api_key for other
suites.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


def _cron_function_names(cron_jobs) -> set[str]:
    """Names of the coroutine each CronJob actually fires.

    arq's cron() resolves a dotted-string reference to the real function at
    cron-construction time and stores it on cj.coroutine, so cj.coroutine
    .__name__ is what the scheduler will execute (not just what's registered
    in functions)."""
    return {getattr(cj.coroutine, "__name__", "") for cj in cron_jobs}


class TestHeartbeatFunctions:
    @pytest.mark.asyncio
    async def test_report_heartbeat_pings_report_worker_env_var(self):
        from workers.main import report_worker_heartbeat

        with patch("services.healthcheck.ping", new=AsyncMock()) as mock_ping:
            result = await report_worker_heartbeat({})

        assert result == "ok"
        mock_ping.assert_awaited_once_with("HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT")

    @pytest.mark.asyncio
    async def test_scrape_heartbeat_still_pings_scrape_worker_env_var(self):
        # Regression guard: the scrape heartbeat must NOT be repointed at the
        # report worker's check — they are distinct liveness signals.
        from workers.main import worker_heartbeat

        with patch("services.healthcheck.ping", new=AsyncMock()) as mock_ping:
            result = await worker_heartbeat({})

        assert result == "ok"
        mock_ping.assert_awaited_once_with("HC_PING_BAGENT_WORKER_HEARTBEAT")


class TestReportWorkerRegistration:
    def test_report_worker_registers_its_heartbeat(self):
        from workers.main import ReportWorkerSettings, report_worker_heartbeat

        assert report_worker_heartbeat in ReportWorkerSettings.functions

    def test_report_worker_does_not_register_scrape_heartbeat(self):
        # The scrape heartbeat is the scrape worker's concern. Keeping it off
        # the report worker makes the scoping explicit (it was dead weight here
        # anyway — REPORT_CRONS never fired it).
        from workers.main import ReportWorkerSettings, worker_heartbeat

        assert worker_heartbeat not in ReportWorkerSettings.functions


class TestCronWiring:
    def test_report_crons_fire_report_heartbeat(self):
        from workers.main import REPORT_CRONS

        names = _cron_function_names(REPORT_CRONS)
        assert "report_worker_heartbeat" in names, (
            "ReportWorkerSettings.cron_jobs must include a cron that fires "
            "report_worker_heartbeat — being in functions does NOT fire it; "
            "arq only runs crons listed in cron_jobs."
        )

    def test_report_crons_do_not_fire_scrape_heartbeat(self):
        from workers.main import REPORT_CRONS

        assert "worker_heartbeat" not in _cron_function_names(REPORT_CRONS)

    def test_scrape_crons_fire_scrape_heartbeat_only(self):
        # The scrape heartbeat stays scoped to the scrape worker: fired by
        # SCRAPE_CRONS, and the report heartbeat is NOT fired here.
        from workers.main import SCRAPE_CRONS

        names = _cron_function_names(SCRAPE_CRONS)
        assert "worker_heartbeat" in names
        assert "report_worker_heartbeat" not in names

    def test_report_heartbeat_runs_every_5_minutes(self):
        from workers.main import REPORT_CRONS

        hb = next(
            cj
            for cj in REPORT_CRONS
            if getattr(cj.coroutine, "__name__", "") == "report_worker_heartbeat"
        )
        # Same cadence as the scrape heartbeat: every 5 minutes.
        assert hb.minute == {i for i in range(0, 60, 5)}
