"""Unit tests for bagent.workers — arq foundation (issue #20).

CRITICAL test isolation note: every import of `workers` or anything that
chains through `config` triggers `Settings()` which reads `.env` and pins
`settings.api_key` for the rest of the process. Other tests (notably
test_free_report) use `os.environ.setdefault("API_KEY", "test-api-key-...")`
at their module-collection time to spoof the auth header — that pattern
ONLY works if no module has loaded `config` first.

Therefore: this file does NOT touch os.environ, does NOT import workers
at module level, and ALL imports happen inside test function bodies.
This lets pytest collect test_workers without polluting the env, leaving
test_free_report free to do its setdefault dance regardless of test order.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest


class TestWorkerSettingsShape:
    def test_settings_has_smoke_test_in_functions(self):
        from workers.main import WorkerSettings, smoke_test

        # Foundation iteration: only the smoke_test task is registered. The
        # 7 production pipelines (audit/competitor/optimization) move into
        # the functions list during issue #21.
        assert smoke_test in WorkerSettings.functions

    def test_settings_has_lifecycle_hooks(self):
        from workers.main import WorkerSettings

        assert WorkerSettings.on_startup is not None
        assert WorkerSettings.on_shutdown is not None

    def test_max_jobs_reasonable_for_io_bound_load(self):
        from workers.main import WorkerSettings

        # Bagent pipelines are HTTP-to-MiniMax bound (network IO), so
        # 20 concurrent tasks per worker is a sane starting point.
        assert WorkerSettings.max_jobs == 20

    def test_job_timeout_long_enough_for_competitor_report(self):
        from workers.main import WorkerSettings

        # Competitor report ~5 min, audit pipeline up to ~15 min,
        # optimization ~10 min. 30 min ceiling leaves room.
        assert WorkerSettings.job_timeout >= 15 * 60
        assert WorkerSettings.job_timeout <= 60 * 60

    def test_keep_result_24h(self):
        from workers.main import WorkerSettings

        # Frontend can poll status long after a slow job. 24h is enough
        # for users who close the tab and come back the next morning.
        assert WorkerSettings.keep_result == 86400

    def test_redis_settings_uses_config(self):
        # The settings object should reflect what config.Settings provides.
        from config import settings
        from workers.main import redis_settings

        assert redis_settings.host == settings.redis_host
        assert redis_settings.port == settings.redis_port
        assert redis_settings.database == settings.redis_database
        # Empty password in config.Settings → None in RedisSettings
        if settings.redis_password:
            assert redis_settings.password == settings.redis_password
        else:
            assert redis_settings.password is None


class TestSmokeTestTask:
    @pytest.mark.asyncio
    async def test_smoke_test_returns_ack_with_echo(self):
        from workers.main import smoke_test

        ctx = {"job_id": "test-job-123", "job_try": 1, "redis": None}
        result = await smoke_test(ctx, "hello world")
        assert result["ack"] is True
        assert result["echo"] == "hello world"
        assert result["job_id"] == "test-job-123"
        assert result["try"] == 1

    @pytest.mark.asyncio
    async def test_smoke_test_uses_default_message(self):
        from workers.main import smoke_test

        ctx = {"job_id": "x", "job_try": 1, "redis": None}
        result = await smoke_test(ctx)
        assert result["echo"] == "hello"

    @pytest.mark.asyncio
    async def test_smoke_test_handles_missing_ctx_keys(self):
        # Defensive: in tests/dev where ctx might not have job_id/job_try,
        # we want sensible defaults rather than KeyError.
        from workers.main import smoke_test

        ctx = {"redis": None}
        result = await smoke_test(ctx, "x")
        assert result["job_id"] == "?"
        assert result["try"] == 1


class TestStartupHook:
    @pytest.mark.asyncio
    async def test_startup_pings_redis(self):
        # Startup must call redis.ping() to verify connectivity. This is
        # the first signal in worker logs that the worker is alive AND
        # connected. Without a successful ping, the worker is useless.
        from workers.main import startup

        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(return_value=True)
        ctx = {"redis": mock_redis}

        await startup(ctx)

        mock_redis.ping.assert_awaited_once()


class TestExports:
    def test_init_exports_public_api(self):
        # Anything imported from `bagent.workers` rather than the inner
        # `workers.main` is part of the public API. Keep it minimal.
        import workers

        # Use the already-cached module — re-importing via importlib.reload
        # would re-execute module-level code (RedisSettings construction,
        # config read) and could leak state into subsequent tests.
        assert hasattr(workers, "WorkerSettings")
        assert hasattr(workers, "get_redis_pool")
        assert hasattr(workers, "redis_settings")
        assert hasattr(workers, "smoke_test")
