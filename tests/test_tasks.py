"""Unit tests for bagent.workers.tasks — production pipeline tasks (issue #21).

Each task is the migration target of the corresponding `run_*_job`
function previously in server.py. The test strategy is to mock the
pipeline + Convex client + Supabase service so we verify orchestration
(enqueue → progress → save → webhook → return) without spinning up
external systems.

Cancel propagation tests verify the Redis-flag pattern: when the
cancel flag is set in ctx['redis'], on_progress() raises _CancelledByUser
and the task hits the cancelled branch (cancel flag cleared, fail
webhook called, exception re-raised so arq marks the job failed).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Cancel flag helpers
# ---------------------------------------------------------------------------


class TestCancelFlagHelpers:
    @pytest.mark.asyncio
    async def test_set_cancel_flag_writes_redis_key_with_ttl(self):
        from workers.tasks import CANCEL_KEY_PREFIX, CANCEL_KEY_TTL, set_cancel_flag

        redis = AsyncMock()
        await set_cancel_flag(redis, "job-abc")

        redis.set.assert_awaited_once_with(
            f"{CANCEL_KEY_PREFIX}job-abc", "1", ex=CANCEL_KEY_TTL,
        )

    @pytest.mark.asyncio
    async def test_clear_cancel_flag_deletes_redis_key(self):
        from workers.tasks import CANCEL_KEY_PREFIX, clear_cancel_flag

        redis = AsyncMock()
        await clear_cancel_flag(redis, "job-abc")

        redis.delete.assert_awaited_once_with(f"{CANCEL_KEY_PREFIX}job-abc")

    @pytest.mark.asyncio
    async def test_clear_cancel_flag_swallows_redis_errors(self):
        # Defensive: clear is fire-and-forget — never block job completion
        # on a Redis hiccup during cleanup.
        from workers.tasks import clear_cancel_flag

        redis = AsyncMock()
        redis.delete.side_effect = ConnectionError("redis down")

        # Must not raise
        await clear_cancel_flag(redis, "job-abc")

    @pytest.mark.asyncio
    async def test_check_cancel_returns_true_when_flag_set(self):
        from workers.tasks import _check_cancel

        redis = AsyncMock()
        redis.get = AsyncMock(return_value="1")
        ctx = {"redis": redis, "job_id": "job-abc"}

        assert await _check_cancel(ctx) is True

    @pytest.mark.asyncio
    async def test_check_cancel_returns_false_when_no_flag(self):
        from workers.tasks import _check_cancel

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-abc"}

        assert await _check_cancel(ctx) is False

    @pytest.mark.asyncio
    async def test_check_cancel_returns_false_on_redis_error(self):
        # Defensive: don't fail the task on a cancel-check Redis hiccup.
        from workers.tasks import _check_cancel

        redis = AsyncMock()
        redis.get = AsyncMock(side_effect=ConnectionError("redis down"))
        ctx = {"redis": redis, "job_id": "job-abc"}

        assert await _check_cancel(ctx) is False


# ---------------------------------------------------------------------------
# All-tasks registry
# ---------------------------------------------------------------------------


class TestAllTasksRegistry:
    def test_all_seven_tasks_exported(self):
        from workers.tasks import ALL_TASKS

        names = {t.__name__ for t in ALL_TASKS}
        assert names == {
            "run_report_task",
            "run_free_report_task",
            "run_cennik_task",
            "run_summary_task",
            "run_competitor_report_task",
            "run_competitor_refresh_task",
            "run_versum_suggest_task",
        }

    def test_worker_settings_includes_all_tasks(self):
        from workers.main import WorkerSettings
        from workers.tasks import ALL_TASKS

        for task in ALL_TASKS:
            assert task in WorkerSettings.functions, (
                f"{task.__name__} not registered in WorkerSettings.functions"
            )


# ---------------------------------------------------------------------------
# run_cennik_task — simplest pipeline (no scrape loading, no Pydantic
# reconstruct on input), good representative test
# ---------------------------------------------------------------------------


class TestRunCennikTask:
    @pytest.mark.asyncio
    async def test_happy_path_calls_pipeline_and_webhook(self):
        from workers.tasks import run_cennik_task

        mock_pipeline = AsyncMock(return_value={
            "category_proposal": {"some": "proposal"},
            "stats": {"changed": 5},
        })
        mock_convex_cls = MagicMock()
        mock_convex = MagicMock()
        mock_convex.cennik_progress = AsyncMock()
        mock_convex.cennik_complete = AsyncMock()
        mock_convex.cennik_fail = AsyncMock()
        mock_convex_cls.return_value = mock_convex

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)  # no cancel
        ctx = {"redis": redis, "job_id": "job-cennik-1"}
        request = {"auditId": "audit-1", "userId": "user-1"}

        with (
            patch("pipelines.cennik.run_cennik_pipeline", mock_pipeline),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            result = await run_cennik_task(ctx, request)

        # Pipeline called with auditId + on_progress callback
        mock_pipeline.assert_awaited_once()
        call_kwargs = mock_pipeline.await_args.kwargs
        assert call_kwargs["audit_id"] == "audit-1"
        assert callable(call_kwargs["on_progress"])

        # Complete webhook fired with category_proposal + stats
        mock_convex.cennik_complete.assert_awaited_once_with(
            "audit-1",
            category_proposal={"some": "proposal"},
            stats={"changed": 5},
        )
        # Fail webhook NOT fired on happy path
        mock_convex.cennik_fail.assert_not_awaited()

        # Result is the pipeline's dict
        assert result["category_proposal"] == {"some": "proposal"}

    @pytest.mark.asyncio
    async def test_pipeline_exception_calls_fail_webhook_and_reraises(self):
        from workers.tasks import run_cennik_task

        mock_pipeline = AsyncMock(side_effect=RuntimeError("pipeline boom"))
        mock_convex = MagicMock()
        mock_convex.cennik_progress = AsyncMock()
        mock_convex.cennik_complete = AsyncMock()
        mock_convex.cennik_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        ctx = {"redis": redis, "job_id": "job-cennik-2"}
        request = {"auditId": "audit-2", "userId": "user-2"}

        with (
            patch("pipelines.cennik.run_cennik_pipeline", mock_pipeline),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            with pytest.raises(RuntimeError, match="pipeline boom"):
                await run_cennik_task(ctx, request)

        # Fail webhook called with error message
        mock_convex.cennik_fail.assert_awaited_once_with("audit-2", "pipeline boom")
        # Complete webhook NOT called
        mock_convex.cennik_complete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_during_progress_calls_fail_webhook(self):
        # Simulate cancel flag set after first progress call
        from workers.tasks import _CancelledByUser, run_cennik_task

        async def pipeline_with_progress(audit_id, on_progress):
            # Trigger a progress callback — this should detect cancel
            await on_progress(50, "halfway")
            return {"category_proposal": None, "stats": {}}

        mock_convex = MagicMock()
        mock_convex.cennik_progress = AsyncMock()
        mock_convex.cennik_complete = AsyncMock()
        mock_convex.cennik_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        # Cancel flag IS set
        redis.get = AsyncMock(return_value="1")
        redis.set = AsyncMock()
        redis.delete = AsyncMock()
        ctx = {"redis": redis, "job_id": "job-cancel-1"}
        request = {"auditId": "audit-cancel", "userId": "user-1"}

        with (
            patch("pipelines.cennik.run_cennik_pipeline", side_effect=pipeline_with_progress),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            with pytest.raises(_CancelledByUser):
                await run_cennik_task(ctx, request)

        # Cancelled fail webhook
        mock_convex.cennik_fail.assert_awaited_once_with("audit-cancel", "Cancelled by user")
        # Cancel flag was cleared
        redis.delete.assert_awaited()


# ---------------------------------------------------------------------------
# run_versum_suggest_task — returns suggestions list, no Convex webhooks
# ---------------------------------------------------------------------------


class TestRunVersumSuggestTask:
    @pytest.mark.asyncio
    async def test_returns_suggestions_from_pipeline(self):
        from workers.tasks import run_versum_suggest_task

        mock_suggest = AsyncMock(return_value=[
            {"name": "Manicure", "matched_taxonomy_id": 123},
            {"name": "Pedicure", "matched_taxonomy_id": 456},
        ])

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-versum-1"}
        request = {
            "services": [{"id": 1, "name": "Manicure"}],
            "taxonomy": [{"id": 123, "name": "Mani"}],
        }

        with patch("pipelines.versum_suggest.suggest_versum_mappings", mock_suggest):
            result = await run_versum_suggest_task(ctx, request)

        assert len(result) == 2
        assert result[0]["matched_taxonomy_id"] == 123


# ---------------------------------------------------------------------------
# run_competitor_refresh_task — no Convex webhooks at all
# ---------------------------------------------------------------------------


class TestRunCompetitorRefreshTask:
    @pytest.mark.asyncio
    async def test_no_convex_webhooks(self):
        from workers.tasks import run_competitor_refresh_task

        mock_refresh = AsyncMock(return_value={
            "snapshot_date": "2026-05-01",
            "next_refresh_at": "2026-05-08",
            "significant_change_count": 3,
        })

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-refresh-1"}
        request = {"reportId": 42, "auditId": "a-1", "userId": "u-1"}

        # Patch ConvexClient to assert it's never instantiated for refresh
        mock_convex_cls = MagicMock()

        with (
            patch("pipelines.competitor_report_refresh.run_refresh", mock_refresh),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            result = await run_competitor_refresh_task(ctx, request)

        # Refresh was called with report_id (not auditId)
        mock_refresh.assert_awaited_once()
        assert mock_refresh.await_args.kwargs["report_id"] == 42

        # ConvexClient was never instantiated for refresh
        mock_convex_cls.assert_not_called()

        assert result["significant_change_count"] == 3
