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

        # Fail webhook called with coded error message (FINDINGS P2:
        # services/error_codes.py — prefiks [CODE] przed treścią błędu)
        mock_convex.cennik_fail.assert_awaited_once_with(
            "audit-2", "[INTERNAL] pipeline boom"
        )
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

        # Cancelled fail webhook (kod [CANCELLED] — FINDINGS P2)
        mock_convex.cennik_fail.assert_awaited_once_with(
            "audit-cancel", "[CANCELLED] Anulowane przez użytkownika"
        )
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


# ---------------------------------------------------------------------------
# run_competitor_report_task — job_id threading (quick 260613-m23 Task 3)
# ---------------------------------------------------------------------------


class TestRunCompetitorReportTask:
    @pytest.mark.asyncio
    async def test_passes_job_id_to_pipeline(self):
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7,
            "narrative": "n",
            "swot_item_count": 0,
            "recommendation_count": 0,
            "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)  # no cancel
        ctx = {"redis": redis, "job_id": "job-3"}
        request = {"auditId": "audit-1", "userId": "user-1"}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        mock_pipeline.assert_awaited_once()
        assert mock_pipeline.await_args.kwargs["job_id"] == "job-3"

    @pytest.mark.asyncio
    async def test_logs_queue_depth_on_start(self, caplog):
        """Best-effort queue-depth probe ZCARDs arq:queue on start (P3)."""
        import logging

        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.zcard = AsyncMock(return_value=4)
        ctx = {"redis": redis, "job_id": "job-q"}
        request = {"auditId": "audit-1", "userId": "user-1"}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
            caplog.at_level(logging.INFO),
        ):
            await run_competitor_report_task(ctx, request)

        redis.zcard.assert_awaited_once_with("arq:queue")
        assert any("queue depth=4" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_queue_depth_probe_never_crashes_task(self):
        """A Redis hiccup on the queue-depth probe must not fail the task."""
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.zcard = AsyncMock(side_effect=ConnectionError("redis down"))
        ctx = {"redis": redis, "job_id": "job-q2"}
        request = {"auditId": "audit-1", "userId": "user-1"}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            # Must complete despite the probe error.
            await run_competitor_report_task(ctx, request)
        mock_pipeline.assert_awaited_once()

    # --- queue slot release (beads BEAUTY_AUDIT-1mb) ---

    @pytest.mark.asyncio
    async def test_completes_queue_slot_on_success(self):
        """With _queue_id present, success releases the slot (p_error=None)."""
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        mock_sb = MagicMock()  # SupabaseService instance (sync .rpc(...).execute())
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)  # no cancel
        ctx = {"redis": redis, "job_id": "job-c1", "supabase": mock_sb}
        request = {"auditId": "a", "userId": "u", "_queue_id": 42}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        mock_sb.rpc.assert_called_once_with(
            "complete_competitor_report_job",
            {"p_id": 42, "p_error": None},
        )

    @pytest.mark.asyncio
    async def test_completes_queue_slot_on_failure(self):
        """A pipeline failure still releases the slot (p_error = coded msg)."""
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(side_effect=RuntimeError("boom"))
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        mock_sb = MagicMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-c2", "supabase": mock_sb}
        request = {"auditId": "a", "userId": "u", "_queue_id": 43}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
            pytest.raises(RuntimeError),
        ):
            await run_competitor_report_task(ctx, request)

        assert mock_sb.rpc.call_count == 1
        name, params = mock_sb.rpc.call_args.args
        assert name == "complete_competitor_report_job"
        assert params["p_id"] == 43
        # slot released even on failure — error string is non-empty
        assert isinstance(params["p_error"], str) and params["p_error"]

    @pytest.mark.asyncio
    async def test_completes_queue_slot_unwraps_supabase_service(self):
        """Regression (2026-06-25): in prod ctx['supabase'] is a SupabaseService
        wrapper that has NO top-level `.rpc` — only `.client` (the raw Supabase
        Client). Pre-fix the finally block called `sb_client.rpc(...)` directly
        on the wrapper → AttributeError, queue row left orphaned until mig 135
        self-heal kicked in after ~8 min. The unwrap branch must call `.rpc` on
        the underlying client so the slot is released cleanly on the first try.
        """
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 9, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        # Simulate prod SupabaseService — spec=['client'] makes hasattr(svc, 'rpc')
        # False (matching the real wrapper class, which only exposes `.client`).
        mock_raw_client = MagicMock()
        mock_service = MagicMock(spec=['client'])
        mock_service.client = mock_raw_client

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-c4", "supabase": mock_service}
        request = {"auditId": "a", "userId": "u", "_queue_id": 44}

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        # The finally block unwrapped to the raw client and released the slot.
        mock_raw_client.rpc.assert_called_once_with(
            "complete_competitor_report_job",
            {"p_id": 44, "p_error": None},
        )

    @pytest.mark.asyncio
    async def test_targets_convex_at_callback_url_when_provided(self):
        """Webhook callback routing (migration 148): when the payload carries
        convexSiteUrl, the task must build ConvexClient(base_url=<that url>) so
        progress/complete/fail webhooks return to the SAME deployment that
        started the job (dev vs prod), not the global settings.convex_url."""
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-cb"}
        request = {
            "auditId": "a",
            "userId": "u",
            "convexSiteUrl": "https://reliable-scorpion-10.convex.site",
        }

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        mock_convex_cls.assert_called_once_with(
            base_url="https://reliable-scorpion-10.convex.site",
        )

    @pytest.mark.asyncio
    async def test_targets_global_convex_when_no_callback_url(self):
        """Back-compat: a payload without convexSiteUrl (legacy row / old Convex)
        builds ConvexClient(base_url=None) → falls back to settings.convex_url."""
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-cb2"}
        request = {"auditId": "a", "userId": "u"}  # no convexSiteUrl

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        mock_convex_cls.assert_called_once_with(base_url=None)

    @pytest.mark.asyncio
    async def test_no_queue_id_skips_completion(self):
        """Back-compat: without _queue_id the completion hook is a no-op.

        The 3 pre-existing tests run this path (ctx has no 'supabase' key, the
        request has no _queue_id) — the hook must NOT construct a client or call
        the RPC when _queue_id is absent.
        """
        from workers.tasks import run_competitor_report_task

        mock_pipeline = AsyncMock(return_value={
            "report_id": 7, "narrative": "n", "swot_item_count": 0,
            "recommendation_count": 0, "used_fallback": False,
        })
        mock_convex = MagicMock()
        mock_convex.competitor_report_progress = AsyncMock()
        mock_convex.competitor_report_complete = AsyncMock()
        mock_convex.competitor_report_fail = AsyncMock()
        mock_convex_cls = MagicMock(return_value=mock_convex)

        mock_sb = MagicMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        ctx = {"redis": redis, "job_id": "job-c3", "supabase": mock_sb}
        request = {"auditId": "a", "userId": "u"}  # NO _queue_id

        with (
            patch(
                "pipelines.competitor_report.run_competitor_report_pipeline",
                mock_pipeline,
            ),
            patch("services.convex.ConvexClient", mock_convex_cls),
        ):
            await run_competitor_report_task(ctx, request)

        # No completion RPC when _queue_id is absent.
        for call in mock_sb.rpc.call_args_list:
            assert call.args[0] != "complete_competitor_report_job"


class TestCompetitorQueueTasksRegistered:
    def test_worker_settings_includes_competitor_queue_tasks(self):
        from workers.competitor_report_queue import ALL_COMPETITOR_QUEUE_TASKS
        from workers.main import WorkerSettings

        for task in ALL_COMPETITOR_QUEUE_TASKS:
            assert task in WorkerSettings.functions, (
                f"{task.__name__} not registered in WorkerSettings.functions"
            )
