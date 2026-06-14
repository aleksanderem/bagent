"""Unit tests for bagent.workers.competitor_report_queue — controlled-concurrency
competitor report drain (beads BEAUTY_AUDIT-1mb).

These tests mock the Supabase RPC layer (claim/reap) so they pass WITHOUT the
migration applied. The drain math (cap is enforced DB-side in
claim_competitor_report_jobs) is verified here at the worker layer: we assert
the drain passes the cap to the claim RPC and enqueues exactly one
run_competitor_report_task per claimed row, threading _job_id=arq_job_id (so
frontend polling holds) + _queue_id (so the task can release its slot).

Import discipline mirrors test_workers.py: NO os.environ mutation at module
level, and all `workers.*` imports happen inside test bodies so collecting this
file never pins Settings for other tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_claim_response(rows: list[dict]):
    """Build a mock supabase client whose claim RPC returns `rows`.

    Mirrors the supabase-py chain: client.rpc(name, params).execute().data
    """
    execute_result = MagicMock()
    execute_result.data = rows
    rpc_chain = MagicMock()
    rpc_chain.execute = MagicMock(return_value=execute_result)
    client = MagicMock()
    client.rpc = MagicMock(return_value=rpc_chain)
    return client


class TestDrainCompetitorReportQueue:
    @pytest.mark.asyncio
    async def test_drain_enqueues_one_job_per_claimed_row(self):
        from workers.competitor_report_queue import drain_competitor_report_queue

        rows = [
            {
                "id": 10 + i,
                "audit_id": f"aud{i}",
                "user_id": "u",
                "tier": "base",
                "selection_mode": "auto",
                "target_count": 5,
                "arq_job_id": f"uuid-{i}",
            }
            for i in range(3)
        ]
        client = _make_claim_response(rows)

        redis = AsyncMock()
        redis.enqueue_job = AsyncMock(return_value=MagicMock())
        ctx = {"redis": redis}

        with patch(
            "workers.competitor_report_queue._get_client", return_value=client
        ):
            result = await drain_competitor_report_queue(ctx)

        assert result == {"claimed": 3, "enqueued": 3}
        assert redis.enqueue_job.await_count == 3

        for i, call in enumerate(redis.enqueue_job.await_args_list):
            args, kwargs = call
            # positional task name
            assert args[0] == "run_competitor_report_task"
            payload = args[1]
            assert payload["auditId"] == f"aud{i}"
            assert payload["userId"] == "u"
            assert payload["tier"] == "base"
            assert payload["selectionMode"] == "auto"
            assert payload["targetCount"] == 5
            assert payload["_queue_id"] == 10 + i
            # _job_id MUST be the row's arq_job_id so frontend polling holds
            assert kwargs["_job_id"] == f"uuid-{i}"

    @pytest.mark.asyncio
    async def test_drain_passes_cap_to_claim_rpc(self):
        from workers.competitor_report_queue import (
            COMPETITOR_REPORT_MAX_CONCURRENT,
            drain_competitor_report_queue,
        )
        from config import settings

        client = _make_claim_response([])
        redis = AsyncMock()
        redis.enqueue_job = AsyncMock()
        ctx = {"redis": redis}

        with patch(
            "workers.competitor_report_queue._get_client", return_value=client
        ):
            await drain_competitor_report_queue(ctx)

        client.rpc.assert_called_once_with(
            "claim_competitor_report_jobs",
            {
                "p_cap": COMPETITOR_REPORT_MAX_CONCURRENT,
                "p_worker_id": settings.scrape_worker_id,
            },
        )

    @pytest.mark.asyncio
    async def test_drain_empty_when_cap_full(self):
        from workers.competitor_report_queue import drain_competitor_report_queue

        # cap full → claim RPC returns the empty set
        client = _make_claim_response([])
        redis = AsyncMock()
        redis.enqueue_job = AsyncMock()
        ctx = {"redis": redis}

        with patch(
            "workers.competitor_report_queue._get_client", return_value=client
        ):
            result = await drain_competitor_report_queue(ctx)

        assert result == {"claimed": 0, "enqueued": 0}
        redis.enqueue_job.assert_not_awaited()


class TestCapResolution:
    def test_cap_default_is_4(self, monkeypatch):
        monkeypatch.delenv("COMPETITOR_REPORT_MAX_CONCURRENT", raising=False)
        from workers.competitor_report_queue import _read_cap

        assert _read_cap() == 4

    def test_cap_env_tunable(self, monkeypatch):
        monkeypatch.setenv("COMPETITOR_REPORT_MAX_CONCURRENT", "7")
        from workers.competitor_report_queue import _read_cap

        assert _read_cap() == 7

    def test_cap_floored_at_1(self, monkeypatch):
        monkeypatch.setenv("COMPETITOR_REPORT_MAX_CONCURRENT", "0")
        from workers.competitor_report_queue import _read_cap

        assert _read_cap() == 1

        monkeypatch.setenv("COMPETITOR_REPORT_MAX_CONCURRENT", "-5")
        assert _read_cap() == 1

    def test_cap_bad_value_falls_back_to_4(self, monkeypatch):
        monkeypatch.setenv("COMPETITOR_REPORT_MAX_CONCURRENT", "not-a-number")
        from workers.competitor_report_queue import _read_cap

        assert _read_cap() == 4


class TestReapStuckCompetitorReportJobs:
    @pytest.mark.asyncio
    async def test_reap_calls_rpc_and_returns_count(self):
        from workers.competitor_report_queue import (
            reap_stuck_competitor_report_jobs,
        )

        client = _make_claim_response(2)  # .data == 2 (int count)
        ctx = {}

        with patch(
            "workers.competitor_report_queue._get_client", return_value=client
        ):
            result = await reap_stuck_competitor_report_jobs(ctx)

        assert result == {"reaped": 2}
        client.rpc.assert_called_once_with("reap_stuck_competitor_report_jobs", {})


class TestAllCompetitorQueueTasks:
    def test_all_competitor_queue_tasks_exported(self):
        from workers.competitor_report_queue import ALL_COMPETITOR_QUEUE_TASKS

        names = {t.__name__ for t in ALL_COMPETITOR_QUEUE_TASKS}
        assert names == {
            "drain_competitor_report_queue",
            "reap_stuck_competitor_report_jobs",
        }
