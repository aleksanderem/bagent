"""S0055 — tests for tier-0 monitoring watchlist refresh.

Covers:
* scheduler._tier_zero_due — RPC happy path + graceful degradation when
  migration 126 isn't yet applied
* workers.scrape_refresh._maybe_trigger_monitoring_diff — no-op when salon
  isn't monitored, alert assembly when services added, no POST when no diffs
* workers.scrape_refresh._severity_for_price_change — boundary tests for
  the 20%/5% thresholds

Mocks supabase Client + httpx.AsyncClient. Sync supabase calls are made on
the real (mocked) client object; async httpx calls use unittest.mock.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import scheduler module-level (no env side-effects beyond config load).
from scheduler.refresh_scheduler import _tier_zero_due, TIER_BATCH_LIMIT
from workers.scrape_refresh import (
    _build_monitoring_alerts,
    _maybe_trigger_monitoring_diff,
    _severity_for_price_change,
)


# ---------------------------------------------------------------------------
# Helpers — RPC/table response builders
# ---------------------------------------------------------------------------


def _rpc_result(data):
    """Mock supabase client .rpc(...).execute() chain returning SimpleNamespace."""
    return SimpleNamespace(data=data)


def _make_client_rpc(rpc_responses: dict):
    """Build a MagicMock client whose .rpc(name, args).execute() returns
    rpc_responses[name]. Raises whatever exception is mapped when the value is
    an Exception instance.
    """
    def _rpc_side(name, _args=None):
        executable = MagicMock()
        resp = rpc_responses.get(name)
        if isinstance(resp, Exception):
            executable.execute.side_effect = resp
        else:
            executable.execute.return_value = _rpc_result(resp)
        return executable

    client = MagicMock()
    client.rpc.side_effect = _rpc_side
    return client


def _make_table_query_chain(rows):
    """Build a table query chain that survives any .select/.eq/.order/.limit
    calls and returns rows on .execute()."""
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.order.return_value = chain
    chain.limit.return_value = chain
    chain.execute.return_value = _rpc_result(rows)
    return chain


# ---------------------------------------------------------------------------
# C1 — _tier_zero_due
# ---------------------------------------------------------------------------


class TestTierZeroDue:
    def test_returns_list_when_rpc_responds(self):
        """RPC returns rows → resolver extracts booksy_ids."""
        client = _make_client_rpc({
            "get_monitoring_due_booksy_ids": [
                {"booksy_id": 111},
                {"booksy_id": 222},
                {"booksy_id": 333},
            ],
        })
        result = _tier_zero_due(client)
        assert result == [111, 222, 333]
        # Verify the limit is passed through.
        client.rpc.assert_called_once_with(
            "get_monitoring_due_booksy_ids",
            {"p_limit": TIER_BATCH_LIMIT[0]},
        )

    def test_returns_empty_when_rpc_errors(self):
        """Migration 126 not applied → RPC errors → resolver returns []
        gracefully (does not propagate the exception)."""
        client = _make_client_rpc({
            "get_monitoring_due_booksy_ids": Exception(
                "PGRST204 - function does not exist"
            ),
        })
        result = _tier_zero_due(client)
        assert result == []

    def test_skips_rows_without_booksy_id(self):
        """RPC oddities (NULL booksy_id) — resolver filters them out."""
        client = _make_client_rpc({
            "get_monitoring_due_booksy_ids": [
                {"booksy_id": 111},
                {"booksy_id": None},
                {"booksy_id": 222},
                {},
            ],
        })
        result = _tier_zero_due(client)
        assert result == [111, 222]


# ---------------------------------------------------------------------------
# C2/C3 — _maybe_trigger_monitoring_diff
# ---------------------------------------------------------------------------


class TestMaybeTriggerMonitoringDiff:
    @pytest.mark.asyncio
    async def test_no_op_when_advance_returns_zero(self):
        """advance_monitoring_due returns 0 → salon not on any watchlist →
        function returns early without reading the schedule table or making
        any HTTP call."""
        client = _make_client_rpc({"advance_monitoring_due": 0})
        # If function tried to call .table(), it'd raise AttributeError on the
        # not-configured MagicMock chain — but we still set it up safely.
        client.table = MagicMock()

        post_mock = AsyncMock()

        with patch("workers.scrape_refresh._get_client", return_value=client), \
             patch("workers.scrape_refresh._post_monitoring_alerts", post_mock):
            await _maybe_trigger_monitoring_diff(12345, emit_diffs=True)

        post_mock.assert_not_awaited()
        # We should not have hit the schedule table (early exit).
        client.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_when_advance_rpc_errors(self):
        """Migration 126 not yet applied → advance_monitoring_due RPC errors
        → function returns gracefully (logs warning, no HTTP call)."""
        client = _make_client_rpc({
            "advance_monitoring_due": Exception("PGRST204 - function not found"),
        })
        client.table = MagicMock()

        post_mock = AsyncMock()
        with patch("workers.scrape_refresh._get_client", return_value=client), \
             patch("workers.scrape_refresh._post_monitoring_alerts", post_mock):
            await _maybe_trigger_monitoring_diff(12345, emit_diffs=True)

        post_mock.assert_not_awaited()
        client.table.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_post_when_emit_diffs_false(self):
        """advance returns >0 (salon monitored) BUT emit_diffs=False
        (skipped scrape / unchanged rollup) → clock advanced, no POST."""
        client = _make_client_rpc({"advance_monitoring_due": 1})
        client.table = MagicMock()

        post_mock = AsyncMock()
        with patch("workers.scrape_refresh._get_client", return_value=client), \
             patch("workers.scrape_refresh._post_monitoring_alerts", post_mock):
            await _maybe_trigger_monitoring_diff(12345, emit_diffs=False)

        post_mock.assert_not_awaited()
        # Schedule table not queried (we early-returned after step 1).
        client.table.assert_not_called()
        # advance_monitoring_due WAS called (the clock got bumped).
        client.rpc.assert_called_once_with(
            "advance_monitoring_due", {"p_booksy_id": 12345}
        )

    @pytest.mark.asyncio
    async def test_builds_alert_when_service_added(self):
        """Happy path: scrape shows a new service added → POST one
        'service_added' alert per (watchlist, user)."""
        schedule_rows = [
            {
                "watchlist_id": "wl_abc",
                "user_id": "user_xyz",
                "salon_ref_id": 4242,
                "salon_name": "Studio Beauty",
            }
        ]
        pair_row = {
            "booksy_id": 12345,
            "current_scraped_at": "2026-06-06T10:00:00Z",
            "services_count_delta": 1,
            "reviews_count_delta": 0,
            "reviews_rank_delta": 0.0,
            "promoted_changed": False,
            "pricing_level_delta": 0,
        }
        service_diffs = [
            {
                "service_name": "Henna brwi",
                "status": "added",
                "prev_price": None,
                "current_price": "80 zł",
                "prev_price_grosze": None,
                "current_price_grosze": 8000,
                "prev_duration_min": None,
                "current_duration_min": 30,
            }
        ]

        # Compose a client where:
        #   .rpc("advance_monitoring_due") → 1
        #   .rpc("fn_salon_service_diffs") → service_diffs
        #   .table("monitoring_refresh_schedule") chain → schedule_rows
        #   .table("v_salon_scrape_pairs") chain → [pair_row]
        client = _make_client_rpc({
            "advance_monitoring_due": 1,
            "fn_salon_service_diffs": service_diffs,
        })

        def _table_side(name):
            if name == "monitoring_refresh_schedule":
                return _make_table_query_chain(schedule_rows)
            if name == "v_salon_scrape_pairs":
                return _make_table_query_chain([pair_row])
            if name == "salon_scrapes":
                return _make_table_query_chain([{"promoted": False, "salon_name": "Studio Beauty"}])
            return _make_table_query_chain([])
        client.table = MagicMock(side_effect=_table_side)

        post_mock = AsyncMock()
        with patch("workers.scrape_refresh._get_client", return_value=client), \
             patch("workers.scrape_refresh._post_monitoring_alerts", post_mock):
            await _maybe_trigger_monitoring_diff(12345, emit_diffs=True)

        post_mock.assert_awaited_once()
        alerts = post_mock.await_args[0][0]
        assert len(alerts) == 1
        a = alerts[0]
        assert a["userId"] == "user_xyz"
        assert a["watchlistId"] == "wl_abc"
        # CRITICAL: salonId must be salons.id (salon_ref_id), NOT booksy_id.
        assert a["salonId"] == 4242
        assert a["salonName"] == "Studio Beauty"
        assert a["type"] == "service_added"
        assert a["severity"] == "info"
        assert "Henna brwi" in a["title"] or "Henna brwi" in a["body"]
        # metadataJson must be a JSON string per Convex spec.
        meta = json.loads(a["metadataJson"])
        assert meta["service_name"] == "Henna brwi"

    @pytest.mark.asyncio
    async def test_no_post_when_everything_unchanged(self):
        """advance returns >0, schedule rows exist, pair row exists with all
        zero deltas, fn_salon_service_diffs all 'unchanged' → no alerts → no POST."""
        schedule_rows = [
            {
                "watchlist_id": "wl_1",
                "user_id": "user_1",
                "salon_ref_id": 100,
                "salon_name": "Quiet Salon",
            }
        ]
        pair_row = {
            "booksy_id": 999,
            "current_scraped_at": "2026-06-06T10:00:00Z",
            "services_count_delta": 0,
            "reviews_count_delta": 0,
            "reviews_rank_delta": 0.0,
            "promoted_changed": False,
            "pricing_level_delta": 0,
        }
        service_diffs = [
            {
                "service_name": "Strzyżenie",
                "status": "unchanged",
                "prev_price": "50 zł",
                "current_price": "50 zł",
                "prev_price_grosze": 5000,
                "current_price_grosze": 5000,
                "prev_duration_min": 30,
                "current_duration_min": 30,
            }
        ]

        client = _make_client_rpc({
            "advance_monitoring_due": 1,
            "fn_salon_service_diffs": service_diffs,
        })

        def _table_side(name):
            if name == "monitoring_refresh_schedule":
                return _make_table_query_chain(schedule_rows)
            if name == "v_salon_scrape_pairs":
                return _make_table_query_chain([pair_row])
            return _make_table_query_chain([])
        client.table = MagicMock(side_effect=_table_side)

        post_mock = AsyncMock()
        with patch("workers.scrape_refresh._get_client", return_value=client), \
             patch("workers.scrape_refresh._post_monitoring_alerts", post_mock):
            await _maybe_trigger_monitoring_diff(999, emit_diffs=True)

        post_mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# Severity threshold tests (C4 step 6)
# ---------------------------------------------------------------------------


class TestSeverityThresholds:
    def test_price_100_to_120_is_warning(self):
        """100→120 = exactly +20% → warning (NOT critical; threshold is >20)."""
        assert _severity_for_price_change(10000, 12000) == "warning"

    def test_price_100_to_121_is_critical(self):
        """100→121 = +21% → critical (strict >20%)."""
        assert _severity_for_price_change(10000, 12100) == "critical"

    def test_price_below_5pct_is_info(self):
        """Tiny moves (<5%) are 'info'."""
        assert _severity_for_price_change(10000, 10400) == "info"

    def test_price_drop_5pct_is_warning(self):
        """Price drops in 5-20% band are also 'warning' (still material)."""
        assert _severity_for_price_change(10000, 9000) == "warning"

    def test_zero_or_none_prev_returns_info(self):
        """Defensive: prev=0 or None must not divide by zero."""
        assert _severity_for_price_change(None, 5000) == "info"
        assert _severity_for_price_change(0, 5000) == "info"

    def test_full_alert_severity_via_build(self):
        """End-to-end through _build_monitoring_alerts: a 100→120 (+20%) price
        change yields a 'warning' severity alert; a 100→121 yields 'critical'."""
        schedule_rows = [
            {"watchlist_id": "wl1", "user_id": "u1", "salon_ref_id": 1, "salon_name": "S"}
        ]

        warning_diffs = [{
            "service_name": "X",
            "status": "price_changed",
            "prev_price": "100 zł",
            "current_price": "120 zł",
            "prev_price_grosze": 10000,
            "current_price_grosze": 12000,
            "prev_duration_min": 30,
            "current_duration_min": 30,
        }]
        alerts = _build_monitoring_alerts(
            schedule_rows=schedule_rows,
            pair_row=None,
            service_diffs=warning_diffs,
            promoted_current=None,
            salon_name_fallback="S",
        )
        assert len(alerts) == 1
        assert alerts[0]["type"] == "price_increase"
        assert alerts[0]["severity"] == "warning"

        critical_diffs = [{
            "service_name": "X",
            "status": "price_changed",
            "prev_price": "100 zł",
            "current_price": "121 zł",
            "prev_price_grosze": 10000,
            "current_price_grosze": 12100,
            "prev_duration_min": 30,
            "current_duration_min": 30,
        }]
        alerts = _build_monitoring_alerts(
            schedule_rows=schedule_rows,
            pair_row=None,
            service_diffs=critical_diffs,
            promoted_current=None,
            salon_name_fallback="S",
        )
        assert len(alerts) == 1
        assert alerts[0]["type"] == "price_increase"
        assert alerts[0]["severity"] == "critical"


# ---------------------------------------------------------------------------
# Bonus: promotion_started vs _ended disambiguation
# ---------------------------------------------------------------------------


class TestPromotionAlerts:
    def test_promotion_started_when_current_true(self):
        """promoted_changed=True + promoted_current=True → 'promotion_started'."""
        rows = [{"watchlist_id": "w", "user_id": "u", "salon_ref_id": 1, "salon_name": "X"}]
        pair_row = {
            "services_count_delta": 0,
            "reviews_count_delta": 0,
            "reviews_rank_delta": 0.0,
            "promoted_changed": True,
        }
        alerts = _build_monitoring_alerts(
            schedule_rows=rows,
            pair_row=pair_row,
            service_diffs=[],
            promoted_current=True,
            salon_name_fallback="X",
        )
        types = {a["type"] for a in alerts}
        assert "promotion_started" in types
        assert "promotion_ended" not in types

    def test_promotion_ended_when_current_false(self):
        """promoted_changed=True + promoted_current=False → 'promotion_ended'."""
        rows = [{"watchlist_id": "w", "user_id": "u", "salon_ref_id": 1, "salon_name": "X"}]
        pair_row = {
            "services_count_delta": 0,
            "reviews_count_delta": 0,
            "reviews_rank_delta": 0.0,
            "promoted_changed": True,
        }
        alerts = _build_monitoring_alerts(
            schedule_rows=rows,
            pair_row=pair_row,
            service_diffs=[],
            promoted_current=False,
            salon_name_fallback="X",
        )
        types = {a["type"] for a in alerts}
        assert "promotion_ended" in types
        assert "promotion_started" not in types
