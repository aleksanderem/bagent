"""Daily staff identity links refresh (arq cron).

Runs at 03:45 UTC after the taxonomy cascade (03:00–03:30). Invokes the
Postgres RPC ``fn_refresh_staff_identity_links(90)`` which scans the last
90 days of ``v_salon_staff_events``, pairs lefts↔joins by normalized name +
geographic proximity + time window, scores confidence, and upserts results
into ``staff_identity_links``.

The full match algorithm and confidence scoring live inside the SQL
function (migration 052). This worker is just the scheduler glue + the
healthcheck wrapper — it stays thin on purpose so refining the matching
logic is a Postgres-only deploy.

Idempotent: re-running over the same lookback window updates confidence
in place via INSERT ... ON CONFLICT DO UPDATE.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


# Lookback window in days for the nightly refresh. Aligned with the
# 90-day window already used by v_salon_staff_events. Bigger windows
# would re-evaluate older events forever — keep it bounded.
REFRESH_LOOKBACK_DAYS = 90


async def refresh_staff_identity_links(ctx: dict[str, Any]) -> str:
    """Refresh staff_identity_links via the Postgres RPC.

    Returns a short log line. On failure, pings the healthcheck `/fail`
    endpoint and re-raises so arq surfaces the error in the worker log.
    """
    from services.healthcheck import ping
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    t0 = time.time()
    try:
        result = await asyncio.to_thread(
            lambda: client.rpc(
                "fn_refresh_staff_identity_links",
                {"p_lookback_days": REFRESH_LOOKBACK_DAYS},
            ).execute()
        )
        rows = result.data or []
        if rows:
            inserted = rows[0].get("rows_inserted", 0)
            updated = rows[0].get("rows_updated", 0)
        else:
            inserted = updated = 0
        dt = time.time() - t0
        msg = (
            f"staff_identity_links refreshed in {dt:.1f}s "
            f"(inserted={inserted}, updated={updated})"
        )
        logger.info(msg)
        await ping("HC_PING_STAFF_IDENTITY_LINKS")
        return msg
    except Exception as exc:
        logger.exception("refresh_staff_identity_links failed: %s", exc)
        await ping("HC_PING_STAFF_IDENTITY_LINKS", fail=True)
        raise


# Exported list for WorkerSettings.functions
ALL_STAFF_IDENTITY_TASKS = [refresh_staff_identity_links]
