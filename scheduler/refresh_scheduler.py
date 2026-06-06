"""Issue #23 — tier-aware refresh scheduler.

Runs hourly. For each tier picks salons whose latest scrape is older than
the tier's cadence and appends them to ``salon_refresh_queue`` via the
``enqueue_salon_refresh()`` SQL helper (which suppresses duplicates that
are already queued or running).

Tiers:

* tier 1: subjects of an active competitor_monitoring_watchlists row
  (paying customers monitoring competitors).  Cadence: 7 days.
* tier 2: subject_salon_id of audits in the last 90 days.  Cadence: 30
  days.
* tier 3: everything else in salons (cold catalogue used as comparison
  fodder for new audits).  Cadence: 90 days.

The scheduler is purely a SELECT + INSERT loop. It never calls bextract
itself — that's the worker's job. This keeps the scheduler cheap to run
synchronously inside an arq cron task.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from supabase import Client

from services.sb_client import make_supabase_client

from config import settings

logger = logging.getLogger("bagent.scheduler.refresh_scheduler")


# Cadence per tier (days). Issue #23 acceptance criteria.
# tier 1: paying monitoring subscribers (weekly)
# tier 2: recent audit subjects (monthly)
# tier 3: cold catalogue (= continuous change-detection sweep). The
#   ingester dedupes by content_hash so a stable salon producing the
#   same JSON re-checks but only one salon_scrapes row stays. Cadence
#   was 14d but we lowered to 7d on 2026-05-13 — at 300 salons/h we
#   can re-scrape 50k salons in ~7 days, matching the cadence so the
#   queue is always non-empty and the salon-changes dashboard always
#   shows fresh diffs.
TIER_CADENCE_DAYS: dict[int, int] = {
    1: 7,
    2: 30,
    3: 7,
}

# Per-tier max enqueue per scheduler run. Tier 3 is huge (>5K salons),
# so we cap to avoid one cron tick filling the queue with quarterly cold
# refreshes. The scheduler runs hourly so caps are an upper bound on
# throughput per hour.
#
# Tier 0 (S0055 — watchlist refresh): per-row cadence (6/24/48h) lives in
# monitoring_refresh_schedule. The scheduler only enqueues due salons —
# advance_monitoring_due is called from the post-scan hook, not here.
TIER_BATCH_LIMIT: dict[int, int] = {
    0: 100,
    1: 200,
    2: 500,
    3: 800,
}


@dataclass
class ScheduleResult:
    enqueued: dict[int, int] = field(default_factory=lambda: {0: 0, 1: 0, 2: 0, 3: 0})
    suppressed_dupes: int = 0
    skipped_no_booksy_id: int = 0


_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


def _enqueue(client: Client, booksy_id: int, tier: int) -> bool:
    """Call the SQL helper. Returns True when actually enqueued, False when
    the helper suppressed a duplicate (already queued/running)."""
    res = client.rpc(
        "enqueue_salon_refresh",
        {
            "p_booksy_id": booksy_id,
            "p_tier": tier,
            "p_priority": tier,  # tier number doubles as priority — lower = sooner
        },
    ).execute()
    new_id = res.data
    return new_id is not None


def _tier_zero_due(client: Client) -> list[int]:
    """booksy_ids w monitoring_refresh_schedule z next_due_at <= NOW().

    Resolver dla watchlist refresh (tier 0). Cadence per-row (6/24/48h)
    siedzi w monitoring_refresh_schedule, NIE per-tier — schedule_due_refreshes
    woła tylko enqueue, advance next_due_at robi post-scan hook
    (advance_monitoring_due RPC w workers/scrape_refresh.py).

    Returns [] gracefully if migration 126 nie została jeszcze applied
    (RPC nie istnieje) — tier 1/2/3 jadą dalej normalnie.
    """
    try:
        res = client.rpc(
            "get_monitoring_due_booksy_ids",
            {"p_limit": TIER_BATCH_LIMIT.get(0, 100)},
        ).execute()
        return [row["booksy_id"] for row in (res.data or []) if row.get("booksy_id")]
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "[refresh_scheduler] _tier_zero_due failed "
            "(migration 126 may not be applied yet): %s", e,
        )
        return []


def _tier_one_due(client: Client) -> list[int]:
    """booksy_ids in active competitor_monitoring_watchlists whose subject
    salon hasn't been scraped in the last `TIER_CADENCE_DAYS[1]` days.

    NOTE: ``competitorMonitoringWatchlists`` is a Convex table, NOT a
    Supabase one — querying via supabase-py returns a 404 (table not
    in the schema cache). Until we either add a synchronization mirror
    table on the Supabase side or move tier-1 selection into a Convex
    internal action, this returns an empty list. tier-2 and tier-3
    keep covering monitoring subjects via the audit-subject path.
    """
    return []
    # Original implementation kept for reference — re-enable when a
    # Convex-side fanout writes to a supabase mirror table:
    res = client.table("competitorMonitoringWatchlists").select("salons").execute()
    if not res.data:
        return []
    booksy_ids: set[int] = set()
    for row in res.data:
        for s in row.get("salons", []) or []:
            sid = s.get("salonId")
            if isinstance(sid, int):
                # `salonId` is the Supabase salons.id, not booksy_id, so
                # we'll resolve it next via salons table.
                booksy_ids.add(sid)
    if not booksy_ids:
        return []
    # Resolve salons.id → booksy_id, then check freshness.
    salons = (
        client.table("salons")
        .select("id, booksy_id")
        .in_("id", list(booksy_ids))
        .execute()
    )
    id_to_booksy: dict[int, int] = {
        row["id"]: row["booksy_id"] for row in (salons.data or []) if row.get("booksy_id")
    }
    return _filter_stale(client, list(id_to_booksy.values()), TIER_CADENCE_DAYS[1])


def _tier_two_due(client: Client) -> list[int]:
    """booksy_ids whose audits row was created in the last 90 days.
    Filter to those whose latest scrape is older than 30 days.

    NOTE: ``audits`` is a Convex table, not a Supabase one. We read
    the Supabase mirror via ``competitor_reports.subject_salon_id``
    which represents every salon that's ever been an audit subject.
    The earlier client.table("audits") query was a leftover that
    crashed the whole schedule_refresh_cron with PGRST205.
    """
    cutoff_iso = _iso_days_ago(90)
    cr = (
        client.table("competitor_reports")
        .select("subject_salon_id, created_at")
        .gte("created_at", cutoff_iso)
        .limit(2000)
        .execute()
    )
    salon_ids = sorted({row["subject_salon_id"] for row in (cr.data or []) if row.get("subject_salon_id")})
    if not salon_ids:
        return []
    salons = (
        client.table("salons")
        .select("id, booksy_id")
        .in_("id", salon_ids)
        .execute()
    )
    booksy_ids = [row["booksy_id"] for row in (salons.data or []) if row.get("booksy_id")]
    return _filter_stale(client, booksy_ids, TIER_CADENCE_DAYS[2])


def _tier_three_due(client: Client, limit: int) -> list[int]:
    """Tier 3: cold sweep of catalog.

    Delegates to the SQL RPC ``get_tier_three_due_booksy_ids`` (migration 050)
    which selects booksy_ids whose **chain head** (or pre-dedup latest scrape)
    is older than ``TIER_CADENCE_DAYS[3]`` days.

    Why an RPC: pre-dedup we filtered on ``salons.last_scraped_at`` which is
    bumped on EVERY crawl (including 'unchanged' dedup rollups under the
    post-2026-05-12 write path). That meant the entire universe of 54k+
    salons looked "fresh" to the scheduler and the queue stayed empty
    indefinitely — the salon-changes dashboard observed 0 pairs/day. The
    RPC instead looks at ``salon_scrapes`` chain head ``scraped_at``, i.e.
    "when did this salon's content last actually change". A salon whose
    content has been confirmed unchanged for N days is exactly the right
    candidate to re-check.

    Excludes ``salons.deleted_at IS NOT NULL`` — those are 404'd on bextract.
    """
    cadence_days = TIER_CADENCE_DAYS[3]
    res = client.rpc(
        "get_tier_three_due_booksy_ids",
        {"p_cadence_days": cadence_days, "p_limit": limit},
    ).execute()
    return [row["booksy_id"] for row in (res.data or []) if row.get("booksy_id")]


def _filter_stale(
    client: Client,
    booksy_ids: list[int],
    cadence_days: int,
) -> list[int]:
    """Return booksy_ids whose latest salon_scrapes.scraped_at is older
    than `cadence_days` (or that have no scrape at all).

    Chunked by 100 IDs per IN clause — PostgREST/nginx URI cap kicks
    in around ~6 KB and 800-element IN was hitting HTTP 414.
    """
    if not booksy_ids:
        return []
    cutoff_iso = _iso_days_ago(cadence_days)
    fresh: set[int] = set()
    for i in range(0, len(booksy_ids), 100):
        chunk = booksy_ids[i : i + 100]
        try:
            res = (
                client.table("salon_scrapes")
                .select("booksy_id, scraped_at")
                .in_("booksy_id", chunk)
                .gte("scraped_at", cutoff_iso)
                .execute()
            )
            for row in (res.data or []):
                bid = row.get("booksy_id")
                if bid is not None:
                    fresh.add(bid)
        except Exception as e:  # noqa: BLE001
            logger.warning("[refresh_scheduler] _filter_stale chunk %d failed: %s", i, e)
    return [bid for bid in booksy_ids if bid not in fresh]


def _iso_days_ago(days: int) -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()


def schedule_due_refreshes() -> ScheduleResult:
    """Top up salon_refresh_queue for all 3 tiers.

    Called by the arq cron job ``schedule_refresh_cron`` registered in
    ``workers.scrape_refresh``. Returns counts so the caller can log /
    ping Healthchecks.
    """
    client = _get_client()
    result = ScheduleResult()

    # Tier 0 (S0055 watchlist refresh) MUST come first — priority=0 = lowest
    # number = claim_salon_refresh_jobs ORDER BY priority ASC picks it first.
    # Enqueueing first also lets the dedup-guard suppress lower-tier dupes
    # rather than the other way around.
    tier_resolvers: list[tuple[int, list[int]]] = [
        (0, _tier_zero_due(client)[: TIER_BATCH_LIMIT[0]]),
        (1, _tier_one_due(client)[: TIER_BATCH_LIMIT[1]]),
        (2, _tier_two_due(client)[: TIER_BATCH_LIMIT[2]]),
        (3, _tier_three_due(client, TIER_BATCH_LIMIT[3])),
    ]

    for tier, booksy_ids in tier_resolvers:
        for bid in booksy_ids:
            if bid is None:
                result.skipped_no_booksy_id += 1
                continue
            try:
                if _enqueue(client, int(bid), tier):
                    result.enqueued[tier] += 1
                else:
                    result.suppressed_dupes += 1
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[refresh_scheduler] enqueue failed booksy_id=%s tier=%s: %s",
                    bid, tier, e,
                )

    logger.info(
        "[refresh_scheduler] enqueued tier0=%d tier1=%d tier2=%d tier3=%d (dupes=%d, no-id=%d)",
        result.enqueued.get(0, 0), result.enqueued[1], result.enqueued[2], result.enqueued[3],
        result.suppressed_dupes, result.skipped_no_booksy_id,
    )
    return result
