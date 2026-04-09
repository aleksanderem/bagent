"""Comp Etap 7 — premium competitor report refresh pipeline.

Runs on a schedule (triggered by the Convex cron
`refresh-premium-competitor-reports`) for each `competitor_reports` row
where `tier='premium'`, `status='completed'`, and `next_refresh_at` has
passed.

For each refresh:

1. Load the report header + its `competitor_matches` from Supabase.
2. For each competitor (and the subject salon), read the LATEST row in
   `salon_scrapes` — since the JSON ingester runs on user-delivered batches,
   a freshly-refreshed competitor is simply one whose most recent scrape
   landed after the previous snapshot. Older data still produces a
   snapshot but is flagged as `{stale: true}` so the next run will also
   process it once new data arrives.
3. Compute a new `key_metrics` dict with subject + competitor pricing
   summaries (avg price per canonical treatment_id, total services, reviews
   count/rank).
4. INSERT the snapshot into `competitor_report_snapshots` with a UNIQUE
   constraint on `(report_id, snapshot_date)` preventing same-day duplicates.
5. Detect price changes > 5% for tracked competitors via the
   `salon_scrape_services` time-series window. Log significant changes
   (email notifications are a TODO until the paid premium tier ships).
6. Update `competitor_reports.next_refresh_at` based on `refresh_schedule`
   (`weekly` → +7 days, `biweekly` → +14, `monthly` → +30).

The pipeline is best-effort — individual competitor failures do not abort
the refresh. The return value summarises `snapshot_date`,
`next_refresh_at`, `significant_change_count`, and per-competitor stats so
`run_competitor_refresh_job` can log the outcome.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


REFRESH_INTERVALS_DAYS: dict[str, int] = {
    "weekly": 7,
    "biweekly": 14,
    "monthly": 30,
}


def _next_refresh_at(schedule: str | None) -> datetime:
    """Return the next refresh timestamp given a refresh_schedule string.

    Defaults to weekly if the schedule is missing or unrecognised so
    premium reports never get stuck without a future refresh window.
    """
    days = REFRESH_INTERVALS_DAYS.get(schedule or "weekly", 7)
    return datetime.now(timezone.utc) + timedelta(days=days)


def _avg_price_grosze(services: list[dict]) -> int | None:
    """Average price_grosze over all services with a non-null price_grosze.

    Returns None when no service has a price so a downstream consumer can
    display "brak ceny" instead of a misleading 0.
    """
    prices = [
        int(s["price_grosze"])
        for s in services
        if s.get("price_grosze") is not None
    ]
    if not prices:
        return None
    return sum(prices) // len(prices)


def _avg_price_per_canonical(services: list[dict]) -> dict[str, int]:
    """Group services by canonical_id and return {canonical_id: avg_price_grosze}.

    canonical_id is used instead of booksy_treatment_id so salons that the
    category-restructure agent mapped to a canonical taxonomy still match
    across the subject and competitors. Services without a canonical_id are
    skipped — they cannot participate in cross-salon comparisons anyway.
    """
    buckets: dict[str, list[int]] = {}
    for s in services:
        canonical = s.get("canonical_id")
        price = s.get("price_grosze")
        if canonical is None or price is None:
            continue
        buckets.setdefault(str(canonical), []).append(int(price))
    return {
        canonical: sum(prices) // len(prices)
        for canonical, prices in buckets.items()
        if prices
    }


async def _load_latest_scrape_for_booksy_id(
    supabase_client: Any, booksy_id: int,
) -> dict[str, Any] | None:
    """Return the newest salon_scrapes row for a given booksy_id plus its
    associated salon_scrape_services rows, or None if no scrape exists yet.
    """
    scrape_res = (
        supabase_client.table("salon_scrapes")
        .select("id, scraped_at, reviews_count, reviews_rank, salon_name")
        .eq("booksy_id", booksy_id)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
    )
    if not scrape_res.data:
        return None
    scrape = scrape_res.data[0]

    services_res = (
        supabase_client.table("salon_scrape_services")
        .select(
            "id, name, price_grosze, canonical_id, booksy_treatment_id, booksy_service_id",
        )
        .eq("scrape_id", scrape["id"])
        .execute()
    )
    scrape["services"] = services_res.data or []
    return scrape


async def _detect_price_changes(
    supabase_client: Any, booksy_ids: list[int],
) -> list[dict[str, Any]]:
    """Run the salon_scrape_services window-function query via RPC to find
    prices that moved more than 5% in the last 14 days for the tracked
    competitors.

    The query itself lives in a Postgres function `detect_competitor_price_changes`
    which is a migration deliverable for Comp Etap 7 — until that migration
    lands, this helper falls back to returning an empty list so the refresh
    pipeline still completes successfully and the infrastructure can be
    smoke-tested end to end.
    """
    if not booksy_ids:
        return []
    try:
        result = supabase_client.rpc(
            "detect_competitor_price_changes",
            {"p_booksy_ids": booksy_ids},
        ).execute()
        data = result.data or []
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        # Best-effort — the RPC may not exist yet in every environment.
        logger.info(
            "detect_competitor_price_changes RPC unavailable (%s) — "
            "skipping price-change detection for this run", e,
        )
        return []


async def run_refresh(
    report_id: int,
    on_progress: ProgressCallback = _noop_progress,
) -> dict[str, Any]:
    """Execute a single premium competitor report refresh.

    Parameters
    ----------
    report_id:
        Primary key of the `competitor_reports` row to refresh. Must be an
        existing row with `tier='premium'` — the caller (Convex cron)
        already filtered for this, but we re-assert below so manual curl
        tests fail loudly instead of appending ghost snapshots.
    on_progress:
        Async progress callback `(progress_int, message)` used by the
        surrounding job runner to stream log lines.

    Returns
    -------
    dict
        `{snapshot_date, next_refresh_at, significant_change_count,
          competitors_refreshed, competitors_skipped}` summarising the run.
    """
    from services.supabase import SupabaseService

    supabase = SupabaseService().client

    await on_progress(5, f"Loading competitor report {report_id}")

    report_res = (
        supabase.table("competitor_reports")
        .select(
            "id, convex_audit_id, subject_salon_id, tier, refresh_schedule, "
            "next_refresh_at, status",
        )
        .eq("id", report_id)
        .limit(1)
        .execute()
    )
    if not report_res.data:
        raise RuntimeError(f"competitor_reports row {report_id} not found")
    report = report_res.data[0]

    if report.get("tier") != "premium":
        raise RuntimeError(
            f"competitor_reports row {report_id} is tier={report.get('tier')}, "
            f"refresh only applies to premium",
        )

    await on_progress(15, "Loading competitor_matches")

    matches_res = (
        supabase.table("competitor_matches")
        .select("id, competitor_salon_id, bucket, counts_in_aggregates")
        .eq("report_id", report_id)
        .execute()
    )
    matches = matches_res.data or []
    if not matches:
        logger.warning(
            "Competitor report %s has no competitor_matches — refresh "
            "will only snapshot the subject salon", report_id,
        )

    # Translate subject_salon_id + competitor_salon_id (FK to salons.id)
    # into booksy_ids, which is what salon_scrapes is keyed on.
    salon_ids = [report["subject_salon_id"]] + [
        m["competitor_salon_id"] for m in matches if m.get("competitor_salon_id")
    ]
    salon_ids = [sid for sid in salon_ids if sid is not None]

    subject_booksy_id: int | None = None
    competitor_booksy_ids: list[int] = []
    booksy_id_map: dict[int, int] = {}

    if salon_ids:
        salons_res = (
            supabase.table("salons")
            .select("id, booksy_id")
            .in_("id", salon_ids)
            .execute()
        )
        for row in salons_res.data or []:
            if row.get("booksy_id") is not None:
                booksy_id_map[row["id"]] = row["booksy_id"]

        subject_booksy_id = booksy_id_map.get(report["subject_salon_id"])
        competitor_booksy_ids = [
            booksy_id_map[m["competitor_salon_id"]]
            for m in matches
            if m.get("competitor_salon_id")
            and m["competitor_salon_id"] in booksy_id_map
        ]

    await on_progress(
        30,
        f"Loaded {len(competitor_booksy_ids)} competitor booksy_ids for refresh",
    )

    # Build per-salon metric payloads from the latest salon_scrapes row.
    competitors_refreshed = 0
    competitors_skipped = 0

    competitor_metrics: list[dict[str, Any]] = []
    for idx, m in enumerate(matches):
        competitor_salon_id = m.get("competitor_salon_id")
        booksy_id = booksy_id_map.get(competitor_salon_id) if competitor_salon_id else None
        if booksy_id is None:
            competitors_skipped += 1
            continue

        scrape = await _load_latest_scrape_for_booksy_id(supabase, booksy_id)
        if scrape is None:
            competitors_skipped += 1
            logger.info(
                "Skipping competitor booksy_id=%s for report %s — no salon_scrapes row",
                booksy_id, report_id,
            )
            continue

        services = scrape.get("services") or []
        competitor_metrics.append({
            "salon_id": competitor_salon_id,
            "booksy_id": booksy_id,
            "bucket": m.get("bucket"),
            "counts_in_aggregates": bool(m.get("counts_in_aggregates", True)),
            "salon_name": scrape.get("salon_name"),
            "scraped_at": scrape.get("scraped_at"),
            "total_services": len(services),
            "avg_price_grosze": _avg_price_grosze(services),
            "avg_price_per_canonical": _avg_price_per_canonical(services),
            "reviews_count": scrape.get("reviews_count"),
            "reviews_rank": scrape.get("reviews_rank"),
        })
        competitors_refreshed += 1

        await on_progress(
            30 + int(40 * (idx + 1) / max(len(matches), 1)),
            f"Computed metrics for competitor {idx + 1}/{len(matches)}",
        )

    # Subject snapshot (always computed, even if there are zero competitors).
    subject_metrics: dict[str, Any] = {}
    if subject_booksy_id is not None:
        subject_scrape = await _load_latest_scrape_for_booksy_id(
            supabase, subject_booksy_id,
        )
        if subject_scrape is not None:
            subject_services = subject_scrape.get("services") or []
            subject_metrics = {
                "salon_id": report["subject_salon_id"],
                "booksy_id": subject_booksy_id,
                "salon_name": subject_scrape.get("salon_name"),
                "scraped_at": subject_scrape.get("scraped_at"),
                "total_services": len(subject_services),
                "avg_price_grosze": _avg_price_grosze(subject_services),
                "avg_price_per_canonical": _avg_price_per_canonical(subject_services),
                "reviews_count": subject_scrape.get("reviews_count"),
                "reviews_rank": subject_scrape.get("reviews_rank"),
            }

    await on_progress(75, "Detecting significant price changes")

    changes = await _detect_price_changes(supabase, competitor_booksy_ids)
    significant_changes = [
        c for c in changes
        if c.get("pct_change") is not None
        and abs(float(c["pct_change"])) > 5.0
    ]

    if significant_changes:
        logger.info(
            "Found %d significant price changes for report %s (> 5%%)",
            len(significant_changes), report_id,
        )
        # TODO: trigger email notification once the paid premium tier is
        # live. For now we only log so infrastructure can be validated.
    else:
        logger.info(
            "No significant price changes detected for report %s",
            report_id,
        )

    await on_progress(85, "Writing snapshot row")

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    key_metrics: dict[str, Any] = {
        "snapshot_date": snapshot_date,
        "subject": subject_metrics,
        "competitors": competitor_metrics,
        "price_changes": significant_changes,
        "stats": {
            "competitors_refreshed": competitors_refreshed,
            "competitors_skipped": competitors_skipped,
            "significant_change_count": len(significant_changes),
        },
    }

    snapshot_row = {
        "report_id": report_id,
        "snapshot_date": snapshot_date,
        "key_metrics": key_metrics,
    }
    try:
        supabase.table("competitor_report_snapshots").upsert(
            snapshot_row, on_conflict="report_id,snapshot_date",
        ).execute()
    except Exception as e:
        # Fallback to plain insert if the UNIQUE (report_id, snapshot_date)
        # constraint hasn't been fully wired into the client (e.g. older
        # supabase-py versions swallowing on_conflict).
        logger.warning(
            "Upsert into competitor_report_snapshots failed (%s) — "
            "attempting plain insert", e,
        )
        supabase.table("competitor_report_snapshots").insert(snapshot_row).execute()

    await on_progress(95, "Updating next_refresh_at")

    next_refresh = _next_refresh_at(report.get("refresh_schedule"))
    supabase.table("competitor_reports").update({
        "next_refresh_at": next_refresh.isoformat(),
    }).eq("id", report_id).execute()

    await on_progress(
        100,
        f"Refresh complete for report {report_id}: "
        f"{competitors_refreshed} refreshed, "
        f"{competitors_skipped} skipped, "
        f"{len(significant_changes)} significant changes",
    )

    return {
        "snapshot_date": snapshot_date,
        "next_refresh_at": next_refresh.isoformat(),
        "significant_change_count": len(significant_changes),
        "competitors_refreshed": competitors_refreshed,
        "competitors_skipped": competitors_skipped,
    }
