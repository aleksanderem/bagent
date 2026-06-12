"""arq tasks for Meta Ads automation.

Three cron-driven workers:

1. ``push_approved_campaigns`` (every 5 min)
   Reads approved drafts from Convex via internal query, calls Meta API
   to create campaign+adsets+ads, records returned Meta IDs back to
   Convex. Idempotent — already-pushed campaigns skipped.

2. ``fetch_daily_metrics`` (daily at 02:00 UTC)
   For every active campaign, pull yesterday's insights from Meta
   Marketing API and upsert into campaign_daily_metrics. Idempotent
   re-runs OK.

3. ``attribute_bookings`` (every 30 min)
   Reads new salon_scrapes diffs (via v_salon_scrape_pairs from
   migration 039), checks each one for ad clicks within 7-day window
   on that booksy_id (via ad_clicks table from migration 040),
   inserts ad_booking_attributions rows + recordAttributionEvent on
   Convex side.

These tasks operate on the bagent-side Redis/Supabase/Convex stack
already in place. No new infrastructure dependencies.
"""

from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from arq.connections import ArqRedis

from config import settings
from services.convex import ConvexClient
from services.meta_ads import (
    CampaignConfig, DemographicTarget, GeoTarget, MetaAdsClient,
    build_booksy_destination_url,
)
from services.sb_client import make_supabase_client

logger = logging.getLogger("bagent.workers.campaign_tasks")


# ---------------------------------------------------------------------------
# Task 1: push approved campaigns to Meta API
# ---------------------------------------------------------------------------

async def push_approved_campaigns(ctx: dict[str, Any]) -> dict[str, int]:
    """Read approved-but-not-pushed campaign drafts from Convex,
    push to Meta API, record returned IDs.

    Cron: every 5 min. Convex has SELECT FOR UPDATE-like semantics via
    the metaCampaignId guard in the internal query, so concurrent
    workers won't double-push (last-writer-wins is fine here)."""
    convex = ConvexClient()
    drafts = await convex.run_query("campaigns:listApprovedCampaignsForPush", {})
    if not drafts:
        return {"pushed": 0, "errors": 0}

    pushed = 0
    errors = 0
    for draft in drafts:
        try:
            await _push_one(convex, draft)
            pushed += 1
        except Exception as e:  # noqa: BLE001
            errors += 1
            logger.error(
                "Failed to push campaign %s: %s", draft.get("_id"), e,
                exc_info=True,
            )
            # Mark as error in Convex so user/operator sees the failure
            try:
                await convex.run_mutation(
                    "campaigns:setCampaignErrorStatus",
                    {"campaignId": draft["_id"], "error": str(e)[:500]},
                )
            except Exception:
                pass

    if pushed or errors:
        logger.info("[campaigns] push: %d ok, %d errors", pushed, errors)
    return {"pushed": pushed, "errors": errors}


async def _push_one(convex: ConvexClient, draft: dict[str, Any]) -> None:
    """Single campaign push — fetches creds + creatives, calls Meta API,
    records IDs."""
    # Look up ad account creds (token, page, IG actor) for this user/salon
    creds = await convex.run_query(
        "campaigns:getAdAccountCredsForCampaign",
        {"campaignId": draft["_id"]},
    )
    if not creds:
        raise RuntimeError(
            "No meta_ad_accounts row for this user/salon — client must "
            "complete Facebook BM connection flow first"
        )

    # Pull adsets + creatives from Convex for this campaign
    structure = await convex.run_query(
        "campaigns:getDraftStructure",
        {"campaignId": draft["_id"]},
    )
    if not structure or not structure.get("adSets") or not structure.get("creatives"):
        raise RuntimeError("Campaign draft missing adsets or creatives")

    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    async with MetaAdsClient(
        ad_account_id=creds["metaAdAccountId"],
        access_token=creds["accessToken"],
        page_id=creds.get("metaPageId"),
        instagram_actor_id=creds.get("metaInstagramActorId"),
    ) as meta:
        # 1. Create campaign
        special = ["HEALTH"] if draft["healthRegulated"] else []
        meta_campaign_id = await meta.create_campaign(
            name=draft["name"],
            objective=draft["objective"],
            status="PAUSED",
            special_ad_categories=special,
        )

        # 2. Create ad sets — each has stored targeting JSON
        adset_results = []
        for adset in structure["adSets"]:
            import json as _json
            targeting = _json.loads(adset["targetingJson"])
            # Reconstruct CampaignConfig from stored fields for create_ad_set
            cfg = CampaignConfig(
                name=adset["name"],
                objective=draft["objective"],  # type: ignore[arg-type]
                daily_budget_pln=adset["dailyBudgetPln"],
                geo=GeoTarget(
                    lat=targeting["geo"]["lat"],
                    lng=targeting["geo"]["lng"],
                    radius_km=targeting["geo"]["radius_km"],
                ),
                demographic=DemographicTarget(
                    age_min=targeting["demo"]["age_min"],
                    age_max=targeting["demo"]["age_max"],
                    genders=targeting["demo"]["genders"],
                ),
                interests=targeting.get("interests", []),
            )
            meta_adset_id = await meta.create_ad_set(
                meta_campaign_id, adset["name"], cfg,
                optimization_goal=adset.get("optimizationGoal", "LINK_CLICKS"),
            )
            adset_results.append({
                "adSetId": adset["_id"], "metaAdSetId": meta_adset_id,
            })

        # 3. Upload creatives + create ads. Each creative has imageStoragePath
        # in Supabase Storage — fetch bytes, upload to Meta, create ad.
        creative_results = []
        adset_id_map = {a["adSetId"]: a["metaAdSetId"] for a in adset_results}
        for creative in structure["creatives"]:
            meta_adset_id = adset_id_map.get(creative["adSetId"])
            if not meta_adset_id:
                continue

            # Fetch image bytes from Supabase Storage
            image_bytes = sb.storage.from_("campaign-creatives").download(
                creative["imageStoragePath"]
            )

            image_hash = await meta.upload_creative_image(
                image_bytes,
                filename=f"{creative['_id']}.png",
            )

            # Replace 'pending' UTM placeholders with actual IDs in the
            # destination URL — done here because we just learned the IDs.
            real_destination = build_booksy_destination_url(
                _strip_utm(draft["destinationUrl"]),
                campaign_id=meta_campaign_id,
                adset_id=meta_adset_id,
                # ad_id filled per-creative below since not yet known
            )

            meta_ad_id = await meta.create_ad(
                meta_adset_id,
                name=creative["name"],
                image_hash=image_hash,
                headline=creative["headline"],
                body=creative["bodyText"],
                destination_url=real_destination,
                cta_type=creative.get("ctaType", "BOOK_TRAVEL"),
            )

            creative_results.append({
                "creativeId": creative["_id"],
                "metaAdId": meta_ad_id,
                "metaCreativeId": meta_ad_id,  # Meta returns same ID; refine if needed
                "metaImageHash": image_hash,
            })

    await convex.run_mutation(
        "campaigns:recordMetaIds",
        {
            "campaignId": draft["_id"],
            "metaCampaignId": meta_campaign_id,
            "adSets": adset_results,
            "creatives": creative_results,
        },
    )


def _strip_utm(url: str) -> str:
    """Remove existing UTM params from URL — needed because the draft
    URL had 'pending' placeholder UTMs that we now replace with real IDs."""
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    cleaned = {k: v for k, v in qs.items() if not k.startswith("utm_") and k != "ba_audit"}
    flat = {k: v[0] for k, v in cleaned.items()}
    return urlunparse(parsed._replace(query=urlencode(flat)))


# ---------------------------------------------------------------------------
# Task 2: daily metrics fetch
# ---------------------------------------------------------------------------

async def fetch_daily_metrics(ctx: dict[str, Any]) -> dict[str, int]:
    """Daily cron at 02:00 UTC — pull yesterday's insights from Meta
    for every active campaign. Idempotent upsert in Convex."""
    convex = ConvexClient()
    active = await convex.run_query("campaigns:listActiveCampaigns", {})
    if not active:
        return {"campaigns": 0, "rows_inserted": 0}

    rows_total = 0
    for campaign in active:
        try:
            n = await _fetch_one_campaign_metrics(convex, campaign)
            rows_total += n
        except Exception as e:  # noqa: BLE001
            logger.error(
                "Daily fetch failed for campaign %s: %s",
                campaign.get("_id"), e,
            )

    logger.info("[campaigns] daily fetch: %d campaigns, %d rows", len(active), rows_total)
    return {"campaigns": len(active), "rows_inserted": rows_total}


async def _fetch_one_campaign_metrics(
    convex: ConvexClient, campaign: dict[str, Any],
) -> int:
    """Fetch yesterday's metrics for a single campaign + all its ads."""
    creds = await convex.run_query(
        "campaigns:getAdAccountCredsForCampaign",
        {"campaignId": campaign["_id"]},
    )
    if not creds or not campaign.get("metaCampaignId"):
        return 0

    async with MetaAdsClient(
        ad_account_id=creds["metaAdAccountId"],
        access_token=creds["accessToken"],
    ) as meta:
        insights = await meta.fetch_insights(
            campaign["metaCampaignId"],
            date_preset="yesterday",
        )

    if not insights:
        return 0

    # Map Meta insights rows → our schema. Insights returns one row per
    # day per ad (when time_increment=1) so multiple rows per call.
    convex_rows = []
    for row in insights:
        convex_rows.append({
            "campaignId": campaign["_id"],
            "metaEntityId": row.get("ad_id") or row.get("adset_id") or row.get("campaign_id"),
            "entityLevel": "ad" if row.get("ad_id") else (
                "adset" if row.get("adset_id") else "campaign"
            ),
            "date": row.get("date_start"),
            "spendGrosze": int(float(row.get("spend", 0)) * 100),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "cpcGrosze": int(float(row.get("cpc", 0)) * 100),
            "cpmGrosze": int(float(row.get("cpm", 0)) * 100),
            "ctrPct": float(row.get("ctr", 0)),
            "frequency": float(row.get("frequency", 0)),
            "reach": int(row.get("reach", 0)),
        })

    inserted = await convex.run_mutation(
        "campaigns:insertDailyMetrics", {"rows": convex_rows},
    )
    return int(inserted or 0)


# ---------------------------------------------------------------------------
# Task 3: attribution diff processor
# ---------------------------------------------------------------------------

async def attribute_bookings(ctx: dict[str, Any]) -> dict[str, int]:
    """Every 30 min: scan recent salon_scrapes diffs. For each diff
    that suggests a new booking (review_count_delta > 0 strongest),
    look up clicks within 7-day window via fn_recent_clicks_for_salon,
    record attribution row + push event to Convex."""
    sb = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )

    # Pull pairs from last 1 hour (cron runs :00, :30 — 1h window
    # leaves margin for delayed scrapes)
    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    pairs = (
        sb.table("v_salon_scrape_pairs")
        .select(
            "current_scrape_id, booksy_id, current_scraped_at, "
            "reviews_count_delta, services_count_delta"
        )
        .gte("current_scraped_at", cutoff)
        .execute()
        .data
        or []
    )
    candidate_pairs = [
        p for p in pairs
        if (p.get("reviews_count_delta") or 0) > 0
           or (p.get("services_count_delta") or 0) != 0
    ]
    if not candidate_pairs:
        return {"pairs_examined": 0, "attributions": 0}

    convex = ConvexClient()
    attributions = 0

    for pair in candidate_pairs:
        booksy_id = pair["booksy_id"]
        # Find candidate clicks within 7-day attribution window
        clicks = sb.rpc(
            "fn_recent_clicks_for_salon",
            {"p_booksy_id": booksy_id, "p_window_hours": 168},
        ).execute().data or []
        if not clicks:
            continue

        # Determine signal type — prefer 'review' over 'service_count_delta'
        if (pair.get("reviews_count_delta") or 0) > 0:
            signal_type = "review"
        else:
            signal_type = "service_count_delta"

        # Last-click attribution: take most-recent click, attribute booking
        # to it. Future enhancement: multi-touch, time-decay.
        click = clicks[0]
        sb.table("ad_booking_attributions").insert({
            "click_id": click["click_id"],
            "scrape_pair_current_id": pair["current_scrape_id"],
            "booksy_id": booksy_id,
            "utm_campaign": click.get("utm_campaign"),
            "utm_term": click.get("utm_term"),
            "signal_type": signal_type,
            "attribution_window_hours": 168,
            "click_to_booking_hours": float(click.get("hours_ago", 0)),
        }).execute()

        # FUNNEL_AUDIT R6: atrybucja = zdarzenie lejka (idempotentnie po
        # click_id + scrape pair; zapis best-effort).
        from services.funnel_events import record_funnel_event

        record_funnel_event(
            sb,
            event_type="ad_attribution",
            source="meta_ads",
            dedupe_key=f"ad_attr:{click['click_id']}:{pair['current_scrape_id']}",
            campaign_id=click.get("utm_campaign"),
            metadata={
                "booksy_id": booksy_id,
                "signal_type": signal_type,
                "utm_term": click.get("utm_term"),
            },
        )

        # Mirror event to Convex if utm_campaign matches a known campaign
        if click.get("utm_campaign"):
            try:
                await convex.run_mutation(
                    "campaigns:recordAttributionEventByMetaIds",
                    {
                        "metaCampaignId": click["utm_campaign"],
                        "metaAdId": click.get("utm_term"),
                        "salonId": booksy_id,
                        "eventType": "booking_inferred"
                            if signal_type == "service_count_delta"
                            else "booking_confirmed",
                        "occurredAt": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                        "metadata": f"signal={signal_type}, hours_ago={click.get('hours_ago')}",
                    },
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("Convex attribution event mirror failed: %s", e)

        attributions += 1

    if attributions:
        logger.info("[campaigns] attributed %d new bookings to ad clicks", attributions)
    return {"pairs_examined": len(candidate_pairs), "attributions": attributions}


ALL_CAMPAIGN_TASKS = [
    push_approved_campaigns,
    fetch_daily_metrics,
    attribute_bookings,
]
