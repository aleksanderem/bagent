"""Nightly taxonomy refresh tasks (arq cron).

Three jobs run sequentially via the arq scheduler:

  03:00 UTC — refresh_taxonomy_views
              REFRESH MATERIALIZED VIEW CONCURRENTLY for mv_booksy_treatments
              and mv_treatment_name_lookup. Picks up new treatments added by
              recent scrapes.

  03:15 UTC — embed_new_services
              Find salon_scrape_services rows where embedding_applied_at IS NULL
              and embed via OpenAI text-embedding-3-small in batches. Capped at
              50 000 per night (~$0.015 budget per night).

  03:30 UTC — refresh_inferred_treatments
              Apply backfill_inferred_treatments() in loop until exhausted
              or cap hit. Handles both new scrapes (embedding_applied_at just
              got set) and stale rows (>7d since last inference).

Crons declared in workers/main.py SCRAPE_CRONS list. ALL_TAXONOMY_TASKS
export adds the functions to the WorkerSettings.functions list.

The whole thing is idempotent: tasks check their work-stamps (embedding_applied_at,
inference_applied_at) and skip rows already done. Safe to run partial / interrupted.
"""

from __future__ import annotations

import asyncio
import httpx
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


# Cap how many rows each nightly job processes — bounds OpenAI cost and
# runtime even if a giant scrape batch landed during the day.
EMBED_BATCH_CAP = 50_000        # ~$0.015 OpenAI cost at $0.02/1M tokens
INFERENCE_BATCH_CAP = 200_000   # ~10 min Postgres time
INFERENCE_BATCH_SIZE = 5_000    # per LATERAL call to match_treatment_hybrid
STALE_INFERENCE_DAYS = 7        # re-run inference for rows older than this
FOCUS_BATCH_CAP = 5_000         # ~10 min @ 10/s; trigger refreshes ~daily
STALE_FOCUS_DAYS = 14           # re-compute focus older than this

# variant_id backfill (added 2026-06-22, S0078). variant_id is written ONLY by
# the mig-127 RPCs (backfill_service_variants / _untagged); nothing in bagent
# ever called them, so fresh scrapes kept variant_id NULL and the competitor
# pricing pipeline early-exited to empty. This nightly job calls them. Caps
# bound the nightly run; any overflow keeps variant_matched_at NULL and is
# picked up the next night (the RPCs only stamp rows they actually evaluate).
VARIANT_BATCH_CAP = 300_000     # in-tid daily inflow + margin (~4 min @ 5k/~4s)
VARIANT_UNTAGGED_CAP = 100_000  # untagged inflow is much smaller
VARIANT_BATCH_SIZE = 5_000      # per RPC call (mig 127 statement_timeout=120s)


async def refresh_taxonomy_views(ctx: dict[str, Any]) -> str:
    """Refresh mv_booksy_treatments + mv_treatment_name_lookup CONCURRENTLY.

    These views are derived from salon_scrape_services. New scrapes during
    the day add new (treatment_id, name, parent_id) tuples → mv needs to
    pick them up.

    CONCURRENTLY = doesn't block readers during refresh. Both views have
    UNIQUE indexes (required for CONCURRENTLY).
    """
    from services.healthcheck import ping
    from services.sb_client import make_supabase_client
    from config import settings

    # This RPC runs REFRESH MATERIALIZED VIEW CONCURRENTLY on views derived
    # from salon_scrape_services (44 GB+). Measured 22-32s and climbing as the
    # table grows, so the module-default 30s httpx read timeout intermittently
    # fires ReadTimeout even though Postgres finishes (statement_timeout=10min).
    # Per-call 120s override keeps us well under Kong's 150s proxy read_timeout
    # without loosening fail-fast on every other supabase call in the worker.
    client = make_supabase_client(
        settings.supabase_url,
        settings.supabase_service_key,
        timeout=httpx.Timeout(120.0, connect=10.0),
    )
    t0 = time.time()
    try:
        await asyncio.to_thread(
            lambda: client.rpc("refresh_booksy_treatments", {}).execute()
        )
        dt = time.time() - t0
        msg = f"taxonomy_views refreshed in {dt:.1f}s"
        logger.info(msg)
        await ping("HC_PING_TAXONOMY_VIEWS_REFRESH")
        return msg
    except Exception as e:
        logger.exception("refresh_taxonomy_views failed: %s", e)
        await ping("HC_PING_TAXONOMY_VIEWS_REFRESH", fail=True)
        raise


async def embed_new_services(ctx: dict[str, Any]) -> str:
    """Embed up to EMBED_BATCH_CAP services whose embedding_applied_at IS NULL.

    Uses OpenAI text-embedding-3-small + bulk_update_service_embeddings RPC
    for efficient persistence. Skips silently when OPENAI_API_KEY missing.

    Note: as of 2026-05-13 the ingest write path embeds inline (see
    scripts/ingest_salon_jsons._embed_service_names_sync). This cron's
    role is therefore CATCH-UP for legacy rows or rows where the inline
    embed failed (network blip / OpenAI rate limit during ingest).
    """
    from services.healthcheck import ping

    if not os.getenv("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY missing — embed_new_services skipped")
        # Still ping so the check stays green when intentionally unwired.
        await ping("HC_PING_TAXONOMY_EMBED_CATCHUP")
        return "skipped: no openai key"

    from openai import AsyncOpenAI
    from services.sb_client import make_supabase_client
    from services.embeddings import token_budget_row_batches
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    oai = AsyncOpenAI()

    BATCH_FETCH = 1000
    PARALLELISM = 4

    total_done = 0
    t_start = time.time()

    sem = asyncio.Semaphore(PARALLELISM)

    async def embed_chunk(rows: list[dict]) -> list[dict]:
        names = [r["name"].strip()[:512] for r in rows]
        async with sem:
            try:
                resp = await oai.embeddings.create(
                    model="text-embedding-3-small", input=names,
                )
            except Exception as e:
                logger.warning("OpenAI embedding error: %s — retry after 3s", e)
                await asyncio.sleep(3)
                resp = await oai.embeddings.create(
                    model="text-embedding-3-small", input=names,
                )
        return [
            {"id": r["id"], "embedding": list(d.embedding)}
            for r, d in zip(rows, resp.data)
        ]

    while total_done < EMBED_BATCH_CAP:
        rows = await asyncio.to_thread(
            lambda: client.table("salon_scrape_services")
            .select("id,name")
            .is_("embedding_applied_at", "null")
            .not_.is_("name", "null")
            .order("id")
            .limit(BATCH_FETCH)
            .execute()
            .data
        )
        rows = [r for r in (rows or []) if r.get("name") and len(r["name"].strip()) >= 3]
        if not rows:
            break

        # Token-budget chunks (not fixed row count) — keeps each OpenAI request
        # under the 300k cap even if name inputs grow. embed_chunk sends
        # name[:512], so size each row by that exact text.
        chunks = token_budget_row_batches(rows, lambda r: r["name"].strip()[:512])
        results = await asyncio.gather(*(embed_chunk(c) for c in chunks))

        for payload in results:
            await asyncio.to_thread(
                lambda p=payload: client.rpc(
                    "bulk_update_service_embeddings", {"payloads": p},
                ).execute()
            )

        total_done += sum(len(c) for c in chunks)

    dt = time.time() - t_start
    msg = f"embed_new_services: {total_done} rows in {dt:.0f}s"
    logger.info(msg)
    await ping("HC_PING_TAXONOMY_EMBED_CATCHUP")
    return msg


async def refresh_inferred_treatments(ctx: dict[str, Any]) -> str:
    """Run backfill_inferred_treatments in a loop until nothing left or cap hit.

    Two flows:
      1. New rows (embedding_applied_at just got set by embed_new_services)
         get inference_applied_at = NULL → picked up here.
      2. Stale rows (inference_applied_at < now() - STALE_INFERENCE_DAYS):
         reset to NULL before this job → re-processed with current logic
         (catches taxonomy evolution + retroactive specificity-rule changes).

    Step (2) is done by mark_stale_inferred_treatments_for_refresh().
    """
    from services.healthcheck import ping
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    t_start = time.time()

    # Step 1: mark stale for refresh
    res = await asyncio.to_thread(
        lambda: client.rpc(
            "mark_stale_inferred_treatments_for_refresh",
            {"p_stale_after_days": STALE_INFERENCE_DAYS, "p_max_rows": 50000},
        ).execute()
    )
    marked_stale = res.data if isinstance(res.data, int) else 0
    logger.info("Marked %d stale rows for re-inference", marked_stale)

    # Step 2: drain backfill in batches
    total_processed = 0
    while total_processed < INFERENCE_BATCH_CAP:
        res = await asyncio.to_thread(
            lambda: client.rpc(
                "backfill_inferred_treatments",
                {"p_batch_size": INFERENCE_BATCH_SIZE},
            ).execute()
        )
        n = res.data if isinstance(res.data, int) else 0
        if n == 0:
            break
        total_processed += n

    dt = time.time() - t_start
    msg = (
        f"refresh_inferred_treatments: stale_marked={marked_stale}, "
        f"processed={total_processed} in {dt:.0f}s"
    )
    logger.info(msg)
    await ping("HC_PING_TAXONOMY_INFERENCE_REFRESH")
    return msg


async def refresh_service_variants(ctx: dict[str, Any]) -> str:
    """Backfill salon_scrape_services.variant_id for chain-head services.

    The MISSING nightly job (added 2026-06-22, S0078). variant_id is written
    ONLY by backfill_service_variants / backfill_untagged_services_via_variant
    (mig 127). Nothing in bagent ever called them — variant_id was populated by
    ad-hoc manual RPC runs until ~2026-06-12, then stopped, leaving ~1.7M
    chain-head services with variant_id NULL. Without variant_id the competitor
    report pricing pipeline early-exits (competitor_analysis.py ~679) and emits
    an empty priceComparison. This job closes the loop:
      inline embedding (ingest) -> booksy_treatment_id (inline/inferred)
      -> variant_id (THIS job) -> competitor pricing comparisons.

    Both RPCs are idempotent + self-scoping: they target chain-head rows with
    variant_id NULL and a watermark retry (variant_matched_at IS NULL OR
    < max(treatment_variants.refreshed_at)), stamp variant_matched_at on every
    evaluation (cost control), and return ROW_COUNT (rows evaluated, not
    matched). Loop each until it returns 0 or the nightly cap is hit; capped
    overflow keeps variant_matched_at NULL and is drained the next night.
    """
    from services.healthcheck import ping
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    t_start = time.time()

    # Phase A: in-tid match (booksy_treatment_id present). Search space is
    # pre-scoped by parent_treatment_id so cross-family mistakes are impossible
    # — hence the looser 0.55 default. This covers the overwhelming majority
    # (fresh scrapes carry booksy_treatment_id inline at ingest).
    in_tid = 0
    while in_tid < VARIANT_BATCH_CAP:
        res = await asyncio.to_thread(
            lambda: client.rpc(
                "backfill_service_variants",
                {"p_batch_size": VARIANT_BATCH_SIZE, "p_min_similarity": 0.55},
            ).execute()
        )
        n = res.data if isinstance(res.data, int) else 0
        if n == 0:
            break
        in_tid += n

    # Phase B: untagged catch-up (booksy_treatment_id NULL). Global ANN over all
    # variants with no taxonomy guard — a wrong match re-tags the whole family,
    # so the stricter 0.70 default. Sets booksy_treatment_id + variant_id from
    # the matched variant's parent. Much smaller population.
    untagged = 0
    while untagged < VARIANT_UNTAGGED_CAP:
        res = await asyncio.to_thread(
            lambda: client.rpc(
                "backfill_untagged_services_via_variant",
                {"p_batch_size": VARIANT_BATCH_SIZE, "p_min_similarity": 0.70},
            ).execute()
        )
        n = res.data if isinstance(res.data, int) else 0
        if n == 0:
            break
        untagged += n

    dt = time.time() - t_start
    msg = (
        f"refresh_service_variants: in_tid={in_tid}, untagged={untagged} "
        f"in {dt:.0f}s"
    )
    logger.info(msg)
    await ping("HC_PING_VARIANT_MATCH_REFRESH")
    return msg


async def refresh_salon_focus_distributions(ctx: dict[str, Any]) -> str:
    """Refresh salons.{portfolio_embedding, focus_distribution,
    focus_variant_distribution} dla salonów stale lub nigdy nie liczonych.

    Trigger: trg_invalidate_salon_focus (mig 063) nulluje focus_computed_at gdy
    chain head zmienia się. Plus stale po STALE_FOCUS_DAYS od ostatniego compute.
    Cap: FOCUS_BATCH_CAP salonów per night (~10 min @ 10/s).
    """
    from datetime import datetime, timedelta, timezone

    from services.focus_score import SalonFocusBundle
    from services.healthcheck import ping
    from services.sb_client import make_supabase_client
    from config import settings

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    t_start = time.time()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_FOCUS_DAYS)).isoformat()

    null_ids: list[int] = []
    start = 0
    while True:
        page = await asyncio.to_thread(
            lambda s=start: client.table("salons")
                .select("id")
                .is_("focus_computed_at", "null")
                .range(s, s + 999)
                .execute()
        )
        rows = page.data or []
        null_ids.extend(r["id"] for r in rows)
        if len(rows) < 1000 or len(null_ids) >= FOCUS_BATCH_CAP:
            break
        start += 1000

    old_ids: list[int] = []
    if len(null_ids) < FOCUS_BATCH_CAP:
        start = 0
        while True:
            page = await asyncio.to_thread(
                lambda s=start: client.table("salons")
                    .select("id")
                    .lt("focus_computed_at", cutoff)
                    .range(s, s + 999)
                    .execute()
            )
            rows = page.data or []
            old_ids.extend(r["id"] for r in rows)
            if len(rows) < 1000 or len(null_ids) + len(old_ids) >= FOCUS_BATCH_CAP:
                break
            start += 1000

    salon_ids = list(set(null_ids + old_ids))[:FOCUS_BATCH_CAP]
    logger.info(
        "refresh_salon_focus: %d candidates (null=%d, old=%d, cap=%d)",
        len(salon_ids), len(null_ids), len(old_ids), FOCUS_BATCH_CAP,
    )

    if not salon_ids:
        await ping("HC_PING_SALON_FOCUS_REFRESH")
        return "refresh_salon_focus: 0 stale"

    success = 0
    skipped = 0
    for salon_id in salon_ids:
        try:
            salon_res = await asyncio.to_thread(
                lambda sid=salon_id: client.table("salons")
                    .select("id, booksy_id, top_service_names")
                    .eq("id", sid).single().execute()
            )
            if not salon_res.data:
                skipped += 1
                continue
            booksy_id = salon_res.data.get("booksy_id")
            top_names = salon_res.data.get("top_service_names") or []

            scrape_res = await asyncio.to_thread(
                lambda bid=booksy_id: client.table("salon_scrapes")
                    .select("id").eq("booksy_id", bid).eq("is_chain_head", True)
                    .order("scraped_at", desc=True).limit(1).execute()
            )
            if not scrape_res.data:
                skipped += 1
                continue
            scrape_id = scrape_res.data[0]["id"]

            svc_res = await asyncio.to_thread(
                lambda sid=scrape_id: client.table("salon_scrape_services").select(
                    "id, name, description, photos, booksy_treatment_id, "
                    "variant_id, price_grosze, name_embedding",
                ).eq("scrape_id", sid).execute()
            )
            services = svc_res.data or []

            bundle = SalonFocusBundle.from_services(
                salon_id=salon_id, booksy_id=booksy_id,
                services=services, top_service_names=top_names,
            )
            payload = bundle.to_db_payload()
            payload["focus_computed_at"] = datetime.now(timezone.utc).isoformat()
            await asyncio.to_thread(
                lambda sid=salon_id, p=payload: client.table("salons")
                    .update(p).eq("id", sid).execute()
            )
            success += 1
        except Exception as e:  # noqa: BLE001
            logger.warning("refresh_salon_focus failed for salon_id=%s: %s", salon_id, e)
            skipped += 1

    dt = time.time() - t_start
    msg = f"refresh_salon_focus: {success} done, {skipped} skipped in {dt:.0f}s"
    logger.info(msg)
    await ping("HC_PING_SALON_FOCUS_REFRESH")
    return msg


ALL_TAXONOMY_TASKS = [
    refresh_taxonomy_views,
    embed_new_services,
    refresh_inferred_treatments,
    refresh_service_variants,
    refresh_salon_focus_distributions,
]
