"""Issue #34 — quad-tree salon discovery."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from supabase import Client

from services.sb_client import make_supabase_client

from config import settings

logger = logging.getLogger("bagent.discovery")

# Booksy listing API caps every response at 20 results; that's the
# split threshold: any bbox showing >20 needs subdivision.
PAGE_CAP = 20

# Hard ceiling for recursion depth — at 12 levels the bbox is ~1km^2
# at Polish latitudes, more than sharp enough for any city. Past this
# we accept the partial result so an API quirk doesn't infinite-loop.
MAX_DEPTH = 12

# Minimum bbox span (degrees) below which we stop recursing even if
# count > 20 — prevents us from probing 100m × 100m squares.
MIN_SPAN_DEG = 0.005

# Politeness delay between API calls (seconds). bextract is local
# so this throttles the discovery side's per-bbox cadence. Lowered
# 0.4 → 0.2 (= 5 probes/s discovery side). Combined with scrape
# orchestrator at ~2 req/s, total Booksy load ~7 req/s sustained —
# still under the 429 threshold observed earlier in this issue.
INTER_CALL_DELAY_SEC = 0.2

# Flush progress to discovery_runs every N probes. Lets the Workers
# dashboard show live bboxes_walked / salons_found counters for long
# sweeps (mazowieckie x dense category can probe 500+ bboxes).
PROGRESS_FLUSH_INTERVAL = 5

# Saturation early-stop: once K consecutive bboxes contribute zero
# NEW salons (everything we see is already deduped against existing
# discovered_salon_categories rows), the subtree is exhausted —
# further subdivision just wastes API calls without surfacing more
# salons. Bumped 25 → 200 after coverage audit found 4.85% global
# coverage (16k/345k Booksy-reported). Salon Kosmetyczny × mazowieckie
# was bailing at 42/7984 = 0.5% because the first 25 bboxes returned
# the SAME top-20 ranked salons from every quadrant of Warsaw —
# the streak triggered before quad-tree could go deep enough to
# surface ranks 21+. 200 gives the walker enough runway to descend
# into actually-different geographic areas before giving up.
SATURATION_ZERO_STREAK = 200

# Hard "you don't get to bail" coverage floor: if Booksy reported
# total_count_hint says there are >5x more salons in this combo than
# we've discovered so far in this run, IGNORE the saturation streak
# and keep walking. This catches the "dense city" pathology where
# top-20 ranking overlap dominates early bboxes and looks like
# saturation but actually just means we haven't subdivided enough.
COVERAGE_FLOOR_RATIO = 5


@dataclass
class DiscoveryResult:
    category_id: int
    voivodeship_id: int
    bboxes_walked: int = 0
    salons_found: int = 0
    salons_new: int = 0
    total_count_hint: int | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    error: str | None = None
    # Tracking for saturation early-stop. Reset to 0 every time a
    # bbox surfaces ANY new salon; bumped on each zero-new bbox.
    # When it crosses SATURATION_ZERO_STREAK the recursion bails.
    zero_new_streak: int = 0

    @property
    def duration_sec(self) -> float:
        end = self.finished_at if self.finished_at is not None else time.time()
        return end - self.started_at


_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


def _reset_client() -> Client:
    """Drop the cached client and build a fresh one. Use at the start
    of each combo to clear out HTTP/2 connection pools that have been
    poisoned by ConnectionTerminated cascades — once httpx's HTTP/2
    pool sees a bad GOAWAY frame, every subsequent upsert in that pool
    raises ConnectionInputs.RECV_WINDOW_UPDATE-in-CLOSED-state until
    the client is rebuilt. 19k errors in worker log = exactly this."""
    global _supabase_client
    _supabase_client = None
    return _get_client()


async def _fetch_listing(
    http: httpx.AsyncClient,
    category_id: int,
    location_id: int,
    bbox: tuple[float, float, float, float],
) -> dict[str, Any]:
    """Call bextract /api/booksy/listing for one bbox probe.

    bbox = (lat_max, lng_max, lat_min, lng_min)

    On HTTP 429 from Booksy (surfaced as 500 by bextract) we backoff
    and retry up to 4 times. Discovery is rate-limited end-to-end so
    even when the cron fans out many combos serial-ish, bursts can hit
    the limit; serial-with-backoff is enough.
    """
    if not settings.bextract_api_url or not settings.bextract_api_key:
        raise RuntimeError("bextract_api_url / bextract_api_key not configured")
    area = f"{bbox[0]:.5f},{bbox[1]:.5f},{bbox[2]:.5f},{bbox[3]:.5f}"
    url = f"{settings.bextract_api_url.rstrip('/')}/api/booksy/listing"
    params = {
        "category": str(category_id),
        "location_id": str(location_id),
        "area": area,
    }
    headers = {"x-api-key": settings.bextract_api_key}

    backoff = 5.0
    for attempt in range(1, 5):
        r = await http.get(url, params=params, headers=headers, timeout=30.0)
        if r.status_code == 200:
            return r.json()
        body = r.text[:300]
        is_rate_limit = "429" in body or r.status_code in (429, 503)
        if is_rate_limit and attempt < 4:
            logger.warning(
                "[discovery] bextract %s rate-limited (attempt %d), backing off %.0fs",
                r.status_code, attempt, backoff,
            )
            await asyncio.sleep(backoff)
            backoff *= 2
            continue
        raise RuntimeError(f"bextract listing returned HTTP {r.status_code}: {body}")
    raise RuntimeError("bextract listing exhausted retries")


def _split_bbox(
    bbox: tuple[float, float, float, float],
) -> list[tuple[float, float, float, float]]:
    """Split into 4 equal quadrants. Each child bbox is half-width × half-height."""
    lat_max, lng_max, lat_min, lng_min = bbox
    lat_mid = (lat_max + lat_min) / 2
    lng_mid = (lng_max + lng_min) / 2
    return [
        (lat_max, lng_max, lat_mid, lng_mid),  # NE
        (lat_max, lng_mid, lat_mid, lng_min),  # NW
        (lat_mid, lng_max, lat_min, lng_mid),  # SE
        (lat_mid, lng_mid, lat_min, lng_min),  # SW
    ]


def _bbox_span(bbox: tuple[float, float, float, float]) -> float:
    return min(abs(bbox[0] - bbox[2]), abs(bbox[1] - bbox[3]))


def _upsert_salon(
    client: Client,
    business: dict[str, Any],
    *,
    category_id: int,
    voivodeship_id: int,
) -> bool:
    """Discovery writes a partial row directly to `salons` (NULL
    last_scraped_at marks "needs full scrape" — scrape orchestrator
    drains those via enqueue_discovered_salons SQL helper).

    Returns a tuple (is_new_salon, is_new_mapping):
      - is_new_salon: first time seeing this booksy_id ANYWHERE in DB
      - is_new_mapping: first time seeing this booksy_id in this
        specific (category × voivodeship) combo

    The mapping bool is what saturation tracking uses. A re-run of an
    already-walked combo will report is_new_salon=False everywhere
    (the salon exists from previous run) but is_new_mapping=False
    only if the mapping row was already there. This keeps re-runs
    naturally resumable: previously-walked subtrees saturate fast,
    new bboxes still surface as work to do.
    """
    from datetime import datetime, timezone

    booksy_id = business.get("id")
    if not isinstance(booksy_id, int):
        return (False, False)

    location = business.get("location") or {}
    name = (business.get("name") or "")[:300] or f"Salon {booksy_id}"
    slug = (business.get("slug") or business.get("url") or "")[:200] or None
    city = (location.get("city") or location.get("city_name") or "")[:100] or None
    lat = location.get("latitude") or business.get("latitude")
    lng = location.get("longitude") or business.get("longitude")
    reviews_count = business.get("reviews_count")
    reviews_rank = business.get("reviews_rank")
    now_iso = datetime.now(tz=timezone.utc).isoformat()

    # Check existence in salons (the canonical store). booksy_id has a
    # UNIQUE constraint so this is a fast index hit.
    existing = (
        client.table("salons")
        .select("booksy_id,last_scraped_at")
        .eq("booksy_id", booksy_id)
        .limit(1)
        .execute()
    )
    existing_row = (existing.data or [None])[0]
    is_new = existing_row is None
    has_full_scrape = existing_row is not None and existing_row.get("last_scraped_at") is not None

    if is_new:
        # Insert partial. last_scraped_at MUST be explicit None — the
        # salons schema has DEFAULT now() so omitting the column would
        # set it to NOW, hiding the "needs full scrape" marker.
        row: dict[str, Any] = {
            "booksy_id": booksy_id,
            "name": name,
            "slug": slug,
            "city": city,
            "primary_category_id": category_id,
            "reviews_count": reviews_count or 0,
            "created_at": now_iso,
            "updated_at": now_iso,
            "last_scraped_at": None,
        }
        if lat is not None:
            row["latitude"] = float(lat)
        if lng is not None:
            row["longitude"] = float(lng)
        if reviews_rank is not None:
            row["reviews_rank"] = float(reviews_rank)
        client.table("salons").insert(row).execute()
    elif not has_full_scrape:
        # Discovery already saw this salon (also still no full scrape) —
        # refresh the partial fields in case Booksy renamed/reclassified.
        update: dict[str, Any] = {
            "name": name,
            "city": city,
            "reviews_count": reviews_count or 0,
            "updated_at": now_iso,
        }
        if reviews_rank is not None:
            update["reviews_rank"] = float(reviews_rank)
        if lat is not None:
            update["latitude"] = float(lat)
        if lng is not None:
            update["longitude"] = float(lng)
        client.table("salons").update(update).eq("booksy_id", booksy_id).execute()
    # else: full-scrape salon — leave it alone, ingester knows better.

    # Many-to-many mapping (category x voivodeship) — write regardless
    # of whether the salons row was new, partial-update, or untouched.
    existing_map = (
        client.table("discovered_salon_categories")
        .select("seen_count")
        .eq("booksy_id", booksy_id)
        .eq("category_id", category_id)
        .eq("voivodeship_id", voivodeship_id)
        .limit(1)
        .execute()
    )
    is_new_mapping = not existing_map.data
    if existing_map.data:
        prev = existing_map.data[0].get("seen_count") or 1
        client.table("discovered_salon_categories").update({
            "last_seen_at": now_iso,
            "seen_count": prev + 1,
        }).eq("booksy_id", booksy_id).eq("category_id", category_id).eq("voivodeship_id", voivodeship_id).execute()
    else:
        client.table("discovered_salon_categories").insert({
            "booksy_id": booksy_id,
            "category_id": category_id,
            "voivodeship_id": voivodeship_id,
            "first_seen_at": now_iso,
            "last_seen_at": now_iso,
            "seen_count": 1,
        }).execute()

    return (is_new, is_new_mapping)


async def _walk_bbox(
    http: httpx.AsyncClient,
    _client_unused: Client | None,  # kept for back-compat; we always use module-level
    *,
    category_id: int,
    voivodeship_id: int,
    bbox: tuple[float, float, float, float],
    depth: int,
    result: DiscoveryResult,
    run_id: int | None = None,
) -> None:
    """Quad-tree walker. Probes a bbox; if API returned full page (20)
    AND total_count > 20 AND we still have room to subdivide, recurse;
    otherwise upsert what we got. Updates the discovery_runs row's
    progress counters every PROGRESS_FLUSH_INTERVAL probes so the
    Workers dashboard shows live activity for long-running sweeps."""
    # Saturation early-stop: streak only triggers if we're NOT
    # severely under-covered. Without this guard, dense urban combos
    # (Salon Kosmetyczny × mazowieckie: 7984 reported) bailed at
    # ~42 unique salons because Booksy's top-20 ranking returns the
    # same Warsaw salons from every quadrant — looks like saturation
    # but is just shallow exploration.
    #
    # Coverage floor rule: if Booksy hint says 5x more salons exist
    # than we've discovered THIS RUN, ignore the streak — there's
    # provably more to find, the streak is a false signal.
    if result.zero_new_streak >= SATURATION_ZERO_STREAK:
        hint = result.total_count_hint or 0
        provably_under_covered = (
            hint > 0
            and hint > result.salons_new * COVERAGE_FLOOR_RATIO
        )
        if not provably_under_covered:
            if not result.error:
                result.error = (
                    f"saturated: {SATURATION_ZERO_STREAK} zero-new bboxes "
                    f"(salons_new={result.salons_new}, hint={hint or 'unknown'})"
                )
            return
        # Coverage floor blocks the bail-out — reset the streak so
        # we don't immediately re-trigger on the next bbox, and keep
        # walking. Logged so the dashboard can show "we're trying
        # harder than usual on this combo".
        logger.info(
            "[discovery] coverage-floor override: salons_new=%d hint=%d ratio=%.1fx — keep walking",
            result.salons_new, hint, hint / max(result.salons_new, 1),
        )
        result.zero_new_streak = 0

    response = await _fetch_listing(http, category_id, voivodeship_id, bbox)
    await asyncio.sleep(INTER_CALL_DELAY_SEC)
    result.bboxes_walked += 1

    # Live progress: flush counters to the discovery_runs row every
    # PROGRESS_FLUSH_INTERVAL probes (with debounce to skip flushing
    # on every single probe). Cheap UPDATE — keys are PK btree.
    if run_id is not None and result.bboxes_walked % PROGRESS_FLUSH_INTERVAL == 0:
        try:
            _get_client().table("discovery_runs").update({
                "bboxes_walked": result.bboxes_walked,
                "salons_found": result.salons_found,
                "salons_new": result.salons_new,
                "total_count_hint": result.total_count_hint,
                # Heartbeat for the smart reaper — see migration 031.
                # Reaper kills 'running' rows whose last_progress_at
                # is >30 min old, instead of an absolute 4h age cap.
                "last_progress_at": "now()",
            }).eq("id", run_id).execute()
        except Exception as e:  # noqa: BLE001
            logger.warning("[discovery] progress flush failed: %s", e)
            if "ConnectionTerminated" in str(e) or "ConnectionInputs" in str(e):
                _reset_client()

    businesses = response.get("businesses") or []
    total = response.get("businesses_count") or 0
    if result.total_count_hint is None and depth == 0:
        result.total_count_hint = total

    # Booksy listing API returns the top-20 ranked salons per bbox. To
    # see anything past those 20 we must shrink the bbox. needs_split
    # captures the four conditions that say "shrinking is worth it":
    #
    #   1. We hit the page cap (20 returned) AND total is bigger than
    #      that — otherwise we already have everything.
    #   2. We haven't recursed past MAX_DEPTH (~1km^2) yet.
    #   3. The bbox is still bigger than the MIN_SPAN floor.
    #
    # Two early-stops added (#34 follow-up to fix infinite-ish runs in
    # zachodniopomorskie x trening-i-dieta where 1050 bboxes were probed
    # for 269 total salons):
    #
    #   4. If total_count_hint at the root is small (<= NO_SPLIT_TOTAL),
    #      give up and accept the top-20 — quadtree won't surface much
    #      and ranks 21-100 aren't worth 100+ extra API calls per combo.
    #   5. If a probe returns zero NEW salons relative to what we've
    #      seen across this run AND we already have >= total_count_hint
    #      in result.salons_new, the rest of subdivisions are wasted.
    NO_SPLIT_TOTAL = 100
    needs_split = (
        len(businesses) >= PAGE_CAP
        and total > PAGE_CAP
        and depth < MAX_DEPTH
        and _bbox_span(bbox) > MIN_SPAN_DEG
    )
    if needs_split and result.total_count_hint is not None:
        if result.total_count_hint <= NO_SPLIT_TOTAL:
            needs_split = False
        elif result.salons_new >= result.total_count_hint:
            # We've already discovered more unique salons than Booksy
            # told us about (cross-bbox ranking overlap accounts for the
            # rest). Keep walking but don't subdivide further.
            needs_split = False

    if needs_split:
        # Recurse into 4 quadrants. Don't upsert here — children will probe
        # the same area more granularly, deduped by booksy_id.
        for child in _split_bbox(bbox):
            await _walk_bbox(
                http, None,  # _client_unused; walk_bbox always uses module-level _get_client()
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                bbox=child,
                depth=depth + 1,
                result=result,
                run_id=run_id,
            )
        return

    # Leaf — store every business in this response. Always grab a
    # fresh handle from _get_client() so a mid-walk _reset_client()
    # actually takes effect on the next iteration.
    bbox_added_new_mapping = False
    for biz in businesses:
        try:
            is_new_salon, is_new_mapping = _upsert_salon(
                _get_client(), biz,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
            )
            result.salons_found += 1
            if is_new_salon:
                result.salons_new += 1
            if is_new_mapping:
                bbox_added_new_mapping = True
        except Exception as e:  # noqa: BLE001
            err_str = str(e)
            logger.warning("[discovery] upsert failed for %s: %s", biz.get("id"), err_str)
            # HTTP/2 cascade detection — supabase-py's httpx pool gets
            # stuck after a single ConnectionTerminated and every
            # subsequent upsert blows up the same way. Reset the
            # module-level client so the next iteration of THIS loop
            # AND every recursive child call gets a fresh pool.
            if "ConnectionTerminated" in err_str or "ConnectionInputs" in err_str:
                logger.info("[discovery] HTTP/2 cascade detected — resetting supabase client")
                _reset_client()

    # Saturation tracking — bump streak when this bbox added zero new
    # combo mappings, reset to 0 when at least one new mapping landed.
    # Empty businesses list also counts as zero-new.
    if bbox_added_new_mapping:
        result.zero_new_streak = 0
    else:
        result.zero_new_streak += 1


async def discover_combo(
    category_id: int,
    voivodeship_id: int,
) -> DiscoveryResult:
    """Walk one (category, voivodeship) combo. Records a discovery_runs row.
    Idempotent: re-running the same combo just refreshes last_seen_at."""
    # Fresh client per combo — supabase-py's HTTP/2 pool gets poisoned
    # by a single ConnectionTerminated and every subsequent upsert in
    # that pool then raises ConnectionInputs.RECV_WINDOW_UPDATE-in-
    # CLOSED-state forever (see 19k-error log incident). Rebuilding
    # the client here scopes the cascade to a single combo at most.
    client = _reset_client()

    voiv_resp = (
        client.table("booksy_voivodeships")
        .select("bbox_lat_max,bbox_lng_max,bbox_lat_min,bbox_lng_min")
        .eq("id", voivodeship_id)
        .single()
        .execute()
    )
    voiv = voiv_resp.data
    if not voiv:
        raise RuntimeError(f"voivodeship_id={voivodeship_id} not found")
    root_bbox = (
        float(voiv["bbox_lat_max"]),
        float(voiv["bbox_lng_max"]),
        float(voiv["bbox_lat_min"]),
        float(voiv["bbox_lng_min"]),
    )

    # Open run row for the audit trail
    run_insert = (
        client.table("discovery_runs")
        .insert({
            "category_id": category_id,
            "voivodeship_id": voivodeship_id,
        })
        .execute()
    )
    run_id = (run_insert.data or [{}])[0].get("id")

    result = DiscoveryResult(category_id=category_id, voivodeship_id=voivodeship_id)
    try:
        async with httpx.AsyncClient() as http:
            await _walk_bbox(
                http, None,  # _client_unused; walker uses module-level _get_client()
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                bbox=root_bbox,
                depth=0,
                result=result,
                run_id=run_id,
            )
        result.finished_at = time.time()
        # Walk completed normally (with or without cap_hit). Record as
        # 'done' regardless — collected data is real and committed.
        # The optional `error` field carries cap_hit notes so operators
        # can see "we stopped early" without it counting as a failure.
        _get_client().table("discovery_runs").update({
            "status": "done",
            "finished_at": "now()",
            "bboxes_walked": result.bboxes_walked,
            "salons_found": result.salons_found,
            "salons_new": result.salons_new,
            "total_count_hint": result.total_count_hint,
            "error": result.error,  # None for clean runs, "cap_hit:..." for graceful early-stop
        }).eq("id", run_id).execute()
        logger.info(
            "[discovery] cat=%s voiv=%s done bboxes=%d found=%d new=%d hint=%s in %.1fs%s",
            category_id, voivodeship_id,
            result.bboxes_walked, result.salons_found, result.salons_new,
            result.total_count_hint, result.duration_sec,
            f" [{result.error}]" if result.error else "",
        )
    except Exception as e:  # noqa: BLE001
        result.error = str(e)[:500]
        result.finished_at = time.time()
        _get_client().table("discovery_runs").update({
            "status": "failed",
            "finished_at": "now()",
            "bboxes_walked": result.bboxes_walked,
            "salons_found": result.salons_found,
            "salons_new": result.salons_new,
            "error": result.error,
        }).eq("id", run_id).execute()
        logger.exception("[discovery] cat=%s voiv=%s FAILED", category_id, voivodeship_id)
    return result


async def discover_all() -> list[DiscoveryResult]:
    """Sweep every (category, voivodeship) combo. Used by the weekly
    arq cron. Runs sequentially so one bextract call at a time —
    discovery is throughput-bounded by Booksy rate limits, not by us."""
    client = _get_client()
    cats = client.table("booksy_categories").select("id").execute().data or []
    voivs = client.table("booksy_voivodeships").select("id").execute().data or []
    cat_ids = sorted(c["id"] for c in cats)
    voiv_ids = sorted(v["id"] for v in voivs)

    logger.info(
        "[discovery] starting full sweep: %d categories x %d voivodeships = %d combos",
        len(cat_ids), len(voiv_ids), len(cat_ids) * len(voiv_ids),
    )
    results: list[DiscoveryResult] = []
    for cat_id in cat_ids:
        for voiv_id in voiv_ids:
            try:
                results.append(await discover_combo(cat_id, voiv_id))
            except Exception as e:  # noqa: BLE001
                logger.warning("[discovery] combo (%s,%s) raised: %s", cat_id, voiv_id, e)
    total_new = sum(r.salons_new for r in results)
    total_found = sum(r.salons_found for r in results)
    logger.info(
        "[discovery] full sweep done: %d combos, %d found, %d new",
        len(results), total_found, total_new,
    )
    return results
