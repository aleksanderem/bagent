"""Issue #34 — quad-tree salon discovery."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from supabase import Client, ClientOptions, create_client

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

# Politeness delay between API calls (seconds). bextract is local so
# this is per-call sleep rather than a token bucket; works fine for
# discovery cron throughput.
INTER_CALL_DELAY_SEC = 0.4


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

    @property
    def duration_sec(self) -> float:
        end = self.finished_at if self.finished_at is not None else time.time()
        return end - self.started_at


_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
            options=ClientOptions(schema="public"),
        )
    return _supabase_client


async def _fetch_listing(
    http: httpx.AsyncClient,
    category_id: int,
    location_id: int,
    bbox: tuple[float, float, float, float],
) -> dict[str, Any]:
    """Call bextract /api/booksy/listing for one bbox probe.

    bbox = (lat_max, lng_max, lat_min, lng_min)
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
    r = await http.get(url, params=params, headers=headers, timeout=30.0)
    if r.status_code != 200:
        raise RuntimeError(f"bextract listing returned HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


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
    """UPSERT one salon. Returns True if new (first sighting), False if update."""
    booksy_id = business.get("id")
    if not isinstance(booksy_id, int):
        return False

    location = business.get("location") or {}
    row = {
        "booksy_id": booksy_id,
        "slug": (business.get("slug") or business.get("url") or "")[:200] or None,
        "name": (business.get("name") or "")[:300] or None,
        "city": (location.get("city") or location.get("city_name") or "")[:100] or None,
        "lat": location.get("latitude") or business.get("latitude"),
        "lng": location.get("longitude") or business.get("longitude"),
        "primary_category_id": category_id,
        "voivodeship_id": voivodeship_id,
        "reviews_count": business.get("reviews_count"),
        "reviews_rank": business.get("reviews_rank"),
        "last_seen_at": "now()",
    }
    # Drop None geo so we don't overwrite better data on second sighting.
    if row["lat"] is None:
        row.pop("lat")
    if row["lng"] is None:
        row.pop("lng")
    if row["last_seen_at"] == "now()":
        # Supabase Python client doesn't expand server-side now() inside upsert;
        # use a clientside timestamp instead.
        from datetime import datetime, timezone
        row["last_seen_at"] = datetime.now(tz=timezone.utc).isoformat()

    # Check if existed before by counting — cheaper than INSERT-ON-CONFLICT
    # round-trip with selected return.
    existing = (
        client.table("discovered_salons")
        .select("booksy_id")
        .eq("booksy_id", booksy_id)
        .limit(1)
        .execute()
    )
    is_new = not (existing.data or [])

    if is_new:
        row["first_seen_at"] = row["last_seen_at"]
        row["seen_count"] = 1
        client.table("discovered_salons").insert(row).execute()
    else:
        # Don't increment seen_count via supabase-py raw — RPC would be cleaner,
        # but a 1-extra-query update is fine at discovery scale.
        client.rpc(
            "exec_sql",  # if not present, fall through to plain update with RPC missing
            {},
        ) if False else None
        client.table("discovered_salons").update({
            "name": row.get("name"),
            "city": row.get("city"),
            "lat": row.get("lat"),
            "lng": row.get("lng"),
            "reviews_count": row.get("reviews_count"),
            "reviews_rank": row.get("reviews_rank"),
            "last_seen_at": row["last_seen_at"],
        }).eq("booksy_id", booksy_id).execute()
    return is_new


async def _walk_bbox(
    http: httpx.AsyncClient,
    client: Client,
    *,
    category_id: int,
    voivodeship_id: int,
    bbox: tuple[float, float, float, float],
    depth: int,
    result: DiscoveryResult,
) -> None:
    """Quad-tree walker. Probes a bbox; if API returned full page (20)
    AND total_count > 20 AND we still have room to subdivide, recurse;
    otherwise upsert what we got."""
    response = await _fetch_listing(http, category_id, voivodeship_id, bbox)
    await asyncio.sleep(INTER_CALL_DELAY_SEC)
    result.bboxes_walked += 1

    businesses = response.get("businesses") or []
    total = response.get("businesses_count") or 0
    if result.total_count_hint is None and depth == 0:
        result.total_count_hint = total

    needs_split = (
        len(businesses) >= PAGE_CAP
        and total > PAGE_CAP
        and depth < MAX_DEPTH
        and _bbox_span(bbox) > MIN_SPAN_DEG
    )

    if needs_split:
        # Recurse into 4 quadrants. Don't upsert here — children will probe
        # the same area more granularly, deduped by booksy_id.
        for child in _split_bbox(bbox):
            await _walk_bbox(
                http, client,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                bbox=child,
                depth=depth + 1,
                result=result,
            )
        return

    # Leaf — store every business in this response.
    for biz in businesses:
        try:
            is_new = _upsert_salon(
                client, biz,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
            )
            result.salons_found += 1
            if is_new:
                result.salons_new += 1
        except Exception as e:  # noqa: BLE001
            logger.warning("[discovery] upsert failed for %s: %s", biz.get("id"), e)


async def discover_combo(
    category_id: int,
    voivodeship_id: int,
) -> DiscoveryResult:
    """Walk one (category, voivodeship) combo. Records a discovery_runs row.
    Idempotent: re-running the same combo just refreshes last_seen_at."""
    client = _get_client()

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
                http, client,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                bbox=root_bbox,
                depth=0,
                result=result,
            )
        result.finished_at = time.time()
        client.table("discovery_runs").update({
            "status": "done",
            "finished_at": "now()",
            "bboxes_walked": result.bboxes_walked,
            "salons_found": result.salons_found,
            "salons_new": result.salons_new,
            "total_count_hint": result.total_count_hint,
        }).eq("id", run_id).execute()
        logger.info(
            "[discovery] cat=%s voiv=%s done bboxes=%d found=%d new=%d hint=%s in %.1fs",
            category_id, voivodeship_id,
            result.bboxes_walked, result.salons_found, result.salons_new,
            result.total_count_hint, result.duration_sec,
        )
    except Exception as e:  # noqa: BLE001
        result.error = str(e)[:500]
        result.finished_at = time.time()
        client.table("discovery_runs").update({
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
