"""Issue #34 — location-hierarchy discovery (replaces bbox quad-tree).

Why: Booksy listing API caps results at top-100 per (category, location_id)
query. Previous bbox quad-tree returned the same TOP-20 ranked salons
from every sub-bbox — empirically achieved 0.5% coverage on dense urban
combos (Salon Kosmetyczny × mazowieckie: 42/7984).

The new endpoint `/api/booksy/location/:id` exposes Booksy's canonical
location hierarchy: voivodeship → city → district. Each location_id
returns its own top-100 ranking (different across siblings — verified
empirically: city3 vs voiv6 share only 4/100 IDs).

Strategy:
  1. For each (category, voivodeship) combo, query the voivodeship
     itself with per_page=100.
  2. Recursively expand canonical_children — each city, each district —
     querying each with per_page=100.
  3. Aggregate unique salons via `discovered_salon_categories` upserts.
  4. Optional: keep the bbox walker as backup for areas Booksy doesn't
     have hierarchy for.

For Warsaw alone: 194 districts × 100 = 19,400 potential listings vs
1,587 actual barber shops = ~10x oversample → near-100% coverage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from supabase import Client

from config import settings
from discovery.discover import (  # reuse existing bits
    _get_client,
    _reset_client,
    _upsert_salon,
    PROGRESS_FLUSH_INTERVAL,
)

logger = logging.getLogger("bagent.discovery.locations")


# Per-call delay to keep Booksy happy. bextract proxies and Booksy
# itself observed limit ~5 req/s; 0.4s = 2.5 req/s sustained.
INTER_CALL_DELAY_SEC = 0.4

# How deep into canonical_children to recurse. Booksy's hierarchy
# goes country → voivodeship → city → district, so 4 is enough.
# Beyond that you hit street-level which has 0-2 salons each — not
# worth the cost.
MAX_DEPTH = 4

# Per-call result cap.
PER_PAGE = 100


@dataclass
class LocationDiscoveryResult:
    category_id: int
    voivodeship_id: int
    locations_walked: int = 0  # how many distinct location_ids we queried
    salons_found: int = 0  # total responses (incl duplicates)
    salons_new: int = 0  # NEW to salons table (first-ever sighting)
    new_mappings: int = 0  # NEW (salon × category × voivodeship) rows
    total_count_hint: int | None = None  # Booksy's reported total at root
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    error: str | None = None
    visited: set[int] = field(default_factory=set)

    @property
    def duration_sec(self) -> float:
        end = self.finished_at if self.finished_at is not None else time.time()
        return end - self.started_at


async def _fetch_listing_by_location(
    http: httpx.AsyncClient,
    category_id: int,
    location_id: int,
) -> dict[str, Any]:
    """Call bextract /api/booksy/listing with per_page=100 (no bbox)."""
    if not settings.bextract_api_url or not settings.bextract_api_key:
        raise RuntimeError("bextract_api_url / bextract_api_key not configured")
    url = f"{settings.bextract_api_url.rstrip('/')}/api/booksy/listing"
    params = {
        "category": str(category_id),
        "location_id": str(location_id),
        "per_page": str(PER_PAGE),
    }
    headers = {"x-api-key": settings.bextract_api_key}
    backoff = 5.0
    for attempt in range(1, 5):
        r = await http.get(url, params=params, headers=headers, timeout=60.0)
        if r.status_code == 200:
            return r.json()
        body = r.text[:300]
        is_rate_limit = "429" in body or r.status_code in (429, 503)
        if is_rate_limit and attempt < 4:
            logger.warning(
                "[locations] bextract %s rate-limited (attempt %d), backing off %.0fs",
                r.status_code, attempt, backoff,
            )
            await asyncio.sleep(backoff)
            backoff *= 2
            continue
        raise RuntimeError(
            f"bextract returned HTTP {r.status_code} for cat={category_id} loc={location_id}: {body}"
        )
    raise RuntimeError("rate-limited after 4 attempts")


async def _walk_location(
    http: httpx.AsyncClient,
    category_id: int,
    voivodeship_id: int,
    location_id: int,
    depth: int,
    result: LocationDiscoveryResult,
    run_id: int | None = None,
) -> None:
    """Query one location_id, upsert all returned salons, recurse into
    canonical_children (up to MAX_DEPTH)."""
    if location_id in result.visited:
        return
    result.visited.add(location_id)

    response = await _fetch_listing_by_location(http, category_id, location_id)
    await asyncio.sleep(INTER_CALL_DELAY_SEC)
    result.locations_walked += 1

    businesses = response.get("businesses") or []
    if result.total_count_hint is None and depth == 0:
        result.total_count_hint = response.get("businesses_count")

    # Upsert each business — _upsert_salon uses voivodeship_id (NOT the
    # current location_id) so the mapping rolls up to the parent
    # (category × voivodeship) for cohort consistency with the old
    # bbox walker.
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
                result.new_mappings += 1
        except Exception as e:  # noqa: BLE001
            err_str = str(e)
            logger.warning("[locations] upsert failed for %s: %s", biz.get("id"), err_str)
            if "ConnectionTerminated" in err_str or "ConnectionInputs" in err_str:
                _reset_client()

    # Live progress flush (heartbeat for the smart reaper)
    if run_id is not None and result.locations_walked % PROGRESS_FLUSH_INTERVAL == 0:
        try:
            _get_client().table("discovery_runs").update({
                "bboxes_walked": result.locations_walked,  # reuse existing column
                "salons_found": result.salons_found,
                "salons_new": result.salons_new,
                "total_count_hint": result.total_count_hint,
                "last_progress_at": "now()",
            }).eq("id", run_id).execute()
        except Exception as e:  # noqa: BLE001
            logger.warning("[locations] progress flush failed: %s", e)
            if "ConnectionTerminated" in str(e) or "ConnectionInputs" in str(e):
                _reset_client()

    # Recurse into canonical_children for deeper coverage. The listing
    # response embeds location_details on every call so we don't need a
    # separate /api/booksy/location/:id roundtrip.
    if depth < MAX_DEPTH:
        details = response.get("location_details") or {}
        children = details.get("canonical_children") or []
        for child_id in children:
            try:
                child_int = int(child_id)
            except (TypeError, ValueError):
                continue
            if child_int in result.visited:
                continue
            await _walk_location(
                http,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                location_id=child_int,
                depth=depth + 1,
                result=result,
                run_id=run_id,
            )


async def discover_combo_via_locations(
    category_id: int,
    voivodeship_id: int,
) -> LocationDiscoveryResult:
    """Walk one (category, voivodeship) combo via location-id hierarchy.
    Drop-in replacement for discover_combo() in the bbox approach."""
    client = _reset_client()

    # Open a discovery_runs row so the dashboard sees progress
    run_insert = (
        client.table("discovery_runs")
        .insert({
            "category_id": category_id,
            "voivodeship_id": voivodeship_id,
        })
        .execute()
    )
    run_id = (run_insert.data or [{}])[0].get("id")

    result = LocationDiscoveryResult(
        category_id=category_id,
        voivodeship_id=voivodeship_id,
    )
    try:
        async with httpx.AsyncClient() as http:
            await _walk_location(
                http,
                category_id=category_id,
                voivodeship_id=voivodeship_id,
                location_id=voivodeship_id,
                depth=0,
                result=result,
                run_id=run_id,
            )
        result.finished_at = time.time()
        _get_client().table("discovery_runs").update({
            "status": "done",
            "finished_at": "now()",
            "bboxes_walked": result.locations_walked,
            "salons_found": result.salons_found,
            "salons_new": result.salons_new,
            "total_count_hint": result.total_count_hint,
            "error": result.error,
        }).eq("id", run_id).execute()
        logger.info(
            "[locations] cat=%s voiv=%s done locations=%d found=%d new=%d new_mappings=%d hint=%s in %.1fs",
            category_id, voivodeship_id,
            result.locations_walked, result.salons_found, result.salons_new,
            result.new_mappings, result.total_count_hint, result.duration_sec,
        )
    except Exception as e:  # noqa: BLE001
        result.error = str(e)[:500]
        result.finished_at = time.time()
        _get_client().table("discovery_runs").update({
            "status": "failed",
            "finished_at": "now()",
            "bboxes_walked": result.locations_walked,
            "salons_found": result.salons_found,
            "salons_new": result.salons_new,
            "error": result.error,
        }).eq("id", run_id).execute()
        logger.exception("[locations] cat=%s voiv=%s FAILED", category_id, voivodeship_id)
    return result
