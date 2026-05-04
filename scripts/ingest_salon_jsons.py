"""Re-runnable JSON batch ingester for Booksy salon JSONs.

Walks a folder of raw Booksy business endpoint responses (filename pattern
`{booksy_id}.json`) and writes each one to Supabase as:

1. salons — UPSERT by booksy_id (registry / latest state)
2. salon_scrapes — APPEND (time-series)
3. salon_scrape_services — APPEND (per-service normalized rows)
4. salon_reviews — APPEND with ON CONFLICT DO NOTHING (unique per salon+review_id)
5. salon_top_services — UPSERT (latest snapshot, not time-series)
6. json_ingestion_log — audit trail with SHA-256 content_hash for idempotency

Idempotency is driven by json_ingestion_log.content_hash UNIQUE — each file's
content is hashed up-front, and if a row with that hash already exists the
file is skipped. Re-running the ingester on the same folder is a no-op.

Usage:
    python -m scripts.ingest_salon_jsons \\
        --source /Users/alex/Desktop/MOJE_PROJEKTY/BEAUTY_AUDIT/json/ \\
        --batch-tag first-batch-2026-04-08 \\
        --batch-size 100

See docs/plans/2026-04-08-competitor-report-pipeline.md (Comp Etap 0.2).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

# Make bagent package importable when running as `python -m scripts.ingest_salon_jsons`
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from supabase import Client

from services.sb_client import make_supabase_client  # noqa: E402

from config import settings  # noqa: E402

logger = logging.getLogger("ingest_salon_jsons")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str | None) -> str | None:
    """Lowercase + strip diacritics + collapse non-alnum to single spaces.

    Mirrors convex/audit/booksyParser.normalizeServiceName so normalized_name
    matches across the pipeline.
    """
    if not name:
        return None
    decomposed = unicodedata.normalize("NFD", name.lower())
    without_diacritics = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", without_diacritics)
    return re.sub(r"\s+", " ", cleaned).strip() or None


def _as_int(value: Any) -> int | None:
    """Coerce to int or None without exceptions."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "1", "yes"}:
            return True
        if low in {"false", "0", "no"}:
            return False
    return None


def _non_empty(value: Any) -> Any:
    """Return value unless it's an empty string (Booksy uses '' for missing)."""
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _parse_iso(value: Any) -> str | None:
    """Pass through an ISO-looking datetime string, else None."""
    if not value:
        return None
    if isinstance(value, str):
        return value
    return None


def _extract_city_from_regions(regions: list[dict] | None) -> tuple[str | None, str | None, str | None]:
    """Pull (city, state/region, country) from business.regions[]."""
    if not isinstance(regions, list):
        return None, None, None
    city = state = country = None
    for r in regions:
        if not isinstance(r, dict):
            continue
        t = r.get("type")
        name = r.get("name")
        if t == "city" and city is None:
            city = name
        elif t == "state" and state is None:
            state = name
        elif t == "country" and country is None:
            country = name
    return city, state, country


def _zip_from_address(address: str | None) -> str | None:
    """Pull Polish postal code (XX-XXX) from a free-form address string."""
    if not isinstance(address, str):
        return None
    match = re.search(r"\b(\d{2}-\d{3})\b", address)
    return match.group(1) if match else None


T = TypeVar("T")


def _retry_http(fn: Callable[[], T], *, op_name: str, max_attempts: int = 4) -> T:
    """Run `fn()` with exponential backoff on transient HTTP/2 protocol errors.

    The Supabase reverse proxy occasionally resets HTTP/2 streams mid-batch
    (seen as httpcore.LocalProtocolError / RemoteProtocolError). Each retry
    forces a fresh connection via the underlying httpx client's keepalive
    logic — no explicit pool reset needed.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — broad catch, match by message
            msg = str(e)
            transient = (
                "ConnectionTerminated" in msg
                or "RECV_WINDOW_UPDATE" in msg
                or "RemoteProtocolError" in msg
                or "LocalProtocolError" in msg
                or "Server disconnected" in msg
                or "ConnectionState.CLOSED" in msg
                or "ReadError" in msg
                or "ENHANCE_YOUR_CALM" in msg
            )
            last_exc = e
            if not transient or attempt == max_attempts:
                raise
            sleep_for = (0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.25)
            logger.warning(
                "Transient HTTP error on %s (attempt %d/%d): %s — retrying in %.1fs",
                op_name, attempt, max_attempts, type(e).__name__, sleep_for,
            )
            time.sleep(sleep_for)
    # Should be unreachable because we raise in the loop on final attempt
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Core ingester
# ---------------------------------------------------------------------------

class IngestError(Exception):
    """Raised when a single-file ingest fails. Caller should log + continue."""


class SalonJsonIngester:
    def __init__(self, client: Client, batch_tag: str | None, dry_run: bool) -> None:
        self.client = client
        self.batch_tag = batch_tag
        self.dry_run = dry_run

    # -- Idempotency ---------------------------------------------------------

    def _hashes_already_ingested(self, hashes: list[str]) -> set[str]:
        """Return the subset of `hashes` already present in json_ingestion_log.

        Uses a single full-table scan of content_hash (5267 rows in a typical
        first-batch run is trivially small — ~350KB of payload) and intersects
        locally. This avoids generating an IN filter with 5267 hex strings
        that produces a >300KB URL which the reverse proxy rejects with HTTP/2
        ENHANCE_YOUR_CALM.
        """
        if not hashes:
            return set()

        wanted = set(hashes)
        found: set[str] = set()
        # Paginate through the whole table in 1000-row pages via Range header.
        page_size = 1000
        offset = 0
        while True:
            res = _retry_http(
                lambda: self.client.table("json_ingestion_log")
                    .select("content_hash")
                    .range(offset, offset + page_size - 1)
                    .execute(),
                op_name="json_ingestion_log.select",
            )
            rows = res.data or []
            if not rows:
                break
            for row in rows:
                h = row.get("content_hash")
                if isinstance(h, str) and h in wanted:
                    found.add(h)
            if len(rows) < page_size:
                break
            offset += page_size
        return found

    # -- salons (upsert registry) -------------------------------------------

    def _upsert_salon(self, business: dict, scraped_at: str) -> int:
        """UPSERT into salons by booksy_id. Returns salons.id (internal PK)."""
        booksy_id = _as_int(business.get("id"))
        if booksy_id is None:
            raise IngestError("business.id missing")

        location = business.get("location") or {}
        coord = (location or {}).get("coordinate") or {}
        city, _state, _country = _extract_city_from_regions(business.get("regions"))
        address = location.get("address") if isinstance(location, dict) else None
        if city is None and isinstance(location, dict):
            city = location.get("city")

        # top_service_names: list of strings (from top_services[].name)
        top_services = business.get("top_services") or []
        top_service_names = [
            ts.get("name") for ts in top_services if isinstance(ts, dict) and ts.get("name")
        ]

        row: dict[str, Any] = {
            "booksy_id": booksy_id,
            "name": business.get("name") or "Unknown",
            "slug": _non_empty(business.get("slug")),
            "description": _non_empty(business.get("description")),
            "city": city,
            "address": address,
            "latitude": _as_float(coord.get("latitude")),
            "longitude": _as_float(coord.get("longitude")),
            "primary_category_id": _as_int(business.get("primary_category")),
            "reviews_rank": _as_float(business.get("reviews_rank")),
            "reviews_count": _as_int(business.get("reviews_count")) or 0,
            "pricing_level": _as_int(business.get("pricing_level")),
            "thumbnail_photo": _non_empty(business.get("thumbnail_photo")),
            "staff_count": len(business.get("staff") or []),
            "phone": _non_empty(business.get("phone")),
            "email": _non_empty(business.get("public_email")),
            "website": _non_empty(business.get("website")),
            "facebook_url": _non_empty(business.get("facebook_link")),
            "instagram_url": _non_empty(business.get("instagram_link")),
            "booksy_url": _non_empty(business.get("url")),
            "cover_photo": _non_empty(business.get("photo")),
            "logo_photo": _non_empty(business.get("thumbnail_photo")),
            "booking_policy": _non_empty(business.get("booking_policy")),
            "amenities": business.get("amenities") or [],
            "top_service_names": top_service_names,
            "promoted": bool(business.get("promoted")),
            "is_recommended": bool(business.get("is_recommended")),
            "last_scraped_at": scraped_at,
            "updated_at": scraped_at,
        }

        if self.dry_run:
            return -1

        res = _retry_http(
            lambda: self.client.table("salons").upsert(row, on_conflict="booksy_id").execute(),
            op_name="salons.upsert",
        )
        if not res.data:
            # Supabase may return empty data on upsert — fetch id explicitly
            sel = _retry_http(
                lambda: self.client.table("salons")
                    .select("id")
                    .eq("booksy_id", booksy_id)
                    .limit(1)
                    .execute(),
                op_name="salons.select",
            )
            if not sel.data:
                raise IngestError(f"Failed to upsert salon booksy_id={booksy_id}")
            return int(sel.data[0]["id"])
        return int(res.data[0]["id"])

    # -- salon_scrapes (append) ---------------------------------------------

    def _insert_scrape(self, business: dict, raw_payload: dict, scraped_at: str,
                       partner_system: str, salon_ref_id: int | None) -> str:
        """APPEND a row into salon_scrapes. Returns the new scrape UUID."""
        location = business.get("location") or {}
        coord = (location or {}).get("coordinate") or {}
        city, region_state, country = _extract_city_from_regions(business.get("regions"))
        address = location.get("address") if isinstance(location, dict) else None

        service_categories = business.get("service_categories") or []
        total_services = sum(len(c.get("services") or []) for c in service_categories)
        active_services = 0
        promo_services = 0
        for cat in service_categories:
            for s in cat.get("services") or []:
                if s.get("active"):
                    active_services += 1
                variants = s.get("variants") or []
                if any((v or {}).get("promotion") for v in variants if isinstance(v, dict)):
                    promo_services += 1

        deposit_cancel = business.get("deposit_cancel_time") or {}
        deposit_cancel_days = _as_int(deposit_cancel.get("days")) if isinstance(deposit_cancel, dict) else None

        row: dict[str, Any] = {
            # Identity
            "booksy_id": _as_int(business.get("id")),
            "salon_ref_id": salon_ref_id,
            "convex_audit_id": None,
            "triggered_by": "manual",  # CHECK constraint allows: audit|cron|manual|retry
            "source_url": _non_empty(business.get("url")),

            # Salon metadata
            "salon_name": business.get("name") or "Unknown",
            "salon_slug": _non_empty(business.get("slug")),
            "salon_description": _non_empty(business.get("description")),

            # Location
            "salon_address": address,
            "salon_city": city or (location.get("city") if isinstance(location, dict) else None),
            "salon_zip": _zip_from_address(address),
            "salon_region": region_state,
            "salon_country": country or "Polska",
            "salon_lat": _as_float(coord.get("latitude")),
            "salon_lng": _as_float(coord.get("longitude")),

            # Media
            "salon_logo_url": _non_empty(business.get("thumbnail_photo")),
            "cover_photo_url": _non_empty(business.get("photo")),
            "thumbnail_photo_url": _non_empty(business.get("thumbnail_photo")),

            # Contact
            "phone": _non_empty(business.get("phone")),
            "email": _non_empty(business.get("public_email")),
            "website": _non_empty(business.get("website")),
            "facebook_url": _non_empty(business.get("facebook_link")),
            "instagram_url": _non_empty(business.get("instagram_link")),
            "booksy_url": _non_empty(business.get("url")),

            # Reputation
            "reviews_rank": _as_float(business.get("reviews_rank")),
            "reviews_count": _as_int(business.get("reviews_count")),
            "pricing_level": _as_int(business.get("pricing_level")),
            "promoted": bool(business.get("promoted")),

            # Categorization
            "primary_category_id": _as_int(business.get("primary_category")),
            "business_categories": business.get("business_categories") or [],

            # Staff
            "staff_count": len(business.get("staff") or []),
            "staff": business.get("staff") or [],

            # Aggregate counts
            "total_services": total_services,
            "total_categories": len(service_categories),
            "active_services_count": active_services,
            "promo_services_count": promo_services,

            # Raw payload — store the full file, not just the business object
            "raw_response": raw_payload,
            "scraped_at": scraped_at,

            # Etap 0.1 columns
            "partner_system": partner_system,
            "salon_subdomain": _non_empty(business.get("subdomain")),
            "booking_max_modification_time": _as_int(business.get("booking_max_modification_time")),
            "booking_max_lead_time": _as_int(business.get("booking_max_lead_time")),
            "deposit_cancel_days": deposit_cancel_days,
            "pos_pay_by_app": _as_bool(business.get("pos_pay_by_app_enabled")),
            "pos_market_pay": _as_bool(business.get("pos_market_pay_enabled")),
            "has_online_services": _as_bool(business.get("has_online_services")),
            "has_online_vouchers": _as_bool(business.get("has_online_vouchers")),
            "has_safety_rules": _as_bool(business.get("has_safety_rules")),
            "low_availability": _as_bool(business.get("low_availability")),
            "max_discount_rate": _as_float(business.get("max_discount_rate")),
            "salon_network": _non_empty(business.get("salon_network")),
            "service_fee": _as_float(business.get("service_fee")),
            "parking_info": _non_empty(business.get("parking")),
            "wheelchair_access": _non_empty(business.get("wheelchair_access")),
        }

        if self.dry_run:
            return "dry-run-uuid"

        res = _retry_http(
            lambda: self.client.table("salon_scrapes").insert(row).execute(),
            op_name="salon_scrapes.insert",
        )
        if not res.data:
            raise IngestError(f"Failed to insert salon_scrapes for booksy_id={row['booksy_id']}")
        return str(res.data[0]["id"])

    # -- salon_scrape_services (append) -------------------------------------

    def _insert_services(self, business: dict, scrape_id: str, booksy_id: int,
                         scraped_at: str) -> int:
        """APPEND every service into salon_scrape_services. Returns count inserted."""
        service_categories = business.get("service_categories") or []
        rows: list[dict[str, Any]] = []

        for cat in service_categories:
            if not isinstance(cat, dict):
                continue
            cat_name = cat.get("name")
            cat_id = _as_int(cat.get("id"))
            cat_sort = _as_int(cat.get("order"))

            for svc in cat.get("services") or []:
                if not isinstance(svc, dict):
                    continue

                variants = svc.get("variants") or []
                first_variant = variants[0] if variants and isinstance(variants[0], dict) else {}
                duration_minutes = _as_int(first_variant.get("duration"))
                price_float = _as_float(first_variant.get("price"))
                price_grosze = int(round(price_float * 100)) if price_float is not None else None
                variant_type = first_variant.get("type")
                is_from_price = variant_type == "S" if variant_type else False

                treatment = svc.get("treatment") if isinstance(svc.get("treatment"), dict) else None
                treatment_id = _as_int(svc.get("treatment_id"))
                canonical_id = f"booksy_{treatment_id}" if treatment_id is not None else None

                photos = svc.get("photos") or []
                first_photo = photos[0] if photos and isinstance(photos[0], dict) else None
                image_url = first_photo.get("url") if first_photo else None

                is_promo = any((v or {}).get("promotion") for v in variants if isinstance(v, dict))
                promotion_data = svc.get("promotion") or None
                if promotion_data is None and is_promo:
                    # Pull the first variant's promotion field for convenience
                    for v in variants:
                        if isinstance(v, dict) and v.get("promotion"):
                            promotion_data = v.get("promotion")
                            break

                suggest = svc.get("suggest_services") or {}
                suggest_tokens = suggest.get("input") if isinstance(suggest, dict) else None
                suggest_context = suggest.get("contexts") if isinstance(suggest, dict) else None

                row: dict[str, Any] = {
                    "scrape_id": scrape_id,
                    "booksy_id": booksy_id,
                    "scraped_at": scraped_at,
                    "convex_audit_id": None,

                    "category_name": cat_name,
                    "category_id": cat_id,
                    "category_sort_order": cat_sort,

                    "name": svc.get("name") or "",
                    "normalized_name": _normalize_name(svc.get("name")),
                    "description": _non_empty(svc.get("description")),

                    "canonical_id": canonical_id,
                    "booksy_treatment_id": treatment_id,
                    "booksy_service_id": _as_int(svc.get("id")),
                    "treatment_name": treatment.get("name") if treatment else None,
                    "treatment_parent_id": _as_int(svc.get("treatment_parent_id")),

                    # body_part / target_gender / technology / classification_confidence
                    # are rule-based enrichments that happen downstream — leave NULL.
                    "body_part": None,
                    "target_gender": None,
                    "technology": None,
                    "classification_confidence": None,

                    "price": _non_empty(svc.get("service_price")),
                    "price_grosze": price_grosze,
                    "is_from_price": is_from_price,
                    "omnibus_price_grosze": None,

                    "duration_seconds": duration_minutes * 60 if duration_minutes is not None else None,
                    "duration_minutes": duration_minutes,

                    "image_url": image_url,
                    "photos": photos if photos else None,

                    "is_promo": is_promo,
                    "promotion_data": promotion_data,

                    "is_active": _as_bool(svc.get("active")) if svc.get("active") is not None else True,
                    "is_online_service": _as_bool(svc.get("is_online_service")) or False,
                    "is_traveling_service": _as_bool(svc.get("is_traveling_service")) or False,
                    "addons_available": _as_bool(svc.get("addons_available")) or False,
                    "is_available_for_booking": _as_bool(svc.get("is_available_for_customer_booking")) if svc.get("is_available_for_customer_booking") is not None else True,

                    "service_type": _non_empty(svc.get("type")),
                    "description_type": _non_empty(svc.get("description_type")),
                    "padding_type": _non_empty(svc.get("padding_type")),

                    "staffer_ids": svc.get("staffer_id") or None,

                    "variants": variants or None,

                    "combo_type": _non_empty(svc.get("combo_type")),
                    "combo_children": svc.get("combo_children") or None,

                    "suggest_tokens": suggest_tokens,
                    "suggest_context": suggest_context,

                    "tags": svc.get("tags") or None,
                    "sort_order": _as_int(svc.get("order")),
                }
                rows.append(row)

        if not rows:
            return 0
        if self.dry_run:
            return len(rows)

        # Batch-insert in chunks of 500 to keep single requests reasonable
        inserted = 0
        for i in range(0, len(rows), 500):
            chunk = rows[i : i + 500]
            res = _retry_http(
                lambda chunk=chunk: self.client.table("salon_scrape_services").insert(chunk).execute(),
                op_name="salon_scrape_services.insert",
            )
            if not res.data:
                raise IngestError(
                    f"Failed to insert salon_scrape_services chunk "
                    f"(booksy_id={booksy_id}, chunk_start={i})"
                )
            inserted += len(res.data)
        return inserted

    # -- salon_reviews (append with ON CONFLICT DO NOTHING) -----------------

    def _insert_reviews(self, business: dict, salon_id: int, scraped_at: str) -> int:
        reviews = business.get("reviews") or []
        rows: list[dict[str, Any]] = []
        for r in reviews:
            if not isinstance(r, dict):
                continue
            user = r.get("user") or {}
            rank = _as_int(r.get("rank"))
            if rank is None or not (1 <= rank <= 5):
                # CHECK constraint requires rank BETWEEN 1 AND 5 — skip invalid
                continue
            review_id = _as_int(r.get("id"))
            if review_id is None:
                continue
            rows.append({
                "salon_id": salon_id,
                "booksy_review_id": review_id,
                "rank": rank,
                "title": _non_empty(r.get("title")),
                "review_text": _non_empty(r.get("review")),
                "user_first_name": user.get("first_name") if isinstance(user, dict) else None,
                "user_last_initial": user.get("last_name") if isinstance(user, dict) else None,
                "user_avatar_url": user.get("avatar") if isinstance(user, dict) else None,
                "services": r.get("services") or None,
                "staff": r.get("staff") or None,
                "reply_content": _non_empty(r.get("reply_content")),
                "reply_updated_at": _parse_iso(r.get("reply_updated")),
                "review_created_at": _parse_iso(r.get("created")) or scraped_at,
                "review_updated_at": _parse_iso(r.get("updated")),
                "source": _non_empty(r.get("source")),
                "scraped_at": scraped_at,
            })
        if not rows:
            return 0
        if self.dry_run:
            return len(rows)
        # UPSERT on (salon_id, booksy_review_id) — ignore_duplicates leaves
        # existing rows untouched, which matches the "append with ON CONFLICT
        # DO NOTHING" requirement for re-runs against the same salon.
        res = _retry_http(
            lambda: self.client.table("salon_reviews")
                .upsert(rows, on_conflict="salon_id,booksy_review_id", ignore_duplicates=True)
                .execute(),
            op_name="salon_reviews.upsert",
        )
        return len(res.data or [])

    # -- salon_top_services (upsert — latest snapshot) ----------------------

    def _insert_top_services(self, business: dict, salon_id: int, scraped_at: str) -> int:
        top = business.get("top_services") or []
        rows: list[dict[str, Any]] = []
        for idx, ts in enumerate(top):
            if not isinstance(ts, dict):
                continue
            service_id = _as_int(ts.get("id"))
            if service_id is None:
                continue
            rows.append({
                "salon_id": salon_id,
                "booksy_service_id": service_id,
                "booksy_treatment_id": _as_int(ts.get("treatment_id")),
                "name": ts.get("name") or "",
                "category_name": _non_empty(ts.get("category_name")),
                "description": _non_empty(ts.get("description")),
                "variants": ts.get("variants") or [],
                "is_online_service": _as_bool(ts.get("is_online_service")),
                "is_traveling_service": _as_bool(ts.get("is_traveling_service")),
                "sort_order": idx,
                "scraped_at": scraped_at,
            })
        if not rows:
            return 0
        if self.dry_run:
            return len(rows)
        res = _retry_http(
            lambda: self.client.table("salon_top_services")
                .upsert(rows, on_conflict="salon_id,booksy_service_id")
                .execute(),
            op_name="salon_top_services.upsert",
        )
        return len(res.data or [])

    # -- json_ingestion_log (audit trail) -----------------------------------

    def _log_ingestion(self, *, file_name: str, content_hash: str, booksy_id: int,
                       salon_id: int | None, scrape_id: str | None,
                       rows_inserted: dict[str, int], file_size: int,
                       file_mtime: str) -> None:
        row = {
            "file_name": file_name,
            "content_hash": content_hash,
            "booksy_id": booksy_id,
            "salon_id": salon_id,
            "scrape_id": scrape_id,
            "rows_inserted": rows_inserted,
            "source_batch": self.batch_tag,
            "file_size_bytes": file_size,
            "file_mtime": file_mtime,
        }
        if self.dry_run:
            return
        try:
            _retry_http(
                lambda: self.client.table("json_ingestion_log").insert(row).execute(),
                op_name="json_ingestion_log.insert",
            )
        except Exception as e:  # noqa: BLE001
            # The HTTP/2 retry loop sometimes causes a "phantom duplicate":
            # the first attempt's INSERT actually landed on the server, but
            # the response was lost when the proxy closed the stream. The
            # retry then hits the content_hash UNIQUE constraint. Treat this
            # as success — the audit row exists, idempotency is intact.
            if "23505" in str(e) or "duplicate key" in str(e):
                logger.info(
                    "json_ingestion_log already has content_hash (phantom duplicate — treating as success)"
                )
                return
            raise

    # -- Manual cleanup on partial failure ----------------------------------

    def _unwind_scrape(self, scrape_id: str) -> None:
        """Delete a salon_scrapes row and its cascade children.

        PostgREST doesn't support multi-request transactions. When a file
        ingest fails AFTER salon_scrapes has been inserted but BEFORE the
        json_ingestion_log audit row lands, the content_hash isn't recorded
        and a re-run would insert a duplicate salon_scrape. To keep the
        ingester idempotent we unwind the partial write here.

        salon_scrape_services FK has `ON DELETE CASCADE`, so deleting the
        parent row automatically removes its children.

        We best-effort swallow errors here because: (a) on restart the hash
        is still not in json_ingestion_log so the file will retry anyway,
        and (b) we already have a downstream error from the primary failure
        that we want to preserve for the caller.
        """
        try:
            _retry_http(
                lambda: self.client.table("salon_scrapes").delete().eq("id", scrape_id).execute(),
                op_name="salon_scrapes.delete (unwind)",
                max_attempts=2,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("Unwind of scrape_id=%s failed (will leak row): %s", scrape_id, e)

    # -- Per-file driver ----------------------------------------------------

    def ingest_file(self, file_path: Path, content_hash: str) -> dict[str, int]:
        """Ingest one JSON file. Raises IngestError on any failure — caller
        logs + continues, or in dry-run the Supabase writes are no-ops."""
        raw_bytes = file_path.read_bytes()
        try:
            payload = json.loads(raw_bytes)
        except json.JSONDecodeError as e:
            raise IngestError(f"invalid JSON: {e}") from e

        if not isinstance(payload, dict):
            raise IngestError("top-level JSON is not an object")

        # Top-level has `business` key in the sampled files. Fall back to
        # treating payload as the business object itself for safety.
        business = payload.get("business") if isinstance(payload.get("business"), dict) else payload
        if not isinstance(business, dict):
            raise IngestError("no business object found")

        booksy_id = _as_int(business.get("id"))
        if booksy_id is None:
            raise IngestError("business.id missing or non-numeric")

        # partners is a LIST (research spike bug — was misread as dict earlier)
        partners = business.get("partners")
        partner_system = "versum" if isinstance(partners, list) and "versum" in partners else "native"

        # scraped_at: prefer business.updated_at; else file mtime; else now
        scraped_at = (
            _parse_iso(business.get("updated_at"))
            or datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc).isoformat()
        )

        file_mtime_iso = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc).isoformat()
        file_size = len(raw_bytes)

        # 1. salons upsert (idempotent in itself — safe even on partial failure)
        salon_id = self._upsert_salon(business, scraped_at)

        # 2. salon_scrapes append — from here we own an unwind dependency
        scrape_id = self._insert_scrape(
            business, payload, scraped_at, partner_system,
            salon_ref_id=salon_id if salon_id > 0 else None,
        )

        # 3-5. services / reviews / top_services — on ANY failure we unwind
        # the scrape (which cascades children) so a retry produces a clean
        # new row instead of a duplicate.
        try:
            services_count = self._insert_services(business, scrape_id, booksy_id, scraped_at)

            reviews_count = self._insert_reviews(business, salon_id if salon_id > 0 else 0, scraped_at) \
                if salon_id > 0 else 0

            top_services_count = self._insert_top_services(business, salon_id if salon_id > 0 else 0, scraped_at) \
                if salon_id > 0 else 0
        except Exception as e:
            # Best-effort cleanup before re-raising so the retry on next
            # run isn't working against a half-committed state.
            if not self.dry_run and scrape_id != "dry-run-uuid":
                self._unwind_scrape(scrape_id)
            raise IngestError(f"failed during child inserts: {type(e).__name__}: {e}") from e

        counts = {
            "scrapes": 1,
            "services": services_count,
            "reviews": reviews_count,
            "top_services": top_services_count,
        }

        # 6. audit log (MUST be last so that if anything above failed, the
        # content_hash is NOT recorded and next run will retry this file)
        try:
            self._log_ingestion(
                file_name=file_path.name,
                content_hash=content_hash,
                booksy_id=booksy_id,
                salon_id=salon_id if salon_id > 0 else None,
                scrape_id=scrape_id if scrape_id != "dry-run-uuid" else None,
                rows_inserted=counts,
                file_size=file_size,
                file_mtime=file_mtime_iso,
            )
        except Exception as e:
            if not self.dry_run and scrape_id != "dry-run-uuid":
                self._unwind_scrape(scrape_id)
            raise IngestError(f"failed during audit log: {type(e).__name__}: {e}") from e

        return counts


# ---------------------------------------------------------------------------
# CLI driver
# ---------------------------------------------------------------------------

def _collect_files(source: Path, limit: int | None) -> list[Path]:
    files = sorted(p for p in source.iterdir() if p.suffix == ".json" and p.is_file())
    if limit is not None:
        files = files[:limit]
    return files


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-runnable JSON batch ingester for Booksy salon JSONs",
    )
    parser.add_argument("--source", required=True, help="Path to folder of JSON files")
    parser.add_argument("--batch-tag", default=None, help="Tag written to json_ingestion_log.source_batch")
    parser.add_argument("--batch-size", type=int, default=50, help="Progress log interval")
    parser.add_argument("--dry-run", action="store_true", help="Parse + skip but do not write")
    parser.add_argument("--limit", type=int, default=None, help="Max files to process (testing)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    source = Path(args.source).expanduser().resolve()
    if not source.is_dir():
        logger.error("source is not a directory: %s", source)
        return 2

    if not settings.supabase_url or not settings.supabase_service_key:
        logger.error("SUPABASE_URL or SUPABASE_SERVICE_KEY not set in environment/.env")
        return 2

    client: Client = make_supabase_client(
        settings.supabase_url,
        settings.supabase_service_key,
        headers={"ngrok-skip-browser-warning": "true"},
    )
    ingester = SalonJsonIngester(client, batch_tag=args.batch_tag, dry_run=args.dry_run)

    # -- Pre-flight: collect files, hash them all, skip list check ---------
    logger.info("Scanning %s ...", source)
    files = _collect_files(source, args.limit)
    logger.info("Found %d .json files", len(files))

    if not files:
        logger.warning("No .json files found")
        return 0

    logger.info("Hashing files ...")
    t0 = time.monotonic()
    hashes: list[tuple[Path, str]] = []
    for i, path in enumerate(files, start=1):
        try:
            hashes.append((path, _hash_file(path)))
        except OSError as e:
            logger.error("Failed to hash %s: %s", path.name, e)
        if i % 500 == 0:
            logger.info("  hashed %d/%d", i, len(files))
    logger.info("Hashing complete in %.1fs", time.monotonic() - t0)

    logger.info("Checking existing json_ingestion_log entries ...")
    already = ingester._hashes_already_ingested([h for _, h in hashes])
    logger.info("  %d files already ingested, %d new", len(already), len(hashes) - len(already))

    pending = [(p, h) for (p, h) in hashes if h not in already]

    # -- Per-file ingest with error tolerance ------------------------------
    total = len(hashes)
    processed = 0
    skipped = len(already)
    errors = 0
    error_examples: list[str] = []
    inserted_totals = {"scrapes": 0, "services": 0, "reviews": 0, "top_services": 0}

    abort_threshold = max(10, int(0.05 * total))

    t_start = time.monotonic()
    for idx, (path, content_hash) in enumerate(pending, start=1):
        try:
            counts = ingester.ingest_file(path, content_hash)
            for k, v in counts.items():
                inserted_totals[k] = inserted_totals.get(k, 0) + v
            processed += 1
        except IngestError as e:
            errors += 1
            msg = f"{path.name}: {e}"
            logger.error("Ingest error — %s", msg)
            if len(error_examples) < 10:
                error_examples.append(msg)
            if errors > abort_threshold:
                logger.error(
                    "Aborting — error count %d exceeds 5%% of processed files (%d)",
                    errors, abort_threshold,
                )
                break
        except Exception as e:  # noqa: BLE001 — log anything and continue
            errors += 1
            msg = f"{path.name}: UNEXPECTED {type(e).__name__}: {e}"
            logger.exception("Unexpected error on %s", path.name)
            if len(error_examples) < 10:
                error_examples.append(msg)
            if errors > abort_threshold:
                logger.error(
                    "Aborting — error count %d exceeds 5%% of processed files (%d)",
                    errors, abort_threshold,
                )
                break

        if idx % args.batch_size == 0:
            elapsed = time.monotonic() - t_start
            rate = idx / elapsed if elapsed else 0
            logger.info(
                "Processed %d/%d (skipped %d, errors %d) — %.1f files/s",
                idx, len(pending), skipped, errors, rate,
            )

    elapsed = time.monotonic() - t_start
    logger.info("=" * 60)
    logger.info("INGEST COMPLETE")
    logger.info("  scanned:        %d", total)
    logger.info("  new ingested:   %d", processed)
    logger.info("  already done:   %d", skipped)
    logger.info("  errors:         %d", errors)
    logger.info("  elapsed:        %.1fs (%.1f files/s)", elapsed, processed / elapsed if elapsed else 0)
    logger.info("  rows inserted:")
    logger.info("    salon_scrapes:         %d", inserted_totals["scrapes"])
    logger.info("    salon_scrape_services: %d", inserted_totals["services"])
    logger.info("    salon_reviews:         %d", inserted_totals["reviews"])
    logger.info("    salon_top_services:    %d", inserted_totals["top_services"])

    if error_examples:
        logger.info("  first errors (up to 10):")
        for e in error_examples:
            logger.info("    %s", e)

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
