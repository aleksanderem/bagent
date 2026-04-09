"""Pure deterministic computation of dimensional scores for a single salon.

Used by pipelines/competitor_analysis.py to compute ~30 comparison axes
grouped into 6 categories (content_quality, pricing, operations,
digital_maturity, social_proof, portfolio). Each function here takes
raw scrape data and returns a {dimension_name: float_value} dict —
completely pure, no IO, easily unit-testable.

Dimensions list comes from docs/plans/2026-04-08-competitor-report-pipeline.md
section "Dimensional scores — pełna lista ~20 wymiarów".

All functions tolerate missing/NULL/empty inputs gracefully and never
raise — a salon with no reviews produces review_velocity_30d=0, not an
error. Callers aggregate these into market distributions and percentiles
in the pipeline.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Metadata: dimensions in display order with their categories and units.
# ---------------------------------------------------------------------------

# (category, dimension, unit, better_is_higher)
# Order here determines sort_order in the output table — grouped by category
# so the UI can render category headers deterministically.
DIMENSION_METADATA: list[tuple[str, str, str, bool]] = [
    # content_quality
    ("content_quality", "description_coverage", "percent", True),
    ("content_quality", "photo_coverage", "percent", True),
    ("content_quality", "self_description_length", "count", True),
    ("content_quality", "avg_description_length", "count", True),
    # pricing
    ("pricing", "fixed_price_ratio", "percent", True),
    ("pricing", "price_range_spread", "zloty", False),
    ("pricing", "omnibus_price_compliance", "percent", True),
    ("pricing", "promo_intensity", "percent", False),
    # operations
    ("operations", "opening_hours_per_week", "hours", True),
    ("operations", "weekend_availability", "boolean", True),
    ("operations", "evening_availability", "boolean", True),
    ("operations", "booking_flexibility", "score", True),
    ("operations", "booking_lead_time_days", "days", True),
    ("operations", "deposit_required", "boolean", False),
    # digital_maturity
    ("digital_maturity", "has_online_services", "boolean", True),
    ("digital_maturity", "has_online_vouchers", "boolean", True),
    ("digital_maturity", "pos_pay_by_app", "boolean", True),
    ("digital_maturity", "digital_maturity_score", "count", True),
    # social_proof
    ("social_proof", "reviews_count", "count", True),
    ("social_proof", "reviews_rank", "score", True),
    ("social_proof", "review_velocity_30d", "count", True),
    ("social_proof", "owner_reply_rate", "percent", True),
    ("social_proof", "social_presence_count", "count", True),
    # portfolio
    ("portfolio", "total_services", "count", True),
    ("portfolio", "total_categories", "count", False),
    ("portfolio", "services_per_category", "count", True),
    ("portfolio", "combo_ratio", "percent", True),
    ("portfolio", "unique_treatment_count", "count", True),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _word_count(text: str | None) -> int:
    """Count whitespace-separated words in a string. None/empty → 0."""
    if not text:
        return 0
    return len([w for w in str(text).split() if w.strip()])


def _bool_to_float(value: Any) -> float:
    """Coerce a value to 1.0/0.0. None → 0.0, truthy → 1.0."""
    if value is None:
        return 0.0
    return 1.0 if bool(value) else 0.0


def _safe_ratio(numerator: float, denominator: float) -> float:
    """Return numerator/denominator as a float, or 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _pct(numerator: float, denominator: float) -> float:
    """Return 100 * numerator/denominator, or 0.0 if denominator is 0."""
    return 100.0 * _safe_ratio(numerator, denominator)


# ---------------------------------------------------------------------------
# content_quality
# ---------------------------------------------------------------------------


def compute_content_quality_scores(
    services: list[dict[str, Any]],
    salon_description: str | None,
) -> dict[str, float]:
    """Compute content quality metrics from active services + salon description.

    Dimensions:
    - description_coverage: % of active services with description_type='M' (manual)
    - photo_coverage: % of active services with at least one entry in photos jsonb
    - self_description_length: word count of salon.description
    - avg_description_length: avg word count per service.description (where set)
    """
    active = [s for s in services if s.get("is_active", True)]
    total = len(active)

    with_desc_m = sum(
        1 for s in active if (s.get("description_type") or "").upper() == "M"
    )
    with_photo = sum(
        1
        for s in active
        if isinstance(s.get("photos"), list) and len(s.get("photos") or []) > 0
    )

    svc_desc_lengths = [
        _word_count(s.get("description"))
        for s in active
        if s.get("description")
    ]

    return {
        "description_coverage": _pct(with_desc_m, total),
        "photo_coverage": _pct(with_photo, total),
        "self_description_length": float(_word_count(salon_description)),
        "avg_description_length": (
            float(sum(svc_desc_lengths)) / len(svc_desc_lengths)
            if svc_desc_lengths
            else 0.0
        ),
    }


# ---------------------------------------------------------------------------
# pricing
# ---------------------------------------------------------------------------


def compute_pricing_scores(services: list[dict[str, Any]]) -> dict[str, float]:
    """Compute pricing metrics from service rows.

    Dimensions:
    - fixed_price_ratio: % of services with is_from_price=false (fixed vs "from" pricing)
    - price_range_spread: max price - min price in zloty (lower = more predictable)
    - omnibus_price_compliance: % of services with omnibus_price_grosze set
    - promo_intensity: % of services with is_promo=true (too many = unstable)
    """
    active = [s for s in services if s.get("is_active", True)]
    total = len(active)

    fixed_count = sum(1 for s in active if s.get("is_from_price") is False)
    # Count prices for spread — only include services with price_grosze
    prices = [int(s["price_grosze"]) for s in active if s.get("price_grosze") is not None]
    spread_grosze = (max(prices) - min(prices)) if prices else 0
    spread_zloty = spread_grosze / 100.0

    with_omnibus = sum(
        1 for s in active if s.get("omnibus_price_grosze") is not None
    )
    with_promo = sum(1 for s in active if s.get("is_promo") is True)

    return {
        "fixed_price_ratio": _pct(fixed_count, total),
        "price_range_spread": spread_zloty,
        "omnibus_price_compliance": _pct(with_omnibus, total),
        "promo_intensity": _pct(with_promo, total),
    }


# ---------------------------------------------------------------------------
# operations
# ---------------------------------------------------------------------------


def compute_operations_scores(
    open_hours: list[dict[str, Any]],
    booking_max_modification_time: int | None,
    booking_max_lead_time: int | None,
    deposit_cancel_days: int | None,
) -> dict[str, float]:
    """Compute operational metrics.

    Dimensions:
    - opening_hours_per_week: sum of (open_till - open_from) across all days
    - weekend_availability: boolean — open Saturday (6) OR Sunday (7) in Booksy indexing
    - evening_availability: boolean — any day with open_till > 18:00
    - booking_flexibility: score 0-10, ideal is 120 min modification time
    - booking_lead_time_days: booking_max_lead_time seconds / 86400
    - deposit_required: boolean — deposit_cancel_days > 0
    """
    # open_hours is a list of {day_of_week, open_from, open_till} dicts.
    # day_of_week in Booksy is 1=Monday through 7=Sunday.
    total_hours = 0.0
    weekend_open = False
    evening_open = False

    for oh in open_hours or []:
        if not isinstance(oh, dict):
            continue
        open_from = oh.get("open_from")
        open_till = oh.get("open_till")
        day = oh.get("day_of_week")
        if not open_from or not open_till:
            continue
        try:
            from_hr, from_min = map(int, str(open_from).split(":"))
            till_hr, till_min = map(int, str(open_till).split(":"))
        except (ValueError, AttributeError):
            continue
        from_float = from_hr + from_min / 60.0
        till_float = till_hr + till_min / 60.0
        if till_float < from_float:
            # Crosses midnight — treat as till 24:00 for simplicity
            till_float = 24.0
        total_hours += max(0.0, till_float - from_float)
        # Weekend: day 6 (Saturday) or 7 (Sunday)
        if day in (6, 7):
            weekend_open = True
        # Evening: any day with close time after 18:00
        if till_float > 18.0:
            evening_open = True

    # Booking flexibility: ideal = 120 min, scale 0-10
    # Formula from plan: 10 - abs(120 - bmt) / 30
    # Clamped to [0, 10] and defaults to 0 when missing.
    if booking_max_modification_time is None:
        booking_flex = 0.0
    else:
        booking_flex = max(
            0.0,
            min(10.0, 10.0 - abs(120.0 - float(booking_max_modification_time)) / 30.0),
        )

    # Lead time in days
    lead_days = (
        float(booking_max_lead_time) / 86400.0
        if booking_max_lead_time is not None
        else 0.0
    )

    # Deposit required: deposit_cancel_days > 0 means deposit is taken
    deposit_req = (deposit_cancel_days or 0) > 0

    return {
        "opening_hours_per_week": total_hours,
        "weekend_availability": _bool_to_float(weekend_open),
        "evening_availability": _bool_to_float(evening_open),
        "booking_flexibility": booking_flex,
        "booking_lead_time_days": lead_days,
        "deposit_required": _bool_to_float(deposit_req),
    }


# ---------------------------------------------------------------------------
# digital_maturity
# ---------------------------------------------------------------------------


def compute_digital_maturity_scores(
    has_online_services: bool | None,
    has_online_vouchers: bool | None,
    pos_pay_by_app: bool | None,
) -> dict[str, float]:
    """Compute digital maturity flags + composite score.

    Dimensions:
    - has_online_services: boolean
    - has_online_vouchers: boolean
    - pos_pay_by_app: boolean
    - digital_maturity_score: sum of the three booleans above (0-3 integer)
    """
    o_serv = _bool_to_float(has_online_services)
    o_vouch = _bool_to_float(has_online_vouchers)
    pos_pay = _bool_to_float(pos_pay_by_app)
    return {
        "has_online_services": o_serv,
        "has_online_vouchers": o_vouch,
        "pos_pay_by_app": pos_pay,
        "digital_maturity_score": o_serv + o_vouch + pos_pay,
    }


# ---------------------------------------------------------------------------
# social_proof
# ---------------------------------------------------------------------------


def compute_social_proof_scores(
    reviews_count: int | None,
    reviews_rank: float | None,
    reviews: list[dict[str, Any]],
    facebook_url: str | None,
    instagram_url: str | None,
    website: str | None,
    now: datetime | None = None,
) -> dict[str, float]:
    """Compute social proof metrics.

    Dimensions:
    - reviews_count: total number of reviews from salons/salon_scrapes
    - reviews_rank: average star rating
    - review_velocity_30d: count of reviews where review_created_at > now - 30d
      (NOTE: limited to 3 sample per salon since only the business endpoint
      is used — this metric is expected to be low, mostly 0 or 1)
    - owner_reply_rate: % of reviews in sample with non-empty reply_content
    - social_presence_count: count of non-null values among facebook/instagram/website
    """
    now = now or datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    # review_velocity: count recent reviews in the SAMPLE we have
    recent = 0
    for r in reviews or []:
        if not isinstance(r, dict):
            continue
        created_raw = r.get("review_created_at") or r.get("created")
        if not created_raw:
            continue
        try:
            # Supabase timestamp string or ISO8601
            if isinstance(created_raw, str):
                # Handle 'Z' suffix and microseconds
                dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            elif isinstance(created_raw, datetime):
                dt = created_raw
            else:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt >= thirty_days_ago:
                recent += 1
        except (ValueError, TypeError):
            continue

    # owner_reply_rate: % of sample reviews with non-empty reply_content
    total_reviews_sample = len(reviews or [])
    with_reply = sum(
        1
        for r in (reviews or [])
        if isinstance(r, dict) and (r.get("reply_content") or "").strip()
    )

    social_count = 0
    if facebook_url and str(facebook_url).strip():
        social_count += 1
    if instagram_url and str(instagram_url).strip():
        social_count += 1
    if website and str(website).strip():
        social_count += 1

    return {
        "reviews_count": float(reviews_count or 0),
        "reviews_rank": float(reviews_rank) if reviews_rank is not None else 0.0,
        "review_velocity_30d": float(recent),
        "owner_reply_rate": _pct(with_reply, total_reviews_sample),
        "social_presence_count": float(social_count),
    }


# ---------------------------------------------------------------------------
# portfolio
# ---------------------------------------------------------------------------


def compute_portfolio_scores(services: list[dict[str, Any]]) -> dict[str, float]:
    """Compute portfolio metrics from service rows.

    Dimensions:
    - total_services: count of active services
    - total_categories: count of distinct category names (active services)
    - services_per_category: average services per category
    - combo_ratio: % of services with non-null combo_type
    - unique_treatment_count: count of distinct booksy_treatment_id values
    """
    active = [s for s in services if s.get("is_active", True)]
    total = len(active)

    categories: set[str] = set()
    for s in active:
        cat = s.get("category_name")
        if cat:
            categories.add(cat)
    n_cats = len(categories)

    combo_count = sum(1 for s in active if s.get("combo_type"))
    treatment_ids = {
        s.get("booksy_treatment_id")
        for s in active
        if s.get("booksy_treatment_id") is not None
    }

    return {
        "total_services": float(total),
        "total_categories": float(n_cats),
        "services_per_category": (
            _safe_ratio(total, n_cats) if n_cats > 0 else 0.0
        ),
        "combo_ratio": _pct(combo_count, total),
        "unique_treatment_count": float(len(treatment_ids)),
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def compute_all_dimensions_for_salon(salon_data: dict[str, Any]) -> dict[str, float]:
    """Compute all ~28 dimensional scores for a single salon.

    Input dict shape (produced by SupabaseService.get_subject_full_data or
    get_competitor_full_data):

        {
            "scrape": {
                "salon_description": str | None,
                "booking_max_modification_time": int | None,
                "booking_max_lead_time": int | None,
                "deposit_cancel_days": int | None,
                "pos_pay_by_app": bool | None,
                "has_online_services": bool | None,
                "has_online_vouchers": bool | None,
                "reviews_count": int | None,
                "reviews_rank": float | None,
                "facebook_url": str | None,
                "instagram_url": str | None,
                "website": str | None,
                "open_hours": list[dict],  # from raw_response.business.open_hours
            },
            "services": list[dict],  # salon_scrape_services rows
            "reviews": list[dict],   # salon_reviews rows
            # "top_services" is loaded but not used here (pricing uses them elsewhere)
        }

    Returns a flat {dimension_name: value} dict. Never raises — missing
    subkeys default to sensible zeros.
    """
    scrape = salon_data.get("scrape") or {}
    services = salon_data.get("services") or []
    reviews = salon_data.get("reviews") or []

    result: dict[str, float] = {}

    result.update(
        compute_content_quality_scores(
            services=services,
            salon_description=scrape.get("salon_description"),
        )
    )
    result.update(compute_pricing_scores(services=services))
    result.update(
        compute_operations_scores(
            open_hours=scrape.get("open_hours") or [],
            booking_max_modification_time=scrape.get("booking_max_modification_time"),
            booking_max_lead_time=scrape.get("booking_max_lead_time"),
            deposit_cancel_days=scrape.get("deposit_cancel_days"),
        )
    )
    result.update(
        compute_digital_maturity_scores(
            has_online_services=scrape.get("has_online_services"),
            has_online_vouchers=scrape.get("has_online_vouchers"),
            pos_pay_by_app=scrape.get("pos_pay_by_app"),
        )
    )
    result.update(
        compute_social_proof_scores(
            reviews_count=scrape.get("reviews_count"),
            reviews_rank=scrape.get("reviews_rank"),
            reviews=reviews,
            facebook_url=scrape.get("facebook_url"),
            instagram_url=scrape.get("instagram_url"),
            website=scrape.get("website"),
        )
    )
    result.update(compute_portfolio_scores(services=services))

    return result


# ---------------------------------------------------------------------------
# Market distribution helpers
# ---------------------------------------------------------------------------


def compute_percentiles(values: list[float]) -> dict[str, float]:
    """Compute min/p25/p50/p75/max from a list of numeric values.

    Uses linear interpolation between ranks (same as numpy default).
    Returns all zeros if the list is empty. Pure function, zero deps.
    """
    if not values:
        return {
            "market_min": 0.0,
            "market_p25": 0.0,
            "market_p50": 0.0,
            "market_p75": 0.0,
            "market_max": 0.0,
        }
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def _percentile(p: float) -> float:
        if n == 1:
            return sorted_vals[0]
        # Linear interpolation
        rank = p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac

    return {
        "market_min": float(sorted_vals[0]),
        "market_p25": _percentile(0.25),
        "market_p50": _percentile(0.50),
        "market_p75": _percentile(0.75),
        "market_max": float(sorted_vals[-1]),
    }


def compute_subject_percentile(subject_value: float, market_values: list[float]) -> float:
    """Return subject's percentile rank (0-100) within the market distribution.

    Formula: (count of values <= subject / total count) * 100.
    When two values tie at the subject, the subject scores where it lies,
    which is the standard "percentile_rank" definition. Empty market → 50.
    """
    if not market_values:
        return 50.0
    below_or_equal = sum(1 for v in market_values if v <= subject_value)
    return 100.0 * below_or_equal / len(market_values)
