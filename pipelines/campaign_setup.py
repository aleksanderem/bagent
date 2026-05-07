"""Campaign setup pipeline — convert audit data into Meta ad config.

Input:  salon_id (Booksy ref) + optional audit_id
Output: CampaignConfig + 3-5 creative variants + UTM-tagged destination URL

Decisions automated from audit data:
    - Geo: salon_lat/lng from latest scrape, radius 10km (urban) or 25km (rural)
    - Demographic: derived from primary_category + reviews demographics
    - Interests: mapped from category + treatment_parent_id taxonomy
    - Budget: 50 PLN/day default for first week, scales per optimization rules
    - Creative: top 3 services by reviews × 3 style variants = 9 creatives total
    - Booksy URL: salon's booksy.com profile + UTM for attribution

Compliance:
    - Med-est / dental: special_ad_categories=['HEALTH'] (Meta requirement)
    - Beauty mass: no special category, full demographic targeting allowed

Critical: pipeline returns config, NOT activated campaign. Caller (operator
or auto-launch flow) reviews, then explicitly activates via update_status.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

from services.meta_ads import (
    CampaignConfig, CampaignObjective, DemographicTarget, GeoTarget,
    build_booksy_destination_url,
)
from services.openai_image import generate_ad_variants
from services.sb_client import make_supabase_client
from config import settings

logger = logging.getLogger("bagent.pipelines.campaign_setup")


# Categories requiring HEALTH special ad category (PL regulatory + Meta policy)
HEALTH_REGULATED_CATEGORIES = {
    "Medycyna Estetyczna", "Stomatolog", "Fizjoterapia",
    "Podologia", "Psychoterapia", "Zdrowie", "Medycyna Naturalna",
}

# Default demographic windows per category — empirically tuned from
# salon_reviews aggregations (most-active reviewer demographics for each
# vertical). Override per client after first 30 days based on actuals.
CATEGORY_DEMOGRAPHICS: dict[str, DemographicTarget] = {
    "Salon Kosmetyczny": DemographicTarget(age_min=25, age_max=55, genders=[2]),
    "Paznokcie": DemographicTarget(age_min=20, age_max=50, genders=[2]),
    "Fryzjer": DemographicTarget(age_min=22, age_max=60, genders=[1, 2]),
    "Brwi i rzęsy": DemographicTarget(age_min=20, age_max=45, genders=[2]),
    "Barber shop": DemographicTarget(age_min=20, age_max=55, genders=[1]),
    "Medycyna Estetyczna": DemographicTarget(age_min=28, age_max=55, genders=[2]),
    "Stomatolog": DemographicTarget(age_min=25, age_max=65, genders=[1, 2]),
    "Fizjoterapia": DemographicTarget(age_min=30, age_max=70, genders=[1, 2]),
    "Masaż": DemographicTarget(age_min=28, age_max=60, genders=[1, 2]),
    "Tatuaż i Piercing": DemographicTarget(age_min=18, age_max=40, genders=[1, 2]),
}

# Interest mapping per category — Meta requires named interests. These
# resolve to actual Meta interest IDs at campaign-create time via
# /search?type=adinterest (TODO: cache lookup)
CATEGORY_INTERESTS: dict[str, list[str]] = {
    "Salon Kosmetyczny": ["Beauty", "Cosmetics", "Skincare", "Self-care"],
    "Paznokcie": ["Manicure", "Nail art", "Beauty"],
    "Fryzjer": ["Hair styling", "Hair coloring", "Beauty"],
    "Brwi i rzęsy": ["Eyelash extensions", "Eyebrows", "Beauty"],
    "Barber shop": ["Men's grooming", "Beard care", "Barbershop"],
    "Medycyna Estetyczna": ["Aesthetic medicine", "Anti-aging", "Cosmetic surgery"],
    "Stomatolog": ["Dental care", "Oral health", "Cosmetic dentistry"],
    "Fizjoterapia": ["Physical therapy", "Sports medicine", "Pain management"],
    "Masaż": ["Massage therapy", "Wellness", "Spa"],
    "Tatuaż i Piercing": ["Tattoo art", "Body art", "Piercing"],
}


@dataclass
class CampaignProposal:
    """Output of campaign_setup — operator reviews this before activation."""
    salon_id: int
    salon_name: str
    salon_city: str
    primary_category: str
    objective: CampaignObjective
    config: CampaignConfig
    creatives: list[dict[str, Any]]   # [{service, headline, body, image_bytes, variant_idx}]
    destination_url: str
    requires_health_special_category: bool
    estimated_monthly_spend_pln: int
    notes: list[str]                  # warnings, rationale


async def setup_campaign_proposal(
    salon_ref_id: int,
    *,
    objective: CampaignObjective = "OUTCOME_LEADS",
    daily_budget_pln: int = 50,
    audit_id: str | None = None,
    n_creative_variants: int = 3,
) -> CampaignProposal:
    """Build a complete campaign proposal from a salon's Booksy data."""
    client = make_supabase_client(
        settings.supabase_url, settings.supabase_service_key,
    )

    # Pull latest scrape — has all the data we need (location, services, category)
    scrape_rows = (
        client.table("salon_scrapes")
        .select(
            "id, booksy_id, salon_name, salon_city, salon_address, "
            "salon_lat, salon_lng, primary_category_id, booksy_url, "
            "salon_logo_url"
        )
        .eq("salon_ref_id", salon_ref_id)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not scrape_rows:
        raise RuntimeError(f"No scrape found for salon_ref_id={salon_ref_id}")
    scrape = scrape_rows[0]

    cat_row = (
        client.table("booksy_categories")
        .select("name_pl")
        .eq("id", scrape.get("primary_category_id") or -1)
        .limit(1)
        .execute()
        .data
        or [{}]
    )
    primary_category = cat_row[0].get("name_pl", "Salon Kosmetyczny")

    # Top services by review count (within this salon, last 90 days)
    top_services = await _fetch_top_services(client, scrape_ref_id=salon_ref_id, scrape_id=scrape["id"])

    # Geographic targeting
    if not (scrape.get("salon_lat") and scrape.get("salon_lng")):
        raise RuntimeError(f"Salon {salon_ref_id} has no geo coordinates")
    is_urban = (scrape.get("salon_city") or "").lower() in {
        "warszawa", "kraków", "wrocław", "poznań", "gdańsk",
        "łódź", "szczecin", "katowice", "lublin", "bydgoszcz", "gdynia",
    }
    geo = GeoTarget(
        lat=float(scrape["salon_lat"]),
        lng=float(scrape["salon_lng"]),
        radius_km=10 if is_urban else 25,
        country="PL",
    )

    # Demographic + interest targeting
    demographic = CATEGORY_DEMOGRAPHICS.get(
        primary_category,
        DemographicTarget(age_min=25, age_max=55, genders=[1, 2]),
    )
    interests = CATEGORY_INTERESTS.get(primary_category, [])

    # Health regulatory check
    is_health = primary_category in HEALTH_REGULATED_CATEGORIES
    notes: list[str] = []
    if is_health:
        notes.append(
            f"Kategoria '{primary_category}' wymaga special_ad_category=HEALTH. "
            "Meta restricts targeting (no age/gender filtering, larger min radius). "
            "Demographic config zignorowany przy uruchomieniu."
        )

    # Build campaign config
    config = CampaignConfig(
        name=f"{scrape['salon_name']} — {primary_category} — {objective.replace('OUTCOME_', '')}",
        objective=objective,
        daily_budget_pln=daily_budget_pln,
        geo=geo,
        demographic=demographic,
        interests=interests,
    )

    # UTM-tagged destination
    booksy_url = scrape.get("booksy_url") or f"https://booksy.com/pl-pl/{scrape.get('booksy_id')}"
    destination_url = build_booksy_destination_url(
        booksy_url,
        campaign_id="pending",   # filled in after create_campaign
        adset_id="pending",
        audit_id=audit_id,
    )

    # Generate creative variants for top 3 services
    creatives: list[dict[str, Any]] = []
    for svc in top_services[:3]:
        try:
            variants = await generate_ad_variants(
                salon_name=scrape["salon_name"],
                service_name=svc["name"],
                salon_category=primary_category,
                price_pln=svc.get("price_pln"),
                n_variants=n_creative_variants,
            )
            for idx, image_bytes in enumerate(variants):
                creatives.append({
                    "service": svc["name"],
                    "variant_idx": idx,
                    "headline": _build_headline(svc, primary_category),
                    "body": _build_body(svc, scrape["salon_name"], primary_category),
                    "image_bytes": image_bytes,
                })
        except Exception as e:  # noqa: BLE001
            logger.warning("Creative gen failed for %s: %s", svc["name"], e)
            notes.append(f"Creative generation failed for service '{svc['name']}': {e}")

    if not creatives:
        notes.append("CRITICAL: zero creatives generated — campaign cannot launch")

    return CampaignProposal(
        salon_id=salon_ref_id,
        salon_name=scrape["salon_name"],
        salon_city=scrape.get("salon_city") or "",
        primary_category=primary_category,
        objective=objective,
        config=config,
        creatives=creatives,
        destination_url=destination_url,
        requires_health_special_category=is_health,
        estimated_monthly_spend_pln=daily_budget_pln * 30,
        notes=notes,
    )


async def _fetch_top_services(
    client: Any, *, scrape_ref_id: int, scrape_id: str,
) -> list[dict[str, Any]]:
    """Top services for a salon, ordered by review count.

    Falls back to top services by name length / non-empty description if
    review attribution data is sparse for new salons."""
    # Reviews-attributed top services (preferred — actual booking signal)
    review_top = (
        client.rpc(
            "fn_top_services_by_reviews",
            {"p_salon_id": scrape_ref_id, "p_days": 90, "p_limit": 5},
        )
        .execute()
        .data
        or []
    )
    if review_top:
        return [
            {
                "name": r.get("service_name"),
                "price_pln": (r.get("median_price_grosze") or 0) // 100 or None,
                "review_count": r.get("review_count"),
            }
            for r in review_top
        ]

    # Fallback: order by description length + price > 0 (cleaner-looking services first)
    rows = (
        client.table("salon_scrape_services")
        .select("name, price_grosze, description")
        .eq("scrape_id", scrape_id)
        .gt("price_grosze", 0)
        .not_.is_("description", "null")
        .limit(20)
        .execute()
        .data
        or []
    )
    rows.sort(key=lambda r: len(r.get("description") or ""), reverse=True)
    return [
        {
            "name": r["name"],
            "price_pln": (r.get("price_grosze") or 0) // 100 or None,
            "review_count": 0,
        }
        for r in rows[:5]
    ]


def _build_headline(service: dict[str, Any], category: str) -> str:
    """Single-line headline. Max 40 chars per Meta best-practice for IG feed."""
    name = service["name"]
    if len(name) > 35:
        name = name[:32] + "…"
    return name


def _build_body(
    service: dict[str, Any], salon_name: str, category: str,
) -> str:
    """Ad body copy. Polish, conversational, soft CTA. Compliance-safe
    (no claims like 'natychmiast', 'najtańsze', medical promises)."""
    price = service.get("price_pln")
    price_part = f" Już od {price} zł." if price else ""
    if category in HEALTH_REGULATED_CATEGORIES:
        # Compliance-safe medical copy — no efficacy claims
        return (
            f"{salon_name} – konsultacja i zabieg w komfortowych warunkach.{price_part} "
            f"Sprawdź szczegóły i zarezerwuj termin."
        )
    return (
        f"Zarezerwuj {service['name'].lower()} w {salon_name}.{price_part} "
        f"Komfortowy zabieg, profesjonalny zespół, łatwa rezerwacja online."
    )
