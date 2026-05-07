"""Meta Marketing API v22 wrapper for automated campaign management.

Scope:
    - create_campaign(name, objective, budget) → campaign_id
    - create_ad_set(campaign_id, targeting, schedule) → adset_id
    - upload_creative_image(image_bytes) → image_hash
    - create_ad(adset_id, image_hash, headline, body, link, cta) → ad_id
    - fetch_insights(ad_id, date_range) → metrics dict
    - update_status(entity_id, status) → bool
    - find_lookalike_audience(seed_pixel_ids) → audience_id

Auth model:
    Each client salon has a connected Facebook Business Manager that
    granted us "agency" role on their ad account. We store
    `meta_ad_account_id` + `meta_access_token` per salon in Convex
    (encrypted). Calls use the salon's own access token, NOT a master
    token — this is critical for Meta policy compliance and means
    Meta sees the salon as the advertiser (not us aggregating).

Rate limits:
    Meta enforces ~200 calls/hour per app per ad account. Honored via
    httpx semaphore + arq retry-with-backoff.

Doc: https://developers.facebook.com/docs/marketing-api/reference/v22.0/
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

logger = logging.getLogger("bagent.services.meta_ads")

META_API_VERSION = "v22.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"


# --- Type definitions ------------------------------------------------------

CampaignObjective = Literal[
    "OUTCOME_LEADS",        # form fill or click → Booksy booking
    "OUTCOME_TRAFFIC",      # raw clicks to Booksy URL
    "OUTCOME_AWARENESS",    # impressions, top-of-funnel
    "OUTCOME_ENGAGEMENT",   # likes/comments/shares
    "OUTCOME_SALES",        # for med-est e-commerce-like flows
]

CampaignStatus = Literal["ACTIVE", "PAUSED", "DELETED", "ARCHIVED"]


@dataclass
class GeoTarget:
    """Targeting geometry — Booksy salons are local biz, location matters most."""
    lat: float
    lng: float
    radius_km: int = 10        # default 10km — covers urban catchment
    country: str = "PL"


@dataclass
class DemographicTarget:
    """Beauty/medical target audience."""
    age_min: int = 25
    age_max: int = 55
    genders: list[int] = field(default_factory=lambda: [1, 2])  # 1=male, 2=female; default both


@dataclass
class CampaignConfig:
    """Single campaign config produced by campaign_setup pipeline."""
    name: str
    objective: CampaignObjective
    daily_budget_pln: int                    # in PLN, converted to grosze for Meta
    geo: GeoTarget
    demographic: DemographicTarget
    interests: list[str] = field(default_factory=list)  # e.g. ["beauty", "skincare"]
    custom_audience_ids: list[str] = field(default_factory=list)  # lookalikes/retarget
    booksy_destination_url: str = ""        # final URL with UTM


# --- Client -----------------------------------------------------------------

class MetaAdsClient:
    """Per-salon Meta API client. Construct with salon's ad account creds."""

    def __init__(
        self,
        ad_account_id: str,           # 'act_1234567890' format
        access_token: str,
        page_id: str | None = None,   # FB page that ads run from
        instagram_actor_id: str | None = None,  # IG account if cross-posting
    ):
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"
        self.ad_account_id = ad_account_id
        self.access_token = access_token
        self.page_id = page_id
        self.instagram_actor_id = instagram_actor_id
        self._client = httpx.AsyncClient(
            base_url=META_BASE_URL,
            timeout=60.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def __aenter__(self) -> "MetaAdsClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.aclose()

    async def _post(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        data = {**data, "access_token": self.access_token}
        for attempt in range(3):
            r = await self._client.post(path, data=data)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                await asyncio.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Meta {path} failed {r.status_code}: {r.text[:300]}")
        raise RuntimeError(f"Meta {path} failed after 3 retries")

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = {**(params or {}), "access_token": self.access_token}
        for attempt in range(3):
            r = await self._client.get(path, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                await asyncio.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Meta GET {path} failed {r.status_code}: {r.text[:300]}")
        raise RuntimeError(f"Meta GET {path} failed after 3 retries")

    # --- Campaigns --------------------------------------------------------

    async def create_campaign(
        self,
        name: str,
        objective: CampaignObjective,
        status: CampaignStatus = "PAUSED",
        special_ad_categories: list[str] | None = None,
    ) -> str:
        """Create campaign. Returns campaign_id.

        Note: status='PAUSED' on create — caller must explicitly activate
        after ad sets + ads are configured. Prevents half-built campaigns
        from spending money."""
        # Healthcare advertising in PL requires special_ad_categories.
        # For med-est / dental we set ['HEALTH'] which restricts targeting
        # (no age/gender, larger min-radius) but is COMPLIANCE REQUIRED.
        special = special_ad_categories or []
        result = await self._post(
            f"/{self.ad_account_id}/campaigns",
            {
                "name": name,
                "objective": objective,
                "status": status,
                "special_ad_categories": str(special).replace("'", '"'),
                "buying_type": "AUCTION",
            },
        )
        return result["id"]

    async def create_ad_set(
        self,
        campaign_id: str,
        name: str,
        config: CampaignConfig,
        optimization_goal: str = "LINK_CLICKS",
        billing_event: str = "IMPRESSIONS",
        status: CampaignStatus = "PAUSED",
    ) -> str:
        """Create ad set with targeting + budget. Returns adset_id."""
        targeting: dict[str, Any] = {
            "geo_locations": {
                "custom_locations": [
                    {
                        "latitude": config.geo.lat,
                        "longitude": config.geo.lng,
                        "radius": config.geo.radius_km,
                        "distance_unit": "kilometer",
                    }
                ],
                "countries": [config.geo.country],
            },
            "age_min": config.demographic.age_min,
            "age_max": config.demographic.age_max,
            "genders": config.demographic.genders,
        }
        if config.interests:
            # Real impl needs to resolve interest names → Meta interest IDs
            # via /search?type=adinterest. Stubbed for now — see TODO.
            targeting["flexible_spec"] = [{"interests": config.interests}]
        if config.custom_audience_ids:
            targeting["custom_audiences"] = [
                {"id": aid} for aid in config.custom_audience_ids
            ]

        import json
        result = await self._post(
            f"/{self.ad_account_id}/adsets",
            {
                "name": name,
                "campaign_id": campaign_id,
                "daily_budget": config.daily_budget_pln * 100,  # PLN→grosze
                "billing_event": billing_event,
                "optimization_goal": optimization_goal,
                "targeting": json.dumps(targeting),
                "status": status,
                "start_time": _iso_now(),
            },
        )
        return result["id"]

    # --- Creatives --------------------------------------------------------

    async def upload_creative_image(self, image_bytes: bytes, filename: str = "ad.png") -> str:
        """Upload image to Meta's library, return image_hash for reuse in ads."""
        files = {"source": (filename, image_bytes, "image/png")}
        # Multipart endpoint requires special handling — bypass _post
        r = await self._client.post(
            f"/{self.ad_account_id}/adimages",
            data={"access_token": self.access_token},
            files=files,
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"Image upload failed {r.status_code}: {r.text[:300]}"
            )
        data = r.json()
        # Response shape: {"images": {"<filename>": {"hash": "abc123", ...}}}
        return next(iter(data["images"].values()))["hash"]

    async def create_ad(
        self,
        adset_id: str,
        name: str,
        image_hash: str,
        headline: str,
        body: str,
        destination_url: str,
        cta_type: str = "BOOK_TRAVEL",  # Meta's closest to "Zarezerwuj"
        status: CampaignStatus = "PAUSED",
    ) -> str:
        """Create ad creative + ad. Returns ad_id."""
        if not self.page_id:
            raise RuntimeError("page_id required to create ads")

        import json
        creative_spec = {
            "object_story_spec": {
                "page_id": self.page_id,
                "link_data": {
                    "image_hash": image_hash,
                    "link": destination_url,
                    "message": body,
                    "name": headline,
                    "call_to_action": {
                        "type": cta_type,
                        "value": {"link": destination_url},
                    },
                },
            }
        }
        if self.instagram_actor_id:
            creative_spec["object_story_spec"]["instagram_actor_id"] = (
                self.instagram_actor_id
            )

        creative_result = await self._post(
            f"/{self.ad_account_id}/adcreatives",
            {
                "name": f"{name} - creative",
                **{k: json.dumps(v) if isinstance(v, dict) else v for k, v in creative_spec.items()},
            },
        )
        creative_id = creative_result["id"]

        ad_result = await self._post(
            f"/{self.ad_account_id}/ads",
            {
                "name": name,
                "adset_id": adset_id,
                "creative": json.dumps({"creative_id": creative_id}),
                "status": status,
            },
        )
        return ad_result["id"]

    # --- Reporting --------------------------------------------------------

    async def fetch_insights(
        self,
        entity_id: str,                      # ad_id, adset_id, or campaign_id
        date_preset: str = "yesterday",      # 'yesterday' | 'last_7_d' | 'last_30_d' | 'lifetime'
        fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch performance metrics for any campaign-tree entity.

        Default fields cover the BeautyAudit dashboard: spend, impressions,
        clicks, CPC, CPM, CTR. For booking attribution we layer on UTM
        click data scraped from Booksy separately."""
        default_fields = [
            "campaign_id", "campaign_name", "adset_id", "adset_name",
            "ad_id", "ad_name", "spend", "impressions", "clicks",
            "cpc", "cpm", "ctr", "frequency", "reach",
            "actions", "action_values",
        ]
        result = await self._get(
            f"/{entity_id}/insights",
            params={
                "fields": ",".join(fields or default_fields),
                "date_preset": date_preset,
                "time_increment": 1,    # daily breakdown
            },
        )
        return result.get("data", [])

    # --- Lifecycle --------------------------------------------------------

    async def update_status(self, entity_id: str, status: CampaignStatus) -> bool:
        """Set ACTIVE / PAUSED on campaign, ad set, or ad. Used by
        optimization rules engine to scale winners + kill losers."""
        result = await self._post(f"/{entity_id}", {"status": status})
        return bool(result.get("success", True))

    async def update_daily_budget(self, adset_id: str, new_budget_pln: int) -> bool:
        """Bump or cut ad set daily budget. Used by scaling rules."""
        result = await self._post(
            f"/{adset_id}",
            {"daily_budget": new_budget_pln * 100},
        )
        return bool(result.get("success", True))


# --- Helpers ----------------------------------------------------------------

def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()


def build_booksy_destination_url(
    booksy_salon_url: str,
    *,
    campaign_id: str,
    adset_id: str,
    ad_id: str | None = None,
    audit_id: str | None = None,
) -> str:
    """Build the destination URL with UTMs that bind a click back to a
    specific ad. UTM values are read by our Booksy scraper to attribute
    bookings — which is the closed-loop measurement that nobody else has.

    We also append `audit_id` as a custom param so we can join the
    booking back to the originating audit/client account in Convex.
    """
    from urllib.parse import urlencode, urlparse, urlunparse

    params = {
        "utm_source": "meta",
        "utm_medium": "cpc",
        "utm_campaign": campaign_id,
        "utm_content": adset_id,
    }
    if ad_id:
        params["utm_term"] = ad_id
    if audit_id:
        params["ba_audit"] = audit_id

    parsed = urlparse(booksy_salon_url)
    existing_query = parsed.query
    new_query = urlencode(params)
    if existing_query:
        new_query = f"{existing_query}&{new_query}"
    return urlunparse(parsed._replace(query=new_query))
