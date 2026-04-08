"""Pydantic models for scraped salon data."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ServiceVariant(BaseModel):
    label: str
    price: str
    duration: str | None = None


class ScrapedService(BaseModel):
    # Allow Booksy canonical taxonomy + scrape provenance fields to flow
    # through the pipeline without schema bloat. These extras originate in
    # the salon_scrape_services row and are needed by save_optimized_pricelist
    # so optimized_services can link back via source_scrape_service_id and
    # inherit Booksy taxonomy (canonical_id, body_part, etc.).
    model_config = ConfigDict(extra="allow")

    name: str
    price: str
    duration: str | None = None
    description: str | None = None
    imageUrl: str | None = None
    variants: list[ServiceVariant] | None = None

    # Optional provenance + canonical taxonomy (populated by
    # SupabaseService.get_scraped_data when reading from salon_scrape_services).
    scrape_service_id: int | None = None
    canonical_id: str | None = None
    booksy_treatment_id: int | None = None
    booksy_service_id: int | None = None
    treatment_name: str | None = None
    treatment_parent_id: int | None = None
    body_part: str | None = None
    target_gender: str | None = None
    technology: str | None = None
    classification_confidence: float | None = None
    price_grosze: int | None = None
    is_from_price: bool | None = None
    duration_minutes: int | None = None


class ScrapedCategory(BaseModel):
    # Empty name falls back to "Bez kategorii" — some Booksy salons don't
    # assign services to categories. Without this default the whole pipeline
    # fails on pydantic validation at step 0.
    name: str = "Bez kategorii"
    services: list[ScrapedService]


class ScrapedData(BaseModel):
    salonName: str | None = None
    salonAddress: str | None = None
    salonLogoUrl: str | None = None
    categories: list[ScrapedCategory]
    totalServices: int
    # Booksy primary category (e.g. 7 = Salon Kosmetyczny). Used by the report
    # pipeline to look up category-scoped benchmarks. Populated by
    # SupabaseService.get_scraped_data when reading from salon_scrapes.
    primaryCategoryId: int | None = None
