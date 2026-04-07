"""Pydantic models for scraped salon data."""

from __future__ import annotations

from pydantic import BaseModel


class ServiceVariant(BaseModel):
    label: str
    price: str
    duration: str | None = None


class ScrapedService(BaseModel):
    name: str
    price: str
    duration: str | None = None
    description: str | None = None
    imageUrl: str | None = None
    variants: list[ServiceVariant] | None = None


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
