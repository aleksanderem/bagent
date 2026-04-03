"""Pydantic models for the optimization pipeline."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class OptimizationRequest(BaseModel):
    auditId: str
    userId: str
    pricelistId: str
    jobId: str
    scrapedData: dict  # raw ScrapedData dict
    auditReport: dict  # EnhancedAuditReport as dict
    selectedOptions: list[str]  # ["descriptions", "seo", "categories", ...]
    promptTemplates: dict | None = None


class CategoryMapping(BaseModel):
    originalCategory: str
    newCategory: str
    services: list[str]
    reason: str


class OptimizedService(BaseModel):
    originalName: str
    categoryName: str
    newName: str
    newDescription: str | None = None
    tags: list[str] | None = None
    sortOrder: int | None = None


class ChangeRecord(BaseModel):
    type: Literal["name", "description", "category", "tag", "order", "dedup"]
    serviceName: str
    before: str
    after: str
    reason: str


class QualityVerification(BaseModel):
    fixed: list[str]
    remaining: list[str]
    qualityScore: int


class OptimizationSummary(BaseModel):
    totalChanges: int
    namesImproved: int
    descriptionsAdded: int
    duplicatesFound: int
    categoriesOptimized: int
    seoKeywordsAdded: int


class OptimizationResult(BaseModel):
    optimizedPricingData: dict  # full optimized pricelist
    changes: list[ChangeRecord]
    summary: OptimizationSummary
    recommendations: list[str]
    qualityScore: int
