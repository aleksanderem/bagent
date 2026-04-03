"""Pydantic models for the enhanced audit report."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class ScoreBreakdown(BaseModel):
    completeness: int
    naming: int
    descriptions: int
    structure: int
    pricing: int
    seo: int
    ux: int


class AuditIssue(BaseModel):
    severity: Literal["critical", "major", "minor"]
    dimension: Literal["completeness", "naming", "descriptions", "structure", "pricing", "seo", "ux"]
    issue: str
    impact: str
    affectedCount: int
    example: str
    fix: str


class ServiceTransformation(BaseModel):
    type: Literal["name", "description", "category"]
    serviceName: str
    before: str
    after: str
    reason: str
    impactScore: int


class MissingSeoKeyword(BaseModel):
    keyword: str
    searchVolume: Literal["high", "medium", "low"]
    suggestedPlacement: str
    reason: str | None = None


class QuickWin(BaseModel):
    action: str
    effort: Literal["low", "medium", "high"]
    impact: Literal["high", "medium", "low"]
    example: str
    affectedServices: int


class IndustryComparison(BaseModel):
    yourScore: int
    industryAverage: int
    topPerformers: int
    percentile: int
    sampleSize: int | None = None
    scope: str | None = None


class CompetitorInfo(BaseModel):
    name: str
    lat: float
    lng: float
    distanceKm: float
    reviewsRank: int | None = None
    reviewsCount: int
    serviceCount: int
    pricingLevel: int | None = None
    booksyId: int
    thumbnailPhoto: str | None = None
    competitionScore: float
    city: str | None = None


class CompetitorContext(BaseModel):
    competitorsFound: int
    radiusKm: int
    pricePositioning: Literal["budget", "mid-range", "premium"]
    topServiceGaps: list[str]
    dataFreshness: str


class SalonLocation(BaseModel):
    lat: float
    lng: float


class AuditStats(BaseModel):
    totalServices: int
    totalVariants: int
    totalPricePoints: int
    servicesWithVariants: int
    totalCategories: int
    servicesWithDescription: int
    servicesWithDuration: int
    servicesWithFixedPrice: int
    servicesWithImage: int
    variantsWithDuration: int
    avgServicesPerCategory: float
    largestCategory: dict
    smallestCategory: dict
    duplicateNames: list[str]
    emptyCategories: list[str]
    oversizedCategories: list[str]
    undersizedCategories: list[str]


class EnhancedAuditReport(BaseModel):
    version: str = "v2"
    totalScore: int
    scoreBreakdown: ScoreBreakdown
    stats: AuditStats
    topIssues: list[AuditIssue]
    transformations: list[ServiceTransformation]
    missingSeoKeywords: list[MissingSeoKeyword]
    quickWins: list[QuickWin]
    industryComparison: IndustryComparison
    competitorContext: CompetitorContext | None = None
    salonLocation: SalonLocation | None = None
    competitors: list[CompetitorInfo] | None = None
    summary: str
