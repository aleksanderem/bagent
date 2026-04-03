"""Pydantic models for the competitor report pipeline."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CompetitorRequest(BaseModel):
    auditId: str
    userId: str
    subjectSalonId: int
    salonName: str
    salonCity: str
    salonLat: float | None = None
    salonLng: float | None = None
    selectedCompetitorIds: list[int]
    services: list[str]


class PriceCategoryComparison(BaseModel):
    categoryName: str
    salonAvgPrice: float | None = None
    marketAvgPrice: float | None = None
    positioning: str | None = None  # below_p25, p25_p50, p50_p75, above_p75


class LocalRanking(BaseModel):
    rank: int
    totalInCity: int
    city: str
    percentile: float


class ServiceGapEntry(BaseModel):
    serviceName: str
    salonHas: bool
    competitorCount: int


class CompetitorPromotion(BaseModel):
    competitorName: str
    serviceName: str
    originalPrice: float
    discountedPrice: float
    discountPercent: float


class PerServicePrice(BaseModel):
    serviceName: str
    salonPrice: float | None = None
    marketAvg: float | None = None
    marketMin: float | None = None
    marketMax: float | None = None
    percentilePosition: float | None = None


class MarketSummary(BaseModel):
    headline: str
    paragraphs: list[str]
    keyInsights: list[str]


class CompetitiveAdvantage(BaseModel):
    area: str
    type: Literal["win", "loss", "neutral"]
    description: str
    impact: Literal["high", "medium", "low"]


class Recommendation(BaseModel):
    title: str
    description: str
    estimatedImpactPln: float | None = None
    priority: int
    category: str


class RadarMetric(BaseModel):
    metric: str
    salonValue: float
    marketAvg: float


class SwotAnalysis(BaseModel):
    strengths: list[str]
    weaknesses: list[str]
    opportunities: list[str]
    threats: list[str]


class MarketNiche(BaseModel):
    name: str
    description: str
    opportunity: str


class ActionPlanItem(BaseModel):
    title: str
    description: str
    timeline: str
    priority: Literal["high", "medium", "low"]
    estimatedImpact: str


class CompetitorReportData(BaseModel):
    subjectSalonId: int
    salonName: str
    salonCity: str
    competitorIds: list[int]
    priceComparison: list[PriceCategoryComparison]
    localRanking: LocalRanking | None = None
    serviceGaps: list[ServiceGapEntry]
    promotions: list[CompetitorPromotion]
    perServicePricing: list[PerServicePrice]
    marketSummary: MarketSummary | None = None
    advantages: list[CompetitiveAdvantage]
    recommendations: list[Recommendation]
    radarMetrics: list[RadarMetric]
    swot: SwotAnalysis | None = None
    marketNiches: list[MarketNiche]
    actionPlan: list[ActionPlanItem]
