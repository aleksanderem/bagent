"""Pydantic models for the bagent audit analyzer."""

from models.analysis import (
    DescriptionAnalysisResult,
    NamingAnalysisResult,
    QualityReport,
    StructureAnalysisResult,
)
from models.report import (
    AuditIssue,
    AuditStats,
    CompetitorContext,
    CompetitorInfo,
    EnhancedAuditReport,
    IndustryComparison,
    MissingSeoKeyword,
    QuickWin,
    ScoreBreakdown,
    SalonLocation,
    ServiceTransformation,
)
from models.scraped_data import (
    ScrapedCategory,
    ScrapedData,
    ScrapedService,
    ServiceVariant,
)

__all__ = [
    "AuditIssue",
    "AuditStats",
    "CompetitorContext",
    "CompetitorInfo",
    "DescriptionAnalysisResult",
    "EnhancedAuditReport",
    "IndustryComparison",
    "MissingSeoKeyword",
    "NamingAnalysisResult",
    "QualityReport",
    "QuickWin",
    "SalonLocation",
    "ScoreBreakdown",
    "ScrapedCategory",
    "ScrapedData",
    "ScrapedService",
    "ServiceTransformation",
    "ServiceVariant",
    "StructureAnalysisResult",
]
