"""Intermediate analysis result models."""

from __future__ import annotations

from pydantic import BaseModel

from models.report import AuditIssue, MissingSeoKeyword, QuickWin, ServiceTransformation


class NamingAnalysisResult(BaseModel):
    score: int
    issues: list[AuditIssue]
    transformations: list[ServiceTransformation]


class DescriptionAnalysisResult(BaseModel):
    score: int
    issues: list[AuditIssue]
    transformations: list[ServiceTransformation]


class StructureAnalysisResult(BaseModel):
    structureScore: int
    pricingScore: int
    issues: list[AuditIssue]
    quickWins: list[QuickWin]
    missingSeoKeywords: list[MissingSeoKeyword]


class QualityReport(BaseModel):
    isAcceptable: bool
    score: int
    passedChecks: list[str]
    failedChecks: list[dict]
    refinementPrompt: str | None = None
