"""Helpers that invoke each pipeline in-process by mocking the
SupabaseService reads each pipeline does for input data.

Why this approach (vs refactoring pipelines to accept inputs):
    Adding `_preloaded_inputs` kwargs to production pipelines is a
    risky change on a live system. These helpers achieve the same
    test-seam effect by monkey-patching SupabaseService's read methods
    to return synthetic fixtures, then calling the pipeline with its
    normal signature. Zero production-code change.

Each runner returns the pipeline's output dict + a captured writes log
(via the read_only_supabase fixture).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from .conftest import _ScrapedData


# ---------------------------------------------------------------------------
# Fake objects matching what SupabaseService methods return
# ---------------------------------------------------------------------------

class _FakeSupabaseReads:
    """Helper to install a set of canned return values on
    ``SupabaseService`` instance methods. Pipelines instantiate
    SupabaseService themselves; we patch the class so every instance
    sees the same fake data."""

    def __init__(
        self,
        scraped_data: _ScrapedData,
        audit_report: dict[str, Any] | None = None,
        competitor_salons: list[dict[str, Any]] | None = None,
        benchmarks: dict[str, Any] | None = None,
        optimized_pricelist: dict[str, Any] | None = None,
    ):
        self.scraped_data = scraped_data
        self.audit_report = audit_report or _default_audit_report(scraped_data)
        self.competitor_salons = competitor_salons or _default_competitors(scraped_data)
        self.benchmarks = benchmarks or {
            "industry_average": 52,
            "top_performers": 78,
            "sample_size": 500,
        }
        self.optimized_pricelist = optimized_pricelist or {
            "categoryProposals": [
                {"name": c.name, "services": [
                    {"name": s.name, "price": s.price, "duration": s.duration,
                     "description": s.description}
                    for s in c.services
                ]}
                for c in scraped_data.categories
            ]
        }

    def install(self, monkeypatch) -> None:
        """Monkey-patch SupabaseService methods to return our fakes."""
        from services.supabase import SupabaseService

        async def _async_scraped(*a, **kw):
            return self.scraped_data

        async def _async_report(*a, **kw):
            return self.audit_report

        async def _async_competitors(*a, **kw):
            return self.competitor_salons

        async def _async_benchmarks(*a, **kw):
            return self.benchmarks

        async def _async_optimized(*a, **kw):
            return self.optimized_pricelist

        # Map any plausibly-named getter to our fake. This is a
        # best-effort list — if a method doesn't exist on
        # SupabaseService it's silently skipped.
        method_to_fake = {
            "load_salon_scrape": _async_scraped,
            "get_salon_scrape": _async_scraped,
            "load_scrape": _async_scraped,
            "load_audit_report": _async_report,
            "get_audit_report": _async_report,
            "load_report": _async_report,
            "load_competitors": _async_competitors,
            "get_competitors": _async_competitors,
            "load_competitor_salons": _async_competitors,
            "get_benchmarks": _async_benchmarks,
            "load_benchmarks": _async_benchmarks,
            "load_optimized_pricelist": _async_optimized,
            "get_optimized_pricelist": _async_optimized,
        }

        for name, fake in method_to_fake.items():
            if hasattr(SupabaseService, name):
                monkeypatch.setattr(SupabaseService, name, fake)


def _default_audit_report(scraped_data: _ScrapedData) -> dict[str, Any]:
    """Minimal audit_report payload sufficient for cennik/summary to consume."""
    return {
        "version": "v2",
        "totalScore": 75,
        "scoreBreakdown": {"naming": 12, "descriptions": 10, "structure": 13, "pricing": 13, "seo": 12, "ux": 15},
        "stats": {
            "totalServices": scraped_data.totalServices,
            "totalCategories": len(scraped_data.categories),
            "servicesWithDescription": 0,
            "servicesWithDuration": scraped_data.totalServices,
            "duplicateNames": [],
            "emptyCategories": [],
            "oversizedCategories": [],
            "avgServicesPerCategory": (
                scraped_data.totalServices / max(len(scraped_data.categories), 1)
            ),
        },
        "topIssues": [
            {"issue": "Brak opisów usług", "severity": "major", "fix": "Dodaj opisy 2-3 zdania", "affectedCount": scraped_data.totalServices},
        ],
        "transformations": [],  # identity — no changes
        "missingSeoKeywords": [],
        "quickWins": [
            {"action": "Uzupełnij opisy", "effort": "low", "impact": "high", "example": "", "affectedServices": scraped_data.totalServices},
        ],
        "industryComparison": {
            "yourScore": 75, "industryAverage": 52, "topPerformers": 78,
            "percentile": 65, "sampleSize": 500,
        },
        "summary": "Twój salon jest powyżej średniej branżowej, ale opisy usług wymagają uzupełnienia.",
        "categoryMapping": {},  # identity
        "categoryChanges": [],
        "coverage": {
            "naming": {"totalChecked": scraped_data.totalServices, "optimized": 0, "alreadyOptimal": scraped_data.totalServices, "rejected": 0},
            "descriptions": {"totalChecked": scraped_data.totalServices, "optimized": 0, "alreadyOptimal": 0, "rejected": scraped_data.totalServices},
        },
    }


def _default_competitors(scraped_data: _ScrapedData) -> list[dict[str, Any]]:
    """Minimal competitor list — 3 fake nearby salons. Just enough for
    competitor_report pipeline to do something. Real test would pass
    real competitors via constructor."""
    return [
        {
            "id": f"comp-{i}",
            "salon_name": f"Konkurent {i}",
            "salon_address": scraped_data.salonAddress,
            "salon_city": scraped_data.salonCity,
            "primary_category_id": scraped_data.primaryCategoryId,
            "total_services": scraped_data.totalServices,
            "reviews_rank": 4.5 - i * 0.1,
            "reviews_count": 100 - i * 20,
        }
        for i in range(1, 4)
    ]


# ---------------------------------------------------------------------------
# Pipeline runners
# ---------------------------------------------------------------------------

async def run_audit(scraped_data: _ScrapedData) -> dict[str, Any]:
    """Audit pipeline takes scraped_data directly — no DB reads needed."""
    from pipelines.report import run_audit_pipeline
    return await run_audit_pipeline(
        scraped_data=scraped_data,
        audit_id=f"e2e-{getattr(scraped_data, 'salonId', 'unknown')}",
        on_progress=None,
    )


async def run_cennik(
    scraped_data: _ScrapedData,
    audit_report: dict[str, Any] | None = None,
    monkeypatch=None,
) -> dict[str, Any]:
    """Cennik takes audit_id and reads from DB. We mock the reads."""
    from pipelines.cennik import run_cennik_pipeline

    fakes = _FakeSupabaseReads(scraped_data, audit_report=audit_report)
    if monkeypatch is not None:
        fakes.install(monkeypatch)
    return await run_cennik_pipeline(
        audit_id=f"e2e-cennik-{getattr(scraped_data, 'salonId', 'unknown')}",
        on_progress=None,
    )


async def run_summary(
    scraped_data: _ScrapedData,
    audit_report: dict[str, Any] | None = None,
    selected_competitor_ids: list[int] | None = None,
    monkeypatch=None,
) -> dict[str, Any]:
    """Summary takes audit_id + user_id + competitor_ids."""
    from pipelines.summary import run_summary_pipeline

    fakes = _FakeSupabaseReads(scraped_data, audit_report=audit_report)
    if monkeypatch is not None:
        fakes.install(monkeypatch)
    return await run_summary_pipeline(
        audit_id=f"e2e-summary-{getattr(scraped_data, 'salonId', 'unknown')}",
        user_id="e2e-test-user",
        selected_competitor_ids=selected_competitor_ids or [],
        on_progress=None,
    )


async def run_competitor_report(
    scraped_data: _ScrapedData,
    competitors: list[dict[str, Any]] | None = None,
    monkeypatch=None,
) -> dict[str, Any]:
    """Competitor report — 9-step pipeline. Takes audit_id, tier,
    selection_mode, target_count, convex_user_id."""
    from pipelines.competitor_report import run_competitor_report_pipeline

    fakes = _FakeSupabaseReads(scraped_data, competitor_salons=competitors)
    if monkeypatch is not None:
        fakes.install(monkeypatch)
    return await run_competitor_report_pipeline(
        audit_id=f"e2e-comp-{getattr(scraped_data, 'salonId', 'unknown')}",
        tier="base",
        selection_mode="auto",
        target_count=5,
        convex_user_id="e2e-test-user",
        on_progress=None,
    )


async def run_versum_suggest(
    scraped_data: _ScrapedData,
) -> list[dict[str, Any]]:
    """Versum mapping — takes services + taxonomy, no audit_id needed.
    Builds a synthetic taxonomy that mirrors the scraped categories so
    the mapping has plausible candidates."""
    from pipelines.versum_suggest import suggest_versum_mappings

    services = []
    for c in scraped_data.categories:
        for s in c.services:
            services.append({
                "name": s.name,
                "description": s.description,
                "categoryName": c.name,
            })

    # Synthetic taxonomy — one entry per (category, service) so mapping
    # has at least one strong candidate per source.
    taxonomy = []
    seen_pairs = set()
    for tid, c in enumerate(scraped_data.categories, start=1):
        for sid, s in enumerate(c.services, start=tid * 100):
            key = (c.name, s.name)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            taxonomy.append({
                "treatmentId": sid,
                "treatmentName": s.name,
                "parentCategoryName": c.name,
                "occurrenceCount": 1,
            })

    suggestions = await suggest_versum_mappings(services=services, taxonomy=taxonomy)
    return {"suggestions": suggestions}
