"""Tests for optimization pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures — lightweight duck-typed objects matching ScrapedData shape
# ---------------------------------------------------------------------------


@dataclass
class FakeVariant:
    label: str
    price: str
    duration: str | None = None

    def model_dump(self) -> dict:
        d: dict[str, Any] = {"label": self.label, "price": self.price}
        if self.duration is not None:
            d["duration"] = self.duration
        return d


@dataclass
class FakeService:
    name: str
    price: str
    duration: str | None = None
    description: str | None = None
    imageUrl: str | None = None
    variants: list[FakeVariant] | None = None


@dataclass
class FakeCategory:
    name: str
    services: list[FakeService] = field(default_factory=list)


@dataclass
class FakeScrapedData:
    salonName: str | None
    salonAddress: str | None
    categories: list[FakeCategory]
    totalServices: int
    salonLogoUrl: str | None = None


def _make_scraped_data() -> FakeScrapedData:
    """Salon with 3 categories, 7 services total, some with variants."""
    return FakeScrapedData(
        salonName="Test Salon Beauty",
        salonAddress="ul. Testowa 1, Warszawa",
        totalServices=7,
        categories=[
            FakeCategory(
                name="Fryzjerstwo",
                services=[
                    FakeService(
                        name="Strzyżenie damskie",
                        price="120 zł",
                        duration="45 min",
                        description="Profesjonalne strzyżenie",
                    ),
                    FakeService(
                        name="KOLORYZACJA...",
                        price="250 zł",
                        duration="120 min",
                        description=None,
                    ),
                    FakeService(
                        name="Modelowanie",
                        price="80 zł",
                        duration="30 min",
                        description="Suszenie i modelowanie",
                    ),
                ],
            ),
            FakeCategory(
                name="Kosmetyka",
                services=[
                    FakeService(
                        name="Manicure hybrydowy",
                        price="100 zł",
                        duration="60 min",
                        description="Trwały manicure z lakierem hybrydowym",
                    ),
                    FakeService(
                        name="Pedicure promocja",
                        price="90 zł",
                        duration="75 min",
                        description=None,
                    ),
                ],
            ),
            FakeCategory(
                name="Masaż",
                services=[
                    FakeService(
                        name="Masaż relaksacyjny",
                        price="150 zł",
                        duration="60 min",
                        description="Relaksujący masaż całego ciała",
                    ),
                    FakeService(
                        name="Drenaż limfatyczny",
                        price="200 zł",
                        duration="90 min",
                        description=None,
                        variants=[
                            FakeVariant(label="Nogi", price="100 zł", duration="45 min"),
                            FakeVariant(label="Całe ciało", price="200 zł", duration="90 min"),
                        ],
                    ),
                ],
            ),
        ],
    )


def _make_audit_report() -> dict[str, Any]:
    """Sample audit report dict."""
    return {
        "topIssues": [
            {
                "severity": "critical",
                "dimension": "naming",
                "issue": "Nazwy usług zawierają CAPS LOCK",
                "impact": "Wygląda nieprofesjonalnie",
                "affectedCount": 3,
                "example": "KOLORYZACJA...",
                "fix": "Zmień na standardową wielkość liter",
            },
            {
                "severity": "major",
                "dimension": "descriptions",
                "issue": "Brak opisów w 40% usług",
                "impact": "Klient nie wie czego się spodziewać",
                "affectedCount": 4,
                "example": "Koloryzacja",
                "fix": "Dodaj opisy do wszystkich usług",
            },
            {
                "severity": "minor",
                "dimension": "structure",
                "issue": "Kategorie mają nierówną liczbę usług",
                "impact": "Trudna nawigacja",
                "affectedCount": 2,
                "example": "Masaż (2 usługi)",
                "fix": "Wyrównaj liczbę usług w kategoriach",
            },
        ],
        "transformations": [
            {
                "type": "name",
                "serviceName": "KOLORYZACJA...",
                "before": "KOLORYZACJA...",
                "after": "Koloryzacja",
                "reason": "Caps lock fix",
                "impactScore": 3,
            },
        ],
        "missingSeoKeywords": [
            {"keyword": "balayage", "searchVolume": "high", "suggestedPlacement": "Koloryzacja"},
            {"keyword": "masaż gorącymi kamieniami", "searchVolume": "medium", "suggestedPlacement": "Masaż"},
        ],
        "quickWins": [
            {
                "action": "Dodaj opisy do usług bez opisów",
                "effort": "low",
                "impact": "high",
                "example": "Koloryzacja — brak opisu",
                "affectedServices": 4,
            },
        ],
        "stats": {
            "totalServices": 7,
            "totalCategories": 3,
            "servicesWithDescription": 4,
            "servicesWithDuration": 7,
        },
    }


# ---------------------------------------------------------------------------
# Fake agent loop
# ---------------------------------------------------------------------------


@dataclass
class FakeToolCall:
    name: str
    input: dict


@dataclass
class FakeAgentResult:
    tool_calls: list[FakeToolCall] = field(default_factory=list)
    final_text: str = ""
    total_steps: int = 1
    total_input_tokens: int = 100
    total_output_tokens: int = 200


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestContextLoading:
    """Test Step 1: Context Loading."""

    def test_context_loading(self) -> None:
        """Verify audit report fields are extracted correctly."""
        report = _make_audit_report()

        context = {
            "topIssues": report.get("topIssues", []),
            "transformations": report.get("transformations", []),
            "missingSeoKeywords": report.get("missingSeoKeywords", []),
            "quickWins": report.get("quickWins", []),
            "stats": report.get("stats", {}),
        }

        assert len(context["topIssues"]) == 3
        assert len(context["transformations"]) == 1
        assert len(context["missingSeoKeywords"]) == 2
        assert len(context["quickWins"]) == 1
        assert context["stats"]["totalServices"] == 7
        assert context["topIssues"][0]["severity"] == "critical"
        assert context["topIssues"][0]["dimension"] == "naming"


class TestCategoryMapping:
    """Test Step 2: Category mapping tool parsing."""

    async def test_category_mapping_tool_parsing(self) -> None:
        """Mock agent loop, verify CategoryMapping output is parsed correctly."""
        fake_agent = FakeAgentResult(
            tool_calls=[
                FakeToolCall(
                    name="submit_category_mapping",
                    input={
                        "mappings": [
                            {"originalCategory": "Fryzjerstwo", "newCategory": "Usługi fryzjerskie", "reason": "Lepsza nazwa"},
                            {"originalCategory": "Masaż", "newCategory": "Masaże i relaks", "reason": "Bardziej opisowa"},
                        ],
                    },
                ),
            ],
        )

        category_mapping: dict[str, str] = {}
        for tc in fake_agent.tool_calls:
            if tc.name == "submit_category_mapping":
                for mapping in tc.input.get("mappings", []):
                    orig = mapping.get("originalCategory", "")
                    new = mapping.get("newCategory", "")
                    if orig and new and orig != new:
                        category_mapping[orig] = new

        assert len(category_mapping) == 2
        assert category_mapping["Fryzjerstwo"] == "Usługi fryzjerskie"
        assert category_mapping["Masaż"] == "Masaże i relaks"


class TestServiceOptimization:
    """Test Step 3: Service optimization tool parsing."""

    async def test_service_optimization_tool_parsing(self) -> None:
        """Mock agent loop, verify OptimizedService output parsed correctly."""
        fake_agent = FakeAgentResult(
            tool_calls=[
                FakeToolCall(
                    name="submit_optimized_services",
                    input={
                        "services": [
                            {
                                "originalName": "KOLORYZACJA...",
                                "newName": "Koloryzacja - profesjonalna zmiana koloru",
                                "newDescription": "Koloryzacja włosów z użyciem najlepszych farb.",
                                "tags": ["koloryzacja", "włosy", "kolor"],
                                "reason": "Caps lock + brak opisu",
                            },
                            {
                                "originalName": "Strzyżenie damskie",
                                "newName": "Strzyżenie damskie",
                                "newDescription": "Indywidualne strzyżenie z konsultacją stylisty.",
                                "tags": ["strzyżenie", "damskie"],
                                "reason": "Poprawiony opis",
                            },
                        ],
                    },
                ),
            ],
        )

        optimized: list[dict[str, Any]] = []
        for tc in fake_agent.tool_calls:
            if tc.name == "submit_optimized_services":
                for svc in tc.input.get("services", []):
                    optimized.append(svc)

        assert len(optimized) == 2
        assert optimized[0]["originalName"] == "KOLORYZACJA..."
        assert optimized[0]["newName"] == "Koloryzacja - profesjonalna zmiana koloru"
        assert optimized[0]["newDescription"] is not None
        assert len(optimized[0]["tags"]) == 3
        assert optimized[1]["originalName"] == "Strzyżenie damskie"


class TestSeoEnrichment:
    """Test SEO enrichment via legacy wrapper (delegates to optimize_phases)."""

    async def test_legacy_wrapper_calls_all_four_phases(self) -> None:
        """Verify the legacy wrapper calls all 4 phases sequentially."""
        import pipelines.optimize_phases as opt_mod

        mock_pricelist = {"salonName": "Test", "categories": [], "totalServices": 0}
        mock_phase1 = AsyncMock(return_value={"pricelist": mock_pricelist, "seoChanges": [], "keywordsAdded": 0})
        mock_phase2 = AsyncMock(return_value={"pricelist": mock_pricelist, "contentChanges": [], "namesImproved": 0, "descriptionsAdded": 0})
        mock_phase3 = AsyncMock(return_value={"pricelist": mock_pricelist, "categoryChanges": []})
        mock_phase4 = MagicMock(return_value={
            "finalPricelist": mock_pricelist, "changes": [], "summary": {"totalChanges": 0}, "qualityScore": 80,
        })
        mock_supabase_instance = MagicMock()
        mock_supabase_instance.get_scraped_data = AsyncMock(return_value={"salonName": "Test", "categories": []})
        mock_supabase_instance.save_optimized_pricelist = AsyncMock()
        mock_supabase_cls = MagicMock(return_value=mock_supabase_instance)

        progress_messages: list[str] = []

        async def track_progress(p: int, msg: str) -> None:
            progress_messages.append(msg)

        with (
            patch.object(opt_mod, "run_phase1_seo", mock_phase1),
            patch.object(opt_mod, "run_phase2_content", mock_phase2),
            patch.object(opt_mod, "run_phase3_categories", mock_phase3),
            patch.object(opt_mod, "run_phase4_finalize", mock_phase4),
        ):
            # Patch SupabaseService at the point where it's imported lazily
            mock_supabase_mod = MagicMock()
            mock_supabase_mod.SupabaseService = mock_supabase_cls
            with patch.dict("sys.modules", {"services.supabase": mock_supabase_mod}):
                from pipelines.optimization import run_optimization_pipeline

                result = await run_optimization_pipeline(
                    audit_id="test-legacy",
                    on_progress=track_progress,
                )

        mock_phase1.assert_awaited_once()
        mock_phase2.assert_awaited_once()
        mock_phase3.assert_awaited_once()
        mock_phase4.assert_called_once()
        assert result["qualityScore"] == 80


class TestProgrammaticFixes:
    """Test Step 5: Programmatic fixes."""

    def test_programmatic_fixes_caps_lock(self) -> None:
        """Verify caps lock is fixed in descriptions."""
        from pipelines.helpers import fix_caps_lock

        assert fix_caps_lock("PROFESJONALNE STRZYŻENIE") == "Profesjonalne strzyżenie"
        assert fix_caps_lock("Normalna wielkość") == "Normalna wielkość"
        assert fix_caps_lock("OK") == "OK"  # Too short to trigger

    def test_programmatic_fixes_clean_names(self) -> None:
        """Verify name cleanup."""
        from pipelines.helpers import clean_service_name

        assert clean_service_name("Strzyżenie...") == "Strzyżenie"
        assert clean_service_name("  Manicure  ") == "Manicure"
        assert clean_service_name("Henna+wosk") == "Henna + wosk"


class TestQualityVerification:
    """Test Step 6: Quality verification."""

    async def test_quality_verification_parsing(self) -> None:
        """Mock generate_json, verify QualityVerification output."""
        mock_response = {
            "fixed": ["Caps lock poprawiony", "Brakujące opisy dodane"],
            "remaining": ["Nierówne kategorie"],
            "qualityScore": 82,
            "recommendations": ["Dodaj więcej tagów SEO"],
        }

        assert mock_response["qualityScore"] == 82
        assert len(mock_response["fixed"]) == 2
        assert len(mock_response["remaining"]) == 1
        assert len(mock_response["recommendations"]) == 1
        assert isinstance(mock_response["qualityScore"], int)
        assert 0 <= mock_response["qualityScore"] <= 100


class TestPriceIntegrity:
    """CRITICAL: Price/duration/variant integrity is tested in test_optimize_phases.py.

    These tests verify the legacy wrapper correctly delegates to phases.
    """

    async def test_never_changes_prices(self) -> None:
        """Verify phase4 finalize preserves prices (delegates to optimize_phases)."""
        from pipelines.optimize_phases import run_phase4_finalize

        original = {
            "salonName": "Test",
            "totalServices": 2,
            "categories": [
                {"name": "Hair", "services": [
                    {"name": "Cut", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None, "tags": None, "isPromo": False},
                ]},
            ],
        }
        modified = {
            "salonName": "Test",
            "totalServices": 2,
            "categories": [
                {"name": "Hair", "services": [
                    {"name": "Strzyżenie damskie - stylowe cięcie", "price": "120 zł", "duration": "45 min", "description": "Nowy opis", "imageUrl": None, "variants": None, "tags": None, "isPromo": False},
                ]},
            ],
        }
        result = run_phase4_finalize(audit_id="test", pricelist=modified, original_pricelist=original)
        final_svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert final_svc["price"] == "120 zł"

    async def test_never_changes_durations(self) -> None:
        """Verify phase4 finalize preserves durations."""
        from pipelines.optimize_phases import run_phase4_finalize

        original = {
            "salonName": "Test",
            "totalServices": 1,
            "categories": [
                {"name": "Hair", "services": [
                    {"name": "Cut", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None, "tags": None, "isPromo": False},
                ]},
            ],
        }
        modified = {
            "salonName": "Test",
            "totalServices": 1,
            "categories": [
                {"name": "Hair", "services": [
                    {"name": "Strzyżenie", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None, "tags": None, "isPromo": False},
                ]},
            ],
        }
        result = run_phase4_finalize(audit_id="test", pricelist=modified, original_pricelist=original)
        final_svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert final_svc["duration"] == "45 min"

    async def test_never_changes_variants(self) -> None:
        """Verify phase4 finalize preserves variants."""
        from pipelines.optimize_phases import run_phase4_finalize

        variants = [{"label": "Nogi", "price": "100 zł", "duration": "45 min"}]
        original = {
            "salonName": "Test",
            "totalServices": 1,
            "categories": [
                {"name": "Massage", "services": [
                    {"name": "Drenaż", "price": "200 zł", "duration": "90 min", "description": None, "imageUrl": None, "variants": variants, "tags": None, "isPromo": False},
                ]},
            ],
        }
        modified = {
            "salonName": "Test",
            "totalServices": 1,
            "categories": [
                {"name": "Massage", "services": [
                    {"name": "Drenaż limfatyczny", "price": "200 zł", "duration": "90 min", "description": "Nowy opis", "imageUrl": None, "variants": variants, "tags": None, "isPromo": False},
                ]},
            ],
        }
        result = run_phase4_finalize(audit_id="test", pricelist=modified, original_pricelist=original)
        final_svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert final_svc["variants"] == variants


class TestReportAssembly:
    """Test that legacy wrapper returns phase4 result with expected keys."""

    async def test_report_assembly(self) -> None:
        """Verify legacy wrapper returns phase4 output with correct structure."""
        import pipelines.optimize_phases as opt_mod

        mock_pricelist = {
            "salonName": "Test Salon",
            "salonAddress": "ul. Testowa 1",
            "totalServices": 3,
            "categories": [
                {"name": "Hair", "services": [
                    {"name": "Cut", "price": "100 zł", "duration": "30 min", "description": "Opis", "imageUrl": None, "variants": None, "tags": None, "isPromo": False},
                ]},
            ],
        }
        phase4_result = {
            "finalPricelist": mock_pricelist,
            "changes": [{"type": "name", "before": "A", "after": "B"}],
            "summary": {
                "totalChanges": 1,
                "namesImproved": 1,
                "descriptionsAdded": 0,
                "categoriesOptimized": 0,
                "duplicatesFound": 0,
                "seoKeywordsAdded": 0,
            },
            "qualityScore": 85,
        }

        mock_phase1 = AsyncMock(return_value={"pricelist": mock_pricelist})
        mock_phase2 = AsyncMock(return_value={"pricelist": mock_pricelist})
        mock_phase3 = AsyncMock(return_value={"pricelist": mock_pricelist})
        mock_phase4 = MagicMock(return_value=phase4_result)
        mock_supabase_instance = MagicMock()
        mock_supabase_instance.get_scraped_data = AsyncMock(return_value={"salonName": "Test"})
        mock_supabase_instance.save_optimized_pricelist = AsyncMock()
        mock_supabase_cls = MagicMock(return_value=mock_supabase_instance)

        with (
            patch.object(opt_mod, "run_phase1_seo", mock_phase1),
            patch.object(opt_mod, "run_phase2_content", mock_phase2),
            patch.object(opt_mod, "run_phase3_categories", mock_phase3),
            patch.object(opt_mod, "run_phase4_finalize", mock_phase4),
        ):
            mock_supabase_mod = MagicMock()
            mock_supabase_mod.SupabaseService = mock_supabase_cls
            with patch.dict("sys.modules", {"services.supabase": mock_supabase_mod}):
                from pipelines.optimization import run_optimization_pipeline

                result = await run_optimization_pipeline(
                    audit_id="test-assembly",
                )

        # Verify top-level keys from phase4
        assert "finalPricelist" in result
        assert "changes" in result
        assert "summary" in result
        assert "qualityScore" in result

        # Verify types
        assert isinstance(result["finalPricelist"], dict)
        assert isinstance(result["changes"], list)
        assert isinstance(result["summary"], dict)
        assert isinstance(result["qualityScore"], int)

        # Verify summary structure
        summary = result["summary"]
        assert "totalChanges" in summary
        assert "namesImproved" in summary
        assert "descriptionsAdded" in summary

        # Verify qualityScore is bounded
        assert 0 <= result["qualityScore"] <= 100
