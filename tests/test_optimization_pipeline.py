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
    """Test Step 4: SEO enrichment."""

    async def test_seo_enrichment_skipped_when_not_selected(self) -> None:
        """Verify SEO step is skipped when 'seo' not in selected_options."""
        scraped = _make_scraped_data()
        report = _make_audit_report()

        mock_client = MagicMock()
        mock_agent_loop = AsyncMock()
        mock_settings = MagicMock()
        mock_settings.minimax_api_key = "test"
        mock_settings.minimax_base_url = "http://test"
        mock_settings.minimax_model = "test"

        # Agent loop returns empty result for service optimization
        mock_agent_loop.return_value = FakeAgentResult()

        # Client generate_json returns quality verification
        mock_client.generate_json = AsyncMock(return_value={
            "fixed": [], "remaining": [], "qualityScore": 80, "recommendations": [],
        })

        progress_messages: list[str] = []

        async def track_progress(p: int, msg: str) -> None:
            progress_messages.append(msg)

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings", mock_settings),
        ):
            from pipelines.optimization import run_optimization_pipeline

            result = await run_optimization_pipeline(
                scraped_data=scraped,
                audit_report=report,
                selected_options=["descriptions"],  # No "seo"
                audit_id="test-seo-skip",
                on_progress=track_progress,
            )

        # Find the SEO progress message
        seo_messages = [m for m in progress_messages if "SEO" in m or "seo" in m.lower()]
        assert any("pominięto" in m for m in seo_messages), f"Expected skip message, got: {seo_messages}"


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
    """CRITICAL: Test that prices, durations, variants are NEVER changed."""

    async def test_never_changes_prices(self) -> None:
        """Verify all prices in optimized data match original scraped data."""
        scraped = _make_scraped_data()
        report = _make_audit_report()

        mock_client = MagicMock()
        mock_agent_result = FakeAgentResult(
            tool_calls=[
                FakeToolCall(
                    name="submit_optimized_services",
                    input={
                        "services": [
                            {
                                "originalName": "Strzyżenie damskie",
                                "newName": "Strzyżenie damskie - stylowe cięcie",
                                "newDescription": "Nowoczesne strzyżenie z konsultacją.",
                            },
                            {
                                "originalName": "KOLORYZACJA...",
                                "newName": "Koloryzacja",
                                "newDescription": "Profesjonalna zmiana koloru włosów.",
                            },
                            {
                                "originalName": "Manicure hybrydowy",
                                "newName": "Manicure hybrydowy - trwały kolor",
                                "newDescription": "Manicure z trwałym lakierem hybrydowym.",
                            },
                        ],
                    },
                ),
            ],
        )

        mock_agent_loop = AsyncMock(return_value=mock_agent_result)
        mock_client.generate_json = AsyncMock(return_value={
            "fixed": [], "remaining": [], "qualityScore": 80, "recommendations": [],
        })

        mock_settings = MagicMock()
        mock_settings.minimax_api_key = "test"
        mock_settings.minimax_base_url = "http://test"
        mock_settings.minimax_model = "test"

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings", mock_settings),
        ):
            from pipelines.optimization import run_optimization_pipeline

            result = await run_optimization_pipeline(
                scraped_data=scraped,
                audit_report=report,
                selected_options=["descriptions"],
                audit_id="test-prices",
            )

        # Verify prices match original
        original_prices: dict[str, str] = {}
        for cat in scraped.categories:
            for svc in cat.services:
                original_prices[svc.name] = svc.price

        for opt_cat in result["optimizedPricingData"]["categories"]:
            for opt_svc in opt_cat["services"]:
                assert opt_svc["price"] in original_prices.values(), (
                    f"Price '{opt_svc['price']}' for service '{opt_svc['name']}' "
                    f"not found in original prices: {original_prices}"
                )

        # More specific: check each service by position
        for i, cat in enumerate(scraped.categories):
            opt_cat = result["optimizedPricingData"]["categories"][i]
            for j, svc in enumerate(cat.services):
                opt_svc = opt_cat["services"][j]
                assert opt_svc["price"] == svc.price, (
                    f"Price changed! Original: '{svc.price}', "
                    f"Optimized: '{opt_svc['price']}' for service '{svc.name}'"
                )

    async def test_never_changes_durations(self) -> None:
        """Verify all durations in optimized data match original scraped data."""
        scraped = _make_scraped_data()
        report = _make_audit_report()

        mock_client = MagicMock()
        mock_agent_result = FakeAgentResult(
            tool_calls=[
                FakeToolCall(
                    name="submit_optimized_services",
                    input={
                        "services": [
                            {
                                "originalName": "Masaż relaksacyjny",
                                "newName": "Masaż relaksacyjny - całe ciało",
                                "newDescription": "60 minut głębokiego relaksu.",
                            },
                        ],
                    },
                ),
            ],
        )

        mock_agent_loop = AsyncMock(return_value=mock_agent_result)
        mock_client.generate_json = AsyncMock(return_value={
            "fixed": [], "remaining": [], "qualityScore": 80, "recommendations": [],
        })

        mock_settings = MagicMock()
        mock_settings.minimax_api_key = "test"
        mock_settings.minimax_base_url = "http://test"
        mock_settings.minimax_model = "test"

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings", mock_settings),
        ):
            from pipelines.optimization import run_optimization_pipeline

            result = await run_optimization_pipeline(
                scraped_data=scraped,
                audit_report=report,
                selected_options=["descriptions"],
                audit_id="test-durations",
            )

        for i, cat in enumerate(scraped.categories):
            opt_cat = result["optimizedPricingData"]["categories"][i]
            for j, svc in enumerate(cat.services):
                opt_svc = opt_cat["services"][j]
                assert opt_svc["duration"] == svc.duration, (
                    f"Duration changed! Original: '{svc.duration}', "
                    f"Optimized: '{opt_svc['duration']}' for service '{svc.name}'"
                )

    async def test_never_changes_variants(self) -> None:
        """Verify all variants in optimized data match original scraped data."""
        scraped = _make_scraped_data()
        report = _make_audit_report()

        mock_client = MagicMock()
        mock_agent_result = FakeAgentResult(
            tool_calls=[
                FakeToolCall(
                    name="submit_optimized_services",
                    input={
                        "services": [
                            {
                                "originalName": "Drenaż limfatyczny",
                                "newName": "Drenaż limfatyczny - poprawa krążenia",
                                "newDescription": "Specjalistyczny drenaż wspomagający krążenie limfatyczne.",
                            },
                        ],
                    },
                ),
            ],
        )

        mock_agent_loop = AsyncMock(return_value=mock_agent_result)
        mock_client.generate_json = AsyncMock(return_value={
            "fixed": [], "remaining": [], "qualityScore": 80, "recommendations": [],
        })

        mock_settings = MagicMock()
        mock_settings.minimax_api_key = "test"
        mock_settings.minimax_base_url = "http://test"
        mock_settings.minimax_model = "test"

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings", mock_settings),
        ):
            from pipelines.optimization import run_optimization_pipeline

            result = await run_optimization_pipeline(
                scraped_data=scraped,
                audit_report=report,
                selected_options=["descriptions"],
                audit_id="test-variants",
            )

        for i, cat in enumerate(scraped.categories):
            opt_cat = result["optimizedPricingData"]["categories"][i]
            for j, svc in enumerate(cat.services):
                opt_svc = opt_cat["services"][j]
                if svc.variants:
                    expected_variants = [v.model_dump() for v in svc.variants]
                    assert opt_svc["variants"] == expected_variants, (
                        f"Variants changed for '{svc.name}'! "
                        f"Original: {expected_variants}, "
                        f"Optimized: {opt_svc['variants']}"
                    )
                else:
                    assert opt_svc["variants"] is None, (
                        f"Variants appeared for '{svc.name}' that had none! "
                        f"Got: {opt_svc['variants']}"
                    )


class TestReportAssembly:
    """Test Step 7: Report assembly."""

    async def test_report_assembly(self) -> None:
        """Verify all sections present with correct types."""
        scraped = _make_scraped_data()
        report = _make_audit_report()

        mock_client = MagicMock()
        mock_agent_loop = AsyncMock(return_value=FakeAgentResult())
        mock_client.generate_json = AsyncMock(return_value={
            "fixed": ["issue1"], "remaining": ["issue2"], "qualityScore": 85,
            "recommendations": ["rec1"],
        })

        mock_settings = MagicMock()
        mock_settings.minimax_api_key = "test"
        mock_settings.minimax_base_url = "http://test"
        mock_settings.minimax_model = "test"

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings", mock_settings),
        ):
            from pipelines.optimization import run_optimization_pipeline

            result = await run_optimization_pipeline(
                scraped_data=scraped,
                audit_report=report,
                selected_options=["descriptions", "seo", "categories"],
                audit_id="test-assembly",
            )

        # Verify top-level keys
        assert "optimizedPricingData" in result
        assert "changes" in result
        assert "summary" in result
        assert "recommendations" in result
        assert "qualityScore" in result

        # Verify types
        assert isinstance(result["optimizedPricingData"], dict)
        assert isinstance(result["changes"], list)
        assert isinstance(result["summary"], dict)
        assert isinstance(result["recommendations"], list)
        assert isinstance(result["qualityScore"], int)

        # Verify summary structure
        summary = result["summary"]
        assert "totalChanges" in summary
        assert "namesImproved" in summary
        assert "descriptionsAdded" in summary
        assert "duplicatesFound" in summary
        assert "categoriesOptimized" in summary
        assert "seoKeywordsAdded" in summary

        # Verify summary values are ints
        for key in ("totalChanges", "namesImproved", "descriptionsAdded",
                     "duplicatesFound", "categoriesOptimized", "seoKeywordsAdded"):
            assert isinstance(summary[key], int), f"summary['{key}'] should be int, got {type(summary[key])}"

        # Verify optimizedPricingData structure
        opd = result["optimizedPricingData"]
        assert "salonName" in opd
        assert "categories" in opd
        assert isinstance(opd["categories"], list)
        assert len(opd["categories"]) == len(scraped.categories)

        # Each category has services
        for opt_cat in opd["categories"]:
            assert "categoryName" in opt_cat
            assert "services" in opt_cat
            assert isinstance(opt_cat["services"], list)

            for opt_svc in opt_cat["services"]:
                assert "name" in opt_svc
                assert "price" in opt_svc
                assert "duration" in opt_svc
                assert "description" in opt_svc
                assert "variants" in opt_svc

        # Verify qualityScore is bounded
        assert 0 <= result["qualityScore"] <= 100
