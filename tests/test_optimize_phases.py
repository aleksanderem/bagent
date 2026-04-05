"""Tests for run_phase4_finalize — deterministic, no AI mocks needed."""

from __future__ import annotations

from typing import Any

import pytest

from pipelines.optimize_phases import run_phase4_finalize


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_pricelist(**overrides: Any) -> dict[str, Any]:
    """Build a base pricelist dict for testing."""
    base: dict[str, Any] = {
        "salonName": "Test Salon",
        "salonAddress": "ul. Testowa 1, Warszawa",
        "salonLogoUrl": None,
        "totalServices": 5,
        "categories": [
            {
                "name": "Fryzjerstwo",
                "services": [
                    {
                        "name": "Strzyżenie damskie",
                        "price": "120 zł",
                        "duration": "45 min",
                        "description": "Profesjonalne strzyżenie",
                        "imageUrl": None,
                        "variants": None,
                        "tags": None,
                        "isPromo": False,
                    },
                    {
                        "name": "Koloryzacja",
                        "price": "250 zł",
                        "duration": "120 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                        "tags": None,
                        "isPromo": False,
                    },
                ],
            },
            {
                "name": "Kosmetyka",
                "services": [
                    {
                        "name": "Manicure hybrydowy",
                        "price": "100 zł",
                        "duration": "60 min",
                        "description": "Trwały manicure z lakierem hybrydowym",
                        "imageUrl": None,
                        "variants": None,
                        "tags": None,
                        "isPromo": False,
                    },
                    {
                        "name": "Pedicure",
                        "price": "120 zł",
                        "duration": "75 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                        "tags": None,
                        "isPromo": False,
                    },
                    {
                        "name": "Masaż relaksacyjny",
                        "price": "150 zł",
                        "duration": "60 min",
                        "description": "Relaksujący masaż całego ciała",
                        "imageUrl": None,
                        "variants": [
                            {"label": "30 min", "price": "80 zł", "duration": "30 min"},
                            {"label": "60 min", "price": "150 zł", "duration": "60 min"},
                        ],
                        "tags": None,
                        "isPromo": False,
                    },
                ],
            },
        ],
    }
    for key, val in overrides.items():
        base[key] = val
    return base


def _deep_copy_pricelist(pl: dict[str, Any]) -> dict[str, Any]:
    """Deep copy a pricelist dict (avoids import json for simple structure)."""
    import copy
    return copy.deepcopy(pl)


# ---------------------------------------------------------------------------
# Tests: No changes -> empty diff
# ---------------------------------------------------------------------------


class TestNoChanges:
    """When pricelist and original are identical, diff should be empty."""

    def test_no_changes_empty_diff(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-no-changes", pricelist, original)

        assert result["changes"] == []
        assert result["summary"]["totalChanges"] == 0
        assert result["summary"]["namesImproved"] == 0
        assert result["summary"]["descriptionsAdded"] == 0
        assert result["summary"]["categoriesOptimized"] == 0
        assert result["qualityScore"] == 0

    def test_no_changes_preserves_structure(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-structure", pricelist, original)

        final = result["finalPricelist"]
        assert final["salonName"] == "Test Salon"
        assert len(final["categories"]) == 2
        assert len(final["categories"][0]["services"]) == 2
        assert len(final["categories"][1]["services"]) == 3

    def test_preserves_prices_and_durations(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-prices", pricelist, original)

        for cat_idx, cat in enumerate(original["categories"]):
            final_cat = result["finalPricelist"]["categories"][cat_idx]
            for svc_idx, svc in enumerate(cat["services"]):
                final_svc = final_cat["services"][svc_idx]
                assert final_svc["price"] == svc["price"]
                assert final_svc["duration"] == svc["duration"]

    def test_preserves_variants(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-variants", pricelist, original)

        # Service at index [1][2] has variants
        final_svc = result["finalPricelist"]["categories"][1]["services"][2]
        orig_svc = original["categories"][1]["services"][2]
        assert final_svc["variants"] == orig_svc["variants"]


# ---------------------------------------------------------------------------
# Tests: Name change detection
# ---------------------------------------------------------------------------


class TestNameChangeDetection:
    """When service names differ between pricelist and original, detect as change."""

    def test_name_change_detected(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie damskie - stylowe cięcie"

        result = run_phase4_finalize("test-name-change", pricelist, original)

        assert result["summary"]["namesImproved"] == 1
        name_changes = [c for c in result["changes"] if c["type"] == "name"]
        assert len(name_changes) == 1
        assert name_changes[0]["before"] == "Strzyżenie damskie"
        assert name_changes[0]["after"] == "Strzyżenie damskie - stylowe cięcie"

    def test_multiple_name_changes(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie damskie - balayage"
        pricelist["categories"][0]["services"][1]["name"] = "Koloryzacja - profesjonalna zmiana koloru"

        result = run_phase4_finalize("test-multi-name", pricelist, original)

        assert result["summary"]["namesImproved"] == 2
        name_changes = [c for c in result["changes"] if c["type"] == "name"]
        assert len(name_changes) == 2


# ---------------------------------------------------------------------------
# Tests: Description change detection
# ---------------------------------------------------------------------------


class TestDescriptionChangeDetection:
    """When descriptions differ between pricelist and original, detect as change."""

    def test_description_added(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        # Koloryzacja originally has no description
        pricelist["categories"][0]["services"][1]["description"] = "Profesjonalna zmiana koloru włosów"

        result = run_phase4_finalize("test-desc-add", pricelist, original)

        assert result["summary"]["descriptionsAdded"] == 1
        desc_changes = [c for c in result["changes"] if c["type"] == "description"]
        assert len(desc_changes) == 1
        assert desc_changes[0]["before"] == ""
        assert desc_changes[0]["after"] == "Profesjonalna zmiana koloru włosów"

    def test_description_modified(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["description"] = "Nowoczesne strzyżenie z konsultacją"

        result = run_phase4_finalize("test-desc-mod", pricelist, original)

        assert result["summary"]["descriptionsAdded"] == 1
        desc_changes = [c for c in result["changes"] if c["type"] == "description"]
        assert len(desc_changes) == 1
        assert desc_changes[0]["before"] == "Profesjonalne strzyżenie"
        assert desc_changes[0]["after"] == "Nowoczesne strzyżenie z konsultacją"


# ---------------------------------------------------------------------------
# Tests: Category name change detection
# ---------------------------------------------------------------------------


class TestCategoryChangeDetection:
    """When category names differ between pricelist and original, detect as change."""

    def test_category_name_change(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["name"] = "Usługi fryzjerskie"

        result = run_phase4_finalize("test-cat-change", pricelist, original)

        assert result["summary"]["categoriesOptimized"] == 1
        cat_changes = [c for c in result["changes"] if c["type"] == "category"]
        assert len(cat_changes) == 1
        assert cat_changes[0]["before"] == "Fryzjerstwo"
        assert cat_changes[0]["after"] == "Usługi fryzjerskie"

    def test_multiple_category_changes(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["name"] = "Usługi fryzjerskie"
        pricelist["categories"][1]["name"] = "Zabiegi kosmetyczne"

        result = run_phase4_finalize("test-multi-cat", pricelist, original)

        assert result["summary"]["categoriesOptimized"] == 2


# ---------------------------------------------------------------------------
# Tests: CAPS lock fix
# ---------------------------------------------------------------------------


class TestCapsLockFix:
    """Verify CAPS LOCK text is fixed to sentence case."""

    def test_caps_lock_name_fixed(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "STRZYŻENIE DAMSKIE"
        # Original also needs the CAPS name for proper diff
        original["categories"][0]["services"][0]["name"] = "STRZYŻENIE DAMSKIE"

        result = run_phase4_finalize("test-caps", pricelist, original)

        # The fix is applied to the final pricelist
        final_name = result["finalPricelist"]["categories"][0]["services"][0]["name"]
        assert final_name == "Strzyżenie damskie"

    def test_caps_lock_description_fixed(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["description"] = "PROFESJONALNE STRZYŻENIE Z KONSULTACJĄ"
        original["categories"][0]["services"][0]["description"] = "PROFESJONALNE STRZYŻENIE Z KONSULTACJĄ"

        result = run_phase4_finalize("test-caps-desc", pricelist, original)

        final_desc = result["finalPricelist"]["categories"][0]["services"][0]["description"]
        assert final_desc == "Profesjonalne strzyżenie z konsultacją"

    def test_caps_lock_creates_diff_against_original(self) -> None:
        """When input has CAPS and original also has CAPS, fix should show in diff."""
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        # Both have CAPS — the fix is applied to pricelist only during finalize
        pricelist["categories"][0]["services"][0]["name"] = "STRZYŻENIE DAMSKIE BALAYAGE"
        original["categories"][0]["services"][0]["name"] = "Strzyżenie damskie"

        result = run_phase4_finalize("test-caps-diff", pricelist, original)

        # After fix_caps_lock: "Strzyżenie damskie balayage" (differs from original)
        final_name = result["finalPricelist"]["categories"][0]["services"][0]["name"]
        assert final_name == "Strzyżenie damskie balayage"
        assert result["summary"]["namesImproved"] == 1

    def test_short_text_not_affected(self) -> None:
        """Short text (<=5 letters) should not be affected by CAPS fix."""
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "SPA"
        original["categories"][0]["services"][0]["name"] = "SPA"

        result = run_phase4_finalize("test-short-caps", pricelist, original)

        final_name = result["finalPricelist"]["categories"][0]["services"][0]["name"]
        assert final_name == "SPA"


# ---------------------------------------------------------------------------
# Tests: Promo detection
# ---------------------------------------------------------------------------


class TestPromoDetection:
    """Verify promo keywords are detected in service names."""

    def test_promo_detected(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie promocja"

        result = run_phase4_finalize("test-promo", pricelist, original)

        final_svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert final_svc["isPromo"] is True

    def test_no_promo_when_absent(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-no-promo", pricelist, original)

        for cat in result["finalPricelist"]["categories"]:
            for svc in cat["services"]:
                assert svc["isPromo"] is False

    def test_multiple_promo_keywords(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie -50% rabat"

        result = run_phase4_finalize("test-multi-promo", pricelist, original)

        final_svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert final_svc["isPromo"] is True


# ---------------------------------------------------------------------------
# Tests: Quality score calculation
# ---------------------------------------------------------------------------


class TestQualityScore:
    """Verify quality score is calculated correctly."""

    def test_quality_score_zero_when_no_changes(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-qs-zero", pricelist, original)

        assert result["qualityScore"] == 0

    def test_quality_score_increases_with_changes(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        # Change 2 out of 5 services (names)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie damskie - balayage"
        pricelist["categories"][0]["services"][1]["name"] = "Koloryzacja - zmiana koloru"

        result = run_phase4_finalize("test-qs-mid", pricelist, original)

        # 2 name changes / 5 total services = 40%
        assert result["qualityScore"] == 40

    def test_quality_score_capped_at_100(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        # Change all 5 services (both name and description)
        for cat in pricelist["categories"]:
            for svc in cat["services"]:
                svc["name"] = svc["name"] + " - SEO keyword"
                svc["description"] = "Nowy opis usługi z korzyścią dla klienta."

        result = run_phase4_finalize("test-qs-cap", pricelist, original)

        assert result["qualityScore"] <= 100

    def test_quality_score_zero_for_empty_pricelist(self) -> None:
        original = {"salonName": "X", "totalServices": 0, "categories": []}
        pricelist = {"salonName": "X", "totalServices": 0, "categories": []}

        result = run_phase4_finalize("test-qs-empty", pricelist, original)

        assert result["qualityScore"] == 0


# ---------------------------------------------------------------------------
# Tests: Summary structure
# ---------------------------------------------------------------------------


class TestSummaryStructure:
    """Verify summary dict has all required fields."""

    def test_summary_keys(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-summary", pricelist, original)

        summary = result["summary"]
        assert "totalChanges" in summary
        assert "namesImproved" in summary
        assert "descriptionsAdded" in summary
        assert "categoriesOptimized" in summary
        assert "duplicatesFound" in summary
        assert "seoKeywordsAdded" in summary

    def test_summary_values_are_ints(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-summary-types", pricelist, original)

        for key, val in result["summary"].items():
            assert isinstance(val, int), f"summary['{key}'] should be int, got {type(val)}"

    def test_result_top_level_keys(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-keys", pricelist, original)

        assert "finalPricelist" in result
        assert "changes" in result
        assert "summary" in result
        assert "qualityScore" in result


# ---------------------------------------------------------------------------
# Tests: Duplicate detection
# ---------------------------------------------------------------------------


class TestDuplicateDetection:
    """Verify duplicate services across categories are detected."""

    def test_duplicate_across_categories(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        # Add a service in category 1 that matches one in category 0
        pricelist["categories"][1]["services"].append({
            "name": "Strzyżenie damskie",
            "price": "120 zł",
            "duration": "45 min",
            "description": None,
            "imageUrl": None,
            "variants": None,
            "tags": None,
            "isPromo": False,
        })
        pricelist["totalServices"] = 6

        result = run_phase4_finalize("test-dupe", pricelist, original)

        assert result["summary"]["duplicatesFound"] >= 1

    def test_no_duplicates_within_same_category(self) -> None:
        """Duplicates within the same category should NOT count."""
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)

        result = run_phase4_finalize("test-no-dupe", pricelist, original)

        assert result["summary"]["duplicatesFound"] == 0


# ---------------------------------------------------------------------------
# Tests: clean_service_name applied
# ---------------------------------------------------------------------------


class TestCleanServiceName:
    """Verify programmatic name cleanup is applied during finalize."""

    def test_trailing_dots_removed(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Strzyżenie..."

        result = run_phase4_finalize("test-clean-dots", pricelist, original)

        final_name = result["finalPricelist"]["categories"][0]["services"][0]["name"]
        assert not final_name.endswith("...")
        assert final_name == "Strzyżenie"

    def test_spacing_around_plus_fixed(self) -> None:
        original = _make_pricelist()
        pricelist = _deep_copy_pricelist(original)
        pricelist["categories"][0]["services"][0]["name"] = "Henna+wosk"

        result = run_phase4_finalize("test-clean-plus", pricelist, original)

        final_name = result["finalPricelist"]["categories"][0]["services"][0]["name"]
        assert final_name == "Henna + wosk"
