"""Tests for audit pipeline helper functions."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from pipelines.helpers import (
    build_full_pricelist_text,
    calculate_audit_stats,
    calculate_completeness_score,
    calculate_similarity,
    clean_service_name,
    fix_caps_lock,
    is_fixed_price,
    validate_name_transformation,
)


# ---------------------------------------------------------------------------
# Fixtures — lightweight duck-typed objects matching ScrapedData shape
# ---------------------------------------------------------------------------


@dataclass
class FakeVariant:
    label: str
    price: str
    duration: str | None = None


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


def _make_simple_data() -> FakeScrapedData:
    """Salon with 2 categories, 5 services total, some with variants."""
    return FakeScrapedData(
        salonName="Test Salon",
        salonAddress="ul. Testowa 1, Warszawa",
        totalServices=5,
        categories=[
            FakeCategory(
                name="Fryzjer",
                services=[
                    FakeService(
                        name="Strzyżenie damskie",
                        price="120 zł",
                        duration="60 min",
                        description="Profesjonalne strzyżenie",
                    ),
                    FakeService(
                        name="Koloryzacja",
                        price="od 200 zł",
                        duration="120 min",
                        description=None,
                        variants=[
                            FakeVariant(label="Krótkie", price="200 zł", duration="90 min"),
                            FakeVariant(label="Długie", price="350 zł", duration="150 min"),
                        ],
                    ),
                    FakeService(
                        name="Modelowanie",
                        price="80 zł",
                        duration=None,
                        description=None,
                    ),
                ],
            ),
            FakeCategory(
                name="Paznokcie",
                services=[
                    FakeService(
                        name="Manicure hybrydowy",
                        price="100 zł",
                        duration="45 min",
                        description="Manicure z lakierem hybrydowym",
                        imageUrl="https://example.com/img.jpg",
                    ),
                    FakeService(
                        name="Pedicure",
                        price="120 zł",
                        duration="60 min",
                        description=None,
                    ),
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCleanServiceName:
    def test_trailing_dots_removed(self) -> None:
        assert clean_service_name("Strzyżenie...") == "Strzyżenie"

    def test_spacing_around_plus(self) -> None:
        assert clean_service_name("Strzyżenie+modelowanie") == "Strzyżenie + modelowanie"

    def test_spacing_around_plus_with_extra_space(self) -> None:
        assert clean_service_name("A+  B") == "A + B"

    def test_spacing_around_minus(self) -> None:
        assert clean_service_name("Usługa-  opis") == "Usługa - opis"

    def test_strips_whitespace(self) -> None:
        assert clean_service_name("  test  ") == "test"

    def test_unchanged_when_clean(self) -> None:
        assert clean_service_name("Strzyżenie damskie") == "Strzyżenie damskie"


class TestFixCapsLock:
    def test_all_caps_fixed(self) -> None:
        result = fix_caps_lock("STRZYŻENIE DAMSKIE")
        assert result == "Strzyżenie damskie"

    def test_short_caps_unchanged(self) -> None:
        # Only 3 letters, below threshold of 5
        assert fix_caps_lock("ABC") == "ABC"

    def test_normal_case_unchanged(self) -> None:
        assert fix_caps_lock("Strzyżenie damskie") == "Strzyżenie damskie"

    def test_polish_caps(self) -> None:
        result = fix_caps_lock("ÓŁTKA MANICURE PEDICURE")
        assert result[0].isupper()
        assert result[1:] == result[1:].lower()

    def test_mixed_case_below_threshold(self) -> None:
        # Less than 60% uppercase
        assert fix_caps_lock("Hello World Nice") == "Hello World Nice"


class TestValidateNameTransformation:
    def test_too_short_rejected(self) -> None:
        assert validate_name_transformation("Test", "AB") is False

    def test_too_long_rejected(self) -> None:
        assert validate_name_transformation("Test", "A" * 81) is False

    def test_marketing_garbage_rejected(self) -> None:
        assert validate_name_transformation("Zabieg", "Zabieg - 100% skuteczny") is False

    def test_enriched_without_separator_rejected(self) -> None:
        # Enriched (longer by >5) but no separator
        assert validate_name_transformation("Manicure", "Manicure hybrydowy klasyczny piękny") is False

    def test_enriched_with_separator_accepted(self) -> None:
        assert validate_name_transformation("Manicure", "Manicure - hybrydowy klasyczny") is True

    def test_cleaning_accepted(self) -> None:
        assert validate_name_transformation("Strzyżenie...", "Strzyżenie") is True

    def test_same_length_similar_accepted(self) -> None:
        # Same words, just reordered — high similarity
        assert validate_name_transformation("Manicure klasyczny", "Klasyczny manicure") is True

    def test_same_length_different_rejected(self) -> None:
        assert validate_name_transformation("Manicure klasyczny", "Pedicure luksusowy") is False

    def test_low_similarity_enriched_with_original_start(self) -> None:
        assert validate_name_transformation("Botox", "Botox - zaawansowana terapia odmładzająca") is True


class TestCalculateSimilarity:
    def test_identical_strings(self) -> None:
        assert calculate_similarity("hello world", "hello world") == 1.0

    def test_completely_different(self) -> None:
        assert calculate_similarity("abc def", "ghi jkl") == 0.0

    def test_partial_overlap(self) -> None:
        sim = calculate_similarity("manicure hybrydowy", "manicure klasyczny")
        assert 0.0 < sim < 1.0
        assert sim == pytest.approx(1 / 3)  # {"manicure"} & {"manicure", "hybrydowy", "klasyczny"}

    def test_empty_strings(self) -> None:
        assert calculate_similarity("", "") == 1.0

    def test_single_char_words_ignored(self) -> None:
        # Single-char words are filtered out
        assert calculate_similarity("a b c", "a b c") == 1.0  # all filtered, both empty -> 1.0


class TestCalculateAuditStats:
    def test_basic_stats(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)

        assert stats["totalServices"] == 5
        assert stats["totalCategories"] == 2
        assert stats["servicesWithVariants"] == 1  # Koloryzacja
        assert stats["totalVariants"] == 2  # Krótkie + Długie
        assert stats["totalPricePoints"] == 6  # 4 without variants + 2 variants

    def test_descriptions_counted(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # "Profesjonalne strzyżenie" and "Manicure z lakierem hybrydowym"
        assert stats["servicesWithDescription"] == 2

    def test_durations_counted(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # Strzyżenie(60), Koloryzacja(120), Manicure(45), Pedicure(60) = 4
        assert stats["servicesWithDuration"] == 4

    def test_fixed_prices(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # "120 zł", "80 zł", "100 zł", "120 zł" are fixed; "od 200 zł" is not
        assert stats["servicesWithFixedPrice"] == 4

    def test_images_counted(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        assert stats["servicesWithImage"] == 1

    def test_category_sizes(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # Fryzjer: Strzyżenie(1) + Koloryzacja variants(2) + Modelowanie(1) = 4
        # Paznokcie: Manicure(1) + Pedicure(1) = 2
        assert stats["largestCategory"]["name"] == "Fryzjer"
        assert stats["largestCategory"]["count"] == 4
        assert stats["smallestCategory"]["name"] == "Paznokcie"
        assert stats["smallestCategory"]["count"] == 2

    def test_no_duplicates(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        assert stats["duplicateNames"] == []

    def test_empty_categories_detected(self) -> None:
        data = _make_simple_data()
        data.categories.append(FakeCategory(name="Pusty", services=[]))
        stats = calculate_audit_stats(data)
        assert "Pusty" in stats["emptyCategories"]

    def test_variants_with_duration(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # Both Krótkie and Długie variants have duration
        assert stats["variantsWithDuration"] == 2

    def test_avg_services_per_category(self) -> None:
        data = _make_simple_data()
        stats = calculate_audit_stats(data)
        # Total price points: 6, categories: 2 -> avg = 3.0
        assert stats["avgServicesPerCategory"] == 3.0


class TestBuildFullPricelistText:
    def test_contains_salon_name(self) -> None:
        data = _make_simple_data()
        text = build_full_pricelist_text(data)
        assert "SALON: Test Salon" in text

    def test_contains_category_names(self) -> None:
        data = _make_simple_data()
        text = build_full_pricelist_text(data)
        assert "KATEGORIA: Fryzjer" in text
        assert "KATEGORIA: Paznokcie" in text

    def test_contains_service_names(self) -> None:
        data = _make_simple_data()
        text = build_full_pricelist_text(data)
        assert "Strzyżenie damskie" in text
        assert "Manicure hybrydowy" in text

    def test_contains_prices(self) -> None:
        data = _make_simple_data()
        text = build_full_pricelist_text(data)
        assert "120 zł" in text

    def test_contains_description_snippet(self) -> None:
        data = _make_simple_data()
        text = build_full_pricelist_text(data)
        assert "OPIS: Profesjonalne strzyżenie" in text

    def test_missing_salon_name(self) -> None:
        data = _make_simple_data()
        data.salonName = None
        text = build_full_pricelist_text(data)
        assert "SALON: Nieznany" in text


class TestCalculateCompletenessScore:
    def test_full_completeness(self) -> None:
        stats = {
            "totalServices": 10,
            "servicesWithDescription": 10,
            "servicesWithDuration": 10,
            "servicesWithFixedPrice": 10,
        }
        score = calculate_completeness_score(stats)
        assert score == 15  # 6 + 5 + 4 = 15

    def test_zero_services(self) -> None:
        stats = {
            "totalServices": 0,
            "servicesWithDescription": 0,
            "servicesWithDuration": 0,
            "servicesWithFixedPrice": 0,
        }
        score = calculate_completeness_score(stats)
        assert score == 0

    def test_partial_completeness(self) -> None:
        stats = {
            "totalServices": 10,
            "servicesWithDescription": 5,
            "servicesWithDuration": 5,
            "servicesWithFixedPrice": 5,
        }
        score = calculate_completeness_score(stats)
        # 0.5*6 + 0.5*5 + 0.5*4 = 3 + 2.5 + 2 = 7.5 -> round = 8
        assert score == 8

    def test_capped_at_15(self) -> None:
        stats = {
            "totalServices": 1,
            "servicesWithDescription": 1,
            "servicesWithDuration": 1,
            "servicesWithFixedPrice": 1,
        }
        score = calculate_completeness_score(stats)
        assert score <= 15


class TestIsFixedPrice:
    def test_fixed_price(self) -> None:
        assert is_fixed_price("120 zł") is True

    def test_od_prefix(self) -> None:
        assert is_fixed_price("od 200 zł") is False

    def test_od_no_space(self) -> None:
        assert is_fixed_price("od200 zł") is False

    def test_range_with_dash(self) -> None:
        assert is_fixed_price("100 - 200 zł") is False

    def test_range_with_en_dash(self) -> None:
        assert is_fixed_price("100\u2013200 zł") is False

    def test_from_prefix(self) -> None:
        assert is_fixed_price("from 50 EUR") is False
