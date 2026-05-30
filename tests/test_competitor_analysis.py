"""Unit and integration tests for Comp Etap 4 competitor analysis.

Three test layers:
1. Pure function tests for pipelines.competitor_dimensional_scores — no mocks,
   known inputs, expected outputs. These exercise every dimension formula.
2. Pure function tests for pricing comparisons, service gaps, and Versum
   mapping application in pipelines.competitor_analysis.
3. An integration test with a fully mocked SupabaseService + CompetitorCandidate
   list that verifies the orchestration function calls all the write helpers
   with the expected row counts.

E2E verification against live Supabase happens via the separate script
scripts/test_etap4_beauty4ever.py (and was also verified manually during
development — see the commit message).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

import pipelines.competitor_selection as selection_mod
from services.focus_score import SalonFocusBundle
from pipelines.competitor_analysis import (
    _active_services_with_treatment,
    _apply_versum_mappings,
    _classify_pricing_action,
    _compute_dimensional_scores,
    _compute_pricing_comparisons,
    _compute_service_gaps,
    compute_competitor_analysis,
)
from pipelines.competitor_dimensional_scores import (
    DIMENSION_METADATA,
    compute_all_dimensions_for_salon,
    compute_content_quality_scores,
    compute_digital_maturity_scores,
    compute_operations_scores,
    compute_percentiles,
    compute_portfolio_scores,
    compute_pricing_scores,
    compute_social_proof_scores,
    compute_subject_percentile,
)
from pipelines.competitor_selection import CompetitorCandidate
from services.supabase import SupabaseService


# ---------------------------------------------------------------------------
# compute_content_quality_scores
# ---------------------------------------------------------------------------


class TestContentQualityScores:
    def test_empty_services_returns_zero(self) -> None:
        result = compute_content_quality_scores([], salon_description=None)
        assert result["description_coverage"] == 0.0
        assert result["photo_coverage"] == 0.0
        assert result["self_description_length"] == 0.0
        assert result["avg_description_length"] == 0.0

    def test_description_coverage_percent(self) -> None:
        # 4 active services, 3 have a real description (≥30 chars) → 75%.
        # Contract (2026-05-15): coverage counts any service whose
        # `description` string is ≥30 chars, regardless of description_type
        # (manual 'M' / predefined 'P' both count). See helper docstring.
        long_desc = "Szczegolowy opis uslugi kosmetycznej dla klientow"
        services = [
            {"description": long_desc, "is_active": True},
            {"description": long_desc, "is_active": True},
            {"description": long_desc, "is_active": True},
            {"description": "—", "is_active": True},  # placeholder < 30 chars
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        assert result["description_coverage"] == 75.0

    def test_photo_coverage_only_counts_nonempty_lists(self) -> None:
        services = [
            {"photos": [{"url": "x"}], "is_active": True},
            {"photos": [], "is_active": True},
            {"photos": None, "is_active": True},
            {"photos": [{"url": "y"}, {"url": "z"}], "is_active": True},
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        # 2 of 4 services have non-empty photos → 50%
        assert result["photo_coverage"] == 50.0

    def test_self_description_length_word_count(self) -> None:
        desc = "To jest pięknie opisany salon kosmetyczny w Warszawie"  # 8 words
        result = compute_content_quality_scores([], salon_description=desc)
        assert result["self_description_length"] == 8.0

    def test_inactive_services_excluded(self) -> None:
        long_desc = "Szczegolowy opis uslugi kosmetycznej dla klientow"
        services = [
            {"description": long_desc, "is_active": True},
            {"description": long_desc, "is_active": False},  # excluded (inactive)
            {"description": "—", "is_active": True},  # active, no real desc
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        # 1 of 2 active services has a real description → 50%
        assert result["description_coverage"] == 50.0

    def test_avg_description_length(self) -> None:
        # Only descriptions ≥30 chars are counted (real-description gate,
        # 2026-05-15). Both strings below clear the gate with known word
        # counts; the None row is excluded.
        services = [
            # 7 words, 63 chars
            {"description": "Profesjonalny zabieg pielegnacyjny dla wymagajacej cery klienta", "is_active": True},
            # 8 words, 69 chars
            {"description": "Kompleksowa terapia odmladzajaca z konsultacja kosmetyczna w pakiecie", "is_active": True},
            {"description": None, "is_active": True},  # no description → excluded
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        # avg of [7, 8] = 7.5
        assert result["avg_description_length"] == 7.5


# ---------------------------------------------------------------------------
# compute_pricing_scores
# ---------------------------------------------------------------------------


class TestPricingScores:
    def test_fixed_price_ratio(self) -> None:
        services = [
            {"is_from_price": False, "is_active": True},
            {"is_from_price": False, "is_active": True},
            {"is_from_price": True, "is_active": True},
            {"is_from_price": True, "is_active": True},
        ]
        result = compute_pricing_scores(services)
        assert result["fixed_price_ratio"] == 50.0

    def test_price_range_spread_in_zloty(self) -> None:
        services = [
            {"price_grosze": 5000, "is_active": True},  # 50 zł
            {"price_grosze": 12000, "is_active": True},  # 120 zł
            {"price_grosze": 20000, "is_active": True},  # 200 zł
        ]
        result = compute_pricing_scores(services)
        # max 200 - min 50 = 150 zł
        assert result["price_range_spread"] == 150.0

    def test_price_range_spread_empty(self) -> None:
        result = compute_pricing_scores([])
        assert result["price_range_spread"] == 0.0

    def test_omnibus_price_compliance(self) -> None:
        services = [
            {"omnibus_price_grosze": 10000, "is_active": True},
            {"omnibus_price_grosze": None, "is_active": True},
        ]
        result = compute_pricing_scores(services)
        assert result["omnibus_price_compliance"] == 50.0

    def test_promo_intensity(self) -> None:
        services = [
            {"is_promo": True, "is_active": True},
            {"is_promo": False, "is_active": True},
            {"is_promo": False, "is_active": True},
            {"is_promo": False, "is_active": True},
        ]
        result = compute_pricing_scores(services)
        assert result["promo_intensity"] == 25.0


# ---------------------------------------------------------------------------
# compute_operations_scores
# ---------------------------------------------------------------------------


class TestOperationsScores:
    def test_opening_hours_mon_fri_9_17(self) -> None:
        # Mon-Fri 9:00-17:00 = 8 hours * 5 days = 40 hours
        open_hours = [
            {"day_of_week": i, "open_from": "09:00", "open_till": "17:00"}
            for i in range(1, 6)
        ]
        result = compute_operations_scores(
            open_hours=open_hours,
            booking_max_modification_time=120,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["opening_hours_per_week"] == 40.0
        assert result["weekend_availability"] == 0.0
        assert result["evening_availability"] == 0.0  # 17:00 is not > 18:00

    def test_weekend_availability_saturday_only(self) -> None:
        open_hours = [
            {"day_of_week": 6, "open_from": "10:00", "open_till": "14:00"},
        ]
        result = compute_operations_scores(
            open_hours=open_hours,
            booking_max_modification_time=None,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["weekend_availability"] == 1.0
        assert result["opening_hours_per_week"] == 4.0

    def test_evening_availability(self) -> None:
        open_hours = [
            {"day_of_week": 1, "open_from": "12:00", "open_till": "20:30"},
        ]
        result = compute_operations_scores(
            open_hours=open_hours,
            booking_max_modification_time=None,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["evening_availability"] == 1.0

    def test_booking_flexibility_ideal_at_120(self) -> None:
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=120,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["booking_flexibility"] == 10.0

    def test_booking_flexibility_penalty(self) -> None:
        # 120 - 150 = 30 min off. Penalty = 30/30 = 1. Score = 10 - 1 = 9
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=150,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["booking_flexibility"] == 9.0

    def test_booking_flexibility_clamped_to_zero(self) -> None:
        # 120 - 500 = 380 off. Penalty would be 12.67 but clamped to 0 min.
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=500,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["booking_flexibility"] == 0.0

    def test_booking_lead_time_days(self) -> None:
        # 7 days in seconds = 604800
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=None,
            booking_max_lead_time=604800,
            deposit_cancel_days=None,
        )
        assert result["booking_lead_time_days"] == 7.0

    def test_deposit_required(self) -> None:
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=None,
            booking_max_lead_time=None,
            deposit_cancel_days=2,
        )
        assert result["deposit_required"] == 1.0

    def test_deposit_not_required_when_none(self) -> None:
        result = compute_operations_scores(
            open_hours=[],
            booking_max_modification_time=None,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        assert result["deposit_required"] == 0.0

    def test_malformed_hours_ignored(self) -> None:
        open_hours = [
            {"day_of_week": 1, "open_from": "bad", "open_till": "17:00"},
            {"day_of_week": 2, "open_from": "09:00", "open_till": "17:00"},
        ]
        result = compute_operations_scores(
            open_hours=open_hours,
            booking_max_modification_time=None,
            booking_max_lead_time=None,
            deposit_cancel_days=None,
        )
        # Only day 2 contributes
        assert result["opening_hours_per_week"] == 8.0


# ---------------------------------------------------------------------------
# compute_digital_maturity_scores
# ---------------------------------------------------------------------------


class TestDigitalMaturityScores:
    def test_all_true(self) -> None:
        result = compute_digital_maturity_scores(
            has_online_services=True,
            has_online_vouchers=True,
            pos_pay_by_app=True,
        )
        assert result["digital_maturity_score"] == 3.0
        assert result["has_online_services"] == 1.0

    def test_none_becomes_zero(self) -> None:
        result = compute_digital_maturity_scores(
            has_online_services=None,
            has_online_vouchers=None,
            pos_pay_by_app=None,
        )
        assert result["digital_maturity_score"] == 0.0

    def test_partial(self) -> None:
        result = compute_digital_maturity_scores(
            has_online_services=True,
            has_online_vouchers=False,
            pos_pay_by_app=True,
        )
        assert result["digital_maturity_score"] == 2.0


# ---------------------------------------------------------------------------
# compute_social_proof_scores
# ---------------------------------------------------------------------------


class TestSocialProofScores:
    def test_no_reviews_zero_velocity(self) -> None:
        result = compute_social_proof_scores(
            reviews_count=0,
            reviews_rank=None,
            reviews=[],
            facebook_url=None,
            instagram_url=None,
            website=None,
        )
        assert result["reviews_count"] == 0.0
        assert result["reviews_rank"] == 0.0
        assert result["review_velocity_30d"] == 0.0
        assert result["owner_reply_rate"] == 0.0
        assert result["social_presence_count"] == 0.0

    def test_review_velocity_counts_recent(self) -> None:
        now = datetime(2026, 4, 1, tzinfo=timezone.utc)
        reviews = [
            {"review_created_at": "2026-03-20T12:00:00+00:00"},  # 12 days ago
            {"review_created_at": "2026-01-01T12:00:00+00:00"},  # too old
            {"review_created_at": "2026-03-05T12:00:00+00:00"},  # 27 days ago
        ]
        result = compute_social_proof_scores(
            reviews_count=100,
            reviews_rank=4.5,
            reviews=reviews,
            facebook_url=None,
            instagram_url=None,
            website=None,
            now=now,
        )
        assert result["review_velocity_30d"] == 2.0

    def test_owner_reply_rate(self) -> None:
        reviews = [
            {"reply_content": "Dziękujemy!"},
            {"reply_content": ""},
            {"reply_content": None},
            {"reply_content": "Zapraszamy"},
        ]
        result = compute_social_proof_scores(
            reviews_count=4,
            reviews_rank=4.5,
            reviews=reviews,
            facebook_url=None,
            instagram_url=None,
            website=None,
        )
        # 2 of 4 have reply content → 50%
        assert result["owner_reply_rate"] == 50.0

    def test_social_presence_count(self) -> None:
        result = compute_social_proof_scores(
            reviews_count=0,
            reviews_rank=None,
            reviews=[],
            facebook_url="https://fb.com/x",
            instagram_url="https://ig.com/y",
            website=None,
        )
        assert result["social_presence_count"] == 2.0


# ---------------------------------------------------------------------------
# compute_portfolio_scores
# ---------------------------------------------------------------------------


class TestPortfolioScores:
    def test_total_services_and_categories(self) -> None:
        services = [
            {"category_name": "A", "is_active": True},
            {"category_name": "A", "is_active": True},
            {"category_name": "B", "is_active": True},
            {"category_name": "C", "is_active": True},
        ]
        result = compute_portfolio_scores(services)
        assert result["total_services"] == 4.0
        assert result["total_categories"] == 3.0
        assert result["services_per_category"] == 4.0 / 3.0

    def test_combo_ratio(self) -> None:
        services = [
            {"combo_type": "package", "is_active": True},
            {"combo_type": None, "is_active": True},
        ]
        result = compute_portfolio_scores(services)
        assert result["combo_ratio"] == 50.0

    def test_unique_treatment_count(self) -> None:
        services = [
            {"booksy_treatment_id": 1, "is_active": True},
            {"booksy_treatment_id": 1, "is_active": True},  # duplicate
            {"booksy_treatment_id": 2, "is_active": True},
            {"booksy_treatment_id": None, "is_active": True},
        ]
        result = compute_portfolio_scores(services)
        assert result["unique_treatment_count"] == 2.0


# ---------------------------------------------------------------------------
# compute_percentiles + subject_percentile
# ---------------------------------------------------------------------------


class TestPercentiles:
    def test_empty_list_returns_zeros(self) -> None:
        result = compute_percentiles([])
        assert all(v == 0.0 for v in result.values())

    def test_single_value(self) -> None:
        result = compute_percentiles([50.0])
        assert result["market_min"] == 50.0
        assert result["market_p50"] == 50.0
        assert result["market_max"] == 50.0

    def test_linear_interpolation(self) -> None:
        # Five equal-spaced values: min=0, max=100, p25=25, p50=50, p75=75
        result = compute_percentiles([0, 25, 50, 75, 100])
        assert result["market_min"] == 0.0
        assert result["market_p25"] == 25.0
        assert result["market_p50"] == 50.0
        assert result["market_p75"] == 75.0
        assert result["market_max"] == 100.0


class TestSubjectPercentile:
    def test_empty_market_returns_fifty(self) -> None:
        assert compute_subject_percentile(50.0, []) == 50.0

    def test_subject_is_median(self) -> None:
        # 5 values, subject equals median → 60% (3 of 5 are ≤)
        assert compute_subject_percentile(50.0, [10, 20, 50, 80, 100]) == 60.0

    def test_subject_is_top(self) -> None:
        # subject > all market → 100
        assert compute_subject_percentile(1000.0, [10, 20, 50]) == 100.0

    def test_subject_below_all(self) -> None:
        assert compute_subject_percentile(5.0, [10, 20, 50]) == 0.0


# ---------------------------------------------------------------------------
# compute_all_dimensions_for_salon
# ---------------------------------------------------------------------------


class TestAllDimensions:
    def test_returns_all_expected_dimensions(self) -> None:
        salon = {
            "scrape": {
                "salon_description": "Test salon description",
                "booking_max_modification_time": 120,
                "booking_max_lead_time": 604800,
                "deposit_cancel_days": None,
                "pos_pay_by_app": True,
                "has_online_services": True,
                "has_online_vouchers": False,
                "reviews_count": 100,
                "reviews_rank": 4.5,
                "facebook_url": "x",
                "instagram_url": None,
                "website": "y",
                "open_hours": [
                    {"day_of_week": i, "open_from": "09:00", "open_till": "17:00"}
                    for i in range(1, 6)
                ],
            },
            "services": [
                {
                    "category_name": "A",
                    "description_type": "M",
                    "description": "desc",
                    "photos": [{"url": "p"}],
                    "booksy_treatment_id": 1,
                    "price_grosze": 10000,
                    "is_from_price": False,
                    "is_active": True,
                    "is_promo": False,
                    "combo_type": None,
                    "omnibus_price_grosze": None,
                },
            ],
            "reviews": [],
        }
        result = compute_all_dimensions_for_salon(salon)
        # Every dimension in the metadata must be present
        expected = {dim for _, dim, _, _ in DIMENSION_METADATA}
        assert set(result.keys()) == expected

    def test_missing_scrape_fields_graceful(self) -> None:
        salon: dict = {"scrape": {}, "services": [], "reviews": []}
        result = compute_all_dimensions_for_salon(salon)
        # Should not raise, all dimensions should be 0 or neutral defaults
        assert result["total_services"] == 0.0
        assert result["reviews_count"] == 0.0
        assert result["opening_hours_per_week"] == 0.0


# ---------------------------------------------------------------------------
# _classify_pricing_action (pipelines.competitor_analysis)
# ---------------------------------------------------------------------------


class TestClassifyPricingAction:
    def test_far_below_market_raise(self) -> None:
        assert _classify_pricing_action(-30.0) == "raise"

    def test_far_above_market_lower(self) -> None:
        assert _classify_pricing_action(35.0) == "lower"

    def test_aligned_hold(self) -> None:
        assert _classify_pricing_action(0.0) == "hold"
        assert _classify_pricing_action(-10.0) == "hold"
        assert _classify_pricing_action(15.0) == "hold"

    def test_edges(self) -> None:
        assert _classify_pricing_action(-15.0) == "hold"  # exact boundary — hold
        assert _classify_pricing_action(-15.01) == "raise"
        assert _classify_pricing_action(20.0) == "hold"
        assert _classify_pricing_action(20.01) == "lower"


# ---------------------------------------------------------------------------
# _active_services_with_treatment
# ---------------------------------------------------------------------------


class TestActiveServicesWithTreatment:
    def test_drops_inactive(self) -> None:
        # has_embedding required by the 2026-05-15 hard gate (services
        # without a name embedding are dropped from pricing comparisons).
        services = [
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 1000, "has_embedding": True},
            {"booksy_treatment_id": 2, "is_active": False, "price_grosze": 2000, "has_embedding": True},
        ]
        result = _active_services_with_treatment(services)
        assert 1 in result
        assert 2 not in result

    def test_drops_null_treatment(self) -> None:
        services = [
            {"booksy_treatment_id": None, "is_active": True, "price_grosze": 1000},
        ]
        assert _active_services_with_treatment(services) == {}

    def test_duplicate_treatment_picks_lowest_price(self) -> None:
        services = [
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 2000, "name": "variant2", "has_embedding": True},
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 1500, "name": "variant1", "has_embedding": True},
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 3000, "name": "variant3", "has_embedding": True},
        ]
        result = _active_services_with_treatment(services)
        assert result[1]["name"] == "variant1"


# ---------------------------------------------------------------------------
# _apply_versum_mappings
# ---------------------------------------------------------------------------


class TestApplyVersumMappings:
    def test_applies_mapping_to_unmapped_services(self) -> None:
        salon_data = {
            "salon_id": 100,
            "services": [
                {"booksy_service_id": 1, "booksy_treatment_id": None},
                {"booksy_service_id": 2, "booksy_treatment_id": None},
                {"booksy_service_id": 3, "booksy_treatment_id": 999},  # already mapped
            ],
        }
        versum_map = {
            (100, 1): 555,
            (100, 2): 777,
        }
        _apply_versum_mappings(salon_data, versum_map)
        assert salon_data["services"][0]["booksy_treatment_id"] == 555
        assert salon_data["services"][1]["booksy_treatment_id"] == 777
        assert salon_data["services"][2]["booksy_treatment_id"] == 999  # untouched

    def test_no_mapping_leaves_untouched(self) -> None:
        salon_data = {
            "salon_id": 100,
            "services": [
                {"booksy_service_id": 1, "booksy_treatment_id": None},
            ],
        }
        _apply_versum_mappings(salon_data, {})
        assert salon_data["services"][0]["booksy_treatment_id"] is None


# ---------------------------------------------------------------------------
# _compute_pricing_comparisons
# ---------------------------------------------------------------------------


def _cand(*, salon_id: int, counts_in_aggregates: bool = True) -> CompetitorCandidate:
    return CompetitorCandidate(
        salon_id=salon_id,
        booksy_id=salon_id + 1000,
        name=f"Cand{salon_id}",
        city="Warszawa",
        primary_category_id=11,
        reviews_count=100,
        reviews_rank=4.5,
        distance_km=1.0,
        female_weight_diff=0.0,
        composite_score=50.0,
        bucket="cluster",
        counts_in_aggregates=counts_in_aggregates,
        similarity_scores={},
        partner_system="native",
    )


_NEXT_SVC_ID = [10_000]


def _svc(
    *,
    treatment_id: int,
    price_grosze: int | None = 10000,
    name: str = "x",
    is_active: bool = True,
    variant_id: int | None = None,
    service_id: int | None = None,
) -> dict:
    # Auto-id so each fixture row has a unique pk for embedding lookups.
    _NEXT_SVC_ID[0] += 1
    return {
        "id": service_id if service_id is not None else _NEXT_SVC_ID[0],
        "booksy_treatment_id": treatment_id,
        # Phase 5 made variant_id mandatory for pricing comparisons; default
        # to a deterministic synthetic per-tid variant so callers don't have
        # to thread it through every assertion.
        "variant_id": variant_id if variant_id is not None else treatment_id * 10,
        "price_grosze": price_grosze,
        "name": name,
        "treatment_name": name,
        "is_active": is_active,
        "treatment_parent_id": None,
        "is_from_price": False,
        "duration_minutes": None,
    }


def _mock_supabase_no_verify() -> AsyncMock:
    """Stub SupabaseService that returns no centroids / no embeddings.

    Verification still runs when |deviation| > 80%, but with no centroid +
    no name_embedding the embedding check is skipped, no package keyword is
    detected (test names use 'x' / 'A' / 'Lase'), duration is unknown, so
    every extreme row falls to 'extreme_outlier' and is kept.
    """
    service = AsyncMock()
    service.get_variant_centroids.return_value = {}
    service.get_service_embeddings.return_value = {}
    # Tier-3 (sub_variant) pricing loads sub-variants via this async method.
    # Synthetic fixtures have no native Booksy multi-variants, so prod would
    # return {} here too → zero tier-3 rows. Without the explicit stub the
    # bare AsyncMock returns a MagicMock that isn't iterable at the
    # `for sv in sub_variants_map.get(sid, [])` loop.
    service.get_sub_variants_for_services.return_value = {}
    return service


class TestComputePricingComparisons:
    @pytest.mark.asyncio
    async def test_empty_subject_returns_empty(self) -> None:
        subject_data = {"services": []}
        result = await _compute_pricing_comparisons(
            _mock_supabase_no_verify(), report_id=1,
            subject_data=subject_data, aligned_competitors=[],
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_single_competitor_price_still_emits_variant_row(self) -> None:
        # Contract change (commit a72440a, 2026-05-16 user feedback "nie
        # możemy wycinać całego bloku zabiegu"): the variant tier no longer
        # requires a minimum of 2 competitor prices. A single matched
        # competitor still emits a comparison_tier='variant' row, carrying
        # sample_size=1 so the UI can flag the thin sample. The OLD
        # `if len(samples) < 2: continue` gate was deliberately removed.
        subject_data = {
            "services": [_svc(treatment_id=1, price_grosze=1000, name="A")],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [_svc(treatment_id=1, price_grosze=900)], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase_no_verify(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
        )
        assert len(result) == 1
        row = result[0]
        assert row["comparison_tier"] == "variant"
        assert row["booksy_treatment_id"] == 1
        # Single competitor → market median == that competitor's price.
        assert row["sample_size"] == 1
        assert row["market_median_grosze"] == 900
        assert len(row["competitor_samples"]) == 1

    @pytest.mark.asyncio
    async def test_computes_deviation_and_action(self) -> None:
        # Subject 1000, market [500, 600, 700] → median 600, deviation = +66.67% → lower
        subject_data = {
            "services": [_svc(treatment_id=5, price_grosze=1000, name="Lase")],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [_svc(treatment_id=5, price_grosze=500)], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [_svc(treatment_id=5, price_grosze=600)], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [_svc(treatment_id=5, price_grosze=700)], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase_no_verify(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
        )
        assert len(result) == 1
        row = result[0]
        assert row["booksy_treatment_id"] == 5
        assert row["subject_price_grosze"] == 1000
        assert row["market_median_grosze"] == 600
        assert row["sample_size"] == 3
        assert abs(row["deviation_pct"] - 66.67) < 0.01
        assert row["recommended_action"] == "lower"
        # Mig 064 fields surface on every row. VERIFICATION_THRESHOLD_PCT was
        # lowered to 0.0 (2026-05-15 — "apply checks ALWAYS"), so any non-zero
        # deviation runs verify_pricing_comparison. With no package keyword,
        # no centroid (mock returns {}), and no duration, the row passes every
        # check but still has deviation > 0 → verification_status is
        # 'extreme_outlier' (kept for display, not dropped). See
        # services/pricing_verification.py verify_pricing_comparison.
        assert row["verification_status"] == "extreme_outlier"
        assert isinstance(row["competitor_samples"], list)
        assert len(row["competitor_samples"]) == 3

    @pytest.mark.asyncio
    async def test_non_counting_competitors_excluded_from_samples(self) -> None:
        # counts_in_aggregates=False competitors must NOT contribute price
        # samples (still a real contract). Cand1 (price 500) is excluded;
        # only Cand2 (counting, price 1000) drives the market. Post-2026-05-16
        # the single remaining counting price still emits a variant row
        # (min-2 gate removed — see test_single_competitor_price_*).
        subject_data = {
            "services": [_svc(treatment_id=5, price_grosze=1000)],
        }
        aligned = [
            (_cand(salon_id=1, counts_in_aggregates=False), {"services": [_svc(treatment_id=5, price_grosze=500)], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [_svc(treatment_id=5, price_grosze=1000)], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase_no_verify(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
        )
        assert len(result) == 1
        row = result[0]
        # Only the counting competitor (salon_id=2, price 1000) contributes —
        # the non-counting 500 zł price is excluded, so median stays 1000.
        assert row["sample_size"] == 1
        assert row["market_median_grosze"] == 1000
        sample_salon_ids = [s.get("salon_id") for s in row["competitor_samples"]]
        assert sample_salon_ids == [2]

    @pytest.mark.asyncio
    async def test_extreme_deviation_with_package_keyword_is_dropped(self) -> None:
        # Subject 280000 grosze (2800 zł) "Onda 4 zabiegi" vs market median
        # ~20000 (200 zł) = +1300% deviation. Package keyword "4 zabiegi"
        # in subject name should drop the row before display.
        subject_data = {
            "services": [_svc(
                treatment_id=7, price_grosze=280000,
                name="Onda 4 zabiegi 1 obszar",
            )],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [_svc(treatment_id=7, price_grosze=15000)], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [_svc(treatment_id=7, price_grosze=20000)], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [_svc(treatment_id=7, price_grosze=25000)], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase_no_verify(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
        )
        # Dropped — package mismatch.
        assert result == []


# ---------------------------------------------------------------------------
# _compute_service_gaps
# ---------------------------------------------------------------------------


def _svc_embedded(**kwargs) -> dict:
    """Like _svc but marks the row as embedded (passes the hard-gate in
    _active_services_with_treatment which would otherwise drop it)."""
    row = _svc(**kwargs)
    row["has_embedding"] = True
    return row


def _mock_supabase_for_gaps(
    *,
    subject_categories_by_svc: dict[int, set[str]] | None = None,
    competitor_categories_by_svc: dict[int, set[str]] | None = None,
) -> AsyncMock:
    """Stub SupabaseService driving the method-category walk-up + USP
    embedding verification used by _compute_service_gaps.

    Tests that exercise the walk-up pass `subject_categories_by_svc`
    and `competitor_categories_by_svc` keyed by service id. Tests that
    don't care can pass empty dicts to get fail-open behaviour (walk-
    up no-ops; legacy gap output preserved).
    """
    s_cats = subject_categories_by_svc or {}
    c_cats = competitor_categories_by_svc or {}

    # `_resolve_method_categories_for_services` is the function we need to
    # stub. It takes (service, service_ids) and returns
    # {service_id: set[category]}. We patch it directly on the module so
    # the SupabaseService stub doesn't need to mimic the table chain.
    service = AsyncMock()
    # USP embedding lookup — empty so no pseudo-USP suppression.
    service.get_service_embeddings.return_value = {}

    return service, s_cats, c_cats


class TestComputeServiceGaps:
    @pytest.mark.asyncio
    async def test_missing_type_ranked_by_competitor_count(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Subject has nothing → walk-up trivially no-ops, legacy ranking
        # should hold.
        subject_data = {"services": []}
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc_embedded(treatment_id=1), _svc_embedded(treatment_id=2)],
                "reviews": [],
            }),
            (_cand(salon_id=2), {
                "services": [_svc_embedded(treatment_id=1)],
                "reviews": [],
            }),
        ]
        service = AsyncMock()
        service.get_service_embeddings.return_value = {}
        # Walk-up no-ops with empty subject service list, but stub the
        # resolver anyway so any incidental call returns {}.
        async def _resolve_stub(svc, sids):
            return {}
        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )
        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        missing = [r for r in result if r["gap_type"] == "missing"]
        # tid=1 has 2 competitors, tid=2 has 1 — tid=1 should be first
        assert missing[0]["booksy_treatment_id"] == 1
        assert missing[0]["competitor_count"] == 2
        assert missing[1]["booksy_treatment_id"] == 2
        assert missing[1]["competitor_count"] == 1

    @pytest.mark.asyncio
    async def test_unique_usp_extraction(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subject_data = {
            "services": [
                _svc_embedded(treatment_id=100, price_grosze=5000, name="Unique1"),
                _svc_embedded(treatment_id=200, price_grosze=3000, name="SharedWithCompetitor"),
            ],
        }
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc_embedded(treatment_id=200, price_grosze=2500)],
                "scrape": {"salon_name": "Cand1"},
                "reviews": [],
            }),
        ]
        service = AsyncMock()
        service.get_service_embeddings.return_value = {}
        async def _resolve_stub(svc, sids):
            return {}
        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )
        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        uniques = [r for r in result if r["gap_type"] == "unique_usp"]
        assert len(uniques) == 1
        assert uniques[0]["booksy_treatment_id"] == 100
        assert uniques[0]["competitor_count"] == 0

    @pytest.mark.asyncio
    async def test_missing_capped_at_10(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subject_data = {"services": []}
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc_embedded(treatment_id=i) for i in range(1, 16)],
                "reviews": [],
            }),
        ]
        service = AsyncMock()
        service.get_service_embeddings.return_value = {}
        async def _resolve_stub(svc, sids):
            return {}
        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )
        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        missing = [r for r in result if r["gap_type"] == "missing"]
        assert len(missing) == 10

    @pytest.mark.asyncio
    async def test_walk_up_filters_missing_when_subject_covers_same_category(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression test for the false-positive "Lifting falą radiową"
        gap (2026-05-24): subject has Onda + fala_radiowa under tid 100
        (classified `rf_hifu`); competitor has "Lifting falą radiową"
        under tid 200 (also `rf_hifu`). Set difference {200} − {100} =
        {200} would report tid 200 as missing, but subject already has
        equivalent rf_hifu coverage — walk-up must suppress this row.
        """
        subject_svc = _svc_embedded(
            treatment_id=100, price_grosze=15000, name="Onda 1 zabieg",
            service_id=9001,
        )
        comp_svc = _svc_embedded(
            treatment_id=200, price_grosze=20000,
            name="Lifting falą radiową", service_id=9101,
        )
        subject_data = {"services": [subject_svc]}
        aligned = [
            (_cand(salon_id=1), {
                "services": [comp_svc],
                "scrape": {"salon_name": "Cand1"},
                "reviews": [],
            }),
        ]

        service = AsyncMock()
        service.get_service_embeddings.return_value = {}

        async def _resolve_stub(svc, sids):
            # Same category for both subject (svc id 9001) and competitor
            # (svc id 9101). Walk-up should suppress the gap row.
            out: dict[int, set[str]] = {}
            for sid in sids:
                if int(sid) == 9001:
                    out[9001] = {"rf_hifu"}
                elif int(sid) == 9101:
                    out[9101] = {"rf_hifu"}
            return out

        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )

        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        missing = [r for r in result if r["gap_type"] == "missing"]
        # Walk-up must have suppressed tid 200 (rf_hifu already covered
        # by subject's tid 100 under a different brand name).
        assert missing == [], (
            f"Expected walk-up to suppress rf_hifu sibling gap, got: {missing}"
        )

    @pytest.mark.asyncio
    async def test_walk_up_keeps_gap_when_category_differs(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Walk-up must not over-filter: subject in rf_hifu does NOT
        cover a real laser_skin gap from the competitor.
        """
        subject_svc = _svc_embedded(
            treatment_id=100, price_grosze=15000, name="Onda",
            service_id=8001,
        )
        comp_svc = _svc_embedded(
            treatment_id=200, price_grosze=20000,
            name="Laser frakcyjny", service_id=8101,
        )
        subject_data = {"services": [subject_svc]}
        aligned = [
            (_cand(salon_id=1), {
                "services": [comp_svc],
                "scrape": {"salon_name": "Cand1"},
                "reviews": [],
            }),
        ]
        service = AsyncMock()
        service.get_service_embeddings.return_value = {}

        async def _resolve_stub(svc, sids):
            out: dict[int, set[str]] = {}
            for sid in sids:
                if int(sid) == 8001:
                    out[8001] = {"rf_hifu"}
                elif int(sid) == 8101:
                    out[8101] = {"laser_skin"}
            return out

        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )

        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        missing = [r for r in result if r["gap_type"] == "missing"]
        assert len(missing) == 1
        assert missing[0]["booksy_treatment_id"] == 200

    @pytest.mark.asyncio
    async def test_walk_up_fails_open_when_classifications_empty(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the classification cache is cold for either side, walk-up
        must keep the gap row (legacy behaviour — fail-open contract).
        """
        subject_svc = _svc_embedded(
            treatment_id=100, name="Onda", service_id=7001,
        )
        comp_svc = _svc_embedded(
            treatment_id=200, name="Lifting falą radiową", service_id=7101,
        )
        subject_data = {"services": [subject_svc]}
        aligned = [
            (_cand(salon_id=1), {
                "services": [comp_svc],
                "scrape": {"salon_name": "Cand1"},
                "reviews": [],
            }),
        ]
        service = AsyncMock()
        service.get_service_embeddings.return_value = {}

        async def _resolve_stub(svc, sids):
            # Empty — simulate cold classification cache.
            return {}

        monkeypatch.setattr(
            "pipelines.competitor_analysis._resolve_method_categories_for_services",
            _resolve_stub,
        )

        result = await _compute_service_gaps(
            service, report_id=1, subject_data=subject_data,
            aligned_competitors=aligned,
        )
        missing = [r for r in result if r["gap_type"] == "missing"]
        # Legacy behaviour: row survives because we can't prove overlap.
        assert len(missing) == 1
        assert missing[0]["booksy_treatment_id"] == 200


# ---------------------------------------------------------------------------
# _compute_dimensional_scores
# ---------------------------------------------------------------------------


class TestComputeDimensionalScores:
    def test_emits_one_row_per_dimension(self) -> None:
        subject_data = {
            "scrape": {
                "salon_description": "test",
                "open_hours": [],
                "reviews_count": 100,
                "reviews_rank": 4.5,
            },
            "services": [],
            "reviews": [],
        }
        aligned = [
            (_cand(salon_id=1), {
                "scrape": {"reviews_count": 50, "reviews_rank": 4.2, "open_hours": []},
                "services": [],
                "reviews": [],
            }),
        ]
        rows = _compute_dimensional_scores(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        assert len(rows) == len(DIMENSION_METADATA)
        dims_in_rows = [r["dimension"] for r in rows]
        expected_dims = [dim for _, dim, _, _ in DIMENSION_METADATA]
        assert dims_in_rows == expected_dims

    def test_percentile_and_category_populated(self) -> None:
        subject_data = {
            "scrape": {
                "reviews_count": 1000,  # subject is top
                "reviews_rank": 5.0,
                "open_hours": [],
            },
            "services": [],
            "reviews": [],
        }
        aligned = [
            (_cand(salon_id=i), {
                "scrape": {"reviews_count": c, "reviews_rank": 4.5, "open_hours": []},
                "services": [],
                "reviews": [],
            })
            for i, c in enumerate([10, 20, 30], start=1)
        ]
        rows = _compute_dimensional_scores(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        reviews_row = next(r for r in rows if r["dimension"] == "reviews_count")
        assert reviews_row["subject_value"] == 1000
        assert reviews_row["subject_percentile"] == 100.0
        assert reviews_row["category"] == "social_proof"
        assert reviews_row["better_is_higher"] is True


# ---------------------------------------------------------------------------
# compute_competitor_analysis end-to-end with mocks
# ---------------------------------------------------------------------------


def _mock_supabase_for_e2e() -> AsyncMock:
    mock = AsyncMock()

    # service.client is the SYNCHRONOUS supabase builder in prod
    # (.table().insert().execute()). TraceWriter.flush() writes through it.
    # Use a plain MagicMock so the chain returns regular mocks instead of
    # coroutines (a bare AsyncMock child would make .table() awaitable).
    mock.client = MagicMock()

    # select_competitors calls these — minimal viable subject + 2 candidates
    mock.get_subject_salon_for_audit = AsyncMock(return_value={
        "salon_id": 100,
        "booksy_id": 1000,
        "name": "Subject",
        "city": "Warszawa",
        "salon_lat": 52.0,
        "salon_lng": 21.0,
        "primary_category_id": 11,
        "business_categories": [{"id": 1, "female_weight": 90}],
        "reviews_count": 500,
        "reviews_rank": 4.5,
        "partner_system": "native",
    })
    mock.get_salon_top_services = AsyncMock(return_value=[])
    mock.get_candidate_salons = AsyncMock(return_value=[
        {
            "salon_id": 200, "booksy_id": 2000, "name": "Comp1",
            "city": "Warszawa", "distance_km": 1.0,
            "reviews_rank": 4.5, "reviews_count": 400, "service_count": 50,
        },
        {
            "salon_id": 300, "booksy_id": 3000, "name": "Comp2",
            "city": "Warszawa", "distance_km": 2.0,
            "reviews_rank": 4.6, "reviews_count": 350, "service_count": 60,
        },
        {
            "salon_id": 400, "booksy_id": 4000, "name": "Comp3",
            "city": "Warszawa", "distance_km": 3.0,
            "reviews_rank": 4.7, "reviews_count": 250, "service_count": 70,
        },
    ])
    mock.get_latest_business_categories_for_booksy_ids = AsyncMock(return_value={
        2000: [{"id": 1, "female_weight": 90}],
        3000: [{"id": 1, "female_weight": 90}],
        4000: [{"id": 1, "female_weight": 90}],
    })
    mock.get_latest_top_services_for_salon_ids = AsyncMock(return_value={})
    mock.get_latest_partner_system_for_booksy_ids = AsyncMock(return_value={
        2000: "native", 3000: "native", 4000: "native",
    })

    # Etap 4 writer helpers — track call counts
    mock.create_competitor_report = AsyncMock(return_value=999)
    mock.delete_competitor_report_children = AsyncMock(return_value=None)
    mock.insert_competitor_matches = AsyncMock(return_value=3)
    mock.insert_competitor_pricing_comparisons = AsyncMock(return_value=1)
    mock.insert_competitor_service_gaps = AsyncMock(return_value=0)
    mock.insert_competitor_dimensional_scores = AsyncMock(return_value=28)
    mock.update_competitor_report_status = AsyncMock(return_value=None)
    mock.get_versum_mappings = AsyncMock(return_value={})
    mock.get_active_promotions = AsyncMock(return_value={})
    # Mig 064: pricing verification fetches — return empty dicts so no
    # row enters the verify path (smoke tests above cover the verify branch).
    mock.get_variant_centroids = AsyncMock(return_value={})
    mock.get_service_embeddings = AsyncMock(return_value={})
    # Tier-3 sub_variant pricing — synthetic fixtures have no native Booksy
    # multi-variants, so prod returns {} here too → zero tier-3 rows.
    mock.get_sub_variants_for_services = AsyncMock(return_value={})

    # Etap 4 readers
    def _mk_salon_data(salon_id: int, reviews_count: int) -> dict:
        return {
            "salon_id": salon_id,
            "booksy_id": salon_id * 10,
            "scrape": {
                "salon_description": "test",
                "booking_max_modification_time": 120,
                "booking_max_lead_time": None,
                "deposit_cancel_days": None,
                "pos_pay_by_app": True,
                "has_online_services": False,
                "has_online_vouchers": False,
                "reviews_count": reviews_count,
                "reviews_rank": 4.5,
                "facebook_url": None,
                "instagram_url": None,
                "website": None,
                "open_hours": [],
                "partner_system": "native",
            },
            "services": [
                {
                    "booksy_treatment_id": 1,
                    "booksy_service_id": 11,
                    "price_grosze": 10000,
                    "name": "Test",
                    "treatment_name": "Test",
                    "is_active": True,
                    "is_from_price": False,
                    "duration_minutes": None,
                    "treatment_parent_id": None,
                    "description_type": "M",
                    "photos": [],
                    "category_name": "A",
                    "is_promo": False,
                    "combo_type": None,
                    "omnibus_price_grosze": None,
                    "description": None,
                },
            ],
            "reviews": [],
            "top_services": [],
            "partner_system": "native",
        }

    mock.get_subject_full_data = AsyncMock(return_value=_mk_salon_data(100, 500))
    mock.get_competitor_full_data = AsyncMock(return_value={
        2000: _mk_salon_data(200, 400),
        3000: _mk_salon_data(300, 350),
        4000: _mk_salon_data(400, 250),
    })

    return mock


@pytest.mark.asyncio
async def test_compute_competitor_analysis_end_to_end_with_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Integration test — verify orchestration calls every expected write."""
    # select_competitors reaches into service.client for portfolio embeddings
    # (mig 087) via _fetch_subject_focus_bundle / _fetch_candidate_focus_
    # bundles_batch, which the AsyncMock-based supabase can't provide offline.
    # Stub the v2 focus-bundle loaders (same pattern as
    # test_competitor_selection.py::_stub_focus_bundles): identical unit
    # embedding + focus distribution → every candidate buckets as 'direct',
    # isolating the orchestration path from the embedding plumbing.
    _emb = np.ones(1536, dtype=np.float64)
    _focus = {503: 1.0}

    def _subject_bundle(service, salon_id, booksy_id):  # noqa: ANN001
        return SalonFocusBundle(
            salon_id=salon_id, booksy_id=booksy_id,
            portfolio_embedding=_emb, focus_distribution=dict(_focus),
            focus_variant_distribution=dict(_focus),
            service_count=10, embedded_count=10,
        )

    def _candidate_bundles(service, salon_ids):  # noqa: ANN001
        return {
            sid: SalonFocusBundle(
                salon_id=sid, booksy_id=None,
                portfolio_embedding=_emb, focus_distribution=dict(_focus),
                focus_variant_distribution=dict(_focus),
                service_count=10, embedded_count=10,
            )
            for sid in salon_ids
        }

    monkeypatch.setattr(
        selection_mod, "_fetch_subject_focus_bundle", _subject_bundle,
    )
    monkeypatch.setattr(
        selection_mod, "_fetch_candidate_focus_bundles_batch", _candidate_bundles,
    )

    mock = _mock_supabase_for_e2e()
    progress_calls = []

    async def progress(p: int, m: str) -> None:
        progress_calls.append((p, m))

    report_id = await compute_competitor_analysis(
        audit_id="test_audit",
        tier="base",
        selection_mode="auto",
        on_progress=progress,
        supabase=mock,
        convex_user_id="user_1",
    )

    assert report_id == 999
    mock.create_competitor_report.assert_called_once()
    mock.delete_competitor_report_children.assert_called_once_with(999)
    mock.insert_competitor_matches.assert_called_once()
    mock.insert_competitor_pricing_comparisons.assert_called_once()
    mock.insert_competitor_service_gaps.assert_called_once()
    mock.insert_competitor_dimensional_scores.assert_called_once()
    mock.update_competitor_report_status.assert_called_once()
    status_args = mock.update_competitor_report_status.call_args
    assert status_args[0][0] == 999
    assert status_args[0][1] == "completed"
    # Progress should reach 100
    assert progress_calls[-1][0] == 100


# ---------------------------------------------------------------------------
# insert_competitor_matches — is_user_selected wiring (migration 122)
# ---------------------------------------------------------------------------


class TestInsertCompetitorMatchesUserSelected:
    @pytest.mark.asyncio
    async def test_rows_carry_is_user_selected_from_candidate(self) -> None:
        """Each inserted row's is_user_selected reflects the candidate flag,
        and a candidate lacking the attribute falls back to False (defensive
        getattr — see insert_competitor_matches)."""
        from types import SimpleNamespace

        # Bypass __init__ (which builds a live Supabase client) and inject a
        # MagicMock client that captures the insert payload.
        service = object.__new__(SupabaseService)
        insert_mock = MagicMock()
        insert_mock.execute.return_value = MagicMock(data=[{}, {}, {}])
        table_mock = MagicMock()
        table_mock.insert.return_value = insert_mock
        client = MagicMock()
        client.table.return_value = table_mock
        service.client = client

        picked = _cand(salon_id=1)
        picked.is_user_selected = True
        not_picked = _cand(salon_id=2)  # is_user_selected defaults to False
        # A candidate-like object WITHOUT the attribute at all — proves the
        # getattr(..., False) default can't crash the insert.
        attrless = SimpleNamespace(
            salon_id=3, composite_score=50.0, bucket="cluster",
            counts_in_aggregates=True, similarity_scores={}, distance_km=1.0,
        )

        count = await service.insert_competitor_matches(
            report_id=999, candidates=[picked, not_picked, attrless],
        )
        assert count == 3

        # Inspect the rows passed to client.table("competitor_matches").insert.
        client.table.assert_called_with("competitor_matches")
        rows = table_mock.insert.call_args[0][0]
        assert [r["competitor_salon_id"] for r in rows] == [1, 2, 3]
        assert rows[0]["is_user_selected"] is True
        assert rows[1]["is_user_selected"] is False
        assert rows[2]["is_user_selected"] is False  # missing attr → default
