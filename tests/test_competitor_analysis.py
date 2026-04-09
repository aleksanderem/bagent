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
from unittest.mock import AsyncMock

import pytest

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
        # 4 services, 3 have description_type='M' → 75%
        services = [
            {"description_type": "M", "is_active": True},
            {"description_type": "M", "is_active": True},
            {"description_type": "M", "is_active": True},
            {"description_type": "S", "is_active": True},
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
        services = [
            {"description_type": "M", "is_active": True},
            {"description_type": "M", "is_active": False},  # excluded
            {"description_type": "S", "is_active": True},
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        # 1 of 2 active → 50%
        assert result["description_coverage"] == 50.0

    def test_avg_description_length(self) -> None:
        services = [
            {"description": "krótki opis", "is_active": True},  # 2 words
            {"description": "to jest dłuższy opis usługi", "is_active": True},  # 5 words
            {"description": None, "is_active": True},  # no description → excluded
        ]
        result = compute_content_quality_scores(services, salon_description=None)
        # avg of [2, 5] = 3.5
        assert result["avg_description_length"] == 3.5


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
        services = [
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 1000},
            {"booksy_treatment_id": 2, "is_active": False, "price_grosze": 2000},
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
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 2000, "name": "variant2"},
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 1500, "name": "variant1"},
            {"booksy_treatment_id": 1, "is_active": True, "price_grosze": 3000, "name": "variant3"},
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


def _svc(
    *,
    treatment_id: int,
    price_grosze: int | None = 10000,
    name: str = "x",
    is_active: bool = True,
) -> dict:
    return {
        "booksy_treatment_id": treatment_id,
        "price_grosze": price_grosze,
        "name": name,
        "treatment_name": name,
        "is_active": is_active,
        "treatment_parent_id": None,
        "is_from_price": False,
        "duration_minutes": None,
    }


class TestComputePricingComparisons:
    def test_empty_subject_returns_empty(self) -> None:
        subject_data = {"services": []}
        result = _compute_pricing_comparisons(report_id=1, subject_data=subject_data, aligned_competitors=[])
        assert result == []

    def test_skips_treatments_with_less_than_two_competitor_prices(self) -> None:
        subject_data = {
            "services": [_svc(treatment_id=1, price_grosze=1000, name="A")],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [_svc(treatment_id=1, price_grosze=900)]}),
        ]
        result = _compute_pricing_comparisons(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        assert result == []  # Only 1 competitor price → skip

    def test_computes_deviation_and_action(self) -> None:
        # Subject 1000, market [500, 600, 700] → median 600, deviation = +66.67% → lower
        subject_data = {
            "services": [_svc(treatment_id=5, price_grosze=1000, name="Lase")],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [_svc(treatment_id=5, price_grosze=500)]}),
            (_cand(salon_id=2), {"services": [_svc(treatment_id=5, price_grosze=600)]}),
            (_cand(salon_id=3), {"services": [_svc(treatment_id=5, price_grosze=700)]}),
        ]
        result = _compute_pricing_comparisons(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        assert len(result) == 1
        row = result[0]
        assert row["booksy_treatment_id"] == 5
        assert row["subject_price_grosze"] == 1000
        assert row["market_median_grosze"] == 600
        assert row["sample_size"] == 3
        assert abs(row["deviation_pct"] - 66.67) < 0.01
        assert row["recommended_action"] == "lower"

    def test_skips_non_counting_competitors(self) -> None:
        subject_data = {
            "services": [_svc(treatment_id=5, price_grosze=1000)],
        }
        aligned = [
            (_cand(salon_id=1, counts_in_aggregates=False), {"services": [_svc(treatment_id=5, price_grosze=500)]}),
            (_cand(salon_id=2), {"services": [_svc(treatment_id=5, price_grosze=1000)]}),
        ]
        result = _compute_pricing_comparisons(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        # Only 1 counting competitor → skip
        assert result == []


# ---------------------------------------------------------------------------
# _compute_service_gaps
# ---------------------------------------------------------------------------


class TestComputeServiceGaps:
    def test_missing_type_ranked_by_competitor_count(self) -> None:
        subject_data = {"services": []}  # Subject has nothing
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc(treatment_id=1), _svc(treatment_id=2)],
                "reviews": [],
            }),
            (_cand(salon_id=2), {
                "services": [_svc(treatment_id=1)],
                "reviews": [],
            }),
        ]
        result = _compute_service_gaps(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        missing = [r for r in result if r["gap_type"] == "missing"]
        # tid=1 has 2 competitors, tid=2 has 1 — tid=1 should be first
        assert missing[0]["booksy_treatment_id"] == 1
        assert missing[0]["competitor_count"] == 2
        assert missing[1]["booksy_treatment_id"] == 2
        assert missing[1]["competitor_count"] == 1

    def test_unique_usp_extraction(self) -> None:
        subject_data = {
            "services": [
                _svc(treatment_id=100, price_grosze=5000, name="Unique1"),
                _svc(treatment_id=200, price_grosze=3000, name="SharedWithCompetitor"),
            ],
        }
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc(treatment_id=200, price_grosze=2500)],
                "reviews": [],
            }),
        ]
        result = _compute_service_gaps(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        uniques = [r for r in result if r["gap_type"] == "unique_usp"]
        assert len(uniques) == 1
        assert uniques[0]["booksy_treatment_id"] == 100
        assert uniques[0]["competitor_count"] == 0

    def test_missing_capped_at_10(self) -> None:
        subject_data = {"services": []}
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc(treatment_id=i) for i in range(1, 16)],
                "reviews": [],
            }),
        ]
        result = _compute_service_gaps(report_id=1, subject_data=subject_data, aligned_competitors=aligned)
        missing = [r for r in result if r["gap_type"] == "missing"]
        assert len(missing) == 10


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
async def test_compute_competitor_analysis_end_to_end_with_mocks() -> None:
    """Integration test — verify orchestration calls every expected write."""
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
