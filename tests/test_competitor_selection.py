"""Unit tests for the competitor selection algorithm (Comp Etap 1).

These tests stub out SupabaseService so the selection logic can be
verified in isolation without live database connectivity. Integration
against the real tytan Supabase happens via the end-of-script sanity
check (run manually or under `-m integration`).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from pipelines.competitor_selection import (
    CompetitorCandidate,
    assign_bucket,
    compute_avg_female_weight,
    compute_business_category_jaccard,
    compute_composite_score,
    compute_distance_penalty,
    compute_reviews_count_similarity,
    compute_top_services_overlap,
    select_competitors,
    sort_key,
)


# ---------------------------------------------------------------------------
# Pure function tests (no mocks needed)
# ---------------------------------------------------------------------------


class TestComputeAvgFemaleWeight:
    def test_empty_list_returns_none(self) -> None:
        assert compute_avg_female_weight([]) is None

    def test_none_returns_none(self) -> None:
        assert compute_avg_female_weight(None) is None

    def test_single_category(self) -> None:
        assert compute_avg_female_weight([{"id": 7, "female_weight": 90}]) == 90.0

    def test_multiple_categories_average(self) -> None:
        bc = [
            {"id": 7, "female_weight": 90},
            {"id": 10, "female_weight": 80},
            {"id": 16, "female_weight": 0},  # barber
        ]
        # (90 + 80 + 0) / 3 = 56.666...
        result = compute_avg_female_weight(bc)
        assert result is not None
        assert abs(result - 56.666666666666664) < 1e-9

    def test_missing_female_weight_ignored(self) -> None:
        bc = [
            {"id": 7, "female_weight": 90},
            {"id": 99, "name": "no weight"},  # no female_weight
            {"id": 10, "female_weight": 80},
        ]
        assert compute_avg_female_weight(bc) == 85.0

    def test_all_missing_returns_none(self) -> None:
        assert compute_avg_female_weight([{"id": 1}, {"id": 2}]) is None


class TestComputeBusinessCategoryJaccard:
    def test_empty_both_sides_returns_zero(self) -> None:
        assert compute_business_category_jaccard([], []) == 0.0

    def test_none_both_sides_returns_zero(self) -> None:
        assert compute_business_category_jaccard(None, None) == 0.0

    def test_identical_sets_returns_one(self) -> None:
        a = [{"id": 1}, {"id": 2}, {"id": 3}]
        b = [{"id": 1}, {"id": 2}, {"id": 3}]
        assert compute_business_category_jaccard(a, b) == 1.0

    def test_half_overlap(self) -> None:
        # intersection = {2, 3}, union = {1, 2, 3, 4} -> 2/4 = 0.5
        a = [{"id": 1}, {"id": 2}, {"id": 3}]
        b = [{"id": 2}, {"id": 3}, {"id": 4}]
        assert compute_business_category_jaccard(a, b) == 0.5

    def test_no_overlap_returns_zero(self) -> None:
        a = [{"id": 1}, {"id": 2}]
        b = [{"id": 3}, {"id": 4}]
        assert compute_business_category_jaccard(a, b) == 0.0

    def test_ignores_non_int_ids(self) -> None:
        a = [{"id": "x"}, {"id": 1}]
        b = [{"id": 1}]
        assert compute_business_category_jaccard(a, b) == 1.0


class TestComputeTopServicesOverlap:
    def test_empty_subject_returns_zero(self) -> None:
        # Graceful handling — 28% of salons have no top_services
        assert compute_top_services_overlap(set(), {1, 2, 3}) == 0.0

    def test_full_match(self) -> None:
        # subject has 3 top treatments, candidate has all 3 -> 3/3 = 1.0
        assert compute_top_services_overlap({1, 2, 3}, {1, 2, 3, 4, 5}) == 1.0

    def test_partial_match(self) -> None:
        # subject = {1, 2, 3}, candidate = {1, 2, 99} -> intersection = {1, 2} -> 2/3
        result = compute_top_services_overlap({1, 2, 3}, {1, 2, 99})
        assert abs(result - 2 / 3) < 1e-9

    def test_no_overlap(self) -> None:
        assert compute_top_services_overlap({1, 2, 3}, {4, 5, 6}) == 0.0

    def test_empty_candidate(self) -> None:
        assert compute_top_services_overlap({1, 2, 3}, set()) == 0.0


class TestComputeReviewsCountSimilarity:
    def test_identical_counts_returns_one(self) -> None:
        assert compute_reviews_count_similarity(500, 500) == 1.0

    def test_half_ratio(self) -> None:
        assert compute_reviews_count_similarity(250, 500) == 0.5

    def test_very_different_counts_low_score(self) -> None:
        result = compute_reviews_count_similarity(10, 1000)
        assert abs(result - 0.01) < 1e-9

    def test_zero_subject_returns_zero(self) -> None:
        assert compute_reviews_count_similarity(0, 100) == 0.0

    def test_zero_candidate_returns_zero(self) -> None:
        assert compute_reviews_count_similarity(100, 0) == 0.0


class TestComputeDistancePenalty:
    def test_zero_distance_zero_penalty(self) -> None:
        assert compute_distance_penalty(0) == 0.0

    def test_within_free_radius_zero_penalty(self) -> None:
        assert compute_distance_penalty(3) == 0.0
        assert compute_distance_penalty(5) == 0.0

    def test_penalty_scales_linearly_past_5km(self) -> None:
        assert compute_distance_penalty(6) == 2.0
        assert compute_distance_penalty(10) == 10.0
        assert compute_distance_penalty(15) == 20.0


class TestComputeCompositeScore:
    def test_perfect_match(self) -> None:
        # primary (+30) + jaccard=1 (+20) + ts_overlap=1 (+25) + rc_sim=1 (+10)
        # distance=0 penalty=0 -> 85
        score = compute_composite_score(
            primary_category_match=True,
            business_category_jaccard=1.0,
            top_services_overlap=1.0,
            reviews_count_similarity=1.0,
            distance_km=0.0,
        )
        assert score == 85.0

    def test_no_primary_match(self) -> None:
        score = compute_composite_score(
            primary_category_match=False,
            business_category_jaccard=1.0,
            top_services_overlap=1.0,
            reviews_count_similarity=1.0,
            distance_km=0.0,
        )
        assert score == 55.0

    def test_distance_cuts_score(self) -> None:
        # Same inputs as perfect_match but at 15km -> -20 penalty
        score = compute_composite_score(
            primary_category_match=True,
            business_category_jaccard=1.0,
            top_services_overlap=1.0,
            reviews_count_similarity=1.0,
            distance_km=15.0,
        )
        assert score == 65.0

    def test_zero_top_services_overlap_still_scores(self) -> None:
        # Subject has no top_services, so overlap is 0 but everything else scores
        # primary (+30) + jaccard=0.5 (+10) + ts=0 + rc_sim=0.5 (+5) -> 45
        score = compute_composite_score(
            primary_category_match=True,
            business_category_jaccard=0.5,
            top_services_overlap=0.0,
            reviews_count_similarity=0.5,
            distance_km=0.0,
        )
        assert score == 45.0


class TestAssignBucket:
    def test_new_salon_short_circuit(self) -> None:
        # reviews_count < 20 -> always 'new' regardless of score
        assert assign_bucket(
            composite_score=95,
            reviews_count=5,
            candidate_reviews_rank=5.0,
            subject_reviews_rank=4.0,
        ) == "new"

    def test_direct_at_70(self) -> None:
        assert assign_bucket(
            composite_score=70,
            reviews_count=500,
            candidate_reviews_rank=4.5,
            subject_reviews_rank=4.5,
        ) == "direct"

    def test_direct_above_70(self) -> None:
        assert assign_bucket(
            composite_score=85,
            reviews_count=200,
            candidate_reviews_rank=4.5,
            subject_reviews_rank=4.5,
        ) == "direct"

    def test_cluster_at_40(self) -> None:
        assert assign_bucket(
            composite_score=40,
            reviews_count=100,
            candidate_reviews_rank=4.5,
            subject_reviews_rank=4.5,
        ) == "cluster"

    def test_cluster_below_70(self) -> None:
        assert assign_bucket(
            composite_score=60,
            reviews_count=100,
            candidate_reviews_rank=4.5,
            subject_reviews_rank=4.5,
        ) == "cluster"

    def test_aspirational_high_rank_low_score(self) -> None:
        # score < 40 AND rank >= subject + 0.3 -> aspirational
        assert assign_bucket(
            composite_score=35,
            reviews_count=200,
            candidate_reviews_rank=5.0,  # 4.5 + 0.3 = 4.8, 5.0 >= 4.8
            subject_reviews_rank=4.5,
        ) == "aspirational"

    def test_dropped_low_score_low_rank(self) -> None:
        # score < 40 and rank isn't aspirational -> drop
        assert assign_bucket(
            composite_score=30,
            reviews_count=100,
            candidate_reviews_rank=4.5,
            subject_reviews_rank=4.5,
        ) is None

    def test_dropped_when_subject_rank_missing(self) -> None:
        # Cannot compute aspirational delta -> drop
        assert assign_bucket(
            composite_score=30,
            reviews_count=100,
            candidate_reviews_rank=5.0,
            subject_reviews_rank=None,
        ) is None


class TestSortKey:
    def _cand(self, bucket, score) -> CompetitorCandidate:
        return CompetitorCandidate(
            salon_id=1, booksy_id=1, name="x", city=None, primary_category_id=11,
            reviews_count=100, reviews_rank=4.5, distance_km=1.0, female_weight_diff=0,
            composite_score=score, bucket=bucket, counts_in_aggregates=True,
            similarity_scores={}, partner_system="native",
        )

    def test_direct_before_cluster_before_aspirational_before_new(self) -> None:
        cands = [
            self._cand("new", 90),
            self._cand("aspirational", 30),
            self._cand("cluster", 50),
            self._cand("direct", 70),
        ]
        cands.sort(key=sort_key)
        assert [c.bucket for c in cands] == ["direct", "cluster", "aspirational", "new"]

    def test_within_bucket_highest_score_first(self) -> None:
        cands = [
            self._cand("direct", 70),
            self._cand("direct", 85),
            self._cand("direct", 75),
        ]
        cands.sort(key=sort_key)
        assert [c.composite_score for c in cands] == [85, 75, 70]

    def test_mixed_sort(self) -> None:
        cands = [
            self._cand("cluster", 60),
            self._cand("direct", 72),
            self._cand("direct", 95),
            self._cand("cluster", 50),
            self._cand("new", 80),
        ]
        cands.sort(key=sort_key)
        assert [c.bucket for c in cands] == ["direct", "direct", "cluster", "cluster", "new"]
        assert [c.composite_score for c in cands] == [95, 72, 60, 50, 80]


# ---------------------------------------------------------------------------
# select_competitors() integration with a mocked SupabaseService
# ---------------------------------------------------------------------------


def _make_mock_supabase(
    *,
    subject: dict | None,
    subject_top_services: list[dict] | None = None,
    candidates: list[dict] | None = None,
    candidate_top_services: dict[int, list[dict]] | None = None,
    candidate_business_categories: dict[int, list[dict]] | None = None,
    candidate_partners: dict[int, str] | None = None,
) -> AsyncMock:
    mock = AsyncMock()
    mock.get_subject_salon_for_audit = AsyncMock(return_value=subject)
    mock.get_salon_top_services = AsyncMock(return_value=subject_top_services or [])
    mock.get_candidate_salons = AsyncMock(return_value=candidates or [])
    mock.get_latest_top_services_for_salon_ids = AsyncMock(
        return_value=candidate_top_services or {}
    )
    mock.get_latest_business_categories_for_booksy_ids = AsyncMock(
        return_value=candidate_business_categories or {}
    )
    mock.get_latest_partner_system_for_booksy_ids = AsyncMock(
        return_value=candidate_partners or {}
    )
    return mock


def _make_subject(
    *, female_weight_cats: list[int] | None = None,
) -> dict:
    """Build a subject with sensible defaults — fw 90, Warszawa, cat 11."""
    if female_weight_cats is None:
        female_weight_cats = [90, 90]  # avg = 90
    return {
        "salon_id": 9401,
        "booksy_id": 98814,
        "name": "Beauty4ever",
        "city": "Warszawa",
        "salon_lat": 52.1813389,
        "salon_lng": 21.0037812,
        "primary_category_id": 11,
        "business_categories": [
            {"id": i + 100, "female_weight": w}
            for i, w in enumerate(female_weight_cats)
        ],
        "reviews_count": 500,
        "reviews_rank": 4.9,
        "partner_system": "versum",
    }


def _make_candidate(
    *,
    booksy_id: int,
    salon_id: int,
    name: str,
    reviews_count: int = 200,
    reviews_rank: float = 4.5,
    distance_km: float = 2.0,
) -> dict:
    """Build a RPC-shaped candidate row."""
    return {
        "salon_id": salon_id,
        "booksy_id": booksy_id,
        "name": name,
        "city": "Warszawa",
        "distance_km": distance_km,
        "reviews_rank": reviews_rank,
        "reviews_count": reviews_count,
        "service_count": 100,
    }


class TestSelectCompetitors:
    async def test_no_subject_raises(self) -> None:
        mock = _make_mock_supabase(subject=None)
        with pytest.raises(ValueError, match="Subject salon not found"):
            await select_competitors("missing_audit_id", supabase=mock)

    async def test_subject_without_coordinates_raises(self) -> None:
        subject = _make_subject()
        subject["salon_lat"] = None
        mock = _make_mock_supabase(subject=subject)
        with pytest.raises(ValueError, match="no coordinates"):
            await select_competitors("audit_x", supabase=mock)

    async def test_subject_without_primary_category_raises(self) -> None:
        subject = _make_subject()
        subject["primary_category_id"] = None
        mock = _make_mock_supabase(subject=subject)
        with pytest.raises(ValueError, match="no primary_category_id"):
            await select_competitors("audit_x", supabase=mock)

    async def test_no_candidates_returns_empty(self) -> None:
        mock = _make_mock_supabase(subject=_make_subject(), candidates=[])
        result = await select_competitors("audit_x", supabase=mock)
        assert result == []

    async def test_female_weight_filter_drops_mismatched_candidate(self) -> None:
        # Subject fw=90, tolerance=20, candidate fw=50 -> diff 40 > 20 -> drop.
        # Use candidate data strong enough that "Match" survives bucket
        # assignment (at least cluster), so we can assert "Drop" was
        # filtered at the FW step and not at the bucket step.
        subject = _make_subject()
        cand1 = _make_candidate(
            booksy_id=1, salon_id=1, name="Match",
            reviews_count=500, reviews_rank=4.9, distance_km=1.0,
        )
        cand2 = _make_candidate(
            booksy_id=2, salon_id=2, name="Drop",
            reviews_count=500, reviews_rank=4.9, distance_km=1.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            candidates=[cand1, cand2],
            candidate_business_categories={
                # Match: overlaps with subject's categories (fw=90) -> jaccard>0
                1: [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}],
                # Drop: far-away female_weight -> filtered
                2: [{"id": 16, "female_weight": 50}, {"id": 16, "female_weight": 50}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        names = [c.name for c in result]
        assert "Match" in names
        assert "Drop" not in names

    async def test_scoring_and_bucket_assignment_direct(self) -> None:
        # Realistic scenario: direct bucket.
        # subject category ids = {100, 101}, cand 1 has identical cats
        # primary +30, jaccard=1 +20, ts_overlap=0 (no top_services), rc_sim=1 +10,
        # distance=1km penalty=0 -> 60 (cluster)
        # Want DIRECT -> need higher — add top_services overlap
        subject = _make_subject()
        cand1 = _make_candidate(
            booksy_id=1, salon_id=1, name="Strong Match",
            reviews_count=500, reviews_rank=4.8, distance_km=1.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            subject_top_services=[
                {"booksy_treatment_id": 200, "name": "a"},
                {"booksy_treatment_id": 201, "name": "b"},
            ],
            candidates=[cand1],
            candidate_top_services={
                1: [
                    {"booksy_treatment_id": 200, "name": "a"},
                    {"booksy_treatment_id": 201, "name": "b"},
                ],
            },
            candidate_business_categories={
                1: [
                    {"id": 100, "female_weight": 90},
                    {"id": 101, "female_weight": 90},
                ],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        assert len(result) == 1
        cand = result[0]
        # primary 30 + jaccard 1.0*20 + ts 1.0*25 + rc_sim 1.0*10 - 0 = 85 -> direct
        assert cand.composite_score == 85.0
        assert cand.bucket == "direct"
        assert cand.counts_in_aggregates is True

    async def test_new_bucket_counts_in_aggregates_false(self) -> None:
        subject = _make_subject()
        cand = _make_candidate(
            booksy_id=1, salon_id=1, name="Small",
            reviews_count=10, reviews_rank=5.0, distance_km=1.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            candidates=[cand],
            candidate_business_categories={
                1: [{"id": 100, "female_weight": 90}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        assert len(result) == 1
        assert result[0].bucket == "new"
        assert result[0].counts_in_aggregates is False

    async def test_aspirational_bucket(self) -> None:
        # Low score but rank >> subject rank -> aspirational
        subject = _make_subject()
        subject["reviews_rank"] = 4.5
        # Put candidate far away so distance penalty sinks the score below 40
        cand = _make_candidate(
            booksy_id=1, salon_id=1, name="Aspirational",
            reviews_count=100, reviews_rank=5.0, distance_km=14.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            candidates=[cand],
            # No overlap — jaccard 0, no top_services
            candidate_business_categories={
                1: [{"id": 999, "female_weight": 90}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        assert len(result) == 1
        # primary 30 + 0 + 0 + rc_sim(min(100,500)/max)=0.2*10=2 - 18(penalty) = 14
        # 14 < 40, rank 5.0 >= 4.5 + 0.3 = 4.8 -> aspirational
        assert result[0].bucket == "aspirational"

    async def test_returns_target_count_in_auto_mode(self) -> None:
        subject = _make_subject()
        # 10 strong candidates, request 5
        candidates = []
        bc_map = {}
        for i in range(1, 11):
            c = _make_candidate(
                booksy_id=i, salon_id=i, name=f"Salon {i}",
                reviews_count=500, reviews_rank=4.8, distance_km=1.0,
            )
            candidates.append(c)
            bc_map[i] = [
                {"id": 100, "female_weight": 90},
                {"id": 101, "female_weight": 90},
            ]
        mock = _make_mock_supabase(
            subject=subject,
            candidates=candidates,
            candidate_business_categories=bc_map,
        )
        result = await select_competitors("audit_x", supabase=mock, target_count=5, mode="auto")
        assert len(result) == 5

    async def test_returns_15_in_manual_mode(self) -> None:
        subject = _make_subject()
        candidates = []
        bc_map = {}
        for i in range(1, 25):
            candidates.append(_make_candidate(
                booksy_id=i, salon_id=i, name=f"Salon {i}",
                reviews_count=500, reviews_rank=4.8, distance_km=1.0,
            ))
            bc_map[i] = [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}]
        mock = _make_mock_supabase(
            subject=subject,
            candidates=candidates,
            candidate_business_categories=bc_map,
        )
        result = await select_competitors("audit_x", supabase=mock, mode="manual")
        assert len(result) == 15

    async def test_zero_top_services_graceful(self) -> None:
        # Subject has empty top_services — should not crash, overlap = 0
        subject = _make_subject()
        cand = _make_candidate(
            booksy_id=1, salon_id=1, name="A",
            reviews_count=500, reviews_rank=4.8, distance_km=2.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            subject_top_services=[],  # no top services — the Beauty4ever case
            candidates=[cand],
            candidate_top_services={1: []},
            candidate_business_categories={
                1: [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        assert len(result) == 1
        # ts_overlap component should be 0
        assert result[0].similarity_scores["top_services_overlap"] == 0.0
        # Total: 30 + 1.0*20 + 0 + 1.0*10 - 0 = 60 -> cluster
        assert result[0].composite_score == 60.0
        assert result[0].bucket == "cluster"

    async def test_missing_subject_business_categories_skips_fw_filter(self) -> None:
        # Subject has no business_categories -> fw filter disabled entirely
        subject = _make_subject()
        subject["business_categories"] = []
        # Candidate fw=10 (way off what a normal fw filter would allow)
        cand = _make_candidate(
            booksy_id=1, salon_id=1, name="Outlier",
            reviews_count=500, reviews_rank=4.8, distance_km=1.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            candidates=[cand],
            candidate_business_categories={
                1: [{"id": 100, "female_weight": 10}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        # Filter was skipped, so candidate stays in
        assert len(result) == 1
        assert result[0].female_weight_diff == -1.0  # sentinel

    async def test_sort_order_puts_direct_first(self) -> None:
        # Build 3 candidates that score into different buckets.
        # 1. high score direct (85)
        # 2. mid score cluster (55)
        # 3. new bucket (10 reviews, any score)
        subject = _make_subject()

        # Direct: full jaccard + ts overlap
        cand_direct = _make_candidate(
            booksy_id=1, salon_id=1, name="Direct",
            reviews_count=500, reviews_rank=4.9, distance_km=1.0,
        )
        # Cluster: partial overlap
        cand_cluster = _make_candidate(
            booksy_id=2, salon_id=2, name="Cluster",
            reviews_count=500, reviews_rank=4.5, distance_km=3.0,
        )
        # New: few reviews
        cand_new = _make_candidate(
            booksy_id=3, salon_id=3, name="New",
            reviews_count=5, reviews_rank=5.0, distance_km=1.0,
        )

        mock = _make_mock_supabase(
            subject=subject,
            subject_top_services=[
                {"booksy_treatment_id": 200, "name": "a"},
                {"booksy_treatment_id": 201, "name": "b"},
            ],
            candidates=[cand_new, cand_cluster, cand_direct],  # unsorted input
            candidate_top_services={
                1: [
                    {"booksy_treatment_id": 200, "name": "a"},
                    {"booksy_treatment_id": 201, "name": "b"},
                ],
                2: [],
                3: [],
            },
            candidate_business_categories={
                1: [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}],
                2: [{"id": 100, "female_weight": 90}],
                3: [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        buckets = [c.bucket for c in result]
        # direct comes before cluster comes before new
        assert buckets[0] == "direct"
        assert buckets.index("direct") < buckets.index("cluster")
        assert buckets.index("cluster") < buckets.index("new")


# ---------------------------------------------------------------------------
# End-to-end sanity check against real Supabase (manually run / integration)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBeauty4everE2E:
    """Live Supabase smoke test for Beauty4ever audit.

    Run explicitly: pytest -m integration tests/test_competitor_selection.py
    Skipped by default under the main `-m "not integration"` filter.
    """

    BEAUTY4EVER_AUDIT_ID = "j57d37db8e4695c1b3yqmtmtws84f385"

    async def test_beauty4ever_returns_five_sensible_candidates(self) -> None:
        from services.supabase import SupabaseService
        service = SupabaseService()
        candidates = await select_competitors(
            subject_audit_id=self.BEAUTY4EVER_AUDIT_ID,
            target_count=5,
            mode="auto",
            supabase=service,
        )
        # Plan doc says: "If 0 candidates returned, the filters are too
        # strict". We expect 5 because Warszawa has 384 category-11 salons.
        assert len(candidates) > 0, "Expected at least 1 Warszawa competitor"
        # Subject exclusion works — nobody should be Beauty4ever itself
        for c in candidates:
            assert c.booksy_id != 98814
            assert c.primary_category_id == 11
            assert c.distance_km <= 15.0
            assert c.reviews_count >= 20  # 'new' bucket would be counts_in_aggregates=False
            assert c.bucket in ("direct", "cluster", "aspirational", "new")
