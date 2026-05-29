"""Unit tests for the competitor selection algorithm (Comp Etap 1).

These tests stub out SupabaseService so the selection logic can be
verified in isolation without live database connectivity. Integration
against the real tytan Supabase happens via the end-of-script sanity
check (run manually or under `-m integration`).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import numpy as np
import pytest

from collections import Counter

import pipelines.competitor_selection as selection_mod
from pipelines.competitor_selection import (
    CompetitorCandidate,
    assign_bucket,
    assign_bucket_v2,
    compute_avg_female_weight,
    compute_business_category_jaccard,
    compute_composite_score,
    compute_composite_score_v2,
    compute_distance_penalty,
    compute_profile_overlap_sim,
    compute_reviews_count_similarity,
    compute_top_services_overlap,
    profile_atoms_from_services,
    select_competitors,
    sort_key,
)
from services.focus_score import SalonFocusBundle


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


class TestProfileAtomsFromServices:
    """Deterministic atom extraction from a service list."""

    def test_empty_list_returns_empty_counter(self) -> None:
        atoms = profile_atoms_from_services([])
        assert atoms == Counter()

    def test_short_or_empty_names_skipped(self) -> None:
        atoms = profile_atoms_from_services([
            {"name": ""},
            {"name": "ab"},  # < 3 chars
            {"name": None},
            {"name": "   "},
        ])
        assert atoms == Counter()

    def test_multi_area_service_explodes_into_unitary_atoms(self) -> None:
        # "Depilacja laserowa - kark + szyja + dekolt" — extract_body_areas
        # returns frozenset of 3 areas, one atom per area, method=laser.
        atoms = profile_atoms_from_services([
            {"name": "Depilacja laserowa - kark + szyja + dekolt"},
        ])
        # method_marker maps "laser" from "Depilacja laserowa"
        assert atoms[("laser", "kark_szyja")] == 1
        assert atoms[("laser", "dekolt")] == 1

    def test_no_body_area_emits_generic_area_atom(self) -> None:
        # Generic service ("Mezoterapia igłowa") has no body area marker —
        # we emit a single (method, "") atom so the service still counts.
        atoms = profile_atoms_from_services([
            {"name": "Mezoterapia igłowa"},
        ])
        assert atoms[("mezoterapia", "")] == 1

    def test_count_accumulates_across_services(self) -> None:
        atoms = profile_atoms_from_services([
            {"name": "Depilacja laserowa - twarz"},
            {"name": "Laser - twarz pełna"},  # second laser+twarz service
            {"name": "Depilacja laserowa - pachy"},
        ])
        assert atoms[("laser", "twarz")] >= 2
        assert atoms[("laser", "pachy")] == 1


class TestComputeProfileOverlapSim:
    """Weighted recall of subject atoms in candidate portfolio."""

    def test_empty_subject_returns_zero(self) -> None:
        cand = Counter({("laser", "twarz"): 5})
        assert compute_profile_overlap_sim(Counter(), cand) == 0.0

    def test_empty_candidate_returns_zero(self) -> None:
        subj = Counter({("laser", "twarz"): 5})
        assert compute_profile_overlap_sim(subj, Counter()) == 0.0

    def test_full_overlap_returns_one(self) -> None:
        subj = Counter({("laser", "twarz"): 10, ("rf", "twarz"): 5})
        cand = Counter({("laser", "twarz"): 1, ("rf", "twarz"): 1})
        # Candidate has both atoms (counts don't matter for recall) → 1.0
        assert compute_profile_overlap_sim(subj, cand) == 1.0

    def test_partial_overlap_weighted_by_subject_count(self) -> None:
        # Subject: laser/twarz=10, rf/twarz=5  (total=15)
        # Candidate covers ONLY laser/twarz → covered=10/15 ≈ 0.667
        subj = Counter({("laser", "twarz"): 10, ("rf", "twarz"): 5})
        cand = Counter({("laser", "twarz"): 1})
        result = compute_profile_overlap_sim(subj, cand)
        assert abs(result - 10 / 15) < 1e-6

    def test_no_overlap_returns_zero(self) -> None:
        subj = Counter({("laser", "twarz"): 10})
        cand = Counter({("manicure", ""): 5})
        assert compute_profile_overlap_sim(subj, cand) == 0.0


class TestComputeCompositeScoreV2:
    """5-axis composite scoring (v2 + profile_overlap_sim)."""

    def test_perfect_match_5_axes(self) -> None:
        # All sims = 1.0, no distance penalty: 30+25+20+20+10 = 105
        score = compute_composite_score_v2(
            focus_tid_sim=1.0,
            focus_var_sim=1.0,
            portfolio_embedding_sim=1.0,
            reviews_count_similarity=1.0,
            distance_km=0.0,
            profile_overlap_sim=1.0,
        )
        assert score == 105.0

    def test_profile_overlap_default_zero_is_backward_compatible(self) -> None:
        # Callers that don't pass profile_overlap_sim should get the
        # previous 4-axis v2 score (sum = 80).
        old_score = compute_composite_score_v2(
            focus_tid_sim=1.0,
            focus_var_sim=1.0,
            portfolio_embedding_sim=1.0,
            reviews_count_similarity=1.0,
            distance_km=0.0,
        )
        assert old_score == 80.0

    def test_profile_overlap_adds_25_at_max(self) -> None:
        # Same inputs as default test, with profile_overlap=1.0 → +25
        score = compute_composite_score_v2(
            focus_tid_sim=1.0,
            focus_var_sim=1.0,
            portfolio_embedding_sim=1.0,
            reviews_count_similarity=1.0,
            distance_km=0.0,
            profile_overlap_sim=1.0,
        )
        assert score == 105.0

    def test_profile_overlap_lifts_noisy_embedding_match(self) -> None:
        # Simulate Beauty4ever scenario: portfolio_embedding_sim is HIGH
        # (noisy positive), but profile_overlap differs sharply between two
        # candidates. The one with higher profile_overlap should rank ahead.
        weak = compute_composite_score_v2(
            focus_tid_sim=0.5,
            focus_var_sim=0.25,
            portfolio_embedding_sim=0.86,  # noise pulls this up
            reviews_count_similarity=0.5,
            distance_km=1.0,
            profile_overlap_sim=0.43,  # real signal: weak overlap
        )
        strong = compute_composite_score_v2(
            focus_tid_sim=0.5,
            focus_var_sim=0.25,
            portfolio_embedding_sim=0.86,  # same embedding noise
            reviews_count_similarity=0.5,
            distance_km=1.0,
            profile_overlap_sim=0.87,  # strong overlap signal
        )
        # Strong overlap should produce notably higher composite — the 0.44
        # gap × _W_PROFILE_OVERLAP (25) = ~11 raw points.
        assert strong > weak
        assert (strong - weak) >= 10.0  # at least the weight-times-delta


class TestAssignBucketV2ProfileOverlapGate:
    """profile_overlap_sim OR-gate added 2026-05-17 — rescues salons with
    noisy focus_tid_sim (low Booksy tid cosine) but strong (method, area)
    coverage of subject's portfolio."""

    BASE_KWARGS = dict(
        reviews_count=100,
        candidate_reviews_rank=4.8,
        subject_reviews_rank=4.9,
    )

    def test_direct_via_profile_overlap_high(self) -> None:
        # focus_tid_sim below 0.25 but profile_overlap_sim ≥ 0.7 + strong
        # portfolio embedding → still direct.
        bucket = assign_bucket_v2(
            focus_tid_sim=0.06,
            portfolio_embedding_sim=0.92,
            profile_overlap_sim=0.85,
            **self.BASE_KWARGS,
        )
        assert bucket == "direct"

    def test_direct_via_focus_tid_high(self) -> None:
        # Legacy path: focus_tid_sim ≥ 0.25 + emb ≥ 0.85, profile_overlap low.
        bucket = assign_bucket_v2(
            focus_tid_sim=0.30,
            portfolio_embedding_sim=0.90,
            profile_overlap_sim=0.1,
            **self.BASE_KWARGS,
        )
        assert bucket == "direct"

    def test_cluster_via_profile_overlap_moderate(self) -> None:
        # focus_tid_sim too low for cluster (< 0.12) but profile_overlap_sim
        # ≥ 0.4 still rescues into cluster.
        bucket = assign_bucket_v2(
            focus_tid_sim=0.05,
            portfolio_embedding_sim=0.80,
            profile_overlap_sim=0.45,
            **self.BASE_KWARGS,
        )
        assert bucket == "cluster"

    def test_dropped_when_both_signals_weak(self) -> None:
        # focus_tid below 0.12, profile_overlap below 0.4, no aspirational
        # rescue → None (drop).
        bucket = assign_bucket_v2(
            focus_tid_sim=0.05,
            portfolio_embedding_sim=0.60,
            profile_overlap_sim=0.30,
            **self.BASE_KWARGS,
        )
        assert bucket is None

    def test_profile_overlap_default_zero_preserves_legacy_behavior(self) -> None:
        # Caller without profile_overlap_sim — pre-2026-05-17 behavior.
        legacy_bucket = assign_bucket_v2(
            focus_tid_sim=0.15,
            portfolio_embedding_sim=0.80,
            **self.BASE_KWARGS,
        )
        assert legacy_bucket == "cluster"

    def test_direct_still_requires_strong_embedding(self) -> None:
        # profile_overlap_sim high but portfolio_emb_sim < 0.85 → not direct,
        # falls through to cluster (OR-gate on cluster also triggers).
        bucket = assign_bucket_v2(
            focus_tid_sim=0.05,
            portfolio_embedding_sim=0.70,
            profile_overlap_sim=0.85,
            **self.BASE_KWARGS,
        )
        # cluster because profile_overlap >= 0.4 even though direct rejected
        assert bucket == "cluster"


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
    salons_by_id: dict[int, dict] | None = None,
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

    # get_salons_by_ids backs the UNION's "force-add a pick not in scored"
    # path. Return only the requested ids that exist in salons_by_id.
    _salons_by_id = salons_by_id or {}

    async def _get_salons_by_ids(salon_ids: list[int]) -> list[dict]:
        return [_salons_by_id[sid] for sid in salon_ids if sid in _salons_by_id]

    mock.get_salons_by_ids = AsyncMock(side_effect=_get_salons_by_ids)

    # Atom-profile path (profile_overlap_sim, mig 087) — return None for the
    # subject chain head so `subject_atoms` stays empty and the whole atom
    # block is skipped. Without these stubs the AsyncMock returns truthy
    # MagicMocks that crash profile_atoms_from_services.
    mock.get_chain_head_scrape_id = AsyncMock(return_value=None)
    mock.get_chain_head_services_for_scrape = AsyncMock(return_value=[])
    mock.get_chain_head_scrape_ids_for_booksy_ids = AsyncMock(return_value={})
    mock.get_candidate_services_for_atoms = AsyncMock(return_value={})
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


@pytest.fixture(autouse=True)
def _stub_focus_bundles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the v2 focus-bundle loaders for the whole module.

    select_competitors() reaches into service.client for portfolio embeddings
    (mig 087), which the AsyncMock-based _make_mock_supabase can't provide
    offline — without this it raised AttributeError before any selection
    logic ran. We give the subject and every candidate an identical unit
    embedding + focus distribution → portfolio_emb_sim and focus_tid_sim both
    1.0, so every reviews>=20 candidate buckets as 'direct'. This isolates the
    selection logic (FW filter, cap, sort, union) from the embedding plumbing.
    Tests that assert a NON-direct bucket override these loaders themselves.
    """
    emb = np.ones(1536, dtype=np.float64)
    focus = {503: 1.0}

    def _subject_bundle(service, salon_id, booksy_id):  # noqa: ANN001
        return SalonFocusBundle(
            salon_id=salon_id, booksy_id=booksy_id,
            portfolio_embedding=emb, focus_distribution=dict(focus),
            focus_variant_distribution=dict(focus),
            service_count=10, embedded_count=10,
        )

    def _candidate_bundles(service, salon_ids):  # noqa: ANN001
        return {
            sid: SalonFocusBundle(
                salon_id=sid, booksy_id=None,
                portfolio_embedding=emb, focus_distribution=dict(focus),
                focus_variant_distribution=dict(focus),
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
        # v2 scoring (mig 087/088) is embedding/focus-driven — the composite
        # score is no longer the old fixed additive value (was 85.0). The
        # meaningful contract here: a reviews>=20 candidate with full
        # portfolio/focus overlap (forced by the focus-bundle stub) buckets
        # 'direct' and counts in market aggregates. The numeric score formula
        # is pinned by the compute_composite_score_v2 / assign_bucket_v2 unit
        # tests, not by this integration test.
        assert cand.bucket == "direct"
        assert cand.counts_in_aggregates is True
        assert cand.composite_score > 0

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

    def test_assign_bucket_v2_aspirational_branch(self) -> None:
        # Pre-mig-087 this asserted a v1 additive score path. v2 bucketing is
        # focus/embedding-driven, so we pin the documented 'aspirational'
        # contract directly on the pure function instead of echoing a runtime
        # composite score: reviews>=20, NOT direct (focus<0.25 & overlap<0.7),
        # NOT cluster (focus<0.12 & overlap<0.4), candidate rank >= subject
        # +0.3, AND portfolio_embedding_sim >= 0.70.
        assert (
            assign_bucket_v2(
                focus_tid_sim=0.0,
                portfolio_embedding_sim=0.75,
                reviews_count=100,
                candidate_reviews_rank=5.0,
                subject_reviews_rank=4.5,
                profile_overlap_sim=0.0,
            )
            == "aspirational"
        )
        # Below the emb_sim gate (0.70) the aspirational branch must NOT fire.
        assert (
            assign_bucket_v2(
                focus_tid_sim=0.0,
                portfolio_embedding_sim=0.5,
                reviews_count=100,
                candidate_reviews_rank=5.0,
                subject_reviews_rank=4.5,
                profile_overlap_sim=0.0,
            )
            is None
        )

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
        # Graceful: an empty subject top_services list must not crash selection.
        # v2 (mig 087) dropped the standalone top_services_overlap signal
        # (focus_tid_sim subsumes it), so the old composite_score==60.0 /
        # bucket=="cluster" / similarity_scores["top_services_overlap"]
        # assertions are gone — we assert the candidate survives with a valid
        # score bundle.
        assert len(result) == 1
        assert isinstance(result[0].similarity_scores, dict)
        assert result[0].composite_score > 0

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

    def test_sort_key_orders_by_bucket_then_score(self) -> None:
        # Pure-function contract of sort_key / _BUCKET_PRIORITY: bucket
        # priority (direct < cluster < aspirational < new) first, then
        # descending composite_score within a bucket. Driving this through
        # select_competitors is no longer viable because the focus-bundle
        # stub forces uniform sim → all candidates bucket 'direct'; producing
        # mixed buckets here from real candidates would require hand-derived
        # v2 embeddings, so we test the ordering contract directly.
        def _cand(name: str, bucket: str, score: float) -> CompetitorCandidate:
            return CompetitorCandidate(
                salon_id=1, booksy_id=1, name=name, city="Warszawa",
                primary_category_id=11, reviews_count=100, reviews_rank=4.8,
                distance_km=1.0, female_weight_diff=0.0, composite_score=score,
                bucket=bucket, counts_in_aggregates=(bucket != "new"),
            )

        unsorted = [
            _cand("new", "new", 99.0),
            _cand("cluster-lo", "cluster", 10.0),
            _cand("aspirational", "aspirational", 50.0),
            _cand("direct-hi", "direct", 80.0),
            _cand("direct-lo", "direct", 70.0),
            _cand("cluster-hi", "cluster", 40.0),
        ]
        ordered = [c.name for c in sorted(unsorted, key=selection_mod.sort_key)]
        assert ordered == [
            "direct-hi", "direct-lo",    # direct bucket, score desc
            "cluster-hi", "cluster-lo",  # cluster bucket, score desc
            "aspirational",
            "new",
        ]


class TestSelectCompetitorsUserPickUnion:
    """UNION of user-picked salons into the deterministic selection.

    The frontend competitor picker force-includes specific salons. This must
    be a UNION (never a filter): picks always end up in the result marked
    is_user_selected=True. Force-added picks (not in the deterministic scored
    set) must carry counts_in_aggregates=False so they don't distort market
    aggregates.
    """

    # Focus-bundle loaders are stubbed module-wide by the autouse
    # `_stub_focus_bundles` fixture (identical unit embedding → every
    # reviews>=20 candidate buckets 'direct'), so the UNION logic is tested
    # in isolation from the embedding plumbing.

    async def test_union_includes_cap_dropped_and_force_added_picks(self) -> None:
        # 5 strong candidates (salon_ids 1..5) all score into the same bucket.
        # target_count=1 => only the top one survives the cap; ids 2..5 are
        # cap-dropped. The user picks:
        #   - salon_id 3  -> in `scored` but cap-dropped (case b)
        #   - salon_id 99 -> NOT a raw candidate at all (case c, force-added)
        subject = _make_subject()
        candidates = []
        bc_map = {}
        for i in range(1, 6):
            candidates.append(_make_candidate(
                booksy_id=i, salon_id=i, name=f"Salon {i}",
                reviews_count=500, reviews_rank=4.8, distance_km=1.0,
            ))
            bc_map[i] = [
                {"id": 100, "female_weight": 90},
                {"id": 101, "female_weight": 90},
            ]
        # salon_id 99 is the force-add target: a real salons row, but it never
        # appears in the candidate pool (e.g. wrong category / outside radius).
        salons_by_id = {
            99: {
                "id": 99,
                "booksy_id": 9099,
                "name": "User Pick Outsider",
                "city": "Kraków",
                "primary_category_id": None,  # exercises subject-cat fallback
                "reviews_count": 321,
                "reviews_rank": 4.7,
                "partner_system": "versum",
            },
        }
        mock = _make_mock_supabase(
            subject=subject,
            candidates=candidates,
            candidate_business_categories=bc_map,
            salons_by_id=salons_by_id,
        )
        result = await select_competitors(
            "audit_x",
            supabase=mock,
            target_count=1,
            mode="auto",
            must_include_salon_ids=[3, 99],
        )

        by_id = {c.salon_id: c for c in result}
        # Deterministic top-1 must still be present (union only ADDS).
        assert len(result) == 3, [c.salon_id for c in result]
        # Both picks are in the result.
        assert 3 in by_id
        assert 99 in by_id
        # Both picks are flagged.
        assert by_id[3].is_user_selected is True
        assert by_id[99].is_user_selected is True

        # Case (b): cap-dropped pick was a real scored candidate, so it keeps
        # its deterministic bucket/score and counts_in_aggregates stays True.
        assert by_id[3].counts_in_aggregates is True
        assert by_id[3].composite_score > 0

        # Case (c): force-added pick must NOT count in aggregates and gets the
        # minimal-candidate shape (lowest bucket, zero score, sentinel fw).
        forced = by_id[99]
        assert forced.counts_in_aggregates is False
        assert forced.bucket == "new"
        assert forced.composite_score == 0.0
        assert forced.female_weight_diff == -1.0
        assert forced.booksy_id == 9099
        assert forced.name == "User Pick Outsider"
        # primary_category_id falls back to the subject's category (11) when
        # the salons row has none.
        assert forced.primary_category_id == 11

        # Determinism: ordering puts the deterministic survivor first, the
        # force-added pick last (appended after the cap-dropped re-add).
        assert result[0].salon_id == 1
        assert result[-1].salon_id == 99

    async def test_no_must_include_leaves_selection_unchanged(self) -> None:
        # Sanity: passing no picks behaves exactly like before — no flags set.
        subject = _make_subject()
        cand = _make_candidate(
            booksy_id=1, salon_id=1, name="A",
            reviews_count=500, reviews_rank=4.8, distance_km=1.0,
        )
        mock = _make_mock_supabase(
            subject=subject,
            candidates=[cand],
            candidate_business_categories={
                1: [{"id": 100, "female_weight": 90}, {"id": 101, "female_weight": 90}],
            },
        )
        result = await select_competitors("audit_x", supabase=mock)
        assert len(result) == 1
        assert result[0].is_user_selected is False
        mock.get_salons_by_ids.assert_not_called()

    async def test_pick_already_in_cap_is_only_flagged_not_duplicated(self) -> None:
        # A pick that is also the top deterministic candidate must be flagged
        # in-place, not appended a second time (dedup by salon_id).
        subject = _make_subject()
        candidates = []
        bc_map = {}
        for i in range(1, 4):
            candidates.append(_make_candidate(
                booksy_id=i, salon_id=i, name=f"Salon {i}",
                reviews_count=500, reviews_rank=4.8, distance_km=1.0,
            ))
            bc_map[i] = [
                {"id": 100, "female_weight": 90},
                {"id": 101, "female_weight": 90},
            ]
        mock = _make_mock_supabase(
            subject=subject,
            candidates=candidates,
            candidate_business_categories=bc_map,
        )
        # target_count=3 => all three are in the cap; pick one of them.
        result = await select_competitors(
            "audit_x",
            supabase=mock,
            target_count=3,
            mode="auto",
            must_include_salon_ids=[2],
        )
        salon_ids = [c.salon_id for c in result]
        assert salon_ids.count(2) == 1  # not duplicated
        assert len(result) == 3
        by_id = {c.salon_id: c for c in result}
        assert by_id[2].is_user_selected is True
        assert by_id[1].is_user_selected is False
        # No force-add needed since the pick was already a candidate.
        mock.get_salons_by_ids.assert_not_called()


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
