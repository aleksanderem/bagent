"""TDD tests for S0064 promote scope gates (quick task 260612-plk).

Audit S0064 found that the cosine-only semantic promote path pulls in
scope-mismatched samples (Botoks 1 vs 3 okolice 0.878, damskie vs
męskie 0.888, manicure vs pedicure 0.721 — all above the 0.65 promote
threshold), and that the tier-1 subject median is the upper-median for
even-count price lists.

Covers:
  t1 — tier-1 subject median must be interpolated (statistics.median),
       not upper-median, for even-count subject price lists.
  t2 — tier-variant promote path: duration-bucket mismatches (>1 bucket
       apart) are filtered before the >=3 strong / >=2 salons gate.
  t3 — LLM pair verification on the promote path is fail-closed:
       (a) all candidates rejected → demote to subject_only;
       (b) verify raises → demote, no exception propagation.
  t4 — tier-variant promote path applies a cross-brand gate: subject
       with a recognized brand marker only accepts same-marker samples.
  t5 — happy-path control: compatible duration + no brand conflict +
       LLM approve-all → promotion still happens (anty over-blocking).

All LLM / RPC calls are mocked — zero live API traffic.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from pipelines.competitor_analysis import (
    _compute_pricing_comparisons,
    _compute_treatment_tier_rows,
)
from pipelines.competitor_selection import CompetitorCandidate
from services.pair_verification import _normalize_pair_name


# ---------------------------------------------------------------------------
# Fixtures (pattern: tests/test_competitor_analysis.py)
# ---------------------------------------------------------------------------

_NEXT_SVC_ID = [50_000]


def _svc(
    *,
    treatment_id: int,
    price_grosze: int = 10000,
    name: str = "Usluga testowa",
    duration_minutes: int | None = None,
    variant_id: int | None = None,
) -> dict:
    _NEXT_SVC_ID[0] += 1
    return {
        "id": _NEXT_SVC_ID[0],
        "booksy_treatment_id": treatment_id,
        "variant_id": variant_id,
        "price_grosze": price_grosze,
        "name": name,
        "treatment_name": name,
        "is_active": True,
        "treatment_parent_id": None,
        "is_from_price": False,
        "duration_minutes": duration_minutes,
    }


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


def _mock_supabase() -> AsyncMock:
    """Stub SupabaseService — no centroids, no embeddings, no sub-variants."""
    service = AsyncMock()
    service.get_variant_centroids.return_value = {}
    service.get_service_embeddings.return_value = {}
    service.get_sub_variants_for_services.return_value = {}
    return service


_NEXT_SEM_ID = [90_000]


def _sem(
    *,
    salon_id: int,
    name: str,
    price_grosze: int = 10000,
    similarity: float = 0.80,
    duration_minutes: int | None = 60,
) -> dict:
    """Sample shape returned by gather_market_context_samples."""
    _NEXT_SEM_ID[0] += 1
    return {
        "salon_id": salon_id,
        "salon_name": f"Salon{salon_id}",
        "booksy_id": salon_id + 1000,
        "service_id": _NEXT_SEM_ID[0],
        "service_name": name,
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "relation": "semantic_match",
        "similarity": similarity,
    }


def _gather_mock(samples: list[dict]):
    """Async stand-in for services.market_context.gather_market_context_samples.

    Returns fresh copies on every call — the pipeline annotates sample
    dicts in-place and both promote paths (tier-variant + tier-1) consume
    the same fixture.
    """

    async def _fake(*args, **kwargs):  # noqa: ANN002, ANN003
        return [dict(s) for s in samples]

    return _fake


def _verify_mock(is_comparable: bool):
    """Async stand-in for verify_service_pairs with a uniform verdict."""

    async def _fake(*, subject_service_name, candidate_competitor_names, **kwargs):  # noqa: ANN001, ANN003
        return {
            _normalize_pair_name(n): {
                "is_comparable": is_comparable,
                "confidence": 0.95,
                "reasoning": "mock",
                "rejection_reason": None if is_comparable else "different scope",
                "from_cache": False,
                "cached_id": None,
            }
            for n in candidate_competitor_names
        }

    return _fake


async def _verify_raises(**kwargs):  # noqa: ANN003
    raise RuntimeError("LLM timeout (mock)")


_LLM_SENTINEL = object()  # non-None so the fail-closed gate doesn't trip


def _variant_rows(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r.get("comparison_tier") == "variant"]


# ---------------------------------------------------------------------------
# t1 — FIX 3: tier-1 subject median interpolated for even-count lists
# ---------------------------------------------------------------------------


class TestTier1MedianInterpolated:
    @pytest.mark.asyncio
    async def test_even_count_subject_median_is_interpolated(self) -> None:
        # Two subject services in one tid: 10000 + 20000 groszy.
        # Interpolated median = 15000.0; the old upper-median bug gave 20000.
        # supabase=None and llm_client=None skip the LLM verify and the
        # semantic promote path entirely — the median always computes.
        subject_data = {
            "services": [
                _svc(treatment_id=5, price_grosze=10000,
                     name="Manicure hybrydowy", duration_minutes=60),
                _svc(treatment_id=5, price_grosze=20000,
                     name="Manicure hybrydowy premium", duration_minutes=60),
            ],
        }
        aligned = [
            (_cand(salon_id=1), {
                "services": [_svc(treatment_id=5, price_grosze=12000,
                                  name="Manicure hybrydowy", duration_minutes=60)],
                "scrape": {"salon_name": "Cand1"}, "salon_id": 1,
            }),
            (_cand(salon_id=2), {
                "services": [_svc(treatment_id=5, price_grosze=15000,
                                  name="Manicure hybrydowy", duration_minutes=60)],
                "scrape": {"salon_name": "Cand2"}, "salon_id": 2,
            }),
            (_cand(salon_id=3), {
                "services": [_svc(treatment_id=5, price_grosze=18000,
                                  name="Manicure hybrydowy", duration_minutes=60)],
                "scrape": {"salon_name": "Cand3"}, "salon_id": 3,
            }),
        ]
        rows = await _compute_treatment_tier_rows(
            1, subject_data, aligned,
            supabase=None, llm_client=None,
        )
        treatment_rows = [r for r in rows if r["comparison_tier"] == "treatment"]
        assert len(treatment_rows) == 1
        row = treatment_rows[0]
        # Interpolated median of [10000, 20000] — NOT the upper element.
        assert row["subject_price_grosze"] == 15000
        # PLN/min median follows the same rule: [166.67, 333.33] → 250.0.
        assert row["subject_price_per_min_grosze"] == 250.0


# ---------------------------------------------------------------------------
# t2 — FIX 1a: duration-bucket prefilter on the tier-variant promote path
# ---------------------------------------------------------------------------


class TestTierVariantDurationGate:
    @pytest.mark.asyncio
    async def test_duration_bucket_mismatch_blocks_promotion(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Subject: 120 min (long). Samples: 15 min (short) — bucket distance
        # 2 (> 1) → all samples filtered → no promotion → subject_only row
        # keeps related_samples.
        samples = [
            _sem(salon_id=1, name="Masaz relaksacyjny krotki",
                 price_grosze=5000, similarity=0.85, duration_minutes=15),
            _sem(salon_id=2, name="Masaz relaksacyjny mini",
                 price_grosze=5500, similarity=0.80, duration_minutes=15),
            _sem(salon_id=3, name="Masaz relaksacyjny express",
                 price_grosze=6000, similarity=0.75, duration_minutes=15),
        ]
        monkeypatch.setattr(
            "services.market_context.gather_market_context_samples",
            _gather_mock(samples),
        )
        monkeypatch.setattr(
            "pipelines.competitor_analysis.verify_service_pairs",
            _verify_mock(True),
        )
        subject_data = {
            "services": [_svc(
                treatment_id=7, price_grosze=20000,
                name="Masaz relaksacyjny calego ciala",
                duration_minutes=120, variant_id=70,
            )],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
            llm_client=_LLM_SENTINEL,
        )
        variant_rows = _variant_rows(result)
        assert len(variant_rows) == 1
        row = variant_rows[0]
        assert row["recommended_action"] == "subject_only"
        assert row["sample_size"] == 0
        assert row["market_median_grosze"] is None
        assert row["deviation_pct"] is None
        # related_samples preserved for soft context.
        assert len(row["related_samples"]) == 3


# ---------------------------------------------------------------------------
# t3 — FIX 1b: LLM pair verification is fail-closed on the promote path
# ---------------------------------------------------------------------------


class TestPromoteLLMFailClosed:
    def _fixture(self) -> tuple[dict, list, list[dict]]:
        # Duration-compatible samples (would promote without LLM gate).
        samples = [
            _sem(salon_id=1, name="Masaz relaksacyjny plecow",
                 price_grosze=9000, similarity=0.85, duration_minutes=60),
            _sem(salon_id=2, name="Masaz relaksacyjny plecy kark",
                 price_grosze=10000, similarity=0.80, duration_minutes=60),
            _sem(salon_id=3, name="Masaz relaksacyjny klasyczny",
                 price_grosze=11000, similarity=0.75, duration_minutes=60),
        ]
        subject_data = {
            "services": [_svc(
                treatment_id=7, price_grosze=10000,
                name="Masaz relaksacyjny plecow",
                duration_minutes=60, variant_id=70,
            )],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        return subject_data, aligned, samples

    @pytest.mark.asyncio
    async def test_all_rejected_demotes_to_subject_only(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subject_data, aligned, samples = self._fixture()
        monkeypatch.setattr(
            "services.market_context.gather_market_context_samples",
            _gather_mock(samples),
        )
        monkeypatch.setattr(
            "pipelines.competitor_analysis.verify_service_pairs",
            _verify_mock(False),
        )
        result = await _compute_pricing_comparisons(
            _mock_supabase(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
            llm_client=_LLM_SENTINEL,
        )
        variant_rows = _variant_rows(result)
        assert len(variant_rows) == 1
        row = variant_rows[0]
        assert row["recommended_action"] == "subject_only"
        assert row["sample_size"] == 0
        assert len(row["related_samples"]) == 3

    @pytest.mark.asyncio
    async def test_verify_exception_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subject_data, aligned, samples = self._fixture()
        monkeypatch.setattr(
            "services.market_context.gather_market_context_samples",
            _gather_mock(samples),
        )
        monkeypatch.setattr(
            "pipelines.competitor_analysis.verify_service_pairs",
            _verify_raises,
        )
        # Must NOT propagate the exception — fail-closed = no promotion.
        result = await _compute_pricing_comparisons(
            _mock_supabase(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
            llm_client=_LLM_SENTINEL,
        )
        variant_rows = _variant_rows(result)
        assert len(variant_rows) == 1
        row = variant_rows[0]
        assert row["recommended_action"] == "subject_only"
        assert row["sample_size"] == 0
        assert len(row["related_samples"]) == 3


# ---------------------------------------------------------------------------
# t4 — FIX 2: cross-brand gate on the tier-variant promote path
# ---------------------------------------------------------------------------


class TestTierVariantBrandGate:
    @pytest.mark.asyncio
    async def test_cross_brand_samples_block_promotion(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Subject carries brand marker "thunder"; every sample carries
        # marker "onda" → brand gate drops all → fewer than 3 strong →
        # demote to subject_only.
        samples = [
            _sem(salon_id=1, name="Onda depilacja pachy",
                 price_grosze=15000, similarity=0.88, duration_minutes=30),
            _sem(salon_id=2, name="Onda depilacja bikini",
                 price_grosze=16000, similarity=0.82, duration_minutes=30),
            _sem(salon_id=3, name="Onda depilacja nogi",
                 price_grosze=17000, similarity=0.78, duration_minutes=30),
        ]
        monkeypatch.setattr(
            "services.market_context.gather_market_context_samples",
            _gather_mock(samples),
        )
        monkeypatch.setattr(
            "pipelines.competitor_analysis.verify_service_pairs",
            _verify_mock(True),
        )
        subject_data = {
            "services": [_svc(
                treatment_id=9, price_grosze=14000,
                name="Thunder depilacja pachy",
                duration_minutes=30, variant_id=90,
            )],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
            llm_client=_LLM_SENTINEL,
        )
        variant_rows = _variant_rows(result)
        assert len(variant_rows) == 1
        row = variant_rows[0]
        assert row["recommended_action"] == "subject_only"
        assert row["sample_size"] == 0
        assert len(row["related_samples"]) == 3


# ---------------------------------------------------------------------------
# t5 — control: happy path still promotes (guards against over-blocking)
# ---------------------------------------------------------------------------


class TestPromoteHappyPath:
    @pytest.mark.asyncio
    async def test_compatible_samples_still_promote(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Same duration bucket, no brand markers, LLM approve-all →
        # promotion MUST happen: real samples, computed deviation.
        samples = [
            _sem(salon_id=1, name="Masaz relaksacyjny plecow",
                 price_grosze=9000, similarity=0.85, duration_minutes=60),
            _sem(salon_id=2, name="Masaz relaksacyjny plecy kark",
                 price_grosze=10000, similarity=0.80, duration_minutes=60),
            _sem(salon_id=3, name="Masaz relaksacyjny klasyczny",
                 price_grosze=11000, similarity=0.75, duration_minutes=60),
        ]
        monkeypatch.setattr(
            "services.market_context.gather_market_context_samples",
            _gather_mock(samples),
        )
        monkeypatch.setattr(
            "pipelines.competitor_analysis.verify_service_pairs",
            _verify_mock(True),
        )
        subject_data = {
            "services": [_svc(
                treatment_id=7, price_grosze=10000,
                name="Masaz relaksacyjny plecow",
                duration_minutes=60, variant_id=70,
            )],
        }
        aligned = [
            (_cand(salon_id=1), {"services": [], "scrape": {"salon_name": "Cand1"}, "salon_id": 1}),
            (_cand(salon_id=2), {"services": [], "scrape": {"salon_name": "Cand2"}, "salon_id": 2}),
            (_cand(salon_id=3), {"services": [], "scrape": {"salon_name": "Cand3"}, "salon_id": 3}),
        ]
        result = await _compute_pricing_comparisons(
            _mock_supabase(), report_id=1,
            subject_data=subject_data, aligned_competitors=aligned,
            llm_client=_LLM_SENTINEL,
        )
        variant_rows = _variant_rows(result)
        assert len(variant_rows) == 1
        row = variant_rows[0]
        # Promotion happened — direct samples drive a real comparison.
        assert row["sample_size"] == 3
        assert row["market_median_grosze"] == 10000
        assert row["deviation_pct"] == 0.0
        assert row["recommended_action"] == "hold"
