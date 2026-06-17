"""Unit tests for Comp Etap 5 competitor synthesis pipeline.

Covers:
- prompt context builder formats data correctly with Polish labels
- tool call parser extracts narrative + SWOT + recommendations
- sanitizer drops sourceDataPoints that reference unknown IDs
- deterministic fallback when AI fails
- end-to-end synthesize_competitor_insights with mocked MiniMax + Supabase
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipelines.competitor_synthesis import (
    _build_synthesis_prompt_context,
    _deterministic_fallback,
    _extract_insights_from_agent_result,
    _run_minimax_synthesis,
    _sanitize_insights,
    _sanitize_source_data_points,
    synthesize_competitor_insights,
)


# ---------------------------------------------------------------------------
# Fixtures — realistic Beauty4ever-like shapes
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_report() -> dict:
    return {
        "id": 27,
        "convex_audit_id": "j57d37db8e4695c1b3yqmtmtws84f385",
        "subject_salon_id": 500,
        "tier": "base",
        "selection_mode": "auto",
        "competitor_count": 5,
        "report_data": {},
        "status": "processing",
    }


@pytest.fixture
def sample_subject_context() -> dict:
    return {
        "salon_id": 500,
        "salon_name": "Beauty4ever",
        "salon_city": "Warszawa",
        "primary_category_name": "Salon Kosmetyczny",
        "reviews_count": 17609,
        "reviews_rank": 4.8,
        "total_services": 273,
    }


@pytest.fixture
def sample_matches() -> list[dict]:
    return [
        {
            "id": 101,
            "competitor_salon_id": 9411,
            "booksy_id": 9411,
            "salon_name": "Pięknoteka",
            "bucket": "direct",
            "distance_km": 1.2,
            "reviews_count": 4532,
            "reviews_rank": 4.7,
            "composite_score": 0.87,
            "counts_in_aggregates": True,
        },
        {
            "id": 102,
            "competitor_salon_id": 9471,
            "booksy_id": 9471,
            "salon_name": "Yasumi",
            "bucket": "direct",
            "distance_km": 2.0,
            "reviews_count": 3012,
            "reviews_rank": 4.5,
            "composite_score": 0.82,
            "counts_in_aggregates": True,
        },
        {
            "id": 103,
            "competitor_salon_id": 9500,
            "booksy_id": 9500,
            "salon_name": "Now You",
            "bucket": "cluster",
            "distance_km": 3.5,
            "reviews_count": 2100,
            "reviews_rank": 4.6,
            "composite_score": 0.76,
            "counts_in_aggregates": True,
        },
    ]


@pytest.fixture
def sample_pricing() -> list[dict]:
    return [
        {
            "id": 456,
            "booksy_treatment_id": 101,
            "treatment_name": "Endermologia",
            "subject_price_grosze": 20000,
            "market_median_grosze": 11200,
            "deviation_pct": 78.6,
            "sample_size": 4,
            "recommended_action": "lower",
        },
        {
            "id": 457,
            "booksy_treatment_id": 205,
            "treatment_name": "Manicure hybrydowy",
            "subject_price_grosze": 8000,
            "market_median_grosze": 9500,
            "deviation_pct": -15.8,
            "sample_size": 5,
            "recommended_action": "raise",
        },
    ]


@pytest.fixture
def sample_gaps() -> list[dict]:
    return [
        {
            "id": 301,
            "gap_type": "missing",
            "booksy_treatment_id": 708,
            "treatment_name": "Henna pudrowa",
            "competitor_count": 4,
            "avg_price_grosze": 12000,
            "popularity_score": 4.5,
        },
        {
            "id": 302,
            "gap_type": "unique_usp",
            "booksy_treatment_id": 901,
            "treatment_name": "Masaż bamboo",
            "competitor_count": 0,
            "avg_price_grosze": 25000,
            "popularity_score": 1.0,
        },
    ]


@pytest.fixture
def sample_dimensions() -> list[dict]:
    return [
        {
            "id": 1001,
            "dimension": "description_coverage",
            "subject_value": 23.0,
            "market_p50": 85.0,
            "subject_percentile": 15.0,
            "better_is_higher": True,
            "unit": "percent",
            "category": "content_quality",
        },
        {
            "id": 1002,
            "dimension": "total_services",
            "subject_value": 273.0,
            "market_p50": 132.0,
            "subject_percentile": 95.0,
            "better_is_higher": True,
            "unit": "count",
            "category": "portfolio",
        },
        {
            "id": 1003,
            "dimension": "reviews_count",
            "subject_value": 17609.0,
            "market_p50": 3200.0,
            "subject_percentile": 99.0,
            "better_is_higher": True,
            "unit": "count",
            "category": "social_proof",
        },
    ]


# ---------------------------------------------------------------------------
# _build_synthesis_prompt_context
# ---------------------------------------------------------------------------


class TestPromptContextBuilder:
    def test_includes_subject_name_and_city(
        self, sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "Beauty4ever" in ctx
        assert "Warszawa" in ctx
        assert "Salon Kosmetyczny" in ctx
        assert "273" in ctx  # total_services

    def test_includes_pricing_ids_for_traceability(
        self, sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "456" in ctx  # pricing comparison id
        assert "Endermologia" in ctx
        assert "78" in ctx or "+79" in ctx  # deviation percent rounded

    def test_includes_gap_ids_and_types(
        self, sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "301" in ctx  # missing gap id
        assert "302" in ctx  # unique USP id
        assert "Henna pudrowa" in ctx
        assert "Masaż bamboo" in ctx
        assert "BRAKUJĄCE USŁUGI" in ctx
        assert "UNIKALNE USP" in ctx

    def test_includes_dimensional_ids_grouped_by_category(
        self, sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "1001" in ctx  # dim id
        assert "1002" in ctx
        assert "CONTENT QUALITY" in ctx
        assert "PORTFOLIO" in ctx
        assert "SOCIAL PROOF" in ctx

    def test_includes_competitor_booksy_ids(
        self, sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "9411" in ctx  # Pięknoteka
        assert "9471" in ctx  # Yasumi
        assert "Pięknoteka" in ctx

    def test_empty_pricing_block(
        self, sample_report, sample_subject_context, sample_matches, sample_dimensions,
    ) -> None:
        ctx = _build_synthesis_prompt_context(
            report=sample_report,
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=[],
            gaps=[],
            dimensions=sample_dimensions,
        )
        assert "Brak porównań cenowych" in ctx


# ---------------------------------------------------------------------------
# _extract_insights_from_agent_result
# ---------------------------------------------------------------------------


class TestExtractInsights:
    def _mk_agent_result(self, tool_calls: list[dict]) -> SimpleNamespace:
        calls = [
            SimpleNamespace(name=c["name"], input=c["input"]) for c in tool_calls
        ]
        return SimpleNamespace(tool_calls=calls, final_text="")

    def test_single_call_extraction(self) -> None:
        agent_result = self._mk_agent_result([{
            "name": "submit_competitor_insights",
            "input": {
                "positioning_narrative": "Beauty4ever dominuje w Warszawie z 273 usługami.",
                "swot": {
                    "strengths": [
                        {"text": "Najszersze portfolio w regionie — 273 usługi",
                         "sourceDataPoints": [{"type": "dimensional_score", "id": 1002}]},
                    ],
                    "weaknesses": [
                        {"text": "Niska jakość opisów",
                         "sourceDataPoints": [{"type": "dimensional_score", "id": 1001}]},
                    ],
                    "opportunities": [
                        {"text": "Wprowadź hennę pudrową",
                         "sourceDataPoints": [{"type": "service_gap", "id": 301}]},
                    ],
                    "threats": [
                        {"text": "Za wysoka cena endermologii",
                         "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}]},
                    ],
                },
                "recommendations": [
                    {
                        "actionTitle": "Obniż cenę endermologii do 120 zł",
                        "actionDescription": "Konkurenci oferują średnio 112 zł.",
                        "category": "pricing",
                        "impact": "high",
                        "effort": "low",
                        "confidence": 0.85,
                        "sourceCompetitorIds": [9411, 9471],
                        "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
                    },
                ],
            },
        }])
        result = _extract_insights_from_agent_result(agent_result)
        assert result["positioning_narrative"].startswith("Beauty4ever")
        assert len(result["swot"]["strengths"]) == 1
        assert len(result["swot"]["weaknesses"]) == 1
        assert len(result["recommendations"]) == 1

    def test_merges_multiple_calls(self) -> None:
        agent_result = self._mk_agent_result([
            {
                "name": "submit_competitor_insights",
                "input": {
                    "positioning_narrative": "Pozycja A",
                    "swot": {
                        "strengths": [{"text": "A", "sourceDataPoints": [{"type": "dimensional_score", "id": 1}]}],
                        "weaknesses": [],
                        "opportunities": [],
                        "threats": [],
                    },
                    "recommendations": [],
                },
            },
            {
                "name": "submit_competitor_insights",
                "input": {
                    "positioning_narrative": "Ignored — first call wins",
                    "swot": {
                        "strengths": [],
                        "weaknesses": [{"text": "B", "sourceDataPoints": [{"type": "dimensional_score", "id": 2}]}],
                        "opportunities": [],
                        "threats": [],
                    },
                    "recommendations": [
                        {"actionTitle": "Do X", "actionDescription": "Because Y", "category": "pricing",
                         "impact": "high", "effort": "low", "confidence": 0.8,
                         "sourceDataPoints": [{"type": "pricing_comparison", "id": 1}]},
                    ],
                },
            },
        ])
        result = _extract_insights_from_agent_result(agent_result)
        assert result["positioning_narrative"] == "Pozycja A"
        assert len(result["swot"]["strengths"]) == 1
        assert len(result["swot"]["weaknesses"]) == 1
        assert len(result["recommendations"]) == 1

    def test_raises_when_missing_narrative(self) -> None:
        agent_result = self._mk_agent_result([{
            "name": "submit_competitor_insights",
            "input": {
                "swot": {
                    "strengths": [{"text": "x", "sourceDataPoints": [{"type": "dimensional_score", "id": 1}]}],
                    "weaknesses": [], "opportunities": [], "threats": [],
                },
                "recommendations": [{"actionTitle": "x", "actionDescription": "y", "category": "pricing",
                                     "impact": "low", "effort": "low", "confidence": 0.5,
                                     "sourceDataPoints": [{"type": "pricing_comparison", "id": 1}]}],
            },
        }])
        with pytest.raises(ValueError, match="positioning_narrative"):
            _extract_insights_from_agent_result(agent_result)

    def test_ignores_unknown_tool_calls(self) -> None:
        agent_result = self._mk_agent_result([
            {"name": "some_other_tool", "input": {"foo": "bar"}},
            {
                "name": "submit_competitor_insights",
                "input": {
                    "positioning_narrative": "OK",
                    "swot": {
                        "strengths": [{"text": "A", "sourceDataPoints": [{"type": "dimensional_score", "id": 1}]}],
                        "weaknesses": [], "opportunities": [], "threats": [],
                    },
                    "recommendations": [{
                        "actionTitle": "Do", "actionDescription": "Because",
                        "category": "pricing", "impact": "high", "effort": "low",
                        "confidence": 0.7,
                        "sourceDataPoints": [{"type": "pricing_comparison", "id": 1}],
                    }],
                },
            },
        ])
        result = _extract_insights_from_agent_result(agent_result)
        assert result["positioning_narrative"] == "OK"


# ---------------------------------------------------------------------------
# _sanitize_source_data_points
# ---------------------------------------------------------------------------


class TestSanitizeSourceDataPoints:
    def test_drops_unknown_ids(self) -> None:
        refs = [
            {"type": "dimensional_score", "id": 1001},  # valid
            {"type": "dimensional_score", "id": 9999},  # unknown id
            {"type": "pricing_comparison", "id": 456},  # valid
            {"type": "unknown_type", "id": 1},  # unknown type
        ]
        valid_ids = {
            "dimensional_score": {1001, 1002},
            "pricing_comparison": {456, 457},
            "service_gap": {301},
        }
        out = _sanitize_source_data_points(refs, valid_ids)
        assert out == [
            {"type": "dimensional_score", "id": 1001},
            {"type": "pricing_comparison", "id": 456},
        ]

    def test_handles_non_list_input(self) -> None:
        assert _sanitize_source_data_points(None, {}) == []
        assert _sanitize_source_data_points("not a list", {}) == []

    def test_drops_malformed_items(self) -> None:
        refs = [
            {"type": "dimensional_score", "id": 1},
            "string item",
            {"type": "dimensional_score"},  # missing id
            {"id": 1},  # missing type
            {"type": "dimensional_score", "id": "not an int"},
        ]
        out = _sanitize_source_data_points(refs, {"dimensional_score": {1}})
        assert out == [{"type": "dimensional_score", "id": 1}]


# ---------------------------------------------------------------------------
# _sanitize_insights — drops bullets with zero valid refs
# ---------------------------------------------------------------------------


class TestSanitizeInsights:
    def test_drops_swot_bullet_without_valid_refs(self) -> None:
        insights = {
            "positioning_narrative": "Test",
            "swot": {
                "strengths": [
                    {"text": "Valid bullet", "sourceDataPoints": [{"type": "dimensional_score", "id": 1001}]},
                    {"text": "Invalid bullet", "sourceDataPoints": [{"type": "dimensional_score", "id": 9999}]},
                ],
                "weaknesses": [], "opportunities": [], "threats": [],
            },
            "recommendations": [],
        }
        valid_ids = {"dimensional_score": {1001}, "pricing_comparison": set(), "service_gap": set()}
        out = _sanitize_insights(insights, valid_ids=valid_ids, valid_competitor_ids={9411})
        assert len(out["swot"]["strengths"]) == 1
        assert out["swot"]["strengths"][0]["text"] == "Valid bullet"

    def test_drops_recommendation_without_valid_refs(self) -> None:
        insights = {
            "positioning_narrative": "Test narrative",
            "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
            "recommendations": [
                {
                    "actionTitle": "Valid rec",
                    "actionDescription": "This recommendation has valid references",
                    "category": "pricing", "impact": "high", "effort": "low", "confidence": 0.8,
                    "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
                },
                {
                    "actionTitle": "Invalid rec",
                    "actionDescription": "This recommendation references nothing",
                    "category": "content", "impact": "low", "effort": "low", "confidence": 0.3,
                    "sourceDataPoints": [{"type": "pricing_comparison", "id": 9999}],
                },
            ],
        }
        valid_ids = {"dimensional_score": set(), "pricing_comparison": {456}, "service_gap": set()}
        out = _sanitize_insights(insights, valid_ids=valid_ids, valid_competitor_ids={9411})
        assert len(out["recommendations"]) == 1
        assert out["recommendations"][0]["actionTitle"] == "Valid rec"

    def test_clamps_confidence(self) -> None:
        insights = {
            "positioning_narrative": "T",
            "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
            "recommendations": [
                {
                    "actionTitle": "T", "actionDescription": "Long enough description here",
                    "category": "pricing", "impact": "high", "effort": "low",
                    "confidence": 1.5,  # out of range
                    "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
                },
            ],
        }
        valid_ids = {"dimensional_score": set(), "pricing_comparison": {456}, "service_gap": set()}
        out = _sanitize_insights(insights, valid_ids=valid_ids, valid_competitor_ids=set())
        assert out["recommendations"][0]["confidence"] == 1.0

    def test_drops_unknown_competitor_ids(self) -> None:
        insights = {
            "positioning_narrative": "T",
            "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
            "recommendations": [
                {
                    "actionTitle": "T", "actionDescription": "Long enough description here",
                    "category": "pricing", "impact": "high", "effort": "low", "confidence": 0.7,
                    "sourceCompetitorIds": [9411, 9999],  # 9999 is unknown
                    "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
                },
            ],
        }
        valid_ids = {"dimensional_score": set(), "pricing_comparison": {456}, "service_gap": set()}
        out = _sanitize_insights(insights, valid_ids=valid_ids, valid_competitor_ids={9411})
        assert out["recommendations"][0]["sourceCompetitorIds"] == [9411]


# ---------------------------------------------------------------------------
# _deterministic_fallback
# ---------------------------------------------------------------------------


class TestDeterministicFallback:
    def test_produces_valid_structure_when_ai_fails(
        self, sample_subject_context, sample_matches, sample_pricing,
        sample_gaps, sample_dimensions,
    ) -> None:
        result = _deterministic_fallback(
            subject_context=sample_subject_context,
            matches=sample_matches,
            pricing=sample_pricing,
            gaps=sample_gaps,
            dimensions=sample_dimensions,
        )
        assert "positioning_narrative" in result
        assert len(result["positioning_narrative"]) >= 100
        assert len(result["positioning_narrative"]) <= 800
        assert "swot" in result
        assert set(result["swot"].keys()) == {"strengths", "weaknesses", "opportunities", "threats"}
        # SWOT bullets can't be hand-crafted deterministically → stays empty.
        assert all(result["swot"][k] == [] for k in result["swot"])
        # Recommendations ARE emitted deterministically: _deterministic_fallback
        # builds up to 3 from the top pricing deviations (≥5%), so the user
        # always gets actionable items even on total AI failure (see the
        # function docstring). sample_pricing has 2 rows ≥5% → expect 1..3
        # pricing-category recs, each structured + traced to a pricing row.
        recs = result["recommendations"]
        assert isinstance(recs, list)
        assert 1 <= len(recs) <= 3
        for r in recs:
            assert r["category"] == "pricing"
            assert r["actionTitle"]
            assert r["actionDescription"]
            assert r["sourceDataPoints"]
        # narrative mentions subject name
        assert "Beauty4ever" in result["positioning_narrative"]

    def test_counts_wins_and_losses(self, sample_subject_context) -> None:
        dims = [
            # Win: better_is_higher, percentile 80 (good)
            {"subject_percentile": 80, "better_is_higher": True},
            # Loss: better_is_higher, percentile 20 (bad)
            {"subject_percentile": 20, "better_is_higher": True},
            # Win: better_is_higher=False, percentile 20 (price_range low = good)
            {"subject_percentile": 20, "better_is_higher": False},
        ]
        result = _deterministic_fallback(
            subject_context=sample_subject_context,
            matches=[],
            pricing=[],
            gaps=[],
            dimensions=dims,
        )
        # Should mention "2 wymiarach" for wins and "1" for losses
        assert "2" in result["positioning_narrative"]
        assert "1" in result["positioning_narrative"]


# ---------------------------------------------------------------------------
# End-to-end synthesize_competitor_insights with mocked deps
# ---------------------------------------------------------------------------


def _mk_mock_supabase(
    report, subject_context, matches, pricing, gaps, dimensions,
):
    """Build a MagicMock SupabaseService with all methods used by Etap 5."""
    mock = MagicMock()
    mock.get_competitor_report_by_id = AsyncMock(return_value=report)
    mock.get_competitor_matches = AsyncMock(return_value=matches)
    mock.get_competitor_pricing_comparisons = AsyncMock(return_value=pricing)
    mock.get_competitor_service_gaps = AsyncMock(return_value=gaps)
    mock.get_competitor_dimensional_scores = AsyncMock(return_value=dimensions)
    mock.get_subject_salon_context = AsyncMock(return_value=subject_context)
    mock.update_competitor_report_data = AsyncMock(return_value=None)
    mock.delete_competitor_recommendations = AsyncMock(return_value=None)
    mock.insert_competitor_recommendations = AsyncMock(return_value=1)
    return mock


@pytest.mark.asyncio
async def test_synthesize_with_mocked_minimax(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    """Full synthesis happy-path — MiniMax returns a canned result."""
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )

    canned_insights = {
        "positioning_narrative": (
            "Beauty4ever to największy salon kosmetyczny w swojej okolicy z "
            "273 usługami i rekordowymi 17 609 recenzjami. Ma najszersze "
            "portfolio i najwyższą rozpoznawalność, ale cenowo odstaje od "
            "rynku w kluczowych kategoriach."
        ),
        "swot": {
            "strengths": [
                {
                    "text": "Największe portfolio — 273 usługi vs mediana rynku 132",
                    "sourceDataPoints": [{"type": "dimensional_score", "id": 1002}],
                },
            ],
            "weaknesses": [
                {
                    "text": "Tylko 23% usług ma opisy (rynek: 85%)",
                    "sourceDataPoints": [{"type": "dimensional_score", "id": 1001}],
                },
            ],
            "opportunities": [
                {
                    "text": "Wprowadź hennę pudrową — 4 konkurentów ma ją w cenniku",
                    "sourceDataPoints": [{"type": "service_gap", "id": 301}],
                },
            ],
            "threats": [
                {
                    "text": "Endermologia za 200 zł vs rynek 112 zł (+79%) — klienci odpłyną",
                    "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
                },
            ],
        },
        "recommendations": [
            {
                "actionTitle": "Obniż cenę endermologii z 200 do 130 zł",
                "actionDescription": (
                    "Konkurenci w promieniu 3 km oferują endermologię za "
                    "średnio 112 zł. Jesteś 79% powyżej mediany rynku, co "
                    "stwarza ryzyko utraty klientów wrażliwych cenowo."
                ),
                "category": "pricing",
                "impact": "high",
                "effort": "low",
                "confidence": 0.85,
                "estimatedRevenueImpactGrosze": 280000,
                "sourceCompetitorIds": [9411, 9471],
                "sourceDataPoints": [{"type": "pricing_comparison", "id": 456}],
            },
        ],
    }

    with patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=canned_insights),
    ):
        result = await synthesize_competitor_insights(
            report_id=27,
            supabase=mock_supabase,
        )

    assert result["narrative"].startswith("Beauty4ever")
    assert result["swot_item_count"] == 4  # 1 per quadrant
    assert result["recommendation_count"] == 1
    assert result["used_fallback"] is False

    # Verify persistence calls
    mock_supabase.update_competitor_report_data.assert_called_once()
    args, _ = mock_supabase.update_competitor_report_data.call_args
    assert args[0] == 27
    assert "positioning_narrative" in args[1]
    assert "swot" in args[1]
    # Cross-service contract: key is always present (empty list when no picks).
    assert args[1]["userSelectedCompetitorIds"] == []

    mock_supabase.delete_competitor_recommendations.assert_called_once_with(27)
    mock_supabase.insert_competitor_recommendations.assert_called_once()
    inserted_rows = mock_supabase.insert_competitor_recommendations.call_args[0][0]
    assert len(inserted_rows) == 1
    assert inserted_rows[0]["action_title"] == "Obniż cenę endermologii z 200 do 130 zł"
    assert inserted_rows[0]["source_competitor_ids"] == [9411, 9471]
    assert inserted_rows[0]["source_data_points"] == [{"type": "pricing_comparison", "id": 456}]


@pytest.mark.asyncio
async def test_synthesize_persists_user_selected_competitor_ids_intersected(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    """userSelectedCompetitorIds in report_data must be the user's picks
    intersected with the report's competitor salon_ids — picks that didn't
    make it into the report (e.g. 99999) are dropped, surviving picks kept."""
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )

    canned_insights = {
        "positioning_narrative": "x" * 50,
        "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
        "recommendations": [],
    }

    with patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=canned_insights),
    ):
        await synthesize_competitor_insights(
            report_id=27,
            supabase=mock_supabase,
            # 9411 + 9500 are in the report (sample_matches); 99999 is not.
            user_selected_salon_ids=[9411, 9500, 99999],
        )

    args, _ = mock_supabase.update_competitor_report_data.call_args
    persisted = args[1]["userSelectedCompetitorIds"]
    assert sorted(persisted) == [9411, 9500]  # 99999 dropped
    assert 99999 not in persisted


@pytest.mark.asyncio
async def test_synthesize_falls_back_on_minimax_failure(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    """When MiniMax throws, the pipeline must use the deterministic fallback
    and still persist a valid report."""
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    mock_supabase.insert_competitor_recommendations = AsyncMock(return_value=0)

    with patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(side_effect=RuntimeError("MiniMax timeout")),
    ):
        result = await synthesize_competitor_insights(
            report_id=27,
            supabase=mock_supabase,
        )

    assert result["used_fallback"] is True
    assert result["narrative"]  # fallback narrative always set
    assert result["swot_item_count"] == 0
    assert result["recommendation_count"] == 0
    # Still persisted the fallback narrative to report_data
    mock_supabase.update_competitor_report_data.assert_called_once()


@pytest.mark.asyncio
async def test_synthesize_raises_when_report_not_found(
    sample_subject_context, sample_matches, sample_pricing,
    sample_gaps, sample_dimensions,
) -> None:
    mock_supabase = _mk_mock_supabase(
        None, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    with pytest.raises(ValueError, match="not found"):
        await synthesize_competitor_insights(report_id=999, supabase=mock_supabase)


# ===========================================================================
# quick 260613-m23 Task 2 — synthesis observability instrumentation
# ===========================================================================
#
# synthesis.attempt per fallback step (minimax/openai/deterministic),
# phase.timer "synthesis" around the chain, synthesis.sanitizer_dropped when
# the sanitizer drops SWOT/rec bullets. Observability-only: the result dict
# {narrative, swot_item_count, recommendation_count, used_fallback} and the
# fallback chain are byte-identical to before (covered by the tests above).
# ===========================================================================


class _FakeTracer:
    """Capturing fake matching the TraceWriter public surface synthesis uses
    (add/flush/buffered) — synth_tracer is only ever add()'d, never poked
    for _buffer in the synthesis path."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        self.calls: list[dict] = []

    def add(self, step, data, *, salon_ref_id=None, tokens_used=None):  # noqa: ANN001
        self.calls.append({"step": step, "data": data})

    def buffered(self) -> int:
        return len(self.calls)

    async def flush(self) -> int:
        return len(self.calls)

    def of(self, step: str) -> list[dict]:
        return [c for c in self.calls if c["step"] == step]


_CANNED_OK = {
    "positioning_narrative": "Beauty4ever " + "x" * 60,
    "swot": {
        "strengths": [
            {
                "text": "Największe portfolio",
                "sourceDataPoints": [{"type": "dimensional_score", "id": 1002}],
            }
        ],
        "weaknesses": [],
        "opportunities": [],
        "threats": [],
    },
    "recommendations": [],
}


def _patch_tracer():
    """Patch TraceWriter in the synthesis module with a captured _FakeTracer.
    Returns the patcher; the captured tracer is available via the closure
    list after entering."""
    captured: list[_FakeTracer] = []

    def _factory(*args, **kwargs):  # noqa: ANN002, ANN003
        t = _FakeTracer()
        captured.append(t)
        return t

    return patch(
        "pipelines.competitor_synthesis.TraceWriter", side_effect=_factory,
    ), captured


@pytest.mark.asyncio
async def test_synthesis_attempt_minimax_success(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    patcher, captured = _patch_tracer()
    with patcher, patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=_CANNED_OK),
    ):
        await synthesize_competitor_insights(report_id=27, supabase=mock_supabase)

    tracer = captured[0]
    attempts = tracer.of("synthesis.attempt")
    assert len(attempts) == 1
    a = attempts[0]["data"]
    assert a["attempt"] == 1
    assert a["model"] == "minimax"
    assert a["success"] is True
    assert a["error"] is None
    assert isinstance(a["duration_ms"], int)


@pytest.mark.asyncio
async def test_synthesis_attempt_minimax_fail_then_openai_success(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    patcher, captured = _patch_tracer()
    with patcher, patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(side_effect=RuntimeError("minimax boom")),
    ), patch(
        "services.openai_synthesis.synthesize_via_openai",
        new=AsyncMock(return_value=_CANNED_OK),
    ):
        result = await synthesize_competitor_insights(
            report_id=27, supabase=mock_supabase,
        )
    assert result["used_fallback"] is True

    tracer = captured[0]
    attempts = tracer.of("synthesis.attempt")
    assert len(attempts) == 2
    assert attempts[0]["data"]["model"] == "minimax"
    assert attempts[0]["data"]["success"] is False
    assert "boom" in attempts[0]["data"]["error"]
    assert attempts[1]["data"]["model"] == "openai"
    assert attempts[1]["data"]["success"] is True


@pytest.mark.asyncio
async def test_synthesis_attempt_all_fail_deterministic(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    mock_supabase.insert_competitor_recommendations = AsyncMock(return_value=0)
    patcher, captured = _patch_tracer()
    with patcher, patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(side_effect=RuntimeError("minimax boom")),
    ), patch(
        "services.openai_synthesis.synthesize_via_openai",
        new=AsyncMock(side_effect=RuntimeError("openai boom")),
    ):
        result = await synthesize_competitor_insights(
            report_id=27, supabase=mock_supabase,
        )
    assert result["used_fallback"] is True

    tracer = captured[0]
    attempts = tracer.of("synthesis.attempt")
    assert len(attempts) == 3
    assert attempts[2]["data"]["model"] == "deterministic"
    assert attempts[2]["data"]["success"] is True


@pytest.mark.asyncio
async def test_synthesis_phase_timer_emitted(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    patcher, captured = _patch_tracer()
    with patcher, patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=_CANNED_OK),
    ):
        await synthesize_competitor_insights(report_id=27, supabase=mock_supabase)

    tracer = captured[0]
    timers = tracer.of("phase.timer")
    phases = [t["data"]["phase"] for t in timers]
    assert "synthesis" in phases


@pytest.mark.asyncio
async def test_synthesis_sanitizer_dropped_trace(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    """A SWOT bullet referencing an unknown id is dropped by _sanitize_insights
    (sanitizerDropped > 0). The drop is now surfaced as a
    synthesis.sanitizer_dropped trace."""
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    # One valid strength + one strength referencing a non-existent dimensional
    # id (99999) → sanitizer drops the invalid one.
    canned = {
        "positioning_narrative": "Beauty4ever " + "x" * 60,
        "swot": {
            "strengths": [
                {
                    "text": "Valid",
                    "sourceDataPoints": [{"type": "dimensional_score", "id": 1002}],
                },
                {
                    "text": "Invalid ref",
                    "sourceDataPoints": [{"type": "dimensional_score", "id": 99999}],
                },
            ],
            "weaknesses": [],
            "opportunities": [],
            "threats": [],
        },
        "recommendations": [],
    }
    patcher, captured = _patch_tracer()
    with patcher, patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=canned),
    ):
        await synthesize_competitor_insights(report_id=27, supabase=mock_supabase)

    tracer = captured[0]
    dropped = tracer.of("synthesis.sanitizer_dropped")
    assert len(dropped) == 1
    assert dropped[0]["data"].get("swotDropped", 0) >= 1


@pytest.mark.asyncio
async def test_synthesis_tracer_none_safe(
    sample_report, sample_subject_context, sample_matches,
    sample_pricing, sample_gaps, sample_dimensions,
) -> None:
    """When TraceWriter init fails (synth_tracer=None), synthesis must not
    crash and must still return a valid result."""
    mock_supabase = _mk_mock_supabase(
        sample_report, sample_subject_context, sample_matches,
        sample_pricing, sample_gaps, sample_dimensions,
    )
    with patch(
        "pipelines.competitor_synthesis.TraceWriter",
        side_effect=RuntimeError("trace init boom"),
    ), patch(
        "pipelines.competitor_synthesis._run_minimax_synthesis",
        new=AsyncMock(return_value=_CANNED_OK),
    ):
        result = await synthesize_competitor_insights(
            report_id=27, supabase=mock_supabase,
        )
    assert result["used_fallback"] is False


# ---------------------------------------------------------------------------
# Global per-MODEL LLM limiter wiring (beads BEAUTY_AUDIT-ii0 increment 2)
# ---------------------------------------------------------------------------


def _mk_synthesis_agent_result() -> SimpleNamespace:
    """Fake AgentResult shaped like agent.runner.AgentResult — one
    submit_competitor_insights tool call with a valid narrative + SWOT so
    _extract_insights_from_agent_result returns a non-empty dict."""
    call = SimpleNamespace(
        name="submit_competitor_insights",
        input={
            "positioning_narrative": "Beauty4ever " + "x" * 60,
            "swot": {
                "strengths": [
                    {
                        "text": "Największe portfolio",
                        "sourceDataPoints": [
                            {"type": "dimensional_score", "id": 1002}
                        ],
                    }
                ],
                "weaknesses": [],
                "opportunities": [],
                "threats": [],
            },
            "recommendations": [],
        },
    )
    return SimpleNamespace(
        tool_calls=[call],
        final_text="",
        total_steps=1,
        total_input_tokens=0,
        total_output_tokens=0,
    )


@pytest.mark.asyncio
async def test_run_minimax_synthesis_acquires_minimax_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_run_minimax_synthesis must enter provider_slot exactly once, keyed by
    settings.minimax_model, wrapping the run_agent_loop await — and return the
    parsed insights dict unchanged (decision logic untouched).

    Does NOT patch _run_minimax_synthesis (that is what we exercise). Instead it
    patches the late-imported run_agent_loop (resolved as an attribute of module
    agent.runner) + services.minimax.MiniMaxClient, and spies on the MODULE-TOP
    pipelines.competitor_synthesis.provider_slot."""
    from config import settings

    run_loop_mock = AsyncMock(return_value=_mk_synthesis_agent_result())
    monkeypatch.setattr("agent.runner.run_agent_loop", run_loop_mock)

    # Stub MiniMaxClient so the ctor + `client.client = AsyncAnthropic(...)`
    # override in _run_minimax_synthesis is harmless (no real network client).
    monkeypatch.setattr(
        "services.minimax.MiniMaxClient",
        lambda *a, **k: SimpleNamespace(client=None, model=settings.minimax_model),
    )

    slot_entries: list[str] = []

    @asynccontextmanager
    async def _spy_provider_slot(model: str):
        slot_entries.append(model)
        yield

    monkeypatch.setattr(
        "pipelines.competitor_synthesis.provider_slot", _spy_provider_slot
    )

    insights = await _run_minimax_synthesis(context="ctx", tracer=None)

    # Slot entered exactly once, keyed by the MiniMax model.
    assert slot_entries == [settings.minimax_model], (
        f"expected provider_slot entered once == [{settings.minimax_model!r}], got "
        f"{slot_entries}"
    )
    # run_agent_loop was awaited exactly once (inside the slot).
    assert run_loop_mock.await_count == 1
    # Decision UNCHANGED: returns the parsed insights dict.
    assert insights["positioning_narrative"].startswith("Beauty4ever")
    assert len(insights["swot"]["strengths"]) == 1


@pytest.mark.asyncio
async def test_synthesize_via_openai_acquires_gpt4omini_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """synthesize_via_openai (the rare OpenAI fallback) must enter provider_slot
    exactly once keyed "gpt-4o-mini" (shares the gpt-4o-mini bucket), wrapping
    the create() await — and return the parsed/normalized dict unchanged."""
    from services.openai_synthesis import synthesize_via_openai

    valid_json = json.dumps(
        {
            "positioning_narrative": "Pozycja salonu " + "y" * 40,
            "swot": {
                "strengths": [{"text": "S1"}],
                "weaknesses": [],
                "opportunities": [],
                "threats": [],
            },
            "recommendations": [
                {
                    "actionTitle": "Obniż cenę",
                    "actionDescription": "Konkurenci tańsi.",
                    "category": "pricing",
                    "impact": "high",
                    "effort": "low",
                    "confidence": 0.8,
                }
            ],
        }
    )
    fake_resp = SimpleNamespace(
        choices=[
            SimpleNamespace(message=SimpleNamespace(content=valid_json))
        ],
        usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5),
    )
    create_mock = AsyncMock(return_value=fake_resp)
    fake_client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock)),
    )
    monkeypatch.setattr(
        "services.openai_synthesis._get_openai_client", lambda: fake_client
    )

    slot_entries: list[str] = []

    @asynccontextmanager
    async def _spy_provider_slot(model: str):
        slot_entries.append(model)
        yield

    monkeypatch.setattr(
        "services.openai_synthesis.provider_slot", _spy_provider_slot
    )

    result = await synthesize_via_openai("ctx")

    assert slot_entries == ["gpt-4o-mini"], (
        f"expected provider_slot entered once == ['gpt-4o-mini'], got "
        f"{slot_entries}"
    )
    assert create_mock.await_count == 1
    # Parse/normalization UNCHANGED: positioning_narrative present, recs
    # normalized with empty source lists added by the fallback.
    assert result["positioning_narrative"].startswith("Pozycja salonu")
    assert result["recommendations"][0]["sourceCompetitorIds"] == []
    assert result["recommendations"][0]["sourceDataPoints"] == []
