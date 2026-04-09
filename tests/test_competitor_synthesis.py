"""Unit tests for Comp Etap 5 competitor synthesis pipeline.

Covers:
- prompt context builder formats data correctly with Polish labels
- tool call parser extracts narrative + SWOT + recommendations
- sanitizer drops sourceDataPoints that reference unknown IDs
- deterministic fallback when AI fails
- end-to-end synthesize_competitor_insights with mocked MiniMax + Supabase
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipelines.competitor_synthesis import (
    _build_synthesis_prompt_context,
    _deterministic_fallback,
    _extract_insights_from_agent_result,
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
        assert result["recommendations"] == []
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

    mock_supabase.delete_competitor_recommendations.assert_called_once_with(27)
    mock_supabase.insert_competitor_recommendations.assert_called_once()
    inserted_rows = mock_supabase.insert_competitor_recommendations.call_args[0][0]
    assert len(inserted_rows) == 1
    assert inserted_rows[0]["action_title"] == "Obniż cenę endermologii z 200 do 130 zł"
    assert inserted_rows[0]["source_competitor_ids"] == [9411, 9471]
    assert inserted_rows[0]["source_data_points"] == [{"type": "pricing_comparison", "id": 456}]


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
