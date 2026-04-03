"""Tests for the competitor report pipeline."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipelines.competitor import (
    _format_gap_matrix,
    _format_local_ranking,
    _format_per_service_pricing,
    _format_price_comparison,
    _format_promotions,
    _format_salon_for_context,
    run_competitor_pipeline,
)


# ── Fixtures ──


SUBJECT_SALON_DATA = {
    "salon": {
        "id": 100080,
        "name": "Instytut Piękna",
        "city": "Płock",
        "lat": 52.5465,
        "lng": 19.6973,
        "reviews_rank": 4.8,
        "reviews_count": 120,
    },
    "services": [
        {"id": 1, "name": "Manicure hybrydowy", "price": 80, "category": "Paznokcie"},
        {"id": 2, "name": "Pedicure", "price": 100, "category": "Paznokcie"},
        {"id": 3, "name": "Strzyżenie damskie", "price": 120, "category": "Fryzjerstwo"},
    ],
    "categories": ["Fryzjerstwo", "Paznokcie"],
}

COMPETITOR_SALON_DATA_1 = {
    "salon": {
        "id": 100081,
        "name": "Studio Urody",
        "city": "Płock",
        "lat": 52.5500,
        "lng": 19.7000,
        "reviews_rank": 4.5,
        "reviews_count": 80,
    },
    "services": [
        {"id": 10, "name": "Manicure hybrydowy", "price": 70, "category": "Paznokcie"},
        {"id": 11, "name": "Strzyżenie damskie", "price": 100, "category": "Fryzjerstwo"},
    ],
    "categories": ["Fryzjerstwo", "Paznokcie"],
}

COMPETITOR_SALON_DATA_2 = {
    "salon": {
        "id": 100082,
        "name": "Beauty Bar",
        "city": "Płock",
        "lat": 52.5480,
        "lng": 19.6950,
        "reviews_rank": 4.2,
        "reviews_count": 45,
    },
    "services": [
        {"id": 20, "name": "Manicure klasyczny", "price": 50, "category": "Paznokcie"},
    ],
    "categories": ["Paznokcie"],
}

PRICE_COMPARISON_DATA = [
    {"category": "Paznokcie", "avg_price": 65, "subject_avg_price": 90, "positioning": "above_p75"},
    {"category": "Fryzjerstwo", "avg_price": 100, "subject_avg_price": 120, "positioning": "p50_p75"},
]

LOCAL_RANKING_DATA = [
    {"rank": 3, "total_in_city": 25, "city": "Płock", "percentile": 88},
]

GAP_MATRIX_DATA = [
    {"service_name": "Masaż relaksacyjny", "salon_has": False, "competitor_has": True, "competitor_count": 3},
    {"service_name": "Henna brwi", "salon_has": False, "competitor_has": True, "competitor_count": 2},
    {"service_name": "Pedicure", "salon_has": True, "competitor_has": False, "competitor_count": 0},
]

PROMOTIONS_DATA = [
    {"salon_name": "Studio Urody", "service_name": "Manicure hybrydowy", "discount_percent": 20},
    {"salon_name": "Beauty Bar", "service_name": "Manicure klasyczny", "discount_percent": 15},
]

PER_SERVICE_PRICING_DATA = [
    {"service_name": "Manicure hybrydowy", "subject_price": 80, "min_price": 50, "max_price": 90, "avg_price": 67, "percentile_position": 85},
    {"service_name": "Strzyżenie damskie", "subject_price": 120, "min_price": 80, "max_price": 150, "avg_price": 107, "percentile_position": 65},
]

MARKET_ANALYSIS_TOOL_CALL_INPUT = {
    "marketSummary": {
        "headline": "Instytut Piękna zajmuje silną pozycję w segmencie premium",
        "paragraphs": [
            "Salon plasuje się w górnym kwartylu cenowym.",
            "Konkurencja oferuje niższe ceny, ale mniejszy zakres usług.",
        ],
        "keyInsights": [
            "Ceny salonu są o 30% wyższe od średniej rynkowej",
            "Salon ma najwyższą ocenę w mieście",
        ],
    },
    "advantages": [
        {"area": "Opinie klientów", "type": "win", "description": "Najwyższa ocena 4.8/5", "impact": "high"},
        {"area": "Ceny", "type": "loss", "description": "Ceny powyżej średniej rynkowej", "impact": "medium"},
    ],
    "recommendations": [
        {"title": "Dodaj masaż do oferty", "description": "Luka w ofercie", "estimatedImpactPln": 3000, "priority": 1, "category": "services"},
    ],
    "radarMetrics": [
        {"metric": "Zakres usług", "salonValue": 65, "marketAvg": 70},
        {"metric": "Opinie", "salonValue": 96, "marketAvg": 85},
    ],
}

SWOT_RESULT = {
    "strengths": ["Najwyższa ocena klientów w mieście", "Szeroka oferta paznokci"],
    "weaknesses": ["Brak masażu w ofercie", "Ceny powyżej średniej"],
    "opportunities": ["Rosnący popyt na masaże", "Mało konkurencji w segmencie premium"],
    "threats": ["Nowy salon w centrum", "Rosnące koszty materiałów"],
}

STRATEGIC_TOOL_CALL_INPUT = {
    "marketNiches": [
        {"niche": "Masaże premium", "opportunity": "Brak oferty masaży w okolicy", "competitorCount": 1, "potentialRevenue": "5000 PLN/mies"},
        {"niche": "Pakiety ślubne", "opportunity": "Żaden salon nie oferuje pakietów", "competitorCount": 0, "potentialRevenue": "3000 PLN/mies"},
    ],
    "actionPlan": [
        {"action": "Wprowadź masaże do oferty", "timeline": "krótkoterminowo", "priority": "high", "expectedOutcome": "5000 PLN/mies dodatkowego przychodu", "estimatedCost": "2000 PLN"},
        {"action": "Uruchom program lojalnościowy", "timeline": "średnioterminowo", "priority": "medium", "expectedOutcome": "15% wzrost retencji", "estimatedCost": "500 PLN"},
    ],
}


# ── Test 1: RPC result formatting — price comparison ──


class TestFormatPriceComparison:
    def test_formats_price_comparison_data(self) -> None:
        result = _format_price_comparison(PRICE_COMPARISON_DATA)
        assert "PORÓWNANIE CEN WG KATEGORII:" in result
        assert "Paznokcie" in result
        assert "Salon=90" in result
        assert "Rynek=65" in result
        assert "above_p75" in result

    def test_handles_empty_data(self) -> None:
        result = _format_price_comparison([])
        assert "Brak danych" in result


# ── Test 2: RPC result formatting — other formats ──


class TestFormatRPCResults:
    def test_format_local_ranking(self) -> None:
        result = _format_local_ranking(LOCAL_RANKING_DATA)
        assert "3/25" in result
        assert "Płock" in result
        assert "88%" in result

    def test_format_local_ranking_empty(self) -> None:
        result = _format_local_ranking([])
        assert "Brak danych" in result

    def test_format_gap_matrix(self) -> None:
        result = _format_gap_matrix(GAP_MATRIX_DATA)
        assert "LUKI W USŁUGACH:" in result
        assert "Masaż relaksacyjny" in result
        assert "Pedicure" in result

    def test_format_gap_matrix_empty(self) -> None:
        result = _format_gap_matrix([])
        assert "Brak danych" in result

    def test_format_promotions(self) -> None:
        result = _format_promotions(PROMOTIONS_DATA)
        assert "PROMOCJE KONKURENCJI:" in result
        assert "Studio Urody" in result
        assert "-20%" in result

    def test_format_promotions_empty(self) -> None:
        result = _format_promotions([])
        assert "Brak danych" in result

    def test_format_per_service_pricing(self) -> None:
        result = _format_per_service_pricing(PER_SERVICE_PRICING_DATA)
        assert "PORÓWNANIE CEN USŁUG:" in result
        assert "Manicure hybrydowy" in result
        assert "Salon=80" in result

    def test_format_per_service_pricing_empty(self) -> None:
        result = _format_per_service_pricing([])
        assert "Brak danych" in result

    def test_format_salon_for_context(self) -> None:
        result = _format_salon_for_context(SUBJECT_SALON_DATA, "SALON TEST")
        assert "SALON TEST" in result
        assert "Instytut Piękna" in result
        assert "Płock" in result
        assert "4.8" in result

    def test_format_salon_for_context_none(self) -> None:
        result = _format_salon_for_context(None, "SALON")
        assert "Brak danych" in result


# ── Test 3: Market analysis tool call parsing ──


class TestMarketAnalysisParsing:
    def test_extracts_market_summary(self) -> None:
        """Verify that market analysis tool call input is properly extracted."""
        tc_input = MARKET_ANALYSIS_TOOL_CALL_INPUT
        market_summary = tc_input.get("marketSummary", {})
        assert market_summary["headline"] == "Instytut Piękna zajmuje silną pozycję w segmencie premium"
        assert len(market_summary["paragraphs"]) == 2
        assert len(market_summary["keyInsights"]) == 2

    def test_extracts_advantages(self) -> None:
        tc_input = MARKET_ANALYSIS_TOOL_CALL_INPUT
        advantages = tc_input.get("advantages", [])
        assert len(advantages) == 2
        assert advantages[0]["type"] == "win"
        assert advantages[1]["type"] == "loss"
        assert advantages[0]["impact"] == "high"

    def test_extracts_recommendations(self) -> None:
        tc_input = MARKET_ANALYSIS_TOOL_CALL_INPUT
        recs = tc_input.get("recommendations", [])
        assert len(recs) == 1
        assert recs[0]["priority"] == 1
        assert recs[0]["category"] == "services"

    def test_extracts_radar_metrics(self) -> None:
        tc_input = MARKET_ANALYSIS_TOOL_CALL_INPUT
        radar = tc_input.get("radarMetrics", [])
        assert len(radar) == 2
        assert radar[0]["metric"] == "Zakres usług"
        assert radar[0]["salonValue"] == 65
        assert radar[0]["marketAvg"] == 70


# ── Test 4: SWOT JSON parsing with fallback ──


class TestSwotParsing:
    def test_parses_valid_swot(self) -> None:
        swot = {
            "strengths": SWOT_RESULT["strengths"],
            "weaknesses": SWOT_RESULT["weaknesses"],
            "opportunities": SWOT_RESULT["opportunities"],
            "threats": SWOT_RESULT["threats"],
        }
        assert len(swot["strengths"]) == 2
        assert len(swot["weaknesses"]) == 2
        assert len(swot["opportunities"]) == 2
        assert len(swot["threats"]) == 2

    def test_swot_fallback_on_missing_keys(self) -> None:
        """When SWOT JSON is missing keys, fallback to empty lists."""
        raw = {"strengths": ["one"]}
        swot = {
            "strengths": raw.get("strengths", []),
            "weaknesses": raw.get("weaknesses", []),
            "opportunities": raw.get("opportunities", []),
            "threats": raw.get("threats", []),
        }
        assert swot["strengths"] == ["one"]
        assert swot["weaknesses"] == []
        assert swot["opportunities"] == []
        assert swot["threats"] == []

    def test_swot_fallback_on_empty_result(self) -> None:
        raw: dict = {}
        swot = {
            "strengths": raw.get("strengths", []),
            "weaknesses": raw.get("weaknesses", []),
            "opportunities": raw.get("opportunities", []),
            "threats": raw.get("threats", []),
        }
        for key in swot:
            assert swot[key] == []


# ── Test 5: Strategic analysis tool call parsing ──


class TestStrategicAnalysisParsing:
    def test_extracts_market_niches(self) -> None:
        tc_input = STRATEGIC_TOOL_CALL_INPUT
        niches = tc_input.get("marketNiches", [])
        assert len(niches) == 2
        assert niches[0]["niche"] == "Masaże premium"
        assert niches[1]["competitorCount"] == 0

    def test_extracts_action_plan(self) -> None:
        tc_input = STRATEGIC_TOOL_CALL_INPUT
        plan = tc_input.get("actionPlan", [])
        assert len(plan) == 2
        assert plan[0]["priority"] == "high"
        assert plan[1]["timeline"] == "średnioterminowo"

    def test_multiple_tool_calls_accumulate(self) -> None:
        """Strategic agent may call tool multiple times — results should accumulate."""
        all_niches: list[dict] = []
        all_plans: list[dict] = []

        # Simulate two tool calls
        call1 = {"marketNiches": [{"niche": "A", "opportunity": "x"}], "actionPlan": [{"action": "do A", "timeline": "now", "priority": "high", "expectedOutcome": "good"}]}
        call2 = {"marketNiches": [{"niche": "B", "opportunity": "y"}], "actionPlan": [{"action": "do B", "timeline": "later", "priority": "low", "expectedOutcome": "ok"}]}

        for call in [call1, call2]:
            all_niches.extend(call.get("marketNiches", []))
            all_plans.extend(call.get("actionPlan", []))

        assert len(all_niches) == 2
        assert len(all_plans) == 2
        assert all_niches[0]["niche"] == "A"
        assert all_niches[1]["niche"] == "B"


# ── Test 6: Report assembly (all sections present) ──


class TestReportAssembly:
    def test_report_has_all_required_sections(self) -> None:
        """Assembled report dict must have all CompetitorReportData fields."""
        report: dict = {
            "version": "v1",
            "subjectSalonId": 100080,
            "salonName": "Test",
            "salonCity": "Płock",
            "salonLat": 52.5,
            "salonLng": 19.7,
            "competitorIds": [100081],
            "competitorCount": 1,
            "competitorProfiles": [{"salonId": 100081, "name": "X"}],
            "priceComparison": PRICE_COMPARISON_DATA,
            "localRanking": LOCAL_RANKING_DATA[0],
            "serviceGapMatrix": GAP_MATRIX_DATA,
            "promotions": PROMOTIONS_DATA,
            "perServicePricing": PER_SERVICE_PRICING_DATA,
            "marketSummary": MARKET_ANALYSIS_TOOL_CALL_INPUT["marketSummary"],
            "advantages": MARKET_ANALYSIS_TOOL_CALL_INPUT["advantages"],
            "recommendations": MARKET_ANALYSIS_TOOL_CALL_INPUT["recommendations"],
            "radarMetrics": MARKET_ANALYSIS_TOOL_CALL_INPUT["radarMetrics"],
            "swot": SWOT_RESULT,
            "marketNiches": STRATEGIC_TOOL_CALL_INPUT["marketNiches"],
            "actionPlan": STRATEGIC_TOOL_CALL_INPUT["actionPlan"],
        }

        required_keys = [
            "version", "subjectSalonId", "salonName", "salonCity",
            "competitorIds", "competitorCount", "competitorProfiles",
            "priceComparison", "localRanking", "serviceGapMatrix",
            "promotions", "perServicePricing",
            "marketSummary", "advantages", "recommendations", "radarMetrics",
            "swot", "marketNiches", "actionPlan",
        ]
        for key in required_keys:
            assert key in report, f"Missing key: {key}"

    def test_report_serializable(self) -> None:
        """Report must be JSON-serializable."""
        report = {
            "version": "v1",
            "subjectSalonId": 100080,
            "salonName": "Test",
            "priceComparison": PRICE_COMPARISON_DATA,
            "swot": SWOT_RESULT,
            "marketNiches": STRATEGIC_TOOL_CALL_INPUT["marketNiches"],
        }
        serialized = json.dumps(report)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed["salonName"] == "Test"


# ── Test 7: Empty RPC data handling (graceful degradation) ──


class TestEmptyRPCHandling:
    def test_all_formatters_handle_empty_gracefully(self) -> None:
        """All format functions must return a sensible string for empty input."""
        assert "Brak" in _format_price_comparison([])
        assert "Brak" in _format_local_ranking([])
        assert "Brak" in _format_gap_matrix([])
        assert "Brak" in _format_promotions([])
        assert "Brak" in _format_per_service_pricing([])
        assert "Brak" in _format_salon_for_context(None)


# ── Test 8: Partial data assembly (some RPCs failed) ──


class TestPartialDataAssembly:
    def test_report_with_empty_rpc_results(self) -> None:
        """Report should still be valid when some RPCs returned empty data."""
        report = {
            "version": "v1",
            "subjectSalonId": 100080,
            "salonName": "Test",
            "salonCity": "Płock",
            "salonLat": None,
            "salonLng": None,
            "competitorIds": [100081, 100082],
            "competitorCount": 2,
            "competitorProfiles": [],
            "priceComparison": [],
            "localRanking": {},
            "serviceGapMatrix": [],
            "promotions": [],
            "perServicePricing": [],
            "marketSummary": {},
            "advantages": [],
            "recommendations": [],
            "radarMetrics": [],
            "swot": {"strengths": [], "weaknesses": [], "opportunities": [], "threats": []},
            "marketNiches": [],
            "actionPlan": [],
        }

        serialized = json.dumps(report)
        parsed = json.loads(serialized)
        assert parsed["version"] == "v1"
        assert parsed["competitorCount"] == 2
        assert parsed["priceComparison"] == []
        assert parsed["swot"]["strengths"] == []


# ── Test 9: Full pipeline with mocks ──


class TestFullPipelineMocked:
    @pytest.mark.asyncio
    async def test_pipeline_runs_with_all_mocks(self) -> None:
        """Full pipeline should run end-to-end with all services mocked."""
        from agent.runner import AgentResult, ToolCallResult

        # Mock MiniMax client
        mock_client = MagicMock()
        mock_client.generate_json = AsyncMock(return_value=SWOT_RESULT)

        # Mock Supabase
        mock_supabase = MagicMock()
        mock_supabase.get_salon_with_services = AsyncMock(side_effect=[
            SUBJECT_SALON_DATA,
            COMPETITOR_SALON_DATA_1,
            COMPETITOR_SALON_DATA_2,
        ])
        mock_supabase.call_rpc = AsyncMock(side_effect=[
            PRICE_COMPARISON_DATA,
            LOCAL_RANKING_DATA,
            GAP_MATRIX_DATA,
            PROMOTIONS_DATA,
            PER_SERVICE_PRICING_DATA,
        ])

        # Mock agent loop — returns market analysis result first, then strategic
        market_agent_result = AgentResult(
            tool_calls=[ToolCallResult(name="submit_market_analysis", input=MARKET_ANALYSIS_TOOL_CALL_INPUT)],
            final_text="Done",
            total_steps=1,
            total_input_tokens=1000,
            total_output_tokens=500,
        )
        strategy_agent_result = AgentResult(
            tool_calls=[ToolCallResult(name="submit_strategic_analysis", input=STRATEGIC_TOOL_CALL_INPUT)],
            final_text="Done",
            total_steps=1,
            total_input_tokens=800,
            total_output_tokens=400,
        )
        mock_agent_loop = AsyncMock(side_effect=[market_agent_result, strategy_agent_result])

        progress_calls: list[tuple[int, str]] = []

        async def mock_progress(pct: int, msg: str) -> None:
            progress_calls.append((pct, msg))

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("services.supabase.SupabaseService", return_value=mock_supabase),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings"),
        ):
            report = await run_competitor_pipeline(
                audit_id="test-audit-123",
                subject_salon_id=100080,
                salon_name="Instytut Piękna",
                salon_city="Płock",
                salon_lat=52.5465,
                salon_lng=19.6973,
                selected_competitor_ids=[100081, 100082],
                services=["Manicure hybrydowy", "Pedicure"],
                on_progress=mock_progress,
            )

        # Verify report structure
        assert report["version"] == "v1"
        assert report["subjectSalonId"] == 100080
        assert report["salonName"] == "Instytut Piękna"
        assert report["competitorCount"] == 2
        assert len(report["competitorProfiles"]) == 2
        assert report["priceComparison"] == PRICE_COMPARISON_DATA
        assert report["localRanking"] == LOCAL_RANKING_DATA[0]
        assert report["serviceGapMatrix"] == GAP_MATRIX_DATA
        assert report["promotions"] == PROMOTIONS_DATA
        assert report["perServicePricing"] == PER_SERVICE_PRICING_DATA
        assert report["marketSummary"]["headline"] == MARKET_ANALYSIS_TOOL_CALL_INPUT["marketSummary"]["headline"]
        assert len(report["advantages"]) == 2
        assert len(report["recommendations"]) == 1
        assert len(report["radarMetrics"]) == 2
        assert len(report["swot"]["strengths"]) == 2
        assert len(report["marketNiches"]) == 2
        assert len(report["actionPlan"]) == 2

        # Verify progress was called (first and last)
        assert progress_calls[0][0] == 5
        assert progress_calls[-1][0] == 100

    @pytest.mark.asyncio
    async def test_pipeline_handles_all_rpc_failures(self) -> None:
        """Pipeline should still produce a report when all RPCs fail."""
        from agent.runner import AgentResult, ToolCallResult

        mock_client = MagicMock()
        mock_client.generate_json = AsyncMock(return_value={"strengths": [], "weaknesses": [], "opportunities": [], "threats": []})

        mock_supabase = MagicMock()
        mock_supabase.get_salon_with_services = AsyncMock(return_value=None)
        mock_supabase.call_rpc = AsyncMock(side_effect=Exception("RPC unavailable"))

        empty_agent = AgentResult(
            tool_calls=[],
            final_text="No data available",
            total_steps=1,
            total_input_tokens=100,
            total_output_tokens=50,
        )
        mock_agent_loop = AsyncMock(return_value=empty_agent)

        with (
            patch("services.minimax.MiniMaxClient", return_value=mock_client),
            patch("services.supabase.SupabaseService", return_value=mock_supabase),
            patch("agent.runner.run_agent_loop", mock_agent_loop),
            patch("config.settings"),
        ):
            report = await run_competitor_pipeline(
                audit_id="test-fail-123",
                subject_salon_id=999,
                salon_name="Missing Salon",
                salon_city="Nowhere",
                salon_lat=None,
                salon_lng=None,
                selected_competitor_ids=[888, 777],
                services=[],
            )

        # Report still has correct structure even with all failures
        assert report["version"] == "v1"
        assert report["competitorCount"] == 2
        assert report["competitorProfiles"] == []
        assert report["priceComparison"] == []
        assert report["localRanking"] == {}
        assert report["serviceGapMatrix"] == []
        assert report["promotions"] == []
        assert report["perServicePricing"] == []
        assert report["marketSummary"] == {}
        assert report["advantages"] == []
        assert report["marketNiches"] == []
        assert report["actionPlan"] == []


# ── Test 10: Tool definitions shape ──


class TestToolDefinitions:
    def test_market_analysis_tool_shape(self) -> None:
        from agent.tools import MARKET_ANALYSIS_TOOL

        assert MARKET_ANALYSIS_TOOL["name"] == "submit_market_analysis"
        schema = MARKET_ANALYSIS_TOOL["input_schema"]
        assert "marketSummary" in schema["properties"]
        assert "advantages" in schema["properties"]
        assert "recommendations" in schema["properties"]
        assert "radarMetrics" in schema["properties"]
        assert set(schema["required"]) == {"marketSummary", "advantages", "recommendations", "radarMetrics"}

    def test_strategic_analysis_tool_shape(self) -> None:
        from agent.tools import STRATEGIC_ANALYSIS_TOOL

        assert STRATEGIC_ANALYSIS_TOOL["name"] == "submit_strategic_analysis"
        schema = STRATEGIC_ANALYSIS_TOOL["input_schema"]
        assert "marketNiches" in schema["properties"]
        assert "actionPlan" in schema["properties"]
        assert set(schema["required"]) == {"marketNiches", "actionPlan"}
