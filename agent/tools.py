"""Tool definitions for agent loops (naming, descriptions, market analysis, strategy)."""

NAMING_TOOL: dict = {
    "name": "submit_naming_results",
    "description": (
        "Wyślij poprawione nazwy usług. Wywołuj wielokrotnie "
        "— za każdym razem z kolejną partią usług. "
        "Przetwórz WSZYSTKIE usługi które tego wymagają."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "transformations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Oryginalna nazwa usługi"},
                        "improved": {"type": "string", "description": "Ulepszona nazwa (max 80 znaków)"},
                    },
                    "required": ["name", "improved"],
                },
            },
        },
        "required": ["transformations"],
    },
}

DESCRIPTION_TOOL: dict = {
    "name": "submit_description_results",
    "description": (
        "Wyślij poprawione opisy usług. Wywołuj wielokrotnie "
        "— za każdym razem z kolejną partią usług. "
        "Przetwórz WSZYSTKIE usługi które tego wymagają."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "transformations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "serviceName": {"type": "string", "description": "Nazwa usługi"},
                        "newDescription": {"type": "string", "description": "Nowy opis (50-150 znaków)"},
                    },
                    "required": ["serviceName", "newDescription"],
                },
            },
        },
        "required": ["transformations"],
    },
}

MARKET_ANALYSIS_TOOL: dict = {
    "name": "submit_market_analysis",
    "description": (
        "Wyślij analizę rynkową — podsumowanie, przewagi konkurencyjne, rekomendacje i metryki radarowe. "
        "Wywołaj JEDEN RAZ z pełną analizą."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "marketSummary": {
                "type": "object",
                "properties": {
                    "headline": {"type": "string", "description": "Nagłówek podsumowania rynkowego"},
                    "paragraphs": {"type": "array", "items": {"type": "string"}, "description": "Akapity analizy"},
                    "keyInsights": {"type": "array", "items": {"type": "string"}, "description": "Kluczowe wnioski"},
                },
                "required": ["headline", "paragraphs", "keyInsights"],
            },
            "advantages": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string", "description": "Obszar przewagi"},
                        "type": {"type": "string", "enum": ["win", "loss", "neutral"], "description": "Typ"},
                        "description": {"type": "string", "description": "Opis przewagi/straty"},
                        "impact": {"type": "string", "enum": ["high", "medium", "low"], "description": "Wpływ"},
                    },
                    "required": ["area", "type", "description", "impact"],
                },
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "estimatedImpactPln": {"type": "number"},
                        "priority": {"type": "number"},
                        "category": {"type": "string"},
                    },
                    "required": ["title", "description", "priority", "category"],
                },
            },
            "radarMetrics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "metric": {"type": "string"},
                        "salonValue": {"type": "number"},
                        "marketAvg": {"type": "number"},
                    },
                    "required": ["metric", "salonValue", "marketAvg"],
                },
            },
        },
        "required": ["marketSummary", "advantages", "recommendations", "radarMetrics"],
    },
}

STRATEGIC_ANALYSIS_TOOL: dict = {
    "name": "submit_strategic_analysis",
    "description": (
        "Wyślij analizę strategiczną — nisze rynkowe i plan działania. "
        "Możesz wywołać wielokrotnie z kolejnymi częściami analizy."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "marketNiches": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "niche": {"type": "string", "description": "Nazwa niszy"},
                        "opportunity": {"type": "string", "description": "Opis szansy"},
                        "competitorCount": {"type": "number", "description": "Liczba konkurentów w niszy"},
                        "potentialRevenue": {"type": "string", "description": "Szacowany potencjał przychodu"},
                    },
                    "required": ["niche", "opportunity"],
                },
            },
            "actionPlan": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string", "description": "Działanie do podjęcia"},
                        "timeline": {"type": "string", "description": "Horyzont czasowy"},
                        "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                        "expectedOutcome": {"type": "string", "description": "Oczekiwany rezultat"},
                        "estimatedCost": {"type": "string", "description": "Szacowany koszt"},
                    },
                    "required": ["action", "timeline", "priority", "expectedOutcome"],
                },
            },
        },
        "required": ["marketNiches", "actionPlan"],
    },
}
