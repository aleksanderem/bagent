"""Tool definitions for agent loops (naming, descriptions, competitor report, optimization)."""

NAMING_TOOL: dict = {
    "name": "submit_naming_results",
    "description": (
        "Wyślij wynik oceny nazw usług. Wywołuj wielokrotnie "
        "— za każdym razem z kolejną partią 20-30 usług. "
        "MUSISZ ocenić KAŻDĄ usługę z cennika — albo proponując poprawkę, "
        "albo oznaczając ją jako alreadyOptimal=true."
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
                        "improved": {"type": "string", "description": "Ulepszona nazwa (max 80 znaków). Gdy alreadyOptimal=true, powinna być równa oryginalnej nazwie."},
                        "causedByIssueIndex": {
                            "type": "integer",
                            "description": (
                                "0-indexed reference to the issue in the "
                                "KONTEKST.PROBLEMY list shown in the prompt "
                                "that this rename addresses. Omit if the "
                                "rename is a general cleanup not tied to a "
                                "specific reported issue or when "
                                "alreadyOptimal=true."
                            ),
                        },
                        "alreadyOptimal": {
                            "type": "boolean",
                            "description": (
                                "Set to true when you explicitly verified "
                                "the service name is already optimal and "
                                "does not need changes. When true, "
                                "'improved' should equal 'name'. This is "
                                "distinct from simply omitting the service "
                                "— omission means 'not processed', "
                                "alreadyOptimal=true means 'explicitly "
                                "verified OK'."
                            ),
                        },
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
        "Wyślij wynik oceny opisów usług. Wywołuj wielokrotnie "
        "— za każdym razem z kolejną partią 20-30 usług. "
        "MUSISZ ocenić KAŻDĄ usługę z cennika — albo proponując nowy opis, "
        "albo oznaczając ją jako alreadyOptimal=true."
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
                        "newDescription": {"type": "string", "description": "Nowy opis (50-150 znaków). Gdy alreadyOptimal=true, może być równy oryginalnemu opisowi."},
                        "causedByIssueIndex": {
                            "type": "integer",
                            "description": (
                                "0-indexed reference to the issue in the "
                                "KONTEKST.PROBLEMY list shown in the prompt "
                                "that this new description addresses. Omit "
                                "if the description change is a general "
                                "cleanup not tied to a specific reported "
                                "issue or when alreadyOptimal=true."
                            ),
                        },
                        "alreadyOptimal": {
                            "type": "boolean",
                            "description": (
                                "Set to true when you explicitly verified "
                                "the service description is already optimal "
                                "and does not need changes. When true, "
                                "'newDescription' can equal the original "
                                "description. This is distinct from simply "
                                "omitting the service — omission means "
                                "'not processed', alreadyOptimal=true means "
                                "'explicitly verified OK'."
                            ),
                        },
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
    "description": "Wyślij analizę rynkową — podsumowanie, przewagi konkurencyjne, rekomendacje, metryki radar.",
    "input_schema": {
        "type": "object",
        "properties": {
            "marketSummary": {
                "type": "object",
                "properties": {
                    "headline": {"type": "string"},
                    "paragraphs": {"type": "array", "items": {"type": "string"}},
                    "keyInsights": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["headline", "paragraphs", "keyInsights"]
            },
            "advantages": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string"},
                        "type": {"type": "string", "enum": ["win", "loss", "neutral"]},
                        "description": {"type": "string"},
                        "impact": {"type": "string", "enum": ["high", "medium", "low"]}
                    },
                    "required": ["area", "type", "description", "impact"]
                }
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
                        "category": {"type": "string"}
                    },
                    "required": ["title", "description", "priority", "category"]
                }
            },
            "radarMetrics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "metric": {"type": "string"},
                        "salonValue": {"type": "number"},
                        "marketAvg": {"type": "number"}
                    },
                    "required": ["metric", "salonValue", "marketAvg"]
                }
            }
        },
        "required": ["marketSummary", "advantages", "recommendations", "radarMetrics"]
    }
}

STRATEGIC_ANALYSIS_TOOL: dict = {
    "name": "submit_strategic_analysis",
    "description": "Wyślij analizę strategiczną — nisze rynkowe, segmentację, rekomendacje strategiczne, plan działań. Wywołuj wielokrotnie jeśli dane są obszerne.",
    "input_schema": {
        "type": "object",
        "properties": {
            "marketNiches": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "opportunity": {"type": "string"}
                    },
                    "required": ["name", "description", "opportunity"]
                }
            },
            "actionPlan": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "timeline": {"type": "string"},
                        "priority": {"type": "string", "enum": ["high", "medium", "low"]},
                        "estimatedImpact": {"type": "string"}
                    },
                    "required": ["title", "description", "timeline", "priority"]
                }
            }
        },
        "required": ["marketNiches", "actionPlan"]
    }
}

CATEGORY_MAPPING_TOOL: dict = {
    "name": "submit_category_mapping",
    "description": "Wyślij propozycję nowej struktury kategorii — mapowanie usług do nowych/zmienionych kategorii.",
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalCategory": {"type": "string"},
                        "newCategory": {"type": "string"},
                        "services": {"type": "array", "items": {"type": "string"}},
                        "reason": {"type": "string"}
                    },
                    "required": ["originalCategory", "newCategory", "services", "reason"]
                }
            }
        },
        "required": ["mappings"]
    }
}

OPTIMIZED_SERVICES_TOOL: dict = {
    "name": "submit_optimized_services",
    "description": "Wyślij zoptymalizowane usługi. Wywołuj wielokrotnie — partia po 15-20 usług. NIGDY nie zmieniaj ceny, czasu trwania ani wariantów.",
    "input_schema": {
        "type": "object",
        "properties": {
            "services": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalName": {"type": "string", "description": "Oryginalna nazwa usługi (bez zmian)"},
                        "categoryName": {"type": "string", "description": "Kategoria (oryginalna lub nowa)"},
                        "newName": {"type": "string", "description": "Nowa/poprawiona nazwa (max 80 znaków)"},
                        "newDescription": {"type": "string", "description": "Nowy opis (50-200 znaków, korzyść klienta)"},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Max 2 tagi: Bestseller, Nowość, Premium, Promocja"},
                        "sortOrder": {"type": "number"}
                    },
                    "required": ["originalName", "categoryName", "newName"]
                }
            }
        },
        "required": ["services"]
    }
}
