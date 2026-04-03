"""Tool definitions for agent loops (naming, descriptions)."""

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
