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

CATEGORY_MAPPING_TOOL: dict = {
    "name": "submit_category_mapping",
    "description": (
        "Wyślij mapowanie kategorii — przypisanie usług do nowych/zmienionych kategorii. "
        "Wywołaj raz z pełną listą mapowań."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalCategory": {"type": "string", "description": "Oryginalna nazwa kategorii"},
                        "newCategory": {"type": "string", "description": "Nowa nazwa kategorii"},
                        "reason": {"type": "string", "description": "Powód zmiany"},
                    },
                    "required": ["originalCategory", "newCategory"],
                },
            },
        },
        "required": ["mappings"],
    },
}

OPTIMIZED_SERVICES_TOOL: dict = {
    "name": "submit_optimized_services",
    "description": (
        "Wyślij zoptymalizowane usługi. Wywołuj wielokrotnie "
        "— za każdym razem z kolejną partią 15-20 usług. "
        "Przetwórz WSZYSTKIE usługi które tego wymagają."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "services": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalName": {"type": "string", "description": "Oryginalna nazwa usługi"},
                        "newName": {"type": "string", "description": "Zoptymalizowana nazwa (max 80 znaków)"},
                        "newDescription": {"type": "string", "description": "Nowy/poprawiony opis (50-150 znaków)"},
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Tagi usługi (max 5)",
                        },
                        "reason": {"type": "string", "description": "Powód zmiany"},
                    },
                    "required": ["originalName", "newName"],
                },
            },
        },
        "required": ["services"],
    },
}
