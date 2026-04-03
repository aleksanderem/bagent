"""Shared test fixtures for bagent tests."""

import pytest


@pytest.fixture
def sample_scraped_data() -> dict:
    """Small salon with 3 categories, ~10 services."""
    return {
        "salonName": "Beauty Salon Test",
        "salonAddress": "ul. Testowa 1, Warszawa",
        "salonLogoUrl": None,
        "totalServices": 10,
        "categories": [
            {
                "name": "Fryzjerstwo",
                "services": [
                    {
                        "name": "Strzyżenie damskie",
                        "price": "120 zł",
                        "duration": "45 min",
                        "description": "Profesjonalne strzyżenie",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Koloryzacja",
                        "price": "250 zł",
                        "duration": "120 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Modelowanie",
                        "price": "80 zł",
                        "duration": "30 min",
                        "description": "Suszenie i modelowanie",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "STRZYŻENIE MĘSKIE...",
                        "price": "60 zł",
                        "duration": "30 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                ],
            },
            {
                "name": "Kosmetyka",
                "services": [
                    {
                        "name": "Manicure hybrydowy",
                        "price": "100 zł",
                        "duration": "60 min",
                        "description": "Trwały manicure z lakierem hybrydowym",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Pedicure",
                        "price": "120 zł",
                        "duration": "75 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Henna brwi +rzęs",
                        "price": "od 50 zł",
                        "duration": "30 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                ],
            },
            {
                "name": "Masaż",
                "services": [
                    {
                        "name": "Masaż relaksacyjny",
                        "price": "150 zł",
                        "duration": "60 min",
                        "description": "Relaksujący masaż całego ciała",
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Masaż sportowy",
                        "price": "180 zł",
                        "duration": "60 min",
                        "description": None,
                        "imageUrl": None,
                        "variants": None,
                    },
                    {
                        "name": "Drenaż limfatyczny",
                        "price": "200 zł",
                        "duration": "90 min",
                        "description": "Wspomaganie krążenia limfatycznego",
                        "imageUrl": None,
                        "variants": [
                            {"label": "Nogi", "price": "100 zł", "duration": "45 min"},
                            {"label": "Całe ciało", "price": "200 zł", "duration": "90 min"},
                        ],
                    },
                ],
            },
        ],
    }


@pytest.fixture
def large_scraped_data() -> dict:
    """Large salon with many services for stress testing."""
    categories = []
    service_count = 0
    for i in range(15):
        services = []
        for j in range(12):
            services.append(
                {
                    "name": f"Usługa {i + 1}-{j + 1}",
                    "price": f"{50 + j * 20} zł",
                    "duration": f"{30 + j * 15} min",
                    "description": f"Opis usługi {i + 1}-{j + 1}" if j % 2 == 0 else None,
                    "imageUrl": None,
                    "variants": None,
                }
            )
            service_count += 1
        categories.append({"name": f"Kategoria {i + 1}", "services": services})
    return {
        "salonName": "Duży Salon Beauty",
        "salonAddress": "ul. Długa 10, Kraków",
        "salonLogoUrl": None,
        "totalServices": service_count,
        "categories": categories,
    }


@pytest.fixture
def api_key() -> str:
    """Test API key matching config."""
    return "test-api-key-12345"
