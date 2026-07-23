"""Bezpiecznik pełnych nazw kategorii uciętych przez Booksy (2026-07-22)."""
import pytest

from pipelines.category_restructure import _fix_truncated_category_names


class _FakeClient:
    def __init__(self, response):
        self.response = response
        self.prompts = []

    async def generate_json(self, prompt, **kw):
        self.prompts.append(prompt)
        return self.response


PRICELIST = {
    "categories": [
        {"name": "Endermolift LPG Allianc...ing i ujędrnienie skóry",
         "services": [{"name": "Endermolift LPG twarz"}, {"name": "Lifting skóry LPG"}]},
        {"name": "Manicure", "services": [{"name": "Hybryda"}]},
    ]
}


@pytest.mark.asyncio
async def test_fixup_names_broken_categories():
    client = _FakeClient({"names": [{
        "current": "Endermolift LPG Allianc...ing i ujędrnienie skóry",
        "full": "Endermolift LPG Alliance – lifting skóry",
    }]})
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, PRICELIST, mapping, changes, "t")
    assert mapping["Endermolift LPG Allianc...ing i ujędrnienie skóry"] == \
        "Endermolift LPG Alliance – lifting skóry"
    assert changes[0]["reason"] == "Pełna nazwa zamiast uciętej przez Booksy"
    # Czysta kategoria nie trafiła do promptu
    assert "Manicure" not in client.prompts[0].split("USŁUGI")[0]


@pytest.mark.asyncio
async def test_fixup_rejects_bad_candidates_keeps_original():
    client = _FakeClient({"names": [{
        "current": "Endermolift LPG Allianc...ing i ujędrnienie skóry",
        "full": "Nadal ucięta...",  # zawiera "..." -> odrzut
    }]})
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, PRICELIST, mapping, changes, "t")
    assert mapping == {} and changes == []


@pytest.mark.asyncio
async def test_fixup_noop_when_all_clean():
    client = _FakeClient({"names": []})
    mapping, changes = {}, []
    await _fix_truncated_category_names(
        client, {"categories": [{"name": "Manicure", "services": []}]}, mapping, changes, "t",
    )
    assert client.prompts == []  # zero wywołań LLM


@pytest.mark.asyncio
async def test_exact_recovery_within_limit_skips_llm():
    """Pełna nazwa <=50 zn. wpleciona w opis usługi -> odzysk dosłowny, zero LLM."""
    client = _FakeClient({"names": []})
    pricelist = {"categories": [{
        "name": "Peeling kawitacyjny - o...enie skóry",
        "services": [{
            "name": "Peeling kawitacyjny twarzy",
            "description": "Zapraszamy na Peeling kawitacyjny - odświeżenie skóry w salonie.\nDalszy opis.",
        }],
    }]}
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, pricelist, mapping, changes, "t")
    assert mapping["Peeling kawitacyjny - o...enie skóry"] == \
        "Peeling kawitacyjny - odświeżenie skóry"
    assert changes[0]["reason"].startswith("Pełna nazwa odzyskana z opisów")
    assert client.prompts == []  # LLM nieużyty


@pytest.mark.asyncio
async def test_exact_recovery_over_limit_routes_to_llm_with_context():
    """Odzyskana pełna nazwa >50 zn. -> Booksy i tak by ją uciął; idzie do LLM
    jako PEŁNE BRZMIENIE, a finalna propozycja musi zmieścić się w limicie."""
    client = _FakeClient({"names": [{
        "current": "Peeling kawitacyjny - o...świeżenie skóry - twarz",
        "full": "Peeling kawitacyjny — odświeżenie skóry twarzy",
    }]})
    pricelist = {"categories": [{
        "name": "Peeling kawitacyjny - o...świeżenie skóry - twarz",
        "services": [{
            "name": "Peeling kawitacyjny twarzy",
            "description": "Zapraszamy na Peeling kawitacyjny - oczyszczenie i odświeżenie skóry - twarz w naszym salonie.\nDalszy opis.",
        }],
    }]}
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, pricelist, mapping, changes, "t")
    assert "PEŁNE BRZMIENIE" in client.prompts[0]
    assert "Peeling kawitacyjny - oczyszczenie i odświeżenie skóry - twarz" in client.prompts[0]
    final = mapping["Peeling kawitacyjny - o...świeżenie skóry - twarz"]
    assert final == "Peeling kawitacyjny — odświeżenie skóry twarzy" and len(final) <= 50


@pytest.mark.asyncio
async def test_exact_recovery_falls_back_to_llm_when_absent():
    client = _FakeClient({"names": [{
        "current": "Onda Coolwaves - redukc...i modelowanie sylwetki",
        "full": "Onda Coolwaves — redukcja tkanki tłuszczowej",
    }]})
    pricelist = {"categories": [{
        "name": "Onda Coolwaves - redukc...i modelowanie sylwetki",
        "services": [{"name": "Onda Coolwaves - 1 obszar", "description": "Mikrofale Coolwaves."}],
    }]}
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, pricelist, mapping, changes, "t")
    assert len(client.prompts) == 1  # LLM użyty tylko dla nieodzyskanej
    assert mapping["Onda Coolwaves - redukc...i modelowanie sylwetki"].startswith("Onda Coolwaves —")
