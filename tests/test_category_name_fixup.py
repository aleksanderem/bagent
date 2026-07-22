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
        "full": "Endermolift LPG Alliance – lifting i ujędrnienie skóry",
    }]})
    mapping, changes = {}, []
    await _fix_truncated_category_names(client, PRICELIST, mapping, changes, "t")
    assert mapping["Endermolift LPG Allianc...ing i ujędrnienie skóry"] == \
        "Endermolift LPG Alliance – lifting i ujędrnienie skóry"
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
