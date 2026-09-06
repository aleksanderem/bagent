"""GET /api/internal/settings — stan i podgląd pól Settings dla panelu admina."""

from __future__ import annotations

from fastapi.testclient import TestClient

from config import settings
from server import app

client = TestClient(app)


def test_requires_api_key():
    assert client.get("/api/internal/settings").status_code in (401, 422)
    assert client.get("/api/internal/settings", headers={"x-api-key": "zly"}).status_code == 401


def test_status_lists_fields_with_tail_and_hides_secret_values(monkeypatch):
    monkeypatch.setattr(settings, "minimax_api_key", "sk-cp-abcdefgh1234")
    monkeypatch.setattr(settings, "brave_search_api_key", "")
    monkeypatch.setattr(settings, "minimax_model", "MiniMax-M3")
    res = client.get("/api/internal/settings", headers={"x-api-key": settings.api_key})
    assert res.status_code == 200
    s = res.json()["settings"]
    assert s["MINIMAX_API_KEY"] == {"set": True, "tail": "1234"}
    assert s["BRAVE_SEARCH_API_KEY"] == {"set": False, "tail": ""}
    assert s["MINIMAX_MODEL"] == {"set": True, "tail": "x-M3", "value": "MiniMax-M3"}
    assert "value" not in s["MINIMAX_API_KEY"], "sekret nie wraca bez jawnej prośby"


def test_reveal_single_key(monkeypatch):
    monkeypatch.setattr(settings, "minimax_api_key", "sk-cp-abcdefgh1234")
    res = client.get("/api/internal/settings", params={"key": "MINIMAX_API_KEY"}, headers={"x-api-key": settings.api_key})
    assert res.status_code == 200
    assert res.json() == {"key": "MINIMAX_API_KEY", "value": "sk-cp-abcdefgh1234"}
    assert client.get("/api/internal/settings", params={"key": "NOPE"}, headers={"x-api-key": settings.api_key}).status_code == 404
