"""MiniMax-M3 musi dostawac jawne thinking (domyslnie wylaczone po stronie API)."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from services.minimax import MiniMaxClient
from config import settings


def _client(model: str) -> MiniMaxClient:
    c = MiniMaxClient("k", "https://api.minimax.io/anthropic", model, enable_1m_context=False)
    fake = SimpleNamespace(content=[SimpleNamespace(type="text", text="ok")], stop_reason="end_turn")
    c.client.messages.create = AsyncMock(return_value=fake)
    return c


@pytest.mark.asyncio
async def test_m3_create_message_sends_adaptive_thinking(monkeypatch):
    monkeypatch.setattr(settings, "minimax_thinking", "adaptive")
    c = _client("MiniMax-M3")
    await c.create_message("sys", [{"role": "user", "content": "x"}])
    kw = c.client.messages.create.call_args.kwargs
    assert kw["thinking"] == {"type": "adaptive"}


@pytest.mark.asyncio
async def test_m2_does_not_send_thinking(monkeypatch):
    monkeypatch.setattr(settings, "minimax_thinking", "adaptive")
    c = _client("MiniMax-M2.7")
    await c.create_message("sys", [{"role": "user", "content": "x"}])
    assert "thinking" not in c.client.messages.create.call_args.kwargs


@pytest.mark.asyncio
async def test_empty_setting_sends_nothing(monkeypatch):
    monkeypatch.setattr(settings, "minimax_thinking", "")
    c = _client("MiniMax-M3")
    await c.generate_text("x")
    assert "thinking" not in c.client.messages.create.call_args.kwargs
