"""Integration tests for services/minimax.py — require real MiniMax API key."""

import pytest

from agent.tools import NAMING_TOOL
from config import settings
from services.minimax import MiniMaxClient


@pytest.fixture
def client() -> MiniMaxClient:
    """Create a MiniMaxClient from environment settings."""
    return MiniMaxClient(
        api_key=settings.minimax_api_key,
        base_url=settings.minimax_base_url,
        model=settings.minimax_model,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_basic_text_generation(client: MiniMaxClient):
    """Call generate_text with a simple Polish prompt and verify response."""
    result = await client.generate_text(
        prompt="Powiedz 'cześć' po polsku w jednym zdaniu.",
        system="Odpowiadaj krótko po polsku.",
    )
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_json_generation(client: MiniMaxClient):
    """Call generate_json, verify valid JSON dict returned."""
    result = await client.generate_json(
        prompt='Zwróć JSON z kluczami "name" i "age" dla fikcyjnej osoby.',
    )
    assert isinstance(result, dict)
    assert "name" in result
    assert "age" in result


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tool_use_single_turn(client: MiniMaxClient):
    """Call create_message with 3 services + NAMING_TOOL, verify tool_use in response."""
    services_text = (
        "Usługi do poprawienia:\n"
        "1. manicure hyb z zdobieniem\n"
        "2. strzyz damskie krotkie\n"
        "3. koloryzacja wlosow dlugich\n"
    )
    response = await client.create_message(
        system="Jesteś ekspertem od nazw usług beauty. Popraw nazwy używając narzędzia.",
        messages=[{"role": "user", "content": services_text}],
        tools=[NAMING_TOOL],
    )

    tool_uses = [b for b in response.content if b.type == "tool_use"]
    assert len(tool_uses) >= 1
    assert tool_uses[0].name == "submit_naming_results"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tool_use_multi_turn(client: MiniMaxClient):
    """Send tool_result after tool_use, verify M2.7 continues."""
    services_text = "Usługi: 1. manicure hyb"
    response1 = await client.create_message(
        system="Popraw nazwy usług beauty używając narzędzia.",
        messages=[{"role": "user", "content": services_text}],
        tools=[NAMING_TOOL],
    )

    tool_uses = [b for b in response1.content if b.type == "tool_use"]
    if not tool_uses:
        pytest.skip("Model did not use tool on first turn")

    # Send acknowledgment and continue
    messages = [
        {"role": "user", "content": services_text},
        {"role": "assistant", "content": response1.content},
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_uses[0].id,
                    "content": "OK. Kontynuuj.",
                }
            ],
        },
    ]
    response2 = await client.create_message(
        system="Popraw nazwy usług beauty używając narzędzia.",
        messages=messages,
        tools=[NAMING_TOOL],
    )

    # M2.7 should either provide more tool calls or end_turn
    assert response2.stop_reason in ("end_turn", "tool_use")


@pytest.mark.integration
def test_client_initialization():
    """Verify client initializes without errors (no API call)."""
    client = MiniMaxClient(
        api_key="test-key",
        base_url="https://api.minimax.io/anthropic",
        model="MiniMax-M2.7",
    )
    assert client.model == "MiniMax-M2.7"
    assert client.client is not None
