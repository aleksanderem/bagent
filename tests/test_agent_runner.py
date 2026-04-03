"""Unit tests for agent/runner.py — agent loop logic."""

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from agent.runner import AgentResult, ToolCallResult, run_agent_loop
from services.minimax import MiniMaxClient


# ---------------------------------------------------------------------------
# Mock fixtures (inline, not in conftest.py)
# ---------------------------------------------------------------------------

@dataclass
class MockUsage:
    input_tokens: int = 100
    output_tokens: int = 50


@dataclass
class MockTextBlock:
    type: str = "text"
    text: str = "Done."


@dataclass
class MockToolUseBlock:
    type: str = "tool_use"
    id: str = "call_1"
    name: str = "submit_naming_results"
    input: dict = None

    def __post_init__(self):
        if self.input is None:
            self.input = {"transformations": [{"name": "Foo", "improved": "Bar"}]}


@dataclass
class MockResponse:
    content: list = None
    stop_reason: str = "end_turn"
    usage: MockUsage = None

    def __post_init__(self):
        if self.content is None:
            self.content = [MockTextBlock()]
        if self.usage is None:
            self.usage = MockUsage()


def _make_client() -> MiniMaxClient:
    """Create a MiniMaxClient with a mocked underlying anthropic client."""
    client = object.__new__(MiniMaxClient)
    client.client = AsyncMock()
    client.model = "test-model"
    client.create_message = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_completes_on_end_turn():
    """When M2.7 returns text only with stop_reason=end_turn, loop exits after 1 step."""
    client = _make_client()
    client.create_message.return_value = MockResponse(
        content=[MockTextBlock(text="All done.")],
        stop_reason="end_turn",
    )

    result = await run_agent_loop(
        client=client,
        system_prompt="You are a helper.",
        user_message="Hello",
        tools=[],
    )

    assert isinstance(result, AgentResult)
    assert result.tool_calls == []
    assert result.final_text == "All done."
    assert result.total_steps == 1
    assert client.create_message.await_count == 1


@pytest.mark.asyncio
async def test_agent_collects_tool_calls():
    """When M2.7 returns 3 tool_use blocks, all 3 are collected in result."""
    client = _make_client()

    tool_blocks = [
        MockToolUseBlock(id=f"call_{i}", name="submit_naming_results", input={"transformations": [{"name": f"svc_{i}", "improved": f"Svc {i}"}]})
        for i in range(3)
    ]

    # First call: 3 tool uses
    client.create_message.side_effect = [
        MockResponse(content=tool_blocks, stop_reason="tool_use"),
        MockResponse(content=[MockTextBlock(text="Finished.")], stop_reason="end_turn"),
    ]

    result = await run_agent_loop(
        client=client,
        system_prompt="System",
        user_message="Process services",
        tools=[{"name": "submit_naming_results"}],
    )

    assert len(result.tool_calls) == 3
    for i, tc in enumerate(result.tool_calls):
        assert tc.name == "submit_naming_results"
        assert tc.input["transformations"][0]["name"] == f"svc_{i}"
    assert result.final_text == "Finished."


@pytest.mark.asyncio
async def test_agent_maintains_conversation_history():
    """Multi-turn: messages list grows with assistant responses preserved."""
    client = _make_client()

    tool_block = MockToolUseBlock(id="call_1", name="submit_naming_results")

    client.create_message.side_effect = [
        MockResponse(content=[tool_block], stop_reason="tool_use"),
        MockResponse(content=[MockTextBlock(text="Done.")], stop_reason="end_turn"),
    ]

    result = await run_agent_loop(
        client=client,
        system_prompt="System",
        user_message="Go",
        tools=[{"name": "submit_naming_results"}],
    )

    # 2 calls to create_message
    assert client.create_message.await_count == 2

    # The messages list is shared and mutated, so after the loop it has all 4 entries:
    # user, assistant (tool_use), user (tool_result), assistant (end_turn)
    # Verify the conversation history is preserved correctly
    second_call_args = client.create_message.call_args_list[1]
    messages_arg = second_call_args[0][1]  # positional arg: messages (same list reference)

    assert messages_arg[0]["role"] == "user"
    assert messages_arg[1]["role"] == "assistant"
    assert messages_arg[2]["role"] == "user"

    # The assistant content should be the raw content blocks (preserved for thinking chain)
    assert messages_arg[1]["content"] == [tool_block]

    assert result.total_steps == 2


@pytest.mark.asyncio
async def test_agent_respects_max_steps():
    """When tool calls never stop, max_steps limit stops the loop."""
    client = _make_client()

    tool_block = MockToolUseBlock(id="call_1", name="submit_naming_results")
    # Always return tool_use, never end_turn
    client.create_message.return_value = MockResponse(
        content=[tool_block],
        stop_reason="tool_use",
    )

    result = await run_agent_loop(
        client=client,
        system_prompt="System",
        user_message="Go forever",
        tools=[{"name": "submit_naming_results"}],
        max_steps=5,
    )

    assert client.create_message.await_count == 5
    assert len(result.tool_calls) == 5


@pytest.mark.asyncio
async def test_agent_handles_api_error():
    """When create_message raises an exception, it propagates to the caller."""
    client = _make_client()
    client.create_message.side_effect = RuntimeError("API connection failed")

    with pytest.raises(RuntimeError, match="API connection failed"):
        await run_agent_loop(
            client=client,
            system_prompt="System",
            user_message="Go",
            tools=[],
        )
