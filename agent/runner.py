"""Generic multi-turn agent loop for MiniMax M2.7 tool_use."""

from collections.abc import Callable
from dataclasses import dataclass, field

from services.minimax import MiniMaxClient


@dataclass
class ToolCallResult:
    name: str
    input: dict


@dataclass
class AgentResult:
    tool_calls: list[ToolCallResult] = field(default_factory=list)
    final_text: str = ""
    total_steps: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0


async def run_agent_loop(
    client: MiniMaxClient,
    system_prompt: str,
    user_message: str,
    tools: list[dict],
    max_steps: int = 30,
    on_step: Callable[[int, int], None] | None = None,
) -> AgentResult:
    """Multi-turn agent loop with tool_use.

    Flow:
    1. Send task + tools to M2.7
    2. M2.7 thinks (Interleaved Thinking) + calls tool with batch of results
    3. We acknowledge, append full response to conversation
    4. M2.7 continues until stop_reason="end_turn"
    5. Return all collected tool call results
    """
    messages: list[dict] = [{"role": "user", "content": user_message}]
    collected_calls: list[ToolCallResult] = []
    final_text = ""
    total_input_tokens = 0
    total_output_tokens = 0

    for step in range(max_steps):
        response = await client.create_message(system_prompt, messages, tools)

        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        # Append full assistant response (CRITICAL: includes thinking blocks)
        messages.append({"role": "assistant", "content": response.content})

        # Single pass: extract tool calls and latest text
        tool_calls = []
        for block in response.content:
            if block.type == "tool_use":
                tool_calls.append(block)
            elif block.type == "text":
                final_text = block.text

        # If no tool calls, agent is done
        if not tool_calls or response.stop_reason != "tool_use":
            if on_step:
                on_step(step + 1, len(collected_calls))
            break

        # Collect tool call data
        for call in tool_calls:
            collected_calls.append(ToolCallResult(name=call.name, input=call.input))

        if on_step:
            on_step(step + 1, len(collected_calls))

        # Send acknowledgments
        tool_results = [
            {
                "type": "tool_result",
                "tool_use_id": call.id,
                "content": "OK. Kontynuuj przetwarzanie pozostałych usług.",
            }
            for call in tool_calls
        ]
        messages.append({"role": "user", "content": tool_results})

    assistant_count = sum(1 for m in messages if m["role"] == "assistant")
    return AgentResult(
        tool_calls=collected_calls,
        final_text=final_text,
        total_steps=assistant_count,
        total_input_tokens=total_input_tokens,
        total_output_tokens=total_output_tokens,
    )
