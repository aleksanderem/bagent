"""OpenAI client for Pass 5 taxonomy consistency.

MiniMax M2.7 with interleaved thinking blocks empirically exhausts its
8K-32K token budget on reasoning BEFORE producing any tool call when
the prompt includes 25+ mixed clusters + method gate rules — leading
to `stop_reason=max_tokens, content_types=thinking` and zero
decisions. OpenAI gpt-4o (and gpt-4.1) don't have the interleaved-
thinking-bleed problem and reliably produce structured function calls.

This client mirrors the MiniMax interface so taxonomy_consistency.py
can switch providers via config without touching the prompt logic.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class OpenAITaxonomyClient:
    def __init__(self, api_key: str, model: str = "gpt-4o") -> None:
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def call_decisions_tool(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        tool_schema: dict[str, Any],
        max_tokens: int = 16384,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Forced function-call to `submit_taxonomy_decisions`. Returns
        a tuple of (parsed `decisions` array, usage dict). Usage dict
        has shape {"input": int, "output": int, "model": str} for
        persistence via TraceWriter. Raises on any deviation."""
        # OpenAI uses function-calling schema slightly different from
        # Anthropic's tool_use — function under `function` key.
        openai_tool = {
            "type": "function",
            "function": {
                "name": tool_schema["name"],
                "description": tool_schema["description"],
                "parameters": tool_schema["input_schema"],
            },
        }
        resp = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tools=[openai_tool],
            tool_choice={
                "type": "function",
                "function": {"name": tool_schema["name"]},
            },
            temperature=0.1,
            max_tokens=max_tokens,
        )
        if not resp.choices:
            raise RuntimeError("OpenAI returned no choices")
        msg = resp.choices[0].message
        tool_calls = getattr(msg, "tool_calls", None) or []
        if not tool_calls:
            raise RuntimeError(
                f"OpenAI Pass 5: no tool_calls in response "
                f"(finish_reason={resp.choices[0].finish_reason}, "
                f"content={(msg.content or '')[:200]!r})"
            )
        call = tool_calls[0]
        if call.function.name != tool_schema["name"]:
            raise RuntimeError(
                f"OpenAI called unexpected function {call.function.name!r}; "
                f"expected {tool_schema['name']!r}"
            )
        try:
            payload = json.loads(call.function.arguments)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"OpenAI function arguments not valid JSON: {e}\n"
                f"Raw: {call.function.arguments[:500]!r}"
            )
        decisions = payload.get("decisions")
        if not isinstance(decisions, list):
            raise RuntimeError(
                f"OpenAI function payload missing 'decisions' list: "
                f"keys={sorted(payload.keys())!r}"
            )
        logger.info(
            "OpenAITaxonomyClient: model=%s usage=%s decisions=%d",
            self.model, getattr(resp, "usage", None), len(decisions),
        )
        # Extract usage for trace persistence (mig 121). OpenAI SDK exposes
        # usage on the response with prompt_tokens / completion_tokens.
        usage_obj = getattr(resp, "usage", None)
        usage_dict: dict[str, Any] = {"model": self.model}
        if usage_obj is not None:
            usage_dict["input"] = int(getattr(usage_obj, "prompt_tokens", 0) or 0)
            usage_dict["output"] = int(getattr(usage_obj, "completion_tokens", 0) or 0)
        return decisions, usage_dict
