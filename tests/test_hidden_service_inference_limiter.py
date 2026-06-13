"""Parity test: GeminiLLMClient.generate_json acquires the GLOBAL per-MODEL
slot (provider_slot(self.model), default "gpt-4o-mini") once per call without
changing the parsed result.

Increment 2 of the global LLM limiter (beads BEAUTY_AUDIT-ii0, P0). The
parity load test (LT2, 2026-06-13 prod) proved increment 1's per-PROVIDER
limiter fixed pass5 (gpt-4o) but router_skip rose: router, hidden-inference,
pricing-tier1 and pair-verify all use gpt-4o-mini via this SINGLE
GeminiLLMClient.generate_json, a SEPARATE OpenAI rate-limit bucket. Wrapping
the one create() await here caps all four gpt-4o-mini call-sites with the
gpt-4o-mini bucket (independent of the slow gpt-4o pass5 bucket).

This test proves the wire is in place and the decision/parse logic is
untouched: it stubs self._client so no real AsyncOpenAI call happens, spies on
provider_slot to count entries + the key (== self.model), and asserts the
returned dict is the parsed JSON. provider_slot is patched as a module
attribute of services.hidden_service_inference (module-top import).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import services.hidden_service_inference as hsi
from services.hidden_service_inference import GeminiLLMClient


@pytest.mark.asyncio
async def test_generate_json_acquires_provider_slot_once_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """generate_json must enter provider_slot exactly once, keyed by the
    client model (default gpt-4o-mini), and return the parsed JSON dict
    unchanged (decision/parse logic untouched — limiter only gates the await)."""
    # Build the client with a fake key, then overwrite _client with a stub so
    # the real AsyncOpenAI ctor (which still runs in __init__) is bypassed for
    # the actual call.
    client = GeminiLLMClient(api_key="sk-test", model="gpt-4o-mini")

    fake_resp = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content='{"ok": true}'),
            )
        ],
        usage=SimpleNamespace(prompt_tokens=12, completion_tokens=3),
    )
    create_mock = AsyncMock(return_value=fake_resp)
    client._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock)),
    )

    # Spy wrapping provider_slot semantics (pass-through) while recording the
    # key each entry carries.
    slot_entries: list[str] = []

    @asynccontextmanager
    async def _spy_provider_slot(model: str):
        slot_entries.append(model)
        yield

    monkeypatch.setattr(hsi, "provider_slot", _spy_provider_slot)

    result = await client.generate_json("p", system="s")

    # Slot entered exactly once, keyed by the client model.
    assert slot_entries == ["gpt-4o-mini"], (
        f"expected provider_slot entered once == ['gpt-4o-mini'], got "
        f"{slot_entries}"
    )
    # The underlying OpenAI call happened exactly once.
    assert create_mock.await_count == 1
    # Decision/parse UNCHANGED: returns the parsed JSON dict.
    assert result == {"ok": True}
    # Token accounting still works (stays OUTSIDE the slot but runs per call).
    assert client.total_input_tokens == 12
    assert client.total_output_tokens == 3
    assert client.total_calls == 1
