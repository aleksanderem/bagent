"""Tests for BAGENT #3 narrative tense selection — Bug 3+4 regression guard.

When optimization hasn't been applied, narrative must use conditional/
future tense so it doesn't claim changes that the user can't see yet.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pipelines.summary import _generate_summary_text


def _make_client(captured: dict[str, Any]) -> Any:
    """Fake MiniMax client that captures prompt + system for assertion."""

    class FakeClient:
        async def generate_text(self, prompt: str, system: str = "") -> str:
            captured["prompt"] = prompt
            captured["system"] = system
            return "Narrative text"

    return FakeClient()


@pytest.mark.asyncio
async def test_pending_optimization_uses_conditional_system() -> None:
    """System prompt must forbid past tense when optimization not applied."""
    captured: dict[str, Any] = {}
    client = _make_client(captured)
    await _generate_summary_text(
        client=client,
        salon_name="Beauty4ever",
        score_before=50,
        score_after=50,
        key_wins=[
            {"title": "Usuń symbol ✦", "before": "xxx ✦", "after": "xxx"},
        ],
        stats={},
        optimization_applied=False,
    )
    assert "warunkowym/przyszłym" in captured["system"]
    assert "usunęliśmy" in captured["system"]
    # Prompt body should also reflect the pending status
    prompt = captured["prompt"]
    assert "WYNIK OBECNY" in prompt
    # No "before→after" narrative — the pending prompt uses "MOŻLIWY WYNIK PO"
    # (aspirational cap) but not "WYNIK PRZED / WYNIK PO" pair.
    assert "WYNIK PRZED" not in prompt
    assert "88" in prompt  # mentions audit cap


@pytest.mark.asyncio
async def test_applied_optimization_uses_past_system() -> None:
    """System prompt must allow past tense when optimization applied."""
    captured: dict[str, Any] = {}
    client = _make_client(captured)
    await _generate_summary_text(
        client=client,
        salon_name="Beauty4ever",
        score_before=50,
        score_after=82,
        key_wins=[
            {"title": "Usunięto symbol ✦", "before": "xxx ✦", "after": "xxx"},
        ],
        stats={"namesImproved": 10, "descriptionsAdded": 5},
        optimization_applied=True,
    )
    assert "przeszłym" in captured["system"]
    prompt = captured["prompt"]
    assert "WYNIK PRZED" in prompt
    assert "WYNIK PO" in prompt


@pytest.mark.asyncio
async def test_fallback_pending_does_not_claim_changes() -> None:
    """Offline fallback (client failure) for pending state must be honest."""

    class FailingClient:
        async def generate_text(self, prompt: str, system: str = "") -> str:
            raise RuntimeError("MiniMax down")

    result = await _generate_summary_text(
        client=FailingClient(),
        salon_name="Beauty4ever",
        score_before=50,
        score_after=50,
        key_wins=[],
        stats={"namesImproved": 0, "descriptionsAdded": 0},
        optimization_applied=False,
    )
    assert "Uruchom optymalizację" in result
    # Must NOT say "zaktualizowano" or "dodano" — those imply action taken
    assert "Zaktualizowano" not in result
    assert "dodano/poprawiono" not in result


@pytest.mark.asyncio
async def test_fallback_applied_uses_past_tense() -> None:
    """Offline fallback for applied state uses past tense and actual stats."""

    class FailingClient:
        async def generate_text(self, prompt: str, system: str = "") -> str:
            raise RuntimeError("MiniMax down")

    result = await _generate_summary_text(
        client=FailingClient(),
        salon_name="Beauty4ever",
        score_before=50,
        score_after=82,
        key_wins=[],
        stats={"namesImproved": 12, "descriptionsAdded": 8},
        optimization_applied=True,
    )
    assert "Zaktualizowano 12" in result
    assert "dodano/poprawiono 8" in result
