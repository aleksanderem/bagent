"""Unit tests for the global per-MODEL LLM concurrency limiter.

Load test 2 (2026-06-13, prod) measured taxonomy.pass5_consistency for the
SAME salon at 65s solo but 1191s under 3x concurrency (~18x blowup) — pure
OpenAI contention. Increment 1 added a per-PROVIDER limiter that fixed pass5
(1191s -> 85s) but router_skip rose: router/hidden-inference/pricing-tier1/
pair-verify all use gpt-4o-mini, a SEPARATE OpenAI rate-limit bucket from the
gpt-4o pass5 uses. A single shared "openai" slot would starve the fast
gpt-4o-mini router behind slow gpt-4o pass5. Increment 2 therefore keys the
limiter by MODEL NAME: gpt-4o and gpt-4o-mini hold independent buckets.

arq runs one process / one event loop, so a MODULE-LEVEL asyncio.Semaphore is
shared across all concurrent jobs = exactly the global cap needed. These tests
verify the limiter:

  * caps in-flight calls per MODEL (cap=3 -> max-seen <= 3, all complete)
  * keeps MODEL slots independent — saturating gpt-4o never blocks gpt-4o-mini
    (THE CRUX of increment 2: separate OpenAI rate-limit buckets)
  * fails open on an unknown model (no limiting + a WARNING log)

Style mirrors tests/test_taxonomy_consistency_chunking.py: @pytest.mark.asyncio
on async tests, monkeypatch.setenv for caps, caplog for the warning assertion.
_reset_for_tests() clears the lazy semaphore registry per test for isolation.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from services.llm_rate_limiter import _reset_for_tests, provider_slot


@pytest.fixture(autouse=True)
def _clean_registry() -> None:
    """Clear the lazy per-model semaphore registry before AND after each test
    so the cap (fixed at first creation per model) can be re-set via env in the
    next test."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.mark.asyncio
async def test_cap_limits_in_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With LLM_MAX_CONCURRENCY_GPT_4O_MINI=3, at most 3 coroutines hold the
    gpt-4o-mini slot at once even when 20 try simultaneously; all 20 finish."""
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_GPT_4O_MINI", "3")

    live = 0
    max_seen = 0
    completed = 0

    async def worker() -> None:
        nonlocal live, max_seen, completed
        async with provider_slot("gpt-4o-mini"):
            live += 1
            max_seen = max(max_seen, live)
            # Yield repeatedly so overlap is real (other coroutines run while
            # this one holds the slot).
            await asyncio.sleep(0.01)
            live -= 1
            completed += 1

    await asyncio.gather(*[worker() for _ in range(20)])

    assert max_seen <= 3, f"observed {max_seen} in-flight, cap was 3"
    assert max_seen >= 1
    assert completed == 20


@pytest.mark.asyncio
async def test_models_are_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE CRUX of increment 2: saturating the gpt-4o slot (cap=1, held by a
    coroutine waiting on an Event) must NOT block a coroutine entering the
    gpt-4o-mini slot. gpt-4o and gpt-4o-mini are SEPARATE OpenAI rate-limit
    buckets, so a slow gpt-4o pass5 must never starve the fast gpt-4o-mini
    router/hidden/pricing-tier1/pair-verify call-sites."""
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_GPT_4O", "1")
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_GPT_4O_MINI", "4")

    release = asyncio.Event()
    mini_entered = asyncio.Event()

    async def hold_gpt4o() -> None:
        async with provider_slot("gpt-4o"):
            # Hold the single gpt-4o slot until released at the end.
            await release.wait()

    async def enter_gpt4o_mini() -> None:
        async with provider_slot("gpt-4o-mini"):
            mini_entered.set()

    holder = asyncio.create_task(hold_gpt4o())
    # Let the holder acquire the gpt-4o slot first.
    await asyncio.sleep(0.01)

    entrant = asyncio.create_task(enter_gpt4o_mini())
    # gpt-4o-mini must acquire IMMEDIATELY despite gpt-4o being saturated.
    await asyncio.wait_for(mini_entered.wait(), timeout=1.0)
    await entrant

    # Release the gpt-4o holder so the test cleans up.
    release.set()
    await holder


@pytest.mark.asyncio
async def test_unknown_model_passthrough_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unknown model passes through unlimited (fail-open) and logs a WARNING
    naming the unknown model."""
    entered = 0
    max_seen = 0
    live = 0

    async def worker() -> None:
        nonlocal entered, max_seen, live
        async with provider_slot("totally-unknown-model"):
            live += 1
            entered += 1
            max_seen = max(max_seen, live)
            await asyncio.sleep(0.01)
            live -= 1

    with caplog.at_level(logging.WARNING, logger="services.llm_rate_limiter"):
        await asyncio.gather(*[worker() for _ in range(10)])

    # No cap => all 10 ran truly concurrently (no slot serialized them).
    assert entered == 10
    assert max_seen == 10, f"expected unlimited passthrough, max-seen {max_seen}"
    # A warning was logged naming the unknown model.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected at least one WARNING for unknown model"
    assert any("totally-unknown-model" in r.getMessage() for r in warnings)
