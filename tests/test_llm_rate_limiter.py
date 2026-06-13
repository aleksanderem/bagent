"""Unit tests for the global per-provider LLM concurrency limiter.

Load test 2 (2026-06-13, prod) measured taxonomy.pass5_consistency for the
SAME salon at 65s solo but 1191s under 3x concurrency (~18x blowup) — pure
OpenAI contention. Root cause: the pass5 semaphore is PER-REPORT, so 3 reports
x 7 chunks = up to 21 in-flight gpt-4o calls (@50 reports = up to 350) with NO
cross-report cap, overrunning OpenAI RPM/TPM into 429 + Retry-After backoff.

arq runs one process / one event loop, so a MODULE-LEVEL asyncio.Semaphore is
shared across all concurrent jobs = exactly the global cap needed. These tests
verify the limiter:

  * caps in-flight calls per provider (cap=3 -> max-seen <= 3, all complete)
  * keeps provider slots independent (saturating openai never blocks minimax)
  * fails open on an unknown provider (no limiting + a WARNING log)

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
    """Clear the lazy per-provider semaphore registry before AND after each
    test so the cap (fixed at first creation per provider) can be re-set via
    env in the next test."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.mark.asyncio
async def test_cap_limits_in_flight_to_three(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With LLM_MAX_CONCURRENCY_OPENAI=3, at most 3 coroutines hold the openai
    slot at once even when 20 try simultaneously; all 20 eventually finish."""
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_OPENAI", "3")

    live = 0
    max_seen = 0
    completed = 0

    async def worker() -> None:
        nonlocal live, max_seen, completed
        async with provider_slot("openai"):
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
async def test_providers_are_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Saturating the openai slot (cap=1, held by a coroutine waiting on an
    Event) must NOT block a coroutine entering the minimax slot."""
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_OPENAI", "1")
    monkeypatch.setenv("LLM_MAX_CONCURRENCY_MINIMAX", "4")

    release = asyncio.Event()
    minimax_entered = asyncio.Event()

    async def hold_openai() -> None:
        async with provider_slot("openai"):
            # Hold the single openai slot until released at the end.
            await release.wait()

    async def enter_minimax() -> None:
        async with provider_slot("minimax"):
            minimax_entered.set()

    holder = asyncio.create_task(hold_openai())
    # Let the holder acquire the openai slot first.
    await asyncio.sleep(0.01)

    entrant = asyncio.create_task(enter_minimax())
    # minimax must acquire immediately despite openai being saturated.
    await asyncio.wait_for(minimax_entered.wait(), timeout=1.0)
    await entrant

    # Release the openai holder so the test cleans up.
    release.set()
    await holder


@pytest.mark.asyncio
async def test_unknown_provider_passthrough_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unknown provider passes through unlimited (fail-open) and logs a
    WARNING naming the unknown provider."""
    entered = 0
    max_seen = 0
    live = 0

    async def worker() -> None:
        nonlocal entered, max_seen, live
        async with provider_slot("totally-unknown-provider"):
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
    # A warning was logged naming the unknown provider.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected at least one WARNING for unknown provider"
    assert any("totally-unknown-provider" in r.getMessage() for r in warnings)
