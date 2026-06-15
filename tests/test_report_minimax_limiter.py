"""Global per-MODEL MiniMax slot wired into the AUDIT report pipeline.

2026-06-15 adversarial review (MEDIUM): the audit pipeline's MiniMax calls in
pipelines/report.py BYPASSED the cross-job provider_slot("MiniMax-M2.7") cap-4
bucket (services/llm_rate_limiter.py) that competitor_synthesis / hidden-inference
/ openai-synthesis / pass5 already honour. Audits moved to a dedicated report
worker (arq:reports, max_jobs=6); each audit fires 3 parallel MiniMax
generate_json (Phase 1) + 2 parallel run_agent_loop (Phase 2) + a category
restructure loop + a summary call — so ~6 concurrent audits = 18+ uncapped
in-flight MiniMax calls with NO cross-job backpressure.

These tests prove each audit MiniMax call-site now enters provider_slot exactly
once keyed "MiniMax-M2.7" while leaving the decision/parse logic untouched, that
the slot is RELEASED before the OpenAI fallback (a different bucket), and that the
real limiter caps concurrency WITHOUT deadlock (cap=2 -> max 2 in-flight, all
complete) and never throttles a SINGLE audit's 3 Phase-1 calls (cap=4 >= 3).

Style mirrors tests/test_hidden_service_inference_limiter.py +
tests/test_competitor_synthesis.py (spy on the module-top provider_slot symbol).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pipelines.category_restructure as catr
import pipelines.report as report


def _make_scraped() -> SimpleNamespace:
    """Minimal scraped_data accepted by build_full_pricelist_text."""
    return SimpleNamespace(
        salonName="Test Salon",
        salonCity="Warszawa",
        categories=[
            SimpleNamespace(
                name="Fryzjerstwo",
                services=[
                    SimpleNamespace(
                        name="Strzyżenie",
                        price="100 zł",
                        duration="30 min",
                        description="opis",
                    ),
                ],
            ),
        ],
    )


def _make_agent_result() -> SimpleNamespace:
    """Stand-in for agent.runner.AgentResult — only the fields read by the
    audit call-sites (no tool calls => parse loops are skipped)."""
    return SimpleNamespace(
        tool_calls=[],
        final_text="",
        total_steps=1,
        total_input_tokens=0,
        total_output_tokens=0,
    )


async def _noop_progress(pct: int, msg: str) -> None:
    pass


@pytest.fixture
def slot_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Pass-through spy on pipelines.report.provider_slot recording each key."""
    entries: list[str] = []

    @asynccontextmanager
    async def _spy(model: str):
        entries.append(model)
        yield

    monkeypatch.setattr(report, "provider_slot", _spy, raising=False)
    return entries


# ── Phase 1 — scoring: generate_json (naming / descriptions / structure) ──


@pytest.mark.asyncio
async def test_generate_json_resilient_acquires_minimax_slot(
    slot_spy: list[str],
) -> None:
    """_generate_json_resilient gates the MiniMax generate_json await behind
    provider_slot("MiniMax-M2.7") exactly once and returns the parsed dict +
    source unchanged."""
    client = SimpleNamespace(
        model="MiniMax-M2.7",
        generate_json=AsyncMock(return_value={"score": 12, "issues": []}),
    )

    result, source = await report._generate_json_resilient(client, "p", "s")

    assert slot_spy == ["MiniMax-M2.7"], slot_spy
    assert source == "minimax"
    assert result == {"score": 12, "issues": []}
    assert client.generate_json.await_count == 1


@pytest.mark.asyncio
async def test_generate_json_resilient_releases_slot_before_openai_fallback(
    slot_spy: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """On MiniMax failure the MiniMax slot is entered ONCE (for the attempt) then
    EXITED; the OpenAI fallback runs OUTSIDE pipelines.report.provider_slot
    (it owns its own gpt-4o-mini bucket) so we never hold the MiniMax slot while
    waiting on OpenAI."""
    client = SimpleNamespace(
        model="MiniMax-M2.7",
        generate_json=AsyncMock(side_effect=RuntimeError("minimax down")),
    )
    import services.openai_fallback as ofb

    monkeypatch.setattr(
        ofb,
        "generate_json_via_openai",
        AsyncMock(return_value={"score": 7, "issues": []}),
        raising=False,
    )

    result, source = await report._generate_json_resilient(client, "p", "s")

    # Entered once for the MiniMax attempt, NOT a second time for the fallback.
    assert slot_spy == ["MiniMax-M2.7"], slot_spy
    assert source == "openai_gpt_4o_mini"
    assert result == {"score": 7, "issues": []}


# ── Phase 2 — transformation agents: run_agent_loop (naming / descriptions) ──


@pytest.mark.asyncio
async def test_agent_naming_wraps_run_agent_loop_in_minimax_slot(
    slot_spy: list[str],
) -> None:
    run_loop = AsyncMock(return_value=_make_agent_result())
    client = SimpleNamespace(model="MiniMax-M2.7")

    out = await report._agent_naming(
        client, _make_scraped(), [], run_loop, {"name": "naming_tool"}, _noop_progress
    )

    assert slot_spy == ["MiniMax-M2.7"], slot_spy
    assert run_loop.await_count == 1
    assert out["transformations"] == []


@pytest.mark.asyncio
async def test_agent_descriptions_wraps_run_agent_loop_in_minimax_slot(
    slot_spy: list[str],
) -> None:
    run_loop = AsyncMock(return_value=_make_agent_result())
    client = SimpleNamespace(model="MiniMax-M2.7")

    out = await report._agent_descriptions(
        client, _make_scraped(), [], run_loop, {"name": "desc_tool"}, _noop_progress
    )

    assert slot_spy == ["MiniMax-M2.7"], slot_spy
    assert run_loop.await_count == 1
    assert out["transformations"] == []


# ── Step 7 — summary: generate_text ──


@pytest.mark.asyncio
async def test_generate_summary_acquires_minimax_slot(slot_spy: list[str]) -> None:
    client = SimpleNamespace(
        model="MiniMax-M2.7",
        generate_text=AsyncMock(return_value="Podsumowanie audytu."),
    )
    stats = {"totalServices": 1, "totalCategories": 1, "servicesWithDescription": 1}

    summary = await report._generate_summary(client, 80, stats, [], "Test Salon")

    assert slot_spy == ["MiniMax-M2.7"], slot_spy
    assert summary == "Podsumowanie audytu."
    assert client.generate_text.await_count == 1


# ── Step 6.5 — category restructuring: run_agent_loop (audit-only module) ──


@pytest.mark.asyncio
async def test_restructure_categories_wraps_run_agent_loop_in_minimax_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """restructure_categories (called ONLY by the audit pipeline) gates its agent
    loop behind provider_slot("MiniMax-M2.7")."""
    entries: list[str] = []

    @asynccontextmanager
    async def _spy(model: str):
        entries.append(model)
        yield

    monkeypatch.setattr(catr, "provider_slot", _spy, raising=False)
    monkeypatch.setattr(
        "agent.runner.run_agent_loop", AsyncMock(return_value=_make_agent_result())
    )

    client = SimpleNamespace(model="MiniMax-M2.7")
    pricelist = {"salonName": "X", "categories": [{"name": "C", "services": []}]}

    mapping, changes, status = await catr.restructure_categories(
        client=client,
        transformed_pricelist=pricelist,
        top_issues=[],
        audit_id="a1",
        on_progress=_noop_progress,
    )

    assert entries == ["MiniMax-M2.7"], entries
    assert status == "ok"
    assert mapping == {}
    assert changes == []


# ── Real limiter: backpressure + no deadlock ──


@pytest.mark.asyncio
async def test_phase1_real_limiter_caps_in_flight_no_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REAL provider_slot: with cap=2, at most 2 of 6 concurrent Phase-1
    generate_json run at once and ALL 6 complete — the semaphore enforces
    backpressure and asyncio.gather does not deadlock."""
    from services.llm_rate_limiter import _reset_for_tests

    monkeypatch.setenv("LLM_MAX_CONCURRENCY_MINIMAX_M2_7", "2")
    _reset_for_tests()
    try:
        live = 0
        max_live = 0

        async def _gen_json(prompt: str, system: str | None = None) -> dict:
            nonlocal live, max_live
            live += 1
            max_live = max(max_live, live)
            await asyncio.sleep(0.02)
            live -= 1
            return {"score": 10, "issues": []}

        client = SimpleNamespace(model="MiniMax-M2.7", generate_json=_gen_json)

        results = await asyncio.gather(
            *[report._generate_json_resilient(client, "p", "s") for _ in range(6)]
        )

        assert max_live == 2, f"cap=2 not enforced, saw {max_live} in-flight"
        assert len(results) == 6
        assert all(src == "minimax" for _, src in results)
    finally:
        _reset_for_tests()


@pytest.mark.asyncio
async def test_single_audit_phase1_not_throttled_under_default_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cap=4 (default) >= 3 => a SINGLE audit's 3 parallel Phase-1 calls all run
    concurrently: no self-starvation, zero added latency for a solo audit.
    Guards the invariant that the MiniMax cap must stay >= 3."""
    from services.llm_rate_limiter import _reset_for_tests

    monkeypatch.delenv("LLM_MAX_CONCURRENCY_MINIMAX_M2_7", raising=False)
    _reset_for_tests()
    try:
        live = 0
        max_live = 0

        async def _gen_json(prompt: str, system: str | None = None) -> dict:
            nonlocal live, max_live
            live += 1
            max_live = max(max_live, live)
            await asyncio.sleep(0.02)
            live -= 1
            return {"score": 10, "issues": []}

        client = SimpleNamespace(model="MiniMax-M2.7", generate_json=_gen_json)

        # Mirror run_audit_pipeline Phase 1: 3 concurrent generate_json.
        results = await asyncio.gather(
            *[report._generate_json_resilient(client, "p", "s") for _ in range(3)]
        )

        assert max_live == 3, f"solo audit throttled: only {max_live}/3 ran at once"
        assert len(results) == 3
    finally:
        _reset_for_tests()
