"""Observability tests for pair_verification trace instrumentation
(quick 260613-m23 Task 1, P0 silent-fail surfacing).

verify_service_pairs gained an optional, keyword-only `tracer` param. When a
tracer is supplied:
  - every LLM batch call emits `pair_verify.llm_batch` with {candidates,
    duration_ms, batch_index} (P3 LLM latency)
  - any candidate the LLM silently skipped (kept by the permissive
    is_comparable=True default) is surfaced as an aggregate
    `pair_verify.no_verdict` trace {count, subject, booksy_tid, synthetic_tid}

This is observability-only: the RETURN VALUE and the no-verdict default
(is_comparable=True) are byte-identical to before. tracer=None must never
crash and must never change behaviour.

Zero live LLM / DB — llm_client + supabase cache methods are mocked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.pair_verification import _normalize_pair_name, verify_service_pairs


class _FakeTracer:
    """Capturing fake matching the TraceWriter.add signature."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def add(
        self,
        step: str,
        data: dict,
        *,
        salon_ref_id: Any = None,
        tokens_used: Any = None,
    ) -> None:
        self.calls.append(
            {"step": step, "data": data, "salon_ref_id": salon_ref_id}
        )

    def buffered(self) -> int:
        return len(self.calls)

    async def flush(self) -> int:
        return len(self.calls)

    def steps(self) -> list[str]:
        return [c["step"] for c in self.calls]

    def of(self, step: str) -> list[dict[str, Any]]:
        return [c for c in self.calls if c["step"] == step]


def _mock_supabase_empty_cache() -> AsyncMock:
    """SupabaseService stub: empty cache, insert/touch are no-ops."""
    sb = AsyncMock()
    sb.lookup_pair_verifications = AsyncMock(return_value={})
    sb.insert_pair_verifications = AsyncMock(return_value=None)
    sb.touch_pair_verifications = AsyncMock(return_value=None)
    return sb


def _verdict(index: int, name: str, is_comparable: bool = True) -> dict[str, Any]:
    return {
        "index": index,
        "competitor_name": name,
        "is_comparable": is_comparable,
        "confidence": 0.9,
        "reasoning": "ok",
        "rejection_reason": None,
    }


def _llm_returning(verdicts: list[dict[str, Any]]) -> MagicMock:
    llm = MagicMock()
    llm.generate_json = AsyncMock(return_value={"verdicts": verdicts})
    return llm


# ---------------------------------------------------------------------------
# Test 7a — no_verdict aggregate trace fires when LLM skips a candidate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_verdict_emits_aggregate_trace() -> None:
    tracer = _FakeTracer()
    # Two candidates, LLM only returns a verdict for index 1 → candidate 2
    # falls into the permissive default branch.
    llm = _llm_returning([_verdict(1, "Cand A")])
    supabase = _mock_supabase_empty_cache()

    result = await verify_service_pairs(
        subject_service_name="Botoks 1 okolica",
        candidate_competitor_names=["Cand A", "Cand B"],
        booksy_treatment_id=630,
        synthetic_treatment_id=None,
        supabase=supabase,
        llm_client=llm,
        audit_id="audit_x",
        tracer=tracer,
    )

    # Behaviour UNCHANGED: missing verdict defaults to comparable=True.
    norm_b = _normalize_pair_name("Cand B")
    assert result[norm_b]["is_comparable"] is True
    assert result[norm_b]["reasoning"] == "missing LLM verdict — kept by default"

    # Observability: exactly one aggregate no_verdict trace with count=1.
    no_verdict = tracer.of("pair_verify.no_verdict")
    assert len(no_verdict) == 1
    data = no_verdict[0]["data"]
    assert data["count"] == 1
    assert data["booksy_tid"] == 630
    assert data["synthetic_tid"] is None
    assert "Botoks" in data["subject"]


# ---------------------------------------------------------------------------
# Test 7b — no no_verdict trace when every candidate gets a verdict
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_verdict_trace_absent_when_all_resolved() -> None:
    tracer = _FakeTracer()
    llm = _llm_returning([_verdict(1, "Cand A"), _verdict(2, "Cand B")])
    supabase = _mock_supabase_empty_cache()

    await verify_service_pairs(
        subject_service_name="Botoks",
        candidate_competitor_names=["Cand A", "Cand B"],
        booksy_treatment_id=630,
        synthetic_treatment_id=None,
        supabase=supabase,
        llm_client=llm,
        audit_id="audit_x",
        tracer=tracer,
    )

    assert tracer.of("pair_verify.no_verdict") == []


# ---------------------------------------------------------------------------
# Test 8 — LLM latency trace per batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_llm_batch_latency_trace_emitted() -> None:
    tracer = _FakeTracer()
    llm = _llm_returning([_verdict(1, "Cand A"), _verdict(2, "Cand B")])
    supabase = _mock_supabase_empty_cache()

    await verify_service_pairs(
        subject_service_name="Botoks",
        candidate_competitor_names=["Cand A", "Cand B"],
        booksy_treatment_id=630,
        synthetic_treatment_id=None,
        supabase=supabase,
        llm_client=llm,
        audit_id="audit_x",
        tracer=tracer,
    )

    batches = tracer.of("pair_verify.llm_batch")
    assert len(batches) == 1
    data = batches[0]["data"]
    assert data["candidates"] == 2
    assert data["batch_index"] == 0
    assert isinstance(data["duration_ms"], int)
    assert data["duration_ms"] >= 0


# ---------------------------------------------------------------------------
# Test 8 — tracer=None is safe (no crash, behaviour unchanged)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tracer_none_is_safe() -> None:
    llm = _llm_returning([_verdict(1, "Cand A")])
    supabase = _mock_supabase_empty_cache()

    # No tracer passed at all (default None) + a missing verdict for Cand B.
    result = await verify_service_pairs(
        subject_service_name="Botoks",
        candidate_competitor_names=["Cand A", "Cand B"],
        booksy_treatment_id=630,
        synthetic_treatment_id=None,
        supabase=supabase,
        llm_client=llm,
        audit_id="audit_x",
    )
    norm_b = _normalize_pair_name("Cand B")
    assert result[norm_b]["is_comparable"] is True


# ---------------------------------------------------------------------------
# Cache-only path: no LLM batch (and therefore no llm_batch trace), and
# every candidate resolved from cache → no no_verdict trace either.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_cache_hit_emits_no_llm_or_no_verdict_trace() -> None:
    tracer = _FakeTracer()
    norm_a = _normalize_pair_name("Cand A")
    supabase = _mock_supabase_empty_cache()
    supabase.lookup_pair_verifications = AsyncMock(
        return_value={
            norm_a: {
                "is_comparable": True,
                "confidence": 0.9,
                "reasoning": "cached",
                "rejection_reason": None,
                "cached_id": 7,
            }
        }
    )
    llm = _llm_returning([])  # must not be called

    await verify_service_pairs(
        subject_service_name="Botoks",
        candidate_competitor_names=["Cand A"],
        booksy_treatment_id=630,
        synthetic_treatment_id=None,
        supabase=supabase,
        llm_client=llm,
        audit_id="audit_x",
        tracer=tracer,
    )

    llm.generate_json.assert_not_called()
    assert tracer.of("pair_verify.llm_batch") == []
    assert tracer.of("pair_verify.no_verdict") == []
