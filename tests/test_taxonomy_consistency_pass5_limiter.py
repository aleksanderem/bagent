"""Parity test: pass5 OpenAI chunk dispatch acquires the GLOBAL per-provider
slot (provider_slot("openai")) without changing decisions.

Wired as increment 1 of the global LLM limiter (beads BEAUTY_AUDIT-ii0, P0):
the pass5 per-report semaphore in taxonomy_consistency.py is PER-REPORT, so N
concurrent reports fan out to N x chunks in-flight gpt-4o calls with no
cross-report cap (LT2 measured ~18x pass5 blowup under 3x concurrency). The
limiter adds a process-wide ceiling that COMPOSES with the per-report sem.

This test proves the wire is in place and decision logic is untouched: it
reuses the harness from tests/test_taxonomy_consistency_chunking.py (the
_FakeOAIClient, the patches of build_clusters / find_mixed_clusters /
_apply_decision / match_taxonomy_candidates / OpenAITaxonomyClient / config.
settings, the env_openai_provider fixture) and adds a spy that wraps
provider_slot to count entries. With N=31 clusters at chunk_size=30 (-> 2
chunks), it asserts:

  * provider_slot("openai") entered exactly once per LLM call (== 2)
  * every entry named the openai provider
  * decisions/stats invariants match the chunking test (clusters_mixed == N,
    rerouted == N, each cluster_id decided exactly once)

The spy patches services.taxonomy_consistency.provider_slot — i.e. the limiter
is imported at MODULE TOP in taxonomy_consistency.py (unambiguous patch path).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

import services.hidden_service_inference  # noqa: F401 — needed for patch path
import services.taxonomy_consistency as tc


def _decision_for(cluster_id: int) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "type": "salon_synthetic",
        "canonical_name": f"Cluster {cluster_id}",
        "reasoning": "test",
    }


class _FakeOAIClient:
    """Stand-in for OpenAITaxonomyClient — returns one decision per requested
    cluster, mirroring tests/test_taxonomy_consistency_chunking.py."""

    def __init__(self) -> None:
        self.calls: list[int] = []  # clusters per call

    async def call_decisions_tool(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        tool_schema: dict[str, Any],
        max_tokens: int = 16384,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        import re
        ids = [int(m) for m in re.findall(r"### KLASTER #(\d+)", user_prompt)]
        self.calls.append(len(ids))
        usage = {"input": len(user_prompt) // 4, "output": 100 * len(ids),
                 "model": "fake-oai"}
        return [_decision_for(cid) for cid in ids], usage


@pytest.fixture
def env_openai_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAXONOMY_PASS5_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-fake")


@pytest.mark.asyncio
async def test_pass5_openai_acquires_provider_slot_once_per_call(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N=31 clusters, chunk_size=30 -> 2 chunks -> 2 LLM calls. The global
    provider_slot("openai") must be entered exactly once per LLM call, naming
    the openai provider, and decisions must be byte-identical to the existing
    chunking behavior (every cluster decided exactly once)."""
    n_clusters = 31
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")

    fake_clusters = {
        i: [{"id": i, "is_active": True, "name": f"svc{i}",
             "category_name": "test"}]
        for i in range(n_clusters)
    }
    fake_mixed = [
        ((f"Brand{i}", "laser", ("nogi",)),
         [{"id": i, "name": f"svc{i}", "category_name": "test",
           "is_active": True, "booksy_treatment_id": None,
           "synthetic_treatment_id": None, "name_embedding": None}])
        for i in range(n_clusters)
    ]

    fake_oai = _FakeOAIClient()
    applied_cluster_ids: list[int] = []

    async def _fake_apply_decision(**kwargs):
        applied_cluster_ids.append(kwargs["cid"])
        return 1

    async def _fake_match(*args, **kwargs):
        return []

    # Spy that wraps the real provider_slot semantics (pass-through) while
    # counting entries + the provider arg each call carries.
    slot_entries: list[str] = []

    @asynccontextmanager
    async def _spy_provider_slot(provider: str):
        slot_entries.append(provider)
        yield

    with patch.object(tc, "build_clusters", return_value=fake_clusters), \
         patch.object(tc, "find_mixed_clusters", return_value=fake_mixed), \
         patch.object(tc, "_apply_decision", side_effect=_fake_apply_decision), \
         patch.object(tc, "provider_slot", _spy_provider_slot), \
         patch("services.hidden_service_inference.match_taxonomy_candidates",
               side_effect=_fake_match), \
         patch("services.openai_taxonomy_client.OpenAITaxonomyClient",
               return_value=fake_oai), \
         patch("config.settings") as _settings:
        _settings.openai_api_key = "sk-test-fake"
        stats = await tc.apply_intra_salon_consistency(
            services=[],
            supabase=AsyncMock(),
            minimax=AsyncMock(),
            audit_id=None,
            label="test",
            trace_collector=None,
            dry_run=True,
        )

    # 31 clusters / chunk_size 30 -> 2 chunks -> 2 LLM calls.
    assert len(fake_oai.calls) == 2, (
        f"expected 2 LLM calls, got {len(fake_oai.calls)}"
    )
    # The global openai slot was entered exactly once per LLM call.
    assert len(slot_entries) == len(fake_oai.calls), (
        f"provider_slot entered {len(slot_entries)} times, expected "
        f"{len(fake_oai.calls)} (once per LLM call)"
    )
    assert all(p == "openai" for p in slot_entries), (
        f"all slot entries must name openai, got {slot_entries}"
    )
    # Decisions UNCHANGED vs. chunking behavior: every cluster decided once.
    assert sorted(applied_cluster_ids) == sorted(range(1, n_clusters + 1))
    assert stats["clusters_mixed"] == n_clusters
    assert stats["rerouted"] == n_clusters
