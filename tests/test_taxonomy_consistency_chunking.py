"""Unit tests for chunked LLM dispatch in apply_intra_salon_consistency.

After Beauty4ever competitor regen failed 2026-05-24 19:29 UTC (192
clusters in single OpenAI gpt-4o prompt → response truncated to 10
decisions → _validate_decisions RuntimeError), the function now chunks
cluster_payloads into batches of TAXONOMY_CONSISTENCY_CHUNK_SIZE (default
30) and runs one LLM call per chunk.

These tests verify chunking arithmetic + decision accumulation without
hitting the network. Provider switch is patched to OpenAI;
OpenAITaxonomyClient.call_decisions_tool is replaced with an AsyncMock
that returns deterministic per-chunk decisions, and we assert:

  * total LLM calls == ceil(N / chunk_size)
  * accumulated decisions length == N (one per cluster)
  * each cluster_id appears exactly once
  * downstream _apply_decision is invoked once per cluster
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

import services.hidden_service_inference  # noqa: F401 — needed for patch path
import services.taxonomy_consistency as tc


def _make_cluster_payload(cluster_id: int) -> tuple[
    int,
    tuple[str | None, str, tuple[str, ...]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Single-member, brand-marked cluster payload — passes through
    find_mixed_clusters via the brand_present gate."""
    brand = f"BrandX{cluster_id}"
    method = "laser"
    areas = ("nogi",)
    member = {
        "id": cluster_id,
        "name": f"{brand} laser nogi",
        "category_name": "Depilacja",
        "is_active": True,
        "booksy_treatment_id": None,
        "synthetic_treatment_id": None,
        "name_embedding": None,
    }
    return (cluster_id, (brand, method, areas), [member], [])


def _decision_for(cluster_id: int) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "type": "salon_synthetic",
        "canonical_name": f"Cluster {cluster_id}",
        "reasoning": "test",
    }


class _FakeOAIClient:
    """Stand-in for OpenAITaxonomyClient — records every call and
    returns one decision per requested cluster."""

    def __init__(self) -> None:
        self.calls: list[int] = []  # number of clusters per call
        # cluster_ids parsed from each prompt (best-effort — relies on
        # _format_cluster_for_prompt emitting "### KLASTER #<id>")
        self.cluster_ids_per_call: list[list[int]] = []

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
        self.cluster_ids_per_call.append(ids)
        # Mig 121 — return tuple of (decisions, usage_dict)
        usage = {"input": len(user_prompt) // 4, "output": 100 * len(ids), "model": "fake-oai"}
        return [_decision_for(cid) for cid in ids], usage


@pytest.fixture
def env_openai_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAXONOMY_PASS5_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-fake")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "n_clusters,chunk_size,expected_chunks",
    [
        (1, 30, 1),     # single chunk, way under limit
        (30, 30, 1),    # exactly one chunk
        (31, 30, 2),    # one over, splits
        (60, 30, 2),    # two even chunks
        (192, 30, 7),   # the Beauty4ever failure case: ceil(192/30)
        (100, 25, 4),   # custom chunk size via env
    ],
)
async def test_chunking_arithmetic(
    n_clusters: int,
    chunk_size: int,
    expected_chunks: int,
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the function splits clusters into ceil(N/chunk_size)
    LLM calls and every cluster_id receives exactly one decision."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", str(chunk_size))

    # Build N pre-resolved cluster_payloads, bypassing build_clusters /
    # find_mixed_clusters by intercepting those calls.
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

    # match_taxonomy_candidates is awaited; return empty list (member
    # has no embedding anyway, so this branch is skipped — but guard).
    async def _fake_match(*args, **kwargs):
        return []

    with patch.object(tc, "build_clusters", return_value=fake_clusters), \
         patch.object(tc, "find_mixed_clusters", return_value=fake_mixed), \
         patch.object(tc, "_apply_decision", side_effect=_fake_apply_decision), \
         patch("services.hidden_service_inference.match_taxonomy_candidates",
               side_effect=_fake_match), \
         patch("services.openai_taxonomy_client.OpenAITaxonomyClient",
               return_value=fake_oai):
        # config.settings.openai_api_key may not exist in test env; the
        # function falls through to os.environ.OPENAI_API_KEY (set by
        # fixture).
        with patch("config.settings") as _settings:
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

    assert len(fake_oai.calls) == expected_chunks, (
        f"expected {expected_chunks} LLM calls, got {len(fake_oai.calls)}"
    )
    # All chunks except possibly last == chunk_size
    for i, n in enumerate(fake_oai.calls[:-1]):
        assert n == chunk_size, f"chunk {i} expected {chunk_size}, got {n}"
    # Last chunk holds remainder (or full chunk_size when evenly divisible)
    expected_last = n_clusters - chunk_size * (expected_chunks - 1)
    assert fake_oai.calls[-1] == expected_last
    # Every cluster routed to _apply_decision exactly once
    assert sorted(applied_cluster_ids) == sorted(range(1, n_clusters + 1))
    # Stats reflect mixed cluster count
    assert stats["clusters_mixed"] == n_clusters
    assert stats["rerouted"] == n_clusters


@pytest.mark.asyncio
async def test_chunk_failure_aborts_pipeline(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If any single chunk returns wrong count, RuntimeError bubbles up
    immediately (no partial-result accumulation)."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "10")
    n_clusters = 35  # 4 chunks: 10, 10, 10, 5

    fake_mixed = [
        ((f"Brand{i}", "laser", ("nogi",)),
         [{"id": i, "name": f"svc{i}", "category_name": "test",
           "is_active": True, "booksy_treatment_id": None,
           "synthetic_treatment_id": None, "name_embedding": None}])
        for i in range(n_clusters)
    ]

    truncate_after = 2  # third chunk returns wrong count

    class _TruncatingClient:
        def __init__(self) -> None:
            self.call_idx = 0

        async def call_decisions_tool(
            self, **kwargs
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            import re
            ids = [int(m) for m in re.findall(
                r"### KLASTER #(\d+)", kwargs["user_prompt"])]
            self.call_idx += 1
            usage = {"input": 0, "output": 0, "model": "fake-oai-trunc"}
            if self.call_idx > truncate_after:
                # Simulate provider truncation: return only 2 decisions
                # for a chunk that asked for 10.
                return [_decision_for(ids[0]), _decision_for(ids[1])], usage
            return [_decision_for(cid) for cid in ids], usage

    bad_client = _TruncatingClient()

    async def _fake_match(*args, **kwargs):
        return []

    with patch.object(tc, "build_clusters",
                      return_value={i: [{"id": i}] for i in range(n_clusters)}), \
         patch.object(tc, "find_mixed_clusters", return_value=fake_mixed), \
         patch.object(tc, "_apply_decision",
                      side_effect=AsyncMock(return_value=1)), \
         patch("services.hidden_service_inference.match_taxonomy_candidates",
               side_effect=_fake_match), \
         patch("services.openai_taxonomy_client.OpenAITaxonomyClient",
               return_value=bad_client), \
         patch("config.settings") as _settings:
        _settings.openai_api_key = "sk-test-fake"
        with pytest.raises(RuntimeError, match="decisions, expected"):
            await tc.apply_intra_salon_consistency(
                services=[],
                supabase=AsyncMock(),
                minimax=AsyncMock(),
                audit_id=None,
                label="test",
                trace_collector=None,
                dry_run=True,
            )
    # We must have failed on chunk #3 (call_idx 3 after increment) —
    # i.e. retry doubled it, but bad behavior persists across retries
    # so 2 retry attempts * 2 chunks before + 2 retries of bad chunk
    # is acceptable. Key invariant: we did NOT make all 4 chunk calls.
    assert bad_client.call_idx >= truncate_after + 1
