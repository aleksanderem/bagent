"""Pass 5 candidate hydration — BEAUTY_AUDIT-tfyb.

Measured on prod-shaped A/B input 2026-08-22 (BEAUTY_AUDIT-qqsv.2): 201/201
clusters of report 34 and 150/150 of report 181 shipped the line

    kandydaci Booksy (area-compatible, top-15):

with NOTHING under it. Every booksy_tid Pass 5 picked therefore came from
model memory, and the method/area gate in `_SYSTEM_PROMPT` — which only ever
talks about "kandydaci" — had nothing to gate.

Cause chain (all four links verified in code before this test was written):
  1. `apply_intra_salon_consistency` gates the RPC on
     `members[0].get("name_embedding")`.
  2. `SupabaseService._load_services_for_scrape` selects
     `embedding_applied_at`, never `name_embedding` — so no service dict
     entering Pass 5 has that key.
  3. `_resolve_service_taxonomy` (Rules 1-4) keeps its `emb_map` local and
     writes nothing back onto the service dicts.
  4. The only write-back that exists lives in `_compute_treatment_tier_rows`
     (Etap 5), which runs AFTER Pass 5.

`_hydrate_reference_embeddings` closes it with ONE batch query for the single
reference service of each mixed cluster. These tests pin:

  (a) with hydration, `match_taxonomy_candidates` is called with a REAL
      embedding and the rendered prompt lists candidates — and with hydration
      disabled (the pre-fix state) the same input yields an empty list;
  (b) hydration is exactly ONE `get_service_embeddings` call regardless of
      cluster count, with deduped ids;
  (c) a cluster whose service has no embedding row degrades to the old
      no-candidates path instead of raising, and a failing query does the same;
  (d) an embedding already present on the dict is neither refetched nor
      overwritten (the dev endpoint may pass services inline).

Harness mirrors tests/test_taxonomy_consistency_chunking.py: provider forced
to OpenAI, `OpenAITaxonomyClient` replaced by a recording fake, clusters
injected past build_clusters/find_mixed_clusters, `_apply_decision` stubbed.
No network, no DB.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

import services.hidden_service_inference  # noqa: F401 — needed for patch path
import services.taxonomy_consistency as tc

# 3-float stand-ins for the 1536-dim vectors; the RPC is mocked, only
# truthiness and identity matter here.
_EMB = [0.11, 0.22, 0.33]


def _decision_for(cluster_id: int) -> dict[str, Any]:
    return {
        "cluster_id": cluster_id,
        "type": "salon_synthetic",
        "canonical_name": f"Cluster {cluster_id}",
        "reasoning": "test",
    }


class _RecordingOAIClient:
    """Stand-in for OpenAITaxonomyClient — answers every cluster it is asked
    about and keeps the rendered prompts for inspection."""

    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def call_decisions_tool(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        tool_schema: dict[str, Any],
        max_tokens: int = 16384,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        self.prompts.append(user_prompt)
        ids = [int(m) for m in re.findall(r"### KLASTER #(\d+)", user_prompt)]
        usage = {"input": 0, "output": 0, "model": "fake-oai"}
        return [_decision_for(cid) for cid in ids], usage


def _candidates_block(prompt: str, cluster_id: int) -> list[str]:
    """Lines listed under 'kandydaci Booksy' for one cluster in the prompt."""
    chunk = prompt.split(f"### KLASTER #{cluster_id}\n", 1)[1]
    chunk = chunk.split("### KLASTER #", 1)[0]
    tail = chunk.split("kandydaci Booksy (area-compatible, top-15):\n", 1)[1]
    return [
        line.strip() for line in tail.splitlines()
        if line.strip().startswith("- tid=")
    ]


def _service(sid: int) -> dict[str, Any]:
    return {
        "id": sid,
        "name": f"Thunder laser nogi {sid}",
        "category_name": "Depilacja",
        "is_active": True,
        "booksy_treatment_id": None,
        "synthetic_treatment_id": None,
    }


def _mixed(n_clusters: int) -> list[Any]:
    """N single-member, brand-marked clusters. Service dicts carry NO
    `name_embedding` key — exactly what `_load_services_for_scrape` returns."""
    return [
        (("thunder", "laser", ("nogi",)), [_service(100 + i)])
        for i in range(n_clusters)
    ]


@pytest.fixture
def env_openai_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAXONOMY_PASS5_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-fake")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")


async def _run(
    *,
    mixed: list[Any],
    supabase: Any,
    client: _RecordingOAIClient,
    match_calls: list[Any],
    disable_hydration: bool = False,
) -> dict[str, int]:
    """Drive apply_intra_salon_consistency with no network/DB. Records every
    embedding handed to match_taxonomy_candidates in `match_calls`."""

    async def _fake_apply_decision(**kwargs):
        return 1

    async def _fake_match(_supabase, embedding, **kwargs):
        match_calls.append(embedding)
        return [
            {
                "tid": 240,
                "canonical_name": "Depilacja laserowa",
                "parent_canonical_name": "Depilacja",
                "similarity": 0.81,
            },
        ]

    async def _no_hydration(*_args, **_kwargs) -> int:
        return 0

    stack = [
        patch.object(tc, "build_clusters", return_value={"k": [{"id": 1}]}),
        patch.object(tc, "find_mixed_clusters", return_value=mixed),
        patch.object(tc, "_apply_decision", side_effect=_fake_apply_decision),
        patch(
            "services.hidden_service_inference.match_taxonomy_candidates",
            side_effect=_fake_match,
        ),
        patch(
            "services.openai_taxonomy_client.OpenAITaxonomyClient",
            return_value=client,
        ),
    ]
    if disable_hydration:
        stack.append(
            patch.object(
                tc, "_hydrate_reference_embeddings", side_effect=_no_hydration,
            )
        )

    from contextlib import ExitStack

    with ExitStack() as es:
        for ctx in stack:
            es.enter_context(ctx)
        settings_mock = es.enter_context(patch("config.settings"))
        settings_mock.openai_api_key = "sk-test-fake"
        return await tc.apply_intra_salon_consistency(
            services=[],
            supabase=supabase,
            minimax=AsyncMock(),
            audit_id=None,
            label="test",
            trace_collector=None,
            dry_run=True,
        )


def _supabase_returning(emb_by_id: dict[int, Any]) -> Any:
    sb = AsyncMock()
    sb.get_service_embeddings = AsyncMock(return_value=dict(emb_by_id))
    return sb


# ---------------------------------------------------------------------------
# (a) the regression itself: empty candidate list before, populated after
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_without_hydration_candidate_list_is_empty(
    env_openai_provider: None,
) -> None:
    """Pre-fix state, reproduced by disabling the hydration step: the RPC is
    never called and the prompt ships an empty candidate list — the exact
    shape found in payloads_r34/payloads_r181."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    sb = _supabase_returning({100: _EMB, 101: _EMB})

    await _run(
        mixed=_mixed(2), supabase=sb, client=client,
        match_calls=match_calls, disable_hydration=True,
    )

    assert match_calls == [], (
        "without hydration nothing may reach match_taxonomy_candidates"
    )
    assert _candidates_block(client.prompts[0], 1) == []
    assert _candidates_block(client.prompts[0], 2) == []


@pytest.mark.asyncio
async def test_hydration_puts_real_candidates_in_the_prompt(
    env_openai_provider: None,
) -> None:
    """Same input, hydration on: the RPC receives the actual embedding and
    the rendered prompt lists the Booksy candidate for every cluster."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    sb = _supabase_returning({100: _EMB, 101: _EMB})

    await _run(mixed=_mixed(2), supabase=sb, client=client, match_calls=match_calls)

    assert match_calls == [_EMB, _EMB], (
        f"match_taxonomy_candidates must get the hydrated vector, got {match_calls}"
    )
    for cluster_id in (1, 2):
        lines = _candidates_block(client.prompts[0], cluster_id)
        assert lines, f"cluster {cluster_id} still has an empty candidate list"
        assert "tid=240" in lines[0]
        assert "Depilacja laserowa" in lines[0]


# ---------------------------------------------------------------------------
# (b) one batch query, not N
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("n_clusters", [1, 12, 40])
async def test_hydration_is_a_single_batch_query(
    n_clusters: int,
    env_openai_provider: None,
) -> None:
    """One `get_service_embeddings` call for the whole report, whatever the
    cluster count (40 clusters also spans 2 LLM chunks — the query still
    happens once, before chunking)."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    ids = [100 + i for i in range(n_clusters)]
    sb = _supabase_returning({sid: _EMB for sid in ids})

    await _run(
        mixed=_mixed(n_clusters), supabase=sb, client=client,
        match_calls=match_calls,
    )

    assert sb.get_service_embeddings.await_count == 1, (
        f"expected 1 batch query, got {sb.get_service_embeddings.await_count}"
    )
    requested = sb.get_service_embeddings.await_args.args[0]
    assert sorted(requested) == ids
    assert len(requested) == len(set(requested)), "ids must be deduped"
    assert len(match_calls) == n_clusters


@pytest.mark.asyncio
async def test_duplicate_reference_ids_are_requested_once(
    env_openai_provider: None,
) -> None:
    """Two clusters headed by the same service id (caller passed the same
    salon twice) still produce ONE id in the query, and both get hydrated."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    svc_a = _service(100)
    svc_b = _service(100)
    mixed = [
        (("thunder", "laser", ("nogi",)), [svc_a]),
        (("thunder", "laser", ("pachy",)), [svc_b]),
    ]
    sb = _supabase_returning({100: _EMB})

    await _run(mixed=mixed, supabase=sb, client=client, match_calls=match_calls)

    assert sb.get_service_embeddings.await_args.args[0] == [100]
    assert svc_a["name_embedding"] == _EMB
    assert svc_b["name_embedding"] == _EMB
    assert match_calls == [_EMB, _EMB]


# ---------------------------------------------------------------------------
# (c) missing / failing embeddings must not break the run
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_service_without_embedding_row_does_not_break_the_run(
    env_openai_provider: None,
) -> None:
    """Cluster 2's service has no embedding in Supabase: it keeps the old
    no-candidates path, the other two clusters still get candidates, and the
    run completes with a decision for every cluster."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    sb = _supabase_returning({100: _EMB, 102: _EMB})  # 101 absent

    stats = await _run(
        mixed=_mixed(3), supabase=sb, client=client, match_calls=match_calls,
    )

    assert len(match_calls) == 2, "the embedding-less cluster must not call the RPC"
    assert _candidates_block(client.prompts[0], 1)
    assert _candidates_block(client.prompts[0], 2) == []
    assert _candidates_block(client.prompts[0], 3)
    assert stats["clusters_mixed"] == 3
    assert stats["rerouted"] == 3


@pytest.mark.asyncio
async def test_failing_embedding_query_degrades_instead_of_raising(
    env_openai_provider: None,
) -> None:
    """A dead Supabase call leaves Pass 5 exactly where it was before this
    fix (no candidates) rather than aborting the report."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    sb = AsyncMock()
    sb.get_service_embeddings = AsyncMock(side_effect=RuntimeError("PostgREST 500"))

    stats = await _run(
        mixed=_mixed(2), supabase=sb, client=client, match_calls=match_calls,
    )

    assert match_calls == []
    assert _candidates_block(client.prompts[0], 1) == []
    assert stats["rerouted"] == 2


@pytest.mark.asyncio
async def test_null_embedding_value_is_not_written_onto_the_service(
    env_openai_provider: None,
) -> None:
    """`get_service_embeddings` skips NULL rows, but a defensive check keeps a
    falsy value from landing on the dict and faking presence downstream."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    services = _mixed(1)
    sb = _supabase_returning({100: None})

    await _run(mixed=services, supabase=sb, client=client, match_calls=match_calls)

    assert "name_embedding" not in services[0][1][0]
    assert match_calls == []


# ---------------------------------------------------------------------------
# (d) never clobber an embedding the caller already supplied
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_existing_embedding_is_neither_refetched_nor_overwritten(
    env_openai_provider: None,
) -> None:
    """Services handed in with an embedding (dev endpoint `/api/dev/trace-
    taxonomy` passes them inline) are left alone; only the bare one is
    queried."""
    client = _RecordingOAIClient()
    match_calls: list[Any] = []
    preset = [0.9, 0.9, 0.9]
    with_emb = dict(_service(100), name_embedding=preset)
    without_emb = _service(101)
    mixed = [
        (("thunder", "laser", ("nogi",)), [with_emb]),
        (("thunder", "laser", ("pachy",)), [without_emb]),
    ]
    sb = _supabase_returning({100: _EMB, 101: _EMB})

    await _run(mixed=mixed, supabase=sb, client=client, match_calls=match_calls)

    assert sb.get_service_embeddings.await_args.args[0] == [101], (
        "only the service missing an embedding may be queried"
    )
    assert with_emb["name_embedding"] == preset, "preset embedding was clobbered"
    assert match_calls == [preset, _EMB]
