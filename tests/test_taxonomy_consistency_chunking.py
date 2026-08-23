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

CONTRACT CHANGE 2026-06-14 (BEAUTY_AUDIT-8j8): a chunk missing only a FEW
cluster_ids no longer aborts the report — those clusters are re-asked and,
failing that, get a deterministic salon_synthetic stand-in
(`test_small_shortfall_recovers_via_rescue`; full rescue coverage in
tests/test_taxonomy_consistency_missing_clusters.py).

The abort is NOT gone, only narrowed: an empty or heavily truncated first
response — more than tc._MAX_MISSING_FRACTION_FOR_RESCUE of the chunk
undecided — is a provider failure and still raises, which is what
`test_chunk_failure_aborts_pipeline` below pins. Without that gate a
provider outage would produce a "successful" report standing entirely on
stand-in decisions.

CONTRACT CHANGE 2026-08-23 (BEAUTY_AUDIT-kfgx): that fraction gate was
unreachable-by-design on a tiny chunk — one skipped cluster out of one is
a 100% shortfall — so 31 mixed clusters at chunk_size=30 left a
one-cluster tail and died with the pre-8j8 message. Now a remainder of at
most tc._MAX_MERGEABLE_TAIL_CLUSTERS joins the previous chunk (largest
chunk 30 -> 32, bounded), and a chunk below
tc._MIN_CHUNK_FOR_PROVIDER_FAILURE_GATE — only reachable when the salon
has 1-2 mixed clusters in total — skips the gate and takes the rescue.
From 3 clusters up the gate is byte-for-byte the old behaviour, pinned by
`test_empty_response_for_smallest_full_chunk_still_aborts`.
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
        # BEAUTY_AUDIT-kfgx: a 1-cluster remainder is NOT dispatched on
        # its own any more — it joins the previous chunk (31 in one call).
        (31, 30, 1),    # tail of 1 merged back
        (32, 30, 1),    # tail of 2 merged back — the widest chunk allowed
        (33, 30, 2),    # tail of 3 is too big to merge: 30 + 3
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


async def _run_with_client(
    client: Any,
    n_clusters: int,
    applied: dict[int, dict[str, Any]],
) -> dict[str, int]:
    """Drive apply_intra_salon_consistency against `client` with no
    network/DB, recording the decision each cluster ends up with."""
    fake_mixed = [
        ((f"Brand{i}", "laser", ("nogi",)),
         [{"id": i, "name": f"svc{i}", "category_name": "test",
           "is_active": True, "booksy_treatment_id": None,
           "synthetic_treatment_id": None, "name_embedding": None}])
        for i in range(n_clusters)
    ]

    async def _fake_apply_decision(**kwargs):
        applied[kwargs["cid"]] = kwargs["decision"]
        return 1

    async def _fake_match(*args, **kwargs):
        return []

    with patch.object(tc, "build_clusters",
                      return_value={i: [{"id": i}] for i in range(n_clusters)}), \
         patch.object(tc, "find_mixed_clusters", return_value=fake_mixed), \
         patch.object(tc, "_apply_decision", side_effect=_fake_apply_decision), \
         patch("services.hidden_service_inference.match_taxonomy_candidates",
               side_effect=_fake_match), \
         patch("services.openai_taxonomy_client.OpenAITaxonomyClient",
               return_value=client), \
         patch("config.settings") as _settings:
        _settings.openai_api_key = "sk-test-fake"
        return await tc.apply_intra_salon_consistency(
            services=[],
            supabase=AsyncMock(),
            minimax=AsyncMock(),
            audit_id=None,
            label="test",
            trace_collector=None,
            dry_run=True,
        )


class _TruncatingClient:
    """Provider that cuts every response down to `keep` decisions from
    call `truncate_after`+1 onwards — the response-truncation shape that
    killed the Beauty4ever regen."""

    def __init__(self, *, truncate_after: int, keep: int) -> None:
        self.truncate_after = truncate_after
        self.keep = keep
        self.call_idx = 0
        self.clusters_per_call: list[int] = []

    async def call_decisions_tool(
        self, **kwargs
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        import re
        ids = [int(m) for m in re.findall(
            r"### KLASTER #(\d+)", kwargs["user_prompt"])]
        self.call_idx += 1
        self.clusters_per_call.append(len(ids))
        usage = {"input": 0, "output": 0, "model": "fake-oai-trunc"}
        if self.call_idx > self.truncate_after:
            return [_decision_for(cid) for cid in ids[:self.keep]], usage
        return [_decision_for(cid) for cid in ids], usage


@pytest.mark.asyncio
async def test_chunk_failure_aborts_pipeline(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A heavily truncated chunk response still aborts the whole report.

    This is the pre-BEAUTY_AUDIT-8j8 guarantee, kept: the rescue path is
    for a model skipping a cluster or two, NOT for a provider that
    answers 2 of 10. Losing this assertion is how a provider outage turns
    into a "successful" report built on deterministic stand-ins.
    """
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "10")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")
    n_clusters = 35  # 4 chunks: 10, 10, 10, 5

    # Calls 1-2 answer in full; from call 3 on, at most 2 decisions come
    # back per call → chunk 3 is short 8 of 10 = 80% > the 50% threshold.
    bad_client = _TruncatingClient(truncate_after=2, keep=2)
    applied: dict[int, dict[str, Any]] = {}

    with pytest.raises(RuntimeError) as exc:
        await _run_with_client(bad_client, n_clusters, applied)

    msg = str(exc.value)
    assert "Pass 5" in msg, msg
    assert "rescue" in msg and "threshold" in msg, msg
    assert "provider failure" in msg, msg
    # No re-ask was attempted for a chunk this broken — it failed on the
    # FIRST response, one call per chunk at most.
    assert bad_client.call_idx <= 4, bad_client.clusters_per_call


@pytest.mark.asyncio
async def test_empty_first_response_aborts_pipeline(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """decisions=[] is a failed provider call, not 10 skipped clusters."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "10")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    empty_client = _TruncatingClient(truncate_after=0, keep=0)
    applied: dict[int, dict[str, Any]] = {}

    with pytest.raises(RuntimeError) as exc:
        await _run_with_client(empty_client, 10, applied)

    msg = str(exc.value)
    assert "returned 0 decision(s) for 10 clusters" in msg, msg
    assert not applied, "nothing may be applied when the chunk aborts"
    assert empty_client.call_idx == 1, "empty response must not be re-asked"


@pytest.mark.asyncio
async def test_small_shortfall_recovers_via_rescue(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1 missing cluster out of 10 stays inside the rescue path: re-asked,
    and on a model that never answers, given a deterministic stand-in —
    the run completes and counts the stand-in in stats."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "10")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    class _SkipsOneClient:
        def __init__(self, skip_id: int) -> None:
            self.skip_id = skip_id
            self.calls = 0

        async def call_decisions_tool(
            self, **kwargs
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            import re
            ids = [int(m) for m in re.findall(
                r"### KLASTER #(\d+)", kwargs["user_prompt"])]
            self.calls += 1
            usage = {"input": 0, "output": 0, "model": "fake-oai-skip"}
            return (
                [_decision_for(cid) for cid in ids if cid != self.skip_id],
                usage,
            )

    client = _SkipsOneClient(skip_id=7)
    applied: dict[int, dict[str, Any]] = {}

    stats = await _run_with_client(client, 10, applied)

    assert sorted(applied) == list(range(1, 11))
    assert stats["rerouted"] == 10
    assert stats["fallback_decisions"] == 1
    assert "zastępcza" in applied[7]["reasoning"]
    assert applied[7]["type"] == "salon_synthetic"
    assert applied[7]["canonical_name"].strip()
    # 1 initial call + 2 bounded re-asks for the single missing cluster.
    assert client.calls == 3


# ---------------------------------------------------------------------------
# BEAUTY_AUDIT-kfgx — a 1-2 cluster chunk must not be able to kill a report.
#
# The 8j8 rescue is expressed as a FRACTION of the chunk, and a fraction
# cannot describe "the model skipped one cluster" when the chunk holds one
# cluster: that is a 100% shortfall, so the provider-failure gate fired and
# the report died with the exact pre-8j8 message. 31 mixed clusters at the
# default chunk_size=30 left such a tail and reproduced it.
#
# Two guards, tested separately because neither covers the other's case:
#   * a remainder of <= _MAX_MERGEABLE_TAIL_CLUSTERS joins the previous
#     chunk, so the tiny chunk never gets dispatched (tests below);
#   * a salon with only 1-2 mixed clusters in total has no previous chunk,
#     so for chunks under _MIN_CHUNK_FOR_PROVIDER_FAILURE_GATE the fraction
#     gate is skipped and the 8j8 rescue runs instead.
# The gate itself is untouched from 3 clusters up.
# ---------------------------------------------------------------------------


class _SkipsIdsClient:
    """Answers every requested cluster except the ones in `skip_ids`."""

    def __init__(self, skip_ids: set[int]) -> None:
        self.skip_ids = skip_ids
        self.calls: list[list[int]] = []

    async def call_decisions_tool(
        self, **kwargs
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        import re
        ids = [int(m) for m in re.findall(
            r"### KLASTER #(\d+)", kwargs["user_prompt"])]
        self.calls.append(ids)
        usage = {"input": 0, "output": 0, "model": "fake-oai-skip-ids"}
        return (
            [_decision_for(cid) for cid in ids if cid not in self.skip_ids],
            usage,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("n_clusters,expected_sizes", [
    (31, [31]),          # tail of 1 merged — the reported repro
    (32, [32]),          # tail of 2 merged — widest chunk the cap allows
    (33, [30, 3]),       # tail of 3 stays its own chunk
    (60, [30, 30]),      # no remainder — untouched
    (61, [30, 31]),      # tail of 1 merged into the SECOND chunk
    (91, [30, 30, 31]),  # tail merges into the last full chunk only
])
async def test_small_tail_never_dispatched_alone(
    n_clusters: int,
    expected_sizes: list[int],
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No chunk of 1-2 clusters is ever sent, and merging stays bounded:
    the widest chunk is chunk_size + _MAX_MERGEABLE_TAIL_CLUSTERS (32),
    never 2*chunk_size-1."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")

    client = _FakeOAIClient()
    applied: dict[int, dict[str, Any]] = {}
    stats = await _run_with_client(client, n_clusters, applied)

    assert client.calls == expected_sizes, client.calls
    assert max(client.calls) <= 30 + tc._MAX_MERGEABLE_TAIL_CLUSTERS
    assert min(client.calls) > tc._MAX_MERGEABLE_TAIL_CLUSTERS
    assert sorted(applied) == list(range(1, n_clusters + 1))
    assert stats["fallback_decisions"] == 0


@pytest.mark.asyncio
async def test_tail_cluster_skipped_no_longer_kills_report(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported failure, end to end: 31 mixed clusters at chunk_size
    30 with the model skipping the cluster that used to land in the
    one-cluster tail. Before the merge this raised "provider returned 0
    decision(s) for 1 clusters"; now it is an ordinary 1-of-31 shortfall
    that the 8j8 rescue absorbs."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    client = _SkipsIdsClient(skip_ids={31})
    applied: dict[int, dict[str, Any]] = {}

    stats = await _run_with_client(client, 31, applied)

    assert sorted(applied) == list(range(1, 32))
    assert stats["rerouted"] == 31
    assert stats["fallback_decisions"] == 1
    assert applied[31]["type"] == "salon_synthetic"
    assert "zastępcza" in applied[31]["reasoning"]
    # First call carries all 31; the rest are the bounded re-asks for #31.
    assert client.calls[0] == list(range(1, 32))
    assert all(c == [31] for c in client.calls[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("n_clusters", [1, 2])
async def test_tiny_total_chunk_falls_back_instead_of_aborting(
    n_clusters: int,
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A salon with only 1-2 mixed clusters has no previous chunk to merge
    into, so the chunk really is that small. An empty response there is
    indistinguishable from "the model skipped its clusters", so it takes
    the 8j8 rescue (re-ask, then deterministic stand-in) instead of
    aborting the report."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    client = _SkipsIdsClient(skip_ids=set(range(1, n_clusters + 1)))
    applied: dict[int, dict[str, Any]] = {}

    stats = await _run_with_client(client, n_clusters, applied)

    assert sorted(applied) == list(range(1, n_clusters + 1))
    assert stats["fallback_decisions"] == n_clusters
    for cid in range(1, n_clusters + 1):
        assert applied[cid]["type"] == "salon_synthetic"
        assert applied[cid]["canonical_name"].strip()
        assert "zastępcza" in applied[cid]["reasoning"]
    # Re-asked before giving up: 1 initial + 2 bounded retries.
    assert len(client.calls) == 3


@pytest.mark.asyncio
async def test_empty_response_for_smallest_full_chunk_still_aborts(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The provider-failure gate must NOT be lost at the boundary: three
    clusters is the smallest chunk that still hard-fails on an empty
    first response."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")
    assert tc._MIN_CHUNK_FOR_PROVIDER_FAILURE_GATE == 3

    client = _SkipsIdsClient(skip_ids={1, 2, 3})
    applied: dict[int, dict[str, Any]] = {}

    with pytest.raises(RuntimeError) as exc:
        await _run_with_client(client, 3, applied)

    msg = str(exc.value)
    assert "returned 0 decision(s) for 3 clusters" in msg, msg
    assert "provider failure" in msg, msg
    assert not applied, "nothing may be applied when the chunk aborts"
    assert len(client.calls) == 1, "empty response must not be re-asked"


# ---------------------------------------------------------------------------
# BEAUTY_AUDIT-p24v — the env override may not silently disarm the gate.
#
# The provider-failure gate only runs on chunks of at least
# tc._MIN_CHUNK_FOR_PROVIDER_FAILURE_GATE clusters. A configured chunk_size
# of 1 or 2 therefore means NO chunk ever reaches it (tail merging adds at
# most _MAX_MERGEABLE_TAIL_CLUSTERS, and only to the last chunk), so an
# empty-but-well-formed provider response would quietly produce a report
# built entirely on stand-in decisions. The value is now floored at the
# gate threshold and the log says what the setting would have cost.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["1", "2"])
def test_chunk_size_below_gate_is_floored_and_logged(
    raw: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """1 and 2 are raised to the gate threshold, loudly."""
    with caplog.at_level("WARNING", logger=tc.logger.name):
        resolved = tc._resolve_chunk_size(raw)

    assert resolved == tc._MIN_CHUNK_FOR_PROVIDER_FAILURE_GATE
    warning = "\n".join(
        r.getMessage() for r in caplog.records if r.levelname == "WARNING"
    )
    assert "TAXONOMY_CONSISTENCY_CHUNK_SIZE" in warning, warning
    # The message must name the consequence, not just the correction.
    assert "bramkę awarii" in warning, warning
    assert "zastępczych" in warning, warning


@pytest.mark.parametrize("raw", ["3", "30"])
def test_chunk_size_at_or_above_gate_is_silent(
    raw: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Exactly at the threshold — and above it — nothing is corrected, so
    nothing may be logged. Without this a `<=` slip would keep returning the
    right number while crying wolf on a perfectly legal setting."""
    with caplog.at_level("WARNING", logger=tc.logger.name):
        resolved = tc._resolve_chunk_size(raw)

    assert resolved == int(raw)
    assert not [r for r in caplog.records if r.levelname == "WARNING"], (
        "a legal chunk_size must not produce a warning"
    )


@pytest.mark.parametrize("raw,expected", [
    (None, 30),        # unset — documented default
    ("30", 30),        # explicit default
    ("25", 25),        # legitimate tuning stays untouched
    ("3", 3),          # exactly at the threshold — allowed
    ("abc", 30),       # not a number — config typo, fall back
    ("0", 30),         # non-positive — config typo, fall back
    ("-5", 30),
])
def test_chunk_size_resolution(raw: str | None, expected: int) -> None:
    assert tc._resolve_chunk_size(raw) == expected


@pytest.mark.asyncio
async def test_chunk_size_1_still_aborts_on_empty_response(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end proof of the floor: with CHUNK_SIZE=1 and three mixed
    clusters the provider-failure gate still fires. Before p24v this
    dispatched three 1-cluster chunks, none of which could reach the gate,
    and an empty response ended as three stand-in decisions."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "1")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    client = _SkipsIdsClient(skip_ids={1, 2, 3})
    applied: dict[int, dict[str, Any]] = {}

    with pytest.raises(RuntimeError) as exc:
        await _run_with_client(client, 3, applied)

    assert "returned 0 decision(s) for 3 clusters" in str(exc.value)
    assert not applied, "no stand-in decision may survive a provider failure"
    assert len(client.calls) == 1, "one chunk of 3, not three chunks of 1"
