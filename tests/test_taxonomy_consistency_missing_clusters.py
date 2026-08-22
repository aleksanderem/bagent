"""Pass 5 rescue path for clusters the model skips (BEAUTY_AUDIT-8j8).

Job 577ed580 (competitor report 34, Beauty4ever) died after 106 s with
`RuntimeError: LLM skipped cluster_id=181` — gpt-4o simply omitted one
cluster from an otherwise well-formed tool call, and Pass 5 hard-failed
the WHOLE report. The same prompt shape succeeded in reports 36 and 181,
so this is model non-determinism, not a data defect.

`apply_intra_salon_consistency` now, per chunk:

  1. re-asks the model for ONLY the skipped cluster_ids (bounded by
     TAXONOMY_CONSISTENCY_MISSING_RETRIES, default 2);
  2. if the model keeps skipping them, substitutes a deterministic
     `salon_synthetic` decision built from the cluster key (same
     semantics as `_synthesize_fallback_canonical`), logging WARNING;
  3. hard-fails ONLY when even that stand-in cannot be built — and the
     error then names every missing cluster_id.

The rescue is bounded on BOTH ends. It only engages for a small shortfall:
an empty first response, or one missing more than
tc._MAX_MISSING_FRACTION_FOR_RESCUE of the chunk, is a provider failure and
still raises (pinned in tests/test_taxonomy_consistency_chunking.py), as does
any transport exception. And when stand-ins are used, the run says so out
loud: `fallback_decisions` in the returned stats plus ONE aggregate
logger.error — Bugsink alerts from ERROR up, so the per-cluster WARNINGs
alone would leave a hollow report looking like a clean success.

Harness mirrors tests/test_taxonomy_consistency_chunking.py: provider
patched to OpenAI, OpenAITaxonomyClient replaced by a fake, and
build_clusters / find_mixed_clusters / _apply_decision /
match_taxonomy_candidates patched so no network or DB is touched.
"""

from __future__ import annotations

import logging
import re
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


def _cluster_ids_in(prompt: str) -> list[int]:
    return [int(m) for m in re.findall(r"### KLASTER #(\d+)", prompt)]


class _SkippingOAIClient:
    """Fake OpenAITaxonomyClient that omits decisions for `skip_ids`.

    `skip_calls` = how many leading calls drop them. 1 reproduces
    transient non-determinism (the retry answers); a large number
    reproduces a model that never answers for that cluster.

    `keep_count=True` replaces each skipped decision with one carrying a
    bogus cluster_id, so the response COUNT still matches the request —
    that is the exact shape behind "LLM skipped cluster_id=181" (a count
    mismatch would have tripped the older truncation check instead).
    """

    def __init__(
        self,
        skip_ids: set[int],
        *,
        skip_calls: int,
        keep_count: bool = False,
    ) -> None:
        self.skip_ids = skip_ids
        self.skip_calls = skip_calls
        self.keep_count = keep_count
        self.prompts_cluster_ids: list[list[int]] = []

    async def call_decisions_tool(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        tool_schema: dict[str, Any],
        max_tokens: int = 16384,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        ids = _cluster_ids_in(user_prompt)
        self.prompts_cluster_ids.append(ids)
        call_no = len(self.prompts_cluster_ids)
        usage = {"input": 10, "output": 10, "model": "fake-oai"}
        if call_no > self.skip_calls:
            return [_decision_for(cid) for cid in ids], usage
        out: list[dict[str, Any]] = []
        for cid in ids:
            if cid in self.skip_ids:
                if self.keep_count:
                    # Hallucinated id — keeps len(decisions) == len(ids).
                    out.append(_decision_for(9000 + cid))
                continue
            out.append(_decision_for(cid))
        return out, usage


@pytest.fixture
def env_openai_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TAXONOMY_PASS5_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-fake")


def _fake_mixed(n_clusters: int) -> list[Any]:
    """N single-member, brand-marked clusters (cluster_id 1..N after the
    enumerate() inside apply_intra_salon_consistency)."""
    return [
        ((f"Brand{i}", "laser", ("nogi",)),
         [{"id": i, "name": f"svc{i}", "category_name": "test",
           "is_active": True, "booksy_treatment_id": None,
           "synthetic_treatment_id": None, "name_embedding": None}])
        for i in range(n_clusters)
    ]


async def _run_pass5(
    client: Any,
    n_clusters: int,
    applied: dict[int, dict[str, Any]],
) -> dict[str, int]:
    """Drive apply_intra_salon_consistency against `client`, recording
    the decision each cluster ends up with."""
    fake_clusters = {
        i: [{"id": i, "is_active": True, "name": f"svc{i}",
             "category_name": "test"}]
        for i in range(n_clusters)
    }

    async def _fake_apply_decision(**kwargs):
        applied[kwargs["cid"]] = kwargs["decision"]
        return 1

    async def _fake_match(*args, **kwargs):
        return []

    with patch.object(tc, "build_clusters", return_value=fake_clusters), \
         patch.object(tc, "find_mixed_clusters",
                      return_value=_fake_mixed(n_clusters)), \
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


# ---------------------------------------------------------------------------
# (a) model skips a cluster, the retry returns the full set
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skipped_cluster_recovered_by_retry(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """First call omits cluster_id=3 (count preserved via a bogus id —
    the 577ed580 shape). The retry asks ONLY for cluster 3, gets a real
    decision, and the report completes with NO fallback decision."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")
    n_clusters = 5
    client = _SkippingOAIClient({3}, skip_calls=1, keep_count=True)
    applied: dict[int, dict[str, Any]] = {}

    with caplog.at_level(logging.WARNING, logger="services.taxonomy_consistency"):
        stats = await _run_pass5(client, n_clusters, applied)

    # Exactly two LLM calls: the full chunk, then only the missing cluster.
    assert len(client.prompts_cluster_ids) == 2, client.prompts_cluster_ids
    assert client.prompts_cluster_ids[0] == [1, 2, 3, 4, 5]
    assert client.prompts_cluster_ids[1] == [3], (
        "retry must re-ask ONLY for the skipped cluster"
    )
    # Every cluster decided exactly once, and cluster 3 got a REAL model
    # decision (no rescue stand-in).
    assert sorted(applied) == [1, 2, 3, 4, 5]
    assert applied[3]["canonical_name"] == "Cluster 3"
    assert "zastępcza" not in applied[3]["reasoning"]
    assert stats["clusters_mixed"] == n_clusters
    assert stats["rerouted"] == n_clusters
    # Rescue path left a WARNING trail: how many, which ids, which attempt.
    warnings = [r.getMessage() for r in caplog.records
                if r.levelno >= logging.WARNING]
    assert any("[3]" in m and "próba 2 z 3" in m for m in warnings), warnings
    assert any("odzyskała" in m and "[3]" in m for m in warnings), warnings


# ---------------------------------------------------------------------------
# (b) retries do not help → deterministic stand-in decision
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persistently_skipped_cluster_gets_fallback_decision(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Model never returns cluster_id=2. After the bounded retries the
    cluster receives a deterministic salon_synthetic decision derived
    from its key, the report completes, and a WARNING names the cluster
    and the attempt count."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")
    n_clusters = 4
    client = _SkippingOAIClient({2}, skip_calls=99)
    applied: dict[int, dict[str, Any]] = {}

    with caplog.at_level(logging.WARNING, logger="services.taxonomy_consistency"):
        stats = await _run_pass5(client, n_clusters, applied)

    # 1 initial call + 2 bounded retries — no infinite loop.
    assert len(client.prompts_cluster_ids) == 3, client.prompts_cluster_ids
    assert client.prompts_cluster_ids[1] == [2]
    assert client.prompts_cluster_ids[2] == [2]
    # Report went through; every cluster has a decision.
    assert sorted(applied) == [1, 2, 3, 4]
    assert stats["rerouted"] == n_clusters
    # Cluster 2 carries the deterministic stand-in built from its key
    # ("Brand1" + "laser" + "nogi" — cluster_id 2 is the i=1 fixture row),
    # matching _synthesize_fallback_canonical semantics.
    fallback = applied[2]
    assert fallback["type"] == "salon_synthetic"
    assert fallback["canonical_name"] == tc._synthesize_fallback_canonical(
        ("Brand1", "laser", ("nogi",)),
    )
    assert "zastępcza" in fallback["reasoning"]
    # Untouched clusters still hold real model decisions.
    assert applied[1]["canonical_name"] == "Cluster 1"
    warnings = [r.getMessage() for r in caplog.records
                if r.levelno >= logging.WARNING]
    assert any(
        "cluster_id=2" in m and "decyzji zastępczej" in m and "3 próbach" in m
        for m in warnings
    ), warnings


@pytest.mark.asyncio
async def test_missing_retries_can_be_disabled(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TAXONOMY_CONSISTENCY_MISSING_RETRIES=0 goes straight to the
    deterministic decision — one LLM call, still no hard fail."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "0")
    client = _SkippingOAIClient({1}, skip_calls=99)
    applied: dict[int, dict[str, Any]] = {}

    stats = await _run_pass5(client, 3, applied)

    assert len(client.prompts_cluster_ids) == 1
    assert sorted(applied) == [1, 2, 3]
    assert stats["rerouted"] == 3
    assert "zastępcza" in applied[1]["reasoning"]


# ---------------------------------------------------------------------------
# (c) no stand-in can be built → hard fail listing the missing cluster_ids
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unbuildable_fallback_raises_with_missing_ids(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the canonical name cannot be synthesized the pipeline still
    fails — but the error names every missing cluster_id."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "1")
    client = _SkippingOAIClient({2, 4}, skip_calls=99)
    applied: dict[int, dict[str, Any]] = {}

    with patch.object(tc, "_synthesize_fallback_canonical", return_value="   "):
        with pytest.raises(RuntimeError) as exc:
            await _run_pass5(client, 5, applied)

    msg = str(exc.value)
    assert "LLM skipped cluster_id=[2, 4]" in msg, msg
    assert "cluster_id=[2, 4]" in msg.split("no fallback decision")[-1], msg
    assert "2 attempt(s)" in msg, msg


def test_build_fallback_decision_rejects_malformed_key() -> None:
    """Guard that turns an unusable cluster key into the hard-fail path
    instead of a half-built decision."""
    with pytest.raises((ValueError, TypeError)):
        tc._build_fallback_decision(7, ("BrandOnly",), attempts=3)  # type: ignore[arg-type]


def test_build_fallback_decision_matches_synthesize_semantics() -> None:
    key = ("thunder", "laser", ("nogi", "pachy"))
    decision = tc._build_fallback_decision(11, key, attempts=3)
    assert decision["cluster_id"] == 11
    assert decision["type"] == "salon_synthetic"
    assert decision["canonical_name"] == tc._synthesize_fallback_canonical(key)
    assert decision["canonical_name"].strip()
    assert "3" in decision["reasoning"]


# ---------------------------------------------------------------------------
# (d) transport/provider exception — must abort, never enter the rescue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transport_error_aborts_pipeline(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A network/provider exception during the call is NOT a skipped
    cluster: it must propagate (Bugsink) instead of being swallowed into
    stand-in decisions for the whole chunk."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "2")

    class _TransportErrorClient:
        def __init__(self) -> None:
            self.calls = 0

        async def call_decisions_tool(self, **kwargs) -> Any:
            self.calls += 1
            raise ConnectionError("connection reset by peer")

    client = _TransportErrorClient()
    applied: dict[int, dict[str, Any]] = {}

    with pytest.raises(ConnectionError, match="connection reset by peer"):
        await _run_pass5(client, 5, applied)

    assert not applied, "no cluster may be decided when the call itself fails"
    # with_retry(max_attempts=2) retried once, then re-raised — the rescue
    # loop never ran (it only reacts to MISSING ids in a SUCCESSFUL response).
    assert client.calls == 2, client.calls


# ---------------------------------------------------------------------------
# Degradation visibility: fallback counter in stats + ONE aggregate ERROR
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fallback_count_in_stats_and_single_error_log(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """1 stand-in out of 8 clusters: stats carry the count and exactly ONE
    ERROR record is emitted for the run (Bugsink alerts from ERROR up; the
    per-cluster WARNINGs alone would never raise a flag)."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "1")
    n_clusters = 8
    client = _SkippingOAIClient({5}, skip_calls=99)
    applied: dict[int, dict[str, Any]] = {}

    with caplog.at_level(logging.DEBUG, logger="services.taxonomy_consistency"):
        stats = await _run_pass5(client, n_clusters, applied)

    # Run completed with every cluster decided, one of them a stand-in.
    assert sorted(applied) == list(range(1, n_clusters + 1))
    assert stats["rerouted"] == n_clusters
    assert stats["fallback_decisions"] == 1, stats
    assert "zastępcza" in applied[5]["reasoning"]

    errors = [
        r for r in caplog.records
        if r.name == "services.taxonomy_consistency"
        and r.levelno >= logging.ERROR
    ]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    msg = errors[0].getMessage()
    assert "DEGRADACJA Pass 5" in msg, msg
    assert "[5]" in msg, msg
    assert "1 z 8" in msg, msg


@pytest.mark.asyncio
async def test_clean_run_reports_zero_fallbacks_and_no_error_log(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The ERROR is degradation-only — a run where the model answered
    everything must stay quiet and report fallback_decisions=0."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    client = _SkippingOAIClient(set(), skip_calls=0)
    applied: dict[int, dict[str, Any]] = {}

    with caplog.at_level(logging.DEBUG, logger="services.taxonomy_consistency"):
        stats = await _run_pass5(client, 6, applied)

    assert stats["fallback_decisions"] == 0, stats
    assert not [
        r for r in caplog.records
        if r.name == "services.taxonomy_consistency"
        and r.levelno >= logging.ERROR
    ]


# ---------------------------------------------------------------------------
# Hallucinated cluster_ids must not be counted as decisions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_extra_cluster_ids_are_ignored(
    env_openai_provider: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A decision for a cluster_id that was never asked about is dropped
    (with a WARNING) rather than padding the count."""
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_CHUNK_SIZE", "30")
    monkeypatch.setenv("TAXONOMY_CONSISTENCY_MISSING_RETRIES", "1")
    client = _SkippingOAIClient({2}, skip_calls=1, keep_count=True)
    applied: dict[int, dict[str, Any]] = {}

    with caplog.at_level(logging.WARNING, logger="services.taxonomy_consistency"):
        await _run_pass5(client, 3, applied)

    assert sorted(applied) == [1, 2, 3]
    warnings = [r.getMessage() for r in caplog.records
                if r.levelno >= logging.WARNING]
    assert any("nieistniejących klastrów" in m and "9002" in m
               for m in warnings), warnings
