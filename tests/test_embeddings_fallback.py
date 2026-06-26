"""TDD tests for services/embeddings.embed_texts — the central SYNC embed
helper with a SPECIFIC-error OpenAI -> mmlw-sidecar fallback (beads
BEAUTY_AUDIT-lrh Phase 2).

The whole point of this helper is to make the scrape ingest survive an OpenAI
embedding outage WITHOUT changing the OpenAI-healthy behavior. So the tests
pin both halves:

  a — OpenAI ok                              -> ("openai-3-small"), sidecar NOT called.
  b — OpenAI 429 + sidecar 200, len matches  -> ("mmlw-e5-large").
  c — OpenAI 429 + sidecar ConnectError      -> None (graceful, warned).
  d — OpenAI 429 + local disabled            -> None, sidecar NOT called.
  e — OpenAI NON-listed error (BadRequest)   -> None, sidecar NOT called (no masking).
  f — OpenAI 429 + sidecar 200 len mismatch  -> None.
  g — empty input                            -> None, neither OpenAI nor sidecar called.

Mechanics: embed_texts is built around two module-level seams, `_openai_embed`
and `_sidecar_embed`, so tests can monkeypatch them (no real OpenAI client, no
real HTTP). Flags are read off `services.embeddings.settings` at call time and
flipped via monkeypatch. This is a SYNC helper — no asyncio.

All assertions are about which space tag comes back and whether the sidecar
seam was reached. Zero live API / HTTP traffic.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

import services.embeddings as emb


# ---------------------------------------------------------------------------
# Error stand-ins
# ---------------------------------------------------------------------------
# The real openai error classes (RateLimitError/APIConnectionError/
# InternalServerError) are APIStatusError/APIError subclasses whose __init__
# requires keyword-only `response`/`body`/`request` objects — awkward to build
# in a unit test. We subclass the REAL imported types and relax __init__ to
# take a plain message. Because they remain genuine subclasses, the helper's
# `except (RateLimitError, ...)` matches them by isinstance exactly as it would
# the real errors — so this still tests the real fallback contract.

class _RateLimit(emb.RateLimitError):
    def __init__(self, msg: str = "429") -> None:  # noqa: D401
        Exception.__init__(self, msg)


class _APIConnection(emb.APIConnectionError):
    def __init__(self, msg: str = "conn") -> None:
        Exception.__init__(self, msg)


class _APITimeout(emb.APITimeoutError):
    def __init__(self, msg: str = "timeout") -> None:
        Exception.__init__(self, msg)


class _InternalServer(emb.InternalServerError):
    def __init__(self, msg: str = "500") -> None:
        Exception.__init__(self, msg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_vecs(n: int, dim: int = 4) -> list[list[float]]:
    return [[float(i)] * dim for i in range(n)]


@pytest.fixture(autouse=True)
def _enable_local(monkeypatch):
    """Default each test to the production-safe flags (local enabled, openai
    primary) so individual tests only override what they exercise."""
    monkeypatch.setattr(emb.settings, "embedding_local_enabled", True, raising=False)
    monkeypatch.setattr(emb.settings, "embedding_primary", "openai", raising=False)


# ---------------------------------------------------------------------------
# a — OpenAI healthy: returns openai space, sidecar untouched
# ---------------------------------------------------------------------------

def test_openai_ok_returns_openai_space_no_sidecar(monkeypatch):
    texts = ["Botoks 1 okolica", "Manicure hybrydowy"]
    openai_seam = MagicMock(return_value=_fake_vecs(2))
    sidecar_seam = MagicMock(return_value=_fake_vecs(2))
    monkeypatch.setattr(emb, "_openai_embed", openai_seam)
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts(texts)

    assert result is not None
    vecs, space = result
    assert space == "openai-3-small"
    assert len(vecs) == 2
    openai_seam.assert_called_once_with(texts)
    sidecar_seam.assert_not_called()


# ---------------------------------------------------------------------------
# b — OpenAI 429 + healthy sidecar: falls back to mmlw space
# ---------------------------------------------------------------------------

def test_ratelimit_falls_back_to_sidecar(monkeypatch):
    texts = ["Pedicure klasyczny", "Henna brwi"]
    monkeypatch.setattr(
        emb, "_openai_embed",
        MagicMock(side_effect=_RateLimit("insufficient_quota")),
    )
    sidecar_seam = MagicMock(return_value=_fake_vecs(2))
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts(texts)

    assert result is not None
    vecs, space = result
    assert space == "mmlw-e5-large"
    assert len(vecs) == 2
    sidecar_seam.assert_called_once_with(texts)


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda: _RateLimit("429"),
        lambda: _APIConnection("conn reset"),
        lambda: _APITimeout("timed out"),
        lambda: _InternalServer("500"),
    ],
)
def test_all_four_listed_errors_trigger_sidecar(monkeypatch, exc_factory):
    """Each of the 4 specific OpenAI-unavailable errors must route to the
    sidecar (this is the whole contract — only these 4)."""
    texts = ["Strzyzenie meskie"]
    monkeypatch.setattr(emb, "_openai_embed", MagicMock(side_effect=exc_factory()))
    sidecar_seam = MagicMock(return_value=_fake_vecs(1))
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts(texts)

    assert result is not None
    _, space = result
    assert space == "mmlw-e5-large"
    sidecar_seam.assert_called_once()


# ---------------------------------------------------------------------------
# c — OpenAI 429 + sidecar unreachable: graceful None
# ---------------------------------------------------------------------------

def test_ratelimit_sidecar_unreachable_returns_none(monkeypatch):
    texts = ["Masaz relaksacyjny"]
    monkeypatch.setattr(
        emb, "_openai_embed",
        MagicMock(side_effect=_RateLimit("429")),
    )
    monkeypatch.setattr(
        emb, "_sidecar_embed",
        MagicMock(side_effect=httpx.ConnectError("connection refused")),
    )

    result = emb.embed_texts(texts)

    assert result is None


# ---------------------------------------------------------------------------
# d — OpenAI 429 + local disabled: None, sidecar NOT called
# ---------------------------------------------------------------------------

def test_ratelimit_local_disabled_no_sidecar_call(monkeypatch):
    monkeypatch.setattr(emb.settings, "embedding_local_enabled", False, raising=False)
    texts = ["Peeling kawitacyjny"]
    monkeypatch.setattr(
        emb, "_openai_embed",
        MagicMock(side_effect=_RateLimit("429")),
    )
    sidecar_seam = MagicMock(return_value=_fake_vecs(1))
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts(texts)

    assert result is None
    sidecar_seam.assert_not_called()


# ---------------------------------------------------------------------------
# e — NON-listed OpenAI error: None, sidecar NOT called (no masking)
# ---------------------------------------------------------------------------

class _BadRequest(Exception):
    """Stand-in for openai.BadRequestError — NOT in the listed fallback set."""


def test_non_listed_error_does_not_trigger_sidecar(monkeypatch):
    texts = ["Zabieg na twarz"]
    monkeypatch.setattr(
        emb, "_openai_embed",
        MagicMock(side_effect=_BadRequest("malformed input")),
    )
    sidecar_seam = MagicMock(return_value=_fake_vecs(1))
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts(texts)

    # Malformed input must surface as None (cron catch-up), NOT be retried on
    # the sidecar — sending bad input to a different model is masking the bug.
    assert result is None
    sidecar_seam.assert_not_called()


# ---------------------------------------------------------------------------
# f — sidecar length mismatch: None
# ---------------------------------------------------------------------------

def test_sidecar_length_mismatch_returns_none(monkeypatch):
    texts = ["a", "b", "c"]
    monkeypatch.setattr(
        emb, "_openai_embed",
        MagicMock(side_effect=_RateLimit("429")),
    )
    # Sidecar returns only 2 vectors for 3 inputs — must not be trusted.
    monkeypatch.setattr(emb, "_sidecar_embed", MagicMock(return_value=_fake_vecs(2)))

    result = emb.embed_texts(texts)

    assert result is None


# ---------------------------------------------------------------------------
# g — empty input: None, neither path called
# ---------------------------------------------------------------------------

def test_empty_input_returns_none_no_calls(monkeypatch):
    openai_seam = MagicMock(return_value=[])
    sidecar_seam = MagicMock(return_value=[])
    monkeypatch.setattr(emb, "_openai_embed", openai_seam)
    monkeypatch.setattr(emb, "_sidecar_embed", sidecar_seam)

    result = emb.embed_texts([])

    assert result is None
    openai_seam.assert_not_called()
    sidecar_seam.assert_not_called()


# ---------------------------------------------------------------------------
# 300k-token request cap — _openai_embed sub-batches so a large-salon ingest
# (≈490 services × rich descriptions) never sends one >300k-token request that
# OpenAI rejects with a 400 invalid_request_error. Regression for the prod
# incident where such scrapes landed with name_embedding=NULL (the 400 is NOT
# a _OPENAI_UNAVAILABLE error, so it skipped the sidecar and left rows unembedded).
# ---------------------------------------------------------------------------

class _FakeEmbeddingDatum:
    def __init__(self, embedding: list[float]) -> None:
        self.embedding = embedding


class _FakeEmbeddingResponse:
    def __init__(self, n: int, dim: int = 4) -> None:
        self.data = [_FakeEmbeddingDatum([float(i)] * dim) for i in range(n)]


def test_token_budget_batches_splits_on_token_budget():
    # Each input ≈ a quarter of the budget → multiple batches, none over budget.
    chars = int(emb._EMBED_TOKEN_BUDGET / 4 * emb._CHARS_PER_TOKEN_EST)
    inputs = ["x" * chars for _ in range(10)]
    batches = emb._token_budget_batches(inputs)
    # Splits into more than one request, preserves every item, none over budget.
    assert len(batches) > 1
    assert sum(len(b) for b in batches) == len(inputs)
    for b in batches:
        est = sum(emb._estimated_tokens(t) for t in b)
        assert est <= emb._EMBED_TOKEN_BUDGET


def test_token_budget_batches_caps_item_count(monkeypatch):
    # Tiny inputs: token budget is irrelevant, the 2048 array cap binds.
    monkeypatch.setattr(emb, "_MAX_ITEMS_PER_REQUEST", 100, raising=False)
    inputs = ["a" for _ in range(250)]
    batches = emb._token_budget_batches(inputs)
    assert [len(b) for b in batches] == [100, 100, 50]


def test_token_budget_oversized_single_input_is_own_batch():
    huge = "y" * (emb._EMBED_TOKEN_BUDGET * 3)  # one item way over budget
    inputs = ["small", huge, "small2"]
    batches = emb._token_budget_batches(inputs)
    # The oversized item gets its own batch; neighbors are not merged into it.
    assert [b for b in batches] == [["small"], [huge], ["small2"]]


def test_token_budget_row_batches_splits_by_input_text():
    # Row variant: sizes each row by text_of(row); used by the catch-up cron +
    # embed_services backfill where a 500-row chunk of 1500-char inputs would
    # otherwise overflow the 300k cap.
    chars = int(emb._EMBED_TOKEN_BUDGET / 4 * emb._CHARS_PER_TOKEN_EST)
    rows = [{"id": i, "t": "x" * chars} for i in range(10)]
    batches = emb.token_budget_row_batches(rows, lambda r: r["t"])
    assert len(batches) > 1
    # Every row preserved, in order, WITH its id intact (the whole reason this
    # variant exists vs the bare-string one — the id↔vector mapping must survive).
    flat = [r for b in batches for r in b]
    assert [r["id"] for r in flat] == list(range(10))
    for b in batches:
        est = sum(emb._estimated_tokens(r["t"]) for r in b)
        assert est <= emb._EMBED_TOKEN_BUDGET


def test_token_budget_row_batches_caps_item_count(monkeypatch):
    # Tiny inputs: token budget irrelevant, the array-size cap binds instead.
    monkeypatch.setattr(emb, "_MAX_ITEMS_PER_REQUEST", 100, raising=False)
    rows = [{"id": i, "t": "a"} for i in range(250)]
    batches = emb.token_budget_row_batches(rows, lambda r: r["t"])
    assert [len(b) for b in batches] == [100, 100, 50]


def test_openai_embed_chunks_large_batch_preserving_order(monkeypatch):
    """_openai_embed makes >1 embeddings.create call for a big list and
    concatenates results in input order (1:1 with inputs)."""
    chars = int(emb._EMBED_TOKEN_BUDGET / 4 * emb._CHARS_PER_TOKEN_EST)
    inputs = [f"{i}-" + "x" * chars for i in range(10)]  # → several sub-batches

    calls: list[int] = []

    def _fake_create(*, model, input):  # noqa: A002 — mirror SDK kwarg name
        assert model == "text-embedding-3-small"
        # No single request may exceed the budget (this is the bug's guard).
        assert sum(emb._estimated_tokens(t) for t in input) <= emb._EMBED_TOKEN_BUDGET
        calls.append(len(input))
        return _FakeEmbeddingResponse(len(input))

    fake_client = MagicMock()
    fake_client.embeddings.create.side_effect = _fake_create
    monkeypatch.setattr("openai.OpenAI", MagicMock(return_value=fake_client))

    vecs = emb._openai_embed(inputs)

    assert len(calls) > 1                       # split into multiple sub-requests
    assert sum(calls) == len(inputs)            # every input embedded exactly once
    assert len(vecs) == len(inputs) == 10       # one vector per input, order intact


def test_openai_embed_small_batch_single_call(monkeypatch):
    """Small ingests keep the original single-request behavior (no regression)."""
    inputs = ["Botoks 1 okolica", "Manicure hybrydowy", "Pedicure klasyczny"]
    fake_client = MagicMock()
    fake_client.embeddings.create.return_value = _FakeEmbeddingResponse(len(inputs))
    monkeypatch.setattr("openai.OpenAI", MagicMock(return_value=fake_client))

    vecs = emb._openai_embed(inputs)

    fake_client.embeddings.create.assert_called_once()
    assert len(vecs) == 3
