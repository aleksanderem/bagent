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
