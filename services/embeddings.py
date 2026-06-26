"""Central SYNC embed helper: OpenAI primary + mmlw sidecar fallback.

Phase 2 of beads BEAUTY_AUDIT-lrh. Domain: booksyaudit.pl.

`embed_texts(texts)` returns `(vectors, space)` where `space` is one of
`"openai-3-small"` (OpenAI text-embedding-3-small, 1536-dim) or
`"mmlw-e5-large"` (the local sidecar, 1024-dim). The `space` tag is
load-bearing: the caller writes the matching column (`name_embedding` vs
`name_embedding_mmlw`).

CONTRACT — the OpenAI-healthy path must stay byte-identical to today:
OpenAI is always tried first and, on success, returns `("openai-3-small")`
with the exact same model + payload shape the ingest used inline. The sidecar
fallback fires ONLY when OpenAI raises one of the four specific
"OpenAI-unavailable" errors (RateLimitError / APIConnectionError /
APITimeoutError / InternalServerError) AND the local fallback is enabled. Any
OTHER OpenAI error (e.g. BadRequestError on malformed input) returns None
WITHOUT touching the sidecar — masking a real bug by re-sending bad input to a
different model would be worse than today's nightly-cron-catch-up behavior.

SYNC because the only call-site (scripts/ingest_salon_jsons) is fully
synchronous. No retries here (the inline path had none beyond a try/except);
the ingest's `_retry_http` wraps the DB persist, and OpenAI's own SDK retries
transient connection errors before raising.
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar

import httpx

from config import settings

# Defensive OpenAI-error import — copied verbatim from
# services/method_classifier.py:59-64 so the four classes always exist even on
# an older openai SDK shape (the fallback stubs simply never match a real
# error, degrading the helper to "OpenAI failed -> None").
try:
    from openai import APIConnectionError, APITimeoutError, RateLimitError, InternalServerError
except Exception:  # pragma: no cover — older openai SDK shape
    class APIConnectionError(Exception): ...  # type: ignore[no-redef]
    class APITimeoutError(Exception): ...  # type: ignore[no-redef]
    class RateLimitError(Exception): ...  # type: ignore[no-redef]
    class InternalServerError(Exception): ...  # type: ignore[no-redef]

logger = logging.getLogger("embeddings")

# The four OpenAI-unavailable errors that justify a sidecar fallback. Anything
# outside this set is treated as a real failure (return None, no fallback).
_OPENAI_UNAVAILABLE = (RateLimitError, APIConnectionError, APITimeoutError, InternalServerError)

# OpenAI's embeddings endpoint rejects any single request whose combined input
# exceeds 300_000 tokens with a 400 invalid_request_error (NOT one of the
# _OPENAI_UNAVAILABLE errors, so it bypasses the sidecar fallback and leaves the
# whole scrape's rows unembedded). A single large-salon ingest (≈490 services ×
# rich descriptions) overflows this in ONE call. We sub-batch the request below
# the cap so the OpenAI-healthy path still returns vectors for every input.
#
# Budget is intentionally well under the 300k hard limit: the token count is
# ESTIMATED from char length (no tiktoken dependency on the hot ingest path) and
# Polish text tokenizes denser than the rule of thumb, so we leave generous
# headroom. _MAX_ITEMS_PER_REQUEST mirrors OpenAI's 2048-element array cap.
_EMBED_TOKEN_BUDGET = 250_000
_MAX_ITEMS_PER_REQUEST = 2048
# Conservative chars→tokens divisor (over-estimates tokens → smaller, safer
# batches). 2.5 is pessimistic for Polish service text vs the ~4 English rule.
_CHARS_PER_TOKEN_EST = 2.5


def _estimated_tokens(text: str) -> int:
    """Cheap upper-bound token estimate from char length (no tiktoken).

    `+1` so an empty string still costs a token (matches OpenAI billing a
    minimum per array element) and a batch of empties can't grow unbounded.
    """
    return int(len(text) / _CHARS_PER_TOKEN_EST) + 1


def _token_budget_batches(inputs: list[str]) -> list[list[str]]:
    """Split `inputs` into contiguous, order-preserving sub-batches each under
    BOTH the token budget and the per-request item cap.

    A single input larger than the budget is emitted as its own batch (the cap
    is a guard against the aggregate request size, not the per-item limit —
    text-embedding-3-small handles up to ~8k tokens for one item, far under the
    300k request cap, and the ingest already caps each input at 1500 chars)."""
    batches: list[list[str]] = []
    current: list[str] = []
    current_tokens = 0
    for text in inputs:
        t = _estimated_tokens(text)
        if current and (
            current_tokens + t > _EMBED_TOKEN_BUDGET
            or len(current) >= _MAX_ITEMS_PER_REQUEST
        ):
            batches.append(current)
            current = []
            current_tokens = 0
        current.append(text)
        current_tokens += t
    if current:
        batches.append(current)
    return batches


_RowT = TypeVar("_RowT")


def token_budget_row_batches(
    rows: list[_RowT], text_of: Callable[[_RowT], str]
) -> list[list[_RowT]]:
    """Split `rows` into contiguous, order-preserving sub-batches each under BOTH
    the token budget and the per-request item cap, sizing each row by the token
    estimate of its embedding input `text_of(row)`.

    Mirrors `_token_budget_batches` but keeps the caller's row objects (so the
    id↔vector mapping survives) — for call-sites that batch DB rows rather than
    bare strings: the nightly catch-up cron (`embed_new_services`) and the
    `embed_services` backfill. A fixed row-count chunk (e.g. 500) silently
    overflows OpenAI's 300k-tokens-per-request cap once rows carry rich 1500-char
    inputs; sizing by token budget guarantees no request exceeds the limit."""
    batches: list[list[_RowT]] = []
    current: list[_RowT] = []
    current_tokens = 0
    for row in rows:
        t = _estimated_tokens(text_of(row))
        if current and (
            current_tokens + t > _EMBED_TOKEN_BUDGET
            or len(current) >= _MAX_ITEMS_PER_REQUEST
        ):
            batches.append(current)
            current = []
            current_tokens = 0
        current.append(row)
        current_tokens += t
    if current:
        batches.append(current)
    return batches


def _openai_embed(inputs: list[str]) -> list[list[float]]:
    """Raw OpenAI call — same model string + response shape as the original
    inline ingest path, but sub-batched under the 300k-tokens-per-request limit.

    Order is preserved: batches are contiguous slices and results are
    concatenated in batch order, so the returned vectors line up 1:1 with
    `inputs` exactly as a single call would. A batch failure propagates (the
    caller's try/except handles fallback / None) — we do NOT swallow it here, so
    a genuine outage on any sub-batch is still surfaced."""
    from openai import OpenAI

    client = OpenAI()
    out: list[list[float]] = []
    for batch in _token_budget_batches(inputs):
        resp = client.embeddings.create(
            model="text-embedding-3-small",
            input=batch,
        )
        out.extend(list(d.embedding) for d in resp.data)
    return out


def _sidecar_embed(inputs: list[str]) -> list[list[float]]:
    """Call the local mmlw sidecar `POST /embed`. Callers pass BARE text — the
    sidecar prepends the `passage:` E5 prefix internally."""
    resp = httpx.post(
        settings.embedding_local_url.rstrip("/") + "/embed",
        json={"inputs": inputs},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["embeddings"]


def embed_texts(texts: list[str]) -> tuple[list[list[float]], str] | None:
    """Embed `texts`, returning `(vectors, space)` or None.

    See the module docstring for the full contract. Empty input -> None
    (mirrors the ingest's empty-rows guard) without calling OpenAI or the
    sidecar.
    """
    if not texts:
        return None

    local_enabled = settings.embedding_local_enabled

    # Optional deliberate-switch: operator pins the sidecar as primary. Rare,
    # best-effort — still backstopped by OpenAI below if the sidecar fails.
    if settings.embedding_primary == "mmlw" and local_enabled:
        try:
            vecs = _sidecar_embed(texts)
            if len(vecs) == len(texts):
                return vecs, "mmlw-e5-large"
            logger.warning(
                "primary sidecar returned %d vectors for %d inputs — falling back to OpenAI",
                len(vecs), len(texts),
            )
        except Exception as e:  # noqa: BLE001 — best-effort, fall through to OpenAI
            logger.warning("primary sidecar embed failed (%s) — falling back to OpenAI", e)

    # OpenAI primary (the production-healthy path).
    try:
        vecs = _openai_embed(texts)
        return vecs, "openai-3-small"
    except _OPENAI_UNAVAILABLE as e:
        # OpenAI is unavailable (429/quota/connection/timeout/5xx). This is the
        # outage case the sidecar fallback exists for.
        if not local_enabled:
            logger.warning(
                "OpenAI embeddings unavailable (%s) and local fallback disabled — returning None",
                e,
            )
            return None
        try:
            vecs = _sidecar_embed(texts)
            if len(vecs) == len(texts):
                return vecs, "mmlw-e5-large"
            logger.warning(
                "sidecar fallback returned %d vectors for %d inputs — returning None",
                len(vecs), len(texts),
            )
            return None
        except Exception as se:  # noqa: BLE001 — sidecar down/non-200: graceful
            logger.warning("sidecar fallback failed: %s — returning None", se)
            return None
    except Exception as e:  # noqa: BLE001 — NON-listed OpenAI error: do NOT mask
        logger.warning(
            "OpenAI embedding failed (non-fallback error): %s — nightly cron will catch up",
            e,
        )
        return None
