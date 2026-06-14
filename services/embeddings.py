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


def _openai_embed(inputs: list[str]) -> list[list[float]]:
    """Raw OpenAI call — the EXACT call moved out of the ingest inline path
    (model string + response shape unchanged)."""
    from openai import OpenAI

    client = OpenAI()
    resp = client.embeddings.create(
        model="text-embedding-3-small",
        input=inputs,
    )
    return [list(d.embedding) for d in resp.data]


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
