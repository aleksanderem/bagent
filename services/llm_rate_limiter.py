"""Global (process-wide) per-provider LLM concurrency limiter.

WHY THIS EXISTS
---------------
Load test 2 (2026-06-13, prod) measured taxonomy.pass5_consistency for the SAME
salon at 65s solo (LT1) but 1191s under 3x concurrency (LT2) — a ~18x blowup
that is pure OpenAI contention, NOT compute. The pass5 semaphore in
``services/taxonomy_consistency.py`` is PER-REPORT (concurrency=7), so 3 reports
fire up to 3 x 7 = 21 in-flight gpt-4o calls (@50 queued reports = up to 350)
with NO cross-report cap, overrunning OpenAI RPM/TPM into 429 + Retry-After
backoff that balloons pass5.

arq runs ONE process / ONE event loop, so a MODULE-LEVEL ``asyncio.Semaphore``
is shared across every concurrent job = exactly the global ceiling needed. This
module provides ``provider_slot(provider)``: an async context manager that
acquires a lazily-created, env-sized semaphore per provider name. It is a
SEPARATE cross-report layer that composes with (does not replace) the existing
per-report semaphore.

SCOPE (increment 1)
-------------------
Minimal by design: no token bucket, no metrics, no extra knobs. Wired into the
pass5 OpenAI branch only (see ``services/taxonomy_consistency.py``). MiniMax
wiring + other call-sites (router, synthesis, pricing verify) are follow-up
increments.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

logger = logging.getLogger(__name__)

# Lazy per-provider semaphore registry. Keyed by provider name. Semaphores are
# created on first acquire UNDER THE RUNNING LOOP (never at import time), so the
# event loop that arq runs on owns them. The cap for a given provider is fixed
# at first creation; _reset_for_tests() clears the registry so tests can re-set
# the env-driven cap.
_SEMAPHORES: dict[str, asyncio.Semaphore] = {}

# provider -> (env var name, default cap). Caps read via os.environ ONLY — this
# module deliberately does NOT import config.py / Settings.
_PROVIDER_CAPS: dict[str, tuple[str, int]] = {
    "openai": ("LLM_MAX_CONCURRENCY_OPENAI", 6),
    "minimax": ("LLM_MAX_CONCURRENCY_MINIMAX", 4),
}


def _read_cap(env_var: str, default: int) -> int:
    """Int-parse-floor idiom mirrored from taxonomy_consistency.py
    (try/except ValueError, floor at 1)."""
    try:
        cap = int(os.environ.get(env_var, str(default)))
    except ValueError:
        cap = default
    if cap < 1:
        cap = default
    return cap


@asynccontextmanager
async def provider_slot(provider: str) -> AsyncIterator[None]:
    """Acquire a global concurrency slot for ``provider`` for the duration of
    the ``async with`` block.

    Known provider (in ``_PROVIDER_CAPS``): lazily get-or-create its semaphore
    under the running loop, then hold one slot. Unknown provider: fail open —
    log a WARNING and pass through with NO limiting (never block on a wrong
    provider key)."""
    if provider not in _PROVIDER_CAPS:
        logger.warning(
            "provider_slot: unknown provider %r — passing through with NO "
            "concurrency limit (fail-open)",
            provider,
        )
        yield
        return

    env_var, default = _PROVIDER_CAPS[provider]
    # Get-or-create under the running loop. The cap is read at creation time;
    # subsequent calls reuse the same semaphore (cap fixed for the process).
    sem = _SEMAPHORES.get(provider)
    if sem is None:
        sem = asyncio.Semaphore(_read_cap(env_var, default))
        _SEMAPHORES[provider] = sem

    async with sem:
        yield


def _reset_for_tests() -> None:
    """Clear the per-provider semaphore registry. TEST-ONLY — lets tests re-set
    env-driven caps between cases (caps are otherwise fixed at first creation).
    Not part of the public limiter API."""
    _SEMAPHORES.clear()
