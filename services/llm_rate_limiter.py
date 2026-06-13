"""Global (process-wide) per-MODEL LLM concurrency limiter.

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
module provides ``provider_slot(model)``: an async context manager that
acquires a lazily-created, env-sized semaphore per MODEL NAME. It is a SEPARATE
cross-report layer that composes with (does not replace) the existing
per-report semaphore.

WHY PER-MODEL (increment 2)
---------------------------
Increment 1 keyed the limiter per PROVIDER ("openai"/"minimax") and decisively
fixed pass5 (1191s -> 85s under 3x). BUT router_skip rose: router,
hidden-inference, pricing-tier1, and pair-verify all use gpt-4o-mini, while
pass5 uses gpt-4o. gpt-4o and gpt-4o-mini are SEPARATE OpenAI rate-limit
buckets, so a single shared "openai" slot would force the fast gpt-4o-mini
router to queue behind slow gpt-4o pass5 — starving it and causing MORE skips
(90s per-salon router timeout, #29). Keying by MODEL NAME gives each model its
own bucket, matching how OpenAI rate-limits, so saturating gpt-4o never blocks
gpt-4o-mini.

SCOPE (increment 2)
-------------------
Per-MODEL caps, wired into: pass5 (gpt-4o, preserving increment 1's validated
cap of 6, see ``services/taxonomy_consistency.py``); every gpt-4o-mini
call-site via the single ``GeminiLLMClient.generate_json`` wrap
(router/hidden-inference/pricing-tier1/pair-verify, see
``services/hidden_service_inference.py``); MiniMax synthesis (MiniMax-M2.7, see
``pipelines/competitor_synthesis.py``); and the OpenAI synthesis fallback
(gpt-4o-mini, see ``services/openai_synthesis.py``). Still minimal by design:
no token bucket, no metrics, no extra knobs.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

logger = logging.getLogger(__name__)

# Lazy per-model semaphore registry. Keyed by model name. Semaphores are
# created on first acquire UNDER THE RUNNING LOOP (never at import time), so the
# event loop that arq runs on owns them. The cap for a given model is fixed
# at first creation; _reset_for_tests() clears the registry so tests can re-set
# the env-driven cap.
_SEMAPHORES: dict[str, asyncio.Semaphore] = {}

# model name -> (env var name, default cap). Caps read via os.environ ONLY —
# this module deliberately does NOT import config.py / Settings. Defaults:
#   gpt-4o=6        PRESERVES increment 1's validated pass5 cap (LT2: 1191s->85s)
#   gpt-4o-mini=18  generous — gpt-4o-mini is fast / high RPM and is a SEPARATE
#                   OpenAI bucket from gpt-4o, so it must NOT be starved behind
#                   slow gpt-4o pass5 (env-tunable down for rate-limited keys)
#   MiniMax-M2.7=4  synthesis is heavy + low-volume; matches increment 1 minimax
_MODEL_CAPS: dict[str, tuple[str, int]] = {
    "gpt-4o": ("LLM_MAX_CONCURRENCY_GPT_4O", 6),
    "gpt-4o-mini": ("LLM_MAX_CONCURRENCY_GPT_4O_MINI", 18),
    "MiniMax-M2.7": ("LLM_MAX_CONCURRENCY_MINIMAX_M2_7", 4),
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
async def provider_slot(model: str) -> AsyncIterator[None]:
    """Acquire a global concurrency slot for ``model`` for the duration of the
    ``async with`` block. The argument is a MODEL NAME (e.g. ``"gpt-4o"``,
    ``"gpt-4o-mini"``, ``"MiniMax-M2.7"``) — each model is its own rate bucket,
    matching how OpenAI/MiniMax rate-limit.

    Known model (in ``_MODEL_CAPS``): lazily get-or-create its semaphore under
    the running loop, then hold one slot. Unknown model: fail open — log a
    WARNING and pass through with NO limiting (never block on a wrong key).

    The public NAME stays ``provider_slot`` (call-sites + tests reference it by
    this attribute); only the semantics of the key changed to per-model."""
    if model not in _MODEL_CAPS:
        logger.warning(
            "provider_slot: unknown model %r — passing through with NO "
            "concurrency limit (fail-open)",
            model,
        )
        yield
        return

    env_var, default = _MODEL_CAPS[model]
    # Get-or-create under the running loop. The cap is read at creation time;
    # subsequent calls reuse the same semaphore (cap fixed for the process).
    sem = _SEMAPHORES.get(model)
    if sem is None:
        sem = asyncio.Semaphore(_read_cap(env_var, default))
        _SEMAPHORES[model] = sem

    async with sem:
        yield


def _reset_for_tests() -> None:
    """Clear the per-model semaphore registry. TEST-ONLY — lets tests re-set
    env-driven caps between cases (caps are otherwise fixed at first creation).
    Not part of the public limiter API."""
    _SEMAPHORES.clear()
