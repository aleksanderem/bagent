"""Issue #24 — Sentry/Bugsink init for bagent.

Two entry points:
  init_for_server() — called from server.py lifespan startup, uses
    BUGSINK_DSN_BAGENT; tags events with role=server.
  init_for_worker() — called from workers/main.py startup hook, uses
    BUGSINK_DSN_SCRAPE_ORCHESTRATOR (or falls back to bagent DSN);
    tags events with role=worker so dashboards can split.

Both no-op when the relevant DSN env var is missing so dev environments
keep working without a Bugsink instance.

We use the FastAPI integration on the server side (auto-captures
unhandled exceptions in route handlers) and the Logging + StdLib
integrations on the worker side (auto-captures arq job errors via
exception logging).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Use a sentinel so re-init in pytest reruns isn't catastrophic.
_initialized: Optional[str] = None


def _git_release() -> str | None:
    try:
        sha = os.environ.get("GIT_SHA")
        if sha:
            return sha[:12]
    except Exception:
        pass
    return None


def _init(dsn: str | None, role: str) -> bool:
    """Return True if Sentry was actually initialized."""
    global _initialized
    if not dsn:
        logger.info("[observability] no DSN for role=%s — skipping Sentry init", role)
        return False
    if _initialized == role:
        return True

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration

        integrations = [
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ]
        if role == "server":
            try:
                from sentry_sdk.integrations.fastapi import FastApiIntegration
                integrations.append(FastApiIntegration())
            except Exception as e:  # noqa: BLE001
                logger.warning("[observability] FastApiIntegration unavailable: %s", e)

        sentry_sdk.init(
            dsn=dsn,
            integrations=integrations,
            traces_sample_rate=0.0,  # bugsink doesn't ingest perf data
            send_default_pii=False,
            environment=os.environ.get("BAGENT_ENV", "production"),
            release=_git_release(),
        )
        sentry_sdk.set_tag("service", "bagent")
        sentry_sdk.set_tag("role", role)
        _initialized = role
        logger.info("[observability] Sentry initialized for role=%s", role)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("[observability] Sentry init failed for role=%s: %s", role, e)
        return False


def init_for_server() -> bool:
    return _init(os.environ.get("BUGSINK_DSN_BAGENT"), role="server")


def init_for_worker() -> bool:
    dsn = os.environ.get("BUGSINK_DSN_SCRAPE_ORCHESTRATOR") \
        or os.environ.get("BUGSINK_DSN_BAGENT")
    return _init(dsn, role="worker")
