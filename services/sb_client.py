"""Centralised Supabase client factory.

WHY THIS EXISTS:
    supabase-py's default httpx Client uses HTTP/2. Once an HTTP/2
    connection in the pool encounters a GOAWAY frame (Booksy/Supabase-side
    or network blip), every subsequent upsert through that pool raises:

        ConnectionTerminated(error_code:0, last_stream_id:N, ...)
        Invalid input ConnectionInputs.RECV_WINDOW_UPDATE in state ConnectionState.CLOSED

    These errors persist across thousands of subsequent calls because
    the pool retains the dead connections. Workers logged 19,258
    of these in a single 24h period.

    Mitigation we previously tried: per-call _reset_client() to drop
    the cached supabase client. That helped but produced thrash
    because each reset rebuilds the WHOLE client (auth + REST + storage).

    Permanent fix: configure httpx with http2=False. HTTP/1.1 doesn't
    have stream multiplexing so a broken connection only affects one
    request, not the whole pool. supabase-py's ClientOptions exposes
    `httpx_client` so we can inject a custom one.
"""

from __future__ import annotations

import httpx
from supabase import Client, ClientOptions, create_client


# Lifetime of one underlying TCP connection. Setting both connect and read
# timeouts keeps us from blocking forever when Supabase or PostgREST stalls.
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def make_supabase_client(
    url: str,
    key: str,
    *,
    schema: str = "public",
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
) -> Client:
    """Build a Supabase client backed by an HTTP/1.1-only httpx Client.

    All bagent code paths should use this in preference to calling
    `create_client(...)` directly so we get consistent connection-pool
    behaviour and avoid the HTTP/2 cascade.
    """
    httpx_client = httpx.Client(
        http2=False,
        timeout=timeout,
        # Keep enough connections for the worker's concurrent jobs but cap
        # so we don't fan out 1000s during burst loads.
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        headers=headers or {},
    )
    options_kwargs: dict = {"schema": schema, "httpx_client": httpx_client}
    if headers:
        options_kwargs["headers"] = headers
    options = ClientOptions(**options_kwargs)
    return create_client(url, key, options=options)
