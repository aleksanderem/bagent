"""Regression test for the HTTP/2 ConnectionTerminated cascade.

History: 2026-04-30 we hit 19,258 ConnectionTerminated errors over 24h
because supabase-py's default httpx client has http2=True. Once one
HTTP/2 stream got a goaway, every connection in the pool inherited
the broken multiplexing state and surfaced as 'ConnectionTerminated' /
'ConnectionInputs.RECV_WINDOW_UPDATE in CLOSED'.

Fix: ``services/sb_client.make_supabase_client()`` injects a custom
httpx.Client(http2=False) via ClientOptions.httpx_client. After
deploy: 0 errors/min sustained.

This test pins the fix by:
  1. Asserting the supabase client uses HTTP/1.1 (httpx_client.http2 == False)
  2. Asserting that 20 concurrent SELECTs against prod don't surface
     any ConnectionTerminated / ConnectionInputs errors

Usage:
    pytest tests/e2e/test_http2_cascade_regression.py -v -m e2e
"""

from __future__ import annotations

import asyncio

import pytest

from config import settings
from services.sb_client import make_supabase_client


@pytest.mark.e2e
def test_make_supabase_client_uses_http1():
    """The factory must produce a client whose underlying httpx
    transport has http2=False. Reverting this would re-trigger the
    cascade."""
    if not settings.supabase_url or not settings.supabase_service_key:
        pytest.skip("requires SUPABASE_URL + SUPABASE_SERVICE_KEY")

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    # Probe the postgrest layer's underlying httpx client
    pg = getattr(client, "postgrest", None) or getattr(client, "_postgrest", None)
    assert pg is not None, "supabase client has no postgrest attr — SDK shape changed"

    httpx_client = getattr(pg, "session", None) or getattr(pg, "_client", None)
    if httpx_client is None:
        pytest.skip("postgrest doesn't expose underlying httpx client; check SDK version")

    # Different httpx versions store http2 in different places
    transport = getattr(httpx_client, "_transport", None)
    if transport and hasattr(transport, "_pool") and hasattr(transport._pool, "_http2"):
        assert transport._pool._http2 is False, (
            "supabase httpx pool has http2=True — cascade regression risk; "
            "see services/sb_client.py and memory/reference_full_architecture.md"
        )
    # Fallback: check a flag we set ourselves on the client
    elif hasattr(httpx_client, "http2"):
        assert httpx_client.http2 is False, (
            "httpx client has http2=True — see services/sb_client.py"
        )


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_concurrent_supabase_queries_no_cascade():
    """Spawn 20 concurrent SELECTs against a small table. With http2=True
    you'd see ConnectionTerminated / ConnectionInputs errors propagate
    across the pool. With http2=False they should all succeed.

    Read-only — does NOT need read_only_supabase fixture."""
    if not settings.supabase_url or not settings.supabase_service_key:
        pytest.skip("requires SUPABASE_URL + SUPABASE_SERVICE_KEY")

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    async def _one_query(i: int) -> tuple[int, str | None]:
        try:
            await asyncio.to_thread(
                lambda: client.table("salon_scrapes")
                .select("id")
                .limit(1)
                .execute()
            )
            return i, None
        except Exception as e:
            return i, f"{type(e).__name__}: {e}"

    results = await asyncio.gather(*[_one_query(i) for i in range(20)])
    cascade_errors = [
        (i, msg) for i, msg in results
        if msg and (
            "ConnectionTerminated" in msg
            or "ConnectionInputs" in msg
            or "RECV_WINDOW_UPDATE" in msg
            or "GOAWAY" in msg.upper()
        )
    ]
    assert not cascade_errors, (
        f"HTTP/2 cascade errors detected — http2=False fix may have "
        f"regressed: {cascade_errors[:3]}"
    )

    other_errors = [(i, msg) for i, msg in results if msg]
    if other_errors:
        # Non-cascade errors are allowed (network blips) but log them
        print(f"\n[note] {len(other_errors)}/20 queries had non-cascade errors:")
        for i, msg in other_errors[:5]:
            print(f"  #{i}: {msg[:120]}")
