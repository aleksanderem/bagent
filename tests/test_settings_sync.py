"""Nadpisania z panelu „Klucze i stałe" (services/settings_sync)."""

from __future__ import annotations

import httpx
import pytest

from services import settings_sync
from services.settings_sync import apply_overrides, fetch_overrides, sync_settings


class FakeSettings:
    def __init__(self) -> None:
        self.minimax_api_key = "env-key"
        self.brave_search_api_key = ""
        self.embedding_local_enabled = False
        self.outreach_daily_send_cap_scale = 40
        self.api_key = "shared-secret"
        self.convex_url = "https://x.convex.site"


@pytest.fixture(autouse=True)
def _reset_state():
    settings_sync._originals.clear()
    settings_sync._applied.clear()
    yield
    settings_sync._originals.clear()
    settings_sync._applied.clear()


def test_apply_overrides_sets_known_fields_and_coerces_types():
    s = FakeSettings()
    res = apply_overrides(
        s,
        {
            "MINIMAX_API_KEY": "panel-key",
            "BRAVE_SEARCH_API_KEY": "brave",
            "EMBEDDING_LOCAL_ENABLED": "true",
            "OUTREACH_DAILY_SEND_CAP_SCALE": "55",
            "NIEZNANY": "x",
            "API_KEY": "hijack",
        },
    )
    assert s.minimax_api_key == "panel-key"
    assert s.brave_search_api_key == "brave"
    assert s.embedding_local_enabled is True
    assert s.outreach_daily_send_cap_scale == 55
    assert s.api_key == "shared-secret", "klucz uwierzytelniający nie może być nadpisany zdalnie"
    assert set(res["applied"]) == {
        "minimax_api_key",
        "brave_search_api_key",
        "embedding_local_enabled",
        "outreach_daily_send_cap_scale",
    }
    assert set(res["ignored"]) == {"NIEZNANY", "API_KEY"}


def test_removed_override_restores_env_value():
    s = FakeSettings()
    apply_overrides(s, {"MINIMAX_API_KEY": "panel-key"})
    assert s.minimax_api_key == "panel-key"
    res = apply_overrides(s, {})
    assert s.minimax_api_key == "env-key"
    assert res["restored"] == ["minimax_api_key"]


async def test_fetch_overrides_calls_convex_with_scope_and_key():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("x-api-key", "")
        return httpx.Response(200, json={"scope": "bagent", "settings": {"MINIMAX_API_KEY": "k1"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        out = await fetch_overrides(http, "https://x.convex.site/", "shared-secret")
    assert out == {"MINIMAX_API_KEY": "k1"}
    assert seen["url"] == "https://x.convex.site/api/settings?scope=bagent"
    assert seen["key"] == "shared-secret"


async def test_sync_settings_never_raises(monkeypatch):
    s = FakeSettings()

    async def boom(*_a, **_k):
        raise httpx.ConnectError("dns")

    monkeypatch.setattr(settings_sync, "fetch_overrides", boom)
    res = await sync_settings(s)
    assert "error" in res
    assert s.minimax_api_key == "env-key"

    s.convex_url = ""
    assert (await sync_settings(s)).get("skipped")
