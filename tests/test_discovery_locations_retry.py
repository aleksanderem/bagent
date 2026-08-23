"""Regresja BEAUTY_AUDIT-9pe5 — zerwane polaczenie do bextract nie moze zabijac
calego przechodu po (kategoria x wojewodztwo).

Petla ponowien w _fetch_listing_by_location chronila wylacznie odpowiedzi HTTP.
Gdy bextract rozlaczyl sie przed wyslaniem czegokolwiek, httpx rzucal
RemoteProtocolError, ktory omijal branch is_transient i lecial az do
discover_combo_via_locations — run konczyl sie jako 'failed', a niedopytane
miasta i dzielnice tego combo przepadaly.
"""

from __future__ import annotations

import httpx
import pytest

from discovery import locations


class _FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self) -> dict:
        return self._payload


class _ScriptedClient:
    """Odgrywa kolejne pozycje scenariusza: wyjatek rzuca, odpowiedz zwraca."""

    def __init__(self, script: list):
        self._script = list(script)
        self.calls = 0

    async def get(self, url, params=None, headers=None, timeout=None):
        self.calls += 1
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture
def _bextract_configured(monkeypatch):
    monkeypatch.setattr(locations.settings, "bextract_api_url", "https://api.example.test", raising=False)
    monkeypatch.setattr(locations.settings, "bextract_api_key", "test-key", raising=False)


@pytest.fixture
def _no_wait(monkeypatch):
    """Zbiera dlugosci backoffu zamiast realnie spac."""
    slept: list[float] = []

    async def _fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(locations.asyncio, "sleep", _fake_sleep)
    return slept


async def test_dropped_connection_is_retried_then_succeeds(_bextract_configured, _no_wait):
    payload = {"businesses": [{"id": 1}], "businesses_count": 1}
    client = _ScriptedClient([
        httpx.RemoteProtocolError("Server disconnected without sending a response."),
        _FakeResponse(200, payload),
    ])

    result = await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert result == payload
    assert client.calls == 2
    assert _no_wait == [5.0]


async def test_timeout_is_retried_too(_bextract_configured, _no_wait):
    payload = {"businesses": []}
    client = _ScriptedClient([
        httpx.ReadTimeout("timed out"),
        httpx.ConnectError("connection refused"),
        _FakeResponse(200, payload),
    ])

    result = await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert result == payload
    assert client.calls == 3
    assert _no_wait == [5.0, 10.0]


async def test_permanent_transport_failure_raises_readable_error(_bextract_configured, _no_wait):
    client = _ScriptedClient([
        httpx.RemoteProtocolError("Server disconnected without sending a response.")
        for _ in range(4)
    ])

    with pytest.raises(RuntimeError, match="transport error after 4 attempts"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 4
    assert _no_wait == [5.0, 10.0, 20.0]


async def test_http_retry_path_still_works(_bextract_configured, _no_wait):
    """Dotychczasowe ponawianie po kodzie HTTP ma dzialac bez zmian."""
    payload = {"businesses": []}
    client = _ScriptedClient([
        _FakeResponse(503, text="service unavailable"),
        _FakeResponse(200, payload),
    ])

    result = await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert result == payload
    assert client.calls == 2
    assert _no_wait == [5.0]


async def test_hard_http_error_is_not_retried(_bextract_configured, _no_wait):
    """404 to blad trwaly — ma padac od razu, bez marnowania 35 s na backoff."""
    client = _ScriptedClient([_FakeResponse(404, text="not found")])

    with pytest.raises(RuntimeError, match="HTTP 404"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []
