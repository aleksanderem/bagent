"""Regresja BEAUTY_AUDIT-9pe5 — zerwane polaczenie do bextract nie moze zabijac
calego przechodu po (kategoria x wojewodztwo).

Petla ponowien w _fetch_listing_by_location chronila wylacznie odpowiedzi HTTP.
Gdy bextract rozlaczyl sie przed wyslaniem czegokolwiek, httpx rzucal
RemoteProtocolError, ktory omijal branch is_transient i lecial az do
discover_combo_via_locations — run konczyl sie jako 'failed', a niedopytane
miasta i dzielnice tego combo przepadaly.
"""

from __future__ import annotations

import logging

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


# ---------------------------------------------------------------------------
# BEAUTY_AUDIT-18gt — kod Booksy'ego schowany w tresci wlasnego HTTP 500
# bextracta. Status otoczki (500) nie mowi nic o tym, co sie stalo u zrodla:
# przejsciowe 503 Booksy'ego i realny crash bextracta wygladaly identycznie,
# wiec drabinka ponowien nie ruszala i jedno mrugniecie Booksy'ego przerywalo
# caly spacer po (kategoria x wojewodztwo).
# ---------------------------------------------------------------------------

_UPSTREAM_503 = '{"error":"Booksy API 503: Service Unavailable (listing)"}'
_UPSTREAM_502 = '{"error":"Booksy API 502: Bad Gateway"}'
_UPSTREAM_404 = '{"error":"Booksy API 404: Not Found (listing)"}'
_UPSTREAM_401 = '{"error":"Booksy API 401: Unauthorized"}'
_UPSTREAM_505 = '{"error":"Booksy API 505: HTTP Version Not Supported"}'
_UPSTREAM_5031 = '{"error":"Booksy API 5031: nieznany kod zrodla"}'
_BEXTRACT_CRASH = '{"error":"TypeError: cannot read property"}'


async def test_wrapped_upstream_503_is_retried(_bextract_configured, _no_wait, caplog):
    """500 z 'Booksy API 503' w tresci = przeciazenie zrodla, nie blad trwaly."""
    client = _ScriptedClient([_FakeResponse(500, text=_UPSTREAM_503) for _ in range(4)])

    with caplog.at_level(logging.WARNING, logger="bagent.discovery.locations"):
        with pytest.raises(RuntimeError, match="HTTP 500"):
            await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 4
    assert _no_wait == [5.0, 10.0, 20.0]
    assert "transient (attempt" in caplog.text


async def test_wrapped_upstream_503_recovers_on_retry(_bextract_configured, _no_wait):
    """Drugie podejscie po mrugnieciu Booksy'ego ma zwrocic dane, nie wyjatek."""
    payload = {"businesses": [{"id": 7}], "businesses_count": 1}
    client = _ScriptedClient([
        _FakeResponse(500, text=_UPSTREAM_503),
        _FakeResponse(200, payload),
    ])

    result = await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert result == payload
    assert client.calls == 2
    assert _no_wait == [5.0]


async def test_wrapped_upstream_502_is_retried(_bextract_configured, _no_wait):
    """502 Bad Gateway ze zrodla — ta sama klasa co 503."""
    payload = {"businesses": []}
    client = _ScriptedClient([
        _FakeResponse(500, text=_UPSTREAM_502),
        _FakeResponse(200, payload),
    ])

    result = await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert result == payload
    assert client.calls == 2
    assert _no_wait == [5.0]


async def test_bextract_crash_in_body_is_not_retried(_bextract_configured, _no_wait):
    """Realny crash bextracta ma krzyczec od razu — ponawianie go nie naprawi."""
    client = _ScriptedClient([_FakeResponse(500, text=_BEXTRACT_CRASH)])

    with pytest.raises(RuntimeError, match="HTTP 500"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []


@pytest.mark.parametrize("body", [_UPSTREAM_404, _UPSTREAM_401])
async def test_wrapped_upstream_4xx_is_not_retried(_bextract_configured, _no_wait, body):
    """404/401 ze zrodla to bledy trwale — ponowienie zwroci to samo."""
    client = _ScriptedClient([_FakeResponse(500, text=body)])

    with pytest.raises(RuntimeError, match="HTTP 500"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []


# --- granice rozpoznawania kodu zrodla -------------------------------------
# Trzy testy nizej NIE sprawdzaja nowej funkcjonalnosci — pilnuja, zeby warunek
# nie rozlazl sie w druga strone. Bez nich mozna rozluznic zakres (50[0-9]),
# bramke statusu (500) albo granice slowa (\b) i zestaw zostanie zielony.

async def test_upstream_505_outside_retry_window_is_not_retried(_bextract_configured, _no_wait):
    """Gorna granica zakresu: 505 jest POZA 429/500-504, wiec pada od razu.

    Pinuje `50[0-4]`. Rozluznienie do `50[0-9]` wciagnelo by 505/506/508 —
    kody, przy ktorych ponawianie to 35 s zmarnowane na ten sam blad.
    """
    client = _ScriptedClient([_FakeResponse(500, text=_UPSTREAM_505)])

    with pytest.raises(RuntimeError, match="HTTP 500"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []


async def test_upstream_signature_without_500_wrapper_is_not_retried(_bextract_configured, _no_wait):
    """Bramka statusu: sygnatura 503 w tresci NIE ratuje otoczki 404.

    Pinuje czlon `r.status_code == 500`. Bez niego dowolny status z fraza
    'Booksy API 503' w tresci trafialby w drabinke ponowien — a 404 z
    bextracta znaczy 'takiego endpointu/lokalizacji nie ma', nie 'sprobuj
    jeszcze raz'.
    """
    client = _ScriptedClient([_FakeResponse(404, text=_UPSTREAM_503)])

    with pytest.raises(RuntimeError, match="HTTP 404"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []


async def test_upstream_code_requires_word_boundary(_bextract_configured, _no_wait):
    """Granica slowa: '5031' to nie jest kod 503 z doklejona jedynka.

    Pinuje `\\b`. Bez niego prefiks dowolnego dluzszego numeru ('5031',
    '50412') czytalby sie jako przejsciowy kod HTTP.
    """
    client = _ScriptedClient([_FakeResponse(500, text=_UPSTREAM_5031)])

    with pytest.raises(RuntimeError, match="HTTP 500"):
        await locations._fetch_listing_by_location(client, category_id=4, location_id=31923)

    assert client.calls == 1
    assert _no_wait == []
