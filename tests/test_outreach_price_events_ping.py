"""detect_and_emit_price_events: gwarantowany ping (ok albo /fail).

BEAUTY_AUDIT-t35q, wyodrebnione z diagnozy BEAUTY_AUDIT-08yb.

Cron `outreach_event_detector.detect_and_emit_price_events` (5:40 codziennie)
padal 11 nocy z rzedu (2026-08-12..08-23) i nie zapalil ZADNEGO alarmu, bo
nie mial pingu monitoringu — ani sukcesu, ani porazki. Wykryl to dopiero
skaner logow dyzurnego. Arq nie ponawia zwyklego wyjatku, a cron leci raz na
dobe, wiec kazdy stracony przebieg to caly dzien ciszy.

Ten sam wzorzec pokrycia co tests/test_taxonomy_variant_refresh.py
(BEAUTY_AUDIT-xnb9). Zero sieci: ping jest podmieniany, implementacja tez.
"""

from __future__ import annotations

import pytest

import workers.outreach_event_detector as det


def test_failure_pings_healthcheck_and_reraises(monkeypatch):
    """Porazka musi byc GLOSNA: ping z fail=True oraz wyjatek dalej w gore."""
    pings: list[tuple] = []

    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    boom = RuntimeError("statement timeout")

    async def exploding_impl(_ctx):
        raise boom

    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(det, "_detect_and_emit_price_events_impl", exploding_impl)

    with pytest.raises(RuntimeError) as excinfo:
        _run(det.detect_and_emit_price_events({}))

    assert excinfo.value is boom, "arq musi zobaczyc oryginalny wyjatek"
    assert pings == [("HC_PING_OUTREACH_PRICE_EVENTS", True)], (
        "brak pingu porazki — Healthchecks poszedlby cicho DOWN bez powodu, "
        "tak jak przez 11 nocy z rzedu"
    )


def test_success_pings_exactly_once(monkeypatch):
    """Udany przebieg musi zameldowac sie raz — inaczej check idzie DOWN po grace."""
    pings: list[tuple] = []
    result = {"detected": 3, "emitted": 2, "suppressed": 1, "errors": 0}

    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    async def ok_impl(_ctx):
        return result

    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(det, "_detect_and_emit_price_events_impl", ok_impl)

    out = _run(det.detect_and_emit_price_events({}))

    assert out is result, "wrapper nie moze gubic ani przepakowywac wyniku"
    assert pings == [("HC_PING_OUTREACH_PRICE_EVENTS", False)]


def test_empty_run_still_pings_success(monkeypatch):
    """Noc bez zdarzen to POPRAWNY przebieg — wczesny return tez musi pingnac.

    Implementacja wychodzi natychmiast, gdy RPC nie zwroci wierszy. Gdyby ping
    siedzial w ciele przy zapisie zdarzen, spokojna noc wygladalaby jak awaria.
    """
    pings: list[tuple] = []

    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    empty = {"detected": 0, "emitted": 0, "suppressed": 0, "errors": 0}

    async def empty_impl(_ctx):
        return empty

    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(det, "_detect_and_emit_price_events_impl", empty_impl)

    assert _run(det.detect_and_emit_price_events({})) == empty
    assert pings == [("HC_PING_OUTREACH_PRICE_EVENTS", False)]


def _run(coro):
    import asyncio

    return asyncio.run(coro)
