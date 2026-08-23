"""refresh_service_variants: timeout klienta i gwarantowany ping porazki.

Dwa tryby awarii z BEAUTY_AUDIT-xnb9, oba wczesniej bez zadnego pokrycia:

1. Klient Supabase budowany bez `timeout=` dziedziczyl modul-default 30 s, a
   obie wolane procedury maja po stronie Postgresa statement_timeout=120s
   (mig 127 / mig 149). Kazda porcja liczaca sie dluzej niz 30 s ZAWSZE ginela
   na ReadTimeout, choc baza konczyla prace — cron padal po 233 s.
2. Wyjatek nie wysylal zadnego pingu, wiec Healthchecks szedl cicho DOWN bez
   powodu, a cron chodzi raz na dobe i arq nie ponawia zwyklego wyjatku.

Zero sieci i zero platnych API — klient Supabase i ping sa podmieniane.
"""

from __future__ import annotations

import httpx
import pytest

import workers.taxonomy_refresh as tr


class _FakeRpc:
    """Odpowiada 0 na kazde wywolanie RPC, wiec obie petle konczy pierwszy obrot."""

    def execute(self):
        return type("Res", (), {"data": 0})()


class _FakeClient:
    def rpc(self, _name, _params):
        return _FakeRpc()


def test_client_timeout_sits_above_the_server_statement_timeout(monkeypatch):
    """140 s > statement_timeout=120s procedur — inaczej klient poddaje sie pierwszy."""
    captured: dict = {}

    def fake_make_client(url, key, **kwargs):
        captured.update(kwargs)
        return _FakeClient()

    monkeypatch.setattr(tr, "make_supabase_client", fake_make_client, raising=False)
    monkeypatch.setattr(
        "services.sb_client.make_supabase_client", fake_make_client, raising=False
    )
    monkeypatch.setattr("services.healthcheck.ping", _noop_ping, raising=False)

    import asyncio

    asyncio.get_event_loop_policy()  # brak side-effectu, tylko czytelnosc
    _run(tr._refresh_service_variants_impl({}))

    timeout = captured.get("timeout")
    assert isinstance(timeout, httpx.Timeout), "klient budowany bez jawnego timeoutu"
    assert timeout.read > 120.0, (
        f"read timeout {timeout.read}s nie przekracza statement_timeout=120s "
        "procedur mig 127/149 — kazda dluzsza porcja znow zginie na ReadTimeout"
    )
    assert timeout.read < 150.0, (
        f"read timeout {timeout.read}s wchodzi w ~150 s read_timeout Konga"
    )


def test_failure_pings_healthcheck_and_reraises(monkeypatch):
    """Porazka musi byc GLOSNA: ping z fail=True oraz wyjatek dalej w gore."""
    pings: list[tuple] = []

    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    boom = RuntimeError("backfill wywrocil sie")

    async def exploding_impl(_ctx):
        raise boom

    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(tr, "_refresh_service_variants_impl", exploding_impl)

    with pytest.raises(RuntimeError) as excinfo:
        _run(tr.refresh_service_variants({}))

    assert excinfo.value is boom, "arq musi zobaczyc oryginalny wyjatek"
    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", True)], (
        "brak pingu porazki — Healthchecks poszedlby cicho DOWN bez powodu"
    )


def test_success_pings_exactly_once(monkeypatch):
    """Wrapper nie moze dublowac pingu sukcesu wysylanego przez implementacje."""
    pings: list[tuple] = []

    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    def fake_make_client(url, key, **kwargs):
        return _FakeClient()

    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(
        "services.sb_client.make_supabase_client", fake_make_client, raising=False
    )

    msg = _run(tr.refresh_service_variants({}))

    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", False)]
    assert msg.startswith("refresh_service_variants:")


async def _noop_ping(slug, fail=False):
    return None


def _run(coro):
    import asyncio

    return asyncio.run(coro)
