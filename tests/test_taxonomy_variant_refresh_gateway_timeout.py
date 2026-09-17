"""refresh_service_variants: 504 z Konga po 60 s nie może zabić całej nocy.

BEAUTY_AUDIT-a9v1. Trasa rest-v1 w Kongu nie ma jawnego read_timeout, więc
działa domyślne 60 s — krótsze niż statement_timeout=120s procedur mig 127.
Porcja, która w danej chwili liczy się dłużej niż 60 s (np. gdy o 03:35 baza
jest zajęta nocnym inference), wracała jako APIError code=504, a job padał po
kilku porcjach. 17.09: 10 porcji (50 tys. wierszy) z limitu 300 tys., potem 504.

Kontrakt po naprawie:
  * 504 / ReadTimeout na porcji = zmniejsz porcję o połowę i ponów,
  * przy minimalnej porcji dalsze 504 kończą TĘ fazę (reszta schodzi
    następnej nocy — procedury są idempotentne), job idzie dalej,
  * job bez żadnego postępu i z samymi 504 dalej jest głośną porażką.

Zero sieci — klient Supabase i ping są podmieniane.
"""

from __future__ import annotations

import asyncio

import pytest
from postgrest.exceptions import APIError

import workers.taxonomy_refresh as tr


def _gateway_timeout() -> APIError:
    return APIError(
        {
            "message": "JSON could not be generated",
            "code": 504,
            "hint": "Refer to full message for details",
            "details": '{"message":"The upstream server is timing out"}',
        }
    )


class _Res:
    def __init__(self, data):
        self.data = data


class _Call:
    def __init__(self, client, name, params):
        self._client, self._name, self._params = client, name, params

    def execute(self):
        return self._client.answer(self._name, self._params)


class _ScriptedClient:
    """Porcja większa niż `max_ok_batch` = 504; mniejsza przerabia wiersze z puli."""

    def __init__(self, pools: dict[str, int], max_ok_batch: dict[str, int]):
        self.pools = dict(pools)
        self.max_ok_batch = max_ok_batch
        self.calls: list[tuple[str, int]] = []

    def rpc(self, name, params):
        return _Call(self, name, params)

    def answer(self, name, params):
        size = params["p_batch_size"]
        self.calls.append((name, size))
        if size > self.max_ok_batch[name]:
            raise _gateway_timeout()
        n = min(size, self.pools[name])
        self.pools[name] -= n
        return _Res(n)


def _install(monkeypatch, client, pings, sleeps=None):
    async def recording_ping(slug, fail=False):
        pings.append((slug, fail))

    async def recording_sleep(seconds):
        if sleeps is not None:
            sleeps.append(seconds)

    monkeypatch.setattr(tr, "_backoff_sleep", recording_sleep)
    monkeypatch.setattr("services.healthcheck.ping", recording_ping, raising=False)
    monkeypatch.setattr(
        "services.sb_client.make_supabase_client",
        lambda url, key, **kw: client,
        raising=False,
    )


def test_504_halves_batch_and_finishes_the_night(monkeypatch):
    client = _ScriptedClient(
        pools={"backfill_service_variants": 12_000, "backfill_untagged_services_via_variant": 700},
        max_ok_batch={"backfill_service_variants": 1_250, "backfill_untagged_services_via_variant": 5_000},
    )
    pings: list[tuple] = []
    sleeps: list[float] = []
    _install(monkeypatch, client, pings, sleeps)

    msg = asyncio.run(tr.refresh_service_variants({}))

    assert sleeps == [tr.VARIANT_TIMEOUT_BACKOFF_S] * 2, (
        "Kong ucina po 60 s, ale Postgres liczy porcję dalej (max 91 s zmierzone) — "
        "ponowienie bez przerwy nakłada drugi pełny skan na wciąż trwający"
    )
    assert tr.VARIANT_TIMEOUT_BACKOFF_S >= 60
    assert client.pools["backfill_service_variants"] == 0, "504 nie może zostawić backlogu, który mieścił się w mniejszych porcjach"
    assert client.pools["backfill_untagged_services_via_variant"] == 0, "faza B musi się wykonać mimo 504 w fazie A"
    sizes_a = [s for name, s in client.calls if name == "backfill_service_variants"]
    assert sizes_a[:3] == [5_000, 2_500, 1_250], f"porcja ma się połowić po 504, było {sizes_a[:3]}"
    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", False)]
    assert "in_tid=12000" in msg and "untagged=700" in msg


def test_504_at_minimum_batch_ends_phase_but_keeps_progress(monkeypatch):
    """Postęp był, potem baza tak zajęta, że nawet minimalna porcja dostaje 504."""
    state = {"ok_left": 2}

    class _DegradingClient(_ScriptedClient):
        def answer(self, name, params):
            if name == "backfill_service_variants":
                self.calls.append((name, params["p_batch_size"]))
                if state["ok_left"] > 0:
                    state["ok_left"] -= 1
                    return _Res(params["p_batch_size"])
                raise _gateway_timeout()
            return super().answer(name, params)

    client = _DegradingClient(
        pools={"backfill_untagged_services_via_variant": 300},
        max_ok_batch={"backfill_untagged_services_via_variant": 5_000},
    )
    pings: list[tuple] = []
    _install(monkeypatch, client, pings)

    msg = asyncio.run(tr.refresh_service_variants({}))

    sizes_a = [s for name, s in client.calls if name == "backfill_service_variants"]
    assert min(sizes_a) == tr.VARIANT_MIN_BATCH_SIZE, "przed poddaniem fazy trzeba zejść do minimalnej porcji"
    assert client.pools["backfill_untagged_services_via_variant"] == 0, "faza B rusza mimo poddanej fazy A"
    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", False)]
    assert "in_tid=10000" in msg
    assert "timeouts" in msg, "log musi mówić, że faza skończyła się na 504, nie na pustym backlogu"


def test_only_504_and_zero_progress_is_a_loud_failure(monkeypatch):
    client = _ScriptedClient(
        pools={"backfill_service_variants": 50_000, "backfill_untagged_services_via_variant": 50_000},
        max_ok_batch={"backfill_service_variants": 0, "backfill_untagged_services_via_variant": 0},
    )
    pings: list[tuple] = []
    _install(monkeypatch, client, pings)

    with pytest.raises(RuntimeError):
        asyncio.run(tr.refresh_service_variants({}))

    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", True)]


def test_one_phase_stuck_on_504_with_zero_rows_is_loud_even_if_other_moved(monkeypatch):
    """Faza A zrobiła swoje, faza B nie przerobiła ani wiersza przez 504 — to nie jest „zajęta noc"."""
    client = _ScriptedClient(
        pools={"backfill_service_variants": 8_000, "backfill_untagged_services_via_variant": 9_000},
        max_ok_batch={"backfill_service_variants": 5_000, "backfill_untagged_services_via_variant": 0},
    )
    pings: list[tuple] = []
    _install(monkeypatch, client, pings)

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(tr.refresh_service_variants({}))

    assert client.pools["backfill_service_variants"] == 0, "postęp fazy A zostaje"
    assert "untagged=0 (timeouts)" in str(excinfo.value)
    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", True)]


def test_non_timeout_errors_still_propagate(monkeypatch):
    class _BrokenClient(_ScriptedClient):
        def answer(self, name, params):
            raise APIError({"message": "function does not exist", "code": "42883"})

    client = _BrokenClient(pools={}, max_ok_batch={})
    pings: list[tuple] = []
    _install(monkeypatch, client, pings)

    with pytest.raises(APIError):
        asyncio.run(tr.refresh_service_variants({}))

    assert pings == [("HC_PING_VARIANT_MATCH_REFRESH", True)]
