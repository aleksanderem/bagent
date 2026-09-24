"""services/cron_runs.py — ślad przebiegów cronów w Redisie."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.cron_runs import (
    cron_key,
    expected_every_seconds,
    list_cron_runs,
    publish_schedules,
    schedule_text,
    track_cron_run,
)


class FakeRedis:
    """Minimum redis.asyncio potrzebne przez cron_runs: hash + scan."""

    def __init__(self) -> None:
        self.data: dict[str, dict[str, str]] = {}

    async def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.data.setdefault(key, {}).update(mapping)

    async def hincrby(self, key: str, field: str, amount: int) -> None:
        h = self.data.setdefault(key, {})
        h[field] = str(int(h.get(field, "0")) + amount)

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        return {k.encode(): v.encode() for k, v in self.data.get(key, {}).items()}

    async def scan_iter(self, match: str, count: int = 100):
        prefix = match.rstrip("*")
        for key in list(self.data):
            if key.startswith(prefix):
                yield key.encode()

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.data.pop(key, None)


def cj(**kw):
    base = {"hour": None, "minute": None, "second": None, "weekday": None, "day": None}
    return SimpleNamespace(**{**base, **kw})


def test_schedule_text_and_interval():
    assert schedule_text(cj(hour={6}, minute={15})) == "codziennie 06:15 UTC"
    assert expected_every_seconds(cj(hour={6}, minute={15})) == 86400
    assert schedule_text(cj(hour={4, 10, 16, 22}, minute={27})) == "codziennie 04:27, 10:27, 16:27, 22:27 UTC"
    assert expected_every_seconds(cj(hour={4, 10, 16, 22}, minute={27})) == 21600
    assert schedule_text(cj(minute={5})) == "co godzinę o :05"
    assert expected_every_seconds(cj(minute={5})) == 3600
    assert schedule_text(cj(minute=set(range(3, 60, 5)))) == "co 5 min (od :03)"
    assert expected_every_seconds(cj(minute=set(range(0, 60, 5)))) == 300
    assert schedule_text(cj(minute=set(range(60)))) == "co minutę"
    assert schedule_text(cj(minute=set(range(60)), second={0, 15, 30, 45})) == "co 15 s"
    assert expected_every_seconds(cj(minute=set(range(60)), second={0, 15, 30, 45})) == 15
    assert expected_every_seconds(cj(weekday=0, hour={3})) == 7 * 86400


@pytest.mark.asyncio
async def test_track_cron_run_records_success_and_failure():
    redis = FakeRedis()
    calls: list[str] = []

    async def ok_task(ctx: dict) -> str:
        calls.append("ok")
        return "done"

    async def bad_task(ctx: dict) -> None:
        raise RuntimeError("Konga 504")

    wrapped_ok = track_cron_run("cron:workers.x.ok", ok_task)
    wrapped_bad = track_cron_run("cron:workers.x.bad", bad_task)
    assert wrapped_ok.__name__ == "ok_task", "functools.wraps zachowuje nazwę dla arq"

    assert await wrapped_ok({"redis": redis}) == "done"
    with pytest.raises(RuntimeError):
        await wrapped_bad({"redis": redis})

    ok = redis.data[cron_key("cron:workers.x.ok")]
    assert ok["last_ok"] == "1" and ok["runs"] == "1" and ok["last_error"] == ""
    assert ok["last_started_at"] and ok["last_finished_at"]
    bad = redis.data[cron_key("cron:workers.x.bad")]
    assert bad["last_ok"] == "0" and bad["fails"] == "1"
    assert bad["last_error"] == "RuntimeError: Konga 504"


@pytest.mark.asyncio
async def test_track_cron_run_without_redis_still_runs_task():
    async def task(ctx: dict) -> int:
        return 7

    assert await track_cron_run("cron:x", task)({}) == 7
    assert await track_cron_run("cron:x", task)({"redis": None}) == 7


@pytest.mark.asyncio
async def test_publish_and_list_merge_schedule_with_runs():
    redis = FakeRedis()
    n = await publish_schedules(
        redis,
        [("cron:workers.a.daily", "scrape", cj(hour={6}, minute={15})), ("cron:workers.b.drain", "reports", cj(minute=set(range(60))))],
    )
    assert n == 2

    async def task(ctx: dict) -> None:
        return None

    await track_cron_run("cron:workers.a.daily", task)({"redis": redis})
    rows = await list_cron_runs(redis)
    assert [r["name"] for r in rows] == ["workers.b.drain", "workers.a.daily"], "sortowanie: worker, potem nazwa"
    daily = next(r for r in rows if r["name"] == "workers.a.daily")
    assert daily["worker"] == "scrape"
    assert daily["schedule"] == "codziennie 06:15 UTC"
    assert daily["expected_every_s"] == 86400
    assert daily["last_ok"] is True and daily["runs"] == 1 and daily["fails"] == 0
    drain = next(r for r in rows if r["name"] == "workers.b.drain")
    assert drain["last_ok"] is None and drain["last_started_at"] is None, "jeszcze nie chodził"


@pytest.mark.asyncio
async def test_publish_prunes_traces_of_crons_removed_from_schedule():
    """Cron wyłączony w kodzie (15.09: outreach) nie może wisieć w panelu jako „spóźniony"."""
    redis = FakeRedis()
    daily = ("cron:workers.a.daily", "scrape", cj(hour={6}, minute={15}))
    await publish_schedules(redis, [daily, ("cron:workers.outreach.send", "reports", cj(minute=set(range(60))))])
    await publish_schedules(redis, [daily])
    assert [r["name"] for r in await list_cron_runs(redis)] == ["workers.a.daily"]


@pytest.mark.asyncio
async def test_publish_with_empty_schedule_keeps_existing_traces():
    """Pusta lista to błąd konfiguracji, nie sygnał „skasuj wszystko"."""
    redis = FakeRedis()
    await publish_schedules(redis, [("cron:workers.a.daily", "scrape", cj(hour={6}, minute={15}))])
    await publish_schedules(redis, [])
    assert [r["name"] for r in await list_cron_runs(redis)] == ["workers.a.daily"]
