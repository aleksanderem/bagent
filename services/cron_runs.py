"""Ślad przebiegów cronów arq w Redisie — dla zakładki „Crawlery i diagnostyka".

arq nie zapisuje nigdzie, kiedy cron ostatnio chodził ani czy się udał; widać
to tylko w logach PM2. Tu każdy cron zostawia po sobie mały hash w Redisie
(``bagent:cron:<nazwa>``): kiedy wystartował, kiedy skończył, czy się udał,
ile trwał, ostatni błąd i licznik przebiegów. Harmonogram publikują workery
przy starcie (``publish_schedules``), więc cron, który jeszcze nie chodził,
też jest na liście.

Zapis nigdy nie może położyć zadania — każdy błąd Redisa jest tylko logowany.
"""

from __future__ import annotations

import functools
import logging
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

KEY_PREFIX = "bagent:cron:"
ERROR_MAX_CHARS = 300


def cron_key(name: str) -> str:
    return f"{KEY_PREFIX}{name}"


def display_name(name: str) -> str:
    """arq nazywa crony ``cron:workers.x.y`` — panel pokazuje ``workers.x.y``."""
    return name.removeprefix("cron:")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _sorted(values: Any) -> list[int]:
    if values is None:
        return []
    if isinstance(values, int):
        return [values]
    return sorted(int(v) for v in values)


def expected_every_seconds(cj: Any) -> int:
    """Szacowany odstęp między przebiegami cronu arq (do wykrywania spóźnień).

    Godzina ustawiona → raz na dobę / liczbę godzin. Tylko minuty → co
    (60 / liczba minut) minut. Sekundy zagęszczają dalej. Dzień tygodnia /
    miesiąca → raz na tydzień / miesiąc.
    """
    if getattr(cj, "weekday", None) is not None:
        return 7 * 86400
    if getattr(cj, "day", None) is not None:
        return 30 * 86400
    hours = _sorted(getattr(cj, "hour", None))
    minutes = _sorted(getattr(cj, "minute", None))
    seconds = _sorted(getattr(cj, "second", None))
    if hours:
        per_day = len(hours) * max(1, len(minutes))
        return max(60, math.floor(86400 / per_day))
    if minutes:
        per_hour = len(minutes) * max(1, len(seconds))
        return max(1, math.floor(3600 / per_hour))
    return 60


def schedule_text(cj: Any) -> str:
    """Czytelny opis harmonogramu: „co 5 min”, „codziennie 06:15 UTC”, „o :05”."""
    hours = _sorted(getattr(cj, "hour", None))
    minutes = _sorted(getattr(cj, "minute", None))
    seconds = _sorted(getattr(cj, "second", None))
    if hours:
        minute = minutes[0] if minutes else 0
        times = ", ".join(f"{h:02d}:{minute:02d}" for h in hours)
        return f"codziennie {times} UTC"
    if minutes:
        if len(seconds) > 1:
            return f"co {max(1, 60 // len(seconds))} s"
        if len(minutes) >= 60:
            return "co minutę"
        if len(minutes) > 1:
            step = 60 // len(minutes)
            offset = minutes[0]
            return f"co {step} min" + (f" (od :{offset:02d})" if offset else "")
        return f"co godzinę o :{minutes[0]:02d}"
    return "wg arq"


def track_cron_run(name: str, coro: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Owiń cron arq tak, żeby zapisywał start/koniec/wynik do Redisa."""

    @functools.wraps(coro)
    async def wrapper(ctx: dict, *args: Any, **kwargs: Any) -> Any:
        redis = ctx.get("redis") if isinstance(ctx, dict) else None
        key = cron_key(name)
        started = time.monotonic()
        await _safe_hset(redis, key, {"last_started_at": _now_iso()})
        try:
            result = await coro(ctx, *args, **kwargs)
        except BaseException as exc:
            await _safe_hset(
                redis,
                key,
                {
                    "last_finished_at": _now_iso(),
                    "last_ok": "0",
                    "last_duration_ms": str(int((time.monotonic() - started) * 1000)),
                    "last_error": f"{type(exc).__name__}: {exc}"[:ERROR_MAX_CHARS],
                },
                incr="fails",
            )
            raise
        await _safe_hset(
            redis,
            key,
            {
                "last_finished_at": _now_iso(),
                "last_ok": "1",
                "last_duration_ms": str(int((time.monotonic() - started) * 1000)),
                "last_error": "",
            },
            incr="runs",
        )
        return result

    return wrapper


async def _safe_hset(redis: Any, key: str, mapping: dict[str, str], incr: str | None = None) -> None:
    if redis is None:
        return
    try:
        await redis.hset(key, mapping=mapping)
        if incr:
            await redis.hincrby(key, incr, 1)
    except Exception as exc:  # noqa: BLE001 — ślad nie może położyć zadania
        logger.debug("[cron-runs] zapis %s nieudany: %s", key, exc)


async def publish_schedules(redis: Any, entries: Iterable[tuple[str, str, Any]]) -> int:
    """Zapisz harmonogram i workera każdego cronu: (nazwa, worker, CronJob)."""
    count = 0
    for name, worker, cj in entries:
        await _safe_hset(
            redis,
            cron_key(name),
            {
                "worker": worker,
                "schedule": schedule_text(cj),
                "expected_every_s": str(expected_every_seconds(cj)),
            },
        )
        count += 1
    return count


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def list_cron_runs(redis: Any) -> list[dict[str, Any]]:
    """Wszystkie ślady cronów, posortowane po workerze i nazwie."""
    out: list[dict[str, Any]] = []
    async for raw_key in redis.scan_iter(match=f"{KEY_PREFIX}*", count=200):
        key = _decode(raw_key)
        fields = {_decode(k): _decode(v) for k, v in (await redis.hgetall(key)).items()}
        last_ok = fields.get("last_ok")
        out.append(
            {
                "name": display_name(key.removeprefix(KEY_PREFIX)),
                "worker": fields.get("worker", ""),
                "schedule": fields.get("schedule", ""),
                "expected_every_s": int(fields.get("expected_every_s") or 0),
                "last_started_at": fields.get("last_started_at") or None,
                "last_finished_at": fields.get("last_finished_at") or None,
                "last_ok": None if last_ok in (None, "") else last_ok == "1",
                "last_duration_ms": int(fields["last_duration_ms"]) if fields.get("last_duration_ms") else None,
                "last_error": fields.get("last_error") or None,
                "runs": int(fields.get("runs") or 0),
                "fails": int(fields.get("fails") or 0),
            }
        )
    out.sort(key=lambda r: (r["worker"], r["name"]))
    return out
