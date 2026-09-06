"""GET /api/internal/diag + services/diagnostics.py — autodiagnostyka bagenta."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

import services.diagnostics as diag
from config import settings
from server import app

client = TestClient(app)


def test_parse_arq_health_counters_and_age():
    now = datetime(2026, 9, 6, 15, 30, 0)
    parsed = diag.parse_arq_health(b"Sep-06 15:01:50 j_complete=541 j_failed=1 j_retried=0 j_ongoing=3 queued=3", now=now)
    assert parsed == {
        "at": "2026-09-06T15:01:50",
        "age_s": 1690,
        "complete": 541,
        "failed": 1,
        "retried": 0,
        "ongoing": 3,
        "queued": 3,
    }
    assert diag.parse_arq_health(None) is None
    assert diag.parse_arq_health("garbage") == {"raw": "garbage"}


def test_parse_arq_health_handles_year_rollover():
    now = datetime(2027, 1, 1, 0, 10, 0)
    parsed = diag.parse_arq_health("Dec-31 23:55:00 j_complete=1", now=now)
    assert parsed["at"] == "2026-12-31T23:55:00"
    assert parsed["age_s"] == 900


def test_parse_pm2_jlist_maps_fields():
    now_ms = int(datetime.now().timestamp() * 1000)
    raw = json.dumps(
        [
            {"name": "bagent-worker", "pm2_env": {"status": "online", "restart_time": 2, "pm_uptime": now_ms - 60_000}, "monit": {"memory": 10 * 1024 * 1024, "cpu": 0.5}},
            {"name": "api-booksyaudit", "pm2_env": {"status": "stopped", "restart_time": 9, "pm_uptime": now_ms}, "monit": {"memory": 0, "cpu": 0}},
        ]
    )
    rows = diag.parse_pm2_jlist(raw)
    assert rows[0]["name"] == "bagent-worker" and rows[0]["status"] == "online"
    assert rows[0]["restarts"] == 2 and rows[0]["memory_mb"] == 10.0 and rows[0]["cpu"] == 0.5
    assert 55_000 <= rows[0]["uptime_ms"] <= 120_000
    assert rows[1]["status"] == "stopped" and rows[1]["uptime_ms"] is None


def test_latest_backup_reports_newest_dump(tmp_path):
    old = tmp_path / "2026-09-04.dump"
    new = tmp_path / "2026-09-05.dump"
    old.write_bytes(b"x" * 10)
    new.write_bytes(b"y" * 2048)
    import os
    import time

    os.utime(old, (time.time() - 3 * 86400, time.time() - 3 * 86400))
    info = diag.latest_backup(str(tmp_path))
    assert info["latest_file"] == "2026-09-05.dump"
    assert info["count"] == 2
    assert 0 <= info["age_hours"] < 1
    assert info["size_mb"] == 0.0
    assert diag.latest_backup(str(tmp_path / "nie-ma"))["error"] == "brak plików"


@pytest.mark.asyncio
async def test_redis_queues_reads_depth_and_health():
    pool = MagicMock()
    pool.ping = AsyncMock(return_value=True)
    pool.zcard = AsyncMock(side_effect=[3, 0])
    pool.get = AsyncMock(side_effect=[b"Sep-06 15:01:50 j_complete=5 j_ongoing=1 queued=3", None])
    out = await diag.redis_queues(pool)
    assert out["ok"] is True
    assert out["queues"]["arq:queue"]["depth"] == 3
    assert out["queues"]["arq:queue"]["health"]["complete"] == 5
    assert out["queues"]["arq:reports"]["depth"] == 0
    assert out["queues"]["arq:reports"]["health"] is None, "brak klucza = worker nigdy nie zapisał zdrowia"

    assert (await diag.redis_queues(None))["ok"] is False


def test_endpoint_requires_api_key():
    assert client.get("/api/internal/diag").status_code in (401, 422)
    assert client.get("/api/internal/diag", headers={"x-api-key": "zly"}).status_code == 401


def test_endpoint_assembles_sections_independently(monkeypatch, tmp_path):
    async def fake_pm2() -> dict:
        return {"processes": [{"name": "bagent-worker", "status": "online", "restarts": 0, "uptime_ms": 1, "memory_mb": 1.0, "cpu": 0}], "expected": list(diag.EXPECTED_PM2)}

    async def fake_sha() -> str:
        return "abc1234"

    monkeypatch.setattr(diag, "pm2_processes", fake_pm2)
    monkeypatch.setattr(diag, "git_sha", fake_sha)
    monkeypatch.setattr(settings, "backup_dir", str(tmp_path))
    prev = getattr(app.state, "arq", None)
    app.state.arq = None
    try:
        res = client.get("/api/internal/diag", headers={"x-api-key": settings.api_key})
    finally:
        app.state.arq = prev
    assert res.status_code == 200
    body = res.json()
    assert body["process"]["git_sha"] == "abc1234" and body["process"]["pid"] > 0
    assert body["pm2"]["processes"][0]["name"] == "bagent-worker"
    assert body["disk"]["used_pct"] is not None and body["disk"]["path"] == "/"
    assert body["backup"]["error"] == "brak plików", "brak zrzutów to informacja, nie wyjątek"
    assert body["redis"]["ok"] is False and body["redis"]["queues"] == {}
    assert body["crons"]["items"] == []
