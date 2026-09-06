"""Autodiagnostyka procesu bagenta i serwera tytan — GET /api/internal/diag.

Panel admina (Convex convex/admin/diagnostics.ts) składa z tego zakładkę
„Crawlery i diagnostyka". Zbieramy tylko to, czego Convex sam nie zobaczy:
procesy PM2 na serwerze, dysk, wiek ostatniego zrzutu bazy, kolejki arq
w Redisie z żywotnością workerów i ślady cronów (services/cron_runs.py).

Każda część jest niezależna: gdy np. pm2 nie odpowie, reszta wraca normalnie,
a w danej sekcji jest pole ``error``.
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import re
import shutil
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from services.cron_runs import list_cron_runs

STARTED_AT = datetime.now(UTC)
REPO_ROOT = Path(__file__).resolve().parent.parent
QUEUES: tuple[tuple[str, str], ...] = (("arq:queue", "scrape"), ("arq:reports", "reports"))
EXPECTED_PM2 = ("booksyauditor", "api-booksyaudit", "bagent-booksyauditor", "bagent-worker", "bagent-report-worker")
SUBPROCESS_TIMEOUT_S = 10

_HEALTH_RE = re.compile(r"^(?P<ts>[A-Za-z]{3}-\d{2} \d{2}:\d{2}:\d{2})\s*(?P<rest>.*)$")


def find_pm2() -> str | None:
    found = shutil.which("pm2")
    if found:
        return found
    home = os.path.expanduser("~")
    candidates = sorted(glob.glob(f"{home}/.nvm/versions/node/*/bin/pm2"), reverse=True)
    return candidates[0] if candidates else None


async def _run(cmd: list[str], cwd: Path | None = None) -> str:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=SUBPROCESS_TIMEOUT_S)
    except TimeoutError:
        proc.kill()
        raise RuntimeError(f"{cmd[0]} nie odpowiedział w {SUBPROCESS_TIMEOUT_S} s") from None
    if proc.returncode != 0:
        raise RuntimeError(err.decode(errors="replace").strip()[:200] or f"kod wyjścia {proc.returncode}")
    return out.decode(errors="replace")


def parse_pm2_jlist(raw: str) -> list[dict[str, Any]]:
    """``pm2 jlist`` → lekka lista procesów (nazwa, stan, restarty, uptime, pamięć)."""
    data = json.loads(raw)
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    out = []
    for proc in data:
        env = proc.get("pm2_env") or {}
        monit = proc.get("monit") or {}
        uptime_ms = None
        started_at = None
        if env.get("pm_uptime"):
            started_at = datetime.fromtimestamp(int(env["pm_uptime"]) / 1000, UTC).isoformat(timespec="seconds")
        if env.get("status") == "online" and env.get("pm_uptime"):
            uptime_ms = max(0, now_ms - int(env["pm_uptime"]))
        out.append(
            {
                "name": proc.get("name"),
                "status": env.get("status") or "unknown",
                "restarts": int(env.get("restart_time") or 0),
                "uptime_ms": uptime_ms,
                "started_at": started_at,
                "memory_mb": round((monit.get("memory") or 0) / 1024 / 1024, 1),
                "cpu": monit.get("cpu") or 0,
            }
        )
    return out


async def pm2_processes() -> dict[str, Any]:
    binary = find_pm2()
    if not binary:
        return {"processes": [], "error": "nie znaleziono programu pm2"}
    try:
        raw = await _run([binary, "jlist"])
        return {"processes": parse_pm2_jlist(raw), "expected": list(EXPECTED_PM2)}
    except Exception as exc:  # noqa: BLE001
        return {"processes": [], "expected": list(EXPECTED_PM2), "error": str(exc)}


def disk_usage(path: str = "/") -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return {"path": path, "error": str(exc)}
    gb = 1024**3
    return {
        "path": path,
        "total_gb": round(usage.total / gb, 1),
        "used_gb": round(usage.used / gb, 1),
        "free_gb": round(usage.free / gb, 1),
        "used_pct": round(usage.used / usage.total * 100, 1) if usage.total else None,
    }


def latest_backup(directory: str, pattern: str = "*.dump") -> dict[str, Any]:
    """Najnowszy zrzut bazy w katalogu backupów: nazwa, wiek w godzinach, rozmiar."""
    try:
        files = [p for p in Path(directory).glob(pattern) if p.is_file()]
    except OSError as exc:
        return {"dir": directory, "error": str(exc)}
    if not files:
        return {"dir": directory, "error": "brak plików"}
    newest = max(files, key=lambda p: p.stat().st_mtime)
    stat = newest.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime, UTC)
    return {
        "dir": directory,
        "latest_file": newest.name,
        "latest_at": mtime.isoformat(timespec="seconds"),
        "age_hours": round((datetime.now(UTC) - mtime).total_seconds() / 3600, 1),
        "size_mb": round(stat.st_size / 1024 / 1024, 1),
        "count": len(files),
    }


def parse_arq_health(raw: str | bytes | None, now: datetime | None = None) -> dict[str, Any] | None:
    """Klucz ``<kolejka>:health-check`` arq: „Sep-06 15:01:50 j_complete=541 …”.

    arq zapisuje czas lokalny hosta bez roku (tytan chodzi w UTC), odświeżany
    co health_check_interval (domyślnie 3600 s). ``age_s`` liczymy względem
    lokalnego „teraz” tego samego hosta.
    """
    if raw is None:
        return None
    text = raw.decode() if isinstance(raw, bytes) else str(raw)
    match = _HEALTH_RE.match(text.strip())
    if not match:
        return {"raw": text[:120]}
    now = now or datetime.now()
    try:
        stamp = datetime.strptime(f"{now.year}-{match.group('ts')}", "%Y-%b-%d %H:%M:%S")
    except ValueError:
        return {"raw": text[:120]}
    if stamp > now and (stamp - now).days > 1:
        stamp = stamp.replace(year=now.year - 1)
    counters: dict[str, int] = {}
    for part in match.group("rest").split():
        key, _, value = part.partition("=")
        if value.lstrip("-").isdigit():
            counters[key.removeprefix("j_")] = int(value)
    return {"at": stamp.isoformat(timespec="seconds"), "age_s": int((now - stamp).total_seconds()), **counters}


async def redis_queues(pool: Any) -> dict[str, Any]:
    if pool is None:
        return {"ok": False, "error": "brak połączenia z Redisem w procesie API", "queues": {}}
    queues: dict[str, Any] = {}
    try:
        await pool.ping()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"Redis nie odpowiada: {exc}", "queues": {}}
    for name, label in QUEUES:
        entry: dict[str, Any] = {"label": label}
        try:
            entry["depth"] = await pool.zcard(name)
        except Exception as exc:  # noqa: BLE001
            entry["error"] = str(exc)
        try:
            entry["health"] = parse_arq_health(await pool.get(f"{name}:health-check"))
        except Exception as exc:  # noqa: BLE001
            entry["health"] = None
            entry["error"] = str(exc)
        queues[name] = entry
    return {"ok": True, "queues": queues}


PM2_LOG_DIR = Path(os.path.expanduser("~/.pm2/logs"))
LOG_TAIL_BYTES = 256 * 1024
LOG_LINE_MAX = 300
LOG_ERROR_RE = re.compile(r"(ERROR|CRITICAL|WARNING|Traceback|Exception|Error:|\bERR\b|UnhandledPromise)")


def tail_lines(path: Path, max_bytes: int = LOG_TAIL_BYTES) -> list[str]:
    """Ostatnie linie pliku bez czytania całości (logi PM2 bywają duże)."""
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            fh.seek(max(0, size - max_bytes))
            chunk = fh.read()
    except OSError:
        return []
    text = chunk.decode(errors="replace")
    lines = text.splitlines()
    if size > max_bytes and lines:
        lines = lines[1:]  # pierwsza linia może być ucięta w środku
    return [ln.rstrip()[:LOG_LINE_MAX] for ln in lines if ln.strip()]


def pm2_log_tails(names: tuple[str, ...] = EXPECTED_PM2, log_dir: Path | None = None, errors_n: int = 25, tail_n: int = 10) -> dict[str, Any]:
    """Dla każdego procesu PM2: ostatnie linie z błędami i surowy ogon error-loga.

    Python loguje na stderr, więc `<nazwa>-error.log` zawiera też INFO —
    `errors` to tylko linie wyglądające na problem, `tail` to ostatnie linie
    bez filtra (kontekst, „co robił przed chwilą").
    """
    log_dir = log_dir or PM2_LOG_DIR
    out: dict[str, Any] = {}
    for name in names:
        err_path = log_dir / f"{name}-error.log"
        lines = tail_lines(err_path)
        out[name] = {
            "file": str(err_path),
            "errors": [ln for ln in lines if LOG_ERROR_RE.search(ln)][-errors_n:],
            "tail": lines[-tail_n:],
        }
    return out


SYSTEMD_TIMER_GLOB = "booksy-*"
BACKUP_LOG = Path("/var/log/backup-supabase.log")


def _usec_to_iso(value: Any) -> str | None:
    """systemctl --output=json podaje czasy w mikrosekundach od epoki (0 = brak)."""
    try:
        usec = int(value)
    except (TypeError, ValueError):
        return None
    if usec <= 0:
        return None
    return datetime.fromtimestamp(usec / 1_000_000, UTC).isoformat(timespec="seconds")


def parse_systemd(timers_json: str, unit_files_json: str, failed_json: str) -> dict[str, Any]:
    """Timery booksy-* (ostatni/następny start, czy włączony) + jednostki w stanie failed."""
    states = {u.get("unit_file"): u.get("state") for u in json.loads(unit_files_json or "[]")}
    timers = []
    for t in json.loads(timers_json or "[]"):
        unit = t.get("unit") or ""
        timers.append(
            {
                "unit": unit,
                "activates": t.get("activates"),
                "last": _usec_to_iso(t.get("last")),
                "next": _usec_to_iso(t.get("next")),
                "state": states.get(unit, "unknown"),
            }
        )
    # Timery wyłączone nie pojawiają się w list-timers — dopisz je z unit-files.
    listed = {t["unit"] for t in timers}
    for unit, state in states.items():
        if unit and unit not in listed:
            timers.append({"unit": unit, "activates": None, "last": None, "next": None, "state": state})
    timers.sort(key=lambda t: t["unit"])
    failed = [
        {"unit": u.get("unit"), "description": u.get("description"), "sub": u.get("sub")}
        for u in json.loads(failed_json or "[]")
    ]
    return {"timers": timers, "failed": failed}


async def systemd_status() -> dict[str, Any]:
    """systemctl działa bez uprawnień do odczytu; błąd = sekcja z `error`, reszta normalnie."""
    try:
        timers, units, failed = await asyncio.gather(
            _run(["systemctl", "list-timers", "--all", "--no-pager", "--output=json", SYSTEMD_TIMER_GLOB]),
            _run(["systemctl", "list-unit-files", "--no-pager", "--output=json", f"{SYSTEMD_TIMER_GLOB}.timer"]),
            _run(["systemctl", "list-units", "--state=failed", "--no-pager", "--output=json"]),
        )
        return parse_systemd(timers, units, failed)
    except Exception as exc:  # noqa: BLE001
        return {"timers": [], "failed": [], "error": str(exc)}


def backup_log_tail(path: Path = BACKUP_LOG, n: int = 15) -> list[str]:
    """Ogon logu root-crona zrzutu bazy (04:00 UTC) — gdy plik jest czytelny."""
    return tail_lines(path)[-n:]


async def git_sha() -> str | None:
    try:
        return (await _run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT)).strip() or None
    except Exception:  # noqa: BLE001
        return None


async def collect_diagnostics(pool: Any, backup_dir: str, include_logs: bool = False) -> dict[str, Any]:
    pm2, sha, systemd = await asyncio.gather(pm2_processes(), git_sha(), systemd_status())
    redis = await redis_queues(pool)
    crons: list[dict[str, Any]] = []
    crons_error: str | None = None
    if pool is not None:
        try:
            crons = await list_cron_runs(pool)
        except Exception as exc:  # noqa: BLE001
            crons_error = str(exc)
    now = datetime.now(UTC)
    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "host": {"hostname": socket.gethostname(), "time_utc": now.isoformat(timespec="seconds")},
        "process": {
            "pid": os.getpid(),
            "started_at": STARTED_AT.isoformat(timespec="seconds"),
            "uptime_s": int((now - STARTED_AT).total_seconds()),
            "git_sha": sha,
        },
        "pm2": pm2,
        "disk": disk_usage("/"),
        "backup": {**latest_backup(backup_dir), **({"log_tail": backup_log_tail()} if include_logs else {})},
        "systemd": systemd,
        "redis": redis,
        "crons": {"items": crons, **({"error": crons_error} if crons_error else {})},
        **({"logs": pm2_log_tails()} if include_logs else {}),
    }
