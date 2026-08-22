"""Central writer for `bagent_error_log` (Supabase mig 014 + 094).

WHY: the table was EMPTY since creation (BEAUTY_AUDIT-n3zw). The only
writer in bagent lived on the `pipeline_traces` flush-failure path, which
is never reached in practice, and the Convex-side `withBagentRetry` only
logs failures of the *enqueue* HTTP call (bagent answers 202 immediately),
never failures of the job itself. arq's `on_job_end` hook does not expose
the exception, so the one central point we have is the task registration
list in `workers/main.py` — every task is wrapped with `log_task_errors`
before being handed to arq.

Best-effort by design: a failure to write the log row must NEVER mask or
alter the original exception (arq still retries / fails the job as before).
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import json
import logging
import traceback
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

# Allowed values of bagent_error_log.bagent_pipeline (CHECK in mig 094).
# Task names outside this map are written verbatim — they will be rejected
# by the CHECK until the constraint is relaxed; the rejection is logged.
_PIPELINE_BY_TASK: dict[str, str] = {
    "run_report_task": "report",
    "run_free_report_task": "report",
    "run_cennik_task": "cennik",
    "run_summary_task": "summary",
    "run_competitor_report_task": "competitor_selection",
    "run_competitor_refresh_task": "competitor_selection",
    "embed_new_services": "embedding",
    "refresh_staff_identity_links": "staff_identity_refresh",
    "refresh_inferred_treatments": "taxonomy_inference",
}

# Exceptions that are not errors from the operator's point of view.
_SKIP_EXC_NAMES = frozenset({"_CancelledByUser", "Retry", "RetryJob"})

_AUDIT_ID_KEYS = ("auditId", "audit_id", "convex_audit_id", "convexAuditId")
_USER_ID_KEYS = ("userId", "user_id", "convex_user_id")

_MAX_MESSAGE = 2000
_MAX_STACK = 8000
_MAX_ARG_REPR = 4000
# Hard ceiling for the Supabase insert — a hanging write must never delay
# the job's completion (arq would keep the slot busy and the retry clock frozen).
WRITE_TIMEOUT_S = 5.0

ClientFactory = Callable[[], Any]
_client_factory: Optional[ClientFactory] = None


def set_client_factory(factory: Optional[ClientFactory]) -> None:
    """Override how the Supabase client is obtained (tests / DI)."""
    global _client_factory
    _client_factory = factory


def _default_client() -> Any:
    from config import settings
    from services.sb_client import make_supabase_client

    return make_supabase_client(settings.supabase_url, settings.supabase_service_key)


def _get_client() -> Any:
    return (_client_factory or _default_client)()


def classify_error(exc: BaseException) -> str:
    """Map an exception to bagent_error_log.error_type (CHECK in mig 014)."""
    name = type(exc).__name__
    mro = " ".join(c.__name__ for c in type(exc).__mro__)
    # arq enforces job_timeout via asyncio.wait_for -> the task body sees CancelledError.
    if isinstance(exc, (asyncio.CancelledError, asyncio.TimeoutError, TimeoutError)) or "Timeout" in name:
        return "timeout"
    if "APIError" in mro or "postgrest" in type(exc).__module__:
        return "supabase_error"
    if "HTTPStatusError" in mro or "httpx" in type(exc).__module__:
        return "http_error"
    if "ValidationError" in mro or isinstance(exc, (ValueError, TypeError, KeyError)):
        return "validation_error"
    if any(t in name for t in ("MiniMax", "LLM", "OpenAI", "Anthropic", "Gemini", "Model")):
        return "ai_error"
    return "unknown"


def _first_dict(args: tuple, kwargs: dict) -> Optional[dict]:
    for candidate in (*args, *kwargs.values()):
        if isinstance(candidate, dict):
            return candidate
    return None


def _pick(d: Optional[dict], keys: tuple[str, ...]) -> Optional[str]:
    if not d:
        return None
    for k in keys:
        v = d.get(k)
        if v:
            return str(v)
    return None


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)[:_MAX_ARG_REPR]


def build_error_row(
    task_name: str,
    exc: BaseException,
    *,
    ctx: Optional[dict] = None,
    args: tuple = (),
    kwargs: Optional[dict] = None,
) -> dict[str, Any]:
    """Pure function: shape one bagent_error_log row from a failed task call."""
    ctx = ctx or {}
    kwargs = kwargs or {}
    request = _first_dict(args, kwargs)
    job_try = int(ctx.get("job_try") or 1)
    payload = {
        "task": task_name,
        "job_id": ctx.get("job_id"),
        "job_try": job_try,
        "args": [_safe_json(a) for a in args],
        "kwargs": {k: _safe_json(v) for k, v in kwargs.items()},
    }
    return {
        "convex_audit_id": _pick(request, _AUDIT_ID_KEYS) or "(none)",
        "convex_user_id": _pick(request, _USER_ID_KEYS),
        "bagent_pipeline": _PIPELINE_BY_TASK.get(task_name, task_name),
        "attempt_number": min(max(job_try, 1), 3),
        "request_payload": payload,
        "error_type": classify_error(exc),
        "error_message": f"{type(exc).__name__}: {exc}"[:_MAX_MESSAGE],
        "stack_trace": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )[-_MAX_STACK:],
    }


def _insert_row_sync(row: dict[str, Any]) -> None:
    _get_client().table("bagent_error_log").insert(row).execute()


async def record_task_error(
    task_name: str,
    exc: BaseException,
    *,
    ctx: Optional[dict] = None,
    args: tuple = (),
    kwargs: Optional[dict] = None,
) -> bool:
    """Write one row. Returns True on success; never raises."""
    if type(exc).__name__ in _SKIP_EXC_NAMES:
        return False
    try:
        row = build_error_row(task_name, exc, ctx=ctx, args=args, kwargs=kwargs)
        await asyncio.wait_for(asyncio.to_thread(_insert_row_sync, row), WRITE_TIMEOUT_S)
        logger.info("bagent_error_log: recorded %s failure (audit=%s)", task_name, row["convex_audit_id"])
        return True
    except asyncio.TimeoutError:
        logger.warning("bagent_error_log: write timed out after %.1fs for %s", WRITE_TIMEOUT_S, task_name)
        return False
    except Exception as log_exc:  # noqa: BLE001 — must not mask the job error
        logger.warning("bagent_error_log: write failed for %s: %s", task_name, log_exc)
        return False


def log_task_errors(coro: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Wrap an arq task so any exception is recorded in bagent_error_log
    and then re-raised unchanged. `functools.wraps` keeps `__qualname__`,
    which arq uses as the registered function name."""

    if getattr(coro, "_bagent_error_log_wrapped", False):
        return coro  # idempotent — a task listed in both functions and crons is wrapped once

    @functools.wraps(coro)
    async def wrapper(ctx: dict, *args: Any, **kwargs: Any) -> Any:
        try:
            return await coro(ctx, *args, **kwargs)
        except (Exception, asyncio.CancelledError) as exc:
            # CancelledError is how arq delivers job_timeout (asyncio.wait_for).
            # It is a BaseException: record it as error_type="timeout" and
            # re-raise unchanged so arq still marks the job as timed out.
            await record_task_error(coro.__name__, exc, ctx=ctx, args=args, kwargs=kwargs)
            raise

    wrapper._bagent_error_log_wrapped = True  # type: ignore[attr-defined]
    return wrapper


def wrap_all(tasks: list[Callable[..., Awaitable[Any]]]) -> list[Callable[..., Awaitable[Any]]]:
    return [log_task_errors(t) for t in tasks]


def wrap_crons(cron_jobs: list[Any]) -> list[Any]:
    """Wrap the coroutine of every arq `CronJob`.

    arq registers cron_jobs SEPARATELY from `functions`
    (Worker.__init__: functions.update({cj.name: cj})), so `wrap_all` on the
    functions list never touches them. `cron()` resolves string paths via
    import_string at construction time, so `cj.coroutine` is always a real
    coroutine function here — both registration styles end up identical.
    The CronJob is a dataclass: `replace` keeps name/schedule/timeout/max_tries
    and swaps only the coroutine (new object, original untouched)."""
    return [dataclasses.replace(cj, coroutine=log_task_errors(cj.coroutine)) for cj in cron_jobs]


def unwrapped(functions: list[Callable[..., Any]]) -> list[Callable[..., Any]]:
    """Original task objects behind `wrap_all` (tests assert registration by identity)."""
    return [getattr(f, "__wrapped__", f) for f in functions]
