"""Persistent observability for intermediate pipeline decisions.

Backed by `pipeline_traces` (mig 094). The model is append-only and JSONB-
typed so each pipeline writes its own decision shape without schema
coordination. Use cases:

1. Customer-support replay: "why isn't salon X in my report" → query traces
   for (audit_id, salon_ref_id) and see accept/reject reason w/ component
   scores.
2. Regression detection: re-run new algorithm on historical candidate pool
   (from `candidate_evaluated` traces) and compare accept/reject vs old.
3. Algorithm transparency: customer asks "skąd ta cena", we have related-
   samples list with similarity scores per service that contributed.

Conventions:

- Use `TraceWriter` from a pipeline entry point. It buffers writes and
  flushes once at the end via `await writer.flush()`. Failing to flush is
  a NO-GRACEFUL-FAIL — the exception propagates up to crash the pipeline
  so we never silently lose observability.
- Per-step contracts (`step` column values) documented in the migration's
  COMMENT ON COLUMN — keep them in sync.
- trace_data sub-schema is per-pipeline. Keep payloads <100 KB per row to
  stay healthy at scale (typical row is 1-2 KB).
"""

from __future__ import annotations

import json
import logging
import traceback
from typing import Any, Optional

from supabase import Client

logger = logging.getLogger(__name__)


# Max serialized size per row. Anything larger is an instrumentation bug —
# split into multiple rows instead. We hard-fail rather than silently truncate
# so the caller fixes the trace shape.
_MAX_TRACE_BYTES = 256 * 1024


def _log_to_bagent_error(
    client: Client,
    convex_audit_id: str,
    pipeline: str,
    error_type: str,
    error_message: str,
    stack: str,
    payload: dict[str, Any],
) -> None:
    """Best-effort write to `bagent_error_log` so trace failures are visible
    in the operator dashboards. Synchronous because the caller is already
    in an exception path and we don't want to introduce another await chain
    that itself may fail. If THIS write fails too we silently log + move on
    — the original exception is what matters and re-raises in the caller."""
    try:
        client.table("bagent_error_log").insert(
            {
                "convex_audit_id": convex_audit_id or "(unknown)",
                "bagent_pipeline": pipeline,
                "attempt_number": 1,
                "request_payload": payload,
                "error_type": error_type,
                "error_message": error_message[:2000] if error_message else None,
                "stack_trace": stack[:8000] if stack else None,
            }
        ).execute()
    except Exception:
        logger.exception(
            "pipeline_trace: secondary write to bagent_error_log failed",
        )


class TraceWriter:
    """Accumulates trace rows for one pipeline invocation and flushes them
    in a single bulk insert. The caller MUST call `flush()` at the end of
    the pipeline (or in a `finally:` block) — if the pipeline crashes before
    flush, the buffered traces are lost. That's by design: traces only mean
    something coupled to a successful (or at least completed) run.

    Failure modes are explicit:
    - flush() failure → exception re-raises, pipeline crashes loudly
    - add() failure (e.g. JSON serialization) → exception re-raises
    """

    def __init__(
        self,
        client: Client,
        *,
        audit_id: Optional[str],
        report_id: Optional[int],
        pipeline: str,
    ) -> None:
        if not pipeline:
            raise ValueError("TraceWriter: pipeline name is required")
        self.client = client
        self.audit_id = audit_id
        self.report_id = report_id
        self.pipeline = pipeline
        self._buffer: list[dict[str, Any]] = []

    def add(
        self,
        step: str,
        data: dict[str, Any],
        *,
        salon_ref_id: Optional[int] = None,
        tokens_used: Optional[dict[str, Any]] = None,
    ) -> None:
        """Buffer one trace row. Validates that trace_data is JSON-
        serializable + within size limit BEFORE buffering so we fail fast
        at the call site rather than at flush time.

        tokens_used (mig 121, optional): shape {"input": int, "output": int,
        "model": str}. Set on rows that report on an LLM call (step prefix
        `agent.tokens`). When omitted, the DB column defaults to {}::jsonb."""
        if not step:
            raise ValueError("TraceWriter.add: step is required")
        if not isinstance(data, dict):
            raise TypeError(
                f"TraceWriter.add: data must be dict, got {type(data).__name__}",
            )
        try:
            serialized = json.dumps(data, default=str, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"TraceWriter.add({self.pipeline}/{step}): "
                f"data not JSON-serializable: {e}",
            ) from e
        if len(serialized.encode("utf-8")) > _MAX_TRACE_BYTES:
            raise ValueError(
                f"TraceWriter.add({self.pipeline}/{step}): trace_data "
                f"exceeds {_MAX_TRACE_BYTES} bytes — split into multiple rows.",
            )
        row: dict[str, Any] = {
            "audit_id": self.audit_id,
            "report_id": self.report_id,
            "pipeline": self.pipeline,
            "step": step,
            "salon_ref_id": salon_ref_id,
            "trace_data": data,
        }
        if tokens_used is not None:
            if not isinstance(tokens_used, dict):
                raise TypeError(
                    f"TraceWriter.add: tokens_used must be dict, "
                    f"got {type(tokens_used).__name__}",
                )
            row["tokens_used"] = tokens_used
        self._buffer.append(row)

    def buffered(self) -> int:
        return len(self._buffer)

    async def flush(self) -> int:
        """Write all buffered traces to Supabase. Returns the inserted row
        count. Empty buffer → no-op (returns 0). Failures re-raise after
        logging context to bagent_error_log."""
        if not self._buffer:
            return 0
        count = len(self._buffer)
        try:
            # supabase-py is synchronous under the hood but async wrappers
            # exist in some setups. Stick with the standard .execute() call
            # which is sync. The async signature is for API uniformity.
            self.client.table("pipeline_traces").insert(self._buffer).execute()
        except Exception as exc:
            _log_to_bagent_error(
                client=self.client,
                convex_audit_id=self.audit_id or "(unknown)",
                pipeline="pipeline_trace",
                error_type="supabase_error",
                error_message=str(exc),
                stack=traceback.format_exc(),
                payload={
                    "pipeline": self.pipeline,
                    "buffered_count": count,
                    "first_step": self._buffer[0].get("step")
                    if self._buffer
                    else None,
                    "report_id": self.report_id,
                },
            )
            logger.exception(
                "pipeline_trace: flush failed for %s (%d buffered rows)",
                self.pipeline,
                count,
            )
            # NO GRACEFUL FAIL — let the pipeline crash so the operator
            # sees the missing observability.
            raise
        else:
            self._buffer.clear()
            logger.info(
                "pipeline_trace: flushed %d rows from %s",
                count,
                self.pipeline,
            )
            return count
