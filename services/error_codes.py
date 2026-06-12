"""Stable error codes for pipeline failures (FINDINGS P2).

Before this module, failure webhooks carried `str(e)[:200]` — the frontend
could not route errors (user-facing retry hint vs ops alert) and ops could
not aggregate. Codes are a `[CODE]` prefix on the existing message, so the
Convex contract is unchanged: old consumers still get a string, new ones
can parse the prefix.

Classification is heuristic by exception type + message keywords; default
INTERNAL. Keep the code list short — every code should map to a distinct
user-facing reaction.
"""

from __future__ import annotations

# Reaction map (frontend):
#   INVALID_DATA   → "dane wejściowe są niepełne" — nie ponawiaj bez zmiany
#   AI_TIMEOUT     → "AI nie odpowiedziało na czas" — retry ma sens
#   AI_ERROR       → "błąd generowania AI" — retry ma sens
#   DB_UNAVAILABLE → "błąd bazy danych" — alert ops, retry później
#   CANCELLED      → anulowane przez użytkownika
#   INTERNAL       → nieznany błąd — alert ops
INVALID_DATA = "INVALID_DATA"
AI_TIMEOUT = "AI_TIMEOUT"
AI_ERROR = "AI_ERROR"
DB_UNAVAILABLE = "DB_UNAVAILABLE"
CANCELLED = "CANCELLED"
INTERNAL = "INTERNAL"

_DB_KEYWORDS = ("supabase", "postgres", "postgrest", "connection refused", "redis")
_AI_KEYWORDS = ("minimax", "openai", "anthropic", "llm", "json", "parse")
_TIMEOUT_KEYWORDS = ("timeout", "timed out", "deadline")


def classify_error(e: BaseException) -> str:
    """Map an exception to a stable error code."""
    name = type(e).__name__.lower()
    msg = str(e).lower()

    if any(k in msg or k in name for k in _TIMEOUT_KEYWORDS):
        # Timeout w wywołaniu AI vs DB rozróżniamy po treści; domyślnie AI,
        # bo to one mają 120s+ budżety i to one realnie timeoutują.
        return DB_UNAVAILABLE if any(k in msg for k in _DB_KEYWORDS) else AI_TIMEOUT
    if any(k in msg for k in _DB_KEYWORDS):
        return DB_UNAVAILABLE
    if isinstance(e, (ValueError, KeyError, TypeError)) and "scraped" in msg:
        return INVALID_DATA
    if "validation" in name or isinstance(e, (KeyError,)):
        return INVALID_DATA
    if any(k in msg for k in _AI_KEYWORDS):
        return AI_ERROR
    return INTERNAL


def coded_message(e: BaseException, limit: int = 300) -> str:
    """`[CODE] message` — drop-in replacement for `str(e)[:200]`."""
    return f"[{classify_error(e)}] {str(e)[:limit]}"
