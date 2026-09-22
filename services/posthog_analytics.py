"""Koszty modeli AI z bagenta do PostHoga.

Po co: rachunek za modele powstaje w większości tutaj — w agencie audytu,
w klasyfikacji usług i w nocnych zadaniach taksonomii. Bez tego w PostHogu
widać wyłącznie wywołania z Convexa, czyli wierzchołek góry lodowej.

Wysyłamy WYŁĄCZNIE metadane: model, liczbę tokenów, czas i ewentualny błąd.
Treści promptów nie ruszamy — siedzą w nich cenniki klientek.

Wyłącznik: bez `POSTHOG_PROJECT_KEY` w środowisku zdarzenie nie leci, więc
lokalnie i w testach nie ma ruchu do PostHoga. Podsumowanie dobowe w naszej
bazie (services/zuzycie_ai.py) jest osobne i ma własny wyłącznik — brak
konfiguracji Supabase.

UWAGA na wolumen: wołać to TYLKO przy generowaniu tekstu (jedno wywołanie =
jedno zdarzenie). Nigdy w pętli embeddingów — tam idą dziesiątki tysięcy
wywołań na noc i zdarzenia zjadłyby limit planu.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

import httpx

from services.zuzycie_ai import dopisz as dopisz_zuzycie

logger = logging.getLogger(__name__)

POSTHOG_HOST = "https://eu.i.posthog.com"
_TIMEOUT = httpx.Timeout(5.0, connect=2.0)


def _api_key() -> str | None:
    key = os.getenv("POSTHOG_PROJECT_KEY", "").strip()
    return key or None


async def capture_ai_generation(
    *,
    provider: str,
    model: str,
    span_name: str,
    started_at: float,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    error: BaseException | None = None,
    distinct_id: str = "bagent",
    trace_id: str | None = None,
) -> None:
    """Zgłasza jedno wywołanie modelu. Nigdy nie podnosi wyjątku."""
    # Podsumowanie dobowe w NASZEJ bazie — panel admina pokazuje z tego koszt
    # i tokeny przy każdym mechanizmie, bez wychodzenia do PostHoga.
    await dopisz_zuzycie(
        provider=provider, model=model, span_name=span_name,
        input_tokens=input_tokens, output_tokens=output_tokens, error=error is not None,
    )
    key = _api_key()
    if not key:
        return
    payload = {
        "api_key": key,
        "event": "$ai_generation",
        "distinct_id": distinct_id,
        "properties": {
            "$ai_trace_id": trace_id or f"{provider}-{int(started_at * 1000)}",
            "$ai_span_name": span_name,
            "$ai_provider": provider,
            "$ai_model": model,
            "$ai_input_tokens": input_tokens or 0,
            "$ai_output_tokens": output_tokens or 0,
            "$ai_latency": round(time.monotonic() - started_at, 3),
            "$ai_is_error": error is not None,
            **({"$ai_error": str(error)[:300]} if error is not None else {}),
            "$lib": "booksyaudit-bagent",
            "service": os.getenv("BAGENT_ROLE", "worker"),
        },
        "timestamp": datetime.now(UTC).isoformat(),
    }
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            await client.post(f"{POSTHOG_HOST}/i/v0/e/", json=payload)
    except Exception as exc:  # noqa: BLE001 — analityka nie wywraca pipeline'u
        logger.debug("posthog $ai_generation nieudany: %s", exc)
