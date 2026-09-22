"""Zapis zużycia modeli do podsumowania dobowego (tabela ai_zuzycie_dzienne).

Panel admina ma pokazywać koszt przy każdym mechanizmie, więc liczby muszą być
w naszej bazie, nie tylko w PostHogu. Zapis wisi na tym samym wywołaniu, co
pomiar do PostHoga (services/posthog_analytics.py), czyli obejmuje wszystko,
co woła model: audyt, syntezę, destylację, mostki i zapasowe ścieżki.

Trzy zasady:
* nigdy nie podnosi wyjątku — koszt jest informacją, nie warunkiem działania,
* nie blokuje pętli zdarzeń (klient Supabase jest synchroniczny → osobny wątek),
* przy braku ceny w cenniku zapisuje sam licznik tokenów, a kwotę zostawia pustą.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from config import settings
from services.ceny_modeli import koszt_usd

logger = logging.getLogger(__name__)

RPC = "fn_ai_zuzycie_dopisz"
_klient: Any | None = None
_klient_gotowy = False


def _client() -> Any | None:
    """Klient Supabase trzymany jeden na proces; brak konfiguracji = brak zapisu."""
    global _klient, _klient_gotowy
    if _klient_gotowy:
        return _klient
    _klient_gotowy = True
    if not settings.supabase_url or not settings.supabase_service_key:
        return None
    try:
        from services.sb_client import make_supabase_client

        _klient = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    except Exception as exc:  # noqa: BLE001 — brak zapisu kosztu nie może psuć pipeline'u
        logger.debug("zużycie AI: klient bazy niedostępny: %s", exc)
        _klient = None
    return _klient


def _zapisz(payload: dict[str, Any]) -> None:
    cli = _client()
    if cli is None:
        return
    try:
        cli.rpc(RPC, payload).execute()
    except Exception as exc:  # noqa: BLE001
        logger.debug("zużycie AI: zapis nieudany (%s): %s", type(exc).__name__, str(exc)[:120])


async def dopisz(
    *,
    provider: str,
    model: str,
    span_name: str,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    error: bool = False,
) -> None:
    """Dopisz jedno wywołanie modelu do podsumowania dobowego."""
    usd, szacunek = koszt_usd(provider, model, input_tokens, output_tokens)
    payload = {
        "p_dostawca": provider or "?",
        "p_model": model or "?",
        "p_mechanizm": span_name or "?",
        "p_tokeny_wej": int(input_tokens or 0),
        "p_tokeny_wyj": int(output_tokens or 0),
        "p_usd": usd,
        "p_blad": bool(error),
        "p_usd_szacunek": bool(szacunek),
    }
    try:
        await asyncio.to_thread(_zapisz, payload)
    except Exception as exc:  # noqa: BLE001 — w tym miejscu wolno tylko zalogować
        logger.debug("zużycie AI: pominięte (%s)", type(exc).__name__)
