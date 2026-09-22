"""Stan destylacji profili usług — sekcja autodiagnostyki „destylacja”.

Panel admina ma pokazywać bez czytania kodu: którym sposobem silnik odsiewa
usługi tylko podobne, czy ma czym (klucz + biblioteka), ile profili powstało
w ostatniej dobie, ile to tokenów i złotówek oraz kiedy ostatnio coś przez to
przeszło. Liczby biorą się z jednego zapytania SQL (`fn_service_profile_stats`,
mig 198) — panel nigdy nie przelatuje milionów wierszy.

Każdy błąd => sekcja z polem ``error``; reszta diagnostyki leci normalnie.
"""

from __future__ import annotations

import importlib.util
import logging
from typing import Any

from config import settings

logger = logging.getLogger(__name__)

# Cena i model trzymane tam, gdzie destylacja (services/typesafe_profile).
_RPC = "fn_service_profile_stats"


def _sdk_zainstalowane() -> bool:
    return importlib.util.find_spec("typesafe_sdk") is not None


def _stat(row: dict[str, Any], key: str) -> int:
    try:
        return int(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def zbierz(client: Any | None) -> dict[str, Any]:
    """Sekcja „destylacja” do /api/internal/diag. `client` = klient Supabase albo None."""
    from services.typesafe_profile.destylacja import MODEL, USD_ZA_TOKEN
    from services.typesafe_profile.pytania import WERSJA

    sekcja: dict[str, Any] = {
        "zrodlo": getattr(settings, "taxonomy_veto_source", "glm"),
        "klucz": bool(getattr(settings, "typesafe_api_key", "")),
        "sdk": _sdk_zainstalowane(),
        "model": MODEL,
        "wersja_pytan": WERSJA,
    }
    if client is None:
        sekcja["error"] = "brak połączenia z bazą"
        return sekcja
    try:
        rows = client.rpc(_RPC, {}).execute().data or []
        row = rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else {})
        tokeny = _stat(row, "tokeny_24h")
        sekcja.update({
            "profili_razem": _stat(row, "profili_razem"),
            "profili_24h": _stat(row, "profili_24h"),
            "tokeny_24h": tokeny,
            "usd_24h": round(tokeny * USD_ZA_TOKEN, 4),
            "branz_24h": _stat(row, "branz_24h"),
            "ostatni": row.get("ostatni"),
        })
    except Exception as exc:  # noqa: BLE001 — diagnostyka nie może wywrócić endpointu
        logger.warning("diag destylacji niedostępny (%s): %s", type(exc).__name__, str(exc)[:160])
        sekcja["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return sekcja
