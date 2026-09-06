"""Nadpisania kluczy z panelu admina („Klucze i stałe") dla bagenta.

Panel trzyma nadpisania w Convexie (tabela systemSettings). bagent pobiera je
z GET {CONVEX_URL}/api/settings?scope=bagent (uwierzytelnienie tym samym
x-api-key, którym Convex woła bagenta) i podstawia do obiektu `settings`
w PAMIĘCI procesu — bez edycji .env i bez restartu. Każdy proces (API, worker,
report-worker) synchronizuje się osobno: przy starcie i co 5 minut (cron arq).

Klienci (MiniMaxClient, OpenAI, Brave) są budowani per zadanie z `settings.*`,
więc nowa wartość działa od następnego zadania.

Nazwa klucza w panelu = nazwa pola Settings pisana WIELKIMI literami
(MINIMAX_API_KEY → settings.minimax_api_key). Klucz, którego Settings nie zna,
jest ignorowany z ostrzeżeniem. Pusta lista z Convexa = zdejmij nadpisania
i wróć do wartości z .env (trzymamy kopię oryginałów).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

#: Pola, których panel NIE może nadpisać — bez nich sync nie ma jak działać
#: albo zmiana wymaga restartu procesu.
PROTECTED_FIELDS = frozenset({"api_key", "convex_url", "convex_deploy_key", "port"})

_originals: dict[str, Any] = {}
_applied: set[str] = set()


def field_for_key(key: str) -> str:
    return key.strip().lower()


def apply_overrides(settings_obj: Any, overrides: dict[str, str]) -> dict[str, list[str]]:
    """Podstaw nadpisania do obiektu ustawień. Zwraca {applied, ignored, restored}."""
    applied: list[str] = []
    ignored: list[str] = []
    restored: list[str] = []
    wanted: dict[str, str] = {}
    for key, value in overrides.items():
        field = field_for_key(key)
        if field in PROTECTED_FIELDS or not hasattr(settings_obj, field):
            ignored.append(key)
            continue
        wanted[field] = value

    # Nadpisania, które zniknęły z panelu → wracamy do wartości z .env.
    for field in sorted(_applied - set(wanted)):
        if field in _originals:
            setattr(settings_obj, field, _originals[field])
            restored.append(field)
        _applied.discard(field)

    for field, value in wanted.items():
        if field not in _originals:
            _originals[field] = getattr(settings_obj, field)
        current = getattr(settings_obj, field)
        coerced = _coerce(current, value)
        if current != coerced:
            setattr(settings_obj, field, coerced)
            applied.append(field)
        _applied.add(field)
    return {"applied": applied, "ignored": ignored, "restored": restored}


def _coerce(current: Any, value: str) -> Any:
    """Wartość z panelu to string — dopasuj do typu pola (bool/int/float)."""
    if isinstance(current, bool):
        return value.strip().lower() in ("1", "true", "tak", "yes", "on")
    if isinstance(current, int):
        try:
            return int(value)
        except ValueError:
            return current
    if isinstance(current, float):
        try:
            return float(value)
        except ValueError:
            return current
    return value


async def fetch_overrides(
    http: httpx.AsyncClient, convex_url: str, api_key: str, scope: str = "bagent"
) -> dict[str, str]:
    """GET /api/settings?scope=… → {KEY: value}. Wyjątek przy błędzie HTTP."""
    resp = await http.get(
        f"{convex_url.rstrip('/')}/api/settings",
        params={"scope": scope},
        headers={"x-api-key": api_key},
        timeout=15.0,
    )
    resp.raise_for_status()
    data = resp.json()
    settings_map = data.get("settings") or {}
    return {str(k): str(v) for k, v in settings_map.items()}


async def sync_settings(settings_obj: Any) -> dict[str, Any]:
    """Jedno pełne odświeżenie: pobierz i podstaw. Nigdy nie rzuca — sync jest
    dodatkiem; brak Convexa nie może położyć workera."""
    convex_url = getattr(settings_obj, "convex_url", "") or ""
    api_key = getattr(settings_obj, "api_key", "") or ""
    if not convex_url or not api_key:
        return {"skipped": "brak convex_url/api_key"}
    try:
        async with httpx.AsyncClient() as http:
            overrides = await fetch_overrides(http, convex_url, api_key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[settings-sync] pobranie nadpisań padło: %s", exc)
        return {"error": str(exc)[:200]}
    result = apply_overrides(settings_obj, overrides)
    if result["applied"] or result["restored"] or result["ignored"]:
        logger.info("[settings-sync] %s", result)
    return {"count": len(overrides), **result}


async def settings_sync_cron(ctx: dict[str, Any]) -> dict[str, Any]:
    """Cron arq co 5 minut (workers/main.py)."""
    from config import settings

    return await sync_settings(settings)
