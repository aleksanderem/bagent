"""Nocny detektor zmian cen konkurencji — ZAPIS W BAZIE, bez Wintacta.

2026-09-15 (decyzja Alexa): outreach przez Wintact wyłączony, więc detektor
tylko księguje wykryte zmiany w ``outreach_events``. Nic nie wychodzi poza
naszą bazę; kolumna ``emitted_at`` zostaje pusta, a indeks
``idx_outreach_events_pending`` pozwoli późniejszemu nadawcy wziąć te
zdarzenia, gdy wysyłka wróci — zanim to nastąpi, nic nie jest tracone.

Przepływ:
  1. SELECT z ``price_events_staging`` (mig 182_price_events_staging w repo
     web) — jeden najmocniejszy event per kontakt. Tabelę odświeża raz na noc
     ``fn_refresh_price_events_staging()`` wołana z hosta przez ``docker exec
     psql`` (ops/systemd/booksy-price-events-refresh.*, timer 05:00 UTC), bo
     funkcja trwa ~2,2 min, a PostgREST ma statement_timeout 8 s
     (BEAUTY_AUDIT-mol-m092). Czytamy STRONAMI: PostgREST zwraca najwyżej
     PGRST_DB_MAX_ROWS=1000 wierszy na zapytanie, a staging ma ~36 tys.
  2. Kontakty: tylko te, które nadal istnieją w ``outreach_contacts`` (FK) —
     plus ``is_customer``/``target_product`` do payloadu.
  3. Dedup: ``external_key`` unikatowy (price:contact:competitor:dzień) —
     upsert z ignore_duplicates, więc ta sama zmiana z kolejnych nocy okna
     detekcji nie dubluje się.

Bez limitu na przebieg i bez frequency-capu: to były bezpieczniki wysyłki,
a przy samym zapisie ucinałyby dane (pominięte zdarzenie z tym samym kluczem
nie wróciłoby już nigdy).
"""

from __future__ import annotations

import logging
from typing import Any

from config import settings
from services.sb_client import make_supabase_client

logger = logging.getLogger("bagent.workers.outreach_event_detector")

EVENT_NAME_PRICE = "competitor.price_change"
# Rozmiar strony odczytu = PGRST_DB_MAX_ROWS; wsad zapisu mieści się w 8 s timeoutu.
STAGING_PAGE_SIZE = 1000
CONTACT_CHUNK = 500
UPSERT_BATCH = 500


def _external_key(row: dict[str, Any]) -> str:
    day = str(row.get("detected_ts") or "")[:10]
    return f"price:{row['contact_id']}:{row['competitor_booksy_id']}:{day}"


def _pl_count(n: int, one: str, few: str, many: str) -> str:
    """Polska liczba mnoga: 1 usługa / 3 usługi / 5 usług (12-14 → many)."""
    n = int(n or 0)
    if n == 1:
        return f"{n} {one}"
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return f"{n} {few}"
    return f"{n} {many}"


def _km_label(km: float | None) -> str:
    if km is None:
        return ""
    if km < 1:
        return f"{max(10, int(round(km * 100)) * 10)} m"
    return f"{km:.1f}".replace(".", ",").rstrip(",0").replace(",", ",") + " km" if km % 1 else f"{int(km)} km"


def _zl(v: float | None) -> str:
    if v is None:
        return ""
    return (f"{v:.2f}".rstrip("0").rstrip(".") or "0").replace(".", ",") + " zł"


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": EVENT_NAME_PRICE,
        "competitor_name": row.get("competitor_name"),
        "competitor_km": float(row["competitor_km"]) if row.get("competitor_km") is not None else None,
        "service_name": row.get("top_service"),
        "old_price_zl": round((row.get("old_price_grosze") or 0) / 100, 2),
        "new_price_zl": round((row.get("new_price_grosze") or 0) / 100, 2),
        "pct_change": float(row["pct_change"]) if row.get("pct_change") is not None else None,
        "direction": "spadek" if (row.get("pct_change") or 0) < 0 else "wzrost",
        "changed_services": row.get("changed_services"),
        "area_competitors_with_changes": row.get("area_competitors_with_changes"),
        "detected_at": str(row.get("detected_ts") or ""),
        # Etykiety gotowe do wklejenia w zdanie — odmiana i format po stronie
        # Pythona, nie szablonu (liquid nie zna polskiej fleksji):
        "distance_label": _km_label(
            float(row["competitor_km"]) if row.get("competitor_km") is not None else None),
        "changed_services_label": _pl_count(
            row.get("changed_services") or 0, "usługi", "usług", "usług"),
        "area_salons_label": _pl_count(
            row.get("area_competitors_with_changes") or 0, "salon", "salony", "salonów"),
        "old_price_label": _zl(round((row.get("old_price_grosze") or 0) / 100, 2)),
        "new_price_label": _zl(round((row.get("new_price_grosze") or 0) / 100, 2)),
    }


async def detect_and_emit_price_events(ctx: dict[str, Any]) -> dict[str, int]:
    """Cron entrypoint: uruchamia detekcję i GWARANTUJE ping (ok albo /fail).

    2026-08-23 (BEAUTY_AUDIT-t35q): dodane. Wcześniej ten cron nie miał
    ŻADNEGO pingu — ani sukcesu, ani porażki. Padał 11 nocy z rzędu
    (2026-08-12..08-23) na statement timeout RPC i nie zapalił nic poza linią
    w logu; wykrył to dopiero skaner logów dyżurnego, nie monitoring. Arq nie
    ponawia zwykłego wyjątku, a cron leci raz na dobę (5:40), więc każdy
    stracony przebieg to cały dzień ciszy — musi być głośno. Wzorzec jak
    refresh_service_variants / embed_new_services w workers/taxonomy_refresh.py.

    Bez ustawionego HC_PING_OUTREACH_PRICE_EVENTS ping jest no-opem, więc kod
    działa też zanim check powstanie w Healthchecks.
    """
    from services.healthcheck import ping

    try:
        result = await _detect_and_emit_price_events_impl(ctx)
    except Exception as e:
        logger.exception("detect_and_emit_price_events failed: %s", e)
        await ping("HC_PING_OUTREACH_PRICE_EVENTS", fail=True)
        raise
    # Ping sukcesu jest TYLKO tutaj (implementacja nie pinguje nigdzie), więc
    # spokojna noc bez zdarzeń — wczesny return z pustego RPC — też melduje się
    # jako udany przebieg, zamiast wyglądać na awarię po upływie grace.
    await ping("HC_PING_OUTREACH_PRICE_EVENTS")
    return result


def _read_staging(sb) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = (
            sb.table("price_events_staging")
            .select("*")
            .order("contact_id")
            .range(offset, offset + STAGING_PAGE_SIZE - 1)
            .execute()
            .data
            or []
        )
        rows.extend(page)
        if len(page) < STAGING_PAGE_SIZE:
            return rows
        offset += STAGING_PAGE_SIZE


def _load_contacts(sb, contact_ids: list[int]) -> dict[int, dict[str, Any]]:
    contacts: dict[int, dict[str, Any]] = {}
    for i in range(0, len(contact_ids), CONTACT_CHUNK):
        res = (
            sb.table("outreach_contacts")
            .select("id, is_customer, owned_products")
            .in_("id", contact_ids[i : i + CONTACT_CHUNK])
            .execute()
        )
        contacts.update({c["id"]: c for c in res.data or []})
    return contacts


def _event_record(row: dict[str, Any], contact: dict[str, Any]) -> dict[str, Any]:
    payload = {
        **_payload(row),
        # docelowy produkt wg gałęzi z canvasu (klient → dosprzedaż)
        "target_product": (
            "monitoring" if contact.get("is_customer")
            and "monitoring" not in (contact.get("owned_products") or [])
            else "audit"
        ),
        "is_customer": bool(contact.get("is_customer")),
    }
    return {
        "contact_id": row["contact_id"],
        "event_type": EVENT_NAME_PRICE,
        "external_key": _external_key(row),
        "payload": payload,
    }


async def _detect_and_emit_price_events_impl(ctx: dict[str, Any]) -> dict[str, int]:
    """Staging → outreach_events (tylko zapis, bez Wintacta)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    rows = _read_staging(sb)
    if not rows:
        return {"detected": 0, "recorded": 0, "duplicates": 0, "skipped_no_contact": 0}

    contacts = _load_contacts(sb, sorted({r["contact_id"] for r in rows}))
    records = [_event_record(r, contacts[r["contact_id"]]) for r in rows if r["contact_id"] in contacts]
    skipped = len(rows) - len(records)

    recorded = 0
    for i in range(0, len(records), UPSERT_BATCH):
        ins = (
            sb.table("outreach_events")
            .upsert(records[i : i + UPSERT_BATCH], on_conflict="external_key", ignore_duplicates=True)
            .execute()
        )
        recorded += len(ins.data or [])

    stats = {
        "detected": len(rows),
        "recorded": recorded,
        "duplicates": len(records) - recorded,
        "skipped_no_contact": skipped,
    }
    logger.info("price events (tylko zapis): %s", stats)
    return stats


ALL_OUTREACH_EVENT_DETECTOR_TASKS = [detect_and_emit_price_events]
