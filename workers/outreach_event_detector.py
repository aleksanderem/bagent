"""Nocny detektor zdarzeń outreach → Wintact custom events (F1 planu
docs/outreach-assets/EVENT_DRIVEN_PLAN.md w repo BEAUTY_AUDIT).

Przepływ (pilot: zmiany cen konkurencji):
  1. RPC ``fn_detect_competitor_price_events`` (mig 163) — jeden najmocniejszy
     event per kontakt (staged temp tables po stronie Postgresa, ~5 min).
  2. Frequency-cap: kontakt z ``outreach_events.emitted_at`` młodszym niż
     EVENT_FREQUENCY_CAP_DAYS dostaje wpis ``suppressed_reason='frequency_cap'``
     zamiast emisji. Księga w Supabase jest źródłem prawdy dla capu — Wintact
     deduplikuje po external_id, ale niczego nie ogranicza w czasie.
  3. Dedup: ``external_key`` unikatowy (contact:price:competitor:data) — ten sam
     klucz idzie do Wintacta jako ``external_id`` w customEvents.upsert.
  4. Personalizacja: payload zdarzenia ląduje w ``custom_json_1`` KONTAKTU przed
     emisją eventu — automation email czyta {{ contact.custom_json_1.* }},
     bo silnik NIE przekazuje properties eventu do szablonu
     (notifuse: buildAutomationTemplateData buduje dane wyłącznie z kontaktu).
  5. customEvents.upsert odpala automatyzację (trigger: custom event) — dopóki
     automatyzacja nie istnieje/nieaktywna, event tylko zapisuje się w timeline.

Bez aktywnej automatyzacji w Wintakcie ten worker NICZEGO nie wysyła mailem.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from config import settings
from services.sb_client import make_supabase_client
from services.wintact import WintactClient, WintactError

logger = logging.getLogger("bagent.workers.outreach_event_detector")

EVENT_NAME_PRICE = "competitor.price_change"
EVENT_FREQUENCY_CAP_DAYS = 14
# Bezpiecznik pilota: górny limit emisji na jeden przebieg. Podnosimy świadomie
# po obejrzeniu pierwszych przebiegów, nie domyślnie.
MAX_EMISSIONS_PER_RUN = 200


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


async def _detect_and_emit_price_events_impl(ctx: dict[str, Any]) -> dict[str, int]:
    """Cron nightly: detekcja → księga → (cap/dedup) → Wintact."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    rows = sb.rpc("fn_detect_competitor_price_events", {}).execute().data or []
    if not rows:
        return {"detected": 0, "emitted": 0, "suppressed": 0, "errors": 0}

    contact_ids = sorted({r["contact_id"] for r in rows})
    contacts: dict[int, dict[str, Any]] = {}
    for i in range(0, len(contact_ids), 200):
        chunk = contact_ids[i : i + 200]
        res = (
            sb.table("outreach_contacts")
            .select("id, email, is_customer, owned_products")
            .in_("id", chunk)
            .execute()
        )
        for c in res.data or []:
            contacts[c["id"]] = c

    # Frequency-cap: kontakty z emisją w oknie capu
    from datetime import timedelta
    cap_cutoff = (datetime.now(timezone.utc) - timedelta(days=EVENT_FREQUENCY_CAP_DAYS)).isoformat()
    capped: set[int] = set()
    for i in range(0, len(contact_ids), 200):
        chunk = contact_ids[i : i + 200]
        res = (
            sb.table("outreach_events")
            .select("contact_id")
            .in_("contact_id", chunk)
            .gte("emitted_at", cap_cutoff)
            .execute()
        )
        capped.update(r["contact_id"] for r in res.data or [])

    detected = len(rows)
    emitted = suppressed = errors = 0

    async with WintactClient() as wc:
        for row in rows:
            key = _external_key(row)
            contact = contacts.get(row["contact_id"])
            if not contact or not contact.get("email"):
                continue

            suppress: str | None = None
            if row["contact_id"] in capped:
                suppress = "frequency_cap"
            elif emitted >= MAX_EMISSIONS_PER_RUN:
                suppress = "run_limit"

            payload = _payload(row)
            # docelowy produkt wg gałęzi z canvasu (klient → dosprzedaż)
            payload["target_product"] = (
                "monitoring" if contact.get("is_customer")
                and "monitoring" not in (contact.get("owned_products") or [])
                else "audit"
            )
            payload["is_customer"] = bool(contact.get("is_customer"))

            ins = (
                sb.table("outreach_events")
                .upsert(
                    {
                        "contact_id": row["contact_id"],
                        "event_type": EVENT_NAME_PRICE,
                        "external_key": key,
                        "payload": payload,
                        "suppressed_reason": suppress,
                    },
                    on_conflict="external_key",
                    ignore_duplicates=True,
                )
                .execute()
            )
            if not ins.data:
                continue  # duplikat z wcześniejszego przebiegu — nie emituj ponownie
            if suppress:
                suppressed += 1
                continue

            try:
                await wc.upsert_contact(
                    contact["email"],
                    attributes={
                        "custom_json_1": payload,
                        "custom_string_3": payload["target_product"],
                    },
                )
                await wc.upsert_custom_event(
                    email=contact["email"],
                    event_name=EVENT_NAME_PRICE,
                    external_id=key,
                    properties=payload,
                )
                sb.table("outreach_events").update(
                    {"emitted_at": datetime.now(timezone.utc).isoformat()}
                ).eq("external_key", key).execute()
                capped.add(row["contact_id"])  # cap obowiązuje też w ramach przebiegu
                emitted += 1
            except WintactError as exc:
                errors += 1
                logger.error("Emisja %s nieudana: %s", key, exc)

    logger.info(
        "price events: detected=%d emitted=%d suppressed=%d errors=%d",
        detected, emitted, suppressed, errors,
    )
    return {"detected": detected, "emitted": emitted, "suppressed": suppressed, "errors": errors}


ALL_OUTREACH_EVENT_DETECTOR_TASKS = [detect_and_emit_price_events]
