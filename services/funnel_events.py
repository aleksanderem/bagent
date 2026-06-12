"""FUNNEL_AUDIT R6 — jednolity strumień zdarzeń lejka.

Jedna tabela `funnel_events` w Supabase zbiera przejścia między modułami
trójkąta (zakupy, kliknięcia/odpowiedzi outreach, atrybucje kampanii), żeby
lejek dało się policzyć end-to-end w jednym miejscu zamiast sklejać
outreach_messages + ad_booking_attributions + zakupy z Convex per analizę.

Zapis jest best-effort i idempotentny po dedupe_key — awaria zapisu NIGDY
nie blokuje ścieżki biznesowej, która go emituje.

Schemat: deploy/sql/2026-06-12_funnel_events.sql (jednorazowa aplikacja).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def record_funnel_event(
    sb: Any,
    *,
    event_type: str,
    source: str,
    dedupe_key: str,
    user_id: str | None = None,
    salon_ref_id: int | None = None,
    contact_id: int | None = None,
    audit_id: str | None = None,
    campaign_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Idempotentny zapis zdarzenia (upsert po dedupe_key, duplikaty ignorowane).

    Zwraca True gdy zapis przeszedł (lub zdarzenie już istniało), False przy
    awarii — wyłącznie do logowania, caller nie powinien na tym warunkować
    logiki biznesowej.
    """
    try:
        sb.table("funnel_events").upsert(
            {
                "event_type": event_type,
                "source": source,
                "dedupe_key": dedupe_key,
                "user_id": user_id,
                "salon_ref_id": salon_ref_id,
                "contact_id": contact_id,
                "audit_id": audit_id,
                "campaign_id": campaign_id,
                "metadata": metadata or {},
            },
            on_conflict="dedupe_key",
            ignore_duplicates=True,
        ).execute()
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "funnel_events write failed (type=%s key=%s): %s", event_type, dedupe_key, e
        )
        return False
