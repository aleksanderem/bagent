"""arq tasks: ship approved outreach assets to wintact.io.

Two cron-driven tasks:

1. ``deploy_approved_templates`` (every 2 min)
   Reads outreach_template_variants WHERE approval_status='approved'
   AND wintact_template_id IS NULL. For each row, calls
   wintact /templates.create with subject + body, then records the
   returned wintact_template_id and flips approval_status to
   'deployed'. Idempotent (the wintact_template_id IS NULL guard).

2. ``deploy_approved_segments`` (every 5 min)
   Reads outreach_audience_segments WHERE approval_status='approved'
   AND wintact_list_id IS NULL. For each row, creates a wintact list
   (private), records wintact_list_id, flips status to 'deployed'.
   Population (which contacts go in the list) is handled by the
   orchestrator on the day of send — segments are passive list
   buckets here.

3. ``activate_approved_sequences`` (every 5 min)
   Reads outreach_sequences WHERE approval_status='approved' AND
   active=false. Flips active=true + records activated_at. The
   orchestrator picks up active sequences and starts enrolling
   contacts whose state matches entry_state.

NIC nie jest pchane do wintact bez approval_status='approved' — that
gate lives in Convex/admin UI; the loader writes 'pending_review' and
operator approves via /admin/outreach/approval-queue.

Idempotencja (BEAUTY_AUDIT-tu2m / -mp8u): wintact nie ma upsertu, a nasz
zapis stanu następował dopiero po udanym create. Gdy obiekt już tam był
(400 "Template id already exists", 500 przy duplikacie listy), wyjątek
leciał w górę, stan zostawał niezapisany i cron wracał po ten sam wiersz
co 2-5 minut — 70 błędów/h w logu. Teraz konflikt POTWIERDZONY faktem
(obiekt widoczny na /templates.list albo /lists.list) to wdrożenie:
``upsert_template``/``upsert_list`` zwracają mode='existing', a
``_mark_deployed`` zapisuje ten sam stan co przy sukcesie.

Błąd bez takiego potwierdzenia nadal leci jako WintactError i dzieli się
na dwa światy:

  - awaria usługi (5xx, timeout, brak kodu HTTP, zły klucz API) — wiersz
    zostaje nietknięty w 'approved', leci jednolinijkowy ERROR, deployer
    wróci po niego w kolejnym cyklu. Maszyna NIE cofa zatwierdzenia
    człowieka tylko dlatego, że Wintact miał sześć minut awarii;
  - trwały błąd danych wiersza (4xx z walidacji, wysypka renderera) —
    licznik prób w approval_notes rośnie i po ``MAX_DEPLOY_ATTEMPTS``
    wiersz wraca do kolejki zatwierdzeń, żeby człowiek go poprawił.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from config import settings
from services.sb_client import make_supabase_client
from services.email_renderer import render_email_html
from services.wintact import WintactClient, WintactError, short_error

logger = logging.getLogger("bagent.workers.outreach_deployer")

TEMPLATES_TABLE = "outreach_template_variants"
SEGMENTS_TABLE = "outreach_audience_segments"

# Po tylu nieudanych próbach wiersz wraca do kolejki zatwierdzeń. Dotyczy
# WYŁĄCZNIE błędów trwałych po stronie danych wiersza — patrz
# ``_is_transient_failure``.
MAX_DEPLOY_ATTEMPTS = 3

# approval_notes to pole, w którym człowiek pisze instrukcje dla siebie i
# dla stratega. Nasz ślad po nieudanym wdrożeniu dopisujemy POD jego
# notatką, za tym separatorem — dzięki temu treść człowieka nigdy nie jest
# parsowana ani przepisywana, tylko przenoszona dosłownie. Marker ma być
# ZAWSZE jedną linią (patrz ``short_error``), inaczej po trzech próbach
# operator dostaje kilkanaście linii JSON-a nad swoim tekstem.
_DEPLOY_LOG_SEPARATOR = "--- bagent deploy ---"
_ATTEMPT_MARKER_RE = re.compile(r"^\[deploy-failed (\d+)/\d+\]")

# Kody, przy których wina leży po stronie Wintacta albo sieci, a nie
# danych wiersza. Auth (401/403) i timeout (408) też tu należą: zły klucz
# API to awaria konfiguracji, a nie powód, żeby maszyna cofała
# zatwierdzenia wszystkich szablonów po sześciu minutach.
_TRANSIENT_STATUS_CODES = frozenset({401, 403, 408, 429})


# ---------------------------------------------------------------------------
# Stan wdrożenia — jeden zapis dla sukcesu i dla konfliktu
# ---------------------------------------------------------------------------

def _split_notes(approval_notes: str | None) -> tuple[str, str]:
    """Rozdziela approval_notes na (notatka człowieka, blok maszyny).

    Nie zgaduje po pierwszej linii — szuka naszego separatora, a blok za
    nim uznaje za swój tylko wtedy, gdy zaczyna się markerem prób. Dzięki
    temu notatka człowieka przetrwa w całości, choćby sama zaczynała się
    od "[deploy-failed 2/3]" albo miała dziesięć linii."""
    text = approval_notes or ""
    head, separator, tail = text.rpartition(_DEPLOY_LOG_SEPARATOR)
    if not separator:
        return text.strip(), ""
    machine = tail.lstrip("\n")
    if not _ATTEMPT_MARKER_RE.match(machine):
        return text.strip(), ""
    return head.strip(), machine.strip()


def _failed_attempts(approval_notes: str | None) -> int:
    """Ile razy deploy tego wiersza już padł trwale (0 = jeszcze nigdy)."""
    _, machine = _split_notes(approval_notes)
    match = _ATTEMPT_MARKER_RE.match(machine)
    return int(match.group(1)) if match else 0


def _notes_without_marker(approval_notes: str | None) -> str:
    """Sama treść człowieka — bez naszego bloku, niezależnie od tego, ile
    linii ten blok zajmował."""
    return _split_notes(approval_notes)[0]


def _notes_with_attempt(approval_notes: str | None, attempt: int, error: str) -> str:
    human = _notes_without_marker(approval_notes)
    marker = f"[deploy-failed {attempt}/{MAX_DEPLOY_ATTEMPTS}] {error}"
    block = f"{_DEPLOY_LOG_SEPARATOR}\n{marker}"
    return f"{human}\n{block}" if human else block


def _is_transient_failure(exc: BaseException) -> bool:
    """Czy to awaria usługi/sieci, a nie trwały błąd danych wiersza?

    Awaria Wintacta NIE MOŻE cofać decyzji człowieka: przy cronie co 2
    minuty sześć minut niedostępności wystarczyłoby, żeby wszystkie
    zatwierdzone szablony same wypadły z 'approved' do
    'changes_requested', a w polu instrukcji dla stratega wylądował
    traceback. Przy awarii nie ruszamy wiersza — wróci w kolejnym cyklu.

    Trwałe są tylko błędy przypisywalne temu wierszowi: 4xx spoza listy
    infrastrukturalnej (np. 400 z walidacji payloadu) oraz wszystko, co
    nie jest ``WintactError`` (np. wysypka renderera na body_md)."""
    if not isinstance(exc, WintactError):
        return False
    status = exc.status_code
    if status is None:
        # ``_request`` rzuca bez kodu po trzech nieudanych podejściach do
        # 429/5xx i po błędach sieci — czyli dokładnie przy awarii.
        return True
    return status >= 500 or status in _TRANSIENT_STATUS_CODES


def _mark_deployed(
    sb: Any, table: str, row: dict[str, Any], extra: dict[str, Any],
) -> None:
    """JEDYNY zapis stanu "wdrożone" — używa go tak samo świeże utworzenie
    w wintakcie, jak i konflikt (obiekt tam już był). Bez tego zapisu
    deployer wracałby po ten sam wiersz przy każdym przebiegu crona."""
    patch: dict[str, Any] = {
        **extra,
        "approval_status": "deployed",
        "deployed_at": datetime.now(timezone.utc).isoformat(),
    }
    cleaned = _notes_without_marker(row.get("approval_notes"))
    if cleaned != (row.get("approval_notes") or ""):
        patch["approval_notes"] = cleaned or None
    sb.table(table).update(patch).eq("id", row["id"]).execute()


def _record_deploy_failure(
    sb: Any, table: str, row: dict[str, Any], exc: BaseException,
) -> tuple[int, bool]:
    """Podbija licznik prób; po MAX_DEPLOY_ATTEMPTS odstawia wiersz do
    kolejki zatwierdzeń ('changes_requested'), przez co wypada z zapytania
    deployera. Zwraca (numer_próby, czy_odstawiony).

    Wywoływane WYŁĄCZNIE dla błędów trwałych — awaria Wintacta nie tyka
    tego wiersza (``_is_transient_failure``)."""
    attempt = _failed_attempts(row.get("approval_notes")) + 1
    parked = attempt >= MAX_DEPLOY_ATTEMPTS
    patch: dict[str, Any] = {
        "approval_notes": _notes_with_attempt(
            row.get("approval_notes"), attempt,
            f"{type(exc).__name__}: {short_error(exc)}",
        ),
    }
    if parked:
        patch["approval_status"] = "changes_requested"
    try:
        sb.table(table).update(patch).eq("id", row["id"]).execute()
    except Exception as write_exc:  # noqa: BLE001 — granica z Supabase
        logger.warning(
            "Nie udało się zapisać licznika prób dla %s/%s: %s",
            table, row.get("id"), write_exc,
        )
        return attempt, False
    return attempt, parked


# ---------------------------------------------------------------------------
# Task 1: templates → wintact
# ---------------------------------------------------------------------------

async def deploy_approved_templates(ctx: dict[str, Any]) -> dict[str, int]:
    """Push every approved-but-not-yet-deployed template to wintact."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table(TEMPLATES_TABLE)
        .select("id, funnel, step_key, vertical, variant_label, "
                "subject, body_md, body_html, preview_text, approval_notes")
        .eq("approval_status", "approved")
        .is_("wintact_template_id", "null")
        .limit(50)
        .execute()
    )
    if not rows.data:
        return {"deployed": 0, "reconciled": 0, "errors": 0}

    deployed = 0
    reconciled = 0
    errors = 0
    async with WintactClient() as wc:
        for row in rows.data:
            try:
                # body_html jeśli pre-rendered; markdown renderujemy do HTML
                # (Notifuse chce compiled_preview jako HTML, nie markdown).
                body_html = row.get("body_html")
                if not body_html:
                    body_html = render_email_html(
                        body_md=row.get("body_md") or "",
                        subject=row["subject"],
                        preview_text=row.get("preview_text"),
                    )
                result = await wc.upsert_template(
                    template_id=row["step_key"],
                    name=row["step_key"],
                    subject=row["subject"],
                    html=body_html,
                    preview_text=row.get("preview_text"),
                )
                wintact_id = result["id"]
                if not wintact_id:
                    raise ValueError(
                        f"wintact /templates.create returned no id: {result['raw']}"
                    )
                _mark_deployed(
                    sb, TEMPLATES_TABLE, row,
                    {"wintact_template_id": wintact_id},
                )
                deployed += 1
                if result["mode"] == "created":
                    logger.info(
                        "Deployed template %s → wintact id %s",
                        row["step_key"], wintact_id,
                    )
                else:
                    reconciled += 1
                    logger.warning(
                        "Template %s był już w wintakcie (%s) — zapisany jako "
                        "wdrożony z id %s, bez ponownego tworzenia",
                        row["step_key"], result["mode"], wintact_id,
                    )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if _is_transient_failure(exc):
                    logger.error(
                        "Wintact niedostępny przy szablonie %s — zatwierdzenie "
                        "zostaje, spróbuję w kolejnym cyklu: %s",
                        row.get("id"), short_error(exc),
                    )
                    continue
                attempt, parked = _record_deploy_failure(
                    sb, TEMPLATES_TABLE, row, exc,
                )
                logger.error(
                    "Failed to deploy template %s (próba %d/%d%s): %s",
                    row.get("id"), attempt, MAX_DEPLOY_ATTEMPTS,
                    ", odstawiony do kolejki zatwierdzeń" if parked else "",
                    exc, exc_info=True,
                )

    return {"deployed": deployed, "reconciled": reconciled, "errors": errors}


# ---------------------------------------------------------------------------
# Task 2: segments → wintact lists
# ---------------------------------------------------------------------------

async def deploy_approved_segments(ctx: dict[str, Any]) -> dict[str, int]:
    """Create wintact lists for approved segments. Population deferred
    to orchestrator (lists are buckets, not snapshots)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table(SEGMENTS_TABLE)
        .select("id, funnel, name, description, cohort, approval_notes")
        .eq("approval_status", "approved")
        .is_("wintact_list_id", "null")
        .limit(20)
        .execute()
    )
    if not rows.data:
        return {"deployed": 0, "reconciled": 0, "errors": 0}

    deployed = 0
    reconciled = 0
    errors = 0
    async with WintactClient() as wc:
        for row in rows.data:
            try:
                list_name = f"{row['funnel']}__{row['cohort']}__{row['name']}"
                result = await wc.upsert_list(
                    name=list_name,
                    description=row.get("description") or f"Auto-deployed segment {row['name']}",
                    is_public=False,
                )
                wintact_list_id = result["id"]
                if not wintact_list_id:
                    raise ValueError(
                        f"wintact /lists.create returned no id: {result['raw']}"
                    )
                _mark_deployed(
                    sb, SEGMENTS_TABLE, row,
                    {"wintact_list_id": wintact_list_id},
                )
                deployed += 1
                if result["mode"] == "created":
                    logger.info(
                        "Deployed segment %s → wintact list %s",
                        list_name, wintact_list_id,
                    )
                else:
                    reconciled += 1
                    logger.warning(
                        "Segment %s miał już listę w wintakcie — zapisany jako "
                        "wdrożony z id %s, bez ponownego tworzenia",
                        list_name, wintact_list_id,
                    )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if _is_transient_failure(exc):
                    logger.error(
                        "Wintact niedostępny przy segmencie %s — zatwierdzenie "
                        "zostaje, spróbuję w kolejnym cyklu: %s",
                        row.get("id"), short_error(exc),
                    )
                    continue
                attempt, parked = _record_deploy_failure(
                    sb, SEGMENTS_TABLE, row, exc,
                )
                logger.error(
                    "Failed to deploy segment %s (próba %d/%d%s): %s",
                    row.get("id"), attempt, MAX_DEPLOY_ATTEMPTS,
                    ", odstawiony do kolejki zatwierdzeń" if parked else "",
                    exc, exc_info=True,
                )

    return {"deployed": deployed, "reconciled": reconciled, "errors": errors}


# ---------------------------------------------------------------------------
# Task 3: sequences → activate (no wintact call, just flag)
# ---------------------------------------------------------------------------

async def activate_approved_sequences(ctx: dict[str, Any]) -> dict[str, int]:
    """Flip approved sequences to active=true so the orchestrator picks
    them up. Sequences are owned by bagent (we don't push the DAG to
    wintact — wintact only sees the per-message send)."""
    sb = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        sb.table("outreach_sequences")
        .select("id, funnel, name, entry_state")
        .eq("approval_status", "approved")
        .eq("active", False)
        .limit(20)
        .execute()
    )
    if not rows.data:
        return {"activated": 0}

    now = datetime.now(timezone.utc).isoformat()
    activated = 0
    for row in rows.data:
        sb.table("outreach_sequences").update({
            "active": True,
            "activated_at": now,
            "approval_status": "deployed",
        }).eq("id", row["id"]).execute()
        activated += 1
        logger.info(
            "Activated sequence %s/%s (entry_state=%s)",
            row["funnel"], row["name"], row["entry_state"],
        )
    return {"activated": activated}


ALL_OUTREACH_DEPLOYER_TASKS = [
    deploy_approved_templates,
    deploy_approved_segments,
    activate_approved_sequences,
]
