"""Skan reklam Meta Ad Library dla salonów monitorowanych/raportowanych.

Cel produktowy: zestawianie wypuszczonych reklam konkurencji ze zmianami w
cennikach — starty/stopy reklam wchodzą do tego samego strumienia alertów co
zmiany cen (feed Zmiany/Nowości, digesty mailowe), więc chronologia „reklama →
ruch cenowy" jest widoczna bez dodatkowego UI.

Źródło danych: publiczna Meta Ad Library przez endpointy bextract
(api.booksyaudit.pl, mig 169 po stronie danych):
  GET /api/meta-ads/resolve?q=<facebook_url> → {pageId, pageName}
  GET /api/meta-ads?page_id=<id>             → {ads: [{adArchiveId, ...}]}

Docelowo (gdy user przejdzie weryfikację tożsamości Meta): oficjalne Ad
Library API — ustaw META_ADS_ACCESS_TOKEN, fetcher przełączy się sam.

Model diffu jak przy cennikach:
  - nowy ad_archive_id            → INSERT + alert ad_started
  - znany, obecny w skanie        → last_seen_at = now
  - aktywny w DB, brak w skanie   → is_active=false, ended_seen_at + ad_stopped

Cron: raz dziennie 06:15 (poza oknem nocnych jobów 03:00–05:40 — kolizje z
taxonomy-refresh opisane w memory reference_monitoring_stack).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from supabase import Client

from services.sb_client import make_supabase_client

from config import settings

logger = logging.getLogger(__name__)

# Limity per run — skan jest tani, ale FB nie lubi salw; kolejka ma ~32 salony,
# więc pełny obieg i tak domyka się w jednym runie.
RESOLVE_BATCH = 12
FETCH_BATCH = 40

_supabase_client: Client | None = None


def _get_client() -> Client:
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


def _bextract_headers() -> dict[str, str]:
    return {"x-api-key": settings.bextract_api_key}


async def _resolve_page(http: httpx.AsyncClient, facebook_url: str) -> dict[str, Any]:
    resp = await http.get(
        f"{settings.bextract_api_url}/api/meta-ads/resolve",
        params={"q": facebook_url},
        headers=_bextract_headers(),
        timeout=120.0,
    )
    resp.raise_for_status()
    return resp.json()


async def _fetch_ads_scraper(http: httpx.AsyncClient, page_id: int) -> list[dict[str, Any]]:
    resp = await http.get(
        f"{settings.bextract_api_url}/api/meta-ads",
        params={"page_id": str(page_id)},
        headers=_bextract_headers(),
        timeout=180.0,
    )
    resp.raise_for_status()
    data = resp.json()
    return list(data.get("ads") or [])


async def _fetch_ads_api(http: httpx.AsyncClient, page_id: int, token: str) -> list[dict[str, Any]]:
    """Oficjalne Ad Library API (post-DSA: wszystkie reklamy UE)."""
    resp = await http.get(
        "https://graph.facebook.com/v21.0/ads_archive",
        params={
            "search_page_ids": str(page_id),
            "ad_reached_countries": '["PL"]',
            "ad_active_status": "ACTIVE",
            "fields": "id,ad_creative_bodies,ad_delivery_start_time,publisher_platforms,eu_total_reach",
            "limit": "100",
            "access_token": token,
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    rows = resp.json().get("data") or []
    ads: list[dict[str, Any]] = []
    for r in rows:
        bodies = r.get("ad_creative_bodies") or []
        ads.append(
            {
                "adArchiveId": str(r.get("id")),
                "startedRunningOn": (r.get("ad_delivery_start_time") or "")[:10] or None,
                "creativeText": bodies[0] if bodies else None,
                "platforms": r.get("publisher_platforms") or [],
                "euTotalReach": r.get("eu_total_reach"),
                "raw": {"source": "api"},
            }
        )
    return ads


async def _fetch_ads(http: httpx.AsyncClient, page_id: int) -> list[dict[str, Any]]:
    token = os.environ.get("META_ADS_ACCESS_TOKEN", "").strip()
    if token:
        try:
            return await _fetch_ads_api(http, page_id, token)
        except Exception as exc:  # noqa: BLE001 — API bywa kapryśne, scraper ratuje run
            logger.warning("[meta-ads] API padło (%s) — fallback na scraper", exc)
    return await _fetch_ads_scraper(http, page_id)


def _sync_targets(sb: Client) -> None:
    """Widok targets → wiersze pending w salon_meta_pages (idempotentnie)."""
    targets = (
        sb.table("v_meta_ads_scan_targets").select("salon_ref_id, booksy_id, facebook_url").execute().data
        or []
    )
    known = {
        row["salon_ref_id"]
        for row in (sb.table("salon_meta_pages").select("salon_ref_id").execute().data or [])
    }
    fresh = [t for t in targets if t["salon_ref_id"] not in known]
    if fresh:
        sb.table("salon_meta_pages").insert(
            [
                {
                    "salon_ref_id": t["salon_ref_id"],
                    "booksy_id": t["booksy_id"],
                    "facebook_url": t["facebook_url"],
                }
                for t in fresh
            ]
        ).execute()
        logger.info("[meta-ads] nowych salonów w kolejce: %d", len(fresh))


async def _resolve_pending(sb: Client, http: httpx.AsyncClient) -> None:
    pending = (
        sb.table("salon_meta_pages")
        .select("salon_ref_id, facebook_url")
        .eq("resolve_status", "pending")
        .limit(RESOLVE_BATCH)
        .execute()
        .data
        or []
    )
    for row in pending:
        patch: dict[str, Any] = {"updated_at": datetime.now(timezone.utc).isoformat()}
        try:
            res = await _resolve_page(http, row["facebook_url"])
            if res.get("pageId"):
                patch.update(
                    {
                        "page_id": res["pageId"],
                        "page_name": res.get("pageName"),
                        "resolve_status": "resolved",
                        "resolved_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
            else:
                patch.update(
                    {"resolve_status": "not_found", "resolve_error": res.get("reason")}
                )
        except Exception as exc:  # noqa: BLE001 — pojedynczy salon nie wywala runu
            patch.update({"resolve_status": "error", "resolve_error": str(exc)[:300]})
        sb.table("salon_meta_pages").update(patch).eq(
            "salon_ref_id", row["salon_ref_id"]
        ).execute()
        logger.info(
            "[meta-ads] resolve %s -> %s",
            row["facebook_url"],
            patch.get("page_id") or patch.get("resolve_status"),
        )


async def _refresh_insights(sb: Client, salon_ref_id: int, salon_name: str) -> None:
    """AI-wnioski z kreacji (mig 171) — regeneracja tylko przy zmianie zestawu.

    Nieudana generacja nie psuje skanu — łapiemy i logujemy.
    """
    from datetime import date

    from services.meta_ads_insights import MODEL, ads_fingerprint, generate_insights

    rows = (
        sb.table("salon_meta_ads")
        .select(
            "ad_archive_id, started_running_on, creative_text, platforms, "
            "is_active, first_seen_at, ended_seen_at"
        )
        .eq("salon_ref_id", salon_ref_id)
        .execute()
        .data
        or []
    )
    if not rows:
        return
    today = date.today()

    def days_running(r: dict[str, Any]) -> int:
        start_raw = r.get("started_running_on") or (r.get("first_seen_at") or "")[:10]
        end_raw = (r.get("ended_seen_at") or "")[:10] or today.isoformat()
        try:
            days = (date.fromisoformat(end_raw) - date.fromisoformat(start_raw)).days
        except ValueError:
            return 1
        return max(1, days)

    ads = [
        {
            "adArchiveId": str(r["ad_archive_id"]),
            "startedRunningOn": r.get("started_running_on"),
            "creativeText": r.get("creative_text"),
            "platforms": r.get("platforms"),
            "isActive": bool(r.get("is_active")),
            "daysRunning": days_running(r),
        }
        for r in rows
    ]
    fingerprint = ads_fingerprint(ads)
    existing = (
        sb.table("salon_meta_ads_insights")
        .select("ads_fingerprint")
        .eq("salon_ref_id", salon_ref_id)
        .execute()
        .data
        or []
    )
    if existing and existing[0]["ads_fingerprint"] == fingerprint:
        return
    try:
        insights = await generate_insights(salon_name, ads)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[meta-ads] insights salon %s padły: %s", salon_ref_id, exc)
        return
    sb.table("salon_meta_ads_insights").upsert(
        {
            "salon_ref_id": salon_ref_id,
            "ads_fingerprint": fingerprint,
            "insights": insights,
            "model": MODEL,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    ).execute()
    logger.info("[meta-ads] insights salon %s zregenerowane", salon_ref_id)


def _hosted_image_url(ad: dict[str, Any]) -> str | None:
    """Pełny URL hostowanej miniatury z creativeImagePath bextracta."""
    path = ad.get("creativeImagePath")
    if not path:
        return None
    return f"{settings.bextract_api_url.rstrip('/')}{path}"


def _watchlist_rows_for(sb: Client, salon_ref_id: int) -> list[dict[str, Any]]:
    """Wiersze harmonogramu (user_id/watchlist_id/convex_site_url) dla alertów.

    Konkurenci raportów bez watchlisty → pusta lista → dane bez alertów.
    """
    return (
        sb.table("monitoring_refresh_schedule")
        .select("user_id, watchlist_id, salon_ref_id, salon_name, convex_site_url")
        .eq("salon_ref_id", salon_ref_id)
        .execute()
        .data
        or []
    )


def _ad_alerts(
    schedule_rows: list[dict[str, Any]],
    salon_name: str,
    *,
    started: list[dict[str, Any]],
    stopped: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for row in schedule_rows:
        base = {
            "userId": row["user_id"],
            "watchlistId": row["watchlist_id"],
            "salonId": row["salon_ref_id"],
            "salonName": row.get("salon_name") or salon_name,
            "_convex_site_url": row.get("convex_site_url"),
        }
        for ad in started:
            text = (ad.get("creativeText") or "").strip().replace("\n", " ")
            alerts.append(
                {
                    **base,
                    "type": "ad_started",
                    "severity": "warning",
                    "title": f"{base['salonName']}: nowa reklama na Meta",
                    "body": (
                        f"Konkurent wypuścił reklamę (start {ad.get('startedRunningOn') or 'dziś'}): "
                        f"„{text[:180]}…” Zestaw ją z ruchami w jego cenniku."
                    ),
                    "metadataJson": None,
                }
            )
        for ad in stopped:
            alerts.append(
                {
                    **base,
                    "type": "ad_stopped",
                    "severity": "info",
                    "title": f"{base['salonName']}: reklama na Meta zakończona",
                    "body": (
                        "Kampania przestała być emitowana — sprawdź, czy w ślad za nią "
                        "nie poszła zmiana cen albo koniec promocji."
                    ),
                    "metadataJson": None,
                }
            )
    return alerts


async def _scan_salon(
    sb: Client, http: httpx.AsyncClient, page_row: dict[str, Any]
) -> list[dict[str, Any]]:
    """Skan jednego salonu. Zwraca alerty do wysłania (watchlista) lub []."""
    salon_ref_id = page_row["salon_ref_id"]
    booksy_id = page_row["booksy_id"]
    page_id = page_row["page_id"]
    now_iso = datetime.now(timezone.utc).isoformat()

    ads = await _fetch_ads(http, page_id)
    seen_ids = {int(a["adArchiveId"]) for a in ads if str(a.get("adArchiveId", "")).isdigit()}

    existing = (
        sb.table("salon_meta_ads")
        .select(
            "ad_archive_id, is_active, creative_image_url, eu_total_reach, platforms, started_running_on"
        )
        .eq("salon_ref_id", salon_ref_id)
        .execute()
        .data
        or []
    )
    known = {row["ad_archive_id"] for row in existing}
    active_known = {row["ad_archive_id"] for row in existing if row["is_active"]}
    existing_by_id = {row["ad_archive_id"]: row for row in existing}

    started = [a for a in ads if int(a["adArchiveId"]) not in known]
    stopped_ids = active_known - seen_ids

    if started:
        sb.table("salon_meta_ads").insert(
            [
                {
                    "ad_archive_id": int(a["adArchiveId"]),
                    "salon_ref_id": salon_ref_id,
                    "booksy_id": booksy_id,
                    "page_id": page_id,
                    "started_running_on": a.get("startedRunningOn"),
                    "creative_text": a.get("creativeText"),
                    "platforms": a.get("platforms") or None,
                    "creative_image_url": _hosted_image_url(a),
                    "eu_total_reach": a.get("euTotalReach"),
                    "raw": a.get("raw"),
                }
                for a in started
            ]
        ).execute()
    if seen_ids:
        sb.table("salon_meta_ads").update({"last_seen_at": now_iso}).eq(
            "salon_ref_id", salon_ref_id
        ).in_("ad_archive_id", list(seen_ids)).execute()
    # Refresh znanych reklam: backfill miniatury (starsze wiersze sprzed mig 170)
    # oraz zasięg UE, który ROŚNIE w czasie — aktualizujemy przy każdym skanie.
    for a in ads:
        ad_id = int(a["adArchiveId"]) if str(a.get("adArchiveId", "")).isdigit() else None
        row = existing_by_id.get(ad_id) if ad_id else None
        if not row:
            continue
        patch: dict[str, Any] = {}
        image_url = _hosted_image_url(a)
        if image_url and not row.get("creative_image_url"):
            patch["creative_image_url"] = image_url
        reach = a.get("euTotalReach")
        if reach is not None and reach != row.get("eu_total_reach"):
            patch["eu_total_reach"] = reach
        # Parser embedded (bextract) dostarcza platformy i pewniejszą datę
        # startu niż wcześniejszy DOM-owy — dociągamy braki/rozjazdy.
        platforms = a.get("platforms") or None
        if platforms and platforms != row.get("platforms"):
            patch["platforms"] = platforms
        started = a.get("startedRunningOn")
        if started and started != row.get("started_running_on"):
            patch["started_running_on"] = started
        if patch:
            sb.table("salon_meta_ads").update(patch).eq("ad_archive_id", ad_id).execute()
    if stopped_ids:
        sb.table("salon_meta_ads").update(
            {"is_active": False, "ended_seen_at": now_iso}
        ).eq("salon_ref_id", salon_ref_id).in_(
            "ad_archive_id", list(stopped_ids)
        ).execute()

    logger.info(
        "[meta-ads] salon %s: %d aktywnych, +%d nowych, -%d zakończonych",
        salon_ref_id, len(seen_ids), len(started), len(stopped_ids),
    )
    if not started and not stopped_ids:
        return []
    # Baseline: pierwszy skan salonu zapisuje stan bez alertów — inaczej
    # świeżo dodany salon generowałby falę "nowa reklama" o kampaniach
    # startowanych tygodnie temu.
    if not existing:
        return []

    schedule_rows = _watchlist_rows_for(sb, salon_ref_id)
    if not schedule_rows:
        return []
    stopped_stub = [{"adArchiveId": i} for i in stopped_ids]
    return _ad_alerts(
        schedule_rows,
        page_row.get("page_name") or "Salon",
        started=started,
        stopped=stopped_stub,
    )


async def meta_ads_refresh_cron(ctx: dict[str, Any]) -> dict[str, Any]:
    """Nightly: sync targets → resolve pending → skan resolved → alerty."""
    # Import lokalny — unika cyklu importów przy starcie workera.
    from workers.scrape_refresh import _post_monitoring_alerts

    sb = _get_client()
    if not settings.bextract_api_key:
        logger.warning("[meta-ads] brak BEXTRACT_API_KEY — pomijam run")
        return {"skipped": "no_api_key"}

    async with httpx.AsyncClient() as http:
        _sync_targets(sb)
        await _resolve_pending(sb, http)

        resolved = (
            sb.table("salon_meta_pages")
            .select("salon_ref_id, booksy_id, page_id, page_name")
            .eq("resolve_status", "resolved")
            .limit(FETCH_BATCH)
            .execute()
            .data
            or []
        )
        all_alerts: list[dict[str, Any]] = []
        scanned = 0
        for page_row in resolved:
            try:
                all_alerts.extend(await _scan_salon(sb, http, page_row))
                scanned += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[meta-ads] skan salon_ref_id=%s padł: %s",
                    page_row["salon_ref_id"], exc,
                )
                continue
            await _refresh_insights(
                sb, page_row["salon_ref_id"], page_row.get("page_name") or "Salon"
            )

    if all_alerts:
        await _post_monitoring_alerts(all_alerts)

    result = {"scanned": scanned, "alerts": len(all_alerts)}
    logger.info("[meta-ads] run done: %s", result)
    return result
