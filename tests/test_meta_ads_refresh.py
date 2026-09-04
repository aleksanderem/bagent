"""Testy buildera alertów skanera Meta Ads i planu kaskady znajdowania stron FB."""

from datetime import datetime, timedelta, timezone

from workers.meta_ads_refresh import (
    DISCOVERY_RETRY_DAYS,
    _ad_alerts,
    _ads_page_conflict,
    _discovery_plan,
    _page_name_from_ads,
)


def test_page_name_from_ads_takes_first_non_empty():
    ads = [{"adArchiveId": "1"}, {"adArchiveId": "2", "pageName": " My Day Beauty Space "}]
    assert _page_name_from_ads(ads) == "My Day Beauty Space"
    assert _page_name_from_ads([]) is None


def test_ads_page_conflict_only_for_links_outside_booksy():
    # Link ze stopki WWW prowadzi na cudzą stronę → konflikt, nie skanujemy.
    row = {"facebook_source": "website_crawl"}
    assert _ads_page_conflict(row, "My Day Beauty Space", "KARASEK CLINIC")
    # Ta sama strona pod hasłem → brak konfliktu.
    assert _ads_page_conflict(row, "Beauty4ever Klinika Medycyny Estetycznej", "Beauty4ever - ul. Wołoska 16") is None
    # Link z Booksy wpisał właściciel — nie sprawdzamy, nawet gdy nazwa inna.
    assert _ads_page_conflict({"facebook_source": "booksy"}, "Inna Nazwa", "KARASEK CLINIC") is None
    assert _ads_page_conflict({}, "Inna Nazwa", "KARASEK CLINIC") is None


def test_discovery_plan_new_targets_get_full_cascade_failed_skip_their_source():
    now = datetime(2026, 9, 4, tzinfo=timezone.utc)
    old = (now - timedelta(days=DISCOVERY_RETRY_DAYS + 1)).isoformat()
    fresh = (now - timedelta(hours=3)).isoformat().replace("+00:00", "Z")
    rows = [
        {"salon_ref_id": 1, "resolve_status": None},
        {"salon_ref_id": 2, "resolve_status": "resolved", "page_updated_at": old},
        {"salon_ref_id": 3, "resolve_status": "not_found", "facebook_source": "booksy", "page_updated_at": old},
        {"salon_ref_id": 4, "resolve_status": "error", "facebook_source": None, "page_updated_at": fresh},
        {"salon_ref_id": 5, "resolve_status": "mismatch", "facebook_source": "website_crawl", "page_updated_at": old},
        {"salon_ref_id": 6, "resolve_status": "pending", "page_updated_at": old},
        {"salon_ref_id": 7, "resolve_status": "not_found", "facebook_source": None, "page_updated_at": old},
    ]
    plan = [(r["salon_ref_id"], set(skip)) for r, skip in _discovery_plan(rows, now)]
    assert plan == [(1, set()), (3, {"booksy"}), (7, set())]


def _rows():
    return [{
        "user_id": "u1", "watchlist_id": "w1", "salon_ref_id": 7,
        "salon_name": "Salon X", "convex_site_url": "https://dev.convex.site",
    }]


def test_started_alert_carries_creative_and_date():
    alerts = _ad_alerts(
        _rows(), "Salon X",
        started=[{"adArchiveId": "1", "startedRunningOn": "2026-08-07",
                  "creativeText": "Depilacja -30%\ntylko teraz"}],
        stopped=[],
    )
    assert len(alerts) == 1
    a = alerts[0]
    assert a["type"] == "ad_started"
    assert a["_convex_site_url"] == "https://dev.convex.site"
    assert "2026-08-07" in a["body"]
    assert "Depilacja -30% tylko teraz"[:20] in a["body"]


def test_stopped_alert_and_no_watchlist_means_no_alerts():
    assert _ad_alerts([], "Salon X", started=[{"adArchiveId": "1"}], stopped=[]) == []
    alerts = _ad_alerts(_rows(), "Salon X", started=[], stopped=[{"adArchiveId": "9"}])
    assert alerts[0]["type"] == "ad_stopped"
