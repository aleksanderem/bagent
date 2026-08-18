"""Testy buildera alertów skanera Meta Ads."""

from workers.meta_ads_refresh import _ad_alerts


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
