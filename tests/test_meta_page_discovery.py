"""Kaskada znajdowania strony Facebook salonu (services/meta_page_discovery).

Kontrakt: Booksy → uchwyt z bazy kontaktów → świeży crawl WWW → wyszukiwarka
(Brave). Każde źródło ma stałą pewność, a wynik z każdego źródła poza Booksy
przechodzi kontrolę nazwy strony po rozwiązaniu (page_name vs nazwa salonu).
"""

from __future__ import annotations

import httpx
import pytest

from services.meta_page_discovery import (
    Discovery,
    DiscoveryTarget,
    SOURCE_CONFIDENCE,
    canonical_facebook_url,
    discover_facebook_page,
    discover_from_website,
    extract_facebook_urls,
    name_similarity,
    page_name_matches,
    pick_search_candidate,
    search_facebook_page,
)


def _target(**over) -> DiscoveryTarget:
    base = dict(
        salon_ref_id=9401,
        booksy_id=98814,
        name="Beauty4ever - ul. Wołoska 16",
        city="Warszawa",
        facebook_url_booksy=None,
        facebook_url_crawl=None,
        website=None,
    )
    base.update(over)
    return DiscoveryTarget(**base)


# ── Normalizacja i wyciąganie linków ────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("https://www.facebook.com/beauty4everwarszawa", "https://www.facebook.com/beauty4everwarszawa"),
        ("http://m.facebook.com/beauty4everwarszawa/?ref=x", "https://www.facebook.com/beauty4everwarszawa"),
        ("https://www.facebook.com/profile.php?id=100087763374977&mibextid=w", "https://www.facebook.com/profile.php?id=100087763374977"),
        ("beauty4everwarszawa", "https://www.facebook.com/beauty4everwarszawa"),
        ("100087763374977", "https://www.facebook.com/profile.php?id=100087763374977"),
        ("https://www.facebook.com/p/Eleon-Clinic-Warszawa-100083072273691/", "https://www.facebook.com/profile.php?id=100083072273691"),
        ("https://www.facebook.com/people/Barber-Room-Zielona-G%C3%B3ra/100041373049773/", "https://www.facebook.com/profile.php?id=100041373049773"),
        ("https://www.facebook.com/n30.clinic/videos/witamy-w-zespole/2406730246384426/", "https://www.facebook.com/n30.clinic"),
        ("https://www.facebook.com/sharer/sharer.php?u=x", None),
        ("https://www.facebook.com/login/", None),
        ("https://www.facebook.com/plugins/page.php?href=x", None),
        ("https://www.facebook.com/", None),
    ],
)
def test_canonical_facebook_url(raw, expected):
    assert canonical_facebook_url(raw) == expected


def test_extract_facebook_urls_skips_share_and_login_links_and_dedupes():
    html = """
    <a href="https://www.facebook.com/sharer/sharer.php?u=https://x.pl">udostępnij</a>
    <a href="https://www.facebook.com/login/">zaloguj</a>
    <footer>
      <a href="https://www.facebook.com/beauty4everwarszawa/">FB</a>
      <a href="http://m.facebook.com/beauty4everwarszawa">FB mobile</a>
      <a href="https://www.facebook.com/profile.php?id=100087763374977">FB2</a>
    </footer>
    """
    assert extract_facebook_urls(html) == [
        "https://www.facebook.com/beauty4everwarszawa",
        "https://www.facebook.com/profile.php?id=100087763374977",
    ]


# ── Podobieństwo nazw ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "a, b, at_least",
    [
        ("Beauty4ever - ul. Wołoska 16", "beauty4everwarszawa", 0.5),
        ("Beauty4ever - ul. Wołoska 16", "Beauty4ever Klinika Medycyny Estetycznej", 0.5),
        ("Eleon Clinic Wola", "Eleon Clinic", 0.6),
        ("Eleon Clinic Wola", "Eleon Clinic Warszawa", 0.5),
        ("Twój stary barber", "Twoj Stary Barber Bydgoszcz", 0.6),
        ("MEDESTETIS Klinika Dr Kuschill", "Klinika Kuschill MEDESTETIS", 0.6),
        ("RosaMed Clinic", "RosaMedClinic", 0.6),
    ],
)
def test_name_similarity_recognises_same_salon(a, b, at_least):
    assert name_similarity(a, b) >= at_least


@pytest.mark.parametrize(
    "a, b",
    [
        ("KARASEK CLINIC", "My Day Beauty Space"),
        ("Beauty4ever - ul. Wołoska 16", "Salon Fryzjerski Anna"),
        # Realne pudła z Brave (2026-09-04): wspólne jedno słowo to za mało.
        ("Twój stary barber", "Stacja Barber"),
        ("Twój stary barber", "Barbatus Barber Shop"),
        ("Beauty Hair Boski Sadowski", "BOSKI Salon Urody"),
        ("MEDESTETIS Klinika Dr Kuschill", "Dr med. Joanna Kuschill-Dziurda"),
    ],
)
def test_name_similarity_rejects_other_salon(a, b):
    assert name_similarity(a, b) < 0.45


def test_page_name_matches_uses_threshold():
    assert page_name_matches("Beauty4ever Klinika", "Beauty4ever - ul. Wołoska 16")
    assert not page_name_matches("My Day Beauty Space", "KARASEK CLINIC")
    # Realny fałszywy alarm z prod (2026-09-04): tytuł strony z hasłem i miastem.
    assert page_name_matches("La Beaute Clinique - razem do doskonałości | Warsaw", "La Beaute Clinique")
    assert page_name_matches("Elle Clinic | Warsaw", "Elle Clinic")
    assert page_name_matches("Revival Clinic | Wilanów", "Revival Clinic")
    assert not page_name_matches("BOSKI Salon Urody | Lipinki", "Beauty Hair Boski Sadowski")
    # Brak nazwy strony (resolve przez widget) = nie da się sprawdzić, przepuszczamy.
    assert page_name_matches(None, "KARASEK CLINIC")


# ── Wyszukiwarka ────────────────────────────────────────────────────────────


def test_pick_search_candidate_prefers_matching_page_and_skips_noise():
    results = [
        {"title": "Zaloguj się do Facebooka", "url": "https://www.facebook.com/login/"},
        {"title": "Salon Anna | Facebook", "url": "https://www.facebook.com/salonannawawa"},
        {"title": "Beauty4ever Klinika Medycyny Estetycznej | Warszawa | Facebook",
         "url": "https://www.facebook.com/beauty4everwarszawa/"},
    ]
    picked = pick_search_candidate(results, "Beauty4ever - ul. Wołoska 16", "Warszawa")
    assert picked is not None
    assert picked.facebook_url == "https://www.facebook.com/beauty4everwarszawa"
    assert picked.source == "web_search"


def test_pick_search_candidate_returns_none_when_nothing_matches():
    results = [{"title": "Salon Anna | Facebook", "url": "https://www.facebook.com/salonannawawa"}]
    assert pick_search_candidate(results, "Eleon Clinic Wola", "Warszawa") is None


async def test_search_facebook_page_calls_brave_with_site_filter():
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["token"] = request.headers.get("X-Subscription-Token")
        body = {"web": {"results": [
            {"title": "Eleon Clinic | Facebook", "url": "https://www.facebook.com/eleonclinic"},
        ]}}
        return httpx.Response(200, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await search_facebook_page(http, "Eleon Clinic Wola", "Warszawa", api_key="k1")

    assert found == Discovery("https://www.facebook.com/eleonclinic", "web_search", SOURCE_CONFIDENCE["web_search"], evidence=None) or found.facebook_url == "https://www.facebook.com/eleonclinic"
    assert seen["token"] == "k1"
    assert "site%3Afacebook.com" in str(seen["url"]) or "site:facebook.com" in str(seen["url"])
    assert "Eleon" in str(seen["url"])


async def test_search_facebook_page_without_key_is_skipped():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as http:
        assert await search_facebook_page(http, "X", "Y", api_key="") is None


# ── Crawl WWW ───────────────────────────────────────────────────────────────


async def test_discover_from_website_reads_homepage_then_contact_page():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.path in ("", "/"):
            return httpx.Response(200, text='<a href="/kontakt">Kontakt</a>')
        if request.url.path == "/kontakt":
            return httpx.Response(200, text='<a href="https://www.facebook.com/beauty4everwarszawa">fb</a>')
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await discover_from_website(http, "http://www.beauty4ever.pl/")

    assert found is not None
    assert found.facebook_url == "https://www.facebook.com/beauty4everwarszawa"
    assert found.source == "website_crawl"
    assert found.confidence == SOURCE_CONFIDENCE["website_crawl"]
    assert found.evidence == "http://www.beauty4ever.pl/kontakt"
    assert any(u.endswith("/kontakt") for u in calls)


async def test_discover_from_website_tolerates_dead_site():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        assert await discover_from_website(http, "https://nie-ma-takiej-domeny.pl") is None


# ── Kaskada ─────────────────────────────────────────────────────────────────


async def test_cascade_booksy_first():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as http:
        found = await discover_facebook_page(
            _target(facebook_url_booksy="https://www.facebook.com/beauty4everwarszawa/",
                    facebook_url_crawl="https://www.facebook.com/inna"),
            http, brave_api_key="k",
        )
    assert found is not None
    assert (found.facebook_url, found.source, found.confidence) == (
        "https://www.facebook.com/beauty4everwarszawa", "booksy", SOURCE_CONFIDENCE["booksy"],
    )


async def test_cascade_uses_stored_crawl_handle_before_fetching():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as http:
        found = await discover_facebook_page(
            _target(facebook_url_crawl="https://www.facebook.com/beauty4everwarszawa", website="http://www.beauty4ever.pl/"),
            http, brave_api_key="k",
        )
    assert found is not None
    assert found.source == "website_crawl"


async def test_cascade_crawls_website_then_saves_contact_point():
    saved: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if "facebook.com" in request.url.host:
            return httpx.Response(500)
        return httpx.Response(200, text='<a href="https://www.facebook.com/beauty4everwarszawa">fb</a>')

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await discover_facebook_page(
            _target(website="http://www.beauty4ever.pl/"), http, brave_api_key="",
            save_contact_point=lambda point: saved.append(point),
        )
    assert found is not None and found.source == "website_crawl"
    assert saved and saved[0]["kind"] == "facebook" and saved[0]["value"] == "beauty4everwarszawa"
    assert saved[0]["source"] == "website_crawl"


async def test_cascade_falls_back_to_search_and_skips_sources_already_tried():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.search.brave.com":
            return httpx.Response(200, json={"web": {"results": [
                {"title": "Eleon Clinic | Facebook", "url": "https://www.facebook.com/eleonclinic"},
            ]}})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await discover_facebook_page(
            _target(name="Eleon Clinic Wola", facebook_url_booksy="https://www.facebook.com/stary-link"),
            http, brave_api_key="k", skip_sources={"booksy"},
        )
    assert found is not None
    assert found.source == "web_search"
    assert found.facebook_url == "https://www.facebook.com/eleonclinic"


async def test_cascade_returns_none_when_every_source_fails():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404))) as http:
        assert await discover_facebook_page(_target(website="http://x.pl"), http, brave_api_key="") is None
