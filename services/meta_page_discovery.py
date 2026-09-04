"""Znajdowanie strony Facebook salonu — kaskada źródeł.

Po co: skan reklam Ad Library (workers/meta_ads_refresh) potrzebuje page_id
strony FB salonu, a link do niej ma w Booksy tylko ~54% salonów (35 172 z
65 642 chain-heads, prod 2026-09-04). Bez tej kaskady salon bez linku w Booksy
— np. Beauty4ever (salon_ref_id 9401) — nigdy nie trafiał do skanu.

Kaskada (pierwsze trafienie wygrywa, pewność w SOURCE_CONFIDENCE):
  1. booksy        — link wpisany przez właściciela w profilu Booksy
  2. website_crawl — uchwyt już zapisany w salon_contact_points (mig 167)
  3. website_crawl — świeży crawl własnej WWW (strona główna + „Kontakt");
                     wynik wraca do salon_contact_points, więc korzysta z niego
                     też outreach
  4. web_search    — Brave Search API: site:facebook.com "nazwa" miasto

Czego NIE robimy i dlaczego (sprawdzone z tytana 2026-09-04): Instagram
przekierowuje anonimowe wejścia do logowania; Bing bez JS zwraca pustą stronę
wyników, DuckDuckGo blokuje IP serwerowe; wyszukiwarka Ad Library (typeahead)
też odrzuca IP datacenter — komentarz w bextract metaAds.js.

Wynik spoza Booksy przechodzi po rozwiązaniu strony kontrolę nazwy
(`page_name_matches`): cudza strona ze stopki WWW (przykład z prod: KARASEK
CLINIC → facebook.com/mydaybeautyspace) dostaje w workerze status 'mismatch'
i nie jest skanowana.

Czysty moduł: HTTP przez wstrzyknięty httpx.AsyncClient, zapis do bazy przez
callback — testowalny bez sieci (tests/test_meta_page_discovery.py).
"""

from __future__ import annotations

import inspect
import logging
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Callable, Collection
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

SOURCE_CONFIDENCE: dict[str, int] = {"booksy": 95, "website_crawl": 70, "web_search": 50}

#: Próg zgodności nazwy strony FB z nazwą salonu (po rozwiązaniu strony).
NAME_MATCH_THRESHOLD = 0.45
#: Próg dla kandydata z wyszukiwarki — wyżej, bo to najsłabsze źródło.
SEARCH_MATCH_THRESHOLD = 0.5

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"

_FETCH_TIMEOUT = 15.0
_MAX_BODY = 800_000
_MAX_CONTACT_PAGES = 2

_FB_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|m\.|web\.|mbasic\.|pl-pl\.)?facebook\.com/([^\s\"'<>)]+)",
    re.IGNORECASE,
)
_HANDLE_RE = re.compile(r"^[A-Za-z0-9.\-]{2,60}$")
_PROFILE_ID_RE = re.compile(r"id=(\d{6,})")
_CONTACT_LINK_RE = re.compile(r"""href=["']([^"']*(?:kontakt|contact)[^"']*)["']""", re.IGNORECASE)

#: Pierwsze segmenty ścieżki facebook.com, które NIE są stroną salonu.
_FB_NOT_PAGE = frozenset(
    {
        "login", "sharer", "share", "dialog", "plugins", "help", "policies", "policy",
        "privacy", "public", "hashtag", "groups", "events", "watch", "reel", "reels",
        "photo", "photos", "story", "stories", "people", "home", "pages", "legal",
        "settings", "tr", "ads", "business", "marketplace", "gaming", "messages",
        "posts", "video", "videos", "search", "campaign", "media", "notes", "about",
        "terms", "cookies", "careers", "security", "recover", "checkpoint",
        "bookmarks", "friends", "notifications", "l.php", "permalink.php", "story.php",
        "photo.php", "video.php", "events.php",
    }
)

#: Słowa, które nie odróżniają salonów od siebie — wycinane przed porównaniem.
#: Bez tego „Stacja Barber" pasowało do „Twój stary barber" przez samo „barber".
_GENERIC_TOKENS = frozenset(
    {
        "salon", "studio", "beauty", "clinic", "klinika", "kliniki", "klinik", "clinique",
        "kosmetyczny", "kosmetyczne", "kosmetyka", "kosmetolog", "kosmetologia",
        "gabinet", "spa", "ul", "al", "sp", "z", "o", "oo", "i", "and", "the",
        "medycyna", "medycyny", "medical", "aesthetic", "aesthetics", "estetyczna",
        "estetycznej", "estetyczny", "med", "dr", "lek", "fryzjerski", "fryzjerska",
        "fryzjer", "fryzjerstwo", "pracownia", "barber", "barbershop", "shop",
        "urody", "nails", "hair", "lashes", "brows", "paznokcie", "manicure",
        "centrum", "instytut", "oficjalna", "strona", "official", "page",
        # Duże miasta — w tytułach z wyszukiwarki bywają po angielsku („Warsaw"),
        # więc samo wycięcie podanego miasta nie wystarcza.
        "warszawa", "warsaw", "krakow", "cracow", "wroclaw", "poznan", "gdansk",
        "lodz", "szczecin", "lublin", "katowice", "bydgoszcz",
    }
)


@dataclass(frozen=True)
class DiscoveryTarget:
    """Surowiec kaskady dla jednego salonu (wiersz v_meta_ads_discovery_targets)."""

    salon_ref_id: int
    booksy_id: int
    name: str
    city: str | None
    facebook_url_booksy: str | None
    facebook_url_crawl: str | None
    website: str | None


@dataclass(frozen=True)
class Discovery:
    """Znaleziony link do strony FB wraz z pochodzeniem."""

    facebook_url: str
    source: str
    confidence: int
    evidence: str | None = None


# ── Normalizacja linków ─────────────────────────────────────────────────────


def canonical_facebook_url(raw: str | None) -> str | None:
    """Pełny URL strony FB z linku, uchwytu albo numerycznego id. None, gdy to
    nie jest strona (login, sharer, plugins…) albo nie da się rozpoznać."""
    if not raw:
        return None
    text = raw.strip()
    m = _FB_URL_RE.search(text)
    if m:
        path = m.group(1)
    elif text.isdigit():
        return f"https://www.facebook.com/profile.php?id={text}"
    elif _HANDLE_RE.match(text):
        path = text
    else:
        return None

    path = path.strip("/")
    if not path:
        return None
    if path.lower().startswith("profile.php"):
        pid = _PROFILE_ID_RE.search(path)
        return f"https://www.facebook.com/profile.php?id={pid.group(1)}" if pid else None

    segments = [s for s in re.split(r"[/?#]", path) if s]
    if not segments:
        return None
    if segments[0].lower() == "pg" and len(segments) > 1:
        segments = segments[1:]
    if segments[0].lower() in ("p", "people"):
        # Nowe adresy stron bez uchwytu: /p/Nazwa-Strony-100083072273691/
        # i /people/Nazwa/100041373049773/ — id strony jest na końcu.
        for seg in segments[1:]:
            tail = re.search(r"(\d{8,})$", seg)
            if tail:
                return f"https://www.facebook.com/profile.php?id={tail.group(1)}"
        return None
    handle = segments[0]
    if handle.lower() in _FB_NOT_PAGE or handle.lower().endswith(".php"):
        return None
    if handle.isdigit():
        return f"https://www.facebook.com/profile.php?id={handle}"
    if not _HANDLE_RE.match(handle):
        return None
    return f"https://www.facebook.com/{handle}"


def facebook_handle(canonical_url: str) -> str:
    """Wartość do salon_contact_points: uchwyt albo numeryczny id."""
    pid = _PROFILE_ID_RE.search(canonical_url)
    if pid:
        return pid.group(1)
    return canonical_url.rstrip("/").rsplit("/", 1)[-1]


def extract_facebook_urls(html: str) -> list[str]:
    """Wszystkie linki do stron FB w HTML, znormalizowane, bez duplikatów,
    w kolejności wystąpienia."""
    seen: list[str] = []
    for m in _FB_URL_RE.finditer(html):
        url = canonical_facebook_url(m.group(0))
        if url and url not in seen:
            seen.append(url)
    return seen


# ── Podobieństwo nazw ───────────────────────────────────────────────────────


def _normalize_tokens(text: str) -> list[str]:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    # NFKD nie rozkłada ł/Ł — jedyne polskie litery bez formy z diakrytykiem.
    folded = text.translate(str.maketrans("łŁ", "lL"))
    folded = unicodedata.normalize("NFKD", folded).encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z0-9]+", folded) if t and t not in _GENERIC_TOKENS]


def _containment(tokens: list[str], joined_other: str, longer_len: int) -> float:
    """0.8, gdy mocny token (≥5 znaków) jednej nazwy siedzi w drugiej I stanowi
    co najmniej połowę liter DŁUŻSZEJ z nazw. Sam wspólny wyraz („boski",
    „barber") nie wystarcza — inaczej „BOSKI Salon Urody" pasowało do
    „Beauty Hair Boski Sadowski", a „Stacja Barber" do „Twój stary barber"."""
    for t in tokens:
        if len(t) >= 5 and t in joined_other and len(t) / max(longer_len, 1) >= 0.5:
            return 0.8
    return 0.0


def name_similarity(a: str, b: str) -> float:
    """0..1 — czy dwie nazwy (salonu, strony FB, uchwytu) to ten sam salon.
    Największa z: Jaccard tokenów ważony literami, podobieństwo całych ciągów
    (liczy się tylko, gdy prawie identyczne), zawieranie mocnego tokenu jednej
    nazwy w drugiej (patrz _containment)."""
    ta, tb = _normalize_tokens(a), _normalize_tokens(b)
    if not ta or not tb:
        return 0.0
    sa, sb = set(ta), set(tb)
    # Jaccard ważony literami: wspólne „boski" (5 liter) przy „sadowski" (8)
    # to 0.38, nie 0.5 — jedno krótkie wspólne słowo nie robi tego samego salonu.
    jaccard = sum(len(t) for t in sa & sb) / sum(len(t) for t in sa | sb)
    joined_a, joined_b = "".join(ta), "".join(tb)
    # SequenceMatcher premiuje prefiksy („boski" vs „boskisadowski" = 0.56),
    # więc bierzemy go tylko dla niemal identycznych ciągów.
    ratio = SequenceMatcher(None, joined_a, joined_b).ratio()
    if ratio < 0.75:
        ratio = 0.0
    longer = max(len(joined_a), len(joined_b))
    containment = max(
        _containment(ta, joined_b, longer),
        _containment(tb, joined_a, longer),
    )
    return max(jaccard, ratio, containment)


def page_name_matches(page_name: str | None, salon_name: str) -> bool:
    """Kontrola po rozwiązaniu strony. Brak nazwy (resolve przez widget
    page-plugin nie zwraca tytułu) = nie ma czego sprawdzać, przepuszczamy."""
    if not page_name:
        return True
    return name_similarity(page_name, salon_name) >= NAME_MATCH_THRESHOLD


# ── Wyszukiwarka (Brave) ────────────────────────────────────────────────────


def _strip_facebook_suffix(title: str) -> str:
    return re.sub(r"\s*[|\-–]\s*facebook\s*$", "", title, flags=re.IGNORECASE).strip()


def _without_city(text: str, city: str | None) -> str:
    if not city:
        return text
    return re.sub(re.escape(city), " ", text, flags=re.IGNORECASE)


def _short_name(name: str) -> str:
    """„Beauty4ever - ul. Wołoska 16" → „Beauty4ever": część przed myślnikiem
    lub przecinkiem, gdy reszta wygląda na adres. Do zapytania w wyszukiwarce."""
    head = re.split(r"\s+[-–]\s+|,", name, maxsplit=1)[0].strip()
    return head if len(head) >= 3 else name


def pick_search_candidate(
    results: list[dict[str, Any]], name: str, city: str | None
) -> Discovery | None:
    """Najlepszy wynik wyszukiwarki, który wygląda na stronę TEGO salonu."""
    best: tuple[float, Discovery] | None = None
    for r in results:
        url = canonical_facebook_url(str(r.get("url") or ""))
        if not url:
            continue
        title = _without_city(_strip_facebook_suffix(str(r.get("title") or "")), city)
        slug = "" if "profile.php" in url else facebook_handle(url)
        score = max(name_similarity(name, title), name_similarity(name, slug) if slug else 0.0)
        if score >= SEARCH_MATCH_THRESHOLD and (best is None or score > best[0]):
            best = (score, Discovery(url, "web_search", SOURCE_CONFIDENCE["web_search"],
                                     evidence=str(r.get("url") or "")))
    return best[1] if best else None


async def search_facebook_page(
    http: httpx.AsyncClient, name: str, city: str | None, api_key: str
) -> Discovery | None:
    """Brave Search: site:facebook.com nazwa miasto. Bez klucza krok jest
    pomijany. Nazwa BEZ cudzysłowu — fraza w cudzysłowie zwracała 0 wyników
    dla „MEDESTETIS Klinika Dr Kuschill" i „Eleon Clinic Wola" (sprawdzone
    2026-09-04), bez cudzysłowu ich strony były na pierwszym miejscu.
    Najpierw pełna nazwa, potem krótka (bez adresu), gdy się różnią."""
    if not api_key:
        return None
    queries = [name]
    short = _short_name(name)
    if short != name:
        queries.append(short)
    for q in queries:
        query = f"site:facebook.com {q} {city or ''}".strip()
        try:
            resp = await http.get(
                BRAVE_SEARCH_URL,
                params={"q": query, "count": 10, "country": "PL", "search_lang": "pl"},
                headers={"Accept": "application/json", "X-Subscription-Token": api_key},
                timeout=20.0,
            )
            resp.raise_for_status()
            results = list((resp.json().get("web") or {}).get("results") or [])
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("[meta-discovery] Brave %r padło: %s", query, exc)
            return None
        found = pick_search_candidate(results, name, city)
        if found:
            return found
    return None


# ── Crawl własnej WWW ───────────────────────────────────────────────────────


def _url_variants(website: str) -> list[str]:
    raw = website.strip()
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        raw = "https://" + raw
    parsed = urlparse(raw)
    host = parsed.netloc
    other_host = host[4:] if host.lower().startswith("www.") else "www." + host
    other_scheme = "http" if parsed.scheme == "https" else "https"
    path = parsed.path or "/"
    return [
        raw,
        f"{other_scheme}://{host}{path}",
        f"{parsed.scheme}://{other_host}{path}",
    ]


async def _fetch_html(http: httpx.AsyncClient, url: str) -> str | None:
    try:
        resp = await http.get(url, timeout=_FETCH_TIMEOUT, follow_redirects=True)
    except httpx.HTTPError:
        return None
    if resp.status_code >= 400:
        return None
    return resp.text[:_MAX_BODY]


def _contact_links(html: str, base_url: str) -> list[str]:
    base_host = urlparse(base_url).netloc.lower()
    out: list[str] = []
    for href in _CONTACT_LINK_RE.findall(html):
        target = urljoin(base_url, href.strip())
        if urlparse(target).netloc.lower() != base_host or target in out:
            continue
        out.append(target)
        if len(out) >= _MAX_CONTACT_PAGES:
            break
    return out


async def discover_from_website(http: httpx.AsyncClient, website: str) -> Discovery | None:
    """Strona główna, a gdy tam nie ma linku FB — podstrony „Kontakt"."""
    for candidate in _url_variants(website):
        html = await _fetch_html(http, candidate)
        if html is None:
            continue
        urls = extract_facebook_urls(html)
        if urls:
            return Discovery(urls[0], "website_crawl", SOURCE_CONFIDENCE["website_crawl"], evidence=candidate)
        for contact_url in _contact_links(html, candidate):
            sub = await _fetch_html(http, contact_url)
            if sub is None:
                continue
            urls = extract_facebook_urls(sub)
            if urls:
                return Discovery(urls[0], "website_crawl", SOURCE_CONFIDENCE["website_crawl"], evidence=contact_url)
        return None
    return None


# ── Kaskada ─────────────────────────────────────────────────────────────────

SaveContactPoint = Callable[[dict[str, Any]], Any]


async def discover_facebook_page(
    target: DiscoveryTarget,
    http: httpx.AsyncClient,
    *,
    brave_api_key: str,
    save_contact_point: SaveContactPoint | None = None,
    skip_sources: Collection[str] = (),
) -> Discovery | None:
    """Pierwsze źródło, które da link. `skip_sources` pozwala ponowić kaskadę
    dla salonu, którego poprzedni link (np. z Booksy) nie dał się rozwiązać."""
    skip = set(skip_sources)

    if "booksy" not in skip:
        url = canonical_facebook_url(target.facebook_url_booksy)
        if url:
            return Discovery(url, "booksy", SOURCE_CONFIDENCE["booksy"], evidence="profil Booksy")

    if "website_crawl" not in skip:
        url = canonical_facebook_url(target.facebook_url_crawl)
        if url:
            return Discovery(url, "website_crawl", SOURCE_CONFIDENCE["website_crawl"],
                             evidence="salon_contact_points")
        if target.website:
            found = await discover_from_website(http, target.website)
            if found:
                if save_contact_point is not None:
                    result = save_contact_point(
                        {
                            "salon_ref_id": target.salon_ref_id,
                            "kind": "facebook",
                            "value": facebook_handle(found.facebook_url),
                            "source": "website_crawl",
                            "source_url": found.evidence,
                            "evidence": "link do FB na stronie salonu",
                            "confidence": found.confidence,
                        }
                    )
                    if inspect.isawaitable(result):
                        await result
                return found

    if "web_search" not in skip:
        return await search_facebook_page(http, target.name, target.city, brave_api_key)

    return None
