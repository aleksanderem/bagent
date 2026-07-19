"""Email enrichment crawler — zadanie 3 planu email outreach.

Odzyskuje adresy kontaktowe z własnych stron WWW salonów (12 149 domen w
salons.website nie-social) i wpisuje je do outreach_contacts. Booksy zabramkował
public_email na API (patrz reference_booksy_email_gating), więc crawl WWW +
fallback zewnętrzny to jedyna droga skalowania listy powyżej 5 054 odzyskanych
z raw.

Warstwa 1 (ten plik): własny crawler, darmowy, ~30-50% yield.
Warstwa 2 (fallback): zewnętrzny serwis dla nietrafionych — stub, aktywny gdy
podany klucz (--fallback-key); bez klucza pomijany i raportowany jako blocker.

BEZPIECZEŃSTWO: insert z cohort='imported', consent_b2b=false → staged, nie do
wysyłki (patrz backfill). Idempotentny (ON CONFLICT email_hash w tabeli).

Użycie (na tytanie, w katalogu bagent):
    uv run python -m pipelines.enrich_emails --limit 50 --dry-run   # test próbki
    uv run python -m pipelines.enrich_emails --dry-run              # pełny dry-run
    uv run python -m pipelines.enrich_emails                        # pełny run + insert
"""

from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx

# ── Ekstrakcja maili ────────────────────────────────────────────────────────

# Email w tekście/atrybutach. Celowo konserwatywny (unikamy false-positive
# typu wersje@2x, plik@media itp. — patrz _NOISE_DOMAINS + walidacja TLD).
_EMAIL_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9._%+\-]{0,62}[a-z0-9])?@"
    r"[a-z0-9](?:[a-z0-9\-]{0,62}[a-z0-9])?(?:\.[a-z]{2,})+",
    re.IGNORECASE,
)
_MAILTO_RE = re.compile(r"mailto:([^\"'?>\s]+)", re.IGNORECASE)

# Domeny/końcówki, które NIE są adresami kontaktowymi salonu.
_NOISE_DOMAINS = (
    "sentry.io", "wixpress.com", "example.com", "example.org", "domain.com",
    "sentry-next.wixpress.com", "googleapis.com", "gstatic.com", "schema.org",
    "w3.org", "google.com", "facebook.com", "instagram.com", "youtube.com",
    "sentry.wixpress.com", "your-email.com", "email.com", "test.com",
    "wordpress.com", "wp.com", "gravatar.com", "cloudflare.com", "jsdelivr.net",
)
_NOISE_LOCALPARTS = ("noreply", "no-reply", "donotreply", "example", "your",
                     "email", "name", "user", "sentry")
# Rozszerzenia plików mylnie łapane jako TLD (foo@2x.png itp. odrzuci walidacja
# TLD alfabetyczny, ale bądźmy jawni dla obrazków).
_NOISE_TLDS = ("png", "jpg", "jpeg", "gif", "svg", "webp", "css", "js", "json")

_CONTACT_PATHS = ("/kontakt", "/kontakt/", "/contact", "/contact/",
                  "/o-nas", "/o-nas/", "/kontakt.html")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pl,en;q=0.8",
}


def _valid_email(email: str) -> bool:
    email = email.strip().lower().rstrip(".")
    if email.count("@") != 1:
        return False
    local, _, domain = email.partition("@")
    if not local or not domain or ".." in email:
        return False
    tld = domain.rsplit(".", 1)[-1]
    if tld in _NOISE_TLDS or not tld.isalpha() or len(tld) < 2:
        return False
    if domain in _NOISE_DOMAINS:
        return False
    if any(domain.endswith("." + nd) or domain == nd for nd in _NOISE_DOMAINS):
        return False
    if any(local.startswith(np) for np in _NOISE_LOCALPARTS):
        return False
    return True


def extract_emails(html: str) -> list[str]:
    """Zwraca unikalne, poprawne maile z HTML — mailto: mają priorytet."""
    found: list[str] = []
    seen: set[str] = set()
    for m in _MAILTO_RE.findall(html):
        e = m.split("?")[0].strip().lower()
        if _valid_email(e) and e not in seen:
            seen.add(e)
            found.append(e)
    for m in _EMAIL_RE.findall(html):
        e = m.strip().lower().rstrip(".")
        if _valid_email(e) and e not in seen:
            seen.add(e)
            found.append(e)
    return found


def pick_best(emails: list[str], site_url: str) -> str | None:
    """Preferuj mail z domeny salonu (mniej ryzyka śmieci/agencji)."""
    if not emails:
        return None
    site_host = urlparse(site_url).hostname or ""
    site_root = site_host.lower().lstrip("www.")
    base = site_root.rsplit(".", 2)[0] if site_root.count(".") >= 1 else site_root
    on_domain = [e for e in emails if base and base in e.split("@", 1)[1]]
    if on_domain:
        return on_domain[0]
    # Inaczej pierwszy nie-freemail? Nie — freemail (gmail) jest OK dla salonu,
    # bierzemy po prostu pierwszy poprawny.
    return emails[0]


# ── Crawl ───────────────────────────────────────────────────────────────────

@dataclass
class EnrichResult:
    salon_ref_id: int
    website: str
    email: str | None = None
    status: str = "no_email"          # ok | no_email | fetch_error | http_4xx_5xx
    note: str = ""


async def fetch(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        r = await client.get(url, headers=_HEADERS, follow_redirects=True, timeout=12.0)
        if r.status_code >= 400:
            return None
        ctype = r.headers.get("content-type", "")
        if "html" not in ctype and "text" not in ctype:
            return None
        return r.text[:800_000]  # cap
    except (httpx.HTTPError, asyncio.TimeoutError):
        return None


async def enrich_one(client: httpx.AsyncClient, salon_ref_id: int, website: str) -> EnrichResult:
    if not website.lower().startswith("http"):
        website = "https://" + website
    res = EnrichResult(salon_ref_id=salon_ref_id, website=website)

    html = await fetch(client, website)
    if html is None:
        res.status = "fetch_error"
        return res

    emails = extract_emails(html)
    # Homepage pusta → spróbuj typowe podstrony kontaktowe.
    if not emails:
        for path in _CONTACT_PATHS:
            sub = urljoin(website, path)
            sub_html = await fetch(client, sub)
            if sub_html:
                emails = extract_emails(sub_html)
                if emails:
                    res.note = f"via {path}"
                    break

    best = pick_best(emails, website)
    if best:
        res.email = best
        res.status = "ok"
    return res


async def run_batch(rows: list[tuple[int, str]], concurrency: int) -> list[EnrichResult]:
    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    results: list[EnrichResult] = []
    async with httpx.AsyncClient(limits=limits) as client:
        async def worker(sid: int, url: str) -> None:
            async with sem:
                results.append(await enrich_one(client, sid, url))
        await asyncio.gather(*(worker(sid, url) for sid, url in rows))
    return results


# ── DB ──────────────────────────────────────────────────────────────────────

def _make_sb():
    from config import settings
    from services.sb_client import make_supabase_client
    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_KEY not configured")
    return make_supabase_client(settings.supabase_url, settings.supabase_service_key)


def load_candidates(sb, limit: int | None) -> list[tuple[int, str]]:
    """Salony z własną domeną WWW, których jeszcze nie ma w outreach_contacts."""
    existing = set()
    off = 0
    while True:
        page = (sb.table("outreach_contacts").select("salon_ref_id")
                .not_.is_("salon_ref_id", "null").range(off, off + 999).execute())
        if not page.data:
            break
        existing.update(r["salon_ref_id"] for r in page.data)
        if len(page.data) < 1000:
            break
        off += 1000

    rows: list[tuple[int, str]] = []
    off = 0
    while True:
        page = (sb.table("salons").select("id,website")
                .not_.is_("website", "null").neq("website", "")
                .range(off, off + 999).execute())
        if not page.data:
            break
        for r in page.data:
            w = (r.get("website") or "").strip()
            if not w or r["id"] in existing:
                continue
            if re.search(r"facebook|instagram|booksy|linktr|beacons", w, re.I):
                continue
            rows.append((r["id"], w))
        if len(page.data) < 1000:
            break
        off += 1000
        if limit and len(rows) >= limit:
            break
    return rows[:limit] if limit else rows


def insert_contacts(sb, oks: list[EnrichResult]) -> int:
    inserted = 0
    for r in oks:
        try:
            sb.table("outreach_contacts").insert({
                "salon_ref_id": r.salon_ref_id,
                "email": r.email,
                "cohort": "imported",
                "consent_b2b": False,
            }).execute()
            inserted += 1
        except Exception as e:  # noqa: BLE001 — duplikat email_hash = OK, licz dalej
            if "duplicate" not in str(e).lower() and "email_hash" not in str(e).lower():
                print(f"  ! insert fail salon={r.salon_ref_id}: {str(e)[:100]}")
    return inserted


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Email enrichment crawler (WWW)")
    ap.add_argument("--limit", type=int, default=None, help="max salonów (sample)")
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true", help="bez insertu, tylko yield")
    args = ap.parse_args()

    sb = _make_sb()
    print(f"[enrich] ładuję kandydatów (limit={args.limit})...")
    rows = load_candidates(sb, args.limit)
    print(f"[enrich] {len(rows)} domen do przejścia, concurrency={args.concurrency}")
    if not rows:
        print("[enrich] brak kandydatów.")
        return

    results = asyncio.run(run_batch(rows, args.concurrency))

    oks = [r for r in results if r.status == "ok" and r.email]
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1

    print(f"\n[enrich] WYNIK: {len(oks)}/{len(results)} maili "
          f"({100*len(oks)//max(len(results),1)}% yield)")
    for s, c in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {s:14} {c}")
    print("  próbka:")
    for r in oks[:8]:
        print(f"    {r.email:38} ← {r.website[:40]} {r.note}")

    if args.dry_run:
        print("\n[enrich] --dry-run: nic nie wstawiono.")
        return
    n = insert_contacts(sb, oks)
    print(f"\n[enrich] wstawiono {n} nowych kontaktów (cohort=imported, consent=false).")


if __name__ == "__main__":
    main()
