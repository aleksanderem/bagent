"""Shared fixtures for E2E pipeline tests.

Provides:
    - ``e2e_db_client``: read-only Supabase client (HTTP/1.1 hardened)
    - ``salon_pool``: stratified random sample of salons from prod
    - ``scraped_data_for(salon_id)``: rebuild ScrapedData from DB rows
    - ``capture_progress``: progress callback that records messages

Random selection is seeded via env var ``E2E_SEED`` (default: time-based)
or pytest ``--e2e-seed=N``. Same seed → same salon ids → reproducible runs.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from typing import Any, Iterator

import pytest

from config import settings
from services.sb_client import make_supabase_client


# ---------------------------------------------------------------------------
# Pytest CLI options
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--e2e-seed", action="store", default=None,
        help="Random seed for salon sampling (default: env E2E_SEED or random)",
    )
    parser.addoption(
        "--e2e-salons", action="store", default="3",
        help="How many salons to sample per pipeline test (default: 3)",
    )
    parser.addoption(
        "--e2e-min-services", action="store", default="5",
        help="Minimum services per sampled salon (default: 5)",
    )
    parser.addoption(
        "--e2e-max-services", action="store", default="40",
        help="Maximum services per sampled salon (default: 40 — keeps "
             "tests fast; full audit on 100+ services takes minutes)",
    )


# ---------------------------------------------------------------------------
# DB client (read-only)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def e2e_db_client():
    """Session-scoped Supabase client. HTTP/1.1 hardened — see
    services/sb_client.py for why."""
    if not settings.supabase_url or not settings.supabase_service_key:
        pytest.skip("E2E tests require SUPABASE_URL + SUPABASE_SERVICE_KEY")
    return make_supabase_client(
        settings.supabase_url, settings.supabase_service_key
    )


# ---------------------------------------------------------------------------
# Random sampling
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def e2e_seed(request: pytest.FixtureRequest) -> int:
    seed_str = request.config.getoption("--e2e-seed") or os.environ.get("E2E_SEED")
    if seed_str:
        seed = int(seed_str)
    else:
        seed = random.SystemRandom().randint(0, 2**31 - 1)
    print(f"\n[e2e] seed={seed} (set E2E_SEED={seed} or --e2e-seed={seed} to reproduce)")
    return seed


@pytest.fixture(scope="session")
def salon_pool(e2e_db_client, e2e_seed: int, request: pytest.FixtureRequest) -> list[dict[str, Any]]:
    """Stratified random sample of salon scrapes.

    Strategy: pull salons from each (primary_category_id) bucket, weighted
    by bucket size, with services_count in range. Goal: every test run
    sees a different cross-section of categories so we don't accidentally
    over-fit invariants to one type of salon.
    """
    rng = random.Random(e2e_seed)
    n = int(request.config.getoption("--e2e-salons"))
    min_svc = int(request.config.getoption("--e2e-min-services"))
    max_svc = int(request.config.getoption("--e2e-max-services"))

    # Pull a wide candidate pool: latest scrape per salon, with services
    # count in range, scraped recently (so data is fresh). Schema notes:
    # - `salon_ref_id` FK → salons.id (the canonical identity in our DB)
    # - `booksy_id` is Booksy's external ID, used when salon_ref_id is NULL
    # - There's no salon_scrape_categories table — categories live inline
    #   on salon_scrape_services (category_name + category_sort_order).
    rows = (
        e2e_db_client.table("salon_scrapes")
        .select(
            "id, salon_ref_id, booksy_id, salon_name, salon_address, "
            "salon_city, primary_category_id, total_services, scraped_at"
        )
        .gte("total_services", min_svc)
        .lte("total_services", max_svc)
        .order("scraped_at", desc=True)
        .limit(2000)
        .execute()
        .data
        or []
    )
    if not rows:
        pytest.skip("No salons matched E2E criteria — check Supabase or relax services bounds")

    # Synthesize a stable salon_id per row: salon_ref_id when present,
    # else "booksy:{booksy_id}" so de-dupe works for orphan scrapes too.
    for r in rows:
        r["salon_id"] = r.get("salon_ref_id") or f"booksy:{r.get('booksy_id')}"

    # De-dupe to latest scrape per salon
    by_salon: dict[Any, dict[str, Any]] = {}
    for r in rows:
        sid = r["salon_id"]
        if sid not in by_salon:
            by_salon[sid] = r
    candidates = list(by_salon.values())

    # Stratify by primary_category_id (bucket → list)
    buckets: dict[Any, list[dict[str, Any]]] = {}
    for c in candidates:
        buckets.setdefault(c.get("primary_category_id"), []).append(c)

    picked: list[dict[str, Any]] = []
    bucket_keys = list(buckets.keys())
    rng.shuffle(bucket_keys)
    while len(picked) < n and bucket_keys:
        for k in list(bucket_keys):
            if not buckets[k]:
                bucket_keys.remove(k)
                continue
            picked.append(buckets[k].pop(rng.randrange(len(buckets[k]))))
            if len(picked) >= n:
                break

    print(f"[e2e] sampled {len(picked)} salons across {len(buckets)} category buckets")
    for p in picked:
        print(f"  - salon_id={p.get('salon_id')} cat={p.get('primary_category_name')!r} "
              f"services={p.get('total_services')} name={p.get('salon_name')!r}")
    return picked


# ---------------------------------------------------------------------------
# ScrapedData rebuild from DB
# ---------------------------------------------------------------------------

@dataclass
class _Variant:
    label: str
    price: str
    duration: str | None = None


@dataclass
class _Service:
    name: str
    price: str
    duration: str | None = None
    description: str | None = None
    imageUrl: str | None = None
    variants: list[_Variant] | None = None


@dataclass
class _Category:
    name: str
    services: list[_Service]


@dataclass
class _ScrapedData:
    """Duck-typed match for ``models.scraped_data.ScrapedData``.

    Pipelines accept any object with these attrs — we don't need the
    real pydantic model in tests, and avoiding the import lets these
    tests run even if model schemas drift slightly between deploys.

    Attribute set verified against:
      - pipelines/report.py — reads salonCity for benchmark fetch
      - pipelines/cennik.py — reads salonName, totalServices, categories
      - pipelines/helpers.py — reads totalServices, categories
    """
    salonName: str
    salonAddress: str
    salonLogoUrl: str | None
    totalServices: int
    categories: list[_Category]
    salonCity: str | None = None
    primaryCategoryId: int | None = None
    primaryCategoryName: str | None = None
    salonId: int | None = None


@pytest.fixture(scope="session")
def scraped_data_for(e2e_db_client):
    """Factory: salon row → ScrapedData object.

    Loads salon_scrape_services + salon_scrape_categories and assembles
    a ScrapedData object suitable for direct pipeline invocation."""

    def _build(salon_row: dict[str, Any]) -> _ScrapedData:
        scrape_id = salon_row["id"]
        # Categories live inline on salon_scrape_services — there's no
        # separate salon_scrape_categories table. We derive the
        # ordered category list by grouping services on category_name +
        # min(category_sort_order).
        svcs = (
            e2e_db_client.table("salon_scrape_services")
            .select(
                "id, category_id, category_name, category_sort_order, "
                "name, price, duration_minutes, duration_seconds, "
                "description, image_url"
            )
            .eq("scrape_id", scrape_id)
            .order("category_sort_order")
            .execute()
            .data
            or []
        )

        # Build (cat_name, sort_order) → list[Service]
        cat_meta: dict[str, int] = {}  # name → min sort_order seen
        cat_to_services: dict[str, list[_Service]] = {}
        for s in svcs:
            cname = s.get("category_name") or "Pozostałe"
            sort_order = s.get("category_sort_order")
            if isinstance(sort_order, int):
                cat_meta[cname] = min(cat_meta.get(cname, 10**9), sort_order)
            else:
                cat_meta.setdefault(cname, 10**9)
            cat_to_services.setdefault(cname, [])
            # Normalise duration: prefer duration_minutes, fall back to seconds
            dur_min = s.get("duration_minutes")
            duration_str: str | None
            if isinstance(dur_min, int) and dur_min > 0:
                duration_str = f"{dur_min} min"
            elif isinstance(s.get("duration_seconds"), int) and s["duration_seconds"] > 0:
                duration_str = f"{s['duration_seconds'] // 60} min"
            else:
                duration_str = None
            cat_to_services[cname].append(
                _Service(
                    name=s.get("name") or "",
                    price=s.get("price") or "",
                    duration=duration_str,
                    description=s.get("description"),
                    imageUrl=s.get("image_url"),
                    variants=None,  # variants live in a separate table; not needed for invariant tests
                )
            )

        # Order categories by their min sort_order
        ordered_cat_names = sorted(cat_meta.keys(), key=lambda n: cat_meta[n])
        categories = [
            _Category(name=cname, services=cat_to_services[cname])
            for cname in ordered_cat_names
        ]

        return _ScrapedData(
            salonName=salon_row.get("salon_name") or "Unknown",
            salonAddress=salon_row.get("salon_address") or "",
            salonLogoUrl=None,
            totalServices=(
                salon_row.get("total_services")
                or sum(len(c.services) for c in categories)
            ),
            categories=categories,
            salonCity=salon_row.get("salon_city"),
            primaryCategoryId=salon_row.get("primary_category_id"),
            primaryCategoryName=None,  # not stored on salon_scrapes; resolved via JOIN if needed
            salonId=salon_row.get("salon_id"),
        )

    return _build


# ---------------------------------------------------------------------------
# Progress capture
# ---------------------------------------------------------------------------

@pytest.fixture
def capture_progress():
    """Returns (callback, log) where log is a list[(int, str)] of
    progress events the pipeline emitted. Use to assert pipeline
    actually emitted expected progress checkpoints."""
    log: list[tuple[int, str]] = []

    async def cb(progress: int, message: str) -> None:
        log.append((progress, message))

    return cb, log


# ---------------------------------------------------------------------------
# Supabase write shim (so pipelines don't pollute prod)
# ---------------------------------------------------------------------------

@pytest.fixture
def disable_supabase_writes(monkeypatch):
    """Monkeypatch SupabaseService methods that WRITE so the pipeline
    runs without touching prod tables. The pipelines call SupabaseService
    for benchmark fetches (read OK) and report saves (write — must be
    blocked).

    Usage: include this fixture in any e2e test that runs a pipeline
    end-to-end against real Supabase reads."""
    from services.supabase import SupabaseService

    saved_payloads: list[dict[str, Any]] = []

    def _capture_save(*args, **kwargs):
        # Last positional is usually the report dict
        for a in args:
            if isinstance(a, dict):
                saved_payloads.append(a)
        for v in kwargs.values():
            if isinstance(v, dict):
                saved_payloads.append(v)
        return None

    # Block known write methods. Add more if pipelines start calling
    # additional writers.
    write_methods = [
        "save_audit_report",
        "save_optimized_pricelist",
        "save_competitor_report",
        "upsert_salon",
    ]
    for m in write_methods:
        if hasattr(SupabaseService, m):
            monkeypatch.setattr(SupabaseService, m, _capture_save)

    return saved_payloads
