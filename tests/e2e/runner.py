"""CLI runner for ad-hoc E2E pipeline runs.

Examples:
    python -m tests.e2e.runner --pipeline audit --salons 5
    python -m tests.e2e.runner --pipeline cennik --salons 3 --seed 42
    python -m tests.e2e.runner --pipeline all --salons 2 --report e2e-report.json

Designed for developer iteration + CI dashboards. Writes a structured
JSON report so CI can graph pass/fail rate per pipeline over time.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Make sure bagent root is on path when invoked as `python -m tests.e2e.runner`
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import settings  # noqa: E402
from services.sb_client import make_supabase_client  # noqa: E402

from tests.e2e.conftest import (  # noqa: E402
    _ScrapedData, _Category, _Service, _Variant,
)
from tests.e2e.invariants import (  # noqa: E402
    check_audit_report,
    check_cennik_output,
    InvariantReport,
)


PIPELINES = ("audit", "cennik", "all")


@dataclass
class PipelineRunResult:
    pipeline: str
    salon_id: Any
    salon_name: str
    duration_sec: float
    error: str | None = None
    invariants_passed: list[str] = field(default_factory=list)
    invariants_failed: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and not self.invariants_failed


def pick_salons(client, n: int, seed: int, min_svc: int, max_svc: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    rows = (
        client.table("salon_scrapes")
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
        return []

    for r in rows:
        r["salon_id"] = r.get("salon_ref_id") or f"booksy:{r.get('booksy_id')}"

    by_salon: dict[Any, dict[str, Any]] = {}
    for r in rows:
        sid = r["salon_id"]
        if sid not in by_salon:
            by_salon[sid] = r
    candidates = list(by_salon.values())

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
    return picked


def build_scraped(client, salon_row: dict[str, Any]) -> _ScrapedData:
    scrape_id = salon_row["id"]
    # Categories are inline on services (category_name + category_sort_order)
    svcs = (
        client.table("salon_scrape_services")
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
    cat_meta: dict[str, int] = {}
    cat_to_services: dict[str, list[_Service]] = {}
    for s in svcs:
        cname = s.get("category_name") or "Pozostałe"
        sort_order = s.get("category_sort_order")
        if isinstance(sort_order, int):
            cat_meta[cname] = min(cat_meta.get(cname, 10**9), sort_order)
        else:
            cat_meta.setdefault(cname, 10**9)
        cat_to_services.setdefault(cname, [])
        dur_min = s.get("duration_minutes")
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
                variants=None,
            )
        )
    ordered_cat_names = sorted(cat_meta.keys(), key=lambda n: cat_meta[n])
    categories = [
        _Category(name=cname, services=cat_to_services[cname])
        for cname in ordered_cat_names
    ]
    return _ScrapedData(
        salonName=salon_row.get("salon_name") or "Unknown",
        salonAddress=salon_row.get("salon_address") or "",
        salonLogoUrl=None,
        totalServices=salon_row.get("total_services") or sum(len(c.services) for c in categories),
        categories=categories,
        salonCity=salon_row.get("salon_city"),
        primaryCategoryId=salon_row.get("primary_category_id"),
        primaryCategoryName=None,
        salonId=salon_row.get("salon_id"),
    )


async def _noop_progress(progress: int, message: str) -> None:
    pass


async def run_audit(scraped: _ScrapedData, salon_id: Any, salon_name: str) -> PipelineRunResult:
    from pipelines.report import run_audit_pipeline

    t0 = time.time()
    result = PipelineRunResult(pipeline="audit", salon_id=salon_id, salon_name=salon_name, duration_sec=0)
    try:
        report = await run_audit_pipeline(
            scraped_data=scraped,
            audit_id=f"e2e-runner-{salon_id}",
            on_progress=_noop_progress,
        )
        rep = check_audit_report(report, scraped)
        result.invariants_passed = rep.passed
        result.invariants_failed = rep.failures
    except Exception as e:
        result.error = f"{type(e).__name__}: {e}"
    finally:
        result.duration_sec = round(time.time() - t0, 1)
    return result


async def run_cennik(scraped: _ScrapedData, salon_id: Any, salon_name: str) -> PipelineRunResult:
    """Cennik is tightly coupled to Supabase — `run_cennik_pipeline`
    only takes `audit_id` and loads scrape + audit_report + transformations
    from DB. In-process testing requires either:
      (a) seeding a synthetic audit_reports row (would write to prod), or
      (b) refactoring cennik to accept pre-loaded inputs (test seam), or
      (c) picking a real existing audit_id with a complete report

    For now we mark cennik as not_implemented and focus on audit. Add
    one of the above strategies in a follow-up if cennik regression
    coverage becomes a priority."""
    return PipelineRunResult(
        pipeline="cennik",
        salon_id=salon_id,
        salon_name=salon_name,
        duration_sec=0.0,
        error="not_implemented: cennik is DB-coupled — needs test seam refactor or real audit_id seed",
    )


async def main_async(args: argparse.Namespace) -> int:
    if not settings.supabase_url or not settings.supabase_service_key:
        print("ERROR: SUPABASE_URL + SUPABASE_SERVICE_KEY required", file=sys.stderr)
        return 2

    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2**31 - 1)
    print(f"[e2e-runner] seed={seed}")
    salons = pick_salons(client, args.salons, seed, args.min_services, args.max_services)
    if not salons:
        print("ERROR: no salons matched criteria", file=sys.stderr)
        return 3
    print(f"[e2e-runner] sampled {len(salons)} salons")
    for s in salons:
        print(f"  - salon_id={s.get('salon_id')} cat={s.get('primary_category_name')!r} "
              f"services={s.get('total_services')} {s.get('salon_name')!r}")

    pipelines_to_run: list[str]
    if args.pipeline == "all":
        pipelines_to_run = ["audit", "cennik"]
    else:
        pipelines_to_run = [args.pipeline]

    results: list[PipelineRunResult] = []
    for salon in salons:
        scraped = build_scraped(client, salon)
        salon_id = salon.get("salon_id")
        salon_name = salon.get("salon_name") or "Unknown"
        for pipeline in pipelines_to_run:
            print(f"\n[e2e-runner] running {pipeline} on salon {salon_id}…")
            if pipeline == "audit":
                r = await run_audit(scraped, salon_id, salon_name)
            elif pipeline == "cennik":
                r = await run_cennik(scraped, salon_id, salon_name)
            else:
                continue
            results.append(r)
            if r.ok:
                print(f"  ✓ {pipeline} salon={salon_id} {r.duration_sec}s "
                      f"invariants_passed={len(r.invariants_passed)}")
            else:
                print(f"  ✗ {pipeline} salon={salon_id} {r.duration_sec}s "
                      f"failed={len(r.invariants_failed)} error={r.error or '-'}")
                for f in r.invariants_failed[:5]:
                    print(f"      - {f}")

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.ok)
    failed = total - passed
    print(f"\n[e2e-runner] summary: {passed}/{total} OK, {failed} failed (seed={seed})")

    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({
            "seed": seed,
            "total": total,
            "passed": passed,
            "failed": failed,
            "results": [asdict(r) for r in results],
        }, indent=2))
        print(f"[e2e-runner] wrote report to {report_path}")

    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="tests.e2e.runner")
    parser.add_argument("--pipeline", choices=PIPELINES, default="audit")
    parser.add_argument("--salons", type=int, default=3)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--min-services", type=int, default=5)
    parser.add_argument("--max-services", type=int, default=40)
    parser.add_argument("--report", type=str, default=None,
                        help="Path to write JSON report (default: don't write)")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
