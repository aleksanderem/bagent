#!/usr/bin/env python3
"""Backfill service_method_classification for all active chain-head
services (or a narrower scope via --audit-id or --salon-ids).

Iterates salon_scrape_services where:
  - scrape_id ∈ {chain-head scrapes}
  - is_active = TRUE
  - price_grosze IS NOT NULL  (skip free-form description rows)

For each service runs MethodClassifier.classify_service which:
  - hits in-process cache / DB cache first
  - cascades through alias_exact (multi-match) → ANN → LLM
  - LLM may propose new treatment_methods entries which are INSERT'ed
    with source='llm_inferred' before classifier returns

Resumable: --skip-classified flag drops services that already have
≥1 row in service_method_classification.

Usage (test runs):
    .venv/bin/python scripts/classify_services_backfill.py \\
        --audit-id j574zcqr4xb7pgfp46b865ajs185857v --limit 500
    .venv/bin/python scripts/classify_services_backfill.py \\
        --salon-ids 9401,1099,1102 --skip-classified

Usage (full run):
    .venv/bin/python scripts/classify_services_backfill.py \\
        --skip-classified --concurrent 10
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from openai import OpenAI  # noqa: E402

from services.method_classifier import MethodClassifier  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("classify_backfill")


async def _fetch_target_services(
    supabase: SupabaseService,
    *,
    audit_id: str | None,
    salon_ids: list[int] | None,
    limit: int | None,
    skip_classified: bool,
) -> list[dict]:
    """Build the list of service rows to classify. Scope determined by
    audit_id (= subject + competitors from competitor_reports), salon_ids,
    or all chain heads."""
    if audit_id:
        # Get subject_salon_id + all competitor salon_ref_ids from the
        # existing report (if any)
        rep_res = (
            supabase.client.table("competitor_reports")
            .select("id, subject_salon_id")
            .eq("convex_audit_id", audit_id)
            .limit(1)
            .execute()
        )
        if not rep_res.data:
            raise RuntimeError(f"No competitor_report for audit_id={audit_id}")
        report_id = int(rep_res.data[0]["id"])
        subject_salon_id = int(rep_res.data[0]["subject_salon_id"])
        cmp_res = (
            supabase.client.table("competitor_matches")
            .select("competitor_salon_id")
            .eq("report_id", report_id)
            .execute()
        )
        comp_ids = [int(r["competitor_salon_id"]) for r in (cmp_res.data or [])]
        salon_ids = list({subject_salon_id} | set(comp_ids))
        logger.info(
            "audit_id=%s → %d salons (subject + competitors)",
            audit_id, len(salon_ids),
        )

    # Resolve chain head scrape_ids for the target salons (or all if None)
    if salon_ids:
        sc_res = (
            supabase.client.table("salon_scrapes")
            .select("id, salon_ref_id")
            .in_("salon_ref_id", salon_ids)
            .eq("is_chain_head", True)
            .execute()
        )
    else:
        # Full run — paginate chain head scrapes
        all_scrapes: list[dict] = []
        page_size = 1000
        last_id: str | None = None
        while True:
            q = (
                supabase.client.table("salon_scrapes")
                .select("id, salon_ref_id")
                .eq("is_chain_head", True)
                .order("id")
                .limit(page_size)
            )
            if last_id:
                q = q.gt("id", last_id)
            page = q.execute().data or []
            if not page:
                break
            all_scrapes.extend(page)
            last_id = page[-1]["id"]
            if len(page) < page_size:
                break
        sc_res = type("_R", (), {"data": all_scrapes})()  # mimic res

    scrape_ids = [r["id"] for r in (sc_res.data or [])]
    logger.info("Chain head scrapes in scope: %d", len(scrape_ids))
    if not scrape_ids:
        return []

    # Now load services. Supabase REST .in_() has practical 1000-id limit
    # per call — chunk if needed.
    all_services: list[dict] = []
    CHUNK = 500
    for i in range(0, len(scrape_ids), CHUNK):
        chunk = scrape_ids[i : i + CHUNK]
        try:
            res = (
                supabase.client.table("salon_scrape_services")
                .select("id, name, name_embedding, category_name, scrape_id, price_grosze")
                .in_("scrape_id", chunk)
                .eq("is_active", True)
                .not_.is_("price_grosze", "null")
                .execute()
            )
            all_services.extend(res.data or [])
        except Exception as e:
            logger.warning("services fetch chunk %d-%d failed: %s", i, i+len(chunk), e)
    logger.info("Active services in scope (chain head, with price): %d", len(all_services))

    if skip_classified:
        # Drop services that already have classifications
        sids = [int(s["id"]) for s in all_services]
        classified: set[int] = set()
        for i in range(0, len(sids), CHUNK):
            chunk = sids[i : i + CHUNK]
            try:
                res = (
                    supabase.client.table("service_method_classification")
                    .select("service_id")
                    .in_("service_id", chunk)
                    .execute()
                )
                for r in (res.data or []):
                    classified.add(int(r["service_id"]))
            except Exception as e:
                logger.warning("classified-check chunk failed: %s", e)
        before = len(all_services)
        all_services = [s for s in all_services if int(s["id"]) not in classified]
        logger.info(
            "Dropped %d already-classified services (%d remain)",
            before - len(all_services), len(all_services),
        )

    if limit:
        all_services = all_services[:limit]
        logger.info("Limited to first %d services", len(all_services))

    return all_services


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-id", help="Limit scope to subject + competitors of this audit")
    parser.add_argument("--salon-ids", help="Comma-separated salon_ref_ids")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-classified", action="store_true",
                        help="Skip services that already have any classification row")
    parser.add_argument("--concurrent", type=int, default=8)
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM fallback (alias_exact + ANN only)")
    parser.add_argument("--flush-every", type=int, default=200,
                        help="Flush cache writes every N services")
    args = parser.parse_args()

    salon_ids = None
    if args.salon_ids:
        salon_ids = [int(x.strip()) for x in args.salon_ids.split(",") if x.strip()]

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("OPENAI_API_KEY not set")
        return 1

    supabase = SupabaseService()
    openai_client = OpenAI(api_key=openai_key)
    classifier = MethodClassifier(supabase=supabase, llm_client=openai_client)
    await classifier.warmup()
    initial_dict_size = len(classifier._methods)

    services = await _fetch_target_services(
        supabase,
        audit_id=args.audit_id,
        salon_ids=salon_ids,
        limit=args.limit,
        skip_classified=args.skip_classified,
    )
    if not services:
        logger.info("Nothing to classify, exiting.")
        return 0

    # Classify in batches with periodic cache flush
    classifier_stats: Counter = Counter()
    multi_match_count = 0
    unclassified = 0
    processed = 0
    batch_size = max(args.flush_every, 1)
    for i in range(0, len(services), batch_size):
        batch = services[i : i + batch_size]
        sem = asyncio.Semaphore(args.concurrent)

        async def _one(svc):
            async with sem:
                return int(svc["id"]), await classifier.classify_service(
                    service_id=int(svc["id"]),
                    service_name=svc.get("name") or "",
                    name_embedding=svc.get("name_embedding"),
                    category_hint=svc.get("category_name"),
                    use_llm=not args.no_llm,
                )

        results = await asyncio.gather(*[_one(s) for s in batch])
        for _sid, matches in results:
            if not matches:
                unclassified += 1
            elif len(matches) > 1:
                multi_match_count += 1
            for m in matches:
                classifier_stats[m.classifier] += 1
        processed += len(batch)
        flushed = await classifier.flush_cache_writes()
        logger.info(
            "Batch %d/%d done. processed=%d flushed=%d "
            "dict_size=%d (Δ +%d) multi=%d unclassified=%d",
            i + len(batch), len(services),
            processed, flushed,
            len(classifier._methods),
            len(classifier._methods) - initial_dict_size,
            multi_match_count, unclassified,
        )

    # Final flush (defensive — should be empty)
    await classifier.flush_cache_writes()

    print("\n══════════════════ BACKFILL DONE ══════════════════")
    print(f"Services processed:       {processed}")
    print(f"With ≥1 method:           {processed - unclassified}")
    print(f"Multi-method (≥2):        {multi_match_count}")
    print(f"Unclassified:             {unclassified}")
    print(f"Dictionary size before:   {initial_dict_size}")
    print(f"Dictionary size after:    {len(classifier._methods)}")
    print(f"LLM-extended entries:     {len(classifier._methods) - initial_dict_size}")
    print()
    print("Classifier path breakdown (cache rows written):")
    for c, n in classifier_stats.most_common():
        print(f"  {c:<15s} {n:>6d}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
