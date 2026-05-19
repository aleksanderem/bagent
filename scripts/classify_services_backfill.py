#!/usr/bin/env python3
"""Backfill service_method_classification for active chain-head services.

For each service runs MethodClassifier.classify_service which:
  - hits in-process cache / DB cache first
  - cascades through alias_exact (multi-match) → ANN → LLM
  - LLM may propose new treatment_methods entries which are INSERT'ed
    with source='llm_inferred' (with pre-INSERT dedup guard against
    aliases overlap with existing entries)

Iteration strategy:
  - Full run: paginate `salon_scrape_services` by id ASC where
    is_active=TRUE AND price_grosze IS NOT NULL AND scrape_id is a
    chain head. Chain head check is done via the `embedding_applied_at`
    column as a proxy (chain heads have embeddings populated; delta
    rows in mig 047+ do not). Fallback: per-batch join check.
  - Scoped (--audit-id / --salon-ids): resolve target scrape_ids first,
    then iterate via small `.in_()` chunks.

FAIL-LOUD principle: any DB / LLM / network error during fetch or
classify is logged WITH FULL CONTEXT and RE-RAISED. No silent skips.
Per-service classifier errors are also re-raised to surface root cause.
Resume after fix via --skip-classified.

Usage:
    .venv/bin/python scripts/classify_services_backfill.py --audit-id <id>
    .venv/bin/python scripts/classify_services_backfill.py \\
        --salon-ids 9401,1099 --skip-classified
    .venv/bin/python scripts/classify_services_backfill.py \\
        --skip-classified --concurrent 10 --flush-every 500
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import traceback
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


async def _resolve_audit_salon_ids(
    supabase: SupabaseService, audit_id: str,
) -> list[int]:
    """Get subject_salon_id + all competitor salon_ref_ids from the
    existing competitor_report for this audit. Raises if no report."""
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
    return salon_ids


def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


async def _fetch_scoped_services(
    supabase: SupabaseService,
    salon_ids: list[int],
) -> list[dict]:
    """Scoped fetch — known salon_ref_ids. Resolves chain-head
    scrape_ids and then services. Small chunks (≤100) for URL limit
    safety. RAISES on any failure — no silent skip."""
    # Step 1: resolve chain head scrape ids per salon (chunked)
    scrape_ids: list[str] = []
    for chunk in _chunked(salon_ids, 100):
        res = (
            supabase.client.table("salon_scrapes")
            .select("id, salon_ref_id")
            .in_("salon_ref_id", chunk)
            .eq("is_chain_head", True)
            .execute()
        )
        scrape_ids.extend([r["id"] for r in (res.data or [])])
    logger.info(
        "Chain head scrapes for %d salons: %d",
        len(salon_ids), len(scrape_ids),
    )
    if not scrape_ids:
        return []

    # Step 2: services per scrape_id chunk
    all_services: list[dict] = []
    SVC_CHUNK = 50  # smaller — UUIDs are long
    for chunk in _chunked(scrape_ids, SVC_CHUNK):
        res = (
            supabase.client.table("salon_scrape_services")
            .select(
                "id, name, name_embedding, category_name, "
                "scrape_id, price_grosze"
            )
            .in_("scrape_id", chunk)
            .eq("is_active", True)
            .not_.is_("price_grosze", "null")
            .execute()
        )
        all_services.extend(res.data or [])
    logger.info(
        "Active services in scope: %d (across %d scrapes)",
        len(all_services), len(scrape_ids),
    )
    return all_services


async def _iterate_full_services(
    supabase: SupabaseService,
    *,
    skip_classified: bool,
    page_size: int = 1000,
    last_id_start: int | None = None,
):
    """Async generator — paginate salon_scrape_services by id ASC.
    Yields one batch (list[dict]) at a time. Uses
    `embedding_applied_at IS NOT NULL` as chain-head proxy (chain heads
    have embeddings, delta rows don't in current scheme).

    --skip-classified: per-batch deduplication via LEFT JOIN style
    lookup in service_method_classification.

    Raises on any DB error (fail-loud)."""
    last_id = last_id_start or 0
    while True:
        q = (
            supabase.client.table("salon_scrape_services")
            .select(
                "id, name, name_embedding, category_name, "
                "scrape_id, price_grosze, embedding_applied_at"
            )
            .gt("id", last_id)
            .eq("is_active", True)
            .not_.is_("price_grosze", "null")
            .not_.is_("embedding_applied_at", "null")
            .order("id")
            .limit(page_size)
        )
        res = q.execute()
        page = res.data or []
        if not page:
            return
        last_id = int(page[-1]["id"])

        if skip_classified:
            sids = [int(s["id"]) for s in page]
            classified: set[int] = set()
            for chunk in _chunked(sids, 200):
                cls_res = (
                    supabase.client.table("service_method_classification")
                    .select("service_id")
                    .in_("service_id", chunk)
                    .execute()
                )
                classified.update(int(r["service_id"]) for r in (cls_res.data or []))
            page = [s for s in page if int(s["id"]) not in classified]

        if page:
            yield page, last_id


async def _process_batch(
    classifier: MethodClassifier,
    batch: list[dict],
    *,
    use_llm: bool,
    concurrent: int,
) -> tuple[int, int, Counter]:
    """Classify a batch concurrently. Per-service errors are re-raised
    (no graceful skip — caller decides how to recover). Returns
    (n_with_methods, n_multi_method, classifier_path_counter)."""
    sem = asyncio.Semaphore(concurrent)
    classifier_stats: Counter = Counter()
    with_methods = 0
    multi_methods = 0

    async def _one(svc: dict):
        async with sem:
            sid = int(svc["id"])
            try:
                return sid, await classifier.classify_service(
                    service_id=sid,
                    service_name=svc.get("name") or "",
                    name_embedding=svc.get("name_embedding"),
                    category_hint=svc.get("category_name"),
                    use_llm=use_llm,
                )
            except Exception as e:
                logger.error(
                    "classify_service FAILED svc_id=%s name=%r: %s\n%s",
                    sid, (svc.get("name") or "")[:80], e,
                    traceback.format_exc(),
                )
                raise

    results = await asyncio.gather(*[_one(s) for s in batch])
    for _sid, matches in results:
        if matches:
            with_methods += 1
            if len(matches) > 1:
                multi_methods += 1
            for m in matches:
                classifier_stats[m.classifier] += 1
    return with_methods, multi_methods, classifier_stats


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-id", help="Limit scope to subject + competitors of this audit")
    parser.add_argument("--salon-ids", help="Comma-separated salon_ref_ids")
    parser.add_argument("--limit", type=int, default=None, help="Max services to process (test runs)")
    parser.add_argument("--skip-classified", action="store_true",
                        help="Skip services that already have any classification row")
    parser.add_argument("--concurrent", type=int, default=8)
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM fallback (alias_exact + ANN only)")
    parser.add_argument("--flush-every", type=int, default=500,
                        help="Flush cache writes every N services")
    parser.add_argument("--start-id", type=int, default=0,
                        help="Resume full-scan from this salon_scrape_services.id (exclusive)")
    args = parser.parse_args()

    salon_ids = None
    if args.salon_ids:
        salon_ids = [int(x.strip()) for x in args.salon_ids.split(",") if x.strip()]

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("OPENAI_API_KEY not set — cannot run")
        return 1

    supabase = SupabaseService()
    openai_client = OpenAI(api_key=openai_key)
    classifier = MethodClassifier(supabase=supabase, llm_client=openai_client)
    await classifier.warmup()
    initial_dict_size = len(classifier._methods)

    overall_stats: Counter = Counter()
    overall_with = 0
    overall_multi = 0
    overall_processed = 0

    # Scoped run — assemble service list up front
    if args.audit_id or salon_ids:
        if args.audit_id:
            salon_ids = await _resolve_audit_salon_ids(supabase, args.audit_id)
        services = await _fetch_scoped_services(supabase, salon_ids or [])
        if args.skip_classified:
            sids = [int(s["id"]) for s in services]
            classified: set[int] = set()
            for chunk in _chunked(sids, 200):
                res = (
                    supabase.client.table("service_method_classification")
                    .select("service_id")
                    .in_("service_id", chunk)
                    .execute()
                )
                classified.update(int(r["service_id"]) for r in (res.data or []))
            before = len(services)
            services = [s for s in services if int(s["id"]) not in classified]
            logger.info(
                "skip_classified: %d → %d (dropped %d already-classified)",
                before, len(services), before - len(services),
            )
        if args.limit:
            services = services[: args.limit]
            logger.info("Limited to %d services", len(services))
        if not services:
            logger.info("Nothing to classify, exiting.")
            return 0

        for i, batch in enumerate(_chunked(services, args.flush_every)):
            with_m, multi_m, stats = await _process_batch(
                classifier, batch,
                use_llm=not args.no_llm,
                concurrent=args.concurrent,
            )
            overall_with += with_m
            overall_multi += multi_m
            overall_stats.update(stats)
            overall_processed += len(batch)
            flushed = await classifier.flush_cache_writes()
            logger.info(
                "Batch %d (%d svcs) done. processed=%d/%d flushed=%d "
                "with_methods=%d multi=%d dict_size=%d (Δ +%d)",
                i + 1, len(batch), overall_processed, len(services), flushed,
                overall_with, overall_multi,
                len(classifier._methods),
                len(classifier._methods) - initial_dict_size,
            )

    # Full run — stream-iterate
    else:
        async for batch, last_id in _iterate_full_services(
            supabase,
            skip_classified=args.skip_classified,
            page_size=args.flush_every,
            last_id_start=args.start_id,
        ):
            if args.limit and overall_processed >= args.limit:
                break
            if args.limit:
                batch = batch[: args.limit - overall_processed]
            with_m, multi_m, stats = await _process_batch(
                classifier, batch,
                use_llm=not args.no_llm,
                concurrent=args.concurrent,
            )
            overall_with += with_m
            overall_multi += multi_m
            overall_stats.update(stats)
            overall_processed += len(batch)
            flushed = await classifier.flush_cache_writes()
            logger.info(
                "Batch (last_id=%d, %d svcs) done. processed=%d flushed=%d "
                "with_methods=%d multi=%d dict_size=%d (Δ +%d)",
                last_id, len(batch), overall_processed, flushed,
                overall_with, overall_multi,
                len(classifier._methods),
                len(classifier._methods) - initial_dict_size,
            )

    await classifier.flush_cache_writes()

    print("\n══════════════════ BACKFILL DONE ══════════════════")
    print(f"Services processed:       {overall_processed}")
    print(f"With ≥1 method:           {overall_with}")
    print(f"Multi-method (≥2):        {overall_multi}")
    print(f"Unclassified:             {overall_processed - overall_with}")
    print(f"Dictionary size before:   {initial_dict_size}")
    print(f"Dictionary size after:    {len(classifier._methods)}")
    print(f"LLM-extended entries:     {len(classifier._methods) - initial_dict_size}")
    print("\nClassifier path breakdown (cache rows written):")
    for c, n in overall_stats.most_common():
        print(f"  {c:<15s} {n:>6d}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
