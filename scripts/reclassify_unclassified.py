#!/usr/bin/env python3
"""Re-classify unclassified services using DETERMINISTIC paths only.

Cel: złapać services które Faza G pominęła z 2 powodów:
  1. Słownik rósł W TRAKCIE Faza G — services przetwarzane wcześnie nie
     widziały aliases dodanych później (np. nowe brand entries z LLM
     inference w połowie run'a)
  2. Nowe scrapy/services po Fazie G — fresh data has no classification yet

Klasyfikator cascade jest WYŁĄCZONY DLA LLM (use_llm=False), więc działają
TYLKO deterministic ścieżki:
  - alias_exact (string match aliases — free, fast)
  - embedding_ann (pgvector cosine — free, embedding już mamy)

Cokolwiek nie złapie deterministic = zostaje unclassified (bez kosztu).

Run:
  --batch-size 500    # services per batch
  --max-services N    # limit do testowania
  --dry-run           # tylko classify + report, no DB write (NOTE: cache
                       write IS still triggered via _queue_cache_write —
                       set skip_db_cache=True i don't await flush)

FAIL-LOUD on classifier errors.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from services.supabase import SupabaseService  # noqa: E402
from services.method_classifier import MethodClassifier  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("reclassify")


async def fetch_unclassified_batch(sb: SupabaseService, offset: int, limit: int) -> list[dict]:
    """Returns up to `limit` services without classification entry.
    Uses raw RPC-like query via httpx (PostgREST doesn't expose JOIN-NOT-EXISTS
    natively, but we can use NOT IN with a subquery via RPC OR fetch all
    classified IDs then exclude). Simpler: dedicated RPC."""
    # Use a custom inline SQL via RPC. If no RPC available, we'll fall back to
    # straightforward fetch via paginated salon_scrape_services + per-id check.
    # Most efficient: write a SQL helper RPC. For now use a heuristic — fetch
    # active chain-head services in chunks, then filter Python-side.
    res = sb.client.rpc("fn_unclassified_services_batch", {
        "p_offset": offset,
        "p_limit": limit,
    }).execute()
    return res.data or []


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-services", type=int, default=None, help="Limit dla testowania")
    parser.add_argument("--dry-run", action="store_true", help="Classify ale nie pisz do DB")
    parser.add_argument("--max-concurrent", type=int, default=10)
    args = parser.parse_args()

    sb = SupabaseService()

    # Init classifier
    classifier = MethodClassifier(supabase=sb, llm_client=None)
    logger.info("Warming up classifier (loading dictionary into memory)...")
    await classifier.warmup()
    logger.info("Classifier ready — %d methods in dictionary", len(classifier._methods))

    total_processed = 0
    total_classified_alias = 0
    total_classified_ann = 0
    total_unclassified = 0
    offset = 0

    while True:
        batch = await fetch_unclassified_batch(sb, offset, args.batch_size)
        if not batch:
            logger.info("No more unclassified services to process")
            break

        # Classify each via deterministic-only path
        services_for_classify = [
            {
                "id": int(svc["service_id"]),
                "name": svc["name"],
                "name_embedding": svc.get("name_embedding"),
                "category_name": svc.get("category_name"),
            }
            for svc in batch
        ]
        results = await classifier.classify_services(
            services_for_classify,
            use_llm=False,
            max_concurrent=args.max_concurrent,
        )

        for sid, matches in results.items():
            total_processed += 1
            if not matches:
                total_unclassified += 1
                continue
            # Check which classifier path matched (highest confidence first)
            top = matches[0]
            classifier_name = (
                top.classifier if hasattr(top, "classifier") else "alias_exact"
            )
            if classifier_name == "alias_exact":
                total_classified_alias += 1
            elif classifier_name == "embedding_ann":
                total_classified_ann += 1

        # Flush classifier cache writes (queues INSERT to service_method_classification)
        if not args.dry_run:
            await classifier.flush_cache_writes()

        if total_processed % 5000 == 0 or len(batch) < args.batch_size:
            logger.info(
                "Progress: %d processed, %d alias, %d ann, %d still unclassified",
                total_processed, total_classified_alias, total_classified_ann, total_unclassified,
            )

        if args.max_services and total_processed >= args.max_services:
            logger.info("Reached max-services limit (%d) — stopping", args.max_services)
            break

        if len(batch) < args.batch_size:
            break
        offset += args.batch_size

    logger.info(
        "DONE — %d processed | %d aliases match | %d embedding match | %d still unclassified (no LLM cost)",
        total_processed, total_classified_alias, total_classified_ann, total_unclassified,
    )
    return 0


if __name__ == "__main__":
    asyncio.run(main())
