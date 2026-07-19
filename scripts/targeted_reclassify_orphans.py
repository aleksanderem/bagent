#!/usr/bin/env python3
"""Targeted reclassify dla orphan services post-strip 2026-05-21.

Wybiera DOKŁADNIE service_ids z classification_strip_audit_2026_05_21 które
obecnie nie mają żadnej klasyfikacji (orphan po DELETE). Classify deterministic
only (use_llm=False).
"""
from __future__ import annotations
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from services.supabase import SupabaseService
from services.method_classifier import MethodClassifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("targeted_reclassify")


async def main():
    sb = SupabaseService()

    # Get orphan service IDs (in strip audit but no current classification)
    res = sb.client.rpc("execute_sql", {}).execute() if False else None  # Not used; raw query below
    # Use direct query — via PostgREST list operation
    orphan_res = sb.client.table("classification_strip_audit_2026_05_21") \
        .select("service_id") \
        .execute()
    candidate_ids = list({int(r["service_id"]) for r in (orphan_res.data or [])})
    logger.info("Total strip-affected services: %d", len(candidate_ids))

    # Filter: only those with ZERO current classifications
    BATCH = 500
    orphan_ids: list[int] = []
    for i in range(0, len(candidate_ids), BATCH):
        chunk = candidate_ids[i:i + BATCH]
        existing = sb.client.table("service_method_classification") \
            .select("service_id") \
            .in_("service_id", chunk) \
            .execute()
        classified_set = {int(r["service_id"]) for r in (existing.data or [])}
        orphan_ids.extend(sid for sid in chunk if sid not in classified_set)

    logger.info("Orphan services (zero classifications): %d", len(orphan_ids))

    if not orphan_ids:
        logger.info("No orphans — nothing to do")
        return 0

    # Fetch service rows (name + embedding + category)
    services: list[dict] = []
    for i in range(0, len(orphan_ids), BATCH):
        chunk = orphan_ids[i:i + BATCH]
        res = sb.client.table("salon_scrape_services") \
            .select("id, name, name_embedding, category_name") \
            .in_("id", chunk) \
            .execute()
        services.extend(res.data or [])
    logger.info("Fetched %d service rows", len(services))

    # Init classifier
    classifier = MethodClassifier(supabase=sb, llm_client=None)
    await classifier.warmup()
    logger.info("Classifier ready — %d methods in dictionary", len(classifier._methods))

    # Classify
    payload = [
        {"id": int(s["id"]), "name": s["name"], "name_embedding": s.get("name_embedding"),
         "category_name": s.get("category_name")}
        for s in services
    ]
    results = await classifier.classify_services(payload, use_llm=False, max_concurrent=10)

    classified = sum(1 for matches in results.values() if matches)
    unclassified = len(results) - classified
    logger.info("Classified: %d, still unclassified: %d", classified, unclassified)

    # Flush
    await classifier.flush_cache_writes()
    logger.info("DONE — flushed classifications")
    return 0


if __name__ == "__main__":
    asyncio.run(main())
