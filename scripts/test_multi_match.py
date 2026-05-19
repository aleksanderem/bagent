#!/usr/bin/env python3
"""Smoke test multi-match classifier (Faza 4 dictionary-first refactor).

Verify że classifier zwraca MULTIPLE methods dla combo services,
proposes_new dla nieznanych nazw (np. 'laktoferyna'), i że dictionary
auto-extends are persisted to treatment_methods."""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from openai import OpenAI  # noqa: E402

from services.method_classifier import MethodClassifier  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TEST_CASES = [
    # combo service — should yield 2 matches
    ("PRO XN + laktoferyna + dermapen", "combo"),
    ("ProXN zabieg III stopnia+ Dermapen 4.0", "combo"),
    ("Zabieg Pielęgnacyjny PRO XN + Dermapen", "combo"),
    # single method
    ("PRO XN - Acne Rescue Treatment (twarz + szyja)", "single"),
    ("BOTOKS jedna okolica", "single"),
    ("Restylane Kysse 1 ml", "single"),
    # synonym test (RF/radiofrekwencja)
    ("Radiofrekwencja Mikroigłowa - twarz", "synonym"),
    # unknown name — should trigger LLM proposal (dictionary extension)
    ("Mezo-stymulator NEAUVIA Hydro Deluxe Ultra 2.5ml", "unknown_brand"),
    # genuinely method-less
    ("Konsultacja kosmetologa 30 min", "no_method"),
    ("Voucher prezentowy 500 zł", "no_method"),
]


async def main() -> int:
    supabase = SupabaseService()
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        print("OPENAI_API_KEY not set", file=sys.stderr)
        return 1
    openai_client = OpenAI(api_key=openai_key)

    classifier = MethodClassifier(supabase=supabase, llm_client=openai_client)
    await classifier.warmup()

    print(f"\n{'Service':<70s} {'Expected':<12s} {'N':>2s}  {'Canonicals':<50s}  {'Classifiers':<40s}")
    print("─" * 200)

    for name, expected in TEST_CASES:
        try:
            res = (
                supabase.client.table("salon_scrape_services")
                .select("id, name_embedding")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            svc = (res.data or [{}])[0]
            sid = int(svc["id"]) if svc.get("id") else 999999990 + hash(name) % 9999
            emb = svc.get("name_embedding")
        except Exception:
            sid = 999999990 + hash(name) % 9999
            emb = None

        matches = await classifier.classify_service(
            service_id=sid,
            service_name=name,
            name_embedding=emb,
            use_llm=True,  # Always enable to test full cascade
        )

        canon = ",".join(m.canonical_name for m in matches) or "(none)"
        clsf = ",".join(m.classifier for m in matches) or "(none)"
        print(
            f"{name[:70]:<70s} "
            f"{expected:<12s} "
            f"{len(matches):>2d}  "
            f"{canon[:50]:<50s}  "
            f"{clsf[:40]:<40s}"
        )

    flushed = await classifier.flush_cache_writes()
    print(f"\nFlushed {flushed} cache rows. Dictionary size: {len(classifier._methods)}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
