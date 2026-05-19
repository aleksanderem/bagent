#!/usr/bin/env python3
"""Smoke test for method_classifier — runs full cascade against 14
problem service names from audit 34 (Beauty4ever PRO XN row).

Usage from bagent/:
    .venv/bin/python scripts/test_method_classifier.py
"""
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

# Service names from audit 34 row 6056 (PRO XN - Acne Rescue Treatment
# pool). Plus subject service itself.
TEST_NAMES = [
    "PRO XN - Acne Rescue Treatment (twarz + szyja)",
    "ProXN acne rescue treatment- zabieg na trądzik",
    "PRO XN + laktoferyna + dermapen",
    "ProXN  zabieg I stopnia- Twarz + szyja GLOW",
    "ProXN zabieg II stopnia - Twarz + szyja",
    "ProXN zabieg III stopnia+ Dermapen 4.0",
    "Zabieg Pielęgnacyjny PRO XN + Dermapen",
    "DermaClear Twarz + szyja",
    "DermaClear z PRX twarz",
    "DermaClear z PRX twarz + szyja",
    "DermaClear z PRX twarz + szyja + dekolt",
    "Dermalogica PRO CLEAR ( zabieg przeciwtrądzikowy, oczyszczający )",
    "Eksfoliacja kwasami (Mesoestetic) Twarz + Szyja",
    "Leczenie trądziku na twarzy, lub plecach RESUR FX",
    # Plus a couple unrelated services to check non-pollution
    "BOTOKS jedna okolica",
    "Restylane Kysse 1 ml",
    "Manicure hybrydowy klasyczny",
    "Strzyżenie damskie",
    "Masaż klasyczny relaksacyjny 60 min",
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

    print(f"\n{'Service':<60s} {'Method':<30s} {'Conf':>5s}  {'Classifier':<15s} {'Category':<22s} {'Brand':<20s}")
    print("─" * 160)

    for name in TEST_NAMES:
        # Look up the service ID + embedding from prod (just for testing)
        try:
            res = (
                supabase.client.table("salon_scrape_services")
                .select("id, name, name_embedding")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            svc = (res.data or [{}])[0]
        except Exception as e:
            print(f"  [DB error for {name!r}: {e}]")
            continue

        if not svc.get("id"):
            print(f"  [no service in DB for {name!r}, classifying name-only]")
            svc = {"id": -1, "name": name}

        # Use --no-cache: bypass DB cache so we see the live cascade
        # decision (cache may be empty anyway since this is first run).
        match = await classifier.classify_service(
            service_id=int(svc["id"]) if svc.get("id") else -1,
            service_name=svc.get("name") or name,
            name_embedding=svc.get("name_embedding"),
            use_llm=True,
        )

        if match is None:
            print(f"{name[:60]:<60s} {'(unclassified)':<30s} {'-':>5s}  {'-':<15s} {'-':<22s} {'-':<20s}")
        else:
            print(
                f"{name[:60]:<60s} "
                f"{match.canonical_name[:30]:<30s} "
                f"{match.confidence:>5.2f}  "
                f"{match.classifier:<15s} "
                f"{match.category[:22]:<22s} "
                f"{(match.brand_family or '')[:20]:<20s}"
            )

    await classifier.flush_cache_writes()
    print(f"\nCache writes flushed.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
