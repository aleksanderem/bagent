"""POC test for hidden_service_inference module.

Runs inference for known Beauty4ever brand-only services (Thunder,
Light&Bright, Red Touch, Modelka-ONDA, Plexr) and prints results
to verify quality.

Usage:
    cd bagent && uv run python scripts/test_hidden_service_inference.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from config import settings
from services.hidden_service_inference import (
    GeminiLLMClient,
    infer_hidden_service_taxonomy,
)
from services.supabase import SupabaseService


# Beauty4ever known USP services — POC test cases
TEST_SERVICE_IDS = [
    2178142,  # Thunder - Całe ciało 1 zabieg → expected: Depilacja ciała (637)
    2178192,  # Light&Bright Szyja - 1 zabieg → expected: Fotoodmładzanie (347) or Zabieg laserowy
    2178232,  # 3x Red Touch - Cała Twarz → expected: Zabieg laserowy or Fotoodmładzanie / Usuwanie zmarszczek
    2178363,  # Plexr - niechirurgiczny lifting powiek → expected: Niechirurgiczny lifting twarzy (715) or Lifting (699)
    2178402,  # Modelka - ONDA → expected: Zabiegi na ciało i modelowanie sylwetki (513) / Usuwanie cellulitu (348)
    2178138,  # EMBODY - budowanie mięśni → expected: Trening EMS (397) or Zabiegi na ciało
    2178141,  # X-Wave - fala uderzeniowa → expected: Fala uderzeniowa (775)
]


async def main() -> int:
    if not settings.supabase_url or not settings.supabase_service_key:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 1
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        print("Missing OPENAI_API_KEY", file=sys.stderr)
        return 1

    supabase = SupabaseService()
    llm = GeminiLLMClient(api_key=openai_key, model="gpt-4o-mini", provider="openai")

    # Fetch the test services with their embeddings
    def _fetch() -> list[dict]:
        result = (
            supabase.client.table("salon_scrape_services")
            .select("id, name, description, name_embedding")
            .in_("id", TEST_SERVICE_IDS)
            .execute()
        )
        return result.data or []

    services = await asyncio.to_thread(_fetch)
    print(f"Fetched {len(services)} test services\n")

    print(f"{'='*120}")
    for svc in services:
        print(f"\n--- Service id={svc['id']}: {svc['name']!r} ---")
        desc_preview = (svc.get("description") or "")[:120].replace("\n", " ")
        print(f"    desc: {desc_preview}...")

        # The embedding from Supabase comes as a string "[0.1, 0.2, ...]" — parse it
        emb = svc.get("name_embedding")
        if isinstance(emb, str):
            # pgvector serializes as "[0.1,0.2,...]"
            import json
            try:
                emb = json.loads(emb)
            except json.JSONDecodeError:
                emb = None
        svc["name_embedding"] = emb

        result = await infer_hidden_service_taxonomy(svc, supabase, llm)
        print(
            f"    → tid={result['inferred_tid']} "
            f"name={result['inferred_canonical_name']!r} "
            f"parent={result['inferred_parent_name']!r}"
        )
        print(
            f"      confidence={result['confidence']:.2f} method={result['method']} "
            f"candidates={result['candidate_count']}"
        )
        print(f"      reasoning: {result['reasoning']}")

    print(f"\n{'='*120}\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
