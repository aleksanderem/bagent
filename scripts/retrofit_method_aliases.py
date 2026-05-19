#!/usr/bin/env python3
"""Retrofit `treatment_methods.aliases` with comprehensive synonyms.

For each method in the dictionary, asks LLM (gpt-4o-mini) to enumerate
ALL recognized synonyms / abbreviations / orthography variants in
Polish and English. Extends `aliases` JSONB with new variants (preserves
existing) + regenerates embedding to include new alias text.

USE CASE: initial curated seed (113 + user CSV 594 = 689 entries) was
created with minimal aliases. After backfill we noticed LLM kept
proposing new entries because subject service names referenced
synonyms NOT in our aliases (e.g. service "Toksyna botulinowa" didn't
match curated `botox` whose aliases lacked the phrase). Pre-INSERT
dedup catches most but better to widen aliases proactively.

Targets:
  --source curated (default) — retrofit human-vetted entries
  --source llm_inferred       — fill out machine-proposed entries too

Throughput:
  ~0.5-1s per method (one LLM call) + embedding regen
  689 curated × 0.7s ≈ 8-10 minutes single-threaded
  With concurrent=4: ~3 minutes

FAIL-LOUD: LLM / embedding / UPSERT failures re-raise. Per-method
errors logged with full context.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from openai import OpenAI  # noqa: E402

from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("retrofit_aliases")


async def _ask_llm_for_synonyms(
    openai_client: OpenAI,
    canonical: str,
    display: str,
    category: str,
    method_type: str,
    brand_family: str | None,
    description: str | None,
    existing_aliases: list[str],
) -> list[str]:
    """Returns a NEW list of aliases the LLM thinks should also map to
    this canonical. Empty list if none. Caller merges with existing.
    Includes orthography variants, PL/EN synonyms, common abbreviations,
    medical/trade name variants."""
    system = (
        "Jesteś ekspertem od taksonomii zabiegów beauty / medycyny "
        "estetycznej / fryzjerstwa / kosmetologii w Polsce. Twoim zadaniem "
        "jest WYCZERPUJĄCO wyliczyć wszystkie warianty pisowni, synonimy "
        "PL i EN, skróty oraz nazwy medyczne/handlowe pod którymi dana "
        "metoda jest rozpoznawana w nazwach usług salonów.\n\n"
        "Aliasy to wszystko co user mógłby wpisać w cenniku odnosząc "
        "się do tej samej metody. Przykład dla 'botox':\n"
        "  ['botox', 'botoks', 'toksyna botulinowa', 'toksyna botulinum', "
        "'botox vistabel', 'btx', 'vistabel', 'neuromodulator']\n\n"
        "Dla 'wielobiegunowa_rf' aliasy powinny obejmować 'rf', "
        "'radiofrekwencja', 'fala radiowa', 'multipolar rf', etc.\n\n"
        "ZASADY:\n"
        "1. Aliasy muszą być SPECIFIC dla tej metody — NIE generyczne "
        "(np. dla 'thunder' NIE dawaj 'laser diodowy' bo to ogólnik).\n"
        "2. Lowercase, ASCII gdzie możliwe (np. 'botoks' nie 'BOTOKS').\n"
        "3. Min 2 znaki na alias.\n"
        "4. Zwracaj wyłącznie WARIANTY (nowe + istniejące), bez wyjaśnień.\n"
        "5. Jeśli istniejące aliases już są wyczerpujące, zwróć puste []."
    )
    user = (
        f"Canonical: {canonical}\n"
        f"Display: {display}\n"
        f"Category: {category}\n"
        f"Method type: {method_type}\n"
        f"Brand family: {brand_family or 'n/a'}\n"
        f"Description: {description or 'n/a'}\n"
        f"Existing aliases: {existing_aliases}\n\n"
        "Zwróć JSON: "
        "{\"aliases\": [\"alias1\", \"alias2\", ...]} "
        "— wyłącznie NOWE aliasy (lub puste jeśli wszystkie znane są już w existing)."
    )
    resp = await asyncio.to_thread(
        openai_client.chat.completions.create,
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    content = resp.choices[0].message.content or "{}"
    payload = json.loads(content)
    raw = payload.get("aliases") or []
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for a in raw:
        if isinstance(a, str):
            n = a.strip().lower()
            if n and len(n) >= 2:
                out.append(n)
    return out


async def _regen_embedding(
    openai_client: OpenAI,
    canonical: str,
    display: str,
    aliases: list[str],
    description: str | None,
) -> str:
    """Regenerate embedding text to include the broader alias set."""
    emb_input = "\n".join([
        canonical,
        display,
        "Aliases: " + ", ".join(aliases) if aliases else "",
        description or "",
    ]).strip()
    resp = await asyncio.to_thread(
        openai_client.embeddings.create,
        model="text-embedding-3-small",
        input=emb_input,
    )
    vec = resp.data[0].embedding
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


async def _process_one(
    sb: SupabaseService,
    openai_client: OpenAI,
    row: dict,
) -> tuple[int, int, int]:
    """Returns (existing_count, added_count, total_count) for stats."""
    mid = int(row["id"])
    canonical = row["canonical_name"]
    existing = row.get("aliases") or []
    existing_set = {a.strip().lower() for a in existing if isinstance(a, str) and a.strip()}

    new_aliases = await _ask_llm_for_synonyms(
        openai_client,
        canonical=canonical,
        display=row.get("display_name") or canonical,
        category=row.get("category") or "inny",
        method_type=row.get("method_type") or "technique",
        brand_family=row.get("brand_family"),
        description=row.get("description"),
        existing_aliases=existing,
    )
    truly_new = [a for a in new_aliases if a not in existing_set]
    if not truly_new:
        return (len(existing_set), 0, len(existing_set))

    merged = sorted(existing_set | set(truly_new))
    new_embedding = await _regen_embedding(
        openai_client,
        canonical=canonical,
        display=row.get("display_name") or canonical,
        aliases=merged,
        description=row.get("description"),
    )
    sb.client.table("treatment_methods").update({
        "aliases": merged,
        "embedding": new_embedding,
    }).eq("id", mid).execute()

    logger.info(
        "Retrofitted %s (id=%d): %d → %d aliases (+%d)",
        canonical, mid, len(existing_set), len(merged), len(truly_new),
    )
    return (len(existing_set), len(truly_new), len(merged))


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="curated",
                        choices=["curated", "llm_inferred", "all"])
    parser.add_argument("--concurrent", type=int, default=4)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-from", type=str, default=None,
                        help="Resume from canonical_name (alphabetical)")
    args = parser.parse_args()

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("OPENAI_API_KEY not set")
        return 1
    openai_client = OpenAI(api_key=openai_key)
    sb = SupabaseService()

    # Fetch all methods to process
    q = sb.client.table("treatment_methods").select(
        "id, canonical_name, display_name, category, method_type, "
        "brand_family, description, aliases"
    )
    if args.source != "all":
        q = q.eq("source", args.source)
    if args.start_from:
        q = q.gte("canonical_name", args.start_from)
    q = q.order("canonical_name")
    if args.limit:
        q = q.limit(args.limit)
    res = q.execute()
    rows = res.data or []
    logger.info("Retrofit target: %d methods (source=%s)", len(rows), args.source)

    if not rows:
        return 0

    sem = asyncio.Semaphore(args.concurrent)
    total_added = 0
    total_existing = 0
    total_final = 0
    processed = 0
    errors = 0

    async def _wrapped(row: dict):
        nonlocal total_added, total_existing, total_final, processed, errors
        async with sem:
            try:
                existing, added, final = await _process_one(sb, openai_client, row)
                total_existing += existing
                total_added += added
                total_final += final
                processed += 1
            except Exception as e:
                errors += 1
                logger.error(
                    "retrofit FAILED for method id=%s canonical=%r: %s",
                    row.get("id"), row.get("canonical_name"), e,
                )
                # FAIL-LOUD: re-raise so caller can see failure mode.
                # gather() will collect first exception and abort.
                raise

    try:
        await asyncio.gather(*[_wrapped(r) for r in rows])
    except Exception as e:
        logger.error("Retrofit aborted on %d-th method: %s", processed + 1, e)
        # Continue with summary even on partial success
        pass

    print("\n══════════════════ RETROFIT DONE ══════════════════")
    print(f"Methods processed:       {processed}")
    print(f"Errors:                  {errors}")
    print(f"Aliases before total:    {total_existing}")
    print(f"Aliases after total:     {total_final}")
    print(f"Aliases ADDED:           {total_added}")
    print(f"Avg new aliases / method: {total_added / max(1, processed):.1f}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
