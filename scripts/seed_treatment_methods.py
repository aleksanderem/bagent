#!/usr/bin/env python3
"""Seed treatment_methods table from CSV with OpenAI embeddings.

Reads bagent/data/treatment_methods_seed.csv, generates a 1536-dim
embedding for each row via OpenAI text-embedding-3-small, and UPSERTs
into the treatment_methods table (mig 095).

The embedding is computed over a concatenation of canonical_name +
display_name + aliases + description joined with newlines. This is the
text the ANN classifier sees as the "canonical method description" when
matching service names to methods. Adding new aliases later requires
re-embedding — re-run this script with --rebuild-embeddings to force.

Usage:
    # From bagent/ root (loads .env automatically):
    .venv/bin/python scripts/seed_treatment_methods.py
    .venv/bin/python scripts/seed_treatment_methods.py --rebuild-embeddings
    .venv/bin/python scripts/seed_treatment_methods.py --dry-run
    .venv/bin/python scripts/seed_treatment_methods.py --limit 10

Idempotent: re-running upserts on canonical_name conflict; existing
methods get aliases/description/competitor_methods refreshed BUT
embeddings preserved unless --rebuild-embeddings.

On Tytan:
    ssh root@tytan 'sudo -u booksy bash -lc "
        cd /home/booksy/webapps/bagent-booksyauditor &&
        .venv/bin/python scripts/seed_treatment_methods.py
    "'
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Allow running from anywhere
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from openai import OpenAI  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("seed_treatment_methods")

SEED_CSV = REPO_ROOT / "data" / "treatment_methods_seed.csv"
EMBEDDING_MODEL = "text-embedding-3-small"
BATCH_SIZE = 100  # OpenAI embeddings batch limit-friendly


def _normalize_aliases(raw: str) -> list[str]:
    """Aliases column uses pipe-delimited list inside the CSV cell so we
    don't fight CSV quoting on every line."""
    if not raw or not raw.strip():
        return []
    return [a.strip().lower() for a in raw.split("|") if a.strip()]


def _normalize_jsonb(raw: str) -> list:
    """JSONB columns (body_areas, competitor_methods) come as JSON arrays
    in the CSV cell. Empty string → empty list."""
    if not raw or not raw.strip():
        return []
    try:
        v = json.loads(raw)
        return v if isinstance(v, list) else []
    except (json.JSONDecodeError, TypeError):
        logger.warning("Bad JSONB value: %r — treating as empty list", raw)
        return []


def _embedding_input(record: dict) -> str:
    """Build the text we embed. Lines joined with newline so the model
    sees them as distinct facets but in one document. Operates on the
    already-normalized record (aliases as list, description as str|None)."""
    parts = [
        record["canonical_name"],
        record["display_name"],
    ]
    aliases = record.get("aliases") or []
    if aliases:
        parts.append("Aliases: " + ", ".join(aliases))
    desc = record.get("description") or ""
    if desc.strip():
        parts.append(desc.strip())
    return "\n".join(parts)


def _row_to_record(row: dict) -> dict:
    """CSV row → DB record (without embedding — added later in batch)."""
    return {
        "canonical_name": row["canonical_name"].strip().lower(),
        "display_name": row["display_name"].strip(),
        "category": row["category"].strip().lower(),
        "method_type": row["method_type"].strip().lower(),
        "brand_family": (row.get("brand_family") or "").strip().lower() or None,
        "aliases": _normalize_aliases(row.get("aliases", "")),
        "body_areas": _normalize_jsonb(row.get("body_areas", "")) or None,
        "description": (row.get("description") or "").strip() or None,
        "competitor_methods": _normalize_jsonb(row.get("competitor_methods", "")),
        "source": "curated",
    }


def _load_csv(limit: int | None = None) -> list[dict]:
    if not SEED_CSV.exists():
        raise FileNotFoundError(f"Seed CSV missing: {SEED_CSV}")
    rows: list[dict] = []
    with SEED_CSV.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            rows.append(row)
    logger.info("Loaded %d rows from %s", len(rows), SEED_CSV.name)
    return rows


def _embed_batch(openai_client: OpenAI, texts: list[str]) -> list[list[float]]:
    """Single OpenAI batch embed call. Returns 1536-dim vectors in order."""
    resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
    return [d.embedding for d in resp.data]


def _pgvector_str(vec: list[float]) -> str:
    """pgvector accepts text format [v1,v2,...]"""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse + validate CSV without DB writes (no embeddings either).",
    )
    parser.add_argument(
        "--rebuild-embeddings", action="store_true",
        help="Re-embed every row even if it already has embedding in DB.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process first N rows (smoke test).",
    )
    parser.add_argument(
        "--csv", type=str, default=None,
        help="Override seed CSV path (default: bagent/data/treatment_methods_seed.csv).",
    )
    args = parser.parse_args()

    if args.csv:
        global SEED_CSV
        SEED_CSV = Path(args.csv).resolve()

    rows = _load_csv(limit=args.limit)
    records = [_row_to_record(r) for r in rows]

    # Sanity: canonical_name unique within CSV
    seen: set[str] = set()
    dups: list[str] = []
    for rec in records:
        if rec["canonical_name"] in seen:
            dups.append(rec["canonical_name"])
        seen.add(rec["canonical_name"])
    if dups:
        logger.error("Duplicate canonical_name in CSV: %s", dups)
        return 2

    # Sanity: valid method_type
    bad_types = {
        rec["canonical_name"]: rec["method_type"]
        for rec in records
        if rec["method_type"] not in {"device", "substance", "technique", "protocol"}
    }
    if bad_types:
        logger.error("Invalid method_type values: %s", bad_types)
        return 2

    if args.dry_run:
        logger.info("--dry-run: validated %d records, exiting.", len(records))
        # Show a sample for visual sanity
        import pprint
        pprint.pp(records[:3], width=120)
        return 0

    # ── Init clients ──
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.error("OPENAI_API_KEY not set")
        return 1
    openai_client = OpenAI(api_key=openai_key)

    supabase = SupabaseService()

    # ── Determine which rows need embedding ──
    canonical_names = [r["canonical_name"] for r in records]
    existing_emb_map: dict[str, bool] = {}
    if not args.rebuild_embeddings:
        try:
            existing = (
                supabase.client.table("treatment_methods")
                .select("canonical_name,embedding")
                .in_("canonical_name", canonical_names)
                .execute()
            )
            for r in existing.data or []:
                existing_emb_map[r["canonical_name"]] = r.get("embedding") is not None
        except Exception as e:
            logger.warning("Couldn't preload existing embeddings: %s", e)

    rows_needing_embedding = [
        rec for rec in records
        if args.rebuild_embeddings or not existing_emb_map.get(rec["canonical_name"])
    ]
    logger.info(
        "Embedding plan: %d rows need new embeddings, %d already embedded",
        len(rows_needing_embedding),
        len(records) - len(rows_needing_embedding),
    )

    # ── Batch embed (OpenAI supports many inputs per call) ──
    embedding_by_canonical: dict[str, list[float]] = {}
    for i in range(0, len(rows_needing_embedding), BATCH_SIZE):
        chunk = rows_needing_embedding[i : i + BATCH_SIZE]
        texts = [_embedding_input(r) for r in chunk]
        logger.info(
            "Embedding batch %d-%d (%d items)…",
            i + 1, i + len(chunk), len(chunk),
        )
        try:
            vecs = _embed_batch(openai_client, texts)
        except Exception as e:
            logger.error("OpenAI embed batch failed: %s", e)
            return 1
        for rec, vec in zip(chunk, vecs, strict=True):
            embedding_by_canonical[rec["canonical_name"]] = vec

    # ── UPSERT each record. We do row-by-row instead of bulk because
    # Supabase upsert + pgvector + JSONB is fiddly with conflict syntax
    # via REST. ~100 rows is fine. ──
    inserted = 0
    updated = 0
    failed = 0
    for rec in records:
        cn = rec["canonical_name"]
        # Embedding handling — pgvector accepts text format
        if cn in embedding_by_canonical:
            rec["embedding"] = _pgvector_str(embedding_by_canonical[cn])
        # Else: leave embedding column unchanged (UPSERT will skip it)
        try:
            # UPSERT on canonical_name (UNIQUE constraint)
            r = (
                supabase.client.table("treatment_methods")
                .upsert(rec, on_conflict="canonical_name")
                .execute()
            )
            # supabase-py doesn't reliably return is_new — count by
            # whether we had the row in existing_emb_map preload
            if cn in existing_emb_map:
                updated += 1
            else:
                inserted += 1
            logger.debug("upserted %s", cn)
        except Exception as e:
            logger.error("Upsert failed for %s: %s", cn, e)
            failed += 1

    logger.info(
        "Done. inserted=%d updated=%d failed=%d total=%d",
        inserted, updated, failed, len(records),
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
