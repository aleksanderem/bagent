"""Embed all 368 canonical Booksy treatment names via OpenAI
text-embedding-3-small and persist to booksy_treatment_embeddings.

One-shot: run after migration 045 is applied. Refresh periodically via
nightly cron when mv_booksy_treatments grows (new treatments discovered).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

from config import settings
from services.sb_client import make_supabase_client


def main() -> int:
    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    oai = OpenAI()

    # Load tids without embeddings yet
    treatments = (
        client.table("mv_booksy_treatments")
        .select("id,canonical_name,parent_id")
        .order("id")
        .execute()
        .data
    )

    embedded_tids = {
        r["tid"] for r in (
            client.table("booksy_treatment_embeddings")
            .select("tid")
            .execute()
            .data
            or []
        )
    }
    missing = [t for t in treatments if t["id"] not in embedded_tids]
    print(f"Total treatments: {len(treatments)}  | already embedded: {len(embedded_tids)}  | to embed: {len(missing)}")

    if not missing:
        return 0

    # Batch OpenAI calls (up to 2048 inputs per request — but smaller is safer).
    BATCH = 200
    inserts: list[dict] = []
    for i in range(0, len(missing), BATCH):
        chunk = missing[i:i + BATCH]
        names = [t["canonical_name"] for t in chunk]
        t0 = time.time()
        resp = oai.embeddings.create(model="text-embedding-3-small", input=names)
        dt = time.time() - t0
        print(f"  batch {i // BATCH + 1}/{(len(missing) + BATCH - 1) // BATCH}: {len(names)} embedded in {dt:.1f}s")
        for t, ed in zip(chunk, resp.data):
            inserts.append({"tid": t["id"], "embedding": ed.embedding})

    # Persist — upsert in chunks
    UP = 100
    for i in range(0, len(inserts), UP):
        client.table("booksy_treatment_embeddings").upsert(inserts[i:i + UP]).execute()
    print(f"\nPersisted {len(inserts)} embeddings.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
