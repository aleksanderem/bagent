"""Issue #23 — runtime salon-scrape ingestion API.

Wraps the existing :mod:`scripts.ingest_salon_jsons` batch CLI as an
in-memory function so the scrape orchestrator can persist a fresh
business JSON without touching the disk-based batch runner. Idempotency
is preserved by computing the content_hash up-front and short-circuiting
when it already exists in ``json_ingestion_log``.

Public API:

    write_scrape_to_supabase(booksy_id, raw_json) -> WriteResult

The orchestrator calls this after fetching from bextract.
"""

from __future__ import annotations

from .live_scrape import (
    LiveIngestError,
    WriteResult,
    fetch_and_persist_salon,
    write_scrape_to_supabase,
)

__all__ = [
    "LiveIngestError",
    "WriteResult",
    "fetch_and_persist_salon",
    "write_scrape_to_supabase",
]
