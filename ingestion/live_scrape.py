"""Issue #23 — runtime ingestion glue.

The batch CLI (``scripts/ingest_salon_jsons.py``) reads JSON files off
disk. The scrape orchestrator gets the same JSON live from bextract over
HTTP, so this module wraps the batch ingester with a function that takes
an in-memory dict and routes it through the existing pipeline by
materialising a single tempfile.

Why a tempfile? The ``SalonJsonIngester.ingest_file`` method is the
single source of truth for the write semantics (salons + salon_scrapes
+ services + reviews + top_services + json_ingestion_log) and uses
``Path.read_bytes()`` + filename for the audit log. Refactoring it into a
dict-first API would touch >100 lines we explicitly want to leave
unchanged. The tempfile path keeps the contract identical for both
batch and live callers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from supabase import Client

from services.sb_client import make_supabase_client

from config import settings
from scripts.ingest_salon_jsons import IngestError, SalonJsonIngester

logger = logging.getLogger("bagent.ingestion.live_scrape")


class LiveIngestError(Exception):
    """Raised when fetch+ingest fails. Wraps the underlying cause."""


@dataclass(frozen=True)
class WriteResult:
    booksy_id: int
    skipped: bool
    counts: dict[str, int]
    """One of:
        - {scrapes,services,reviews,top_services} — fresh write counts
        - {} — when skipped=True (duplicate content_hash already present)"""


_supabase_client: Client | None = None


def _get_supabase_client() -> Client:
    """Return a process-cached Supabase client.

    Each scrape worker process re-uses one client across many ingests.
    Tier-3 cold sweeps process thousands of salons per worker, so the
    HTTP keep-alive on the client matters.
    """
    global _supabase_client
    if _supabase_client is None:
        if not settings.supabase_url or not settings.supabase_service_key:
            raise LiveIngestError("supabase_url / supabase_service_key not configured")
        _supabase_client = make_supabase_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )
    return _supabase_client


def write_scrape_to_supabase(
    booksy_id: int,
    raw_json: dict[str, Any],
    *,
    batch_tag: str | None = None,
    dry_run: bool = False,
) -> WriteResult:
    """Persist a fresh Booksy business JSON to Supabase.

    Idempotent: a re-call with the exact same ``raw_json`` content returns
    ``skipped=True`` because the content_hash is already in
    ``json_ingestion_log``.

    The function serialises ``raw_json`` to a stable JSON byte buffer
    (``sort_keys=True``, no whitespace) so two callers seeing the same
    object identity always produce the same SHA-256 — bextract on the
    other hand returns minified JSON which would otherwise be hashed
    differently than a re-run.
    """
    if not isinstance(raw_json, dict) or not raw_json:
        raise LiveIngestError("raw_json must be a non-empty dict")

    serialised = json.dumps(raw_json, sort_keys=True, separators=(",", ":")).encode("utf-8")
    content_hash = hashlib.sha256(serialised).hexdigest()
    client = _get_supabase_client()
    ingester = SalonJsonIngester(client=client, batch_tag=batch_tag, dry_run=dry_run)

    # Short-circuit idempotency check before touching the filesystem.
    already = ingester._hashes_already_ingested([content_hash])  # type: ignore[attr-defined]
    if content_hash in already:
        logger.info(
            "[live_scrape] booksy_id=%s already ingested (content_hash=%s) — skipping",
            booksy_id,
            content_hash,
        )
        return WriteResult(booksy_id=booksy_id, skipped=True, counts={})

    # Materialise to a tempfile in the OS tmpdir. We use a stable filename
    # of `{booksy_id}.json` so the audit log shows the same `file_name`
    # shape as the batch CLI rows.
    tmpdir = Path(tempfile.mkdtemp(prefix="bagent-scrape-"))
    tmpfile = tmpdir / f"{booksy_id}.json"
    try:
        tmpfile.write_bytes(serialised)
        try:
            counts = ingester.ingest_file(tmpfile, content_hash)
        except IngestError as e:
            raise LiveIngestError(
                f"ingest_file failed for booksy_id={booksy_id}: {e}"
            ) from e
        logger.info(
            "[live_scrape] booksy_id=%s ingested counts=%s content_hash=%s",
            booksy_id,
            counts,
            content_hash,
        )
        return WriteResult(booksy_id=booksy_id, skipped=False, counts=counts)
    finally:
        try:
            if tmpfile.exists():
                tmpfile.unlink()
            tmpdir.rmdir()
        except OSError as e:
            # Don't bubble up cleanup failures — they're not customer-impacting.
            logger.warning(
                "[live_scrape] failed to clean tempdir %s: %s", tmpdir, e
            )


async def fetch_and_persist_salon(
    booksy_id: int,
    *,
    timeout_sec: float = 30.0,
    batch_tag: str | None = None,
) -> WriteResult:
    """Fetch a fresh JSON from bextract and persist it via
    :func:`write_scrape_to_supabase`.

    This is the workhorse the scrape worker calls per claimed job.
    The bextract `/api/salon/:id` endpoint returns the raw Booksy
    business JSON identical to what the batch tooling expects, so the
    ingestion call is wholly unchanged.
    """
    if not settings.bextract_api_url or not settings.bextract_api_key:
        raise LiveIngestError("bextract_api_url / bextract_api_key not configured")

    url = f"{settings.bextract_api_url.rstrip('/')}/api/salon/{booksy_id}"
    headers = {"x-api-key": settings.bextract_api_key}

    # Booksy 429 backoff. bextract surfaces the upstream 429 as its own
    # 500 with the body containing "Booksy API 429: Too Many Requests".
    # Retry with exponential backoff so a single hot moment doesn't fail
    # the queue job — caller (scrape worker) requeues with longer backoff
    # only if all retries here exhaust.
    backoff = 5.0
    response = None
    for attempt in range(1, 5):
        try:
            async with httpx.AsyncClient(timeout=timeout_sec) as client:
                response = await client.get(url, headers=headers)
        except httpx.HTTPError as e:
            raise LiveIngestError(f"bextract fetch failed: {e}") from e

        if response.status_code == 200:
            break

        body = response.text[:500]
        is_rate_limit = (
            response.status_code == 429
            or (response.status_code == 500 and "429" in body)
        )
        if is_rate_limit and attempt < 4:
            logger.warning(
                "[live_scrape] booksy_id=%s rate-limited (attempt %d), backing off %.0fs",
                booksy_id, attempt, backoff,
            )
            await asyncio.sleep(backoff)
            backoff *= 2
            continue
        raise LiveIngestError(
            f"bextract returned HTTP {response.status_code} for booksy_id={booksy_id}: {body}"
        )

    try:
        payload = response.json()
    except json.JSONDecodeError as e:
        raise LiveIngestError(
            f"bextract returned invalid JSON for booksy_id={booksy_id}: {e}"
        ) from e

    return write_scrape_to_supabase(booksy_id, payload, batch_tag=batch_tag)
