"""Regression tests: salons.last_scraped_at must follow the INGEST OUTCOME
(beads BEAUTY_AUDIT-70w4).

Production symptom (salon booksy_id=103321): `_upsert_salon` ran as step 1 of
`ingest_file` and wrote `last_scraped_at` there, before services, reviews and
the audit log. When a later step blew up, `_unwind_scrape` deleted the
salon_scrapes row — but the marker stayed at the failed run's timestamp. The
salon advertised last_scraped_at=2026-08-23 01:05 while its newest data was
from 2026-08-15, so every "refresh the stale ones" selector skipped it and the
staleness kept itself alive.

The write is now split: step 1 upserts identity only, and `_mark_salon_scraped`
moves the marker at the very end, once the whole file has landed. An
'unchanged' run counts as landed — the content was re-confirmed against Booksy.

The ingester runs against a per-table MagicMock client (no Supabase).
`embed_texts` is monkeypatched to None so the inline-embedding block issues no
RPC.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ingest_salon_jsons import IngestError, SalonJsonIngester
from services.scrape_history import compute_content_hash

BOOKSY_ID = 103321
SALON_PK = 4242
SCRAPE_ID = "11111111-1111-1111-1111-111111111111"
HEAD_ID = "22222222-2222-2222-2222-222222222222"
SCRAPED_AT = "2026-08-23T01:05:00+00:00"
SERVICE_COUNT = 2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _ns(**kwargs) -> SimpleNamespace:
    """PostgREST-ish response object: whatever attribute the caller reads."""
    return SimpleNamespace(**kwargs)


def _payload() -> dict:
    """Booksy file payload: one category, variant-less services, no reviews.

    Variant-less + review-less keeps the ingest to four tables (salons,
    salon_scrapes, salon_scrape_services, json_ingestion_log), which is exactly
    the surface these tests care about.
    """
    return {
        "business": {
            "id": BOOKSY_ID,
            "name": "Salon Testowy",
            "updated_at": SCRAPED_AT,
            "service_categories": [
                {
                    "id": 1,
                    "name": "Manicure",
                    "order": 0,
                    "services": [
                        {
                            "id": 1000 + i,
                            "name": f"Usluga {i}",
                            "description": "Opis uslugi",
                            "treatment_id": 5000 + i,
                            "variants": [],
                        }
                        for i in range(SERVICE_COUNT)
                    ],
                }
            ],
        }
    }


def _service_rows(n: int = SERVICE_COUNT) -> list[dict]:
    """PostgREST echo for salon_scrape_services.insert()."""
    return [
        {"id": 7000 + i, "name": f"Usluga {i}", "booksy_service_id": 1000 + i}
        for i in range(n)
    ]


def _chain_head(content_hash: str) -> dict:
    return {
        "id": HEAD_ID,
        "content_hash": content_hash,
        "chain_length": 0,
        "raw_response": {"business": {"id": BOOKSY_ID}},
        "snapshot_kind": "base",
        "scraped_at": "2026-08-15T00:00:00+00:00",
    }


def _make_client(
    *,
    salon_exists: bool = True,
    chain_head: dict | None = None,
    service_rows: list[dict] | None = None,
) -> tuple[MagicMock, dict[str, MagicMock]]:
    """Per-table mock client. Returns (client, tables-by-name)."""
    salons = MagicMock()
    salons.select.return_value.eq.return_value.limit.return_value.execute.return_value = _ns(
        data=[{"booksy_id": BOOKSY_ID}] if salon_exists else []
    )
    salons.upsert.return_value.execute.return_value = _ns(data=[{"id": SALON_PK}])
    salons.update.return_value.eq.return_value.execute.return_value = _ns(data=[{"id": SALON_PK}])

    scrapes = MagicMock()
    (
        scrapes.select.return_value.eq.return_value.eq.return_value
        .order.return_value.limit.return_value.execute.return_value
    ) = _ns(data=[chain_head] if chain_head else [])
    scrapes.select.return_value.eq.return_value.single.return_value.execute.return_value = _ns(
        data={"unchanged_crawl_count": 0}
    )
    scrapes.insert.return_value.execute.return_value = _ns(data=[{"id": SCRAPE_ID}])
    scrapes.update.return_value.eq.return_value.execute.return_value = _ns(data=[])
    scrapes.delete.return_value.eq.return_value.execute.return_value = _ns(data=[])

    services = MagicMock()
    services.insert.return_value.execute.return_value = _ns(
        data=_service_rows() if service_rows is None else service_rows
    )
    # _promote_chain_head's embedding gate.
    (
        services.select.return_value.eq.return_value
        .is_.return_value.is_.return_value.execute.return_value
    ) = _ns(count=0)

    ingestion_log = MagicMock()
    ingestion_log.insert.return_value.execute.return_value = _ns(data=[{"id": 1}])

    tables = {
        "salons": salons,
        "salon_scrapes": scrapes,
        "salon_scrape_services": services,
        "json_ingestion_log": ingestion_log,
    }
    client = MagicMock()
    client.table.side_effect = lambda name: tables[name]
    client.rpc.return_value.execute.return_value.data = []
    return client, tables


@pytest.fixture(autouse=True)
def _no_embeddings(monkeypatch):
    """Inline embedding off — these tests are about the marker only."""
    monkeypatch.setattr("scripts.ingest_salon_jsons.embed_texts", lambda inputs: None)


def _write_file(tmp_path: Path, payload: dict) -> tuple[Path, str]:
    """Materialise the payload the way live_scrape does. Returns (path, hash)."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path = tmp_path / f"{BOOKSY_ID}.json"
    path.write_bytes(raw)
    return path, "f" * 64


def _bumps(tables: dict[str, MagicMock]) -> list[dict]:
    """Every payload handed to salons.update() — i.e. every marker bump."""
    return [call.args[0] for call in tables["salons"].update.call_args_list]


def _upsert_payload(tables: dict[str, MagicMock]) -> dict:
    return tables["salons"].upsert.call_args.args[0]


# ---------------------------------------------------------------------------
# (a) success -> bump
# ---------------------------------------------------------------------------

def test_successful_ingest_bumps_last_scraped_at(tmp_path):
    """Happy path is unchanged from the caller's point of view: the salon ends
    the run marked fresh."""
    payload = _payload()
    client, tables = _make_client()
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    counts = ing.ingest_file(path, content_hash)

    assert counts["scrapes"] == 1
    assert counts["services"] == SERVICE_COUNT
    assert _bumps(tables) == [{"last_scraped_at": SCRAPED_AT}]


def test_unchanged_run_still_bumps_last_scraped_at(tmp_path):
    """A dedup rollup re-confirmed the content against Booksy — that IS a
    successful scrape, so the marker has to move."""
    payload = _payload()
    client, tables = _make_client(chain_head=_chain_head(compute_content_hash(payload)))
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    counts = ing.ingest_file(path, content_hash)

    assert counts["dedup_action"] == "unchanged"
    assert counts["scrapes"] == 0
    assert _bumps(tables) == [{"last_scraped_at": SCRAPED_AT}]


# ---------------------------------------------------------------------------
# (b) failure -> marker stays put
# ---------------------------------------------------------------------------

def test_failed_child_insert_leaves_last_scraped_at_untouched(tmp_path):
    """The production shape: services blow up, the scrape is unwound, and the
    salon must NOT look freshly scraped afterwards."""
    payload = _payload()
    client, tables = _make_client(service_rows=[])
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    with pytest.raises(IngestError) as exc:
        ing.ingest_file(path, content_hash)

    assert "child inserts" in str(exc.value)
    # Neither write may carry the marker: not the step-1 upsert (that WAS the
    # bug) and not a late bump (that step is never reached).
    assert "last_scraped_at" not in _upsert_payload(tables)
    assert _bumps(tables) == []
    # Unwind still runs — the fix must not swallow the existing rollback.
    tables["salon_scrapes"].delete.return_value.eq.assert_called_once_with("id", SCRAPE_ID)


def test_failed_audit_log_leaves_last_scraped_at_untouched(tmp_path):
    """Same for the later failure point: the audit row never landed, so the
    file will be retried and the marker must still show the last good scrape."""
    payload = _payload()
    client, tables = _make_client()
    tables["json_ingestion_log"].insert.return_value.execute.side_effect = RuntimeError("boom")
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    with pytest.raises(IngestError) as exc:
        ing.ingest_file(path, content_hash)

    assert "audit log" in str(exc.value)
    assert "last_scraped_at" not in _upsert_payload(tables)
    assert _bumps(tables) == []
    tables["salon_scrapes"].delete.return_value.eq.assert_called_once_with("id", SCRAPE_ID)


# ---------------------------------------------------------------------------
# (c) the identity upsert itself
# ---------------------------------------------------------------------------

def test_identity_upsert_carries_no_last_scraped_at(tmp_path):
    """Step 1 must not touch the column at all for a known salon — otherwise
    the UPDATE branch overwrites the previous, truthful timestamp."""
    payload = _payload()
    client, tables = _make_client(salon_exists=True)
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    ing.ingest_file(path, content_hash)

    assert "last_scraped_at" not in _upsert_payload(tables)


def test_first_insert_pins_last_scraped_at_to_null(tmp_path):
    """A salon nobody has seen before: the column DEFAULTs to now(), which
    would fake a full scrape. discovery/discover.py reads NULL as 'needs full
    scrape', so the INSERT has to spell the NULL out."""
    payload = _payload()
    client, tables = _make_client(salon_exists=False)
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    path, content_hash = _write_file(tmp_path, payload)

    ing.ingest_file(path, content_hash)

    assert _upsert_payload(tables)["last_scraped_at"] is None
    # ...and the successful run then moves it forward.
    assert _bumps(tables) == [{"last_scraped_at": SCRAPED_AT}]


# ---------------------------------------------------------------------------
# (d) dry-run writes nothing
# ---------------------------------------------------------------------------

def test_dry_run_never_touches_the_marker(tmp_path):
    payload = _payload()
    client, tables = _make_client()
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=True)
    path, content_hash = _write_file(tmp_path, payload)

    ing.ingest_file(path, content_hash)

    assert _bumps(tables) == []
    assert tables["salons"].upsert.call_count == 0
