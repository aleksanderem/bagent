"""Regression tests: a dedup-swallowed chunk must NOT kill the whole ingest
(beads BEAUTY_AUDIT-ours).

Production symptom (salon booksy_id=103321, 301 services, chunk=150 -> chunk
boundaries 0/150/300): the tail chunk holds ONE row, that row collides on the
dedup key, the BEFORE INSERT trigger `trg_salon_scrape_services_dedup` UPDATEs
the canonical row and RETURNs NULL, so PostgREST answers 201 with an EMPTY
`data` array. The old per-chunk `raise IngestError` read that empty array as
"insert failed" and rolled back the entire scrape — even though the write had
actually succeeded (row merged into the canonical one). Salon froze on
2026-08-15 data while the worker retried hourly.

The guard therefore moved from per-chunk to aggregate: a chunk returning fewer
rows than it sent is a WARNING (dedup did its job), and only a run where NOTHING
at all got inserted still raises.

The ingester is built with a MagicMock client (no Supabase). `embed_texts` is
monkeypatched to return None so the inline-embedding block issues no RPC.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts.ingest_salon_jsons import IngestError, SalonJsonIngester

# Mirrors _SSS_INSERT_CHUNK in scripts/ingest_salon_jsons.py.
CHUNK = 150
# 301 services = the production shape: 150 + 150 + 1, tail chunk is a single row.
PROD_SERVICE_COUNT = 301
BOOKSY_ID = 103321


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _business_with_services(count: int) -> dict:
    """Booksy business with `count` variant-less services in one category.

    No variants -> _insert_service_variants is a no-op and never touches the
    insert mock, so the side_effect list below maps 1:1 onto service chunks.
    """
    return {
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
                    for i in range(count)
                ],
            }
        ]
    }


def _returned_rows(n: int, offset: int = 0) -> list[dict]:
    """Fake PostgREST `data` payload: n rows with BIGSERIAL ids.

    Carries `booksy_service_id` because PostgREST echoes the FULL row back, and
    that column is what pairs a returned row with its source payload row.
    """
    return [
        {
            "id": offset + i,
            "name": f"Usluga {offset + i}",
            "description": "Opis uslugi",
            "booksy_service_id": 1000 + offset + i,
        }
        for i in range(n)
    ]


def _make_ingester(chunk_results: list[list[dict]]) -> SalonJsonIngester:
    """Ingester whose insert().execute() yields `chunk_results` in order."""
    client = MagicMock()
    client.table.return_value.insert.return_value.execute.side_effect = [
        SimpleNamespace(data=rows) for rows in chunk_results
    ]
    client.rpc.return_value.execute.return_value.data = []
    return SalonJsonIngester(client=client, batch_tag=None, dry_run=False)


@pytest.fixture(autouse=True)
def _no_embeddings(monkeypatch):
    """Inline embedding off — keeps these tests about the insert guard only."""
    monkeypatch.setattr("scripts.ingest_salon_jsons.embed_texts", lambda inputs: None)


def _run(ing: SalonJsonIngester, count: int) -> int:
    return ing._insert_services(
        _business_with_services(count),
        scrape_id="scrape-1",
        booksy_id=BOOKSY_ID,
        scraped_at="2026-08-23T00:00:00Z",
    )


# ---------------------------------------------------------------------------
# (a) tail chunk fully swallowed by dedup -> ingest still succeeds
# ---------------------------------------------------------------------------

def test_empty_tail_chunk_does_not_raise(caplog):
    """The production case: chunks 0 and 150 land, the single-row chunk 300 is
    a duplicate the trigger merged away (empty data). Ingest must NOT raise."""
    ing = _make_ingester([
        _returned_rows(CHUNK, offset=0),
        _returned_rows(CHUNK, offset=CHUNK),
        [],  # chunk_start=300: 1 row sent, 0 returned (dedup merged it)
    ])

    with caplog.at_level(logging.WARNING, logger="ingest_salon_jsons"):
        inserted = _run(ing, PROD_SERVICE_COUNT)

    assert inserted == 2 * CHUNK
    # All three chunks were attempted — no early abort.
    assert ing.client.table.return_value.insert.call_count == 3
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("chunk_start=300" in m for m in warnings), warnings


def test_empty_middle_chunk_does_not_raise():
    """Same tolerance for a duplicate batch anywhere in the middle."""
    ing = _make_ingester([
        _returned_rows(CHUNK, offset=0),
        [],  # whole middle chunk was duplicate
        _returned_rows(1, offset=300),
    ])

    inserted = _run(ing, PROD_SERVICE_COUNT)

    assert inserted == CHUNK + 1
    assert ing.client.table.return_value.insert.call_count == 3


# ---------------------------------------------------------------------------
# (b) nothing inserted at all -> the guard still fires
# ---------------------------------------------------------------------------

def test_all_chunks_empty_raises_ingest_error():
    """A genuinely broken write (zero rows landed) must still abort the scrape."""
    ing = _make_ingester([[], [], []])

    with pytest.raises(IngestError) as exc:
        _run(ing, PROD_SERVICE_COUNT)

    msg = str(exc.value)
    assert "salon_scrape_services" in msg
    assert f"booksy_id={BOOKSY_ID}" in msg
    assert str(PROD_SERVICE_COUNT) in msg


def test_single_empty_chunk_raises_when_it_is_the_only_chunk():
    """Small salon, one chunk, nothing returned -> nothing was inserted."""
    ing = _make_ingester([[]])

    with pytest.raises(IngestError):
        _run(ing, 10)


# ---------------------------------------------------------------------------
# (c) partial chunk -> warning, no raise
# ---------------------------------------------------------------------------

def test_partial_chunk_logs_warning_and_does_not_raise(caplog):
    """149 of 150 rows returned: one duplicate merged, ingest carries on."""
    ing = _make_ingester([
        _returned_rows(CHUNK - 1, offset=0),
        _returned_rows(CHUNK, offset=CHUNK),
        _returned_rows(1, offset=300),
    ])

    with caplog.at_level(logging.WARNING, logger="ingest_salon_jsons"):
        inserted = _run(ing, PROD_SERVICE_COUNT)

    assert inserted == (CHUNK - 1) + CHUNK + 1
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    hit = [m for m in warnings if "chunk_start=0" in m]
    assert hit, warnings
    # Diagnosable without a DB session: who, where, sent, returned, skipped.
    assert f"booksy_id={BOOKSY_ID}" in hit[0]
    assert "150" in hit[0]  # sent
    assert "149" in hit[0]  # returned
    assert "1" in hit[0]    # skipped by dedup


def test_full_chunks_log_no_warning(caplog):
    """Happy path stays silent — no new noise in the worker logs."""
    ing = _make_ingester([
        _returned_rows(CHUNK, offset=0),
        _returned_rows(CHUNK, offset=CHUNK),
        _returned_rows(1, offset=300),
    ])

    with caplog.at_level(logging.WARNING, logger="ingest_salon_jsons"):
        inserted = _run(ing, PROD_SERVICE_COUNT)

    assert inserted == PROD_SERVICE_COUNT
    dedup_warnings = [
        r.getMessage() for r in caplog.records
        if r.levelno == logging.WARNING and "chunk_start=" in r.getMessage()
    ]
    assert dedup_warnings == []


# ---------------------------------------------------------------------------
# (d) sub-variants survive dedup — salon_scrape_service_variants
# ---------------------------------------------------------------------------
# Second-order defect of the fix above: with the per-chunk raise gone, a
# short-returned insert became PERMANENT and silent for
# salon_scrape_service_variants. `_insert_service_variants` used to bail out on
# `len(inserted) != len(source)` and pair rows POSITIONALLY via zip(), so one
# row swallowed by the dedup trigger dropped the whole salon's sub-variant
# snapshot every hourly refresh — with no exception and therefore no log.
# Nothing rebuilds that table (only this ingest writes it), and
# services/supabase.get_sub_variants_for_services reads it for tier-3 pricing.
# Pairing is now by key (booksy_service_id, else the dedup tuple).

VARIANT_A_BASE = 90_000
VARIANT_B_BASE = 95_000
SERVICE_PK_BASE = 7_000
BOOKSY_SERVICE_ID_BASE = 1_000


def _business_with_variant_services(count: int) -> dict:
    """Booksy business where every service carries two sub-variants."""
    return {
        "service_categories": [
            {
                "id": 1,
                "name": "Manicure",
                "order": 0,
                "services": [
                    {
                        "id": BOOKSY_SERVICE_ID_BASE + i,
                        "name": f"Usluga {i}",
                        "description": "Opis uslugi",
                        "treatment_id": 5000 + i,
                        "variants": [
                            {
                                "id": VARIANT_A_BASE + i,
                                "label": f"Wariant A uslugi {i}",
                                "price": 100 + i,
                                "duration": 30,
                                "type": "F",
                            },
                            {
                                "id": VARIANT_B_BASE + i,
                                "label": f"Wariant B uslugi {i}",
                                "price": 200 + i,
                                "duration": 60,
                                "type": "F",
                            },
                        ],
                    }
                    for i in range(count)
                ],
            }
        ]
    }


def _returned_variant_service_rows(indices: list[int]) -> list[dict]:
    """PostgREST echo for the services at `indices` — rows the trigger KEPT."""
    return [
        {
            "id": SERVICE_PK_BASE + i,
            "name": f"Usluga {i}",
            "booksy_service_id": BOOKSY_SERVICE_ID_BASE + i,
            "price_grosze": (100 + i) * 100,
            "duration_minutes": 30,
            "is_active": True,
        }
        for i in indices
    ]


def _make_variant_aware_ingester(service_chunks: list[list[dict]]):
    """Ingester with a per-table mock so the variants insert is observable.

    The single-mock helper above cannot be reused here: `client.table(...)`
    would hand the same mock to both tables and the variants insert would eat
    the services' side_effect list.
    """
    services_tbl = MagicMock()
    services_tbl.insert.return_value.execute.side_effect = [
        SimpleNamespace(data=rows) for rows in service_chunks
    ]
    variants_tbl = MagicMock()
    variants_tbl.insert.return_value.execute.return_value = SimpleNamespace(data=[])

    client = MagicMock()
    client.table.side_effect = lambda name: (
        variants_tbl if name == "salon_scrape_service_variants" else services_tbl
    )
    client.rpc.return_value.execute.return_value.data = []
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)
    return ing, variants_tbl


def _run_with_variants(ing: SalonJsonIngester, count: int) -> int:
    return ing._insert_services(
        _business_with_variant_services(count),
        scrape_id="scrape-1",
        booksy_id=BOOKSY_ID,
        scraped_at="2026-08-23T00:00:00Z",
    )


def _sent_variant_rows(variants_tbl: MagicMock) -> list[dict]:
    """Every row handed to salon_scrape_service_variants.insert()."""
    rows: list[dict] = []
    for call in variants_tbl.insert.call_args_list:
        rows.extend(call.args[0])
    return rows


def test_deduped_tail_chunk_still_writes_sub_variants():
    """The production shape (301 services, tail chunk merged away) must still
    produce sub-variant rows. Positional pairing dropped ALL of them."""
    ing, variants_tbl = _make_variant_aware_ingester([
        _returned_variant_service_rows(list(range(0, CHUNK))),
        _returned_variant_service_rows(list(range(CHUNK, 2 * CHUNK))),
        [],  # chunk_start=300: the single row was merged by the dedup trigger
    ])

    inserted = _run_with_variants(ing, PROD_SERVICE_COUNT)

    assert inserted == 2 * CHUNK
    assert variants_tbl.insert.call_count > 0
    # 300 surviving services x 2 variants each.
    assert len(_sent_variant_rows(variants_tbl)) == 2 * 2 * CHUNK


def test_sub_variants_land_under_the_right_service_id():
    """A row deduped in the MIDDLE must not shift everything after it by one."""
    kept = [i for i in range(10) if i != 4]
    ing, variants_tbl = _make_variant_aware_ingester([
        _returned_variant_service_rows(kept),
    ])

    _run_with_variants(ing, 10)

    sent = _sent_variant_rows(variants_tbl)
    assert len(sent) == 2 * len(kept)
    # The merged service contributed nothing...
    assert all(row["service_id"] != SERVICE_PK_BASE + 4 for row in sent)
    # ...and every variant sits under the service it actually came from.
    for row in sent:
        service_index = row["service_id"] - SERVICE_PK_BASE
        assert row["label"].endswith(f"uslugi {service_index}"), row
        expected_ids = {VARIANT_A_BASE + service_index, VARIANT_B_BASE + service_index}
        assert row["booksy_variant_id"] in expected_ids, row


def test_unmatched_source_rows_log_warning_with_count(caplog):
    """Silence was the defect: skipped services must show up in the log."""
    kept = [i for i in range(10) if i not in (2, 7)]
    ing, _variants_tbl = _make_variant_aware_ingester([
        _returned_variant_service_rows(kept),
    ])

    with caplog.at_level(logging.WARNING, logger="ingest_salon_jsons"):
        _run_with_variants(ing, 10)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    hit = [m for m in warnings if "sub-variants" in m]
    assert hit, warnings
    assert "2 z 10" in hit[0]
    assert f"booksy_id={BOOKSY_ID}" in hit[0]


def test_all_services_matched_logs_no_sub_variant_warning(caplog):
    """Happy path adds no noise to the worker logs."""
    ing, _variants_tbl = _make_variant_aware_ingester([
        _returned_variant_service_rows(list(range(10))),
    ])

    with caplog.at_level(logging.WARNING, logger="ingest_salon_jsons"):
        _run_with_variants(ing, 10)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert [m for m in warnings if "sub-variants" in m] == []


def test_majority_of_payload_missing_logs_error(caplog):
    """More than half the rows swallowed is not routine dedup — say so LOUD."""
    ing, _variants_tbl = _make_variant_aware_ingester([
        _returned_variant_service_rows(list(range(4))),  # 4 of 10 landed
    ])

    with caplog.at_level(logging.ERROR, logger="ingest_salon_jsons"):
        inserted = _run_with_variants(ing, 10)

    assert inserted == 4
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert errors, "expected a loud error when <50% of the payload came back"
    assert f"booksy_id={BOOKSY_ID}" in errors[0]
    assert "sent=10" in errors[0]
    assert "returned=4" in errors[0]
