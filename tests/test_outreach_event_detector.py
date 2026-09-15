"""Testy detektora zmian cen (tryb: tylko zapis w bazie, bez Wintacta).

Kontrakty krytyczne:
  1. zdarzenie ląduje w outreach_events z payloadem, bez emitted_at i bez
     suppressed_reason — gotowe dla przyszłego nadawcy;
  2. moduł w ogóle nie zna Wintacta — nic nie wychodzi poza bazę;
  3. staging czytany stronami (PostgREST oddaje max 1000 wierszy) — żaden
     wiersz nie ginie;
  4. duplikat external_key (ta sama zmiana z kolejnej nocy) nie liczy się
     jako nowy zapis;
  5. wiersz stagingu bez kontaktu w outreach_contacts jest pomijany (FK).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from workers import outreach_event_detector as det


def staging_row(contact_id: int, competitor_id: int = 222) -> dict:
    return {
        "contact_id": contact_id,
        "salon_booksy_id": 111,
        "competitor_booksy_id": competitor_id,
        "competitor_name": "My Beauty Bar",
        "competitor_km": 0.06,
        "top_service": "Mezoterapia igłowa",
        "old_price_grosze": 42000,
        "new_price_grosze": 35000,
        "pct_change": -16.7,
        "changed_services": 3,
        "area_competitors_with_changes": 41,
        "detected_ts": "2026-08-11T03:00:00+00:00",
    }


def contact(contact_id: int, is_customer: bool = False) -> dict:
    return {"id": contact_id, "is_customer": is_customer, "owned_products": []}


class FakeTable:
    """Łańcuszek supabase-py: select/order/range/in_/upsert → execute, z nagrywaniem."""

    def __init__(self, name: str, db: "FakeDb"):
        self.name, self.db = name, db
        self.calls: dict = {}

    def select(self, *_a, **_k):
        return self

    def order(self, *_a, **_k):
        return self

    def range(self, start, end):
        self.calls["range"] = (start, end)
        return self

    def in_(self, _col, values):
        self.calls["in"] = list(values)
        return self

    def upsert(self, records, **kwargs):
        self.calls["upsert"] = (list(records), kwargs)
        return self

    def execute(self):
        if self.name == "price_events_staging":
            start, end = self.calls["range"]
            self.db.staging_reads.append((start, end))
            return SimpleNamespace(data=self.db.staging[start : end + 1])
        if self.name == "outreach_contacts":
            ids = set(self.calls["in"])
            return SimpleNamespace(data=[c for c in self.db.contacts if c["id"] in ids])
        if self.name == "outreach_events":
            records, kwargs = self.calls["upsert"]
            self.db.upserts.append((records, kwargs))
            fresh = [r for r in records if r["external_key"] not in self.db.existing_keys]
            self.db.existing_keys.update(r["external_key"] for r in fresh)
            return SimpleNamespace(data=fresh)
        raise AssertionError(f"nieoczekiwana tabela: {self.name}")


class FakeDb:
    def __init__(self, staging, contacts, existing_keys=()):
        self.staging, self.contacts = staging, contacts
        self.existing_keys = set(existing_keys)
        self.staging_reads: list = []
        self.upserts: list = []

    def client(self):
        sb = MagicMock()
        sb.table.side_effect = lambda name: FakeTable(name, self)
        return sb


async def run(db: FakeDb) -> dict:
    with patch.object(det, "make_supabase_client", return_value=db.client()), \
         patch("services.healthcheck.ping", new=AsyncMock()):
        return await det.detect_and_emit_price_events({})


def test_module_has_no_wintact_dependency():
    assert not hasattr(det, "WintactClient")
    assert not hasattr(det, "WintactError")


@pytest.mark.asyncio
async def test_records_event_without_emission_markers():
    db = FakeDb([staging_row(7)], [contact(7)])
    out = await run(db)

    assert out == {"detected": 1, "recorded": 1, "duplicates": 0, "skipped_no_contact": 0}
    (records, kwargs), = db.upserts
    rec = records[0]
    assert rec["event_type"] == det.EVENT_NAME_PRICE
    assert rec["external_key"].startswith("price:7:222:")
    assert rec["payload"]["competitor_name"] == "My Beauty Bar"
    assert rec["payload"]["direction"] == "spadek"
    assert rec["payload"]["target_product"] == "audit"
    assert "emitted_at" not in rec and "suppressed_reason" not in rec
    assert kwargs == {"on_conflict": "external_key", "ignore_duplicates": True}


@pytest.mark.asyncio
async def test_reads_all_staging_pages_beyond_postgrest_limit():
    n = det.STAGING_PAGE_SIZE * 2 + 17
    db = FakeDb([staging_row(i) for i in range(1, n + 1)], [contact(i) for i in range(1, n + 1)])
    out = await run(db)

    assert out["detected"] == n and out["recorded"] == n
    assert len(db.staging_reads) == 3
    assert all(len(records) <= det.UPSERT_BATCH for records, _ in db.upserts)


@pytest.mark.asyncio
async def test_duplicate_key_from_previous_night_is_not_counted_as_new():
    row = staging_row(7)
    db = FakeDb([row], [contact(7, is_customer=True)], existing_keys=[det._external_key(row)])
    out = await run(db)

    assert out == {"detected": 1, "recorded": 0, "duplicates": 1, "skipped_no_contact": 0}


@pytest.mark.asyncio
async def test_row_without_contact_is_skipped():
    db = FakeDb([staging_row(7), staging_row(8)], [contact(7)])
    out = await run(db)

    assert out == {"detected": 2, "recorded": 1, "duplicates": 0, "skipped_no_contact": 1}
    (records, _), = db.upserts
    assert [r["contact_id"] for r in records] == [7]


@pytest.mark.asyncio
async def test_empty_staging_returns_zero_stats():
    out = await run(FakeDb([], []))
    assert out == {"detected": 0, "recorded": 0, "duplicates": 0, "skipped_no_contact": 0}
