"""Testy detektora zdarzeń outreach (F1 event-driven planu).

Kontrakty krytyczne:
  1. happy path — event ląduje w księdze, payload w custom_json_1 kontaktu,
     customEvents.upsert z external_id = external_key, emitted_at ustawione;
  2. frequency-cap — kontakt z emisją <14 dni dostaje wpis suppressed,
     ZERO wywołań Wintacta;
  3. dedup — external_key już w księdze (upsert ignore) → zero emisji.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from workers import outreach_event_detector as det


RPC_ROW = {
    "contact_id": 7,
    "salon_booksy_id": 111,
    "competitor_booksy_id": 222,
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


class FakeQuery:
    """Minimalny łańcuszek supabase-py: table().select/in_/gte/upsert/update.eq → execute."""

    def __init__(self, result):
        self._result = result

    def __getattr__(self, _name):
        return lambda *a, **k: self

    def execute(self):
        return SimpleNamespace(data=self._result)


def make_sb(rpc_rows, contact_row, capped_ids, upsert_data):
    sb = MagicMock()
    shared = {"events": None}
    sb.rpc.return_value = FakeQuery(rpc_rows)

    def table(name):
        if name == "outreach_contacts":
            return FakeQuery([contact_row])
        if name == "outreach_events":
            # jeden współdzielony mock: sb.table() jest wołane wielokrotnie,
            # a licznik per-instancja mylił upsert z cap-checkiem
            t = shared["events"]
            if t is None:
                t = MagicMock()
                state = {"mode": None}

                def _select(*a, **k): state["mode"] = "cap"; return t
                def _upsert(*a, **k): state["mode"] = "upsert"; return t
                def _update(*a, **k): state["mode"] = "update"; return t

                def execute():
                    if state["mode"] == "cap":
                        return SimpleNamespace(data=[{"contact_id": i} for i in capped_ids])
                    if state["mode"] == "upsert":
                        return SimpleNamespace(data=upsert_data)
                    return SimpleNamespace(data=[])

                t.select.side_effect = _select
                t.upsert.side_effect = _upsert
                t.update.side_effect = _update
                t.in_.return_value = t
                t.gte.return_value = t
                t.eq.return_value = t
                t.execute.side_effect = execute
                shared["events"] = t
            return t
        raise AssertionError(name)

    sb.table.side_effect = table
    return sb


def make_wc():
    wc = AsyncMock()
    wc.__aenter__ = AsyncMock(return_value=wc)
    wc.__aexit__ = AsyncMock(return_value=False)
    return wc


@pytest.mark.asyncio
async def test_happy_path_emits_event_and_sets_contact_attributes():
    contact = {"id": 7, "email": "salon@example.pl", "is_customer": False, "owned_products": []}
    sb = make_sb([RPC_ROW], contact, capped_ids=[], upsert_data=[{"id": 1}])
    wc = make_wc()
    with patch.object(det, "make_supabase_client", return_value=sb), \
         patch.object(det, "WintactClient", return_value=wc):
        out = await det.detect_and_emit_price_events({})

    assert out == {"detected": 1, "emitted": 1, "suppressed": 0, "errors": 0}
    attrs = wc.upsert_contact.call_args.kwargs["attributes"]
    assert attrs["custom_json_1"]["competitor_name"] == "My Beauty Bar"
    assert attrs["custom_json_1"]["direction"] == "spadek"
    assert attrs["custom_string_3"] == "audit"  # nie-klient → cold sprzedaż audytu
    ev = wc.upsert_custom_event.call_args.kwargs
    assert ev["event_name"] == det.EVENT_NAME_PRICE
    assert ev["external_id"].startswith("price:7:222:")


@pytest.mark.asyncio
async def test_frequency_cap_suppresses_without_wintact_calls():
    contact = {"id": 7, "email": "salon@example.pl", "is_customer": False, "owned_products": []}
    sb = make_sb([RPC_ROW], contact, capped_ids=[7], upsert_data=[{"id": 1}])
    wc = make_wc()
    with patch.object(det, "make_supabase_client", return_value=sb), \
         patch.object(det, "WintactClient", return_value=wc):
        out = await det.detect_and_emit_price_events({})

    assert out["suppressed"] == 1 and out["emitted"] == 0
    wc.upsert_contact.assert_not_called()
    wc.upsert_custom_event.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_external_key_is_not_reemitted():
    contact = {"id": 7, "email": "salon@example.pl", "is_customer": False, "owned_products": []}
    # upsert z ignore_duplicates zwraca [] dla istniejącego klucza
    sb = make_sb([RPC_ROW], contact, capped_ids=[], upsert_data=[])
    wc = make_wc()
    with patch.object(det, "make_supabase_client", return_value=sb), \
         patch.object(det, "WintactClient", return_value=wc):
        out = await det.detect_and_emit_price_events({})

    assert out["emitted"] == 0 and out["suppressed"] == 0
    wc.upsert_custom_event.assert_not_called()
