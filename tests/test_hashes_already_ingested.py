"""TDD tests for SalonJsonIngester._hashes_already_ingested (beads BEAUTY_AUDIT-1mb item 2).

The method's behavior contract is UNCHANGED: given a list of content hashes it
returns the subset already present in json_ingestion_log, and short-circuits an
empty input to set() with no network call. Only the IMPLEMENTATION changed — the
per-ingest-call ~140k-row full-table pagination scan was replaced by a single
indexed batch RPC `fn_existing_content_hashes` (migration 133), passing the
batch in the POST body so there is no full-table scan and no oversized IN-filter
URL.

Migration 133 is NOT applied; these tests mock the Supabase RPC (the method only
needs `self.client.rpc(...).execute().data` to be a list of
{"content_hash": "<hex>"} dicts — the PostgREST shape of RETURNS TABLE).

Conventions mirror tests/test_ingest_embedding_routing.py: MagicMock client,
SalonJsonIngester(client=..., batch_tag=None, dry_run=False), stub via
client.rpc.return_value.execute.return_value.data, assert via
client.rpc.call_args_list (.args[0] = rpc name, .args[1] = params dict).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from scripts.ingest_salon_jsons import SalonJsonIngester


def _make_ingester(rpc_data: list | None) -> SalonJsonIngester:
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = rpc_data
    return SalonJsonIngester(client=client, batch_tag=None, dry_run=False)


def test_returns_subset_present_in_log_via_rpc():
    """Test 1 — subset return + single RPC with correct name/params.

    RPC reports h1 and h3 present; input is [h1,h2,h3] -> returns exactly
    {h1,h3}. The RPC is called once as fn_existing_content_hashes with the full
    input batch in the POST body params {"p_hashes": [...]}.
    """
    ing = _make_ingester(
        [{"content_hash": "h1"}, {"content_hash": "h3"}]
    )

    result = ing._hashes_already_ingested(["h1", "h2", "h3"])

    assert result == {"h1", "h3"}

    assert ing.client.rpc.call_count == 1
    call = ing.client.rpc.call_args_list[0]
    assert call.args[0] == "fn_existing_content_hashes"
    assert call.args[1] == {"p_hashes": ["h1", "h2", "h3"]}


def test_empty_input_short_circuits_without_rpc():
    """Test 2 — empty-guard short-circuit: returns set() and issues NO RPC."""
    ing = _make_ingester([])

    result = ing._hashes_already_ingested([])

    assert result == set()
    ing.client.rpc.assert_not_called()


def test_tolerates_malformed_and_none_rows():
    """Test 3 — malformed/None row tolerance: non-str / missing content_hash
    values are filtered out (isinstance guard), no crash."""
    ing = _make_ingester(
        [
            {"content_hash": "h1"},
            {"content_hash": None},
            {},
            {"other": "x"},
        ]
    )

    result = ing._hashes_already_ingested(["h1", "h2"])

    assert result == {"h1"}
