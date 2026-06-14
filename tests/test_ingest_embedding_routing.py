"""TDD tests for the scrape-ingest embedding wiring + chain-head gate widening
(beads BEAUTY_AUDIT-lrh Phase 2).

Two behaviors under test in scripts/ingest_salon_jsons.py:

  ROUTING (_insert_services persist block) — the inline embed now returns
  `(vectors, space)`; the persist block routes by space:
    - "mmlw-e5-large"  -> RPC "bulk_update_service_embeddings_mmlw"
    - "openai-3-small" -> RPC "bulk_update_service_embeddings"  (UNCHANGED — proves
                          the OpenAI-healthy path is byte-identical)
    - None             -> NEITHER RPC called (today's "leave NULL, cron catches up")

  GATE (_promote_chain_head) — the missing-embedding count now ANDs two
  filters: `.is_("embedding_applied_at","null")` AND
  `.is_("embedding_mmlw_applied_at","null")`. A row embedded in EITHER space no
  longer counts as missing, so a backup-embedded scrape can promote during an
  OpenAI outage; a row missing BOTH still blocks the promote.

The ingester is built with a MagicMock client (no Supabase). `embed_texts` is
monkeypatched so the real `_embed_service_names_sync` compose+guards run too.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scripts.ingest_salon_jsons import SalonJsonIngester


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _one_service_business() -> dict:
    """Minimal Booksy business: one category, one service, NO variants (so
    _insert_service_variants is a no-op and never touches .rpc)."""
    return {
        "service_categories": [
            {
                "id": 1,
                "name": "Manicure",
                "order": 0,
                "services": [
                    {
                        "id": 101,
                        "name": "Manicure hybrydowy",
                        "description": "Trwaly kolor",
                        "treatment_id": 555,
                        "variants": [],
                    }
                ],
            }
        ]
    }


def _make_ingester() -> SalonJsonIngester:
    client = MagicMock()
    # Insert returns one row so inserted_rows is populated.
    client.table.return_value.insert.return_value.execute.return_value.data = [
        {"id": 1, "name": "Manicure hybrydowy", "description": "Trwaly kolor"}
    ]
    # RPC chain resolves to an empty-data result.
    client.rpc.return_value.execute.return_value.data = []
    return SalonJsonIngester(client=client, batch_tag=None, dry_run=False)


def _rpc_names(client: MagicMock) -> list[str]:
    """First positional arg of every client.rpc(...) call."""
    return [c.args[0] for c in client.rpc.call_args_list if c.args]


# ---------------------------------------------------------------------------
# ROUTING tests
# ---------------------------------------------------------------------------

def test_mmlw_space_routes_to_mmlw_rpc(monkeypatch):
    ing = _make_ingester()
    monkeypatch.setattr(
        "scripts.ingest_salon_jsons.embed_texts",
        lambda inputs: ([[0.1, 0.2, 0.3]], "mmlw-e5-large"),
    )

    ing._insert_services(_one_service_business(), scrape_id="s1", booksy_id=42, scraped_at="2026-06-14T00:00:00Z")

    names = _rpc_names(ing.client)
    assert "bulk_update_service_embeddings_mmlw" in names
    assert "bulk_update_service_embeddings" not in names


def test_openai_space_routes_to_openai_rpc_unchanged(monkeypatch):
    """REGRESSION GUARD: OpenAI-healthy path must call the SAME RPC as today
    with the SAME payload shape."""
    ing = _make_ingester()
    monkeypatch.setattr(
        "scripts.ingest_salon_jsons.embed_texts",
        lambda inputs: ([[0.1, 0.2, 0.3]], "openai-3-small"),
    )

    ing._insert_services(_one_service_business(), scrape_id="s1", booksy_id=42, scraped_at="2026-06-14T00:00:00Z")

    names = _rpc_names(ing.client)
    assert "bulk_update_service_embeddings" in names
    assert "bulk_update_service_embeddings_mmlw" not in names
    # Payload shape unchanged: [{"id": int, "embedding": [...]}].
    openai_call = next(
        c for c in ing.client.rpc.call_args_list
        if c.args and c.args[0] == "bulk_update_service_embeddings"
    )
    payload = openai_call.args[1]["payloads"]
    assert payload == [{"id": 1, "embedding": [0.1, 0.2, 0.3]}]


def test_none_embed_calls_no_rpc(monkeypatch):
    ing = _make_ingester()
    monkeypatch.setattr(
        "scripts.ingest_salon_jsons.embed_texts",
        lambda inputs: None,
    )

    ing._insert_services(_one_service_business(), scrape_id="s1", booksy_id=42, scraped_at="2026-06-14T00:00:00Z")

    # No embedding RPC issued when embed returns None.
    assert _rpc_names(ing.client) == []


# ---------------------------------------------------------------------------
# GATE tests
# ---------------------------------------------------------------------------

def _gate_client(missing_count: int) -> MagicMock:
    """Build a client whose
    .table(...).select(...).eq(...).is_(...).is_(...).execute().count
    resolves to `missing_count`, recording the .is_ calls for assertion."""
    client = MagicMock()
    select_chain = MagicMock()
    eq_chain = MagicMock()
    is_node = MagicMock()  # SAME node returned by both .is_ so the chain is navigable
    exec_result = MagicMock()
    exec_result.count = missing_count

    client.table.return_value.select.return_value = select_chain
    select_chain.eq.return_value = eq_chain
    eq_chain.is_.return_value = is_node
    is_node.is_.return_value = is_node
    is_node.execute.return_value = exec_result
    # capture the .is_ recorder for assertions (both eq_chain.is_ and is_node.is_)
    client._eq_is = eq_chain.is_
    client._node_is = is_node.is_
    return client


def _is_call_columns(client: MagicMock) -> set[str]:
    cols: set[str] = set()
    for rec in (client._eq_is, client._node_is):
        for c in rec.call_args_list:
            if c.args:
                cols.add(c.args[0])
    return cols


def test_gate_applies_both_embedding_filters(monkeypatch):
    """count=0 (no row missing BOTH) -> promote proceeds to first-head UPDATE,
    and BOTH embedding columns were filtered."""
    client = _gate_client(missing_count=0)
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)

    ing._promote_chain_head(new_scrape_id="s1", prev_head=None, new_payload={})

    cols = _is_call_columns(client)
    assert "embedding_applied_at" in cols
    assert "embedding_mmlw_applied_at" in cols
    # prev_head=None + gate passed -> first-head UPDATE issued.
    assert client.table.return_value.update.called


def test_gate_blocks_when_both_columns_null(monkeypatch):
    """count=2 (rows missing BOTH spaces) -> promote refuses (no UPDATE)."""
    client = _gate_client(missing_count=2)
    ing = SalonJsonIngester(client=client, batch_tag=None, dry_run=False)

    ing._promote_chain_head(new_scrape_id="s1", prev_head=None, new_payload={})

    assert not client.table.return_value.update.called
