"""Parytet testy dla gather_market_context_samples_batch (quick 260613-cl2).

Batch wersja gather_market_context_samples elimuje N+1 w fazie pricing:
zamiast N pojedynczych RPC calli (jeden per subject_only service) robi
JEDEN call fn_find_related_competitor_services_batch i grupuje plaska liste
wynikow po subject_service_id w dict[int, list[dict]].

Te testy gwarantuja, ze batch zwraca DOKLADNIE ten sam wynik per-subject co
N pojedynczych calli starej fn:
  Test 1 — parytet ksztaltu sample dict (te same klucze, round(sim,4),
           relation="semantic_match", price_grosze int).
  Test 2 — parytet semantyczny vs N-single: dla 3 subjectow batch_result[sid]
           == single_result_for(sid) (kolejnosc DESC similarity, skip no-price,
           threshold zachowane).
  Test 3 — fail-closed: RPC exception -> dict pustych list dla wszystkich
           subject_ids; pusta lista subject_ids LUB brak competitor_booksy_ids
           -> brak wywolania RPC.
  Test 4 — skip no-price: wiersz z price_grosze=None pomijany w grupowaniu.

Zero zywych wywolan RPC — supabase.client.rpc zmockowany.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from services.market_context import (
    DEFAULT_LIMIT,
    DEFAULT_MIN_SIMILARITY,
    gather_market_context_samples,
    gather_market_context_samples_batch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(
    *,
    subject_service_id: int | None,
    salon_id: int,
    service_id: int,
    name: str,
    price_grosze: int | None = 10000,
    duration_minutes: int | None = 60,
    similarity: float = 0.80,
) -> dict[str, Any]:
    """Raw RPC row shape — batch fn additionally carries subject_service_id."""
    return {
        "subject_service_id": subject_service_id,
        "service_id": service_id,
        "scrape_id": "00000000-0000-0000-0000-000000000000",
        "salon_id": salon_id,
        "booksy_id": salon_id + 1000,
        "salon_name": f"Salon{salon_id}",
        "service_name": name,
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "similarity": similarity,
    }


def _single_row(row: dict[str, Any]) -> dict[str, Any]:
    """Strip subject_service_id — shape returned by the OLD single RPC."""
    out = dict(row)
    out.pop("subject_service_id", None)
    return out


def _mock_supabase_returning(rows: list[dict[str, Any]]) -> MagicMock:
    """Stub SupabaseService whose .client.rpc(...).execute() returns rows.

    Records the call args on the returned mock so tests can assert the RPC
    was (or was NOT) invoked and with what parameters.
    """
    supabase = MagicMock()
    execute_result = MagicMock()
    execute_result.data = rows
    supabase.client.rpc.return_value.execute.return_value = execute_result
    return supabase


def _mock_supabase_raising() -> MagicMock:
    supabase = MagicMock()
    supabase.client.rpc.return_value.execute.side_effect = RuntimeError(
        "RPC boom (mock)"
    )
    return supabase


# ---------------------------------------------------------------------------
# Test 1 — sample shape parity (single subject)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_sample_shape_matches_old_fn() -> None:
    rows = [
        _row(subject_service_id=111, salon_id=1, service_id=9001,
             name="Masaz relaksacyjny", price_grosze=12000, similarity=0.8123),
        _row(subject_service_id=111, salon_id=2, service_id=9002,
             name="Masaz klasyczny", price_grosze=9500, similarity=0.6712),
    ]
    supabase = _mock_supabase_returning(rows)

    result = await gather_market_context_samples_batch(
        supabase, [111], [1001, 1002],
    )

    assert set(result.keys()) == {111}
    samples = result[111]
    assert len(samples) == 2
    first = samples[0]
    # Exact key set + shape contract (same as old fn).
    assert set(first.keys()) == {
        "salon_id", "salon_name", "booksy_id", "service_id", "service_name",
        "price_grosze", "duration_minutes", "relation", "similarity",
    }
    assert first["price_grosze"] == 12000
    assert isinstance(first["price_grosze"], int)
    assert first["relation"] == "semantic_match"
    assert first["similarity"] == 0.8123  # round(,4) preserved
    assert first["service_name"] == "Masaz relaksacyjny"
    assert first["salon_name"] == "Salon1"
    assert first["booksy_id"] == 1001


# ---------------------------------------------------------------------------
# Test 2 — semantic parity vs N single calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_equals_n_single_calls() -> None:
    # Three subjects, each with its own match rows. The batch RPC returns the
    # flat concatenation (tagged with subject_service_id); each old single RPC
    # returns only its subject's rows (untagged). Result per subject MUST match.
    rows_by_subject: dict[int, list[dict[str, Any]]] = {
        201: [
            _row(subject_service_id=201, salon_id=1, service_id=1,
                 name="A1", price_grosze=10000, similarity=0.90),
            _row(subject_service_id=201, salon_id=2, service_id=2,
                 name="A2", price_grosze=11000, similarity=0.70),
        ],
        202: [
            _row(subject_service_id=202, salon_id=3, service_id=3,
                 name="B1", price_grosze=20000, similarity=0.85),
        ],
        203: [],  # no matches for this subject
    }
    flat_batch_rows = [r for rows in rows_by_subject.values() for r in rows]

    # Batch path — one call, flat rows.
    batch_supabase = _mock_supabase_returning(flat_batch_rows)
    batch_result = await gather_market_context_samples_batch(
        batch_supabase, [201, 202, 203], [1001, 1002, 1003],
    )

    # Single path — one mock per subject, returns that subject's rows
    # (stripped of subject_service_id, mirroring the old RPC contract).
    for sid in (201, 202, 203):
        single_supabase = _mock_supabase_returning(
            [_single_row(r) for r in rows_by_subject[sid]]
        )
        single_result = await gather_market_context_samples(
            single_supabase, sid, [1001, 1002, 1003],
        )
        assert batch_result[sid] == single_result, f"mismatch for subject {sid}"

    # Batch made exactly ONE RPC call (not three).
    assert batch_supabase.client.rpc.call_count == 1
    call_args = batch_supabase.client.rpc.call_args
    assert call_args.args[0] == "fn_find_related_competitor_services_batch"
    params = call_args.args[1]
    assert params["p_subject_service_ids"] == [201, 202, 203]
    assert params["p_competitor_booksy_ids"] == [1001, 1002, 1003]
    assert params["p_limit"] == DEFAULT_LIMIT
    assert params["p_min_similarity"] == DEFAULT_MIN_SIMILARITY


# ---------------------------------------------------------------------------
# Test 3 — fail-closed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rpc_exception_fails_closed_to_empty_lists() -> None:
    supabase = _mock_supabase_raising()
    result = await gather_market_context_samples_batch(
        supabase, [301, 302], [1001],
    )
    # Every requested subject gets an empty list — no crash, no propagation.
    assert result == {301: [], 302: []}


@pytest.mark.asyncio
async def test_empty_subject_ids_skips_rpc() -> None:
    supabase = _mock_supabase_returning([])
    result = await gather_market_context_samples_batch(supabase, [], [1001])
    assert result == {}
    supabase.client.rpc.assert_not_called()


@pytest.mark.asyncio
async def test_no_competitor_ids_skips_rpc() -> None:
    supabase = _mock_supabase_returning([])
    result = await gather_market_context_samples_batch(supabase, [401, 402], [])
    # Empty competitor scope -> empty list per subject, no RPC round-trip.
    assert result == {401: [], 402: []}
    supabase.client.rpc.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4 — skip no-price rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_price_rows_skipped() -> None:
    rows = [
        _row(subject_service_id=501, salon_id=1, service_id=1,
             name="With price", price_grosze=10000, similarity=0.80),
        _row(subject_service_id=501, salon_id=2, service_id=2,
             name="No price", price_grosze=None, similarity=0.75),
    ]
    supabase = _mock_supabase_returning(rows)
    result = await gather_market_context_samples_batch(supabase, [501], [1001])
    samples = result[501]
    assert len(samples) == 1
    assert samples[0]["service_name"] == "With price"
