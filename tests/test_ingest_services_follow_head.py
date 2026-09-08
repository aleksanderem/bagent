"""mig 194 — wiersze usług idą za głową łańcucha.

Ingest woła po wstawieniu dzieci skanu funkcję SQL
``fn_scrape_services_dedup_against_prev(p_new)``; wynik wraca jako tekst.
Tu pilnujemy cienkiej warstwy Pythona: nazwa RPC i argument, rozpakowanie
odpowiedzi (skalar / lista / dict — PostgREST bywa różny) oraz to, że błąd
RPC jest NIEFATALNY (zwraca ``error``, nie wyjątek) — bo alternatywą byłby
unwind nowego skanu, a po przepięciu skasowałby jedyne wiersze salonu.
"""
from unittest.mock import MagicMock

import pytest

from scripts.ingest_salon_jsons import SalonJsonIngester


def _ingester(rpc_data):
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = rpc_data
    return SalonJsonIngester(client=client, batch_tag=None, dry_run=False)


@pytest.mark.parametrize(
    "rpc_data, expected",
    [
        ("moved", "moved"),
        (["kept"], "kept"),
        ([{"fn_scrape_services_dedup_against_prev": "kept_audit_prev"}], "kept_audit_prev"),
        ([], "unknown"),
        (None, "unknown"),
    ],
)
def test_dedup_rpc_name_arg_and_result_unpacking(rpc_data, expected):
    ing = _ingester(rpc_data)
    assert ing._dedup_services_against_prev("11111111-2222-3333-4444-555555555555") == expected
    call = ing.client.rpc.call_args
    assert call.args[0] == "fn_scrape_services_dedup_against_prev"
    assert call.args[1] == {"p_new": "11111111-2222-3333-4444-555555555555"}


def test_dedup_rpc_error_is_non_fatal():
    ing = _ingester("moved")
    ing.client.rpc.return_value.execute.side_effect = RuntimeError("boom")
    assert ing._dedup_services_against_prev("abc") == "error"
