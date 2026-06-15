"""Element A (2026-06-15 entity-resolution fix): MethodClassifier.warmup()
must page through the WHOLE treatment_methods dictionary.

Regression guard: the old single `.execute()` was silently capped by
PostgREST's PGRST_DB_MAX_ROWS=1000, so the alias index saw only the first
1000 of ~23k methods (~4% of the dictionary). These tests assert warmup
pages past 1000 rows (stable order by id) and caches the rows per process.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from services import method_classifier as mc
from services.method_classifier import MethodClassifier


def _make_row(i: int) -> dict:
    return {
        "id": i,
        "canonical_name": f"method_{i}",
        "display_name": f"Method {i}",
        "category": "inny",
        "method_type": "technique",
        "brand_family": None,
        "aliases": [f"alias {i}"],
        "source": "llm_inferred",
    }


class _FakeQuery:
    """Captures the .order()/.range() chain and slices the row list on
    .execute() exactly as PostgREST would for a bounded Range request."""

    def __init__(self, rows: list[dict], calls: dict):
        self._rows = rows
        self._calls = calls
        self._start = 0
        self._end: int | None = None

    def select(self, *a, **k):
        return self

    def order(self, col, **k):
        self._calls["order_cols"].append(col)
        return self

    def range(self, start, end):
        self._start = start
        self._end = end
        self._calls["ranges"].append((start, end))
        return self

    def execute(self):
        end = self._end if self._end is not None else len(self._rows)
        page = self._rows[self._start : end + 1]
        return MagicMock(data=page)


class _FakeClient:
    def __init__(self, rows: list[dict], calls: dict):
        self._rows = rows
        self._calls = calls
        self.table_calls: list[str] = []

    def table(self, name):
        self.table_calls.append(name)
        return _FakeQuery(self._rows, self._calls)


@pytest.fixture(autouse=True)
def _reset_cache():
    mc._reset_warmup_cache()
    yield
    mc._reset_warmup_cache()


@pytest.mark.asyncio
async def test_warmup_pages_beyond_1000_rows():
    """2350 rows must all load — proving warmup walks past the 1000 cap."""
    rows = [_make_row(i) for i in range(1, 2351)]  # 2350 rows
    calls = {"ranges": [], "order_cols": []}
    supa = MagicMock()
    supa.client = _FakeClient(rows, calls)

    clf = MethodClassifier(supabase=supa)
    await clf.warmup(force_reload=True)

    # All rows loaded (would be 1000 with the old single .execute()).
    assert len(clf._methods) == 2350
    # Each method's alias landed in the index (proves >1000 registered).
    assert len(clf._alias_index) >= 2350
    # Paged: 2350 → windows [0-999], [1000-1999], [2000-2999(short)].
    assert len(calls["ranges"]) == 3
    assert calls["ranges"][0] == (0, 999)
    assert calls["ranges"][1] == (1000, 1999)
    # Stable paging requires an explicit order by id on every page.
    assert calls["order_cols"] and all(c == "id" for c in calls["order_cols"])


@pytest.mark.asyncio
async def test_warmup_stops_on_short_page():
    """A single sub-1000 page must not trigger a second range request."""
    rows = [_make_row(i) for i in range(1, 451)]  # 450 < 1000
    calls = {"ranges": [], "order_cols": []}
    supa = MagicMock()
    supa.client = _FakeClient(rows, calls)

    clf = MethodClassifier(supabase=supa)
    await clf.warmup(force_reload=True)

    assert len(clf._methods) == 450
    assert len(calls["ranges"]) == 1  # short first page = done


@pytest.mark.asyncio
async def test_warmup_caches_rows_across_instances():
    """Second instance reuses the process-level row cache — no re-fetch."""
    rows = [_make_row(i) for i in range(1, 1201)]  # 1200 → 2 pages
    calls = {"ranges": [], "order_cols": []}
    fake_client = _FakeClient(rows, calls)
    supa = MagicMock()
    supa.client = fake_client

    clf1 = MethodClassifier(supabase=supa)
    await clf1.warmup()  # cold → fetches
    n_table_calls_after_first = len(fake_client.table_calls)
    assert n_table_calls_after_first >= 2  # 2 pages

    clf2 = MethodClassifier(supabase=supa)
    await clf2.warmup()  # warm → cache hit, no new table() calls
    assert len(fake_client.table_calls) == n_table_calls_after_first
    # But clf2 still built its own full index from the cached rows.
    assert len(clf2._methods) == 1200
