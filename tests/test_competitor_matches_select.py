"""BEAUTY_AUDIT-yvtd — Faza 8a pola gubione w SELECT competitor_matches.

Faza 8a liczy i zapisuje `verified_match_count` + `bucket_pre_verify`
(update_competitor_matches_verify_buckets), ale odczyt
SupabaseService.get_competitor_matches nie prosił o te kolumny. PostgREST
zwraca WYŁĄCZNIE kolumny z ?select=..., więc wiersze wracały bez nich, a
_build_competitor_profiles wstawiało do raportu twarde None — mimo poprawnie
policzonych wartości w bazie.

Atrapa klienta poniżej odwzorowuje tę własność PostgREST (projekcja po liście
kolumn), dzięki czemu test pada na kodzie sprzed poprawki.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from pipelines.competitor_synthesis import _build_competitor_profiles
from services.supabase import SupabaseService

# Realistyczny wiersz competitor_matches po Fazie 8a (mig 081).
_MATCH_ROW: dict[str, Any] = {
    "id": 2840,
    "report_id": 77,
    "competitor_salon_id": 3822,
    "composite_score": 71.5,
    "bucket": "direct",
    "bucket_pre_verify": "direct",
    "counts_in_aggregates": True,
    "verified_match_count": 12,
    "similarity_scores": {
        "focus_tid_sim": 0.61,
        "tid_set_overlap_asym": 0.72,
        "profile_overlap_sim": 0.83,
    },
    "distance_km": 1.4,
    "sort_order": 0,
}

_SALON_ROW: dict[str, Any] = {
    "id": 3822,
    "booksy_id": 239352,
    "name": "Studio Konkurent",
    "reviews_rank": 4.8,
    "reviews_count": 210,
    "city": "Warszawa",
    "thumbnail_photo": "https://example.invalid/thumb.jpg",
    "latitude": 52.2297,
    "longitude": 21.0122,
}


class _FakeQuery:
    """Łańcuch zapytań PostgREST: zapamiętuje SELECT i rzutuje wiersze do
    zamówionych kolumn (dokładnie jak ?select= po stronie serwera)."""

    def __init__(self, table: str, rows: list[dict[str, Any]], recorder: dict[str, str]) -> None:
        self._table = table
        self._rows = rows
        self._recorder = recorder
        self._columns: list[str] | None = None

    def select(self, columns: str) -> "_FakeQuery":
        self._recorder[self._table] = columns
        self._columns = [c.strip() for c in columns.split(",") if c.strip()]
        return self

    def eq(self, *_a: Any, **_k: Any) -> "_FakeQuery":
        return self

    def in_(self, *_a: Any, **_k: Any) -> "_FakeQuery":
        return self

    def order(self, *_a: Any, **_k: Any) -> "_FakeQuery":
        return self

    def limit(self, *_a: Any, **_k: Any) -> "_FakeQuery":
        return self

    def execute(self) -> SimpleNamespace:
        if self._columns is None or self._columns == ["*"]:
            return SimpleNamespace(data=[dict(r) for r in self._rows])
        return SimpleNamespace(
            data=[{c: r[c] for c in self._columns if c in r} for r in self._rows]
        )


class _FakeClient:
    def __init__(self, match_rows: list[dict[str, Any]]) -> None:
        self.selects: dict[str, str] = {}
        self._tables = {
            "competitor_matches": match_rows,
            "salons": [_SALON_ROW],
        }

    def table(self, name: str) -> _FakeQuery:
        return _FakeQuery(name, self._tables.get(name, []), self.selects)


def _service(match_rows: list[dict[str, Any]] | None = None) -> tuple[SupabaseService, _FakeClient]:
    # Pomijamy __init__ (buduje żywego klienta Supabase).
    service = object.__new__(SupabaseService)
    client = _FakeClient(match_rows if match_rows is not None else [dict(_MATCH_ROW)])
    service.client = client  # type: ignore[attr-defined]
    return service, client


async def test_select_asks_for_faza_8a_columns() -> None:
    """SELECT musi prosić o verified_match_count i bucket_pre_verify —
    bez tego PostgREST ich nie zwróci, choćby były zapisane."""
    service, client = _service()

    await service.get_competitor_matches(77)

    requested = {c.strip() for c in client.selects["competitor_matches"].split(",")}
    assert "verified_match_count" in requested
    assert "bucket_pre_verify" in requested


async def test_profiles_carry_verified_count_end_to_end() -> None:
    """Kształt end-to-end: to, co wraca z get_competitor_matches, po przejściu
    przez _build_competitor_profiles ma NIEPUSTE pola Fazy 8a."""
    service, _ = _service()

    matches = await service.get_competitor_matches(77)
    profiles = _build_competitor_profiles(matches)

    assert len(profiles) == 1
    profile = profiles[0]
    assert profile["verified_match_count"] == 12
    assert isinstance(profile["verified_match_count"], int)
    assert profile["bucket_pre_verify"] == "direct"
    # Sanity: reszta wzbogacenia z salons nadal działa (brak regresji w joinie).
    assert profile["name"] == "Studio Konkurent"
    assert profile["booksy_id"] == 239352


async def test_excluded_match_surfaces_pre_verify_intent() -> None:
    """Konkurent odrzucony przez weryfikację ma bucket 'alternative', ale
    raport musi nieść jego pierwotny zamiar (bucket_pre_verify) i licznik —
    inaczej UI nie potrafi wyjaśnić, czemu jest 'Alternatywą'."""
    row = dict(_MATCH_ROW)
    row.update(
        bucket="excluded",
        bucket_pre_verify="cluster",
        verified_match_count=2,
        counts_in_aggregates=False,
    )
    service, _ = _service([row])

    profiles = _build_competitor_profiles(await service.get_competitor_matches(77))

    profile = profiles[0]
    assert profile["bucket"] == "alternative"
    assert profile["is_alternative"] is True
    assert profile["bucket_pre_verify"] == "cluster"
    assert profile["verified_match_count"] == 2
