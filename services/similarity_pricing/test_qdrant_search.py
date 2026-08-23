"""Regresja BEAUTY_AUDIT-hx85: payload Qdranta ma tylko booksy_id (zewnętrzny).
search_twins NIE MOŻE aliasować go na salon_id (wewnętrzny salons.id) —
Faza 8a porównuje salon_id z competitor_matches.competitor_salon_id.
"""
from __future__ import annotations

from types import SimpleNamespace

from .qdrant_search import search_twins


class _FakeQdrant:
    def __init__(self, points):
        self._points = points

    def query_batch_points(self, collection, requests):
        return [SimpleNamespace(points=list(self._points)) for _ in requests]


def _pt(pid, booksy_id, score=0.9):
    return SimpleNamespace(
        id=pid, score=score,
        payload={"booksy_id": booksy_id, "service_name": "Manicure", "price_grosze": 5000,
                 "duration_minutes": 30, "category_name": "Paznokcie", "is_package": False},
    )


def test_sample_salon_id_is_not_booksy_id():
    client = _FakeQdrant([_pt(10, 239352), _pt(11, 116829)])
    out = search_twins([1], [239352, 116829], subject_embeddings={1: [0.1, 0.2]}, client=client)
    samples = out[1]
    assert [s["booksy_id"] for s in samples] == [239352, 116829]
    # salon_id to salons.id — z payloadu go NIE MA, więc None; caller domapuje.
    assert all(s["salon_id"] is None for s in samples)
    assert all(s["salon_id"] != s["booksy_id"] for s in samples)


def test_subject_is_not_its_own_twin():
    client = _FakeQdrant([_pt(1, 1), _pt(2, 2)])
    out = search_twins([1], [1, 2], subject_embeddings={1: [0.1]}, client=client)
    assert [s["service_id"] for s in out[1]] == [2]


class _RecordingQdrant(_FakeQdrant):
    def query_batch_points(self, collection, requests):
        self.requests = requests
        return super().query_batch_points(collection, requests)


def test_exact_flag_sets_search_params():
    """Bez indeksu payload na booksy_id filtrowany HNSW gubi całe salony dla
    małej puli — Faza 8a wymusza exact (BEAUTY_AUDIT-xi18)."""
    client = _RecordingQdrant([_pt(10, 239352)])
    search_twins([1], [239352], subject_embeddings={1: [0.1]}, client=client)
    assert all(r.params is None for r in client.requests)
    search_twins([1], [239352], subject_embeddings={1: [0.1]}, client=client, exact=True)
    assert all(r.params is not None and r.params.exact is True for r in client.requests)
