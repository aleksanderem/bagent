"""Wyszukiwanie tożsamych usług przez Qdrant — zastępuje SQL-owy fn_find_related_v2.

Embeddingi chain-head usług są zindeksowane w Qdrant (collection "salon_services",
HNSW cosine) na osobnym serwerze (atlas). Zamiast full-scan cosine w Postgresie
(11s/usługa), robimy kNN przez indeks (ms). Filtr po `booksy_id` w payloadzie
obsługuje OBA scope'y — "wybrani konkurenci" i "promień N km" — tym samym
mechanizmem, różniąc się tylko listą booksy_id.

salon_name NIE jest w payloadzie (świeży lookup po booksy_id w report_pricing) —
payload mniejszy, nazwy zawsze aktualne.
"""
from __future__ import annotations

import os
from typing import Any

from qdrant_client import QdrantClient, models

COLLECTION = "salon_services"
_client: QdrantClient | None = None


def get_client() -> QdrantClient:
    """Singleton klienta Qdrant (z env QDRANT_URL / QDRANT_API_KEY)."""
    global _client
    if _client is None:
        _client = QdrantClient(
            url=os.environ["QDRANT_URL"],
            api_key=os.environ.get("QDRANT_API_KEY"),
            timeout=60,
        )
    return _client


def search_twins(
    subject_service_ids: list[int],
    competitor_booksy_ids: list[int],
    *,
    limit: int = 60,
    min_similarity: float = 0.82,
    client: QdrantClient | None = None,
) -> dict[int, list[dict[str, Any]]]:
    """Dla każdej usługi subject znajdź tożsamych bliźniaków wśród konkurentów.

    Args:
        subject_service_ids: id usług salonu-klienta (= salon_scrape_services.id,
            równe point id w Qdrant).
        competitor_booksy_ids: pula konkurentów (wybrani LUB w promieniu N km).
        limit: max bliźniaków per usługa.
        min_similarity: próg cosine (score_threshold).

    Returns:
        {subject_service_id: [sample]} gdzie sample ma kształt zgodny z tym,
        czego oczekuje engine.compute_market_price (service_id, booksy_id,
        service_name, price_grosze, duration_minutes, category_name, is_package,
        similarity). salon_name dodaje caller (lookup po booksy_id).
    """
    out: dict[int, list[dict[str, Any]]] = {int(s): [] for s in subject_service_ids}
    if not subject_service_ids or not competitor_booksy_ids:
        return out

    qc = client or get_client()

    # 1. Pobierz embeddingi subjectów (są w tej samej kolekcji — chain-head).
    subj_points = qc.retrieve(
        COLLECTION, ids=list(subject_service_ids), with_vectors=True, with_payload=False
    )
    subj_vec: dict[int, list[float]] = {int(p.id): p.vector for p in subj_points if p.vector}
    if not subj_vec:
        return out

    # 2. Batch kNN — jedna runda do atlasa dla wszystkich usług. Filtr booksy_id.
    flt = models.Filter(must=[models.FieldCondition(
        key="booksy_id", match=models.MatchAny(any=list(competitor_booksy_ids))
    )])
    order = [int(s) for s in subject_service_ids if int(s) in subj_vec]
    requests = [
        models.QueryRequest(
            query=subj_vec[sid], filter=flt, limit=limit,
            score_threshold=min_similarity, with_payload=True,
        )
        for sid in order
    ]
    responses = qc.query_batch_points(COLLECTION, requests=requests)

    # 3. Zmapuj wyniki na samples per subject.
    for sid, resp in zip(order, responses):
        for pt in resp.points:
            if int(pt.id) == sid:
                continue  # subject nie jest własnym bliźniakiem
            pl = pt.payload or {}
            out[sid].append({
                "service_id": int(pt.id),
                "booksy_id": pl.get("booksy_id"),
                "salon_id": pl.get("booksy_id"),  # salon_name dołoży caller
                "salon_name": "",
                "service_name": pl.get("service_name") or "",
                "price_grosze": pl.get("price_grosze"),
                "duration_minutes": pl.get("duration_minutes"),
                "category_name": pl.get("category_name"),
                "is_package": bool(pl.get("is_package", False)),
                "similarity": round(float(pt.score), 4),
            })
    return out
