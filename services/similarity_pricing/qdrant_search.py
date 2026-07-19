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

import math
import os
from typing import Any

from qdrant_client import QdrantClient, models

COLLECTION = "salon_services"
_client: QdrantClient | None = None


def _cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity dwóch wektorów. Wektory Qdranta są znormalizowane
    (cosine collection), więc iloczyn skalarny ≈ cosine, ale liczymy pełny
    wzór dla odporności."""
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def fetch_twin_vectors(
    service_ids: list[int], client: QdrantClient | None = None
) -> dict[int, list[float]]:
    """service_id → wektor z Qdranta (retrieve with_vectors).

    Źródło peer_max_sim dla COHERENCE GUARD — TO SAMO co twins (Qdrant), więc
    bez rozjazdu z Postgresem: fn_pairwise (mig 151) po mig 149 miał ~7%
    pokrycia twin-id (embeddingi historii zmiecione), Qdrant trzyma punkty
    chain-headów → ~100% (zmierzone 2026-07-19). Jedna runda retrieve per
    klaster subjectu (~80 id, ms). Wektory nie wchodzą do QueryRequest —
    ta wersja qdrant-client nie przyjmuje tam with_vectors."""
    if len(service_ids) < 2:
        return {}
    try:
        qc = client or get_client()  # w try: brak QDRANT_URL/klienta = abstain
        pts = qc.retrieve(COLLECTION, ids=[int(x) for x in service_ids],
                          with_vectors=True, with_payload=False)
    except Exception:  # noqa: BLE001 — brak peer = abstain, nie wywala raportu
        return {}
    return {int(p.id): p.vector for p in pts if p.vector}


def compute_peer_max_sims(
    service_ids: list[int], vectors: dict[int, list[float]]
) -> dict[int, float]:
    """peer_max_sim per usługa = max cosine do INNEJ usługi z listy.

    Czysta funkcja (bez I/O) — testowalna offline. `vectors` z fetch_twin_vectors
    (Qdrant). Usługi bez wektora są pomijane → warstwa coherence robi abstain.
    Zasila COHERENCE GUARD (layer_coherence) — patrz jego docstring.
    """
    vecs = [(int(sid), vectors[int(sid)]) for sid in service_ids
            if int(sid) in vectors and vectors[int(sid)]]
    out: dict[int, float] = {}
    for i, (sid_a, va) in enumerate(vecs):
        best = -1.0
        for j, (sid_b, vb) in enumerate(vecs):
            if i == j:
                continue
            sc = _cosine(va, vb)
            if sc > best:
                best = sc
        if best >= 0.0:
            out[sid_a] = round(best, 4)
    return out


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
    subject_embeddings: dict[int, list[float]] | None = None,
    limit: int = 60,
    min_similarity: float = 0.82,
    client: QdrantClient | None = None,
) -> dict[int, list[dict[str, Any]]]:
    """Dla każdej usługi subject znajdź tożsamych bliźniaków wśród konkurentów.

    Args:
        subject_service_ids: id usług salonu-klienta.
        competitor_booksy_ids: pula konkurentów (wybrani LUB w promieniu N km).
        subject_embeddings: {service_id: vector} embeddingi subjectów. WAŻNE —
            subject zwykle pochodzi z AUDIT scrape, którego NIE ma w Qdrant (indeks
            trzyma tylko chain-head), więc embeddingi trzeba podać z Postgresa.
            Gdy None, fallback do retrieve z Qdrant (działa tylko dla chain-head).
        limit: max bliźniaków per usługa.
        min_similarity: próg cosine (score_threshold).

    Returns:
        {subject_service_id: [sample]} w kształcie oczekiwanym przez
        engine.compute_market_price. salon_name dodaje caller (lookup po booksy_id).
    """
    out: dict[int, list[dict[str, Any]]] = {int(s): [] for s in subject_service_ids}
    if not subject_service_ids or not competitor_booksy_ids:
        return out

    qc = client or get_client()

    # 1. Embeddingi subjectów — z Postgresa (preferowane) lub fallback retrieve z Qdrant.
    if subject_embeddings:
        subj_vec: dict[int, list[float]] = {
            int(s): subject_embeddings[int(s)]
            for s in subject_service_ids
            if int(s) in subject_embeddings and subject_embeddings[int(s)]
        }
    else:
        subj_points = qc.retrieve(
            COLLECTION, ids=list(subject_service_ids), with_vectors=True, with_payload=False
        )
        subj_vec = {int(p.id): p.vector for p in subj_points if p.vector}
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
