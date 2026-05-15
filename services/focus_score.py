"""Salon focus / portfolio scoring — pure-Python module (zero Supabase deps).

Wykorzystywane w dwóch miejscach:
  1. Backfill skrypt + refresh cron — pre-compute do salons.portfolio_embedding +
     salons.focus_distribution + salons.focus_variant_distribution.
  2. Competitor selection — gdy ktoś nie jest pre-computed, on-the-fly fallback.

Empiryczna walidacja na Beauty4ever (2026-05-15):
  - Top focus services correctly identify medical aesthetic / body shape core
  - Marginalne services (manicure, EstGen) baseline 0.05 → znikają z portfolio
  - Focus_tid_sim oddziela "fałszywych konkurentów" (Pięknoteka, Now You)
    od "prawdziwych" (Skin & Body Care, Skinline) których aktualny tid-overlap
    algorytm omija.

Cel: pojedynczy modul, każda funkcja czysta (no side effects), łatwy do test.
"""
from __future__ import annotations

import math
from typing import Any, Iterable

import numpy as np


# ---------------------------------------------------------------------------
# Service-level focus weight
# ---------------------------------------------------------------------------

_BASELINE_WEIGHT = 0.05  # każdej usłudze damy min taką wagę żeby się nie wyzerowała
_W_TOP_SERVICE = 0.40    # Booksy own popularity signal — najmocniejszy
_W_DESCRIPTION = 0.20    # salon inwestuje w opis tego co robi
_W_PHOTO = 0.15          # zdjęcia dla flagshipów
_W_VARIANT_DEPTH = 0.15  # salon z 8 wariantami botoksu robi go dużo


def _normalize_name(s: str | None) -> str:
    return (s or "").lower().strip()


def _is_in_top_services(svc_name: str, top_norm: set[str]) -> bool:
    if not svc_name or not top_norm:
        return False
    # Partial match: top "Botoks" matches service "Botoks 2 okolice"
    return any(t and (t in svc_name or svc_name in t) for t in top_norm)


def compute_service_focus_weights(
    services: list[dict[str, Any]],
    top_service_names: Iterable[str] | None,
) -> list[float]:
    """Per-service focus weight ∈ [0..1].

    Sygnały:
      0.05  baseline (każda usługa ma minimalną wagę)
      0.40  obecność w salon top_services (Booksy own popularity ranking)
      0.20  normalized description length (relative within salon)
      0.15  has at least one photo
      0.15  variant clustering depth — udział wariantów w parent_tid względem max
            wariantów per tid w salonie (proxy "ile salon różnicuje w tej kategorii")

    Args:
      services: lista dict z polami: name, description, photos (list),
        booksy_treatment_id, variant_id
      top_service_names: lista stringów z `salons.top_service_names`

    Returns:
      Lista float tej samej długości co services, każdy 0..1.
    """
    if not services:
        return []

    top_norm = {_normalize_name(t) for t in (top_service_names or []) if t}

    # Description length normalization (max-relative within salon)
    desc_lens = [len((s.get("description") or "")) for s in services]
    max_desc = max(desc_lens) if desc_lens and max(desc_lens) > 0 else 1

    # Variants per parent_tid in this salon (count of distinct variants per tid)
    variants_per_tid: dict[int, int] = {}
    for s in services:
        tid = s.get("booksy_treatment_id")
        vid = s.get("variant_id")
        if tid is not None and vid is not None:
            variants_per_tid[tid] = variants_per_tid.get(tid, 0) + 1
    max_variants = max(variants_per_tid.values()) if variants_per_tid else 1

    weights: list[float] = []
    for s in services:
        w = _BASELINE_WEIGHT

        svc_name = _normalize_name(s.get("name", ""))
        if _is_in_top_services(svc_name, top_norm):
            w += _W_TOP_SERVICE

        desc_len = len((s.get("description") or ""))
        w += _W_DESCRIPTION * (desc_len / max_desc)

        photos = s.get("photos") or []
        if isinstance(photos, list) and len(photos) > 0:
            w += _W_PHOTO

        tid = s.get("booksy_treatment_id")
        if tid is not None and tid in variants_per_tid:
            w += _W_VARIANT_DEPTH * (variants_per_tid[tid] / max_variants)

        weights.append(w)

    return weights


# ---------------------------------------------------------------------------
# Focus distribution per key (tid lub variant_id)
# ---------------------------------------------------------------------------

def _focus_distribution(
    services: list[dict[str, Any]],
    weights: list[float],
    key_field: str,
    top_k: int,
) -> dict[int, float]:
    """Aggregate focus weights po key_field, return top_k keys jako {key: share}.

    Share jest normalizowane vs TOTAL focus weight w salonie (nie sum top_k).
    Dlatego sum top_k może być < 1.0 (długi ogon poza top_k).
    """
    by_key: dict[int, float] = {}
    total = 0.0
    for svc, w in zip(services, weights):
        k = svc.get(key_field)
        if k is None:
            continue
        by_key[int(k)] = by_key.get(int(k), 0.0) + w
        total += w
    if total == 0:
        return {}
    # Normalize all, then take top_k
    normalized = sorted(
        ((k, v / total) for k, v in by_key.items()),
        key=lambda x: -x[1],
    )[:top_k]
    return dict(normalized)


def compute_focus_distribution_by_tid(
    services: list[dict[str, Any]],
    weights: list[float],
    top_k: int = 30,
) -> dict[int, float]:
    """Top tid'y po focus share. Returns {tid: share_0..1}."""
    return _focus_distribution(services, weights, "booksy_treatment_id", top_k)


def compute_focus_distribution_by_variant(
    services: list[dict[str, Any]],
    weights: list[float],
    top_k: int = 50,
) -> dict[int, float]:
    """Top variant_id po focus share. Returns {vid: share_0..1}."""
    return _focus_distribution(services, weights, "variant_id", top_k)


# ---------------------------------------------------------------------------
# Portfolio embedding (focus-weighted centroid)
# ---------------------------------------------------------------------------

def _parse_embedding(emb: Any) -> np.ndarray | None:
    """Embedding może być list[float] (Supabase native) lub string (pgvector
    serializacja '[0.1, 0.2, ...]'). Zwraca np.ndarray albo None.
    """
    if emb is None:
        return None
    if isinstance(emb, str):
        try:
            stripped = emb.strip("[]")
            if not stripped:
                return None
            emb = [float(x) for x in stripped.split(",")]
        except (ValueError, AttributeError):
            return None
    try:
        arr = np.array(emb, dtype=np.float64)
    except (TypeError, ValueError):
        return None
    if arr.shape != (1536,):
        return None
    return arr


def compute_salon_portfolio_embedding(
    services: list[dict[str, Any]],
    weights: list[float],
) -> np.ndarray | None:
    """Focus-weighted L2-normalized mean of service embeddings.

    Returns 1536-d np.array (L2-normalized) or None gdy salon ma 0 services
    z embeddingiem.
    """
    if len(services) != len(weights) or not services:
        return None

    weighted_sum = np.zeros(1536, dtype=np.float64)
    total_weight = 0.0
    count = 0

    for svc, w in zip(services, weights):
        emb = _parse_embedding(svc.get("name_embedding"))
        if emb is None:
            continue
        weighted_sum += w * emb
        total_weight += w
        count += 1

    if total_weight == 0 or count == 0:
        return None

    portfolio = weighted_sum / total_weight
    norm = np.linalg.norm(portfolio)
    if norm > 0:
        portfolio = portfolio / norm
    return portfolio


# ---------------------------------------------------------------------------
# Similarity functions
# ---------------------------------------------------------------------------

def cosine_similarity_sparse(d1: dict[int, float], d2: dict[int, float]) -> float:
    """Cosine similarity between two sparse {key: float} distributions.

    Returns 0.0 gdy któryś vector ma norm 0 lub gdy brak wspólnych kluczy.
    Output zakres: 0..1 (assuming nonneg shares).
    """
    if not d1 or not d2:
        return 0.0
    common = set(d1) & set(d2)
    if not common:
        return 0.0
    dot = sum(d1[k] * d2[k] for k in common)
    n1 = math.sqrt(sum(v * v for v in d1.values()))
    n2 = math.sqrt(sum(v * v for v in d2.values()))
    if n1 == 0 or n2 == 0:
        return 0.0
    return dot / (n1 * n2)


def cosine_similarity_dense(a: np.ndarray | None, b: np.ndarray | None) -> float:
    """Cosine similarity dwóch L2-normalized vectorów = dot product."""
    if a is None or b is None:
        return 0.0
    return float(np.dot(a, b))


# ---------------------------------------------------------------------------
# Bundle — wszystko per salon w jednym wywołaniu
# ---------------------------------------------------------------------------

class SalonFocusBundle:
    """Wszystkie focus-related struktury dla salonu — wynik compute lub
    pobrania z pre-computed cache w salons table.
    """

    __slots__ = (
        "salon_id",
        "booksy_id",
        "portfolio_embedding",
        "focus_distribution",
        "focus_variant_distribution",
        "service_count",
        "embedded_count",
    )

    def __init__(
        self,
        salon_id: int,
        booksy_id: int | None,
        portfolio_embedding: np.ndarray | None,
        focus_distribution: dict[int, float],
        focus_variant_distribution: dict[int, float],
        service_count: int,
        embedded_count: int,
    ):
        self.salon_id = salon_id
        self.booksy_id = booksy_id
        self.portfolio_embedding = portfolio_embedding
        self.focus_distribution = focus_distribution
        self.focus_variant_distribution = focus_variant_distribution
        self.service_count = service_count
        self.embedded_count = embedded_count

    @classmethod
    def from_services(
        cls,
        salon_id: int,
        booksy_id: int | None,
        services: list[dict[str, Any]],
        top_service_names: Iterable[str] | None,
        top_k_tid: int = 30,
        top_k_variant: int = 50,
    ) -> "SalonFocusBundle":
        """Compute bundle z surowych services. Caller responsible for fetching
        services + top_service_names z DB.
        """
        weights = compute_service_focus_weights(services, top_service_names)
        emb = compute_salon_portfolio_embedding(services, weights)
        tid_dist = compute_focus_distribution_by_tid(services, weights, top_k=top_k_tid)
        var_dist = compute_focus_distribution_by_variant(
            services, weights, top_k=top_k_variant,
        )
        embedded = sum(1 for s in services if _parse_embedding(s.get("name_embedding")) is not None)
        return cls(
            salon_id=salon_id,
            booksy_id=booksy_id,
            portfolio_embedding=emb,
            focus_distribution=tid_dist,
            focus_variant_distribution=var_dist,
            service_count=len(services),
            embedded_count=embedded,
        )

    def to_db_payload(self) -> dict[str, Any]:
        """Serialize do dict gotowego do INSERT/UPDATE w salons table."""
        # JSON keys muszą być stringi (jsonb wymaga). DB store {"<tid>": share}.
        return {
            "portfolio_embedding": (
                self.portfolio_embedding.tolist()
                if self.portfolio_embedding is not None
                else None
            ),
            "focus_distribution": (
                {str(k): float(v) for k, v in self.focus_distribution.items()}
                if self.focus_distribution
                else None
            ),
            "focus_variant_distribution": (
                {str(k): float(v) for k, v in self.focus_variant_distribution.items()}
                if self.focus_variant_distribution
                else None
            ),
            "focus_computed_at": "now()",  # Caller may override with timestamp string
        }


# ---------------------------------------------------------------------------
# Helpers do parsowania DB jsonb back to dict[int, float]
# ---------------------------------------------------------------------------

def parse_focus_distribution_jsonb(raw: Any) -> dict[int, float]:
    """Parse jsonb focus_distribution z DB ({str: float}) → {int: float}.

    Handles None i niepoprawny shape gracefully (returns empty).
    """
    if not raw or not isinstance(raw, dict):
        return {}
    out: dict[int, float] = {}
    for k, v in raw.items():
        try:
            out[int(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out
