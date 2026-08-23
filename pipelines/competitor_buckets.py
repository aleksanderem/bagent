"""Faza 8a — koszyki konkurentów wg POKRYCIA menu subjecta (czyste funkcje).

Konkurent jest „bezpośredni", gdy oferuje odpowiedniki większości usług,
które w ogóle da się porównać w jego grupie. Progi są WZGLĘDNE (udział w
uniwersum porównywalnych usług), nie stałymi liczbami — dlatego nie psują
się, gdy silnik similarity zmieni liczbę próbek (stare 10/5/3 były
kalibrowane pod pricing z 8100 próbek; silnik similarity daje 500–1000 i
dawał 15/15 'excluded' — BEAUTY_AUDIT-xi18).

Uniwersum = usługi subjecta, dla których KTOKOLWIEK z wybranych ma
odpowiednik ≥ BUCKET_MIN_SIMILARITY. Normalizuje przez unikalność menu:
klinika med-est z autorskimi zabiegami ma małe uniwersum, salon paznokci
duże — udziały zostają porównywalne.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.similarity_pricing.report_pricing import _ADAPTIVE_FALLBACK_SIMILARITY

# Ten sam próg, przy którym wycena uznaje usługę za „porównywalną" w rzadkim
# otoczeniu (adaptacyjny fallback). Celowo NIE 0.82 (próg precyzyjnej wyceny —
# za ostry na pytanie „czy oferują to samo") i NIE 0.65 (promocja related
# samples — za luźny: na raporcie 181 daje 14/15 'direct').
BUCKET_MIN_SIMILARITY = _ADAPTIVE_FALLBACK_SIMILARITY

# Udział pokrytych usług w uniwersum porównywalnych.
DIRECT_SHARE = 0.50        # pokrywa co najmniej połowę tego, co da się porównać
CLUSTER_SHARE = 0.25
ASPIRATIONAL_SHARE = 0.10
# Minimum dowodu: poniżej tylu wspólnych usług udział jest szumem
# (1 z 2 = 50% ≠ bezpośredni konkurent).
MIN_COVERED = 3


@dataclass(frozen=True)
class CoverageAssignment:
    covered: int
    share: float
    bucket: str


EMPTY_ASSIGNMENT = CoverageAssignment(covered=0, share=0.0, bucket="excluded")


def coverage_by_salon(
    clusters: dict[int, list[dict[str, Any]]],
    salon_id_by_booksy: dict[int, int],
    min_similarity: float,
) -> dict[int, set[int]]:
    """{salons.id: zbiór usług subjecta z odpowiednikiem ≥ min_similarity}.

    `clusters` = wynik search_twins ({subject_service_id: [sample]}), próbki
    niosą booksy_id — mapujemy na salons.id (przestrzeń competitor_matches).
    Salony spoza mapy są ignorowane; każdy wybrany dostaje wpis (może pusty).
    """
    out: dict[int, set[int]] = {sid: set() for sid in salon_id_by_booksy.values()}
    for subject_service_id, samples in clusters.items():
        for sample in samples:
            if (sample.get("similarity") or 0.0) < min_similarity:
                continue
            booksy_id = sample.get("booksy_id")
            if booksy_id is None:
                continue
            salon_id = salon_id_by_booksy.get(int(booksy_id))
            if salon_id is None:
                continue
            out[salon_id].add(int(subject_service_id))
    return out


def bucket_for_coverage(covered: int, universe: int) -> str:
    if universe <= 0 or covered < MIN_COVERED:
        return "excluded"
    share = covered / universe
    if share >= DIRECT_SHARE:
        return "direct"
    if share >= CLUSTER_SHARE:
        return "cluster"
    if share >= ASPIRATIONAL_SHARE:
        return "aspirational"
    return "excluded"


def assign_coverage_buckets(
    coverage: dict[int, set[int]],
) -> dict[int, CoverageAssignment]:
    """Koszyk per salon wg udziału w uniwersum (suma pokryć wszystkich)."""
    universe = len(set().union(*coverage.values())) if coverage else 0
    out: dict[int, CoverageAssignment] = {}
    for salon_id, services in coverage.items():
        covered = len(services)
        share = covered / universe if universe else 0.0
        out[salon_id] = CoverageAssignment(
            covered=covered, share=round(share, 4),
            bucket=bucket_for_coverage(covered, universe),
        )
    return out
