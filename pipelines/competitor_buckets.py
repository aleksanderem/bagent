"""Faza 8a — koszyki konkurentów wg POKRYCIA menu subjecta (czyste funkcje).

Konkurent jest „bezpośredni", gdy oferuje odpowiedniki większości usług,
które w ogóle da się porównać w jego grupie. Progi są WZGLĘDNE (udział w
uniwersum porównywalnych usług), nie stałymi liczbami — dlatego nie psują
się, gdy silnik similarity zmieni liczbę próbek (stare 10/5/3 były
kalibrowane pod pricing z 8100 próbek; silnik similarity daje 500–1000 i
dawał 15/15 'excluded' — BEAUTY_AUDIT-xi18).

Mianownik = CAŁE menu klientki (usługi subjecta z embeddingiem), nie suma
pokryć wybranych: „uniwersum" premiowało słabe pule (1 salon pokrywa 3/221,
reszta 0 → 3/3 = 'direct'; 15 salonów po te same 4 usługi → 15× 'direct').
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Próg „czy konkurent oferuje to samo" — WŁASNA stała, celowo NIEZALEŻNA od
# progów wyceny. Celowo NIE 0.82 (próg precyzyjnej wyceny — za ostry na to
# pytanie) i NIE 0.65 (promocja related samples — za luźny: na raporcie 181
# daje 14/15 'direct').
#
# 2026-08-26 (BEAUTY_AUDIT-ye2j): ODPIĘTE od _ADAPTIVE_FALLBACK_SIMILARITY.
# Do dziś ta stała była aliasem fallbacku wyceny, więc zmiana progu WYCENY
# po cichu przestawiała klasyfikację konkurentów. Przy obniżeniu fallbacku
# 0.75 -> 0.60 próg koszyków wylądowałby na wartości LUŹNIEJSZEJ niż 0.65,
# które ten sam komentarz odrzuca — a bucket i counts_in_aggregates są
# TRWAŁE w bazie i wpływają na agregaty rynkowe, nie tylko na jeden raport.
# Wartość 0.75 to ostatni próg, przy którym koszyki były walidowane.
# Zmiana tej stałej wymaga OSOBNEGO pomiaru rozkładu direct/cluster.
BUCKET_MIN_SIMILARITY = 0.75

# Udział pokrytych usług w CAŁYM menu subjecta.
DIRECT_SHARE = 0.20        # oferuje odpowiednik co piątej usługi klientki
CLUSTER_SHARE = 0.10
ASPIRATIONAL_SHARE = 0.03
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


def bucket_for_coverage(covered: int, subject_total: int) -> str:
    if subject_total <= 0 or covered < MIN_COVERED:
        return "excluded"
    share = covered / subject_total
    if share >= DIRECT_SHARE:
        return "direct"
    if share >= CLUSTER_SHARE:
        return "cluster"
    if share >= ASPIRATIONAL_SHARE:
        return "aspirational"
    return "excluded"


def assign_coverage_buckets(
    coverage: dict[int, set[int]],
    subject_total: int,
) -> dict[int, CoverageAssignment]:
    """Koszyk per salon wg udziału pokrytych usług w menu subjecta
    (`subject_total` = liczba usług subjecta z embeddingiem)."""
    out: dict[int, CoverageAssignment] = {}
    for salon_id, services in coverage.items():
        covered = len(services)
        share = covered / subject_total if subject_total else 0.0
        out[salon_id] = CoverageAssignment(
            covered=covered, share=round(share, 4),
            bucket=bucket_for_coverage(covered, subject_total),
        )
    return out
