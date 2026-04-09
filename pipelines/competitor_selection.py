"""Deterministic competitor selection algorithm (Comp Etap 1).

Selects top N candidate salons for a competitor report, based on the subject
salon loaded from Supabase. No LLM calls, fully debuggable scoring.

Algorithm overview (see docs/plans/2026-04-08-competitor-report-pipeline.md
"Competitor selection algorithm" section):

1. Load subject salon from the latest salon_scrapes row for the given audit_id
   (joins to salons, pulls business_categories jsonb and top_services).
2. Query candidates via the PostGIS find_nearby_salons RPC filtered by
   primary_category_id + distance (default 15 km).
3. Fetch each candidate's latest business_categories + top_services.
4. Filter: drop candidates whose avg female_weight differs from the subject
   by more than female_weight_tolerance (default 20).
5. Score each survivor on 5 axes:
     +30  primary_category match (always true due to RPC filter)
     +20  jaccard(business_categories sets)
     +25  top_services treatment overlap (cannot divide by zero)
     +10  reviews_count similarity ratio
     -2/km distance penalty beyond 5 km (no penalty for first 5 km)
6. Assign bucket (strict order of checks):
     - reviews_count < 20               -> 'new' (not counted in aggregates)
     - composite_score >= 70            -> 'direct'
     - 40 <= composite_score < 70       -> 'cluster'
     - reviews_rank >= subject + 0.3
       AND composite_score < 40         -> 'aspirational'
     - else                             -> dropped
7. Sort: bucket priority (direct, cluster, aspirational, new), then score desc.
8. Return top target_count for auto mode, top 15 for manual mode.

Edge cases are handled gracefully (log warning, continue):
- Subject has no top_services (28% of salons) -> overlap score is 0
- Candidate has no business_categories -> jaccard is 0
- Subject has NaN female_weight (no business_categories) -> skip FW filter
- Fewer candidates than target_count -> return what we have, don't relax filters
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


Bucket = Literal["direct", "cluster", "aspirational", "new"]
Mode = Literal["auto", "manual"]


@dataclass
class CompetitorCandidate:
    """A single scored candidate returned by select_competitors().

    Fields use snake_case to match Supabase column names. The dataclass is
    plain (no pydantic) so it can be serialized trivially and unit tests
    can construct instances with real values.
    """

    salon_id: int  # salons.id (internal PK)
    booksy_id: int
    name: str
    city: str | None
    primary_category_id: int
    reviews_count: int
    reviews_rank: float | None
    distance_km: float
    female_weight_diff: float  # abs(candidate_fw - subject_fw); NaN marker is -1
    composite_score: float
    bucket: Bucket
    counts_in_aggregates: bool
    similarity_scores: dict[str, float] = field(default_factory=dict)
    partner_system: str = "native"  # 'native' or 'versum'


# ---------------------------------------------------------------------------
# Math helpers (pure functions — tested in unit tests)
# ---------------------------------------------------------------------------


def compute_avg_female_weight(business_categories: list[dict] | None) -> float | None:
    """Return average female_weight across all categories, or None if empty.

    business_categories is a jsonb list of {id, name, female_weight, ...}.
    Missing or malformed entries are skipped. If no category has a numeric
    female_weight, returns None so callers can skip the FW filter gracefully.
    """
    if not isinstance(business_categories, list):
        return None
    weights: list[float] = []
    for cat in business_categories:
        if not isinstance(cat, dict):
            continue
        fw = cat.get("female_weight")
        if isinstance(fw, (int, float)):
            weights.append(float(fw))
    if not weights:
        return None
    return sum(weights) / len(weights)


def compute_business_category_jaccard(
    subject_bc: list[dict] | None,
    candidate_bc: list[dict] | None,
) -> float:
    """Jaccard similarity between two business_categories sets.

    Computed on the category ids (ints). Returns 0.0 when either side is
    empty or when the union is empty (no division by zero).
    """
    def _ids(bc: list[dict] | None) -> set[int]:
        if not isinstance(bc, list):
            return set()
        result: set[int] = set()
        for c in bc:
            if isinstance(c, dict):
                cid = c.get("id")
                if isinstance(cid, int):
                    result.add(cid)
        return result

    s = _ids(subject_bc)
    c = _ids(candidate_bc)
    union = s | c
    if not union:
        return 0.0
    return len(s & c) / len(union)


def compute_top_services_overlap(
    subject_treatment_ids: set[int],
    candidate_treatment_ids: set[int],
) -> float:
    """Fraction of subject's top treatment_ids that are also in candidate's.

    The denominator is |subject_treatment_ids|, not the union — this matches
    the plan doc formula and rewards candidates that cover the subject's
    flagship services. Returns 0.0 when subject has no top_services (edge
    case: 28% of salons per research spike).
    """
    if not subject_treatment_ids:
        return 0.0
    return len(subject_treatment_ids & candidate_treatment_ids) / len(subject_treatment_ids)


def compute_reviews_count_similarity(
    subject_count: int,
    candidate_count: int,
) -> float:
    """Ratio min/max of review counts. Returns 0.0 if either side is 0.

    Intuitively: two salons with 500 and 600 reviews are very similar (0.83),
    a salon with 10 reviews vs one with 1000 is very dissimilar (0.01).
    """
    if subject_count <= 0 or candidate_count <= 0:
        return 0.0
    hi = max(subject_count, candidate_count)
    if hi == 0:
        return 0.0
    return min(subject_count, candidate_count) / hi


def compute_distance_penalty(distance_km: float) -> float:
    """Distance penalty: -2 points per km beyond the first 5 km.

    Returns a non-negative number (the magnitude of the penalty). Callers
    subtract it from the running score. First 5 km are free, so a 6 km
    candidate gets penalty 2, a 10 km candidate gets 10, a 15 km gets 20.
    """
    return 2.0 * max(0.0, distance_km - 5.0)


def compute_composite_score(
    *,
    primary_category_match: bool,
    business_category_jaccard: float,
    top_services_overlap: float,
    reviews_count_similarity: float,
    distance_km: float,
) -> float:
    """Combine the 5 scoring axes into a single composite_score.

    Weights per plan doc:
      +30  primary_category match
      +20  business categories jaccard
      +25  top services treatment overlap
      +10  reviews count similarity
       -2  per km of distance beyond 5 km
    """
    score = 0.0
    if primary_category_match:
        score += 30.0
    score += 20.0 * business_category_jaccard
    score += 25.0 * top_services_overlap
    score += 10.0 * reviews_count_similarity
    score -= compute_distance_penalty(distance_km)
    return score


def assign_bucket(
    *,
    composite_score: float,
    reviews_count: int,
    candidate_reviews_rank: float | None,
    subject_reviews_rank: float | None,
) -> Bucket | None:
    """Assign a bucket, or return None to drop the candidate.

    Strict order of checks (per plan doc):
      1. reviews_count < 20                         -> 'new'
      2. composite_score >= 70                      -> 'direct'
      3. 40 <= composite_score < 70                 -> 'cluster'
      4. reviews_rank >= subject + 0.3 AND
         composite_score < 40                       -> 'aspirational'
      5. else                                        -> drop (return None)
    """
    if reviews_count < 20:
        return "new"
    if composite_score >= 70:
        return "direct"
    if composite_score >= 40:
        return "cluster"
    if (
        candidate_reviews_rank is not None
        and subject_reviews_rank is not None
        and candidate_reviews_rank >= subject_reviews_rank + 0.3
    ):
        return "aspirational"
    return None


_BUCKET_PRIORITY: dict[Bucket, int] = {
    "direct": 0,
    "cluster": 1,
    "aspirational": 2,
    "new": 3,
}


def sort_key(candidate: CompetitorCandidate) -> tuple[int, float]:
    """Sort key: bucket priority first, then descending composite_score.

    Python sorts ascending by default, so we negate the score to get desc.
    """
    return (_BUCKET_PRIORITY[candidate.bucket], -candidate.composite_score)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def select_competitors(
    subject_audit_id: str,
    target_count: int = 5,
    mode: Mode = "auto",
    max_distance_km: float = 15.0,
    female_weight_tolerance: float = 20.0,
    supabase: SupabaseService | None = None,
) -> list[CompetitorCandidate]:
    """Deterministic candidate selection for a competitor report.

    Returns top `target_count` for auto mode, or top 15 for manual mode
    (frontend lets the user pick from the 15).

    Args:
        subject_audit_id: convex_audit_id of the subject salon's latest scrape.
        target_count: how many candidates to return in auto mode (default 5).
        mode: 'auto' returns target_count, 'manual' returns 15.
        max_distance_km: maximum distance cap in kilometers (default 15).
        female_weight_tolerance: maximum |subject_fw - candidate_fw| (default 20).
        supabase: optional SupabaseService (for tests). Defaults to a new one.

    Raises:
        ValueError: if the subject salon is not found.
    """
    service = supabase or SupabaseService()

    # --- 1. Load subject salon ------------------------------------------------
    subject = await service.get_subject_salon_for_audit(subject_audit_id)
    if subject is None:
        raise ValueError(f"Subject salon not found for audit_id={subject_audit_id!r}")

    subject_salon_id: int = subject["salon_id"]
    subject_booksy_id: int = subject["booksy_id"]
    subject_lat: float | None = subject.get("salon_lat")
    subject_lng: float | None = subject.get("salon_lng")
    subject_primary_cat: int | None = subject.get("primary_category_id")
    subject_business_cats: list[dict] = subject.get("business_categories") or []
    subject_reviews_count: int = subject.get("reviews_count") or 0
    subject_reviews_rank: float | None = subject.get("reviews_rank")

    if subject_lat is None or subject_lng is None:
        raise ValueError(
            f"Subject salon has no coordinates (audit_id={subject_audit_id!r}, "
            f"salon_id={subject_salon_id})"
        )
    if subject_primary_cat is None:
        raise ValueError(
            f"Subject salon has no primary_category_id "
            f"(audit_id={subject_audit_id!r}, salon_id={subject_salon_id})"
        )

    subject_female_weight = compute_avg_female_weight(subject_business_cats)
    subject_top_services = await service.get_salon_top_services(subject_salon_id)
    subject_top_treatment_ids: set[int] = {
        ts["booksy_treatment_id"]
        for ts in subject_top_services
        if ts.get("booksy_treatment_id") is not None
    }

    if subject_female_weight is None:
        logger.warning(
            "Subject salon has no business_categories with female_weight — "
            "skipping female_weight tolerance filter (salon_id=%s)",
            subject_salon_id,
        )
    if not subject_top_treatment_ids:
        logger.info(
            "Subject salon has no top_services with booksy_treatment_id — "
            "top_services overlap score will be 0 for all candidates "
            "(salon_id=%s, top_services_rows=%d)",
            subject_salon_id, len(subject_top_services),
        )

    # --- 2. Query candidates via PostGIS RPC ----------------------------------
    raw_candidates = await service.get_candidate_salons(
        lat=subject_lat,
        lng=subject_lng,
        primary_category_id=subject_primary_cat,
        radius_km=max_distance_km,
        exclude_booksy_id=subject_booksy_id,
        limit=200,
    )
    logger.info(
        "Found %d raw candidates for subject %s in category %d within %.1f km",
        len(raw_candidates), subject_booksy_id, subject_primary_cat, max_distance_km,
    )

    if not raw_candidates:
        return []

    # --- 3. Fetch per-candidate business_categories + top_services -----------
    candidate_salon_ids = [c["salon_id"] for c in raw_candidates]
    candidate_booksy_ids = [c["booksy_id"] for c in raw_candidates]

    bc_map = await service.get_latest_business_categories_for_booksy_ids(candidate_booksy_ids)
    top_map = await service.get_latest_top_services_for_salon_ids(candidate_salon_ids)
    partner_map = await service.get_latest_partner_system_for_booksy_ids(candidate_booksy_ids)

    # --- 4. Filter + Score + Bucket each candidate ---------------------------
    scored: list[CompetitorCandidate] = []
    dropped_fw = 0
    dropped_bucket = 0

    for c in raw_candidates:
        booksy_id = c["booksy_id"]
        salon_id = c["salon_id"]
        cand_bc = bc_map.get(booksy_id)
        cand_top = top_map.get(salon_id, [])
        partner = partner_map.get(booksy_id) or "native"

        cand_female_weight = compute_avg_female_weight(cand_bc)
        if (
            subject_female_weight is not None
            and cand_female_weight is not None
            and abs(cand_female_weight - subject_female_weight) > female_weight_tolerance
        ):
            dropped_fw += 1
            continue

        female_weight_diff = (
            abs(cand_female_weight - subject_female_weight)
            if (subject_female_weight is not None and cand_female_weight is not None)
            else -1.0  # sentinel: FW filter skipped
        )

        distance_km = c["distance_km"]
        bc_jaccard = compute_business_category_jaccard(subject_business_cats, cand_bc)
        cand_top_treatment_ids = {
            ts["booksy_treatment_id"]
            for ts in cand_top
            if ts.get("booksy_treatment_id") is not None
        }
        ts_overlap = compute_top_services_overlap(
            subject_top_treatment_ids, cand_top_treatment_ids,
        )
        rc_sim = compute_reviews_count_similarity(
            subject_reviews_count, c.get("reviews_count") or 0,
        )

        composite = compute_composite_score(
            primary_category_match=True,  # strict filter guarantees match
            business_category_jaccard=bc_jaccard,
            top_services_overlap=ts_overlap,
            reviews_count_similarity=rc_sim,
            distance_km=distance_km,
        )

        bucket = assign_bucket(
            composite_score=composite,
            reviews_count=c.get("reviews_count") or 0,
            candidate_reviews_rank=c.get("reviews_rank"),
            subject_reviews_rank=subject_reviews_rank,
        )
        if bucket is None:
            dropped_bucket += 1
            continue

        scored.append(
            CompetitorCandidate(
                salon_id=salon_id,
                booksy_id=booksy_id,
                name=c.get("name") or "",
                city=c.get("city"),
                primary_category_id=subject_primary_cat,
                reviews_count=c.get("reviews_count") or 0,
                reviews_rank=c.get("reviews_rank"),
                distance_km=distance_km,
                female_weight_diff=female_weight_diff,
                composite_score=round(composite, 2),
                bucket=bucket,
                counts_in_aggregates=(bucket != "new"),
                similarity_scores={
                    "primary_category": 1.0,
                    "business_categories_jaccard": round(bc_jaccard, 4),
                    "top_services_overlap": round(ts_overlap, 4),
                    "reviews_count_similarity": round(rc_sim, 4),
                    "distance_penalty": round(compute_distance_penalty(distance_km), 2),
                },
                partner_system=partner,
            )
        )

    logger.info(
        "Scored %d/%d candidates (dropped %d by female_weight, %d by bucket)",
        len(scored), len(raw_candidates), dropped_fw, dropped_bucket,
    )

    # --- 5. Sort by bucket priority then descending composite_score ----------
    scored.sort(key=sort_key)

    # --- 6. Return top N ------------------------------------------------------
    cap = target_count if mode == "auto" else 15
    return scored[:cap]
