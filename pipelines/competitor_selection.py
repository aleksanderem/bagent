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
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np

from services.body_area_taxonomy import extract_body_areas
from services.focus_score import (
    SalonFocusBundle,
    cosine_similarity_dense,
    cosine_similarity_sparse,
    parse_focus_distribution_jsonb,
)
from services.method_marker import extract_method_marker
from services.pipeline_trace import TraceWriter
from services.supabase import SupabaseService

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Focus-weighted scoring weights (2026-05-15)
# ---------------------------------------------------------------------------
# Empirycznie zwalidowane na Beauty4ever — focus_tid_sim oddziela "fałszywych
# konkurentów" (same portfolio mix, inny nacisk) od prawdziwych. Wagi:
#   +30  focus_tid_sim     — cosine over per-tid focus distribution
#   +20  focus_var_sim     — cosine over per-variant focus distribution (finer)
#   +20  portfolio_emb_sim — cosine over L2-normalized portfolio embedding
#   +10  reviews_count_sim — proxy skali biznesu
#   -2/km poza 5 km
# Max realistic score: ~70-75 (gdy wszystkie sim wysokie).
_W_FOCUS_TID = 30.0
_W_FOCUS_VAR = 20.0
_W_PORTFOLIO_EMB = 20.0
_W_REVIEWS_SIM = 10.0

# Added 2026-05-17 — profile_overlap_sim: weighted recall of subject's
# (method_marker, body_area) atoms covered by candidate's portfolio.
# Bypasses LLM-routed Booksy tid distribution (focus_tid_sim) entirely;
# uses deterministic regex extractors so it stays consistent across
# audits regardless of which Pass-5 anchors have settled.
# Empirically validated on Beauty4ever (booksy_id=98814) where the
# original composite_score_v2 was dominated by portfolio_embedding_sim
# (0.86-0.93 across all selected candidates) — the embedding flattens
# brand/method differences (Thunder vs Onda vs Soprano all read as
# "med-est noise"), while profile_overlap_sim discriminates: 4 of 5
# originally-picked competitors fell below the new top-15 floor.
# Weight 25 sits between focus_tid_sim (30) and focus_var_sim (20)
# — second-strongest signal, intentionally not the dominant one
# because for low-atom-count subjects (nail-only salons, small
# generalists) profile_overlap collapses to ties and the other axes
# must still discriminate.
_W_PROFILE_OVERLAP = 25.0


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
    # True when this salon was picked by the user in the frontend competitor
    # picker and force-included via the UNION in select_competitors. Lets the
    # frontend surface per-competitor DETAIL for picks while analytics stay
    # computed from the full deterministic sample.
    is_user_selected: bool = False


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
    """Legacy v1 composite scoring (5 axes, discrete tid intersection).

    DEPRECATED 2026-05-15 — zachowywane dla testów i porównań A/B.
    Production używa compute_composite_score_v2 z focus-weighted similarity.

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


def compute_composite_score_v2(
    *,
    focus_tid_sim: float,
    focus_var_sim: float,
    portfolio_embedding_sim: float,
    reviews_count_similarity: float,
    distance_km: float,
    profile_overlap_sim: float = 0.0,
) -> float:
    """Focus-weighted composite scoring (5 semantic axes + distance penalty).

    v2 priorytetyzuje focus_tid_sim który mierzy CZY salon faktycznie skupia
    się na TYCH SAMYCH kategoriach co subject. 2026-05-17: dodano
    `profile_overlap_sim` jako piąta oś — deterministyczne (method, area)
    pokrycie portfolio, bypass LLM-routowanego tid distribution.

    Wagi (sum=105, ale typowy max realistic ~50-70 — zero salonów ma 1.0
    we wszystkich osiach jednocześnie):
      +30 × focus_tid_sim         (cornerstone — nacisk na te same tid'y)
      +25 × profile_overlap_sim   (NEW: ważone recall atomów method+area)
      +20 × focus_var_sim         (finer — nacisk na konkretne varianty)
      +20 × portfolio_emb_sim     (semantic backstop — DOMINANT na Beauty4ever
                                   pre-refactor, teraz balansowany przez profile_overlap)
      +10 × reviews_count_sim     (skala biznesu)
      -2 × max(0, distance_km - 5)  (geo)

    `profile_overlap_sim` jest dodawany jako default=0 dla backward
    compatibility z testami i kodem który jeszcze nie zna tej osi.
    """
    score = 0.0
    score += _W_FOCUS_TID * focus_tid_sim
    score += _W_PROFILE_OVERLAP * profile_overlap_sim
    score += _W_FOCUS_VAR * focus_var_sim
    score += _W_PORTFOLIO_EMB * portfolio_embedding_sim
    score += _W_REVIEWS_SIM * reviews_count_similarity
    score -= compute_distance_penalty(distance_km)
    return score


# ---------------------------------------------------------------------------
# Profile overlap (2026-05-17)
# ---------------------------------------------------------------------------
# Subject's profile is the multiset of (method_marker, body_area) atoms
# extracted deterministically from active service names + category labels.
# A service with multiple body_areas (e.g. "Depilacja laserowa - kark + szyja
# + dekolt") is exploded into one atom per area, so a candidate that only
# offers laser on one of those three areas gets credit proportional to the
# subject's investment there.
# Note: when a service has no body_area marker, we emit a single
# `(method, "")` atom — this matches generic services like "Mezoterapia
# igłowa" that don't specify the target area.


def profile_atoms_from_services(services: list[dict]) -> Counter[tuple[str, str]]:
    """Build atom multiset Counter[(method_marker, body_area)] from services.

    services is a list of dicts with at least `name`. Optional `category_name`
    is fed into method_marker for extra context. Empty/very-short names and
    inactive services (caller's responsibility to pre-filter) are skipped.
    """
    atoms: Counter[tuple[str, str]] = Counter()
    for svc in services:
        name = (svc.get("name") or "").strip()
        if not name or len(name) < 3:
            continue
        method = extract_method_marker(name, svc.get("category_name") or "")
        areas = extract_body_areas(name)
        if not areas:
            atoms[(method, "")] += 1
        else:
            for area in areas:
                atoms[(method, area)] += 1
    return atoms


def compute_profile_overlap_sim(
    subject_atoms: Counter[tuple[str, str]],
    candidate_atoms: Counter[tuple[str, str]],
) -> float:
    """Weighted recall of subject's atoms covered by candidate's portfolio.

    Returns a value in [0, 1]:
      - 0.0 if the subject has no atoms (empty profile — caller will normally
        skip the axis by leaving the default 0 in compute_composite_score_v2)
      - 1.0 if every (method, area) the subject offers is also offered by
        the candidate (regardless of how many extra atoms the candidate has)
      - In between: sum of subject weights for atoms present in candidate,
        divided by total subject weight.

    Weighting by subject count matters: laser/twarz with 34 services should
    dominate over laser/lydki with 4 services. Recall (not precision) is the
    right metric because a candidate offering MORE than subject is still
    useful comparison — they cover the same ground plus more (aspirational).
    """
    total_weight = sum(subject_atoms.values())
    if total_weight == 0:
        return 0.0
    covered_weight = 0
    for atom, weight in subject_atoms.items():
        if atom in candidate_atoms:
            covered_weight += weight
    return covered_weight / total_weight


def assign_bucket(
    *,
    composite_score: float,
    reviews_count: int,
    candidate_reviews_rank: float | None,
    subject_reviews_rank: float | None,
) -> Bucket | None:
    """Legacy v1 bucket assignment. DEPRECATED — use assign_bucket_v2."""
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


def assign_bucket_v2(
    *,
    focus_tid_sim: float,
    portfolio_embedding_sim: float,
    reviews_count: int,
    candidate_reviews_rank: float | None,
    subject_reviews_rank: float | None,
    profile_overlap_sim: float = 0.0,
) -> Bucket | None:
    """Focus-weighted bucket assignment.

    Tresholdy z empirii Beauty4ever (focus_align test 2026-05-15) +
    profile_overlap_sim OR-gate (2026-05-17):
      direct        (focus_tid_sim ≥ 0.25 OR profile_overlap_sim ≥ 0.7)
                    AND portfolio_emb_sim ≥ 0.85
                    (skupiają się na tych samych tids LUB pokrywają mocno
                    nasze (method, area) atomy — i mają podobne portfolio)
      cluster       focus_tid_sim ≥ 0.12 OR profile_overlap_sim ≥ 0.4
                    (umiarkowane podobieństwo nacisku LUB ≥40% pokrycia
                    naszych atomów — w tym samym klastrze rynkowym)
      aspirational  reviews_rank ≥ subject + 0.3 AND portfolio_emb_sim ≥ 0.70
                    (wyżej oceniany salon ze zbliżoną semantyką oferty —
                    wzór do podpatrzenia, nawet jeśli różny focus)
      new           reviews_count < 20

    Returns None gdy żadna gałąź nie matchuje → kandydat odrzucony.

    Background: focus_tid_sim opiera się o Booksy tid distribution (cosine
    nad focus_distribution JSONB), które zależy od Rule 1-4 LLM routingu i
    bywa zaszumione dla salonów z brand-marker-laden nazwami (Thunder,
    Onda, Soprano). profile_overlap_sim jest deterministyczny — ekstrakcja
    (method_marker, body_area) regexem, bez LLM. Beauty4ever diagnostic
    pokazał 15 salonów z profile_overlap ≥ 0.5 dropowanych pre-2026-05-17,
    w tym Pani Estetyczna Clinic z overlap 0.88 (najsilniejsze pokrycie w
    całej puli) ale focus_tid_sim tylko 0.057.
    """
    if reviews_count < 20:
        return "new"
    if (
        (focus_tid_sim >= 0.25 or profile_overlap_sim >= 0.7)
        and portfolio_embedding_sim >= 0.85
    ):
        return "direct"
    if focus_tid_sim >= 0.12 or profile_overlap_sim >= 0.4:
        return "cluster"
    if (
        candidate_reviews_rank is not None
        and subject_reviews_rank is not None
        and candidate_reviews_rank >= subject_reviews_rank + 0.3
        and portfolio_embedding_sim >= 0.70
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


def _fetch_subject_focus_bundle(
    service: SupabaseService,
    subject_salon_id: int,
    subject_booksy_id: int,
) -> SalonFocusBundle | None:
    """Build SalonFocusBundle dla subject z LATEST CHAIN HEAD scrape.

    Subject FAKTYCZNIE potrzebuje świeżego compute (nie z cache salons table),
    bo audyt może być przed nowym scrape. Worst case: subject's data is freshest
    in his latest non-chain-head scrape (audit-time snapshot).

    Returns None gdy brak chain head scrape lub embeddingów.
    """
    client = service.client

    # Try cached portfolio z salons table FIRST (jeśli świeży)
    cached_res = (
        client.table("salons")
        .select(
            "id, booksy_id, portfolio_embedding, focus_distribution, "
            "focus_variant_distribution, focus_computed_at",
        )
        .eq("id", subject_salon_id)
        .limit(1)
        .execute()
    )
    if cached_res.data and cached_res.data[0].get("portfolio_embedding") is not None:
        row = cached_res.data[0]
        emb_raw = row["portfolio_embedding"]
        # pgvector serializuje jako string lub list w zależności od config
        if isinstance(emb_raw, str):
            try:
                emb = np.array(
                    [float(x) for x in emb_raw.strip("[]").split(",")],
                    dtype=np.float64,
                )
            except Exception:
                emb = None
        else:
            emb = np.array(emb_raw, dtype=np.float64) if emb_raw else None
        if emb is not None and emb.shape == (1536,):
            return SalonFocusBundle(
                salon_id=subject_salon_id,
                booksy_id=subject_booksy_id,
                portfolio_embedding=emb,
                focus_distribution=parse_focus_distribution_jsonb(row.get("focus_distribution")),
                focus_variant_distribution=parse_focus_distribution_jsonb(row.get("focus_variant_distribution")),
                service_count=0,  # not stored in cache
                embedded_count=0,
            )

    # Cache miss — compute from scratch (latest chain head)
    scrape_res = (
        client.table("salon_scrapes")
        .select("id, scraped_at")
        .eq("booksy_id", subject_booksy_id)
        .eq("is_chain_head", True)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
    )
    if not scrape_res.data:
        return None
    scrape_id = scrape_res.data[0]["id"]

    svc_res = (
        client.table("salon_scrape_services")
        .select(
            "id, name, description, photos, booksy_treatment_id, variant_id, "
            "price_grosze, name_embedding",
        )
        .eq("scrape_id", scrape_id)
        .execute()
    )
    services = svc_res.data or []

    salon_res = (
        client.table("salons")
        .select("top_service_names")
        .eq("id", subject_salon_id)
        .single()
        .execute()
    )
    top_names = salon_res.data.get("top_service_names") if salon_res.data else None

    return SalonFocusBundle.from_services(
        salon_id=subject_salon_id,
        booksy_id=subject_booksy_id,
        services=services,
        top_service_names=top_names,
    )


def _fetch_candidate_focus_bundles_batch(
    service: SupabaseService,
    salon_ids: list[int],
) -> dict[int, SalonFocusBundle]:
    """Batch-fetch pre-computed focus bundles dla candidates z salons table.

    Korzysta z `get_salons_focus_batch` RPC (mig 063) — single round-trip dla
    N candidates. Salony bez pre-computed focus (NULL portfolio_embedding) są
    pomijane — nie liczymy on-the-fly w hot path selection (zbyt wolne dla
    200 kandydatów). Cron `refresh_salon_focus` picks them up offline.

    Returns dict {salon_id: SalonFocusBundle}.
    """
    if not salon_ids:
        return {}

    client = service.client
    res = client.rpc(
        "get_salons_focus_batch",
        {"p_salon_ids": salon_ids},
    ).execute()
    out: dict[int, SalonFocusBundle] = {}
    for row in (res.data or []):
        if row.get("portfolio_embedding") is None:
            continue
        emb_raw = row["portfolio_embedding"]
        if isinstance(emb_raw, str):
            try:
                emb = np.array(
                    [float(x) for x in emb_raw.strip("[]").split(",")],
                    dtype=np.float64,
                )
            except Exception:
                continue
        else:
            emb = np.array(emb_raw, dtype=np.float64) if emb_raw else None
        if emb is None or emb.shape != (1536,):
            continue
        out[row["salon_id"]] = SalonFocusBundle(
            salon_id=row["salon_id"],
            booksy_id=None,
            portfolio_embedding=emb,
            focus_distribution=parse_focus_distribution_jsonb(row.get("focus_distribution")),
            focus_variant_distribution=parse_focus_distribution_jsonb(row.get("focus_variant_distribution")),
            service_count=0,
            embedded_count=0,
        )
    return out


async def select_competitors(
    subject_audit_id: str,
    target_count: int = 15,
    mode: Mode = "auto",
    max_distance_km: float = 15.0,
    female_weight_tolerance: float = 20.0,
    supabase: SupabaseService | None = None,
    tracer: "TraceWriter | None" = None,
    must_include_salon_ids: list[int] | None = None,
) -> list[CompetitorCandidate]:
    """Deterministic candidate selection for a competitor report.

    Returns top `target_count` for auto mode, or top 15 for manual mode
    (frontend lets the user pick from the 15).

    2026-05-17: domyślnie target_count=15 (poprzednio 5). Po dodaniu
    profile_overlap_sim do bucketing OR-gate, pula kwalifikujących się
    salonów wzrosła do 160+ — 5 cap nie pozwala użytkownikowi zobaczyć
    pełnego kontekstu rynkowego (na Beauty4ever pokazał tylko 3 widoczne
    po Fazie 8a weryfikacji). 15 daje zdrowy bufor żeby po weryfikacji
    zostało ≥5-7 mocnych pasujących.

    Args:
        subject_audit_id: convex_audit_id of the subject salon's latest scrape.
        target_count: how many candidates to return in auto mode (default 5).
        mode: 'auto' returns target_count, 'manual' returns 15.
        max_distance_km: maximum distance cap in kilometers (default 15).
        female_weight_tolerance: maximum |subject_fw - candidate_fw| (default 20).
        supabase: optional SupabaseService (for tests). Defaults to a new one.
        must_include_salon_ids: salon_ids the user picked in the frontend
            competitor picker. UNION'd into the result — these salons are
            GUARANTEED to appear in the returned list (marked
            is_user_selected=True), even if they were cap-dropped or filtered
            out earlier (distance / female_weight / bucket) or never raw
            candidates at all. Force-added picks (those not in the
            deterministic `scored` set) get counts_in_aggregates=False so they
            never distort market aggregates. The union only ADDS — it never
            reduces the candidate set below what auto mode would produce.

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

    # --- 1b. Fetch subject focus bundle (cached lub fresh compute) ----------
    subject_bundle = _fetch_subject_focus_bundle(
        service, subject_salon_id, subject_booksy_id,
    )
    if subject_bundle is None or subject_bundle.portfolio_embedding is None:
        logger.warning(
            "Subject salon has no focus bundle (no embeddings or no chain head); "
            "v2 selection will degrade — falling back to v1-style scoring with "
            "neutral focus_tid=0 for all candidates (salon_id=%s)",
            subject_salon_id,
        )

    # --- 1c. Build subject's deterministic (method, area) atom profile ------
    # Used to compute profile_overlap_sim, a 5th axis on top of v2 focus
    # scoring. Bypasses LLM-routed Booksy tid distribution so brand-marker
    # noise (Thunder vs Onda) and Rule 1-4 mis-routings don't pollute ranking.
    # Empty profile = no axis contribution (all candidates get 0).
    subject_scrape_id = await service.get_chain_head_scrape_id(subject_booksy_id)
    subject_atoms: Counter[tuple[str, str]] = Counter()
    if subject_scrape_id is not None:
        subj_services = await service.get_chain_head_services_for_scrape(subject_scrape_id)
        subject_atoms = profile_atoms_from_services(subj_services)
        logger.info(
            "Subject atom profile: %d unique (method, area) atoms, "
            "total weight=%d (salon_id=%s, scrape_id=%s)",
            len(subject_atoms), sum(subject_atoms.values()),
            subject_salon_id, subject_scrape_id,
        )
    else:
        logger.warning(
            "Subject salon has no chain_head scrape — profile_overlap_sim "
            "will be 0 for all candidates (salon_id=%s, booksy_id=%s)",
            subject_salon_id, subject_booksy_id,
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

    # Trace: candidate pool snapshot. This is the input to selection — if a
    # salon doesn't appear here at all, RPC `get_candidate_salons` filtered
    # it out (wrong category, outside radius, deleted, etc.).
    if tracer is not None:
        tracer.add(
            step="selection.candidate_pool",
            data={
                "subject_audit_id": subject_audit_id,
                "subject_booksy_id": subject_booksy_id,
                "subject_salon_id": subject_salon_id,
                "subject_primary_cat": subject_primary_cat,
                "subject_female_weight": subject_female_weight,
                "subject_reviews_count": subject_reviews_count,
                "subject_reviews_rank": subject_reviews_rank,
                "max_distance_km": max_distance_km,
                "female_weight_tolerance": female_weight_tolerance,
                "raw_candidates_count": len(raw_candidates),
                "raw_candidate_booksy_ids": [
                    c.get("booksy_id") for c in raw_candidates
                ],
                "subject_has_focus_bundle": (
                    subject_bundle is not None
                    and subject_bundle.portfolio_embedding is not None
                ),
                "subject_atom_count": len(subject_atoms),
                "target_count": target_count,
                "mode": mode,
            },
            salon_ref_id=subject_salon_id,
        )

    if not raw_candidates:
        return []

    # --- 3. Fetch per-candidate business_categories (only for FW filter) ----
    # + batch-fetch pre-computed focus bundles. Top-services overlap dropped —
    # focus_tid_sim is precyzyjniejsze i już je includes (top services są w
    # focus weighting przez signal _W_TOP_SERVICE).
    candidate_salon_ids = [c["salon_id"] for c in raw_candidates]
    candidate_booksy_ids = [c["booksy_id"] for c in raw_candidates]

    bc_map = await service.get_latest_business_categories_for_booksy_ids(candidate_booksy_ids)
    partner_map = await service.get_latest_partner_system_for_booksy_ids(candidate_booksy_ids)

    # Pre-computed focus bundles (single RPC call)
    focus_bundles = _fetch_candidate_focus_bundles_batch(service, candidate_salon_ids)
    logger.info(
        "Fetched focus bundles for %d/%d candidates (rest: cron will compute)",
        len(focus_bundles), len(candidate_salon_ids),
    )

    # Candidate atom profiles — bulk-fetch chain head services via the new
    # RPC (migration 087). Cheap deterministic regex (~5 µs/service) gives us
    # profile_overlap_sim without any LLM normalization. We map booksy_id ->
    # scrape_uuid first because the RPC works on scrape_ids, but candidate
    # rows from find_nearby_salons only have salon_id/booksy_id.
    candidate_atoms_by_booksy: dict[int, Counter[tuple[str, str]]] = {}
    if subject_atoms:
        chain_head_map = await service.get_chain_head_scrape_ids_for_booksy_ids(
            candidate_booksy_ids,
        )
        scrape_ids_for_atoms = list(chain_head_map.values())
        services_by_scrape = await service.get_candidate_services_for_atoms(
            scrape_ids_for_atoms,
        )
        # Reverse map scrape_uuid -> booksy_id, then build atoms
        booksy_by_scrape = {sid: bid for bid, sid in chain_head_map.items()}
        for sid, svc_list in services_by_scrape.items():
            bid = booksy_by_scrape.get(sid)
            if bid is not None:
                candidate_atoms_by_booksy[bid] = profile_atoms_from_services(svc_list)
        logger.info(
            "Built atom profiles for %d/%d candidates (chain_heads_found=%d)",
            len(candidate_atoms_by_booksy), len(candidate_booksy_ids),
            len(chain_head_map),
        )

    # --- 4. Filter + Score + Bucket each candidate ---------------------------
    scored: list[CompetitorCandidate] = []
    dropped_fw = 0
    dropped_bucket = 0
    dropped_no_focus = 0

    for c in raw_candidates:
        booksy_id = c["booksy_id"]
        salon_id = c["salon_id"]
        cand_bc = bc_map.get(booksy_id)
        partner = partner_map.get(booksy_id) or "native"

        # Female weight tolerance (retained — broni przed barber/men salons)
        cand_female_weight = compute_avg_female_weight(cand_bc)
        if (
            subject_female_weight is not None
            and cand_female_weight is not None
            and abs(cand_female_weight - subject_female_weight) > female_weight_tolerance
        ):
            dropped_fw += 1
            if tracer is not None:
                tracer.add(
                    step="selection.candidate_evaluated",
                    data={
                        "decision": "reject",
                        "reject_reason": "female_weight_mismatch",
                        "salon_id": salon_id,
                        "booksy_id": booksy_id,
                        "name": c.get("name"),
                        "subject_female_weight": subject_female_weight,
                        "candidate_female_weight": cand_female_weight,
                        "diff": abs(cand_female_weight - subject_female_weight),
                        "tolerance": female_weight_tolerance,
                    },
                    salon_ref_id=salon_id,
                )
            continue

        female_weight_diff = (
            abs(cand_female_weight - subject_female_weight)
            if (subject_female_weight is not None and cand_female_weight is not None)
            else -1.0
        )

        distance_km = c["distance_km"]
        rc_sim = compute_reviews_count_similarity(
            subject_reviews_count, c.get("reviews_count") or 0,
        )

        # Pre-computed focus bundle — skip jeśli brak (cron go nadrobi)
        cand_bundle = focus_bundles.get(salon_id)
        if cand_bundle is None or cand_bundle.portfolio_embedding is None:
            dropped_no_focus += 1
            if tracer is not None:
                tracer.add(
                    step="selection.candidate_evaluated",
                    data={
                        "decision": "reject",
                        "reject_reason": "no_focus_bundle",
                        "salon_id": salon_id,
                        "booksy_id": booksy_id,
                        "name": c.get("name"),
                        "distance_km": distance_km,
                        "note": (
                            "Candidate has no precomputed focus bundle yet — "
                            "nightly cron computes these. Reappears in next run."
                        ),
                    },
                    salon_ref_id=salon_id,
                )
            continue

        # 4 semantic axes similarity (v2)
        if subject_bundle is not None and subject_bundle.portfolio_embedding is not None:
            focus_tid_sim = cosine_similarity_sparse(
                subject_bundle.focus_distribution, cand_bundle.focus_distribution,
            )
            focus_var_sim = cosine_similarity_sparse(
                subject_bundle.focus_variant_distribution,
                cand_bundle.focus_variant_distribution,
            )
            portfolio_emb_sim = cosine_similarity_dense(
                subject_bundle.portfolio_embedding, cand_bundle.portfolio_embedding,
            )
            # Asymmetric tid set overlap — answers the intuitive UX question
            # "what fraction of MY treatment categories does this competitor
            # also offer?". Independent of cosine which is focus-weighted.
            # focus_tid_sim says "nasze rozkłady focus są podobne" (e.g. both
            # heavily on tid 503 = Endermologia), this says "ile z moich
            # kategorii w ogóle ma ten konkurent w swoim cenniku". A user
            # parsing competitor cenniks by hand expects to count the
            # latter — see UI label "Nakładanie ofertowe".
            subj_tids = set(subject_bundle.focus_distribution.keys())
            cand_tids = set(cand_bundle.focus_distribution.keys())
            if subj_tids:
                tid_set_overlap_asym = len(subj_tids & cand_tids) / len(subj_tids)
            else:
                tid_set_overlap_asym = 0.0
        else:
            # Subject bez focus — neutral (wszystkim 0; effectively v1 lite)
            focus_tid_sim = 0.0
            focus_var_sim = 0.0
            portfolio_emb_sim = 0.0
            tid_set_overlap_asym = 0.0

        # 5th axis: deterministic (method, area) overlap. Falls back to 0
        # when subject_atoms is empty (no chain head) or candidate has no
        # active services — score gracefully degrades to 4-axis v2.
        cand_atoms = candidate_atoms_by_booksy.get(booksy_id, Counter())
        profile_overlap_sim = compute_profile_overlap_sim(subject_atoms, cand_atoms)

        composite = compute_composite_score_v2(
            focus_tid_sim=focus_tid_sim,
            focus_var_sim=focus_var_sim,
            portfolio_embedding_sim=portfolio_emb_sim,
            reviews_count_similarity=rc_sim,
            distance_km=distance_km,
            profile_overlap_sim=profile_overlap_sim,
        )

        bucket = assign_bucket_v2(
            focus_tid_sim=focus_tid_sim,
            portfolio_embedding_sim=portfolio_emb_sim,
            reviews_count=c.get("reviews_count") or 0,
            candidate_reviews_rank=c.get("reviews_rank"),
            subject_reviews_rank=subject_reviews_rank,
            profile_overlap_sim=profile_overlap_sim,
        )
        if bucket is None:
            dropped_bucket += 1
            if tracer is not None:
                tracer.add(
                    step="selection.candidate_evaluated",
                    data={
                        "decision": "reject",
                        "reject_reason": "bucket_or_gate_failed",
                        "salon_id": salon_id,
                        "booksy_id": booksy_id,
                        "name": c.get("name"),
                        "composite_score": round(composite, 2),
                        "components": {
                            "focus_tid_sim": round(focus_tid_sim, 4),
                            "focus_var_sim": round(focus_var_sim, 4),
                            "portfolio_embedding_sim": round(portfolio_emb_sim, 4),
                            "reviews_count_similarity": round(rc_sim, 4),
                            "tid_set_overlap_asym": round(tid_set_overlap_asym, 4),
                            "profile_overlap_sim": round(profile_overlap_sim, 4),
                            "distance_km": distance_km,
                        },
                        "reviews_count": c.get("reviews_count") or 0,
                        "candidate_reviews_rank": c.get("reviews_rank"),
                        "subject_reviews_rank": subject_reviews_rank,
                        "note": (
                            "assign_bucket_v2 returned None — none of the "
                            "OR-gate thresholds (focus_tid_sim>=0.18 OR "
                            "portfolio_emb_sim>=0.55 OR profile_overlap_sim "
                            ">=0.15) were met."
                        ),
                    },
                    salon_ref_id=salon_id,
                )
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
                    "focus_tid_sim": round(focus_tid_sim, 4),
                    "focus_var_sim": round(focus_var_sim, 4),
                    "portfolio_embedding_sim": round(portfolio_emb_sim, 4),
                    "reviews_count_similarity": round(rc_sim, 4),
                    "distance_penalty": round(compute_distance_penalty(distance_km), 2),
                    "tid_set_overlap_asym": round(tid_set_overlap_asym, 4),
                    "profile_overlap_sim": round(profile_overlap_sim, 4),
                },
                partner_system=partner,
            )
        )
        if tracer is not None:
            tracer.add(
                step="selection.candidate_evaluated",
                data={
                    "decision": "accept",
                    "salon_id": salon_id,
                    "booksy_id": booksy_id,
                    "name": c.get("name"),
                    "city": c.get("city"),
                    "bucket": bucket,
                    "counts_in_aggregates": bucket != "new",
                    "composite_score": round(composite, 2),
                    "components": {
                        "focus_tid_sim": round(focus_tid_sim, 4),
                        "focus_var_sim": round(focus_var_sim, 4),
                        "portfolio_embedding_sim": round(portfolio_emb_sim, 4),
                        "reviews_count_similarity": round(rc_sim, 4),
                        "tid_set_overlap_asym": round(tid_set_overlap_asym, 4),
                        "profile_overlap_sim": round(profile_overlap_sim, 4),
                        "distance_penalty": round(
                            compute_distance_penalty(distance_km), 2
                        ),
                        "distance_km": distance_km,
                    },
                    "reviews_count": c.get("reviews_count") or 0,
                    "reviews_rank": c.get("reviews_rank"),
                    "partner_system": partner,
                },
                salon_ref_id=salon_id,
            )

    logger.info(
        "Scored %d/%d candidates (dropped %d by female_weight, %d by bucket, "
        "%d without pre-computed focus)",
        len(scored), len(raw_candidates), dropped_fw, dropped_bucket, dropped_no_focus,
    )

    # --- 5. Sort by bucket priority then descending composite_score ----------
    scored.sort(key=sort_key)

    # --- 6. Return top N ------------------------------------------------------
    cap = target_count if mode == "auto" else 15
    final = scored[:cap]

    # --- 6b. UNION user-picked salons (must-include) -------------------------
    # The frontend competitor picker lets users force specific salons into the
    # report. This is a UNION (never a filter): market analytics stay computed
    # from the full deterministic `final` set; we only ADD picks that the
    # deterministic selection would otherwise drop. Three cases:
    #   (a) pick already in `final`            → just mark is_user_selected
    #   (b) pick in `scored` but cap-dropped   → mark + append to `final`
    #   (c) pick not in `scored` at all        → fetch salon row, construct a
    #       minimal force-added candidate (counts_in_aggregates=False so it
    #       never distorts aggregates), append to `final`
    must = {int(x) for x in (must_include_salon_ids or [])}
    if must:
        # (a)/(b): mark every scored candidate that is a must-include pick.
        # `final` is a slice of `scored`, so flags set on `scored` items are
        # visible in `final` for the picks already present (case a).
        scored_by_salon: dict[int, CompetitorCandidate] = {}
        for c in scored:
            if c.salon_id in must:
                c.is_user_selected = True
            scored_by_salon[c.salon_id] = c

        # Snapshot which picks were already in the deterministic cap before we
        # mutate `final` (for the trace/log breakdown below).
        in_final_before = {c.salon_id for c in final}
        n_already_in_final = len(must & in_final_before)

        seen_salon_ids = set(in_final_before)

        # (b): cap-dropped picks present in `scored` but not in `final`.
        n_cap_readded = 0
        for sid in must:
            if sid in scored_by_salon and sid not in seen_salon_ids:
                final.append(scored_by_salon[sid])
                seen_salon_ids.add(sid)
                n_cap_readded += 1

        # (c): picks that never reached `scored` (filtered earlier or not raw
        # candidates). Fetch their salon rows and build minimal candidates.
        missing_ids = [
            sid for sid in must
            if sid not in scored_by_salon and sid not in seen_salon_ids
        ]
        if missing_ids:
            try:
                salon_rows = await service.get_salons_by_ids(missing_ids)
            except Exception:
                logger.exception(
                    "get_salons_by_ids failed for must-include picks %s — "
                    "those picks will be absent from the report",
                    missing_ids,
                )
                salon_rows = []
            rows_by_id = {
                r["id"]: r for r in salon_rows if r.get("id") is not None
            }
            for sid in missing_ids:
                row = rows_by_id.get(sid)
                if row is None:
                    logger.warning(
                        "Must-include pick salon_id=%s not found in salons "
                        "table — skipping (audit_id=%s)",
                        sid, subject_audit_id,
                    )
                    continue
                final.append(
                    CompetitorCandidate(
                        salon_id=sid,
                        booksy_id=int(row.get("booksy_id") or 0),
                        name=row.get("name") or "",
                        city=row.get("city"),
                        # salons table has no primary_category_id of its own;
                        # picks are same-category competitors, so fall back to
                        # the subject's category.
                        primary_category_id=(
                            row.get("primary_category_id")
                            if row.get("primary_category_id") is not None
                            else subject_primary_cat
                        ),
                        reviews_count=int(row.get("reviews_count") or 0),
                        reviews_rank=row.get("reviews_rank"),
                        # distance not load-bearing for force-added picks
                        # (counts_in_aggregates=False); 0.0 per spec.
                        distance_km=0.0,
                        female_weight_diff=-1.0,  # sentinel (see line ~852)
                        composite_score=0.0,
                        bucket="new",  # lowest-priority real bucket
                        counts_in_aggregates=False,  # MUST NOT distort aggregates
                        similarity_scores={},
                        partner_system=row.get("partner_system") or "native",
                        is_user_selected=True,
                    )
                )
                seen_salon_ids.add(sid)

        logger.info(
            "Union: %d user-picked salon_ids — %d already in final, "
            "%d cap-dropped re-added, %d force-added (not in scored)",
            len(must), n_already_in_final, n_cap_readded, len(missing_ids),
        )

    # Final trace: summary + which candidates made the cut. Lets us answer
    # "which were dropped at the target_count cap vs which actually failed
    # scoring/bucketing".
    if tracer is not None:
        tracer.add(
            step="selection.summary",
            data={
                "raw_count": len(raw_candidates),
                "scored_count": len(scored),
                "returned_count": len(final),
                "cap": cap,
                "dropped_female_weight": dropped_fw,
                "dropped_bucket": dropped_bucket,
                "dropped_no_focus": dropped_no_focus,
                "dropped_by_cap": max(0, len(scored) - cap),
                "returned_booksy_ids": [c.booksy_id for c in final],
                "user_selected_salon_ids": [
                    c.salon_id for c in final if c.is_user_selected
                ],
                "buckets": {
                    "direct": sum(1 for c in final if c.bucket == "direct"),
                    "cluster": sum(1 for c in final if c.bucket == "cluster"),
                    "aspirational": sum(
                        1 for c in final if c.bucket == "aspirational"
                    ),
                    "new": sum(1 for c in final if c.bucket == "new"),
                    "alternative": sum(
                        1 for c in final if c.bucket == "alternative"
                    ),
                },
            },
            salon_ref_id=subject_salon_id,
        )

    return final
