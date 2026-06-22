"""Integracja silnika similarity-first z raportem konkurencji.

`compute_pricing_comparisons_v2` to drop-in za stary `_compute_pricing_comparisons`
(competitor_analysis.py). Produkuje wiersze w kontrakcie `competitor_pricing_comparisons`
(te same kolumny które czyta frontend), ale liczy je nowym silnikiem (fn_find_related_v2
+ test tożsamości), nie tierami variant/method/structured.

Ziarno: jeden wiersz per realna usługa subjectu (nie per variant taksonomii).
"""
from __future__ import annotations

import logging
from typing import Any

from .engine import MarketResult, compute_market_price

logger = logging.getLogger(__name__)

# Próg deviation poniżej/powyżej którego rekomendujemy zmianę ceny (w %).
_ACTION_THRESHOLD_PCT = 8.0
# Limit bliźniaków per usługa z fn_find_related_v2.
_FN_LIMIT = 60
_FN_MIN_SIMILARITY = 0.82


def _recommended_action(deviation_pct: float | None) -> str:
    """raise (subject tańszy → podnieś) / lower (drożej → obniż) / hold."""
    if deviation_pct is None:
        return "hold"
    if deviation_pct > _ACTION_THRESHOLD_PCT:
        return "lower"
    if deviation_pct < -_ACTION_THRESHOLD_PCT:
        return "raise"
    return "hold"


def _sample_to_jsonb(s: dict[str, Any]) -> dict[str, Any]:
    """Bliźniak → wpis competitor_samples (drill-down w UI)."""
    return {
        "salon_id": s.get("salon_id"),
        "salon_name": s.get("salon_name") or "",
        "booksy_id": s.get("booksy_id"),
        "service_id": s.get("service_id"),
        "service_name": s.get("service_name") or "",
        "price_grosze": s.get("price_grosze"),
        "duration_minutes": s.get("duration_minutes"),
        "name_similarity": s.get("similarity"),
    }


def _build_row(
    report_id: int,
    subject: dict[str, Any],
    res: MarketResult,
    package_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    """Zmapuj MarketResult na wiersz competitor_pricing_comparisons (kontrakt UI)."""
    tid = subject.get("booksy_treatment_id")
    treatment_name = subject.get("treatment_name") or subject.get("name") or subject.get("service_name") or "Unknown"
    subj_price = subject.get("price_grosze")
    subj_dur = subject.get("duration_minutes")

    # subject_percentile: pozycja subjectu w rozkładzie rynku (jeśli mamy cenę rynkową)
    subject_percentile: float | None = None
    if res.market_price_grosze and subj_price and res.p25_grosze is not None and res.p75_grosze is not None:
        lo, hi = res.p25_grosze, res.p75_grosze
        if hi > lo:
            subject_percentile = round(max(0.0, min(100.0, (subj_price - lo) / (hi - lo) * 50.0 + 25.0)), 2)

    insufficient = res.status == "insufficient"
    verification_status = "subject_only" if insufficient else "verified"
    recommended_action = "subject_only" if insufficient else _recommended_action(res.deviation_pct)

    # deviation_pct_per_min: subject zł/min vs rynek zł/min
    deviation_pct_per_min: float | None = None
    if not insufficient and res.zl_per_min_median and subj_price and subj_dur:
        subj_ppm = subj_price / subj_dur if subj_dur else None
        if subj_ppm and res.zl_per_min_median:
            deviation_pct_per_min = round((subj_ppm - res.zl_per_min_median) / res.zl_per_min_median * 100.0, 2)

    return {
        "report_id": report_id,
        "comparison_tier": "identity",
        "booksy_treatment_id": tid if tid is not None else 0,  # kontrakt NOT NULL
        "treatment_name": treatment_name,
        "treatment_parent_id": subject.get("treatment_parent_id"),
        "subject_price_grosze": subj_price,
        "subject_is_from_price": subject.get("is_from_price", False),
        "subject_duration_minutes": subj_dur,
        "subject_price_per_min_grosze": round(subj_price / subj_dur) if (subj_price and subj_dur) else None,
        "market_min_grosze": res.p25_grosze if not insufficient else None,  # robust: p25 jako min wyświetlany
        "market_p25_grosze": res.p25_grosze if not insufficient else None,
        "market_median_grosze": res.market_price_grosze,
        "market_p75_grosze": res.p75_grosze if not insufficient else None,
        "market_max_grosze": res.p75_grosze if not insufficient else None,
        "market_price_per_min_grosze_median": round(res.zl_per_min_median) if res.zl_per_min_median else None,
        "subject_percentile": subject_percentile,
        "deviation_pct": round(res.deviation_pct, 2) if res.deviation_pct is not None else None,
        "deviation_pct_per_min": deviation_pct_per_min,
        "sample_size": res.n_unique_salons,
        "recommended_action": recommended_action,
        "verification_status": verification_status,
        "competitor_samples": [_sample_to_jsonb(s) for s in res.samples],
        # PAKIETY do drill-down (§9 README): odfiltrowane z ceny przez oś `package`,
        # ale zebrane tu w `package_samples`. Dodaj kolumnę JSONB `package_samples`
        # na competitor_pricing_comparisons i odkomentuj, gdy budujesz widok pakietów:
        # "package_samples": [_sample_to_jsonb(s) for s in package_samples] or None,
    }


async def compute_pricing_comparisons_v2(
    service: Any,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[Any, dict[str, Any]]],
    *,
    config: dict[str, Any] | None = None,
    **_ignored: Any,
) -> list[dict[str, Any]]:
    """Policz pricing_comparisons nowym silnikiem (similarity + tożsamość).

    Sygnatura zgodna ze starą `_compute_pricing_comparisons` (przyjmuje i ignoruje
    dodatkowe kwargs: audit_id, tracer, method_classifier, llm_client). Zwraca listę
    wierszy gotowych do insert_competitor_pricing_comparisons.
    """
    subject_services = [
        s for s in (subject_data.get("services") or [])
        if s.get("is_active", True) and s.get("price_grosze") and s.get("id") is not None
    ]
    competitor_booksy_ids = [
        cand.booksy_id for cand, _ in aligned_competitors
        if getattr(cand, "counts_in_aggregates", True)
    ]
    if not subject_services or not competitor_booksy_ids:
        logger.info("similarity pricing v2: brak subject services lub konkurentów — 0 wierszy")
        return []

    subject_ids = [int(s["id"]) for s in subject_services]

    try:
        res = service.client.rpc(
            "fn_find_related_competitor_services_v2",
            {
                "p_subject_service_ids": subject_ids,
                "p_competitor_booksy_ids": list(competitor_booksy_ids),
                "p_limit": _FN_LIMIT,
                "p_min_similarity": _FN_MIN_SIMILARITY,
            },
        ).execute()
    except Exception as e:
        logger.warning("similarity pricing v2: RPC fn_find_related_v2 failed: %s", e)
        return []

    # Grupuj bliźniaków per subject_service_id.
    clusters: dict[int, list[dict[str, Any]]] = {sid: [] for sid in subject_ids}
    for row in (res.data or []):
        sid = row.get("subject_service_id")
        if sid is None or row.get("price_grosze") is None:
            continue
        clusters.setdefault(int(sid), []).append({
            "service_id": row.get("service_id"),
            "booksy_id": row.get("booksy_id"),
            "salon_id": row.get("salon_id"),
            "salon_name": row.get("salon_name") or "",
            "service_name": row.get("service_name") or "",
            "price_grosze": int(row["price_grosze"]),
            "duration_minutes": row.get("duration_minutes"),
            "similarity": float(row["similarity"]) if row.get("similarity") is not None else None,
            "category_name": row.get("category_name"),
            "is_package": bool(row.get("is_package", False)),
        })

    rows: list[dict[str, Any]] = []
    for svc in subject_services:
        sid = int(svc["id"])
        raw = clusters.get(sid, [])
        # Pakiety odłóż osobno (drill-down); test tożsamości i tak je wytnie z ceny.
        package_samples = [s for s in raw if s.get("is_package")]
        subject = {
            "service_name": svc.get("name") or svc.get("treatment_name") or "",
            "price_grosze": svc.get("price_grosze"),
            "duration_minutes": svc.get("duration_minutes"),
            "category_name": svc.get("category_name"),
            "is_package": bool(svc.get("is_package", False)),
        }
        result = compute_market_price(subject, raw, config)
        rows.append(_build_row(report_id, svc, result, package_samples))

    logger.info(
        "similarity pricing v2: %d wierszy (%d sufficient/thin, %d subject_only)",
        len(rows),
        sum(1 for r in rows if r["market_median_grosze"] is not None),
        sum(1 for r in rows if r["market_median_grosze"] is None),
    )
    return rows
