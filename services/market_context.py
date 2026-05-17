"""Market context sample gathering for subject_only pricing rows.

When the subject service has no exact (variant_id, brand) match among
the selected competitors, we still want to surface relevant market
context — services from competitors that share either the brand marker
OR the same method + body_area_set as the subject. The result feeds
into competitor_pricing_comparisons.related_samples (mig 089) so the UI
can render "Brak w tej konfiguracji — konkurenci oferują:" with concrete
alternatives instead of a misleading "Tylko Ty na rynku" badge.

Each emitted sample carries a `relation` discriminator so the UI can
group / label them:

  - 'same_brand'  — same brand_marker as subject, different config
                    (e.g. subject "Red Touch dekolt", related "ESTHETIC&MED
                    Laser Red Touch twarz-szyja 1490 zł").
  - 'same_method' — same method_marker AND area overlap as subject,
                    different brand or no brand (e.g. subject "Thunder
                    pachy + bikini pełne", related "Soprano - pachy
                    400 zł"). Requires subject_method != 'generic' to
                    avoid catch-all generic↔generic matches; requires
                    subject_areas to be non-empty so the row stays
                    targeted.

Capped at MAX_SAMPLES to keep the JSONB row reasonable. Brand matches
are prioritized, then ascending by price.
"""
from __future__ import annotations

from typing import Any, Iterable

from services.body_area_taxonomy import extract_body_areas
from services.brand_marker import extract_brand_marker
from services.method_marker import extract_method_marker


MAX_SAMPLES = 50


def gather_market_context_samples(
    subject_svc: dict[str, Any],
    aligned_competitors: Iterable[tuple[Any, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Return a list of competitor samples relevant to the subject.

    Args:
      subject_svc: subject's service dict (name, category_name required).
      aligned_competitors: iterable of (CompetitorCandidate, cdata_dict).
        cdata_dict carries scrape.salon_name and services list.

    Returns:
      List of sample dicts (capped at MAX_SAMPLES). Each dict has the
      same shape as competitor_pricing_comparisons.competitor_samples
      entries plus a `relation` field ('same_brand' or 'same_method').
      Brand matches first, then ascending by price.
    """
    name = (subject_svc.get("name") or "").strip()
    if not name:
        return []
    subj_brand = extract_brand_marker(name)
    subj_method = extract_method_marker(
        name, subject_svc.get("category_name") or ""
    )
    subj_areas = extract_body_areas(name)

    out: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any]] = set()

    for cand, cdata in aligned_competitors:
        if not getattr(cand, "counts_in_aggregates", True):
            continue
        salon_name = (cdata.get("scrape") or {}).get("salon_name") or ""
        cand_booksy_id = getattr(cand, "booksy_id", None)
        for svc in cdata.get("services") or []:
            if not svc.get("is_active", True):
                continue
            price = svc.get("price_grosze")
            if price is None:
                continue
            svc_name = (svc.get("name") or "").strip()
            if not svc_name:
                continue
            sample_key = (cdata.get("salon_id"), svc.get("id"))
            if sample_key in seen:
                continue
            cand_brand = extract_brand_marker(svc_name)
            cand_method = extract_method_marker(
                svc_name, svc.get("category_name") or ""
            )
            cand_areas = extract_body_areas(svc_name)

            relation: str | None = None
            if subj_brand is not None and cand_brand == subj_brand:
                # Same brand, any config — most relevant context.
                relation = "same_brand"
            elif (
                subj_method != "generic"
                and cand_method == subj_method
                and subj_areas
                # If candidate has no area markers, fall back to method
                # match (generic "Depilacja laserowa pachy" is a fair
                # comparison to "Thunder pachy"). If candidate has areas
                # we require overlap to avoid mixing dłonie ↔ stopy.
                and (not cand_areas or bool(subj_areas & cand_areas))
            ):
                relation = "same_method"

            if relation is None:
                continue
            seen.add(sample_key)
            out.append({
                "salon_id": cdata.get("salon_id"),
                "salon_name": salon_name,
                "booksy_id": cand_booksy_id,
                "service_id": svc.get("id"),
                "service_name": svc_name,
                "price_grosze": int(price),
                "duration_minutes": svc.get("duration_minutes"),
                "brand_marker": cand_brand,
                "method_marker": cand_method,
                "relation": relation,
            })

    # Sort: same_brand first (most relevant), then ascending price.
    out.sort(key=lambda s: (0 if s["relation"] == "same_brand" else 1,
                            s["price_grosze"]))
    return out[:MAX_SAMPLES]
