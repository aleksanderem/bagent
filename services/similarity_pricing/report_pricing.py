"""Integracja silnika similarity-first z raportem konkurencji.

`compute_pricing_comparisons_v2` to drop-in za stary `_compute_pricing_comparisons`.
Liczy ceny nowym silnikiem (Qdrant kNN + test tożsamości).

JEDEN matching, jeden wiersz per usługa. Pula = konkurenci w promieniu N km
(∪ wybrani konkurenci raportu). Cena rynkowa liczona z całego klastra (rynek
okolicy — bogaty, wiarygodny). Wybrani konkurenci NIE są osobnym przebiegiem,
tylko warstwą: każdy bliźniak w `competitor_samples` ma flagę `is_selected`, więc
UI pokazuje rynek z wyróżnionymi rywalami (jeden widok, bez pustej zakładki).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .engine import MarketResult, compute_market_price
from .qdrant_search import compute_peer_max_sims, fetch_twin_vectors, search_twins

logger = logging.getLogger(__name__)

_ACTION_THRESHOLD_PCT = 8.0
_FN_LIMIT = 80
_FN_MIN_SIMILARITY = 0.82
_DEFAULT_RADIUS_KM = 15

# Adaptacyjny próg podobieństwa. Gdy przy precyzyjnym progu (_FN_MIN_SIMILARITY)
# mniej niż TRIGGER usług dostaje wiarygodny rynek, salon jest w rzadkim
# otoczeniu (mało dokładnych twins ≥ progu) — ponawiamy wycenę luźniejszym
# progiem FALLBACK, żeby nie pokazywać klientowi raportu ~100% „Tylko Ty na
# rynku" tam, gdzie odpowiedniki istnieją tuż poniżej progu. Gęste/typowe salony
# nie schodzą poniżej triggera, więc ich precyzja zostaje nietknięta.
_ADAPTIVE_TRIGGER_VERIFIED_RATE = 0.20
_ADAPTIVE_FALLBACK_SIMILARITY = 0.75


def _recommended_action(deviation_pct: float | None) -> str:
    if deviation_pct is None:
        return "hold"
    if deviation_pct > _ACTION_THRESHOLD_PCT:
        return "lower"
    if deviation_pct < -_ACTION_THRESHOLD_PCT:
        return "raise"
    return "hold"


def _sample_to_jsonb(s: dict[str, Any]) -> dict[str, Any]:
    return {
        "salon_id": s.get("salon_id"),
        "salon_name": s.get("salon_name") or "",
        "booksy_id": s.get("booksy_id"),
        "service_id": s.get("service_id"),
        "service_name": s.get("service_name") or "",
        "price_grosze": s.get("price_grosze"),
        "duration_minutes": s.get("duration_minutes"),
        "name_similarity": s.get("similarity"),
        # Wyróżnienie wybranych konkurentów raportu w drill-downie (UI: badge).
        "is_selected": bool(s.get("is_selected", False)),
        # Pakiet (UI segment "Pakiety konkurencji" w drill-down modalu); silnik już
        # wycina pakiety z market_price w layer_unit, frontend filtruje wg viewMode.
        "is_package": bool(s.get("is_package", False)),
    }


def _geo_competitor_booksy_ids(service: Any, subject_booksy_id: int, radius_km: int) -> list[int]:
    """booksy_ids salonów w promieniu radius_km (RPC fn_competitors_in_radius).

    PostgREST RPC zwracający SETOF INTEGER daje listę skalarów; domyślny limit 1000
    — podnosimy, bo gęste miasto ma tysiące salonów.
    """
    try:
        res = service.client.rpc(
            "fn_competitors_in_radius",
            {"p_subject_booksy_id": int(subject_booksy_id), "p_radius_km": radius_km},
        ).limit(20000).execute()
        out: list[int] = []
        for r in (res.data or []):
            v = r if isinstance(r, int) else (r.get("fn_competitors_in_radius") if isinstance(r, dict) else None)
            if v is not None:
                out.append(int(v))
        return out
    except Exception as e:
        logger.warning("geo radius RPC failed (booksy=%s, %dkm): %s", subject_booksy_id, radius_km, e)
        return []


def _lookup_salon_names(service: Any, booksy_ids: list[int]) -> dict[int, str]:
    """booksy_id → salon name (batch, dla drill-down)."""
    if not booksy_ids:
        return {}
    names: dict[int, str] = {}
    uniq = list({int(b) for b in booksy_ids if b is not None})
    for i in range(0, len(uniq), 500):
        chunk = uniq[i:i + 500]
        try:
            res = service.client.table("salons").select("booksy_id,name").in_("booksy_id", chunk).execute()
            for row in (res.data or []):
                if row.get("booksy_id") is not None:
                    names[int(row["booksy_id"])] = row.get("name") or ""
        except Exception as e:
            logger.warning("salon_names lookup failed (chunk %d): %s", i, e)
    return names


def _fetch_subject_embeddings(service: Any, subject_ids: list[int]) -> dict[int, list[float]]:
    """service_id → name_embedding (z Postgresa). Subject z audit scrape nie jest
    w Qdrant, więc embeddingi do wyszukiwania bierzemy stąd. pgvector przez PostgREST
    przychodzi jako string '[...]' — parsujemy do listy floatów."""
    out: dict[int, list[float]] = {}
    for i in range(0, len(subject_ids), 200):
        chunk = subject_ids[i:i + 200]
        try:
            res = service.client.table("salon_scrape_services").select(
                "id,name_embedding"
            ).in_("id", chunk).execute()
            for row in (res.data or []):
                emb = row.get("name_embedding")
                if emb is None or row.get("id") is None:
                    continue
                vec = json.loads(emb) if isinstance(emb, str) else emb
                if vec:
                    out[int(row["id"])] = vec
        except Exception as e:
            logger.warning("subject embeddings fetch failed (chunk %d): %s", i, e)
    return out


def _build_row(
    report_id: int, subject: dict[str, Any], res: MarketResult,
) -> dict[str, Any]:
    tid = subject.get("booksy_treatment_id")
    treatment_name = subject.get("treatment_name") or subject.get("name") or subject.get("service_name") or "Unknown"
    subj_price = subject.get("price_grosze")
    subj_dur = subject.get("duration_minutes")

    subject_percentile: float | None = None
    if res.market_price_grosze and subj_price and res.p25_grosze is not None and res.p75_grosze is not None:
        lo, hi = res.p25_grosze, res.p75_grosze
        if hi > lo:
            subject_percentile = round(max(0.0, min(100.0, (subj_price - lo) / (hi - lo) * 50.0 + 25.0)), 2)

    insufficient = res.status == "insufficient"
    verification_status = "subject_only" if insufficient else "verified"
    recommended_action = "subject_only" if insufficient else _recommended_action(res.deviation_pct)

    deviation_pct_per_min: float | None = None
    if not insufficient and res.zl_per_min_median and subj_price and subj_dur:
        subj_ppm = subj_price / subj_dur if subj_dur else None
        if subj_ppm and res.zl_per_min_median:
            deviation_pct_per_min = round((subj_ppm - res.zl_per_min_median) / res.zl_per_min_median * 100.0, 2)

    return {
        "report_id": report_id,
        "comparison_tier": "identity",
        "booksy_treatment_id": tid if tid is not None else 0,
        "treatment_name": treatment_name,
        "treatment_parent_id": subject.get("treatment_parent_id"),
        "subject_price_grosze": subj_price,
        "subject_is_from_price": subject.get("is_from_price", False),
        "subject_duration_minutes": subj_dur,
        "subject_price_per_min_grosze": round(subj_price / subj_dur) if (subj_price and subj_dur) else None,
        "market_min_grosze": res.p25_grosze if not insufficient else None,
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
        # PAKIETY do drill-down (§9 README): kolumna package_samples gdy budujesz widok.
    }


async def compute_pricing_comparisons_v2(
    service: Any,
    report_id: int,
    subject_data: dict[str, Any],
    aligned_competitors: list[tuple[Any, dict[str, Any]]],
    *,
    radius_km: int = _DEFAULT_RADIUS_KM,
    config: dict[str, Any] | None = None,
    **_ignored: Any,
) -> list[dict[str, Any]]:
    """Policz pricing_comparisons — JEDEN matching, cena z rynku w promieniu N km,
    wybrani konkurenci wyróżnieni flagą is_selected w competitor_samples."""
    subject_services = [
        s for s in (subject_data.get("services") or [])
        if s.get("is_active", True) and s.get("price_grosze") and s.get("id") is not None
    ]
    if not subject_services:
        return []
    subject_ids = [int(s["id"]) for s in subject_services]
    subject_booksy = subject_data.get("booksy_id")

    selected_booksy = {
        cand.booksy_id for cand, _ in aligned_competitors
        if getattr(cand, "counts_in_aggregates", True)
    }
    radius_booksy = (
        _geo_competitor_booksy_ids(service, int(subject_booksy), radius_km)
        if subject_booksy is not None else []
    )

    # MATCHING RAZ na sumie pul (wybrani zwykle ⊂ promień, ale gwarantujemy że są).
    all_booksy = list(set(radius_booksy) | selected_booksy)
    if not all_booksy:
        return []

    # Subject pochodzi z AUDIT scrape (nie chain-head) — NIE ma go w Qdrant.
    # Embeddingi subjectów bierzemy z Postgresa, w Qdrant pytamy tylko o konkurentów.
    subject_embeddings = _fetch_subject_embeddings(service, subject_ids)
    salon_names = _lookup_salon_names(service, all_booksy)

    def _price_at(min_similarity: float) -> list[dict[str, Any]]:
        """Jeden pełny przebieg wyceny przy danym progu podobieństwa twins."""
        clusters = search_twins(
            subject_ids, all_booksy, subject_embeddings=subject_embeddings,
            limit=_FN_LIMIT, min_similarity=min_similarity,
        )
        out: list[dict[str, Any]] = []
        for svc in subject_services:
            sid = int(svc["id"])
            raw = clusters.get(sid, [])
            # peer_max_sim dla coherence guard — z WEKTORÓW QDRANTA (to samo
            # źródło co twins; retrieve+cosine). Postgres RPC fn_pairwise miał
            # ~7% pokrycia twin-id po mig 149 → guard martwy w produkcji.
            twin_ids = [x["service_id"] for x in raw if x.get("service_id") is not None]
            peer_vecs = fetch_twin_vectors(twin_ids)
            peer_sims = compute_peer_max_sims(twin_ids, peer_vecs)
            for s in raw:
                bid = s.get("booksy_id")
                s["salon_name"] = salon_names.get(bid, "")
                s["is_selected"] = bid in selected_booksy
                pm = peer_sims.get(s.get("service_id"))
                if pm is not None:
                    s["peer_max_sim"] = pm
            subject = {
                "service_name": svc.get("name") or svc.get("treatment_name") or "",
                "price_grosze": svc.get("price_grosze"),
                "duration_minutes": svc.get("duration_minutes"),
                "category_name": svc.get("category_name"),
                "is_package": bool(svc.get("is_package", False)),
            }
            result = compute_market_price(subject, raw, config)
            out.append(_build_row(report_id, svc, result))
        return out

    def _n_verified(rs: list[dict[str, Any]]) -> int:
        return sum(1 for r in rs if r["verification_status"] == "verified")

    # PASS 1 — precyzyjny próg (dokładne twins). Domyślne zachowanie.
    rows = _price_at(_FN_MIN_SIMILARITY)
    n_verified = _n_verified(rows)
    verified_rate = n_verified / len(subject_services)

    # ADAPTACYJNY FALLBACK — tylko gdy pokrycie skrajnie niskie (rzadkie
    # otoczenie). Gęste salony nie schodzą poniżej triggera, więc ich precyzja
    # zostaje. Rzadkie dostają szerszy rynek, oznaczony flagą dla UI.
    broadened_sim: float | None = None
    if verified_rate < _ADAPTIVE_TRIGGER_VERIFIED_RATE:
        rows_b = _price_at(_ADAPTIVE_FALLBACK_SIMILARITY)
        n_verified_b = _n_verified(rows_b)
        if n_verified_b > n_verified:
            for r in rows_b:
                vd = r.get("verification_details") or {}
                vd["matching_broadened"] = True
                vd["min_similarity_used"] = _ADAPTIVE_FALLBACK_SIMILARITY
                r["verification_details"] = vd
            rows, n_verified, broadened_sim = rows_b, n_verified_b, _ADAPTIVE_FALLBACK_SIMILARITY

    n_priced = sum(1 for r in rows if r["market_median_grosze"] is not None)
    logger.info(
        "similarity pricing v2: %d wierszy (z ceną=%d, verified=%d/%d) | pula %d salonów "
        "(%d wybranych, %d w %dkm)%s",
        len(rows), n_priced, n_verified, len(subject_services), len(all_booksy),
        len(selected_booksy), len(radius_booksy), radius_km,
        f" | ADAPTIVE broadened→{broadened_sim}" if broadened_sim else "",
    )
    return rows
