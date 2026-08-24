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
# 2026-08-23 (BEAUTY_AUDIT-bgp3): trigger podniesiony 0.20 -> 0.50. Pomiar na
# raporcie 250 (klinika med-est, Warszawa, 221 usług): verified_rate wyszedł
# 45/221 = 0.204 — o 0.4 punktu ZA WYSOKO, żeby fallback się odpalił. Raport
# wyszedł z 72% wierszy "Tylko Ty" mimo że przy progu 0.75 dostaje 73 zamiast
# 45 wierszy z ceną. Próg 0.20 zakładał, że salon poniżej niego jest w pustce;
# w praktyce salon z 20-50% pokrycia też traci połowę raportu. Fallback i tak
# przyjmuje wynik TYLKO gdy poprawia (n_verified_b > n_verified), więc gęste
# salony niczego nie tracą — kończą na precyzyjnym 0.82 jak dotąd.
# 2026-08-24 (BEAUTY_AUDIT-bgp3, druga iteracja): 0.50 -> 0.80. Pierwszy żywy
# raport po podniesieniu progu z 0.20 na 0.50 (nr 259, JETSET CLINIC) wyszedł z
# verified_rate 54/101 = 0.535 i znowu minął się z ratunkiem — o 3.5 punktu.
# Ten sam mechanizm co przy raporcie 250 (0.204 vs próg 0.20), tylko piętro
# wyżej: podnoszenie progu o krok PRZESUWA granicę, zamiast ją usunąć.
# Pomiar na 259 (chain-head e91df2f5, ten sam którego użyła produkcja):
#   próg 0.82 -> 47/101 "Tylko Ty" (47%), 54 z ceną
#   próg 0.75 -> 23/101 (23%),           78 z ceną   <- kryterium odbioru <=25%
#   próg 0.70 -> 16/101 (16%),           85 z ceną
# Fallback przyjmuje wynik TYLKO gdy zwiększa liczbę verified, więc ilościowo
# nie da się pogorszyć — próg triggera jest wyłącznie oszczędnością czasu
# (drugi przebieg wyceny to ~85 s przy raporcie trwającym ~3 min). Przy 0.80
# luźniejsze dopasowanie staje się domyślne dla większości raportów, a
# precyzyjne 0.82 zostaje dla salonów o pokryciu powyżej 80% — świadoma zmiana
# charakteru silnika z "precyzyjny z ratunkiem" na "luźny z wyjątkiem".
_ADAPTIVE_TRIGGER_VERIFIED_RATE = 0.80
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


def _lookup_salons(service: Any, booksy_ids: list[int]) -> dict[int, dict[str, Any]]:
    """booksy_id → {"id": salons.id, "name": nazwa} (batch, dla drill-down i Fazy 8a).

    Dwie przestrzenie identyfikatorów: ``booksy_id`` (zewnętrzny numer Booksy,
    klucz w Qdrancie i w geo RPC) oraz ``salons.id`` (wewnętrzny PK, klucz w
    ``competitor_matches.competitor_salon_id``). Próbki z Qdranta mają tylko
    booksy_id — salon_id domapowujemy TU, żeby Faza 8a porównywała PK z PK.
    """
    if not booksy_ids:
        return {}
    out: dict[int, dict[str, Any]] = {}
    uniq = list({int(b) for b in booksy_ids if b is not None})
    for i in range(0, len(uniq), 500):
        chunk = uniq[i:i + 500]
        try:
            res = service.client.table("salons").select("id,booksy_id,name").in_("booksy_id", chunk).execute()
            for row in (res.data or []):
                if row.get("booksy_id") is not None:
                    out[int(row["booksy_id"])] = {
                        "id": int(row["id"]) if row.get("id") is not None else None,
                        "name": row.get("name") or "",
                    }
        except Exception as e:
            logger.warning("salons lookup failed (chunk %d): %s", i, e)
    return out


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


async def _fetch_subject_embeddings_with_chain_head_fallback(
    service: Any,
    subject_services: list[dict[str, Any]],
    subject_ids: list[int],
    subject_booksy_id: int | None,
) -> tuple[list[dict[str, Any]], list[int], dict[int, list[float]]]:
    """Embeddingi subjecta, z fallbackiem na chain-head scrape tego samego salonu.

    `_fetch_subject_embeddings` na usługach z AUDIT scrape'a zwraca pusty
    słownik, gdy podmiot audytu nie ma jeszcze wektorów nazw usług — świeży
    audyt przed catch-upem nocnego crona (`reference_variant_classification_cron`),
    wyczerpana kwota OpenAI, albo błąd ingestu. Bez fallbacku `search_twins`
    dostaje pusty `subject_embeddings`, każdy wiersz wyceny ląduje jako
    `verification_status='subject_only'`, a raport wygląda na kompletny —
    cicha awaria (BEAUTY_AUDIT-gqul).

    Zamiast poddawać się od razu, próbujemy usług CHAIN-HEAD tego samego
    salonu (`SupabaseService.get_chain_head_services`) — inline embedding przy
    ingest (`_insert_services`) ustawia im wektor zawsze, nocny cron to tylko
    catch-up dla przypadków, które przez to przeszły. Chain-head ma INNE `id`
    usług niż audit scrape, więc lista usług i embeddingi muszą wymienić się
    RAZEM — inaczej `search_twins` dostaje embeddingi kluczowane inaczej niż
    `subject_ids`, które przeszukuje, i traktuje je jako brakujące.

    Zwraca (subject_services, subject_ids, subject_embeddings) — oryginalne
    wejście, gdy audit scrape ma wektory; chain-head trójkę, gdy fallback się
    udał. Pusty `subject_embeddings` w wyniku oznacza, że NIC nie ma wektorów
    (ani audit, ani chain-head) — wywołujący decyduje, jak twardo zareagować.
    """
    subject_embeddings = _fetch_subject_embeddings(service, subject_ids)
    if subject_embeddings or subject_booksy_id is None:
        return subject_services, subject_ids, subject_embeddings

    try:
        ch_scrape_id, ch_services = await service.get_chain_head_services(
            int(subject_booksy_id)
        )
    except Exception as e:
        logger.warning(
            "chain-head fallback: get_chain_head_services failed (booksy_id=%s): %s",
            subject_booksy_id, e,
        )
        return subject_services, subject_ids, subject_embeddings

    ch_subject_services = [
        s for s in ch_services
        if s.get("is_active", True) and s.get("price_grosze") and s.get("id") is not None
    ]
    if not ch_subject_services:
        return subject_services, subject_ids, subject_embeddings

    ch_ids = [int(s["id"]) for s in ch_subject_services]
    ch_embeddings = _fetch_subject_embeddings(service, ch_ids)
    if not ch_embeddings:
        return subject_services, subject_ids, subject_embeddings

    logger.warning(
        "chain-head fallback: subject audit-scrape ma 0/%d name_embedding "
        "(booksy_id=%s) — użyto chain-head scrape %s (%d/%d usług z wektorem)",
        len(subject_ids), subject_booksy_id, ch_scrape_id,
        len(ch_embeddings), len(ch_ids),
    )
    return ch_subject_services, ch_ids, ch_embeddings


def _build_row(
    report_id: int, subject: dict[str, Any], res: MarketResult,
) -> dict[str, Any]:
    tid = subject.get("booksy_treatment_id")
    # 2026-08-24 (BEAUTY_AUDIT-twvf): kolejność ODWRÓCONA — nazwa USŁUGI przed
    # nazwą szuflady Booksy. Szuflada (`treatment_name` z taksonomii Booksy) nie
    # jest jednostką ceny ani jednostką usługi:
    #   pomiar na 1500 skanach — 46% szuflad zawiera >1 usługę salonu, a rozrzut
    #   ceny w nich rośnie monotonicznie z ich liczbą (1,00x przy jednej usłudze,
    #   2,20x przy 4-6, 6,44x przy 13+). Rozkład tego rozrzutu jest GŁADKO
    #   MALEJĄCY, bez doliny — nie istnieje próg "szuflada jeszcze opisuje jedną
    #   usługę / już nie". Najsilniejszy predyktor: ile kategorii cennika obejmuje
    #   szuflada (1 kategoria → 1,50x, 4+ → 5,83x).
    # Skutki brania szuflady jako nazwy były DWA, oba z tej jednej linii:
    #   (a) wiersz raportu nazywał się szufladą, nie usługą;
    #   (b) _dedup_pricing_rows (competitor_analysis.py) ma treatment_name W KLUCZU,
    #       więc dwie usługi z tą samą szufladą i tą samą ceną dostawały identyczny
    #       klucz i jedna z nich znikała. Pomiar na raporcie 259: silnik wyliczył
    #       101 wierszy, do bazy trafiło 87.
    # Nazwa usługi jest unikalna w obrębie cennika salonu, więc jako składnik
    # klucza rozróżnia usługi bez żadnego progu ani heurystyki.
    treatment_name = subject.get("name") or subject.get("service_name") or subject.get("treatment_name") or "Unknown"
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
    # counts_in_aggregates=False to per-subject decyzja z competitor_matches
    # (młody salon bucket='new', albo ręczny must-include z "MUST NOT distort
    # aggregates"). fn_competitors_in_radius o tej fladze nic nie wie, więc
    # wykluczamy jawnie tutaj — inaczej wraca przez radius_booksy i po cichu
    # psuje medianę/percentyle rynku (BEAUTY_AUDIT-2fv6).
    excluded_booksy = {
        cand.booksy_id for cand, _ in aligned_competitors
        if not getattr(cand, "counts_in_aggregates", True)
    }
    radius_booksy = (
        _geo_competitor_booksy_ids(service, int(subject_booksy), radius_km)
        if subject_booksy is not None else []
    )

    # MATCHING RAZ na sumie pul (wybrani zwykle ⊂ promień, ale gwarantujemy że są).
    all_booksy = list((set(radius_booksy) | selected_booksy) - excluded_booksy)
    if not all_booksy:
        return []

    # Subject pochodzi z AUDIT scrape (nie chain-head) — NIE ma go w Qdrant.
    # Embeddingi subjectów bierzemy z Postgresa, w Qdrant pytamy tylko o konkurentów.
    # Gdy audit scrape nie ma jeszcze wektorów, próbujemy chain-head scrape'a
    # tego samego salonu zanim się poddamy (BEAUTY_AUDIT-gqul).
    subject_services, subject_ids, subject_embeddings = (
        await _fetch_subject_embeddings_with_chain_head_fallback(
            service, subject_services, subject_ids, subject_booksy,
        )
    )
    if not subject_embeddings:
        # Ta ścieżka jest user-facing: bez wektorów podmiotu KAŻDY wiersz
        # wyceny ląduje jako verification_status='subject_only' (subject_only
        # → brak market_median_grosze), raport generuje się do końca i z
        # zewnątrz wygląda na kompletny — cicha awaria, zero błędu w bagencie,
        # zero statusu w Convex. Wolimy głośno przerwać etap wyceny.
        raise RuntimeError(
            f"similarity pricing v2: subject (booksy_id={subject_booksy}) nie ma "
            "ANI JEDNEGO name_embedding — ani na audit scrape, ani na chain-head "
            "scrape tego salonu. Wycena bez wektorów podmiotu wygenerowałaby "
            "raport z samymi wierszami 'subject_only' (bez cen rynkowych) bez "
            "żadnego sygnału błędu — przerywam etap zamiast produkować cichy "
            "pusty raport."
        )
    salons_by_booksy = _lookup_salons(service, all_booksy)

    def _price_at(min_similarity: float) -> list[dict[str, Any]]:
        """Jeden pełny przebieg wyceny przy danym progu podobieństwa twins."""
        clusters = search_twins(
            subject_ids, all_booksy, subject_embeddings=subject_embeddings,
            limit=_FN_LIMIT, min_similarity=min_similarity,
        )
        # CROWD-OUT FIX (BEAUTY_AUDIT-0dmq, xi18): powyższy search bierze TOP
        # _FN_LIMIT bliźniaków z całej puli ~9k salonów w promieniu — wybrani
        # konkurenci (zwykle ~15) giną w tłumie. Pomiar (raport 250, exact
        # search): przy wspólnym searchu wybrani mieli 1-4 próbki z 221
        # wierszy; osobny search WYŁĄCZNIE po wybranych przy tym samym progu
        # dawał im 13/9/8/5/5/4/3/3/2/2/1/1/1/1/0. Ten sam wzorzec co Faza 8a
        # (_aggregate_verified_match_counts w pipelines/competitor_analysis.py)
        # — exact=True, bo kolekcja nie ma indeksu payload na booksy_id, więc
        # filtrowany HNSW dla małej puli gubi całe salony. Dogrywamy wynik do
        # `clusters` PRZED pętlą per-usługa, dedup po service_id, żeby
        # is_selected niżej widziało bliźniaków, których pierwszy search
        # zgubił w top-80 z 9k. Pomijamy całkowicie gdy brak wybranych.
        if selected_booksy:
            selected_clusters = search_twins(
                subject_ids, list(selected_booksy), subject_embeddings=subject_embeddings,
                limit=_FN_LIMIT, min_similarity=min_similarity, exact=True,
            )
            for sid, extra in selected_clusters.items():
                existing = clusters.setdefault(sid, [])
                seen_service_ids = {x.get("service_id") for x in existing}
                for x in extra:
                    if x.get("service_id") not in seen_service_ids:
                        existing.append(x)
                        seen_service_ids.add(x.get("service_id"))
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
                info = salons_by_booksy.get(bid) or {}
                s["salon_name"] = info.get("name", "")
                s["salon_id"] = info.get("id")  # salons.id, NIE booksy_id (hx85)
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
