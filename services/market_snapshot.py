"""Mini raport rynkowy (S0082) — fraza usera → grupy TOŻSAMYCH usług z cenami.

v3 (2026-07-21, audyt pokrycia): dwufazowo, bo name_embedding = "nazwa. opis"
i krótka fraza łapała cosine'em tylko ~18% tekstowych trafień (Botoks/Warszawa:
1174 z 6373 ofert), a prefiltr 3000 salonów ucinał 2/3 obszaru (8811 heads).

  FAZA 1 — ANCHORY: fraza → fn_market_service_matches (mig 153; cosine po
    3000 NAJBLIŻSZYCH salonów — wystarczy, by znaleźć typowe NAZWY usługi,
    nie musi znaleźć wszystkich egzemplarzy).
  FAZA 1b — KANAŁ KONCEPTOWY (v4, główny nośnik recallu): fraza →
    match_treatment_hybrid (mig 045: trigram+cosine po KRÓTKICH kanonicznych
    nazwach taksonomii Booksy — uczciwa przestrzeń dla krótkiej frazy) +
    dominujące booksy_treatment_id anchorów → fn_market_tid_services
    (mig 155): WSZYSTKIE usługi obszaru z tymi tid-ami (natywne tagowanie
    salonów w Booksy, ~96% pokrycia; ekspansja twinów przy 0.82 dokładała
    grosze, bo opisy rozjeżdżają nawet identyczne nazwy).
    Pula = anchory ∪ tid_rows → grupowanie po znormalizowanej nazwie
    (transliteracja PL — NFKD gubi "ł").
  FAZA 2 — EKSPANSJA: dla reprezentantów każdej grupy search_twins w Qdrancie
    po PEŁNYM obszarze (fn_market_area_booksy_ids, mig 154, INTEGER[] —
    wzorzec mig 147) w przestrzeni service→service (0.82 jak mig 144) —
    kanał uzupełniający dla nazw poza taksonomią.
  FAZA 3 — SILNIK: per grupa compute_market_price z syntetycznym subjectem
    (dominująca nazwa + MEDIANY ceny/czasu — oś price tnie zadatki), similarity
    członków PRZELICZONE jednolicie vs wektor reprezentanta (spójna skala dla
    coherence guard), peer_max_sim z Qdranta per grupa.

Przypisanie do grup: najpierw tożsamość nazwy (istniejący klucz grupy wygrywa),
potem najwyższy twin-score. Grupy z <2 salonami po ekspansji → "other".
"""

from __future__ import annotations

import logging
import math
import re
from collections import Counter
from statistics import median
from typing import Any

from services.embeddings import embed_texts
from services.similarity_pricing.engine import compute_market_price
from services.similarity_pricing.qdrant_search import (
    compute_peer_max_sims,
    fetch_twin_vectors,
    search_twins,
)

logger = logging.getLogger(__name__)

# Ile grup wariantowych zwracamy (posortowane po sile: n_salons, similarity).
MAX_GROUPS = 12
# Ile grup-kandydatów (anchorowych) rozszerzamy w Qdrancie.
MAX_CANDIDATE_GROUPS = 12
# Reprezentanci grupy używani jako subjecty ekspansji.
MAX_REPS_PER_GROUP = 3
# Max bliźniaków per reprezentant z Qdranta.
TWINS_LIMIT_PER_REP = 500
# Próg service→service ekspansji (jak fn_find_related_v2, mig 144).
TWIN_MIN_SIMILARITY = 0.82
# Próg fazy anchorowej (fraza→usługa; wyżej niż 0.55, bo anchor ma być pewny).
ANCHOR_MIN_SIMILARITY = 0.60
# Minimalna liczba RÓŻNYCH salonów PO ekspansji, żeby grupa dostała statystyki.
MIN_GROUP_SALONS = 2
# Ile sampli per grupa w odpowiedzi (drill-down w UI).
MAX_SAMPLES_PER_GROUP = 30
# Chunk dla PostgREST .in_() (enrichment).
_IN_CHUNK = 200
# Kanał tid: minimalny udział tid-a wśród anchorów, żeby wszedł do puli.
TID_MIN_ANCHOR_COUNT = 3
# Wektory z Qdranta (similarity vs rep + peer_max_sim dla coherence) tylko dla
# grup do tego rozmiaru i łącznie do tylu id — peer-simy to czysty Python
# O(n²·1536), a duże grupy z kanału tid są nazwa-identyczne (coherence robi
# uczciwy abstain bez peer_max_sim).
VECTORS_MAX_GROUP_SIZE = 150
VECTORS_MAX_TOTAL_IDS = 2000

# Konfiguracja silnika dla snapshotu: identycznie jak raport poza progami
# wystarczalności — snapshot pokazuje też cienkie grupy (2-3 salony).
_SNAPSHOT_ENGINE_CONFIG: dict[str, Any] = {
    "min_salons_sufficient": 4,
    "min_salons_thin": 2,
}

# Transliteracja polskich diakrytyków do klucza grupowania. CELOWO nie
# _normalize_text z layer_identity: NFKD nie rozkłada "ł/Ł" (brak combining
# stroke), więc ascii-ignore GUBI literę ("czoło"→"czoo" ≠ "czolo").
_PL_TRANSLIT = str.maketrans(
    "ąćęłńóśżźĄĆĘŁŃÓŚŻŹ",
    "acelnoszzACELNOSZZ",
)

_RE_ORDINAL_PREFIX = re.compile(r"^\s*[\dIVXivx]+\.\s*")
_RE_NON_WORD = re.compile(r"[^\w\s,.]", flags=re.UNICODE)
_RE_SPACES = re.compile(r"\s+")


def _group_key(text: str | None) -> str:
    """Klucz grupy: lowercase, transliteracja PL, bez emoji/prefiksów, 1 spacja."""
    if not text:
        return ""
    text = _RE_ORDINAL_PREFIX.sub("", text)
    text = _RE_NON_WORD.sub(" ", text)
    text = text.translate(_PL_TRANSLIT)
    return _RE_SPACES.sub(" ", text).strip().lower()


def _cosine(a: list[float] | None, b: list[float] | None) -> float | None:
    if not a or not b or len(a) != len(b):
        return None
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return None
    return dot / (na * nb)


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _sample_out(row: dict[str, Any]) -> dict[str, Any]:
    """Pola sample'a dla frontendu (bez wektorów/metadanych silnika)."""
    return {
        "service_id": row.get("service_id"),
        "salon_ref_id": row.get("salon_ref_id"),
        "salon_name": row.get("salon_name"),
        "city": row.get("city"),
        "lat": row.get("latitude"),
        "lng": row.get("longitude"),
        "distance_m": row.get("distance_m"),
        "service_name": row.get("service_name"),
        "price_grosze": row.get("price_grosze"),
        "is_from_price": bool(row.get("is_from_price", False)),
        "duration_minutes": row.get("duration_minutes"),
        "similarity": round(float(row.get("similarity") or 0.0), 3),
    }


def _dominant(values: list[Any]) -> Any:
    filtered = [v for v in values if v]
    if not filtered:
        return None
    return Counter(filtered).most_common(1)[0][0]


def _build_group_subject(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Syntetyczny subject grupy: dominująca nazwa + mediany ceny/czasu.

    Mediana ceny jako subject.price_grosze uzbraja oś price silnika
    (rozjazd rzędu wielkości vs mediana ⇒ against) — wycina zadatki
    nazwane identycznie jak zabieg.
    """
    prices = [r["price_grosze"] for r in rows if r.get("price_grosze")]
    durations = [r["duration_minutes"] for r in rows if r.get("duration_minutes")]
    return {
        "service_name": _dominant([r.get("service_name") for r in rows]) or "",
        "price_grosze": int(median(prices)) if prices else None,
        "duration_minutes": int(median(durations)) if durations else None,
        "category_name": _dominant([r.get("category_name") for r in rows]),
        "is_package": False,
    }


def _chunked(seq: list[Any], n: int) -> list[list[Any]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def _rpc_with_retry(supabase_client: Any, name: str, params: dict[str, Any]) -> Any:
    """RPC z jednym retry na statement timeout (PostgREST 57014, limit 8s).

    Zimny cache (TOAST/indeksy obszaru) potrafi przekroczyć limit przy
    pierwszym strzale; drugi idzie po rozgrzanym cache (pomiar prod: warm
    ~1-2s). Inne błędy propagują od razu.
    """
    try:
        return supabase_client.rpc(name, params).execute()
    except Exception as e:  # noqa: BLE001 — filtrujemy po kodzie niżej
        code = getattr(e, "code", None)
        if code is None and e.args and isinstance(e.args[0], dict):
            code = e.args[0].get("code")
        if code != "57014":
            raise
        logger.warning("market_snapshot: %s statement timeout — retry (warm cache)", name)
        return supabase_client.rpc(name, params).execute()


def _enrich_rows(
    rows: list[dict[str, Any]],
    supabase_client: Any,
    *,
    lat: float | None,
    lng: float | None,
) -> None:
    """Dociągnij metadane salonu (nazwa/miasto/geo) + is_from_price dla wierszy
    z Qdranta (payload ich nie niesie). Mutuje rows in-place. Best-effort."""
    need_salon = [r for r in rows if not r.get("salon_name")]
    booksy_ids = sorted({r["booksy_id"] for r in need_salon if r.get("booksy_id")})
    salon_meta: dict[int, dict[str, Any]] = {}
    for chunk in _chunked(booksy_ids, _IN_CHUNK):
        try:
            res = (
                supabase_client.table("salons")
                .select("id,booksy_id,name,city,latitude,longitude")
                .in_("booksy_id", chunk)
                .execute()
            )
            for s in res.data or []:
                salon_meta[s["booksy_id"]] = s
        except Exception as e:  # noqa: BLE001 — enrichment nie może ubić snapshotu
            logger.warning("market_snapshot: salon enrichment chunk failed (%s)", e)

    need_from_price = [r for r in rows if "is_from_price" not in r]
    sss_ids = sorted({r["service_id"] for r in need_from_price if r.get("service_id")})
    from_price: dict[int, bool] = {}
    for chunk in _chunked(sss_ids, _IN_CHUNK):
        try:
            res = (
                supabase_client.table("salon_scrape_services")
                .select("id,is_from_price")
                .in_("id", chunk)
                .execute()
            )
            for s in res.data or []:
                from_price[s["id"]] = bool(s.get("is_from_price", False))
        except Exception as e:  # noqa: BLE001
            logger.warning("market_snapshot: from_price enrichment chunk failed (%s)", e)

    for r in rows:
        meta = salon_meta.get(r.get("booksy_id"))
        if meta:
            r.setdefault("salon_ref_id", meta.get("id"))
            r["salon_name"] = r.get("salon_name") or meta.get("name")
            r.setdefault("city", meta.get("city"))
            r.setdefault("latitude", meta.get("latitude"))
            r.setdefault("longitude", meta.get("longitude"))
        if "is_from_price" not in r:
            r["is_from_price"] = from_price.get(r.get("service_id"), False)
        if (
            r.get("distance_m") is None
            and lat is not None
            and lng is not None
            and r.get("latitude") is not None
            and r.get("longitude") is not None
        ):
            r["distance_m"] = round(
                _haversine_m(lat, lng, r["latitude"], r["longitude"]), 0
            )


def build_market_snapshot(
    query: str,
    *,
    supabase_client: Any,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float = 15.0,
    city: str | None = None,
    min_similarity: float = ANCHOR_MIN_SIMILARITY,
) -> dict[str, Any]:
    """Zbuduj snapshot rynku dla frazy w obszarze. Synchroniczna (wołać przez
    asyncio.to_thread z endpointu)."""
    embedded = embed_texts([query.strip()])
    if embedded is None:
        raise RuntimeError("embedding providers unavailable")
    vectors, space = embedded
    if space != "openai-3-small":
        raise RuntimeError(f"embedding space mismatch: {space}")
    vector_str = "[" + ",".join(str(v) for v in vectors[0]) + "]"

    # --- FAZA 1: anchory (fraza→usługa, 3000 najbliższych salonów) ---
    res = _rpc_with_retry(
        supabase_client,
        "fn_market_service_matches",
        {
            "p_query_embedding": vector_str,
            "p_lat": lat,
            "p_lng": lng,
            "p_radius_km": radius_km,
            "p_city": city,
            "p_min_similarity": min_similarity,
        },
    )
    anchors: list[dict[str, Any]] = res.data or []

    empty = {
        "query": query,
        "groups": [],
        "other_samples": [],
        "total_offers": 0,
        "total_salons": 0,
    }

    # --- Pełny obszar (filtr ekspansji) ---
    area_ids: list[int] = []
    try:
        area_res = _rpc_with_retry(
            supabase_client,
            "fn_market_area_booksy_ids",
            {"p_lat": lat, "p_lng": lng, "p_radius_km": radius_km, "p_city": city},
        )
        area_ids = list(area_res.data or [])
    except Exception as e:  # noqa: BLE001 — bez obszaru ekspansja niemożliwa, anchory zostają
        logger.warning("market_snapshot: area ids unavailable (%s)", e)

    # --- FAZA 1b: kanał konceptowy (Booksy tid — główny nośnik recallu) ---
    tid_rows: list[dict[str, Any]] = []
    tids: set[int] = set()
    try:
        hyb = _rpc_with_retry(
            supabase_client,
            "match_treatment_hybrid",
            {"p_name": query.strip(), "p_embedding": vector_str},
        )
        for h in hyb.data or []:
            if h.get("inferred_tid") is not None:
                tids.add(int(h["inferred_tid"]))
    except Exception as e:  # noqa: BLE001 — kanał tid jest addytywny
        logger.warning("market_snapshot: match_treatment_hybrid failed (%s)", e)

    # Dominujące tid-y wśród anchorów (odporne na frazy wielo-konceptowe).
    anchor_ids = [int(r["service_id"]) for r in anchors if r.get("service_id")]
    try:
        tid_counts: Counter = Counter()
        for chunk in _chunked(anchor_ids, _IN_CHUNK):
            res_t = (
                supabase_client.table("salon_scrape_services")
                .select("id,booksy_treatment_id")
                .in_("id", chunk)
                .execute()
            )
            for s in res_t.data or []:
                if s.get("booksy_treatment_id") is not None:
                    tid_counts[int(s["booksy_treatment_id"])] += 1
        for tid, n in tid_counts.items():
            if n >= TID_MIN_ANCHOR_COUNT:
                tids.add(tid)
    except Exception as e:  # noqa: BLE001
        logger.warning("market_snapshot: anchor tid lookup failed (%s)", e)

    if tids:
        try:
            res_tid = _rpc_with_retry(
                supabase_client,
                "fn_market_tid_services",
                {
                    "p_tids": sorted(tids),
                    "p_lat": lat,
                    "p_lng": lng,
                    "p_radius_km": radius_km,
                    "p_city": city,
                },
            )
            tid_rows = list(res_tid.data or [])
        except Exception as e:  # noqa: BLE001
            logger.warning("market_snapshot: tid services unavailable (%s)", e)

    # --- Pula bazowa (anchory ∪ tid_rows) i grupowanie po nazwie ---
    base_pool: dict[int, dict[str, Any]] = {}
    for r in anchors:
        sid = r.get("service_id")
        if sid is not None:
            base_pool[int(sid)] = r
    for r in tid_rows:
        sid = r.get("service_id")
        if sid is not None:
            base_pool.setdefault(int(sid), r)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in base_pool.values():
        key = _group_key(r.get("service_name"))
        if key:
            grouped.setdefault(key, []).append(r)

    if not base_pool:
        return empty

    candidate_keys = sorted(
        grouped.keys(),
        key=lambda k: (
            -len({m.get("booksy_id") for m in grouped[k] if m.get("booksy_id")}),
            -max(float(m.get("similarity") or 0.0) for m in grouped[k]),
        ),
    )[:MAX_CANDIDATE_GROUPS]

    # --- FAZA 2: ekspansja bliźniaków po pełnym obszarze (Qdrant, 0.82) ---
    rep_to_key: dict[int, str] = {}
    rep_ids: list[int] = []
    for key in candidate_keys:
        members = sorted(
            grouped[key], key=lambda m: -float(m.get("similarity") or 0.0)
        )
        for m in members[:MAX_REPS_PER_GROUP]:
            sid = m.get("service_id")
            if sid is not None:
                rep_to_key[int(sid)] = key
                rep_ids.append(int(sid))

    twins_by_rep: dict[int, list[dict[str, Any]]] = {}
    if area_ids and rep_ids:
        try:
            twins_by_rep = search_twins(
                rep_ids,
                area_ids,
                limit=TWINS_LIMIT_PER_REP,
                min_similarity=TWIN_MIN_SIMILARITY,
            )
        except Exception as e:  # noqa: BLE001 — Qdrant down ⇒ zostają same anchory
            logger.warning("market_snapshot: twin expansion unavailable (%s)", e)

    # --- Sklejenie puli per grupa; przypisanie: nazwa > najlepszy twin-score ---
    # UWAGA (bugfix v4): pula obejmuje WSZYSTKIE grupy nazwowe (kanał tid
    # potrafi ich przynieść setki) — candidate_keys steruje wyłącznie tym,
    # które grupy dostają ekspansję twinów w Qdrancie.
    pool: dict[int, dict[str, Any]] = {}
    assignment: dict[int, tuple[str, float]] = {}

    for key, members in grouped.items():
        for m in members:
            sid = m.get("service_id")
            if sid is None:
                continue
            pool[int(sid)] = m
            # Tożsamość nazwy = twarde przypisanie.
            assignment[int(sid)] = (key, 2.0)

    for rep_id, twins in twins_by_rep.items():
        rep_key = rep_to_key.get(rep_id)
        if rep_key is None:
            continue
        for t in twins:
            sid = t.get("service_id")
            if sid is None or not t.get("price_grosze"):
                continue
            sid = int(sid)
            score = float(t.get("similarity") or 0.0)
            # Tożsamość nazwy wygrywa: bliźniak o nazwie istniejącej grupy
            # idzie do NIEJ, niezależnie od tego, który rep go znalazł.
            name_key = _group_key(t.get("service_name"))
            target_key = name_key if name_key in grouped else rep_key
            hard = 2.0 if name_key in grouped else score
            prev = assignment.get(sid)
            if prev is None or hard > prev[1]:
                assignment[sid] = (target_key, hard)
                pool.setdefault(sid, t)

    members_by_key: dict[str, list[dict[str, Any]]] = {}
    for sid, (key, _score) in assignment.items():
        row = pool.get(sid)
        if row is not None:
            members_by_key.setdefault(key, []).append(row)

    # --- Spójne similarity + peer_max_sim (wektory z Qdranta, z capem) ---
    vector_ids: list[int] = []
    for key, members in members_by_key.items():
        if len(members) > VECTORS_MAX_GROUP_SIZE:
            continue
        for m in members:
            sid = m.get("service_id")
            if sid is not None:
                vector_ids.append(int(sid))
        if len(vector_ids) >= VECTORS_MAX_TOTAL_IDS:
            break
    vectors_by_id: dict[int, list[float]] = {}
    if vector_ids:
        try:
            vectors_by_id = fetch_twin_vectors(vector_ids[:VECTORS_MAX_TOTAL_IDS])
        except Exception as e:  # noqa: BLE001 — silnik abstains bez peer_max_sim
            logger.warning("market_snapshot: vectors unavailable (%s)", e)

    for key, members in members_by_key.items():
        # Rep = anchor o najwyższym phrase-sim w grupie (pierwszy z rep_ids tej grupy).
        rep_id = next((r for r in rep_ids if rep_to_key.get(r) == key), None)
        rep_vec = vectors_by_id.get(rep_id) if rep_id is not None else None
        member_ids = [int(m["service_id"]) for m in members if m.get("service_id")]
        peer_sims = (
            compute_peer_max_sims(
                member_ids, {i: vectors_by_id[i] for i in member_ids if i in vectors_by_id}
            )
            if 2 <= len(member_ids) <= VECTORS_MAX_GROUP_SIZE
            else {}
        )
        for m in members:
            sid = int(m["service_id"])
            pm = peer_sims.get(sid)
            if pm is not None:
                m["peer_max_sim"] = pm
            # Jednolita skala similarity: cosine vs wektor reprezentanta
            # (service→service). Fallback: dotychczasowa wartość.
            if rep_vec is not None and sid in vectors_by_id:
                sim = _cosine(rep_vec, vectors_by_id[sid])
                if sim is not None:
                    m["similarity"] = round(sim, 4)

    # --- Enrichment metadanych (twins z Qdranta nie mają salon/geo/from_price) ---
    _enrich_rows(list(pool.values()), supabase_client, lat=lat, lng=lng)

    # --- FAZA 3: silnik per grupa ---
    strong: list[dict[str, Any]] = []
    other_rows: list[dict[str, Any]] = []
    for key, members in members_by_key.items():
        n_salons = len({m.get("booksy_id") for m in members if m.get("booksy_id")})
        if n_salons < MIN_GROUP_SALONS:
            other_rows.extend(members)
            continue

        subject = _build_group_subject(members)
        result = compute_market_price(subject, members, _SNAPSHOT_ENGINE_CONFIG)
        if result.status == "insufficient" or result.market_price_grosze is None:
            other_rows.extend(members)
            continue

        kept = result.samples[:MAX_SAMPLES_PER_GROUP]
        prices = [s["price_grosze"] for s in result.samples if s.get("price_grosze")]
        sims = [float(m.get("similarity") or 0.0) for m in members]
        strong.append(
            {
                "label": subject["service_name"],
                "status": result.status,
                "n_salons": result.n_unique_salons,
                "n_offers": len(members),
                "median_grosze": result.market_price_grosze,
                "p25_grosze": result.p25_grosze,
                "p75_grosze": result.p75_grosze,
                "min_grosze": min(prices) if prices else None,
                "max_grosze": max(prices) if prices else None,
                "avg_duration_minutes": subject["duration_minutes"],
                "avg_similarity": round(sum(sims) / len(sims), 3) if sims else 0.0,
                "n_dropped_by_engine": result.n_raw_samples - result.n_identity_kept
                + result.n_coherence_dropped,
                "samples": [_sample_out(s) for s in kept],
            }
        )

    strong.sort(key=lambda g: (-g["n_salons"], -g["avg_similarity"]))
    groups_out = strong[:MAX_GROUPS]
    n_groups_total = len(strong)

    seen_ids: set[Any] = set()
    other_unique: list[dict[str, Any]] = []
    for r in sorted(other_rows, key=lambda x: -(x.get("similarity") or 0.0)):
        sid = r.get("service_id")
        if sid in seen_ids:
            continue
        seen_ids.add(sid)
        other_unique.append(r)
    other_out = [_sample_out(r) for r in other_unique[:MAX_SAMPLES_PER_GROUP]]

    return {
        "query": query,
        "groups": groups_out,
        "n_groups_total": n_groups_total,
        "other_samples": other_out,
        "total_offers": len(pool),
        "total_salons": len(
            {r.get("booksy_id") for r in pool.values() if r.get("booksy_id")}
        ),
    }
