"""Mini raport rynkowy (S0082) — fraza usera → grupy TOŻSAMYCH usług z cenami.

Reużycie silnika similarity_pricing 1:1 zamiast gołego cosine (feedback usera:
"analiza rynku to śmieciowy odpad tego co robimy w audycie"):

  fraza → embed_texts (przestrzeń name_embedding, openai-3-small)
        → RPC fn_market_service_matches (mig 153: geo/city prefiltr + cosine,
          surowe wiersze)
        → peer_max_sim z Qdranta (to samo źródło co raport konkurencji)
        → grupowanie po ZNORMALIZOWANEJ nazwie (_normalize_text — filozofia
          weta nazwy; NIE variant_id, bo klasyfikacja to stary, odrzucony
          paradygmat — patrz similarity_pricing/README §1)
        → per grupa: compute_market_price z SYNTETYCZNYM subjectem
          (dominująca nazwa + MEDIANA ceny/czasu grupy) — dzięki medianie
          oś price (rząd wielkości zł/min) ucina zadatki/konsultacje
          nazwane identycznie jak zabieg, a osie params/package/body_area
          i coherence guard czyszczą resztę dokładnie jak w raporcie.

Grupy z 1 salonem nie dostają statystyk (jak subject_only w raporcie) —
lądują w "other" jako pojedyncze oferty.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from statistics import median
from typing import Any

from services.embeddings import embed_texts
from services.similarity_pricing.engine import compute_market_price
from services.similarity_pricing.qdrant_search import (
    compute_peer_max_sims,
    fetch_twin_vectors,
)

logger = logging.getLogger(__name__)

# Transliteracja polskich diakrytyków do klucza grupowania. CELOWO nie
# _normalize_text z layer_identity: NFKD nie rozkłada "ł/Ł" (brak combining
# stroke), więc ascii-ignore GUBI literę ("czoło"→"czoo" ≠ "czolo") i te same
# usługi pisane z/bez diakrytyków lądowałyby w osobnych grupach.
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

# Ile grup wariantowych zwracamy (posortowane po sile: n_salons, similarity).
MAX_GROUPS = 8
# Minimalna liczba RÓŻNYCH salonów, żeby grupa dostała własny wiersz z engine.
MIN_GROUP_SALONS = 2
# Ile sampli per grupa w odpowiedzi (drill-down w UI).
MAX_SAMPLES_PER_GROUP = 30

# Konfiguracja silnika dla snapshotu: identycznie jak raport poza progami
# wystarczalności — snapshot pokazuje też cienkie grupy (3-4 salony) jako
# pełnoprawne, bo user pyta o rynek, nie o rekomendację cenową dla siebie.
_SNAPSHOT_ENGINE_CONFIG: dict[str, Any] = {
    "min_salons_sufficient": 4,
    "min_salons_thin": 2,
}


def _sample_out(row: dict[str, Any]) -> dict[str, Any]:
    """Pola sample'a dla frontendu (bez embeddingów/metadanych silnika)."""
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
    """Najczęstsza niepusta wartość (nazwa/kategoria reprezentanta grupy)."""
    filtered = [v for v in values if v]
    if not filtered:
        return None
    return Counter(filtered).most_common(1)[0][0]


def _build_group_subject(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Syntetyczny subject grupy: dominująca nazwa + mediany ceny/czasu.

    Mediana ceny jako subject.price_grosze uzbraja oś price silnika
    (rozjazd rzędu wielkości vs mediana ⇒ against) — to ona wycina
    zadatki "Botoks 100 zł" z grupy, w której mediana to 1500 zł.
    """
    prices = [r["price_grosze"] for r in rows if r.get("price_grosze")]
    durations = [
        r["duration_minutes"] for r in rows if r.get("duration_minutes")
    ]
    return {
        "service_name": _dominant([r.get("service_name") for r in rows]) or "",
        "price_grosze": int(median(prices)) if prices else None,
        "duration_minutes": int(median(durations)) if durations else None,
        "category_name": _dominant([r.get("category_name") for r in rows]),
        "is_package": False,
    }


def build_market_snapshot(
    query: str,
    *,
    supabase_client: Any,
    lat: float | None = None,
    lng: float | None = None,
    radius_km: float = 15.0,
    city: str | None = None,
    min_similarity: float = 0.55,
) -> dict[str, Any]:
    """Zbuduj snapshot rynku dla frazy w obszarze. Synchroniczna (wołać przez
    asyncio.to_thread z endpointu)."""
    embedded = embed_texts([query.strip()])
    if embedded is None:
        raise RuntimeError("embedding providers unavailable")
    vectors, space = embedded
    if space != "openai-3-small":
        # mmlw fallback = inna przestrzeń niż name_embedding — cosine bez sensu.
        raise RuntimeError(f"embedding space mismatch: {space}")
    vector_str = "[" + ",".join(str(v) for v in vectors[0]) + "]"

    res = supabase_client.rpc(
        "fn_market_service_matches",
        {
            "p_query_embedding": vector_str,
            "p_lat": lat,
            "p_lng": lng,
            "p_radius_km": radius_km,
            "p_city": city,
            "p_min_similarity": min_similarity,
        },
    ).execute()
    rows: list[dict[str, Any]] = res.data or []

    if not rows:
        return {
            "query": query,
            "groups": [],
            "other_samples": [],
            "total_offers": 0,
            "total_salons": 0,
        }

    # peer_max_sim z Qdranta — pokarm coherence guard (abstain gdy brak).
    ids = [r["service_id"] for r in rows if r.get("service_id") is not None]
    try:
        peer_sims = compute_peer_max_sims(ids, fetch_twin_vectors(ids))
        for r in rows:
            pm = peer_sims.get(r.get("service_id"))
            if pm is not None:
                r["peer_max_sim"] = pm
    except Exception as e:  # noqa: BLE001 — guard abstains bez peer_max_sim
        logger.warning("market_snapshot: peer_max_sim unavailable (%s)", e)

    # Grupowanie po znormalizowanej nazwie (weto nazwy — deterministyczne).
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        grouped.setdefault(_group_key(r.get("service_name")), []).append(r)

    strong: list[dict[str, Any]] = []
    other_rows: list[dict[str, Any]] = []
    for key, members in grouped.items():
        if not key:
            other_rows.extend(members)
            continue
        n_salons = len({m.get("booksy_id") for m in members if m.get("booksy_id")})
        if n_salons < MIN_GROUP_SALONS:
            other_rows.extend(members)
            continue

        subject = _build_group_subject(members)
        result = compute_market_price(subject, members, _SNAPSHOT_ENGINE_CONFIG)
        if result.status == "insufficient" or result.market_price_grosze is None:
            # Silnik nie obronił tożsamej grupy — oferty spadają do "pozostałe".
            other_rows.extend(members)
            continue

        kept = result.samples[:MAX_SAMPLES_PER_GROUP]
        prices = [s["price_grosze"] for s in result.samples if s.get("price_grosze")]
        sims = [float(m.get("similarity") or 0.0) for m in members]
        strong.append(
            {
                "label": subject["service_name"],
                "status": result.status,  # "sufficient" | "thin"
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

    # Sortowanie: najmocniejsze grupy najpierw (salony, potem dopasowanie).
    strong.sort(key=lambda g: (-g["n_salons"], -g["avg_similarity"]))
    groups_out = strong[:MAX_GROUPS]

    # "Pozostałe" — pojedyncze oferty bez tożsamej grupy (dedup po service_id,
    # sort po similarity, cap żeby response nie puchł).
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
        "other_samples": other_out,
        "total_offers": len(rows),
        "total_salons": len({r.get("booksy_id") for r in rows if r.get("booksy_id")}),
    }
