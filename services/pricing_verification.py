"""Pricing comparison verification — re-weryfikacja podejrzanych deviation > 80%.

Po empirycznych obserwacjach raportu Beauty4ever (2026-05-15) variant clustering
łączy semantycznie podobne usługi nawet jeśli różnią się skalą (single vs
pakiet wielokrotny). Skutek: subject "Onda 4 zabiegi 1 obszar" matchuje się
do variantu "Zabieg ujędrniający na brzuch RF + lipo" (single zabieg) i daje
deviation +1300% — fałszywy sygnał.

Verification 2-stage:
  1. Package keyword detection — regex na nazwę subject service:
     - "pakiet", "pakietowo"
     - "Nx ", "N×", "×N" (3x, 4x, ×5)
     - "N zabiegów", "N sesji", "N wizyt"
     - "PRO" jako wzmacniacz (RedTouch PRO często pakiet)
  2. Embedding similarity check — subject_service.name_embedding vs
     variant.centroid_embedding cosine. Threshold 0.85 (po empiryce).
     Variant matching użył similarity 0.55+ podczas Phase 5 backfill, ale dla
     |deviation|>80% chcemy strict check że to FAKTYCZNIE ta sama usługa.

Reasons:
  - "package_mismatch" — subject nazwa zawiera package keyword
  - "low_name_similarity" — emb sim < 0.85
  - "duration_mismatch" — subject_duration > 2× variant median duration
  - "extreme_outlier" — passed checks ale deviation > 80% (keep + flag)
  - "verified" — passed all checks (default)
"""
from __future__ import annotations

import re
from typing import Any

import numpy as np

# Verification threshold — apply checks ALWAYS (0 = no gate). Embedding check
# is cheap (one cosine per row, embeddings already in memory from initial
# fetch), and false-positive variant matches happen at ALL deviation levels.
# Empiryka:
# - "Botoks" subject vs variant "Bruksizm" (deviation -18.52%) — różne zabiegi
#   (botoks na zmarszczki vs botoks na zgrzytanie zębów), embedding sim
#   prawdopodobnie <0.80. Z threshold 20% nie ran, ze threshold 0 — złapie.
# - Wcześniej tryb 80% gate przepuszczał WSZYSTKO poniżej 80% i to było źle.
VERIFICATION_THRESHOLD_PCT = 0.0

# Name similarity threshold — below = mismatch.
# 0.80 (obniżone z 0.85) żeby przepuścić borderline match'e jak
# "Dermapen 4 - 1 zabieg - twarz - PROMOCJA" → "Dermapen 4.0 - twarz + szyja"
# (sim 0.833), które są legit, ale odrzucić Plexr lifting → Masaż transbukalny
# (sim 0.767), które są false positive.
NAME_SIMILARITY_THRESHOLD = 0.80

# Duration ratio threshold — subject_duration / variant_duration > this = pakiet
DURATION_RATIO_THRESHOLD = 2.0


# Regex patterns for package detection
_PACKAGE_PATTERNS = [
    re.compile(r"\bpakiet\w*\b", re.IGNORECASE),       # pakiet, pakietowo, pakietów
    re.compile(r"\b\d+\s*[x×]\s*\b", re.IGNORECASE),    # 3x, 4 x, ×5
    # × is non-word so \b before it doesn't fire after whitespace; allow
    # either a word boundary OR whitespace/start so "Red Touch ×5" matches.
    re.compile(r"(?:\b|(?<=\s)|^)[x×]\s*\d+\b", re.IGNORECASE),  # x3, ×4
    re.compile(r"\b\d+\s*(zabieg\w*|sesj\w*|wizyt\w*)\b", re.IGNORECASE),  # 4 zabiegi, 5 sesji
    re.compile(r"\babonament\w*\b", re.IGNORECASE),     # abonament
    re.compile(r"\bkarnet\w*\b", re.IGNORECASE),        # karnet
    re.compile(r"\bseri\w+\s+(?:\d+|po)\b", re.IGNORECASE),  # seria po, seria 5
    # Serie liczbowe z gratisami / pakiety wieloseryjne (2026-06-22): "5 + 1",
    # "10 + 2" (N zabiegów + M gratis) oraz "5 +", "10 +" ("duża partia 5 +").
    # Depilacja laserowa serie (3900-8700 PLN) przeciekały do pricingu z
    # is_package=false bo żaden wzorzec nie łapał "+". Liczbowe "+" jest
    # jednoznaczne (pakiet), w przeciwieństwie do "pachy + bikini" (kombo
    # obszarów) które celowo NIE łapiemy tu — łapie je flag extreme-deviation.
    re.compile(r"\b\d+\s*\+\s*\d+\b"),                  # 5 + 1, 10 + 2
    re.compile(r"\b\d+\s*\+(?=\s|$)"),                  # 5 +, 10 + (na końcu/spacja)
]


def detect_package_keyword(name: str) -> str | None:
    """Zwraca matchowany fragment jeśli nazwa wygląda na pakiet/multi-pack.

    Returns: matched pattern string lub None.
    """
    if not name:
        return None
    for pattern in _PACKAGE_PATTERNS:
        m = pattern.search(name)
        if m:
            return m.group(0).strip()
    return None


def compute_name_embedding_similarity(
    subject_embedding: Any,
    variant_centroid: Any,
) -> float | None:
    """Cosine similarity między subject service name embedding a variant centroid.

    Both wejścia mogą być list[float], str (pgvector), or np.ndarray.
    Returns float 0..1 lub None gdy któryś nie da się sparsować.
    """
    def _parse(e):
        if e is None:
            return None
        if isinstance(e, np.ndarray):
            return e if e.shape == (1536,) else None
        if isinstance(e, str):
            try:
                stripped = e.strip("[]")
                if not stripped:
                    return None
                e = [float(x) for x in stripped.split(",")]
            except (ValueError, AttributeError):
                return None
        try:
            arr = np.array(e, dtype=np.float64)
        except (TypeError, ValueError):
            return None
        return arr if arr.shape == (1536,) else None

    a = _parse(subject_embedding)
    b = _parse(variant_centroid)
    if a is None or b is None:
        return None

    # L2-normalize defensywnie
    an = np.linalg.norm(a)
    bn = np.linalg.norm(b)
    if an == 0 or bn == 0:
        return None
    return float(np.dot(a, b) / (an * bn))


def verify_pricing_comparison(
    *,
    subject_service: dict[str, Any],
    variant_centroid_embedding: Any | None,
    variant_canonical_name: str | None,
    deviation_pct: float,
    market_median_duration: float | None = None,
) -> tuple[str, dict[str, Any]]:
    """Re-weryfikuj pricing comparison gdy |deviation| > VERIFICATION_THRESHOLD_PCT.

    Args:
      subject_service: dict z name, duration_minutes, name_embedding
      variant_centroid_embedding: centroid embedding variantu (z treatment_variants)
      variant_canonical_name: canonical_variant_name (do logowania)
      deviation_pct: liczona w _compute_pricing_comparisons
      market_median_duration: opcjonalna mediana duration_minutes konkurentów

    Returns:
      (verification_status, verification_details)

    verification_status one of:
      - "verified" (deviation ≤ threshold ALBO passed checks)
      - "package_mismatch"
      - "low_name_similarity"
      - "duration_mismatch"
      - "extreme_outlier" (passed checks, ale deviation > threshold)
    """
    details: dict[str, Any] = {}

    # Below threshold — skip verification
    if abs(deviation_pct) <= VERIFICATION_THRESHOLD_PCT:
        return "verified", details

    subject_name = subject_service.get("name", "") or ""
    details["deviation_pct"] = round(deviation_pct, 2)
    details["subject_name"] = subject_name[:120]
    if variant_canonical_name:
        details["variant_canonical_name"] = variant_canonical_name[:120]

    # 1. Package keyword check
    pkg_keyword = detect_package_keyword(subject_name)
    if pkg_keyword:
        details["package_keyword"] = pkg_keyword
        return "package_mismatch", details

    # 2. Embedding similarity check (subject service name vs variant centroid)
    if variant_centroid_embedding is not None:
        sim = compute_name_embedding_similarity(
            subject_service.get("name_embedding"),
            variant_centroid_embedding,
        )
        if sim is not None:
            details["name_similarity"] = round(sim, 4)
            if sim < NAME_SIMILARITY_THRESHOLD:
                return "low_name_similarity", details

    # 3. Duration mismatch check (opcjonalne — gdy mamy market_median_duration)
    subject_duration = subject_service.get("duration_minutes")
    if (
        subject_duration is not None
        and market_median_duration is not None
        and market_median_duration > 0
    ):
        ratio = subject_duration / market_median_duration
        details["subject_duration_ratio"] = round(ratio, 2)
        if ratio > DURATION_RATIO_THRESHOLD:
            return "duration_mismatch", details

    # Passed all checks but still extreme — flag (UI can still show with warning)
    return "extreme_outlier", details


def should_drop_from_display(verification_status: str) -> bool:
    """UI policy: które statuses dropować przed render'em.

    package_mismatch, low_name_similarity → DROP (strong false positive signal)
    duration_mismatch → DROP (też mocny mismatch)
    extreme_outlier → KEEP (ale UI flaguje badge'm)
    verified → KEEP
    """
    return verification_status in {"package_mismatch", "low_name_similarity", "duration_mismatch"}


# Deviation powyżej tego pułapu = "podlega weryfikacji" dla tierów które nie
# przechodzą pełnego per-variant verify (treatment/structured/method/sub_variant
# nie mają pojedynczego centroidu wariantu do embedding-checku, więc dotąd
# hard-codowały verification_status='verified'). 150% (2.5× ceny) łapie
# pakiet-vs-single mismatche jak "Depilacja Thunder pachy + bikini" (+703%),
# "Fotoodmładzanie twarz + szyja" (+1210%), zostawiając sensowne porównania
# (zwykle ±60%) jako verified. extreme_outlier jest KEEP (UI pokazuje z badge'm
# "wymaga weryfikacji"), nie DROP — dane zostają, tylko sygnalizujemy niepewność.
EXTREME_DEVIATION_PCT = 150.0

# Statusy "pokazanego porównania z danymi rynkowymi" które podlegają flagowaniu
# extreme-deviation. NIE obejmuje subject_only* (deviation=None, brak rynku) ani
# package_mismatch/low_name_similarity/duration_mismatch (już DROP) ani
# extreme_outlier (już oflagowane). method_targeted/structured_direct dotąd nie
# przechodziły per-variant verify i prezentowały skrajne deviation bez badge —
# tier i tak zostaje w comparison_tier, więc podniesienie statusu nic nie traci.
_FLAGGABLE_STATUSES = {"verified", "method_targeted", "structured_direct"}


def flag_extreme_deviation(
    verification_status: str, deviation_pct: float | None
) -> str:
    """Podnieś status pokazanego porównania → 'extreme_outlier' gdy |deviation|
    > EXTREME_DEVIATION_PCT.

    Dotyka wierszy w _FLAGGABLE_STATUSES (verified / method_targeted /
    structured_direct) — nie nadpisuje DROP-owanych mismatchy, subject_only ani
    już oflagowanych. Idempotentne. Tier-agnostyczne — wołane jednym passem na
    pricing_rows przed insertem, więc pokrywa treatment/structured/method/
    sub_variant bez dublowania logiki w każdym _emit_*.
    """
    if (
        verification_status in _FLAGGABLE_STATUSES
        and deviation_pct is not None
        and abs(deviation_pct) > EXTREME_DEVIATION_PCT
    ):
        return "extreme_outlier"
    return verification_status
