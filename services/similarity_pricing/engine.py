"""Silnik cenowy similarity-first — złożenie warstw wokół testu TOŻSAMOŚCI.

CEL: znaleźć usługi tożsame (ta sama usługa u innych salonów) mimo różnic w
nazwach i taksonomiach, a Z TOŻSAMEGO klastra policzyć cenę rynkową. Cena jest
WYNIKIEM, nie kryterium.

Pipeline:
  raw → DRUGIE PRAWO (adaptive_identity_filter) → A (dedup per salon)
      → B (wystarczalność) → D (cena)

Drugie prawo (warstwa tożsamości) zastąpiło surowy filtr kategorii: testuje
każdego bliźniaka na wielu osiach (parametry, pakiet, kategoria, czas) i sam
dobiera surowość per usługa tak, by klaster był maksymalnie TOŻSAMY przy
zachowaniu wystarczalności. Dopiero potem dedup/wystarczalność/cena działają na
zbiorze, o którym wiemy, że to ta sama usługa.

config = meta-pokrętła (jak surowo czyścić, czy bronić wystarczalności czy
czystości) — dostrajane empirycznie. Funkcja czysta, nie mutuje wejścia.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .layer_dedup import dedup_by_salon
from .layer_identity import adaptive_identity_filter
from .layer_sufficiency import assess_sufficiency
from .layer_unit import normalize_unit

DEFAULT_CONFIG: dict[str, Any] = {
    # --- dedup (warstwa A) ---
    "dedup_strategy": "closest",
    # --- wystarczalność (warstwa B) ---
    "min_salons_sufficient": 5,
    "min_salons_thin": 3,
    # --- drugie prawo / tożsamość (meta-pokrętła) ---
    # min salonów które adaptacja stara się utrzymać czyszcząc klaster
    "identity_min_salons": 3,
    # jak TOŻSAMY ma być klaster (udział bliźniaków bez sprzeczności na osiach)
    "identity_purity_target": 0.9,
    # przy konflikcie czystość vs wystarczalność:
    #   "sufficiency" — broń liczby salonów (cena z lekko wymieszanego > brak ceny)
    #   "purity"      — broń tożsamości (raczej "za mało danych" niż wymieszana cena)
    "identity_prefer": "sufficiency",
}


@dataclass
class MarketResult:
    """Wynik wyceny rynkowej z TOŻSAMEGO klastra.

    market_price_grosze == None gdy status == "insufficient" (za mało tożsamych
    salonów — uczciwe "tylko u Ciebie" zamiast ceny z garstki/wymieszanego).
    """
    market_price_grosze: int | None
    status: str                      # "sufficient" | "thin" | "insufficient"
    n_unique_salons: int             # tożsamych salonów po teście tożsamości+dedup
    deviation_pct: float | None
    p25_grosze: int | None
    p50_grosze: int | None
    p75_grosze: int | None
    zl_per_min_median: float | None
    median_raw_grosze: int | None
    # --- diagnostyka tożsamości (drugie prawo) ---
    identity_strictness: float       # surowość dobrana adaptacyjnie dla tej usługi
    identity_purity: float           # czystość tożsamościowa klastra po filtrze
    subject_generic: bool            # czy nazwa subjectu generyczna (waga kategorii)
    n_raw_samples: int               # ile bliźniaków weszło surowo (przed tożsamością)
    n_identity_kept: int             # ile przeszło test tożsamości
    n_used_for_price: int            # ile nie-pakietowych policzyło cenę
    samples: list[dict[str, Any]] = field(default_factory=list)  # tożsame po dedup (drill-down UI)
    provenance: dict[str, Any] = field(default_factory=dict)


def compute_market_price(
    subject: dict[str, Any],
    raw_samples: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
) -> MarketResult:
    """Policz cenę rynkową usługi subject z TOŻSAMEGO klastra bliźniaków.

    Args:
        subject: usługa właściciela (service_name, price_grosze, duration_minutes,
            category_name, is_package).
        raw_samples: surowy klaster z fn_find_related_v2 (każdy: service_id,
            booksy_id, salon_name, service_name, price_grosze, duration_minutes,
            similarity, category_name, is_package).
        config: nadpisania DEFAULT_CONFIG (meta-pokrętła). None => domyślne.

    Returns:
        MarketResult — cena rynkowa, status, odchylenie + diagnostyka tożsamości
        i pełny provenance każdej warstwy.

    Nie mutuje wejścia.
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    n_raw = len(raw_samples)

    # --- DRUGIE PRAWO: test tożsamości (dobiera surowość per usługa) ---
    # Odsiewa bliźniaki które NIE są tą samą usługą (różne parametry/pakiet/domena).
    s_identity, strictness, meta_id = adaptive_identity_filter(
        subject, raw_samples,
        min_salons=cfg["identity_min_salons"],
        purity_target=cfg["identity_purity_target"],
        prefer=cfg["identity_prefer"],
    )

    # --- Warstwa A: dedup per salon (na tożsamym zbiorze) ---
    s_dedup, meta_a = dedup_by_salon(s_identity, strategy=cfg["dedup_strategy"])

    # --- Warstwa B: wystarczalność (liczba tożsamych unikalnych salonów) ---
    status, meta_b = assess_sufficiency(
        s_dedup,
        min_salons_sufficient=cfg["min_salons_sufficient"],
        min_salons_thin=cfg["min_salons_thin"],
    )

    # --- Warstwa D: cena rynkowa (z tożsamego, bez pakietów, zł/min) ---
    market_stats, meta_d = normalize_unit(subject, s_dedup)

    if status == "insufficient":
        market_price = None
        deviation = None
    else:
        market_price = market_stats["market_price_grosze"]
        deviation = market_stats["deviation_pct"]

    final_id = meta_id["final"]
    return MarketResult(
        market_price_grosze=market_price,
        status=status,
        n_unique_salons=meta_b["n_unique_salons"],
        deviation_pct=deviation,
        p25_grosze=market_stats["p25_grosze"],
        p50_grosze=market_stats["p50_grosze"],
        p75_grosze=market_stats["p75_grosze"],
        zl_per_min_median=market_stats["zl_per_min_median"],
        median_raw_grosze=market_stats["median_raw_grosze"],
        identity_strictness=strictness,
        identity_purity=final_id["purity_kept"],
        subject_generic=final_id["subject_generic"],
        n_raw_samples=n_raw,
        n_identity_kept=len(s_identity),
        n_used_for_price=market_stats["n_used"],
        samples=s_dedup,
        provenance={
            "identity": meta_id,
            "dedup": meta_a,
            "sufficiency": meta_b,
            "unit": meta_d,
            "config": cfg,
        },
    )
