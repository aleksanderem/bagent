"""Werdykty b-match → wynik rynkowy (MarketResult) w tym samym kształcie co stary silnik, żeby _build_row,
deduplikacja wierszy, synteza i UI działały bez zmian.

Polityka (decyzje Alexa 2026-10-02, „wariant 3”):
- „ta sama” tylko przy p_ta_sama ≥ 0,9 → próbka do mediany (competitor_samples);
- pewna „odmiana” (≥ 0,8) → półka „usługi powiązane” (related_samples), powód z różnicy kart:
  inna metoda → „metoda” (UI: wymiar_metoda), reszta → „zakres” (UI: wymiar_zakres);
- „inna” i pary niepewne → poza raportem;
- strażnik ceny: cena różna ≥ 5× nigdy nie jest „ta sama” (degradacja do powiązanych, powód „zakres”).
Deduplikacja per salon, wystarczalność i cena = te same warstwy co stary silnik (layer_dedup/sufficiency/unit).
"""
from __future__ import annotations

from typing import Any

from services.similarity_pricing.engine import DEFAULT_CONFIG, MarketResult
from services.similarity_pricing.layer_dedup import dedup_by_salon
from services.similarity_pricing.layer_sufficiency import assess_sufficiency
from services.similarity_pricing.layer_unit import normalize_unit

PROG_TA_SAMA = 0.9
PROG_INNE = 0.8
PROG_CENY = 5.0


def decyzja(p: list[float], prog_ta_sama: float = PROG_TA_SAMA, prog_inne: float = PROG_INNE) -> str:
    """p = [p_inna, p_odmiana, p_ta_sama] → 'ta_sama' | 'odmiana' | 'inna' | 'niepewna'."""
    k = max(range(3), key=lambda i: p[i])
    if k == 2:
        return "ta_sama" if p[2] >= prog_ta_sama else "niepewna"
    if p[k] >= prog_inne:
        return "odmiana" if k == 1 else "inna"
    return "niepewna"


def powod_odmiany(sk_usluga: list[Any], sk_kandydat: list[Any]) -> str:
    metody_a = {tuple(x)[1] for x in sk_usluga if tuple(x)[1]}
    metody_b = {tuple(x)[1] for x in sk_kandydat if tuple(x)[1]}
    return "metoda" if metody_a and metody_b and metody_a != metody_b else "zakres"


def _straznik_ceny(cena_a: int | None, cena_b: int | None) -> bool:
    """True = ceny różnią się co najmniej PROG_CENY razy (para nie może być „ta sama”)."""
    if not cena_a or not cena_b or cena_a <= 0 or cena_b <= 0:
        return False
    return max(cena_a, cena_b) / min(cena_a, cena_b) >= PROG_CENY


def wynik_rynkowy(subject: dict[str, Any], oceny: list[tuple[dict[str, Any], str, str | None]],
                  config: dict[str, Any] | None = None, provenance: dict[str, Any] | None = None) -> MarketResult:
    """subject: jak w starym silniku (service_name, price_grosze, duration_minutes, category_name, is_package).
    oceny: [(próbka w kształcie bliźniaka starego silnika, decyzja, powód_odmiany)]."""
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    tozsame: list[dict[str, Any]] = []
    powiazane: list[dict[str, Any]] = []
    for probka, dec, powod in oceny:
        if dec == "ta_sama" and _straznik_ceny(subject.get("price_grosze"), probka.get("price_grosze")):
            dec, powod = "odmiana", "zakres"
        if dec == "ta_sama":
            tozsame.append(probka)
        elif dec == "odmiana":
            powiazane.append({**probka, "related_reason": powod or "zakres"})
    s_dedup, meta_a = dedup_by_salon(tozsame, strategy=cfg["dedup_strategy"])
    status, meta_b = assess_sufficiency(s_dedup, min_salons_sufficient=cfg["min_salons_sufficient"],
                                        min_salons_thin=cfg["min_salons_thin"])
    stat, meta_d = normalize_unit(subject, s_dedup)
    brak = status == "insufficient"
    powiazane.sort(key=lambda x: -(x.get("similarity") or 0))
    return MarketResult(
        market_price_grosze=None if brak else stat["market_price_grosze"],
        status=status,
        n_unique_salons=meta_b["n_unique_salons"],
        deviation_pct=None if brak else stat["deviation_pct"],
        p25_grosze=stat["p25_grosze"], p50_grosze=stat["p50_grosze"], p75_grosze=stat["p75_grosze"],
        zl_per_min_median=stat["zl_per_min_median"], median_raw_grosze=stat["median_raw_grosze"],
        identity_strictness=0.0, identity_purity=1.0, subject_generic=False,
        n_raw_samples=len(oceny), n_identity_kept=len(tozsame), n_coherence_dropped=0,
        n_used_for_price=stat["n_used"],
        samples=s_dedup,
        related_samples=powiazane[:20],
        provenance={"matching": "bmatch", "dedup": meta_a, "sufficiency": meta_b, "unit": meta_d,
                    **(provenance or {})},
    )
