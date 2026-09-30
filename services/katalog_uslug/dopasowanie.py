"""Katalog usług — wycena oferty podmiotu z werdyktów podpisu (etap 3 planu 29.09).

Tożsamość rozstrzyga podpis (`podpis.porownaj`), nie warstwy tożsamości starego silnika (weta osi, degradacja,
strażnik spójności): próbki „ta sama” idą do mediany, „podobna” do wiersza „ceny podobnych usług”, „inna” odpada.
Dalej bez zmian względem produkcji: jeden reprezentant na salon, wystarczalność (≥ 3 salony), cena zł/min × czas
podmiotu bez pakietów — te same warstwy co `similarity_pricing.engine.compute_market_price`, więc wiersz raportu
(`report_pricing._build_row`) buduje się z wyniku bez zmian.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from services.katalog_uslug.podpis import INNA, PODOBNA, TA_SAMA, Klasy, Podpis, porownaj
from services.similarity_pricing.engine import DEFAULT_CONFIG, MarketResult
from services.similarity_pricing.layer_dedup import dedup_by_salon
from services.similarity_pricing.layer_sufficiency import assess_sufficiency
from services.similarity_pricing.layer_unit import normalize_unit

PODOBNYCH_W_WIERSZU = 20  # tyle co stary silnik (related_samples[:20])


def wycen(subject: dict[str, Any], kandydaci: list[tuple[dict[str, Any], str, str]],
          config: dict[str, Any] | None = None) -> MarketResult:
    """subject jak w `compute_market_price`; kandydaci = [(próbka w kształcie bliźniaka, werdykt, powód)].
    Nie mutuje wejścia."""
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    ta_sama = [dict(s) for s, w, _p in kandydaci if w == TA_SAMA]
    podobne = sorted(({**s, "related_reason": f"podobna: {p}"} for s, w, p in kandydaci if w == PODOBNA),
                     key=lambda x: -(x.get("similarity") or 0))
    s_dedup, meta_a = dedup_by_salon(ta_sama, strategy=cfg["dedup_strategy"])
    status, meta_b = assess_sufficiency(s_dedup, min_salons_sufficient=cfg["min_salons_sufficient"],
                                        min_salons_thin=cfg["min_salons_thin"])
    stats, meta_d = normalize_unit(subject, s_dedup)
    brak = status == "insufficient"
    return MarketResult(
        market_price_grosze=None if brak else stats["market_price_grosze"],
        status=status,
        n_unique_salons=meta_b["n_unique_salons"],
        deviation_pct=None if brak else stats["deviation_pct"],
        p25_grosze=stats["p25_grosze"],
        p50_grosze=stats["p50_grosze"],
        p75_grosze=stats["p75_grosze"],
        zl_per_min_median=stats["zl_per_min_median"],
        median_raw_grosze=stats["median_raw_grosze"],
        identity_strictness=1.0,
        identity_purity=1.0,
        subject_generic=False,
        n_raw_samples=len(kandydaci),
        n_identity_kept=len(ta_sama),
        n_coherence_dropped=0,
        n_used_for_price=stats["n_used"],
        samples=s_dedup,
        related_samples=podobne[:PODOBNYCH_W_WIERSZU],
        provenance={"identity": {"zrodlo": "podpis", "werdykty": dict(Counter(w for _s, w, _p in kandydaci))},
                    "dedup": meta_a, "sufficiency": meta_b, "unit": meta_d, "config": cfg},
    )


def _rdzen(p: Podpis) -> frozenset[str]:
    return frozenset(w for poz, w in p.poziomy if poz == "rdzen")


PROG_CENY = 5.0  # decyzja Alexa 30.09 (test D): ≥ 5× w 0,2% par prawdziwych „ta sama” i 23% błędnych (testy A–D)


def straznik_ceny(werdykt: str, powod: str, cena_a: float | None, cena_b: float | None) -> tuple[str, str]:
    """Para, w której jedna cena jest ≥ PROG_CENY razy wyższa, nie jest „tą samą usługą”, choć słowa się zgadzają:
    „Rekonstrukcja paznokcia” 150 zł u podologa i 15 zł przy manicure (różnica tylko w kontekście, test D).
    Bez obu cen — bez strażnika. Działa tylko na „ta sama”."""
    if werdykt != TA_SAMA or not cena_a or not cena_b or cena_a <= 0 or cena_b <= 0:
        return werdykt, powod
    iloraz = max(cena_a, cena_b) / min(cena_a, cena_b)
    if iloraz >= PROG_CENY:
        return PODOBNA, f"ceny różnią się {iloraz:.1f}× (≥ {PROG_CENY:g}×): {powod}"
    return werdykt, powod


def werdykty(ps: Podpis, kandydaci: Iterable[tuple[dict[str, Any], Podpis]], klasy: Klasy,
             cena_podmiotu_gr: float | None = None) -> list[tuple[dict[str, Any], str, str]]:
    """Werdykt podpisu dla każdej próbki. „Podobna” zostaje tylko przy tym samym rdzeniu (zabieg + metoda) co oferta
    podmiotu — inaczej wiersz „ceny podobnych usług” mieszałby różne zabiegi (reguła raportu testowego, 29.09).
    Przy cenie podmiotu działa strażnik ceny (straznik_ceny)."""
    rdzen = _rdzen(ps)
    wynik = []
    for s, pk in kandydaci:
        w0, p0 = porownaj(ps, pk, klasy)
        w, powod = straznik_ceny(w0, p0, cena_podmiotu_gr, s.get("price_grosze"))
        if w == PODOBNA and not (rdzen and rdzen == _rdzen(pk)):
            w, powod = INNA, f"inny rdzeń: {powod}"
        wynik.append((s, w, powod))
    return wynik
