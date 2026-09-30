"""Katalog usług, etap 3 — wycena usług podmiotu podpisem (ścieżka raportu za przełącznikiem MATCHING_SOURCE).

Czysta funkcja: dostaje podpisy ofert (rozbiór GLM + kontekst kategorii i salonu + słownik rynku) i rozstrzygnięte
klasy różnic; skąd je wziąć (pliki pomiarów czy tabela pamięci na produkcji), decyduje wołający. W wycenie zero
wywołań modelu. Rodzaj wiersza (decyzja Alexa 30.09): ≥ 3 salony „ta sama” → mediana rynku; 1–2 salony → ich ceny
bez mediany; tylko podobne → „ceny podobnych usług”; nic → brak porównania.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from services.katalog_uslug.dopasowanie import werdykty, wycen
from services.katalog_uslug.ekstrakcja import POLA, Oferta
from services.katalog_uslug.normalizacja import bez_kontaktow, normalizuj
from services.katalog_uslug.podpis import PODOBNA, TA_SAMA, Klasy, Podpis
from services.similarity_pricing.engine import MarketResult

TA_SAMA_RYNEK, TA_SAMA_MALO, TYLKO_PODOBNE, BRAK = "ta_sama", "ta_sama_1_2", "podobne", "brak"


def klucz_oferty(o: Oferta) -> str:
    """Klucz pamięci rozbioru: dokładnie pola, które widzi model (ekstrakcja.prompt — typ salonu i POLA), bez
    kontaktów — ten sam tekst u dwóch salonów to jeden rozbiór, a maskowanie telefonu nie zmienia klucza."""
    tekst = " | ".join(normalizuj(bez_kontaktow(getattr(o, p) or "")) for p in ("typ_salonu", *POLA))
    return "o:" + hashlib.sha1(tekst.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class WierszPodpisu:
    oferta: str
    rodzaj: str
    wynik: MarketResult    # z próbek „ta sama”; cena None przy < 3 salonach
    podobne: MarketResult  # z próbek „podobna” — wiersz „ceny podobnych usług”
    powody: dict[str, int]


def _rodzaj(r: MarketResult, r_pod: MarketResult) -> str:
    if r.market_price_grosze is not None:
        return TA_SAMA_RYNEK
    if r.n_unique_salons:
        return TA_SAMA_MALO
    return TYLKO_PODOBNE if r_pod.market_price_grosze is not None else BRAK


def wycen_oferty(podmiot: Mapping[str, tuple[dict[str, Any], Podpis]],
                 kandydaci: Mapping[str, Iterable[tuple[dict[str, Any], Podpis]]], klasy: Klasy,
                 config: dict[str, Any] | None = None) -> dict[str, WierszPodpisu]:
    """podmiot: oferta → (usługa podmiotu jak `subject` w compute_market_price, podpis); kandydaci: oferta podmiotu →
    [(próbka w kształcie bliźniaka, podpis)]. Oferta bez kandydatów = wiersz „brak”. Nie mutuje wejścia."""
    wynik: dict[str, WierszPodpisu] = {}
    for oid, (subject, ps) in podmiot.items():
        w = werdykty(ps, kandydaci.get(oid, ()), klasy)
        r = wycen(subject, w, config)
        r_pod = wycen(subject, [(s, TA_SAMA, p) for s, v, p in w if v == PODOBNA], config)
        powody: dict[str, int] = {}
        for _s, v, _p in w:
            powody[v] = powody.get(v, 0) + 1
        wynik[oid] = WierszPodpisu(oid, _rodzaj(r, r_pod), r, r_pod, powody)
    return wynik


__all__ = ["BRAK", "TA_SAMA_MALO", "TA_SAMA_RYNEK", "TYLKO_PODOBNE", "WierszPodpisu", "klucz_oferty", "wycen_oferty"]
