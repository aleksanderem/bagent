"""Golden-testy korpusowe — ZAMROŻONE realne dane produkcyjne (2026-07-19).

CO: 8 subjectów z prod (pełne wejścia: twins + similarity + peer_max_sim,
fixtures/corpus_golden.json) z przypiętymi oczekiwanymi wynikami silnika.

PO CO: regresja na prawdziwych danych w każdym runie CI (pre-merge-fast
bramkuje services/similarity_pricing/). Dobór przypadków:
  * "Bikini linią"            — twardy mixing (52×"Bikini płytkie", −84.6% na
                                medianie przed fixem) → coherence wycina blok;
  * "Uzupelnienie rzęs 2:1"   — mixing wolumenu przy fallbacku 0.75
                                (13×"1:1") → osie volume+coherence;
  * "Przedłużanie rzęs 5/7:1" — masowe params-veto (48 dropów) BEZ coherence
                                (n_coherence_dropped=0) — oś volume solo;
  * "Fotoodmładzanie Twarz Plus Szyja" / "Henna brwi" — blok + dedup-swap
                                (kept NIE jest podzbiorem baseline — kaskada
                                wyjaśniona w weryfikacji 14 przypadków);
  * "Farbowanie (Farbka) Rzęs" — UDOKUMENTOWANY TRADE-OFF artefaktu opisu:
                                coherence ścina de-facto tożsame sample
                                (mediana bez zmian, status thin — bezpiecznie
                                konserwatywne). Jeśli ten test zacznie failować
                                po zmianie progów/embeddingów — sprawdź, czy
                                trade-off się poprawił, i ZAKTUALIZUJ oczekiwania
                                świadomie, nie odruchowo;
  * 2 przypadki CZYSTE (zero coherence-drop) — stabilność bit-w-bit silnika
                                na danych bez obcych bloków.

INWARIANT (uniwersalny, bez semantyki): w kept-klastrze ŻADEN sample nie może
mieć sygnatury obcego bloku (peer_max_sim − similarity > gap przy sim < s_max
w bloku ≥ min_block) — to samo sprawdza produkcyjnie SLO probe
slo-cluster-coherence (booksy-coherence-probe.sh).
"""
from __future__ import annotations

import json
import pathlib

import pytest

from .engine import DEFAULT_CONFIG, compute_market_price

_FIXTURES = json.loads(
    (pathlib.Path(__file__).parent / "fixtures" / "corpus_golden.json").read_text()
)


def _subject(rec):
    s = rec["subject"]
    return {"service_name": s["name"], "price_grosze": s["price_grosze"],
            "duration_minutes": s["duration_minutes"],
            "category_name": s["category_name"], "is_package": s["is_package"]}


@pytest.mark.parametrize("rec", _FIXTURES, ids=[r["case"] for r in _FIXTURES])
def test_corpus_golden_outputs(rec):
    """Wyniki silnika na zamrożonych realnych wejściach = przypięte oczekiwania."""
    res = compute_market_price(_subject(rec), rec["twins"], None)
    exp = rec["expected"]
    assert res.status == exp["status"]
    assert res.market_price_grosze == exp["median"]
    assert res.n_unique_salons == exp["n_unique_salons"]
    assert res.n_coherence_dropped == exp["n_coherence_dropped"]


@pytest.mark.parametrize("rec", _FIXTURES, ids=[r["case"] for r in _FIXTURES])
def test_corpus_invariant_no_foreign_block_survives(rec):
    """INWARIANT: po silniku w kept nie ma bloku obcych sampli (geometria)."""
    res = compute_market_price(_subject(rec), rec["twins"], None)
    gap = DEFAULT_CONFIG["coherence_gap"]
    s_max = DEFAULT_CONFIG["coherence_s_max"]
    suspects = [
        s for s in res.samples
        if s.get("peer_max_sim") is not None and s.get("similarity") is not None
        and float(s["peer_max_sim"]) - float(s["similarity"]) > gap
        and float(s["similarity"]) < s_max
    ]
    assert len(suspects) < DEFAULT_CONFIG["coherence_min_block"], (
        f"obcy blok przeżył: {[(s['service_name'], s['similarity'], s['peer_max_sim']) for s in suspects]}"
    )
