"""Testy COHERENCE GUARD (layer_coherence) — detekcja obcych bloków.

Filozofia testów: warstwa jest czystą geometrią, więc testy operują na
liczbach (similarity, peer_max_sim), a nazwy usług służą wyłącznie
czytelności. Wartości w teście "realny przypadek" są ZAMROŻONE z produkcji
(2026-07-19, subject 'Manicure hybrydowy' 130 zł, rzadki rynek pedicure-only;
raport #250 miał ten mixing w wierszach verified).
"""
from __future__ import annotations

from .engine import compute_market_price
from .layer_coherence import drop_foreign_blocks


def _s(name, sim, peer=None, bid=1, sid=None, price=10000, dur=60, cat=None):
    return {
        "service_id": sid if sid is not None else bid * 100,
        "booksy_id": bid, "salon_name": f"S{bid}", "service_name": name,
        "price_grosze": price, "duration_minutes": dur, "category_name": cat,
        "is_package": False, "similarity": sim,
        **({"peer_max_sim": peer} if peer is not None else {}),
    }


# --------------------------------------------------------------------------
# Rdzeń: sygnatura geometryczna obcego bloku
# --------------------------------------------------------------------------

def test_foreign_block_dropped():
    # Blok 3 sampli: wzajemnie ~identyczne (peer 0.99+), do subjectu 0.84
    # => gap ~0.15 > 0.08, sim < 0.90 => odrzucone.
    samples = [
        _s("Twin A", 0.95, peer=0.96, bid=1),
        _s("Twin B", 0.93, peer=0.96, bid=2),
        _s("Obcy 1", 0.84, peer=0.995, bid=3),
        _s("Obcy 2", 0.84, peer=0.995, bid=4),
        _s("Obcy 3", 0.841, peer=0.99, bid=5),
    ]
    kept, meta = drop_foreign_blocks(samples)
    assert [s["service_name"] for s in kept] == ["Twin A", "Twin B"]
    assert meta["n_dropped"] == 3
    assert meta["dropped_gap_range"][0] > 0.08


def test_true_twins_kept_gap_near_zero():
    # Prawdziwe bliźniaki: peer_max_sim ≈ similarity (gap ~0) => nietknięte.
    samples = [_s(f"T{i}", 0.93 + i * 0.01, peer=0.94 + i * 0.01, bid=i) for i in range(5)]
    kept, meta = drop_foreign_blocks(samples)
    assert len(kept) == 5 and meta["n_dropped"] == 0


def test_high_similarity_never_dropped_s_max_guard():
    # Artefakt opisu: sample z sim >= s_max (0.90) NIGDY nie odpada, nawet
    # przy dużym gap — to de-facto ta sama usługa (patrz docstring warstwy).
    samples = [
        _s("Subject-like", 0.91, peer=1.0, bid=1),
        _s("Subject-like 2", 0.90, peer=1.0, bid=2),
        _s("Twin", 0.95, peer=0.96, bid=3),
    ]
    kept, meta = drop_foreign_blocks(samples)
    assert len(kept) == 3 and meta["n_dropped"] == 0


def test_single_suspect_below_min_block_kept():
    # Pojedynczy podejrzany (< min_block=2) zostaje — sufficiency/dedup go
    # zneutralizują; nie tniemy na podstawie jednego przypadku.
    samples = [
        _s("Twin A", 0.95, peer=0.96, bid=1),
        _s("Twin B", 0.94, peer=0.96, bid=2),
        _s("Lone stranger", 0.83, peer=0.99, bid=3),
    ]
    kept, meta = drop_foreign_blocks(samples)
    assert len(kept) == 3 and meta["n_dropped"] == 0 and meta["n_suspect"] == 1


def test_missing_peer_max_sim_abstains():
    # Brak peer_max_sim (stary report_pricing / wektor zmieciony mig 149)
    # => abstain: warstwa przezroczysta, zero odrzuceń, licznik w meta.
    samples = [
        _s("A", 0.84, bid=1),  # bez peer
        _s("B", 0.84, bid=2),  # bez peer
        _s("C", 0.95, peer=0.96, bid=3),
    ]
    kept, meta = drop_foreign_blocks(samples)
    assert len(kept) == 3
    assert meta["n_abstained_no_peer_sim"] == 2


def test_input_not_mutated():
    samples = [_s("X", 0.84, peer=0.99, bid=1), _s("Y", 0.84, peer=0.99, bid=2)]
    snapshot = [dict(s) for s in samples]
    drop_foreign_blocks(samples)
    assert samples == snapshot


# --------------------------------------------------------------------------
# ZAMROŻONY PRZYPADEK PRODUKCYJNY (2026-07-19): pedicure w klastrze manicure
# --------------------------------------------------------------------------
# Wartości sim/peer z prod: pedicure↔pedicure ~0.99-1.0 (identyczne nazwy),
# pedicure↔subject(manicure) 0.841, manicure-literówki↔subject 0.854-0.956.
# Przed coherence engine trzymał 29 pedicure i liczył medianę z ich cen
# (106.74 zł vs subject 130 zł => fałszywe "+21.8% nad rynkiem").

def _prod_mani_cluster():
    subject = {"service_name": "Manicure hybrydowy", "price_grosze": 13000,
               "duration_minutes": 60, "category_name": "Manicure", "is_package": False}
    samples = [
        _s("Manicur hybrydowy", 0.956, peer=0.92, bid=101, price=17000, dur=60),
        _s("Maniciure hybrydowe", 0.924, peer=0.92, bid=102, price=11000, dur=60,
           cat="PEDICURE & MANICURE"),
        _s("Manicur hybrydowy french", 0.854, peer=0.92, bid=103, price=20000, dur=75),
    ] + [
        _s("Pedicure hybrydowy", 0.841, peer=0.998, bid=200 + i,
           price=[18000, 14000, 21000, 18900, 12500, 16900, 10000, 9000][i % 8], dur=75,
           cat=[None, "Podologia", "Pedicure", None][i % 4])
        for i in range(12)
    ]
    return subject, samples


def test_prod_case_manicure_pedicure_engine_end_to_end():
    """Regresja PRODUKCYJNA: klaster manicure z blokiem pedicure (raport #250).

    Oczekiwanie po coherence: WSZYSTKIE pedicure odrzucone, cena liczona
    wyłącznie z manicure; provenance dokumentuje odrzucenie (wyjaśnialność).
    """
    subject, samples = _prod_mani_cluster()
    res = compute_market_price(subject, samples, None)
    kept_names = {s["service_name"] for s in res.samples}
    assert not any("edicure" in n for n in kept_names), kept_names
    assert res.n_coherence_dropped == 12
    cm = res.provenance["coherence"]
    assert cm["n_dropped"] == 12 and "Pedicure hybrydowy" in cm["dropped_names"]
    # Zostają 3 manicure => thin (uczciwe "mało danych" zamiast ceny z pedicure).
    assert res.status in ("thin", "insufficient")


def test_prod_case_without_peer_sims_documents_old_behaviour():
    """Kontrola: BEZ peer_max_sim (stara ścieżka) blok pedicure ZOSTAJE.

    Ten test dokumentuje ZNANE złe zachowanie sprzed coherence — jeśli kiedyś
    zacznie failować, znaczy że inna warstwa zaczęła łapać ten przypadek
    (dobra wiadomość — wtedy zaktualizować komentarz i osłabić coherence?
    NIE usuwać bez zrozumienia)."""
    subject, samples = _prod_mani_cluster()
    stripped = [{k: v for k, v in s.items() if k != "peer_max_sim"} for s in samples]
    res = compute_market_price(subject, stripped, None)
    assert any("edicure" in s["service_name"] for s in res.samples)
    assert res.n_coherence_dropped == 0
