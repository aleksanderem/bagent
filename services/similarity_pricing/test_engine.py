"""Testy integracyjne silnika similarity-first (engine.compute_market_price).

Sprawdzają że 4 warstwy składają się poprawnie w pipeline na realistycznych
scenariuszach z prod (presoterapia, generyczna konsultacja, rzadka usługa,
pakiety, pokrętło wagi kategoria↔similarity).
"""
from __future__ import annotations

from typing import Any

from .engine import DEFAULT_CONFIG, MarketResult, compute_market_price


def _sample(
    service_id: int,
    booksy_id: int,
    price_grosze: int,
    duration_minutes: int | None,
    similarity: float = 0.86,
    category_name: str | None = "Drenaż limfatyczny",
    is_package: bool = False,
    service_name: str = "Presoterapia",
) -> dict[str, Any]:
    return {
        "service_id": service_id,
        "booksy_id": booksy_id,
        "salon_id": booksy_id,
        "salon_name": f"Salon {booksy_id}",
        "service_name": service_name,
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "similarity": similarity,
        "category_name": category_name,
        "is_package": is_package,
    }


# --------------------------------------------------------------------------
# Scenariusz 1: presoterapia — realny klaster, różny czas, pakiety
# --------------------------------------------------------------------------

def test_presoterapia_market_price_excludes_packages_and_normalizes():
    subject = {
        "service_id": 1, "service_name": "Presoterapia - drenaż limfatyczny",
        "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż limfatyczny", "is_package": False,
    }
    samples = [
        _sample(10, 100, 9900, 60),    # 165 gr/min
        _sample(11, 101, 6500, 30),    # ~217 gr/min
        _sample(12, 102, 9000, 75),    # 120 gr/min
        _sample(13, 103, 12000, 50),   # 240 gr/min
        _sample(14, 104, 6000, 45),    # ~133 gr/min
        _sample(15, 105, 70000, 40, is_package=True),   # pakiet — wykluczony
        _sample(16, 106, 130000, 30, is_package=True),  # pakiet — wykluczony
    ]
    res = compute_market_price(subject, samples)
    assert isinstance(res, MarketResult)
    assert res.status == "sufficient"          # 5 nie-pakietowych tożsamych salonów
    # pakiety (is_package=True vs subject False) wycięte przez DRUGIE PRAWO
    # (package hard-veto = inna usługa) — nie wchodzą już do dedup/wystarczalności
    assert res.n_unique_salons == 5
    assert res.n_used_for_price == 5
    assert res.market_price_grosze is not None
    # cena rynkowa znormalizowana per-minuta dla 40 min subjectu — w okolicy 5000-9000 gr
    assert 4000 <= res.market_price_grosze <= 10000
    assert res.deviation_pct is not None       # subject 150zł vs rynek — dodatnie odchylenie
    assert res.provenance["unit"]["used_per_minute"] is True


# --------------------------------------------------------------------------
# Scenariusz 2: generyczna "Konsultacja" — pokrętło w_category zawęża po kategorii
# --------------------------------------------------------------------------

def test_generic_konsultacja_identity_filters_foreign_categories():
    # Konsultacja (generyczna) — drugie prawo dobiera surowość i wycina obce
    # domeny (rzęsy, fryzjerstwo), zostawia tożsamy rdzeń (medycyna estetyczna).
    subject = {
        "service_id": 1, "service_name": "Konsultacja",
        "price_grosze": 1000, "duration_minutes": 15,
        "category_name": "Medycyna estetyczna", "is_package": False,
    }
    samples = [
        _sample(10, 100, 5000, 30, category_name="Medycyna estetyczna", service_name="Konsultacja"),
        _sample(11, 101, 4000, 20, category_name="Medycyna estetyczna", service_name="Konsultacja"),
        _sample(12, 102, 3000, 30, category_name="Medycyna estetyczna", service_name="Konsultacja"),
        _sample(13, 103, 8000, 30, category_name="Stylizacja rzęs", service_name="Konsultacja"),
        _sample(14, 104, 9000, 30, category_name="Fryzjerstwo", service_name="Konsultacja"),
    ]
    res = compute_market_price(subject, samples)
    assert res.subject_generic is True
    # obce kategorie wycięte przez test tożsamości; zostaje 3× medycyna estetyczna
    assert res.n_unique_salons == 3
    assert res.identity_purity >= 0.9


# --------------------------------------------------------------------------
# Scenariusz 3: rzadka usługa — insufficient => brak ceny rynkowej
# --------------------------------------------------------------------------

def test_rare_service_insufficient_no_price():
    subject = {
        "service_id": 1, "service_name": "Refleksologia limfatyczna",
        "price_grosze": 20000, "duration_minutes": 45,
        "category_name": "Refleksologia", "is_package": False,
    }
    samples = [
        _sample(10, 100, 18000, 45, category_name="Refleksologia", service_name="Refleksologia"),
    ]
    res = compute_market_price(subject, samples)
    assert res.status == "insufficient"
    assert res.market_price_grosze is None      # NIE pokazujemy ceny z 1 salonu
    assert res.deviation_pct is None
    assert res.n_unique_salons == 1


def test_thin_status_between_thresholds():
    subject = {
        "service_id": 1, "service_name": "Masaż dla Mamy",
        "price_grosze": 33000, "duration_minutes": 60,
        "category_name": "Masaż", "is_package": False,
    }
    samples = [
        _sample(10, 100, 30000, 60, category_name="Masaż", service_name="Masaż dla Mamy"),
        _sample(11, 101, 40000, 60, category_name="Masaż", service_name="Masaż dla Mamy"),
        _sample(12, 102, 41400, 60, category_name="Masaż", service_name="Masaż dla Mamy"),
    ]
    res = compute_market_price(subject, samples)
    assert res.status == "thin"                 # 3 salony: >=min_thin, <min_sufficient
    assert res.market_price_grosze is not None   # thin nadal pokazuje cenę (z ostrożnością)


# --------------------------------------------------------------------------
# Scenariusz 4: dedup per salon — jeden salon z wieloma wariantami
# --------------------------------------------------------------------------

def test_dedup_one_salon_many_variants():
    subject = {
        "service_id": 1, "service_name": "Przedłużanie paznokci",
        "price_grosze": 6000, "duration_minutes": 30,
        "category_name": "Paznokcie", "is_package": False,
    }
    # 1 salon (booksy 100) z 24 wariantami + 2 inne salony
    samples = [
        _sample(100 + i, 100, 15000 + i * 100, 30, category_name="Paznokcie",
                service_name=f"Przedłużanie {i} paznokci")
        for i in range(24)
    ] + [
        _sample(200, 101, 14000, 30, category_name="Paznokcie", service_name="Przedłużanie"),
        _sample(201, 102, 16000, 30, category_name="Paznokcie", service_name="Przedłużanie"),
    ]
    res = compute_market_price(subject, samples)
    assert res.n_raw_samples == 26
    assert res.n_unique_salons == 3             # 24 warianty 1 salonu → 1 reprezentant
    assert res.provenance["dedup"]["n_dropped"] == 23


# --------------------------------------------------------------------------
# Scenariusz 5: wszystkie pakiety => brak ceny
# --------------------------------------------------------------------------

def test_all_packages_no_price():
    subject = {
        "service_id": 1, "service_name": "Presoterapia",
        "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False,
    }
    samples = [
        _sample(10, 100, 70000, 40, is_package=True),
        _sample(11, 101, 80000, 40, is_package=True),
        _sample(12, 102, 65000, 40, is_package=True),
        _sample(13, 103, 90000, 40, is_package=True),
        _sample(14, 104, 75000, 40, is_package=True),
    ]
    res = compute_market_price(subject, samples)
    # 5 salonów => status sufficient, ale wszystkie pakiety => brak ceny
    assert res.n_used_for_price == 0
    assert res.market_price_grosze is None


# --------------------------------------------------------------------------
# Scenariusz 6: pusty klaster
# --------------------------------------------------------------------------

def test_empty_cluster():
    subject = {
        "service_id": 1, "service_name": "Coś unikalnego",
        "price_grosze": 10000, "duration_minutes": 30,
        "category_name": "Inne", "is_package": False,
    }
    res = compute_market_price(subject, [])
    assert res.status == "insufficient"
    assert res.market_price_grosze is None
    assert res.n_unique_salons == 0
    assert res.n_raw_samples == 0


# --------------------------------------------------------------------------
# Scenariusz 7: provenance + immutability
# --------------------------------------------------------------------------

def test_provenance_complete_and_input_not_mutated():
    subject = {
        "service_id": 1, "service_name": "Presoterapia",
        "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False,
    }
    samples = [_sample(10, 100, 9900, 60), _sample(11, 101, 6500, 30)]
    samples_snapshot = [dict(s) for s in samples]

    res = compute_market_price(subject, samples)
    # provenance ma meta wszystkich warstw + config
    # (coherence dołączyła 2026-07-19 — geometryczny strażnik obcych bloków)
    assert set(res.provenance.keys()) == {"identity", "coherence", "dedup", "sufficiency", "unit", "config"}
    assert res.provenance["config"]["identity_purity_target"] == DEFAULT_CONFIG["identity_purity_target"]
    # wejście nietknięte
    assert samples == samples_snapshot


def test_identity_strictness_recorded():
    subject = {
        "service_id": 1, "service_name": "Presoterapia",
        "price_grosze": 15000, "duration_minutes": 40,
        "category_name": "Drenaż", "is_package": False,
    }
    samples = [_sample(10 + i, 100 + i, 9000, 40) for i in range(5)]
    res = compute_market_price(subject, samples)
    # drugie prawo dobrało surowość adaptacyjnie — w prawidłowym zakresie
    assert isinstance(res.identity_strictness, float)
    assert 0.0 <= res.identity_strictness <= 1.0
    assert 0.0 <= res.identity_purity <= 1.0
