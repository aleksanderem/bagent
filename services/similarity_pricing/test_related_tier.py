"""Warstwa "powiązane" (2026-08-28) — degradacja z mediany zamiast noża.

Progi zmierzone na zamrożonym holdoucie (182 pary, kalibracja człowiekiem):
pełny zestaw sygnałów wyprowadza 63% błędnych par poza medianę przy 17%
tożsamych ZDEGRADOWANYCH (widocznych dalej jako powiązane), nie straconych.
"""
from services.similarity_pricing.engine import compute_market_price
from services.similarity_pricing.layer_identity import related_demotion_reason


def _s(**kw):
    b = {"service_name": "Masaż klasyczny", "price_grosze": 15000, "duration_minutes": 60,
         "category_name": "Masaże", "is_package": False}
    b.update(kw); return b


def test_konflikt_metody_degraduje_nie_wetuje():
    a = _s(_tax={"metoda": "masaż klasyczny"})
    b = _s(_tax={"metoda": "drenaż limfatyczny"})
    assert related_demotion_reason(a, b) == "metoda"


def test_czas_2x_degraduje():
    assert related_demotion_reason(_s(duration_minutes=30), _s(duration_minutes=90)) == "czas_trwania"
    # ponizej progu — zostaje w medianie
    assert related_demotion_reason(_s(duration_minutes=60), _s(duration_minutes=90)) is None


def test_jednostronny_wymiar_wymaga_obu_destylacji():
    # jedna strona DEKLARUJE rozmiar, druga zdestylowana ale milczy -> degradacja
    a = _s(_tax={"metoda": "masaż", "rozmiar": "90 min"})
    b = _s(_tax={"metoda": "masaż"})
    assert related_demotion_reason(a, b) == "jednostronna_rozmiar"
    # druga strona BEZ taksonomii -> cisza nic nie znaczy, brak degradacji
    assert related_demotion_reason(a, _s()) is None


def test_engine_wyprowadza_powiazane_poza_mediane():
    subject = _s()
    twins = [
        # rdzeń mediany: 3 salony, ta sama usługa
        {"service_id": 1, "booksy_id": 101, "service_name": "Masaż klasyczny",
         "price_grosze": 14000, "duration_minutes": 60, "similarity": 0.9},
        {"service_id": 2, "booksy_id": 102, "service_name": "Masaż klasyczny",
         "price_grosze": 16000, "duration_minutes": 55, "similarity": 0.9},
        {"service_id": 3, "booksy_id": 103, "service_name": "Masaż klasyczny",
         "price_grosze": 15000, "duration_minutes": 60, "similarity": 0.9},
        # 30 min przy 60 podmiotu -> powiązana, nie w medianie
        {"service_id": 4, "booksy_id": 104, "service_name": "Masaż klasyczny 30 min",
         "price_grosze": 8000, "duration_minutes": 30, "similarity": 0.9},
    ]
    res = compute_market_price(subject, twins, None)
    assert res.n_unique_salons == 3
    assert res.market_price_grosze == 15000  # 8000 NIE zaniża mediany
    assert len(res.related_samples) == 1
    assert res.related_samples[0]["related_reason"] == "czas_trwania"
    assert res.related_samples[0]["price_grosze"] == 8000
