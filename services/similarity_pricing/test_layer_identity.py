"""Testy drugiego prawa — wielo-osiowy test TOŻSAMOŚCI usług.

Sedno: tożsame usługi (ta sama usługa, różne salony/nazwy/kategorie) zostają,
nie-tożsame (różny zabieg, parametry, pakiet, domena) wypadają. Kategoria waży
wg generyczności nazwy. Pusta kategoria nigdy nie wycina.
"""
from __future__ import annotations

from typing import Any

from .layer_identity import (
    adaptive_identity_filter,
    apply_identity_test,
    extract_params,
    identity_margin,
    identity_votes,
    is_identity_match,
    vote_category,
    vote_duration,
    vote_package,
    vote_params,
    vote_price,
)


# --------------------------------------------------------------------------
# Oś PRICE — rozrzut rzędu wielkości = inna usługa; NIGDY 'for'
# --------------------------------------------------------------------------

def test_price_order_of_magnitude_is_against():
    # laser nieablacyjny 600/30 vs frakcyjny 3667/30 => ~6× => against
    subj = _s("Laser resurfacing", dur=30, price=60000)
    twin = _s("Laser resurfacing", dur=30, price=366700, bid=2)
    assert vote_price(subj, twin) == "against"
    # ekstremalny rozjazd cenowy wycina nawet przy zgodnej kategorii/nazwie
    votes = identity_votes(subj, twin)
    assert is_identity_match(votes, strictness=0.5, category_weight=0.4) is False


def test_price_normal_variance_abstains():
    # botoks 900/30 vs 950/30 => ~1.05× => abstain (normalna wariancja)
    assert vote_price(_s("Botoks", dur=30, price=90000),
                      _s("Botoks", dur=30, price=95000, bid=2)) == "abstain"
    # 3× (poniżej progu 4×) też abstain — nie karzemy premium
    assert vote_price(_s("X", dur=30, price=30000),
                      _s("Y", dur=30, price=90000, bid=2)) == "abstain"


def test_price_normalizes_per_minute():
    # 600/60min (10 gr/min) vs 600/30min (20 gr/min) => 2× per-min => abstain
    assert vote_price(_s("X", dur=60, price=60000),
                      _s("Y", dur=30, price=60000, bid=2)) == "abstain"


def test_price_never_for():
    # identyczne ceny => abstain, nigdy 'for' (cena nie buduje tożsamości)
    assert vote_price(_s("X", dur=30, price=50000),
                      _s("Y", dur=30, price=50000, bid=2)) == "abstain"


def test_price_missing_abstains():
    assert vote_price(_s("X", dur=30, price=0), _s("Y", dur=30, price=50000, bid=2)) == "abstain"


# --------------------------------------------------------------------------
# Oś BODY_AREA — różny zakres obszarów = inna usługa (hard veto)
# --------------------------------------------------------------------------

def test_body_area_different_scope_is_against():
    from .layer_identity import vote_body_area
    # twarz vs twarz+szyja+dekolt => różny zakres => against
    subj = _s("Oczyszczanie wodorowe twarz", dur=60, price=28000)
    twin = _s("Oczyszczanie wodorowe twarz + szyja + dekolt", dur=90, price=140000, bid=2)
    assert vote_body_area(subj, twin) == "against"
    # hard veto: wycina niezależnie od surowości i ceny
    votes = identity_votes(subj, twin)
    assert is_identity_match(votes, strictness=0.0, category_weight=0.4) is False


def test_body_area_same_scope_abstains():
    from .layer_identity import vote_body_area
    # twarz vs twarzy => ten sam zakres => abstain (nie 'for')
    assert vote_body_area(_s("Oczyszczanie twarz"), _s("Oczyszczanie skóry twarzy", bid=2)) == "abstain"


def test_body_area_generic_abstains():
    from .layer_identity import vote_body_area
    # jeden bez obszaru => abstain (nie wiadomo)
    assert vote_body_area(_s("Oczyszczanie wodorowe twarz"), _s("Oczyszczanie wodorowe", bid=2)) == "abstain"
    assert vote_body_area(_s("Masaż"), _s("Masaż relaksacyjny", bid=2)) == "abstain"


def test_body_area_disjoint_is_against():
    from .layer_identity import vote_body_area
    # twarz vs plecy => rozłączne obszary => against
    assert vote_body_area(_s("Peeling twarz"), _s("Peeling pleców", bid=2)) == "against"


def _s(name, cat=None, dur=30, price=10000, is_package=False, bid=1, sid=1):
    return {
        "service_id": sid, "booksy_id": bid, "salon_name": f"S{bid}",
        "service_name": name, "price_grosze": price, "duration_minutes": dur,
        "category_name": cat, "is_package": is_package,
    }


# --------------------------------------------------------------------------
# Ekstrakcja parametrów
# --------------------------------------------------------------------------

def test_extract_params_ml():
    assert extract_params("Modelowanie ust 0,5 ml")["ml"] == 0.5
    assert extract_params("Wypełniacz 1ml")["ml"] == 1.0
    assert extract_params("Usta 2 ml")["ml"] == 2.0


def test_extract_params_count():
    assert extract_params("Przedłużanie 5 paznokci")["count"][0] == 5
    assert extract_params("Botoks 1 okolica")["count"][0] == 1


def test_extract_params_none():
    assert extract_params("Presoterapia") == {}
    assert extract_params("") == {}


# --------------------------------------------------------------------------
# SEDNO: presoterapia (specyficzna) tożsama mimo różnej kategorii,
#        konsultacja (generyczna) nie-tożsama przy różnej kategorii
# --------------------------------------------------------------------------

def test_specific_name_survives_different_category():
    # Presoterapia: nazwa specyficzna => kategoria waży mało (0.4). Różna kategoria
    # (niespójność właściciela) NIE wycina tożsamej usługi.
    subj = _s("Presoterapia drenaż limfatyczny", cat="Pielęgnacja ciała")
    twin = _s("Presoterapia drenaż", cat="Modelowanie sylwetki", bid=2)
    votes = identity_votes(subj, twin)
    assert votes["category"] == "against"  # token-rozłączne
    # ale margines płytki (waga 0.4) => keep przy umiarkowanej surowości
    assert is_identity_match(votes, strictness=0.5, category_weight=0.4) is True


def test_generic_name_dropped_on_different_category():
    # Konsultacja: nazwa generyczna => kategoria waży dużo (2.0). Rozłączna
    # kategoria (podologia vs medycyna estetyczna) = mocny dowód innej usługi.
    subj = _s("Konsultacja", cat="Medycyna estetyczna")
    twin = _s("Konsultacja", cat="Podologia", bid=2)
    votes = identity_votes(subj, twin)
    assert votes["category"] == "against"
    # waga generyczna 2.0 => margines -2.0 => drop nawet przy najniższej surowości
    assert is_identity_match(votes, strictness=0.0, category_weight=2.0) is False


def test_generic_name_kept_on_matching_category():
    subj = _s("Konsultacja", cat="Medycyna estetyczna")
    twin = _s("Konsultacja", cat="Medycyna estetyczna gabinet", bid=2)
    votes = identity_votes(subj, twin)
    assert votes["category"] == "for"
    assert is_identity_match(votes, strictness=0.5, category_weight=2.0) is True


# --------------------------------------------------------------------------
# Osie dyskryminujące: parametry, pakiet, czas
# --------------------------------------------------------------------------

def test_params_disagree_is_against():
    subj = _s("Modelowanie ust 0,5 ml")
    twin = _s("Modelowanie ust 2 ml", bid=2)
    assert vote_params(subj, twin) == "against"
    # parametry sprzeczne => inna usługa nawet przy luźnej surowości
    votes = identity_votes(subj, twin)
    assert is_identity_match(votes, strictness=0.0, category_weight=0.4) is False


def test_params_agree_is_for():
    subj = _s("Modelowanie ust 1 ml")
    twin = _s("Powiększanie ust 1ml kwasem", bid=2)
    assert vote_params(subj, twin) == "for"


def test_package_mismatch_is_against():
    subj = _s("Presoterapia", is_package=False)
    twin = _s("Presoterapia pakiet 5", is_package=True, bid=2)
    assert vote_package(subj, twin) == "against"


def test_package_same_is_abstain():
    assert vote_package(_s("X", is_package=False), _s("Y", is_package=False)) == "abstain"


def test_duration_extreme_is_against():
    assert vote_duration(_s("X", dur=15), _s("Y", dur=60, bid=2)) == "against"  # 4×
    assert vote_duration(_s("X", dur=40), _s("Y", dur=50, bid=2)) == "abstain"  # podobny


# --------------------------------------------------------------------------
# Pusta kategoria = abstain (KLUCZOWE: nie wycina tożsamych)
# --------------------------------------------------------------------------

def test_empty_category_abstains():
    assert vote_category(_s("Konsultacja", cat=None), _s("Konsultacja", cat=None, bid=2)) == "abstain"
    assert vote_category(_s("Konsultacja", cat="Medycyna"), _s("Konsultacja", cat="🌸🌸", bid=2)) == "abstain"
    # generyczna nazwa, ale puste kategorie => brak dowodu obcości => keep
    subj = _s("Konsultacja", cat=None)
    twin = _s("Konsultacja", cat=None, bid=2)
    votes = identity_votes(subj, twin)
    assert is_identity_match(votes, strictness=0.5, category_weight=2.0) is True


# --------------------------------------------------------------------------
# apply_identity_test — czystość + generyczność w meta
# --------------------------------------------------------------------------

def test_apply_filters_foreign_keeps_identical():
    subj = _s("Konsultacja", cat="Medycyna estetyczna")
    samples = [
        _s("Konsultacja", cat="Medycyna estetyczna", bid=2),       # tożsama
        _s("Konsultacja", cat="Medycyna estetyczna gabinet", bid=3),  # tożsama
        _s("Konsultacja", cat="Podologia", bid=4),                  # obca
        _s("Konsultacja", cat="Stylizacja rzęs", bid=5),            # obca
    ]
    kept, meta = apply_identity_test(subj, samples, strictness=0.5)
    kept_bids = {k["booksy_id"] for k in kept}
    assert 2 in kept_bids and 3 in kept_bids       # tożsame zostają
    assert 4 not in kept_bids and 5 not in kept_bids  # obce wypadają
    assert meta["subject_generic"] is True
    assert meta["category_weight"] == 2.0
    assert meta["purity_kept"] >= meta["purity_in"]  # czystość nie spada


def test_specific_subject_keeps_varied_categories():
    # presoterapia (specyficzna) z różnymi kategoriami właścicieli — wszystkie tożsame
    subj = _s("Presoterapia drenaż", cat="Drenaż limfatyczny")
    samples = [
        _s("Presoterapia drenaż", cat="Pielęgnacja ciała", bid=2),
        _s("Presoterapia", cat="Modelowanie sylwetki", bid=3),
        _s("Presoterapia drenaż", cat=None, bid=4),
        _s("Presoterapia", cat="Zabiegi na ciało", bid=5),
    ]
    kept, meta = apply_identity_test(subj, samples, strictness=0.5)
    assert meta["subject_generic"] is False
    assert meta["category_weight"] == 0.4
    assert len(kept) == 4  # wszystkie tożsame zachowane mimo różnych kategorii


def test_immutability():
    subj = _s("Konsultacja", cat="Medycyna")
    samples = [_s("Konsultacja", cat="Podologia", bid=2)]
    snap = [dict(s) for s in samples]
    apply_identity_test(subj, samples, 0.5)
    assert samples == snap


# --------------------------------------------------------------------------
# adaptive_identity_filter — drugie prawo (dobór surowości per usługa)
# --------------------------------------------------------------------------

def test_adaptive_picks_low_strictness_for_clean_specific():
    # specyficzna usługa, czysty klaster => niska surowość wystarcza
    subj = _s("Presoterapia drenaż", cat="Drenaż")
    samples = [_s("Presoterapia drenaż", cat="Pielęgnacja ciała", bid=b) for b in range(2, 8)]
    kept, strictness, meta = adaptive_identity_filter(subj, samples, min_salons=3)
    assert meta["final"]["n_unique_salons_kept"] >= 3
    assert strictness == 0.0  # nic nie trzeba czyścić — najniższa surowość


def test_adaptive_escalates_for_mixed_generic():
    # generyczna usługa, wymieszany klaster => eskaluje surowość by oczyścić
    subj = _s("Konsultacja", cat="Medycyna estetyczna")
    samples = [
        _s("Konsultacja", cat="Medycyna estetyczna", bid=b) for b in range(2, 7)
    ] + [
        _s("Konsultacja", cat="Podologia", bid=20),
        _s("Konsultacja", cat="Fryzjerstwo", bid=21),
        _s("Konsultacja", cat="Stylizacja rzęs", bid=22),
    ]
    kept, strictness, meta = adaptive_identity_filter(subj, samples, min_salons=3, purity_target=0.9)
    # obce kategorie odsiane, zostaje czysty rdzeń medycyny estetycznej
    kept_cats = {(k.get("category_name") or "").lower() for k in kept}
    assert not any("podolog" in c or "fryzjer" in c or "rzes" in c for c in kept_cats)
    assert meta["final"]["purity_kept"] >= 0.9


# --------------------------------------------------------------------------
# 2026-07-19 — uniwersalne parametry numeryczne (volume/weeks/length)
# Dowody z prod (sweep 250 subjectów): mixing wariantów przy fallbacku 0.75:
# "Uzupełnienie rzęs 2:1" ← 13×"1:1"; "Przedłużanie rzęs 1:1" ↔ "2:1";
# "Uzupełnienie Rzes 3:1 (Do4tyg)" ← "3:1 (do 3 tyg.)".
# --------------------------------------------------------------------------

def test_extract_params_volume_ratio_and_dim():
    assert extract_params("Przedłużanie rzęs 1:1")["volume"] == (1, 1)
    assert extract_params("Uzupełnienie rzęs 2:1")["volume"] == (2, 2)
    assert extract_params("Przedłużanie rzęs 3D")["volume"] == (3, 3)
    assert extract_params("Przedłużanie rzęs 3/4D")["volume"] == (3, 4)
    assert "volume" not in extract_params("Manicure hybrydowy")


def test_vote_params_volume_conflict_is_against():
    # RZECZYWISTY mixing ze sweepa: 1:1 vs 2:1 => twarde weto (oś strukturalna)
    a = _s("Przedłużanie rzęs 1:1"); b = _s("Przedłużanie rzęs 2:1", bid=2)
    assert vote_params(a, b) == "against"
    votes = identity_votes(a, b)
    assert is_identity_match(votes, strictness=0.0) is False  # weto niezależne od surowości


def test_vote_params_volume_ranges_overlap_is_for():
    # 3D vs 3/4D — zakresy się przecinają => zgodność, nie konflikt
    assert vote_params(_s("Rzęsy 3D"), _s("Rzęsy 3/4D", bid=2)) == "for"


def test_vote_params_ratio_vs_dim_same_dimension():
    # 2:1 i 2D to ta sama wartość wolumenu => for; 1:1 vs 3D => against
    assert vote_params(_s("Uzupełnienie 2:1"), _s("Uzupełnienie 2D", bid=2)) == "for"
    assert vote_params(_s("Przedłużanie 1:1"), _s("Przedłużanie 3D", bid=2)) == "against"


def test_vote_params_weeks_conflict_is_against():
    # refill "do 3 tyg" vs "do 4 tyg" = inna usługa cenowo (sweep: realny case)
    assert vote_params(_s("Uzupełnienie rzęs 3:1 do 4 tyg"),
                       _s("Uzupełnienie rzęs 3:1 (do 3 tyg.)", bid=2)) == "against"


def test_vote_params_weeks_equal_with_volume_for():
    assert vote_params(_s("Uzupełnienie 2:1 do 3 tyg"),
                       _s("Uzupełnienie 2:1 (3 tyg)", bid=2)) == "for"


def test_extract_params_length_range():
    assert extract_params("Przedłużanie paznokci długość 1-2")["length"] == (1, 2)
    # rozłączne długości => against
    assert vote_params(_s("Uzupełnienie żelowe długość 1-2"),
                       _s("Uzupełnienie żelowe długość 4", bid=2)) == "against"


def test_volume_does_not_fire_on_unrelated_numbers():
    # Liczby bez notacji x:y / nD nie tworzą wolumenu — brak false-veto na
    # nazwach typu "Strzyżenie dzieci do lat 5" (sweep: legalny klaster).
    assert "volume" not in extract_params("Strzyżenie dzieci do lat 5")
    assert vote_params(_s("Strzyżenie dzieci do lat 5"),
                       _s("Strzyżenie dzieci ( chłopcy ) do lat 5", bid=2)) == "abstain"
