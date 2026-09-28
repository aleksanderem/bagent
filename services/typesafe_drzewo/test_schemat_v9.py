"""Schemat v9: cechy dziedziczone po przodkach, cechy wspólne dla każdego rodzaju, małe rodzaje w rodzicu.

Przyczyna (pomiar 26.09, 12 salonów): „Depilacja pastą cukrową – szyja” i „– wąsik”
wychodziły jako ta sama usługa, bo klasyfikator wybierał rodzaj z 3 nazw
(„depilacja cukrem lub woskiem”), który w schemacie nie miał cechy „obszar”.
"""
from __future__ import annotations

from services.typesafe_drzewo.podzial import porownaj_v8
from services.typesafe_drzewo.schemat_v9 import (
    MAX_WARTOSCI, cechy_uniwersalne, dziedzicz_cechy, scal_male, schemat_v9,
)


def podzial_testowy() -> dict:
    return {
        "kanon": {"depilacja": "depilacja", "depilacja woskiem": "depilacja woskiem",
                  "depilacja cukrem lub woskiem": "depilacja cukrem lub woskiem", "masaż": "masaż"},
        "rodzic": {"depilacja woskiem": "depilacja", "depilacja cukrem lub woskiem": "depilacja"},
        "korzen": {"depilacja": "depilacja", "depilacja woskiem": "depilacja",
                   "depilacja cukrem lub woskiem": "depilacja", "masaż": "masaż"},
        "kanoniczne": ["depilacja", "depilacja cukrem lub woskiem", "depilacja woskiem", "masaż"],
        "zestawy": {},
        "przyklady": {"depilacja": ["Depilacja"], "depilacja cukrem lub woskiem": ["Depilacja cukrem"],
                      "depilacja woskiem": ["Depilacja woskiem nogi"], "masaż": ["Masaż klasyczny"]},
        "cechy": {
            "depilacja": {"obszar": {"wartosci": ["nogi", "pachy", "wąsik"], "domyslna": None}},
            "depilacja woskiem": {"obszar": {"wartosci": ["nogi", "bikini"], "domyslna": "nogi"},
                                  "technika": {"wartosci": ["wosk twardy", "wosk miękki"], "domyslna": None}},
            "depilacja cukrem lub woskiem": {"technika": {"wartosci": ["pasta cukrowa"], "domyslna": None}},
            "masaż": {"obszar": {"wartosci": ["plecy", "całe ciało"], "domyslna": None}},
        },
        "branze": {"Depilacja": ["depilacja"], "Masaż": ["masaż"]},
        "liczn": {"depilacja": 778, "depilacja woskiem": 2561, "depilacja cukrem lub woskiem": 3, "masaż": 900},
    }


def test_maly_rodzaj_z_rodzicem_przechodzi_do_rodzica():
    p = scal_male(podzial_testowy(), min_n=20)
    assert "depilacja cukrem lub woskiem" not in p["kanoniczne"]
    assert p["kanon"]["depilacja cukrem lub woskiem"] == "depilacja"
    assert p["liczn"]["depilacja"] == 781
    assert "Depilacja cukrem" in p["przyklady"]["depilacja"]
    assert "depilacja cukrem lub woskiem" not in p["rodzic"]


def test_maly_rodzaj_bez_rodzica_zostaje():
    wej = podzial_testowy()
    wej["liczn"]["masaż"] = 5
    assert "masaż" in scal_male(wej, min_n=20)["kanoniczne"]


def test_scalanie_nie_zmienia_wejscia():
    wej = podzial_testowy()
    scal_male(wej, min_n=20)
    assert "depilacja cukrem lub woskiem" in wej["kanoniczne"]


def test_odmiana_dziedziczy_ceche_przodka_bez_wartosci_domyslnej():
    wej = podzial_testowy()
    wej["cechy"]["depilacja woskiem"].pop("obszar")
    c = dziedzicz_cechy(wej)
    assert c["depilacja woskiem"]["obszar"]["wartosci"] == ["nogi", "pachy", "wąsik"]
    assert c["depilacja woskiem"]["obszar"]["domyslna"] is None


def test_wlasna_cecha_zostaje_a_wartosci_przodka_sie_dopisuja():
    c = dziedzicz_cechy(podzial_testowy())
    ob = c["depilacja woskiem"]["obszar"]
    assert ob["domyslna"] == "nogi"
    assert ob["wartosci"][:2] == ["nogi", "bikini"] and "wąsik" in ob["wartosci"]


def test_cecha_wspolna_z_wartosciami_calej_dziedziny():
    wej = podzial_testowy()
    c = cechy_uniwersalne(wej, dziedzicz_cechy(wej), uniwersalne=("technika",))
    # „depilacja” nie miała techniki — dostaje wartości techniki z całej dziedziny Depilacja
    assert set(c["depilacja"]["technika"]["wartosci"]) == {"wosk twardy", "wosk miękki", "pasta cukrowa"}
    # masaż w swojej dziedzinie nie ma żadnej techniki — nie dostaje pustej cechy
    assert "technika" not in c["masaż"]


def test_wartosci_wspolne_wg_liczby_nazw():
    wej = podzial_testowy()
    c = cechy_uniwersalne(wej, dziedzicz_cechy(wej), uniwersalne=("technika",))
    # wartości z rodzaju o 2561 nazwach przed wartością z rodzaju o 3 nazwach
    assert c["depilacja"]["technika"]["wartosci"][-1] == "pasta cukrowa"


def test_limit_wartosci_to_limit_api():
    wej = podzial_testowy()
    wej["cechy"]["depilacja woskiem"]["technika"]["wartosci"] = [f"t{i}" for i in range(400)]
    c = cechy_uniwersalne(wej, dziedzicz_cechy(wej), uniwersalne=("technika",))
    assert len(c["depilacja"]["technika"]["wartosci"]) == MAX_WARTOSCI


def u(rodzaj: str, cechy: dict) -> dict:
    return {"rodzaj": rodzaj, "cechy": {rodzaj: cechy}, "liczba_zabiegow": 1, "ilosc": None}


def test_depilacja_pasta_szyja_i_wasik_to_nie_ta_sama_usluga():
    p = schemat_v9(podzial_testowy())
    a = u("depilacja", {"obszar": "pachy", "technika": "pasta cukrowa"})
    b = u("depilacja", {"obszar": "wąsik", "technika": "pasta cukrowa"})
    assert porownaj_v8(a, b, p) == ("powiazane", "obszar")


def test_ta_sama_depilacja_zostaje_ta_sama():
    p = schemat_v9(podzial_testowy())
    a = u("depilacja", {"obszar": "nogi", "technika": "pasta cukrowa"})
    assert porownaj_v8(a, dict(a), p)[0] == "tozsame"


def test_wersja_schematu():
    assert schemat_v9(podzial_testowy())["wersja_schematu"] == 9
