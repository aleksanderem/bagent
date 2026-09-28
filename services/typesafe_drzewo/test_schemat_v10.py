"""Schemat v10: poprawki porównania z rozbioru podologii i masażu (27–28.09, bd BEAUTY_AUDIT-asrk).

Kontrakt: pytania destylacji jak w v9; dodatek po jednej stronie od „raczej tak”;
liczba 1 przy sztukach, palcach, osobach = brak liczby; poziom zabiegu z nazwy
(słowa zakresu, nie marketingu) zdejmuje „ta sama”; reguła czasu to przełącznik;
„cel” zostaje. Zero sieci.
"""
from __future__ import annotations

from services.typesafe_drzewo.podzial import porownaj_v8
from services.typesafe_drzewo.schemat import NIE_PODANO
from services.typesafe_drzewo.schemat_v10 import (
    JEDNA_OSOBA, PODSTAWOWY, POZIOM, liczby_v10, porownaj_v10, poziom_z_nazwy, schemat_v10,
)


def podzial() -> dict:
    return {
        "kanon": {"masaż": "masaż", "zabieg podologiczny": "zabieg podologiczny"},
        "rodzic": {},
        "korzen": {"masaż": "masaż", "zabieg podologiczny": "zabieg podologiczny"},
        "kanoniczne": ["masaż", "zabieg podologiczny"],
        "przyklady": {"masaż": ["Masaż klasyczny"], "zabieg podologiczny": ["Opracowanie paznokci"]},
        "cechy": {
            "masaż": {"obszar": {"wartosci": ["plecy", "całe ciało"], "domyslna": None},
                      "cel": {"wartosci": ["relaksacyjny", "antycellulitowy"], "domyslna": None},
                      "liczba_osob": {"wartosci": ["para", "4 ręce"], "domyslna": None}},
            "zabieg podologiczny": {"obszar": {"wartosci": ["paznokcie", "stopy"], "domyslna": None}},
        },
        "branze": {"Masaż": ["masaż"], "Paznokcie": ["zabieg podologiczny"]},
        "liczn": {"masaż": 900, "zabieg podologiczny": 160},
    }


def u(rodzaj: str, cechy: dict, **dod) -> dict:
    return {"rodzaj": rodzaj, "cechy": {rodzaj: cechy}, "liczba_zabiegow": 1, "ilosc": None,
            "zestaw": 0.05, "rozszerzenie": 0.05, **dod}


def f(nazwa: str, czas: int | None = None) -> dict:
    return {"nazwa": nazwa, "duration_minutes": czas}


# ---------- schemat ----------
def test_wersja_schematu():
    assert schemat_v10(podzial())["wersja_schematu"] == 10


def test_schemat_nie_zmienia_wejscia():
    wej = podzial()
    schemat_v10(wej)
    assert wej["cechy"]["masaż"]["liczba_osob"]["domyslna"] is None


def test_cel_zostaje():
    assert "cel" in schemat_v10(podzial())["cechy"]["masaż"]


def test_liczba_osob_domyslnie_jedna():
    lo = schemat_v10(podzial())["cechy"]["masaż"]["liczba_osob"]
    assert lo["domyslna"] == JEDNA_OSOBA and lo["wartosci"][0] == JEDNA_OSOBA and "para" in lo["wartosci"]


# ---------- liczby i poziom z nazwy ----------
def test_liczby_sztuk_z_nazwy():
    assert liczby_v10("Podcięcie wrastającego paznokcia 2 palce", None)["ilosc"] == ["2 palec"]
    assert liczby_v10("Opracowanie paznokci zmienionych chorobowo 1szt", None)["ilosc"] is None
    assert liczby_v10("Masaż leczniczy 1 os/60 min", None)["ilosc"] is None
    assert liczby_v10("Masaż dla 2 osób", None)["ilosc"] == ["2 osoba"]
    assert liczby_v10("Botoks 1 okolica", None)["ilosc"] == ["1 okoli"]  # jednostki miary bez zmian


def test_poziom_z_nazwy():
    assert poziom_z_nazwy("Zaawansowany Pedicure Podologiczny") == "rozszerzony"
    assert poziom_z_nazwy("Zabieg podologiczny – rozszerzony") == "rozszerzony"
    assert poziom_z_nazwy("Manicure express") == "skrócony"
    assert poziom_z_nazwy("Pedicure podstawowy") == PODSTAWOWY
    assert poziom_z_nazwy("Pedicure podologiczny") == PODSTAWOWY
    # marketing to nie zakres
    assert poziom_z_nazwy("PAKIET PREMIUM - Bikini głębokie + pachy") == PODSTAWOWY
    assert poziom_z_nazwy("Komplet: manicure i pedicure") == PODSTAWOWY


# ---------- porównanie ----------
def test_regula_czasu_to_przelacznik():
    p = schemat_v10(podzial())
    a, b = u("masaż", {"obszar": "całe ciało"}), u("masaż", {"obszar": "całe ciało"})
    fa, fb = f("Masaż gorącymi kamieniami", 60), f("Masaż gorącymi kamieniami 2 h", 120)
    assert porownaj_v10(a, b, p, fa, fb)[0] == "powiazane"
    assert porownaj_v10(a, b, p, fa, fb, regula_czasu=False) == ("tozsame", "wszystkie cechy równe")


def test_cel_nadal_rozroznia():
    p = schemat_v10(podzial())
    a, b = u("masaż", {"obszar": "całe ciało", "cel": "antycellulitowy"}), u("masaż", {"obszar": "całe ciało", "cel": NIE_PODANO})
    assert porownaj_v10(a, b, p, f("Masaż antycellulitowy"), f("Masaż całego ciała"))[0] != "tozsame"


def test_poziom_zdejmuje_ta_sama():
    p = schemat_v10(podzial())
    a, b = u("zabieg podologiczny", {"obszar": "paznokcie"}), u("zabieg podologiczny", {"obszar": "paznokcie"})
    assert porownaj_v10(a, b, p, f("Zabieg podologiczny"), f("Zabieg podologiczny zaawansowany")) == ("powiazane", POZIOM)
    assert porownaj_v10(a, b, p, f("Zabieg podologiczny"), f("Zabieg podologiczny podstawowy"))[0] == "tozsame"


def test_poziom_nie_zmienia_innych_rodzajow_w_powiazane():
    p = schemat_v10(podzial())
    a, b = u("masaż", {"obszar": "plecy"}), u("zabieg podologiczny", {"obszar": "paznokcie"})
    assert porownaj_v10(a, b, p, f("Masaż pleców"), f("Zabieg podologiczny zaawansowany"))[0] == "rozne"


def test_dodatek_od_raczej_tak():
    p = schemat_v10(podzial())
    a = u("zabieg podologiczny", {"obszar": "paznokcie"}, rozszerzenie=0.72)
    b = u("zabieg podologiczny", {"obszar": "paznokcie"}, rozszerzenie=0.05)
    fa, fb = f("Podcięcie wrastającego paznokcia z opatrunkiem"), f("Podcięcie wrastającego paznokcia")
    assert porownaj_v8(a, b, p, fa, fb)[0] == "tozsame"  # v8: próg 0,8
    assert porownaj_v10(a, b, p, fa, fb) == ("powiazane", "dodatek")


def test_niepewny_dodatek_po_obu_stronach_nie_rozstrzyga():
    p = schemat_v10(podzial())
    a = u("masaż", {"obszar": "plecy"}, rozszerzenie=0.6)
    b = u("masaż", {"obszar": "plecy"}, rozszerzenie=0.4)
    assert porownaj_v10(a, b, p, f("Masaż pleców"), f("Masaż pleców i karku"))[0] == "tozsame"


def test_identyczna_nazwa_wygrywa_z_szumem_dodatku():
    p = schemat_v10(podzial())
    a = u("masaż", {"obszar": "plecy"}, rozszerzenie=0.6)
    b = u("masaż", {"obszar": "plecy"}, rozszerzenie=0.1)
    assert porownaj_v10(a, b, p, f("Masaż pleców"), f("masaż  pleców")) == ("tozsame", "identyczna nazwa")


def test_jedna_sztuka_to_to_samo_co_brak_liczby():
    p = schemat_v10(podzial())
    a, b = u("zabieg podologiczny", {"obszar": "paznokcie"}), u("zabieg podologiczny", {"obszar": "paznokcie"})
    assert porownaj_v10(a, b, p, f("Opracowanie paznokcia wrastającego 1 szt"), f("Opracowanie paznokcia wrastającego"))[0] == "tozsame"


def test_dwa_palce_to_nie_ta_sama_usluga_co_jeden():
    p = schemat_v10(podzial())
    a, b = u("zabieg podologiczny", {"obszar": "paznokcie"}), u("zabieg podologiczny", {"obszar": "paznokcie"})
    assert porownaj_v10(a, b, p, f("Tamponada 2 palce"), f("Tamponada 1 palec"))[0] != "tozsame"


def test_liczba_osob_brak_to_jedna():
    p = schemat_v10(podzial())
    a = u("masaż", {"obszar": "plecy", "liczba_osob": NIE_PODANO})
    b = u("masaż", {"obszar": "plecy", "liczba_osob": JEDNA_OSOBA})
    c = u("masaż", {"obszar": "plecy", "liczba_osob": "para"})
    assert porownaj_v10(a, b, p, f("Masaż pleców"), f("Masaż pleców dla jednej osoby"))[0] == "tozsame"
    assert porownaj_v10(a, c, p, f("Masaż pleców"), f("Masaż pleców dla par")) == ("powiazane", "liczba_osob")
