"""Porównanie dwóch usług według schematu (services/typesafe_drzewo/schemat.py).

Kontrakt: różny rodzaj → różne; rodzaj ogólny vs szczegółowy → niepełne;
identyczna nazwa w tym samym rodzaju → ta sama; cecha podana tylko po jednej
stronie → niepełne, chyba że brak podania oznacza wartość domyślną rodzaju;
różna podana wartość, liczba zabiegów albo czas ≥ 2× → powiązane.
Czysty kod, bez modelu i sieci.
"""
from __future__ import annotations

from services.typesafe_drzewo.schemat import INNA, NIE_PODANO, NIEUSTALONE, liczby, porownaj

SCHEMAT = {
    "rodzaje": {
        "depilacja laserowa": {"cechy": {
            "obszar": {"wartosci": ["nogi", "pachy"], "domyslna": None},
            "preparat_urzadzenie": {"wartosci": ["aleksandryt", "dioda"], "domyslna": None},
        }},
        "przedłużanie rzęs": {"cechy": {
            "obszar": {"wartosci": ["rzęsy"], "domyslna": "rzęsy"},
            "objetosc": {"wartosci": ["1:1", "2-3d"], "domyslna": None},
        }},
        "masaż": {"cechy": {}},
        "masaż leczniczy": {"cechy": {}},
    },
    "rodzice": {"masaż leczniczy": "masaż"},
}


def u(rodzaj: str, cechy: dict | None = None, **liczby_) -> dict:
    return {"rodzaj": rodzaj, "cechy": {rodzaj: cechy or {}}, "liczba_zabiegow": 1, "ilosc": None, **liczby_}


def f(nazwa: str, czas: int | None = None) -> dict:
    return {"nazwa": nazwa, "duration_minutes": czas}


def test_rozny_rodzaj_to_rozne():
    assert porownaj(u("depilacja laserowa"), u("przedłużanie rzęs"), SCHEMAT)[0] == "rozne"


def test_rodzaj_ogolny_vs_szczegolowy_to_niepelne():
    assert porownaj(u("masaż"), u("masaż leczniczy"), SCHEMAT) == ("niepelne", "rodzaj ogólny")


def test_brak_destylacji_albo_rodzaj_spoza_schematu_to_niepelne():
    assert porownaj(None, u("masaż"), SCHEMAT)[0] == "niepelne"
    assert porownaj(u("inny"), u("masaż"), SCHEMAT)[0] == "niepelne"


def test_identyczna_nazwa_w_tym_samym_rodzaju_to_ta_sama():
    a = u("depilacja laserowa", {"obszar": "nogi"})
    b = u("depilacja laserowa", {"obszar": NIEUSTALONE})
    assert porownaj(a, b, SCHEMAT, f("Laser  NOGI"), f("laser nogi"))[0] == "tozsame"


def test_wszystkie_cechy_rowne_to_ta_sama():
    a = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": "aleksandryt"})
    b = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": "aleksandryt"})
    assert porownaj(a, b, SCHEMAT)[0] == "tozsame"


def test_obie_strony_bez_cechy_to_ta_sama():
    a = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": NIE_PODANO})
    b = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": NIE_PODANO})
    assert porownaj(a, b, SCHEMAT)[0] == "tozsame"


def test_cecha_tylko_po_jednej_stronie_to_niepelne():
    a = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": "aleksandryt"})
    b = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": NIE_PODANO})
    assert porownaj(a, b, SCHEMAT) == ("niepelne", "preparat_urzadzenie")


def test_nieustalone_liczy_sie_jak_brak_podania():
    a = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": NIEUSTALONE})
    b = u("depilacja laserowa", {"obszar": "nogi", "preparat_urzadzenie": NIE_PODANO})
    assert porownaj(a, b, SCHEMAT)[0] == "tozsame"


def test_wartosc_domyslna_uzupelnia_brak():
    a = u("przedłużanie rzęs", {"obszar": "rzęsy", "objetosc": "1:1"})
    b = u("przedłużanie rzęs", {"obszar": NIE_PODANO, "objetosc": "1:1"})
    assert porownaj(a, b, SCHEMAT)[0] == "tozsame"


def test_rozna_wartosc_to_powiazane_z_nazwa_cechy():
    a = u("przedłużanie rzęs", {"objetosc": "1:1"})
    b = u("przedłużanie rzęs", {"objetosc": "2-3d"})
    assert porownaj(a, b, SCHEMAT) == ("powiazane", "objetosc")


def test_inna_wartosc_to_niepelne():
    a = u("przedłużanie rzęs", {"objetosc": INNA})
    b = u("przedłużanie rzęs", {"objetosc": "1:1"})
    assert porownaj(a, b, SCHEMAT)[0] == "niepelne"


def test_liczba_zabiegow_i_czas_to_powiazane():
    assert porownaj(u("masaż"), u("masaż", liczba_zabiegow=10), SCHEMAT) == ("powiazane", "liczba_zabiegow")
    assert porownaj(u("masaż"), u("masaż"), SCHEMAT, f("Masaż", 30), f("Masaż pleców", 90)) == ("powiazane", "czas")


def test_liczby_z_nazwy():
    assert liczby("Endermologia LPG - 10 zabiegów na ciało", None)["liczba_zabiegow"] == 10
    assert liczby("Pakiet 5 x Endermologia", None)["liczba_zabiegow"] == 5
    assert liczby("Manicure hybrydowy", None)["liczba_zabiegow"] == 1
    assert liczby("Strzyżenie 2x w miesiącu", None)["liczba_zabiegow"] == 1
    assert liczby("Botoks 2 okolice", None)["ilosc"] == ["2 okoli"]
