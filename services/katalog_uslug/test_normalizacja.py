"""Katalog usług — normalizacja tekstu oferty do klucza (plan 29.09: proste czyszczenie, bez zmiany znaczenia)."""
from __future__ import annotations

import pytest

from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa


@pytest.mark.parametrize(("tekst", "klucz"), [
    ("  STRZYŻENIE   MĘSKIE ", "strzyżenie męskie"),
    ("Przedłużanie paznokci na formie 💫", "przedłużanie paznokci na formie"),
    ("Strzyżenie brody 🧔‍♂️", "strzyżenie brody"),
    ("Masaż Antycellulitowy 90 min", "masaż antycellulitowy"),
    ("Masaż stóp i nóg z refleksologią  60 min", "masaż stóp i nóg z refleksologią"),
    ("Masaż klasyczny 1h", "masaż klasyczny"),
    ("Masaż klasyczny 1,5 h", "masaż klasyczny"),
    ("Dekoloryzacja ,odrost,włosy długie", "dekoloryzacja odrost włosy długie"),
    ("Uzupełnianie Rzęs UV 4-6d", "uzupełnianie rzęs uv 4d 6d"),
])
def test_czysci_szum_bez_zmiany_znaczenia(tekst: str, klucz: str) -> None:
    assert normalizuj(tekst) == klucz


def test_plus_i_ukosnik_zostaja_bo_niosa_znaczenie() -> None:
    # „+” = komplet, „/” = alternatywa — różne usługi nie mogą się skleić po wycięciu znaku
    assert normalizuj("Pachy+łydki+bikini") == "pachy + łydki + bikini"
    assert normalizuj("Szyja/Laser") == "szyja / laser"
    assert normalizuj("Strzyżenie + broda") != normalizuj("Strzyżenie broda")


def test_liczby_i_proporcje_zostaja() -> None:
    assert normalizuj("Przedłużanie rzęs 2:1") == "przedłużanie rzęs 2:1"
    assert normalizuj("Przedłużanie rzęs 2:1") != normalizuj("Przedłużanie rzęs 1:1")
    assert normalizuj("Laser 3 obszary") == "laser 3 obszary"


def test_minuty_w_srodku_slowa_nie_sa_czasem() -> None:
    assert normalizuj("Minimasaż twarzy") == "minimasaż twarzy"
    assert normalizuj("Pakiet 5 minut relaksu") == "pakiet relaksu"


@pytest.mark.parametrize(("a", "b"), [("twarz", "twarzy"), ("pudrowa", "pudrową"), ("włosy", "włosów"),
                                      ("strzyżenie", "strzyżenia"), ("laserowa", "laserowy"), ("masaż", "masażu"),
                                      ("rzęs", "rzes"), ("włosów", "wlosow"), ("średnie", "średnich"),
                                      ("długie", "długich"), ("strzyżenie", "strzyzenie")])
def test_odmiana_tego_samego_slowa_daje_ten_sam_rdzen(a: str, b: str) -> None:
    assert rdzen_slowa(a) == rdzen_slowa(b)


@pytest.mark.parametrize(("a", "b"), [("łydki", "uda"), ("brwi", "rzęsy"), ("2:1", "1:1"), ("hybrydowy", "klasyczny")])
def test_rozne_slowa_zostaja_rozne(a: str, b: str) -> None:
    assert rdzen_slowa(a) != rdzen_slowa(b)


def test_pusty_tekst() -> None:
    assert normalizuj("") == ""
    assert normalizuj(None) == ""


def test_bez_kontaktow_maskuje_telefony_i_maile_a_nie_liczby_uslugi() -> None:
    from services.katalog_uslug.normalizacja import bez_kontaktow
    assert bez_kontaktow("Zapisy tel. 600 100 200 lub 700-100-200, +48 800 100 200, 500100200") == \
        "Zapisy tel. [telefon] lub [telefon], [telefon], [telefon]"
    assert bez_kontaktow("pisz: salon@example.com") == "pisz: [e-mail]"
    assert bez_kontaktow("Laser 755 808 1064nm, pakiet 3 zabiegi 1200 zł, 2:1") == "Laser 755 808 1064nm, pakiet 3 zabiegi 1200 zł, 2:1"


def test_ozdobne_czcionki_to_zwykle_litery() -> None:
    assert normalizuj("Bikini 𝑳𝒂𝒔𝒆𝒓") == "bikini laser"  # Unicode „math bold” z Booksy (test D)


def test_zakres_z_jednostka_na_koncu_dotyczy_obu_liczb() -> None:
    assert normalizuj("Uzupełnienie 2-3D") == normalizuj("Uzupełnienie 2D-3D") == "uzupełnienie 2d 3d"
    assert normalizuj("rzęsy 4/6D") == "rzęsy 4d 6d"
    assert normalizuj("długość 2,5cm") == "długość 2 5cm"  # przecinek to ułamek, nie zakres — bez zmian


def test_liczba_i_jednostka_miary_to_jedno_slowo() -> None:
    assert normalizuj("Lipoliza 20 ml") == normalizuj("Lipoliza 20ml") == "lipoliza 20ml"
    assert normalizuj("Tatuaż do 4 cm²") == normalizuj("Tatuaż do 4cm2") == "tatuaż do 4cm2"
    assert normalizuj("Masaż 1 x") == "masaż 1x"
    assert normalizuj("Pakiet 5 zabiegów") == "pakiet 5 zabiegów"  # słowo, nie jednostka — zostaje osobno
    assert normalizuj("Botoks 1,5 ml") == "botoks 1 5 ml"  # ułamek bez zmian
