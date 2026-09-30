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
    ("Uzupełnianie Rzęs UV 4-6d", "uzupełnianie rzęs uv 4 6d"),
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
