"""Katalog usług — podpis oferty (zbiór rdzeni słów) i porównanie dwóch podpisów (plan 29.09), bez sieci."""
from __future__ import annotations

from services.katalog_uslug.podpis import (INNA, PODOBNA, TA_SAMA, Klasy, klasy_roznicy, podpis, porownaj,
                                           roznica_do_pytania, zamiana_slow)


def rek(zabieg: str = "Strzyżenie", cechy: list[tuple[str, str]] | None = None, pozycja: str = "zabieg",
        nieprzypisane: list[str] | None = None, zrodlo: str = "nazwa", zrodlo_zabiegu: str = "nazwa") -> dict:
    return {"pozycja": pozycja, "zabieg": {"fraza": zabieg, "zrodlo": zrodlo_zabiegu},
            "cechy": [{"rola": r, "fraza": f, "zrodlo": zrodlo} for r, f in (cechy or [])],
            "nieprzypisane": nieprzypisane or []}


def werdykt(a: dict, b: dict, klasy: Klasy | None = None) -> str:
    return porownaj(podpis(a), podpis(b), klasy or Klasy())[0]


def test_ta_sama_mimo_innej_odmiany() -> None:
    assert werdykt(rek(cechy=[("obszar", "brody")]), rek(cechy=[("obszar", "Broda")])) == TA_SAMA


def test_przydzial_slow_do_rol_nie_ma_znaczenia() -> None:
    a = rek(zabieg="Depilacja", cechy=[("metoda", "laserowa"), ("obszar", "Całe ręce")])
    b = rek(zabieg="Depilacja laserowa", cechy=[("obszar", "Całe ręce")])
    assert werdykt(a, b) == TA_SAMA
    c = rek(zabieg="Lifting", cechy=[("obszar", "rzęs"), ("metoda", "z laminacją")])
    d = rek(zabieg="Lifting", cechy=[("obszar", "rzęs"), ("sklad", "z laminacją")])
    assert werdykt(c, d) == TA_SAMA


def test_inny_zabieg_to_inna() -> None:
    assert werdykt(rek(), rek(zabieg="Koloryzacja")) == INNA


def test_dopisek_po_jednej_stronie_bez_rozstrzygniecia_to_podobna() -> None:
    a = podpis(rek(zabieg="Uzupełnianie", cechy=[("obszar", "rzęs"), ("metoda", "UV"), ("rozmiar", "4-6d")]))
    b = podpis(rek(zabieg="Uzupełnianie", cechy=[("obszar", "rzęs"), ("rozmiar", "4-6D")]))
    assert porownaj(a, b, Klasy()) == (PODOBNA, "rdzen: „uv” tylko po jednej stronie")


def test_klasa_nieistotna_daje_ta_sama() -> None:
    a = podpis(rek(cechy=[("obszar", "brody"), ("rozmiar", "do 15 cm")]))
    b = podpis(rek(cechy=[("obszar", "brody")]))
    [(klasa, strona)] = klasy_roznicy(a, b)
    assert klasa[1:] == ("gdzie_ile", "15 cm do") and strona is a
    assert porownaj(a, b, Klasy(opisowe={klasa}))[0] == TA_SAMA


def test_dopisek_z_kilku_poziomow_wymaga_rozstrzygniecia_kazdego() -> None:
    a = podpis(rek(zabieg="Oczyszczanie", cechy=[("metoda", "manualne"), ("obszar", "twarzy"), ("metoda", "darsonval")]))
    b = podpis(rek(zabieg="Oczyszczanie", cechy=[("metoda", "manualne")]))
    kl = klasy_roznicy(a, b)
    assert [k[1] for k, _z in kl] == ["rdzen", "gdzie_ile"]
    tylko_twarz = Klasy(opisowe={k for k, _z in kl if k[1] == "gdzie_ile"})
    assert porownaj(a, b, tylko_twarz)[0] == PODOBNA  # „darsonval” nierozstrzygnięty
    assert porownaj(a, b, Klasy(opisowe={k for k, _z in kl}))[0] == TA_SAMA


def test_dodatek_w_nazwie_zawsze_podobna_bez_pytania() -> None:
    a = podpis(rek(zabieg="Tamponada", cechy=[("sklad", "z opatrunkiem")]))
    b = podpis(rek(zabieg="Tamponada"))
    kl = klasy_roznicy(a, b)
    assert porownaj(a, b, Klasy(opisowe={k for k, _z in kl}))[0] == PODOBNA
    assert roznica_do_pytania(a, b) == []


def test_rozne_slowa_po_obu_stronach_to_podobna_bez_pytania() -> None:
    a, b = podpis(rek(zabieg="Depilacja", cechy=[("obszar", "łydki")])), podpis(rek(zabieg="Depilacja", cechy=[("obszar", "uda")]))
    assert porownaj(a, b, Klasy())[0] == PODOBNA and roznica_do_pytania(a, b) == []


def test_nieprzypisane_slowo_wchodzi_do_zbioru() -> None:
    a = podpis(rek(cechy=[("obszar", "brody")], nieprzypisane=["UV"]))
    b = podpis(rek(cechy=[("obszar", "brody")]))
    assert "uv" in a.zbior
    assert porownaj(a, b, Klasy()) == (PODOBNA, "inne: „uv” tylko po jednej stronie")
    c = podpis(rek(cechy=[("obszar", "brody")], nieprzypisane=["UV"]))
    assert porownaj(a, c, Klasy())[0] == TA_SAMA


def test_produkt_nigdy_nie_rowna_sie_zabiegowi() -> None:
    assert werdykt(rek(zabieg="Maska", pozycja="produkt"), rek(zabieg="Maska")) == PODOBNA


def test_specjalista_i_szum_poza_podpisem() -> None:
    a = rek(cechy=[("obszar", "brody"), ("specjalista", "Paweł")])
    b = rek(cechy=[("obszar", "brody"), ("specjalista", "master barber")])
    assert werdykt(a, b) == TA_SAMA


def test_kontekst_tylko_gdy_nazwa_nie_mowi() -> None:
    def z(cechy: list[tuple[str, str, str]]) -> dict:
        return {"pozycja": "zabieg", "zabieg": {"fraza": "Depilacja", "zrodlo": "kategoria"},
                "cechy": [{"rola": r, "fraza": f, "zrodlo": zr} for r, f, zr in cechy]}
    meska = z([("obszar", "Szyja", "nazwa"), ("dla_kogo", "Mężczyzn", "kategoria"), ("metoda", "laserowa", "kategoria")])
    damska = z([("obszar", "Szyja", "nazwa"), ("metoda", "laserowa", "kategoria")])
    assert werdykt(meska, damska) == PODOBNA
    lomi = {"pozycja": "zabieg", "zabieg": {"fraza": "Lomi Lomi", "zrodlo": "nazwa"},
            "cechy": [{"rola": "metoda", "fraza": "Masaż", "zrodlo": "zabieg_booksy"}]}
    assert podpis(lomi).zbior == {"lomi"}  # nazwa podaje zabieg → kategoria nie dokłada


def test_sklad_z_opisu_poza_podpisem_a_wylaczenie_zostaje() -> None:
    def dekoloryzacja(z_opisu: tuple[str, str] | None = None) -> dict:
        r = rek(zabieg="Dekoloryzacja", cechy=[("rozmiar", "długie")])
        if z_opisu:
            r["cechy"].append({"rola": z_opisu[0], "fraza": z_opisu[1], "zrodlo": "opis"})
        return r
    assert werdykt(dekoloryzacja(("sklad", "strzyżeniem")), dekoloryzacja()) == TA_SAMA
    assert werdykt(dekoloryzacja(("wylaczenie", "bez strzyżenia")), dekoloryzacja()) == PODOBNA


def test_kontekst_kategorii_rozlozonej_raz() -> None:
    def kat(cechy: list[tuple[str, str]], zabieg: str = "") -> dict:
        return {"zabieg": {"fraza": zabieg, "zrodlo": "nazwa"}, "cechy": [{"rola": r, "fraza": f, "zrodlo": "nazwa"} for r, f in cechy]}
    kamienie = rek(zabieg="Masaż", cechy=[("metoda", "gorącymi kamieniami")])
    dla_dwojga = porownaj(podpis(kamienie, kontekst=kat([("dla_kogo", "DLA DWOJGA")], "SPA")), podpis(kamienie), Klasy())
    assert dla_dwojga[0] == PODOBNA  # „dla kogo” z kategorii, bo nazwa go nie podaje
    uda = {"pozycja": "zabieg", "zabieg": {"fraza": "Liposukcja ultradźwiękowa", "zrodlo": "zabieg_booksy"},
           "cechy": [{"rola": "obszar", "fraza": "Uda", "zrodlo": "nazwa"}, {"rola": "obszar", "fraza": "pośladki", "zrodlo": "nazwa"}]}
    lipo = rek(zabieg="Liposukcja", cechy=[("metoda", "ultradźwiękowa"), ("obszar", "Uda"), ("obszar", "pośladki")])
    fale = podpis(uda, kontekst=kat([], "Fale radiowe"))
    assert not any(w.startswith("lipos") for w in fale.zbior) and porownaj(fale, podpis(lipo), Klasy())[0] != TA_SAMA
    bez_rdzenia_w_kategorii = podpis(uda, kontekst=kat([("dla_kogo", "Kobiety")]))
    assert any(w.startswith("liposukc") for w in bez_rdzenia_w_kategorii.zbior)  # etykieta Booksy, gdy kategoria nie mówi
    mobilne = podpis(rek(zabieg="Makijaż", cechy=[("inne", "ślubny")]), kontekst=kat([("miejsce", "mobilne")], "Usługi"))
    assert porownaj(mobilne, podpis(rek(zabieg="Makijaż", cechy=[("inne", "ślubny")])), Klasy())[0] == PODOBNA


def _kat(nazwa: str, zabieg: str, cechy: list[tuple[str, str]]) -> dict:
    return {"nazwa": nazwa, "zabieg": {"fraza": zabieg, "zrodlo": "nazwa"},
            "cechy": [{"rola": r, "fraza": f, "zrodlo": "nazwa"} for r, f in cechy]}


def test_kategoria_doprecyzowuje_zabieg_ktory_nazwa_podaje_ogolniej() -> None:
    pachy = rek(zabieg="Depilacja", cechy=[("obszar", "pach")])
    laser = podpis(pachy, kontekst=_kat("Depilacja laserowa", "Depilacja laserowa", []))
    wosk = podpis(pachy, kontekst=_kat("Depilacja woskiem", "Depilacja", [("metoda", "woskiem")]))
    assert "laser" in laser.zbior and porownaj(laser, wosk, Klasy())[0] != TA_SAMA
    samo_laser = podpis(pachy, kontekst=_kat("LASER", "", [("metoda", "LASER")]))
    assert "laser" in samo_laser.zbior  # kategoria bez zabiegu, jedna metoda → dotyczy wszystkich jej ofert


def test_kategoria_lista_nie_mowi_ktory_wariant() -> None:
    lista = _kat("MANICURE / MANICURE HYBRYDOWY", "MANICURE / MANICURE HYBRYDOWY", [("metoda", "HYBRYDOWY")])
    assert podpis(rek(zabieg="Manicure"), kontekst=lista).zbior == {"manicur"}
    assert podpis(rek(zabieg="Manicure", cechy=[("metoda", "klasyczny")]), kontekst=lista).zbior == {"manicur", "klasyczn"}


def test_kategoria_uzupelnia_obszar_i_poziom_gdy_nazwa_ich_nie_ma() -> None:
    premium = podpis(rek(zabieg="", cechy=[("obszar", "Twarz")]),
                     kontekst=_kat("Oczyszczanie wodorowe PREMIUM", "Oczyszczanie wodorowe", [("poziom", "PREMIUM")]))
    zwykle = podpis(rek(zabieg="Oczyszczanie wodorowe"), kontekst=_kat("Zabiegi na twarz", "", [("obszar", "na twarz")]))
    assert "premium" in premium.zbior and "twarz" in zwykle.zbior and porownaj(premium, zwykle, Klasy())[0] != TA_SAMA
    wlasny = podpis(rek(cechy=[("obszar", "brody")]), kontekst=_kat("Włosy i broda", "", [("obszar", "Włosy i broda")]))
    assert wlasny.zbior == {"strzyzen", "brod"}  # nazwa podaje obszar → kategoria go nie dokłada
    inne = podpis(rek(zabieg="Masaż"), kontekst=_kat("SPA MASAŻ", "MASAŻ", [("inne", "SPA"), ("specjalista", "Ania")]))
    assert inne.zbior == {"masaz"}  # „inne” i wykonawca z kategorii poza podpisem
    kobido = podpis(rek(zabieg="Masaż KOBIDO"), kontekst=_kat("Masaż twarzy i głowy", "Masaż", [("obszar", "twarzy i głowy")]))
    assert kobido.zbior == {"masaz", "kobid"}  # obszar-lista nie mówi, którego obszaru dotyczy oferta


def test_nazwa_bez_tresci_nigdy_ta_sama() -> None:
    a = rek(zabieg="Combo", cechy=[("poziom", "Premium")])
    assert werdykt(a, a) == PODOBNA


def test_zamiana_slow_rozstrzygana_raz_na_klase() -> None:
    glowa = podpis(rek(cechy=[("obszar", "Głowy"), ("obszar", "Brody")]))
    wlosy = podpis(rek(cechy=[("obszar", "włosów"), ("obszar", "brody")]))
    z = zamiana_slow(glowa, wlosy)
    assert z == ("brod strzyzen", "glow", "wlos") and zamiana_slow(wlosy, glowa) == z  # klucz nie zależy od kolejności
    assert porownaj(glowa, wlosy, Klasy())[0] == PODOBNA  # bez rozstrzygnięcia — jak dotąd
    assert porownaj(glowa, wlosy, Klasy(rownowazne={z}))[0] == TA_SAMA


def test_zamiana_nie_dla_skladu_ani_dlugiej_roznicy() -> None:
    maska = podpis(rek(zabieg="Oczyszczanie", cechy=[("sklad", "z maską")]))
    ampulka = podpis(rek(zabieg="Oczyszczanie", cechy=[("sklad", "z ampułką")]))
    assert zamiana_slow(maska, ampulka) is None  # dodatek w nazwie = podobna, bez pytania
    dluga = podpis(rek(cechy=[("obszar", "włosów"), ("rozmiar", "bardzo długich"), ("dla_kogo", "damskie")]))
    assert zamiana_slow(podpis(rek(cechy=[("obszar", "głowy")])), dluga) is None  # 1 słowo / 3 słowa — inny zestaw cech
