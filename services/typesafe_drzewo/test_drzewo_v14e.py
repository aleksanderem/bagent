"""Zamrożone v14e (bilans z v14f) — kontrakt porównania: drzewo do poziomu zabiegu, niżej frazy obu usług wprost (zgoda Alexa 28.09).

Odpowiedzi TypeSafe (czy frazy znaczą to samo, czy usługa ma cechę / obejmuje wariant) dostarcza tu słownik —
w pomiarze rozstrzyga je TypeSafe. Zero sieci; żadnych reguł na nazwach (kod porównuje tylko identyczne frazy).
"""
from __future__ import annotations

from services.typesafe_drzewo.drzewo_v14e import (
    CECHA,
    WARIANT,
    ZMIANA,
    klucz_relacji,
    porownaj_v14,
    potrzebne_domniemania,
    potrzebne_potwierdzenia,
    potrzebne_relacje,
    potwierdz,
    profil_v14,
)

DRZEWO = {"zabiegi": {
    "paznokci|pedicure": {"etykieta": "pedicure", "rodzice": ["paznokci", "stóp"], "synonimy": ["pedicure"]},
    "ciała|depilacja": {"etykieta": "depilacja", "rodzice": ["ciała"], "synonimy": ["depilacja"]},
    "włosów|strzyżenie": {"etykieta": "strzyżenie", "rodzice": ["włosów"], "synonimy": ["strzyżenie", "cięcie"]},
    "włosów|modelowanie": {"etykieta": "modelowanie", "rodzice": ["włosów"], "synonimy": ["modelowanie"]},
    "twarzy|regulacja": {"etykieta": "regulacja", "rodzice": ["twarzy"], "synonimy": ["regulacja"]},
}}


def rek13(g: str, d: str = "x", poz: str = "zabieg", zestaw: float = 0.05, rozszerzenie: float = 0.05) -> dict:
    return {"pozycja": {poz: 0.95}, "zestaw": zestaw, "rozszerzenie": rozszerzenie,
            "zabiegi": [[d, g, 0.9, {}]], "sciezki": [[[d, g, "—", "—", "—"], 0.9, 0.9]]}


def prof(uid: int, g: str, frazy: list[tuple[str, str, str]], rel: dict | None = None, cechy: dict | None = None, **kw) -> dict:
    return potwierdz(profil_v14(uid, frazy, rek13(g, **kw), DRZEWO), rel or {}, cechy or {})


def rel(slownik: dict) -> dict:
    return {klucz_relacji(z, r, a, b): o for (z, r, a, b), o in slownik.items()}


def test_te_same_frazy_z_rolami_niezgodnymi_to_ta_sama_usluga():
    # „1:1” raz metoda, raz wielkość — ta sama fraza, więc zgodne bez pytania
    a = prof(1, "ciała|depilacja", [("zabieg", "depilacja", "nazwa"), ("metoda", "1:1", "nazwa")])
    b = prof(2, "ciała|depilacja", [("zabieg", "depilacja", "nazwa"), ("wielkosc", "1:1", "nazwa")])
    assert potrzebne_relacje(a, b) == set()
    assert porownaj_v14(a, b, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_inny_obszar_to_podobna():
    a = prof(1, "ciała|depilacja", [("metoda", "laserowa", "nazwa"), ("obszar", "pachy", "nazwa")])
    b = prof(2, "ciała|depilacja", [("metoda", "laserowa", "nazwa"), ("obszar", "bikini", "nazwa")])
    assert potrzebne_relacje(a, b) == {klucz_relacji("depilacja", "obszar", "bikini", "pachy")}
    r = rel({("depilacja", "obszar", "bikini", "pachy"): False})
    assert potrzebne_domniemania(a, b, r) == {(2, "pachy", "gdzie i ile", CECHA), (1, "bikini", "gdzie i ile", CECHA)}
    assert porownaj_v14(a, b, r, {}) == ("powiazane", "inny poziom: gdzie i ile", 3)


def test_pakiet_obszarow_z_wariantow_kontra_jeden_obszar_to_podobna():
    # „pachy + bikini” w wariantach kontra „pachy”: pytamy, czy druga OBEJMUJE wariant bikini — nie obejmuje
    a = prof(1, "ciała|depilacja", [("obszar", "pachy", "wariant"), ("obszar", "bikini", "wariant")])
    b = prof(2, "ciała|depilacja", [("obszar", "pachy", "nazwa")])
    r = rel({("depilacja", "obszar", "bikini", "pachy"): False})
    assert potrzebne_domniemania(a, b, r) == {(2, "bikini", "gdzie i ile", WARIANT)}
    assert porownaj_v14(a, b, r, {(2, "bikini", "gdzie i ile", WARIANT): False})[:2] == ("powiazane", "inny poziom: gdzie i ile")


def test_rodzina_dlugosci_kontra_cena_zalezna_od_dlugosci():
    # warianty długości kontra usługa bez wariantów, która obejmuje każdą długość → ta sama
    a = prof(1, "włosów|strzyżenie", [("dla_kogo", "damskie", "nazwa"), ("wielkosc", "krótkie", "wariant"),
                                      ("wielkosc", "długie", "wariant")])
    b = prof(2, "włosów|strzyżenie", [("dla_kogo", "damskie", "nazwa")])
    d = {(2, "krótkie", "gdzie i ile", WARIANT): True, (2, "długie", "gdzie i ile", WARIANT): True}
    assert potrzebne_domniemania(a, b, {}) == set(d)
    assert porownaj_v14(a, b, {}, d)[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_synonim_rozstrzyga_typesafe():
    a = prof(1, "paznokci|pedicure", [("metoda", "podologiczny", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("metoda", "leczniczy", "nazwa")])
    assert potrzebne_relacje(a, b) == {klucz_relacji("pedicure", "metoda", "podologiczny", "leczniczy")}
    r = rel({("pedicure", "metoda", "podologiczny", "leczniczy"): True})
    assert porownaj_v14(a, b, r, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_obszar_domniemany_po_obu_stronach_to_ta_sama():
    # „Pedicure stopy” kontra „Pedicure paznokci”: obie usługi mają cechę podaną przez drugą
    a = prof(1, "paznokci|pedicure", [("obszar", "stopy", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("obszar", "paznokci", "nazwa")])
    d = {(2, "stopy", "gdzie i ile", CECHA): True, (1, "paznokci", "gdzie i ile", CECHA): True}
    assert porownaj_v14(a, b, rel({("pedicure", "obszar", "paznokci", "stopy"): False}), d)[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_jedna_podaje_druga_nie_domniemanie():
    a = prof(1, "włosów|strzyżenie", [("dla_kogo", "męskie", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [])
    k = (2, "męskie", "gdzie i ile", CECHA)
    assert potrzebne_domniemania(a, b, {}) == {k}
    # barber: z salonu wynika, że strzyżenie jest męskie → zgodne
    assert porownaj_v14(a, b, {}, {k: True})[:2] == ("tozsame", "zgodne wszystkie poziomy")
    # nie wynika → za mało danych
    assert porownaj_v14(a, b, {}, {k: False}) == ("niepelne", "gdzie i ile: podaje tylko jedna", 3)


def test_rozne_cechy_po_obu_stronach_to_za_malo_danych():
    a = prof(1, "paznokci|pedicure", [("metoda", "hybrydowy", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("wielkosc", "długie", "nazwa")])
    assert potrzebne_relacje(a, b) == set()  # różne role — nie pytamy, czy znaczą to samo
    d = {(2, "hybrydowy", "metoda", CECHA): False, (1, "długie", "gdzie i ile", CECHA): False}
    assert porownaj_v14(a, b, {}, d)[:2] == ("niepelne", "metoda: podaje tylko jedna")


def test_dodatek_tylko_w_jednej_to_podobna_bez_domniemania():
    a = prof(1, "paznokci|pedicure", [("skladnik", "zdobienie", "nazwa")])
    b = prof(2, "paznokci|pedicure", [])
    assert potrzebne_domniemania(a, b, {}) == set()
    assert porownaj_v14(a, b, {}, {})[:2] == ("powiazane", "inny poziom: dodatek")


def test_kontekst_uzupelnia_tylko_brakujacy_poziom_i_wymaga_potwierdzenia():
    # „Regulacja woskiem” w kategorii „STYLIZACJA RZĘS I BRWI”: obszar z kategorii liczy się dopiero po potwierdzeniu
    frazy = [("metoda", "woskiem", "nazwa"), ("obszar", "rzęs", "kategoria"), ("obszar", "brwi", "kategoria"),
             ("metoda", "henna", "kategoria")]
    surowy = profil_v14(1, frazy, rek13("twarzy|regulacja"), DRZEWO)
    assert surowy["frazy"]["metoda"] == [("metoda", "woskiem", "nazwa")]  # nazwa podaje metodę — kategoria nie uzupełnia
    cechy, _rel = potrzebne_potwierdzenia(surowy)
    assert cechy == {(1, "rzęs", "gdzie i ile", CECHA), (1, "brwi", "gdzie i ile", CECHA)}
    p = potwierdz(surowy, {}, {(1, "rzęs", "gdzie i ile", CECHA): False, (1, "brwi", "gdzie i ile", CECHA): True})
    assert p["frazy"]["gdzie i ile"] == [("obszar", "brwi", "kategoria")]
    b = prof(2, "twarzy|regulacja", [("metoda", "woskiem", "nazwa"), ("obszar", "brwi", "nazwa")])
    assert porownaj_v14(p, b, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_kontekst_uzupelnia_kazda_ceche_osobno():
    # nazwa podaje długość, zabieg z Booksy „Strzyżenie damskie” — „damskie” ma wejść (sprawdzian 2, 28.09)
    frazy = [("wielkosc", "długich", "nazwa"), ("dla_kogo", "damskie", "zabieg_booksy"), ("obszar", "twarz i ciało", "kategoria"),
             ("wielkosc", "krótkie", "kategoria")]
    surowy = profil_v14(1, frazy, rek13("włosów|strzyżenie"), DRZEWO)
    assert surowy["frazy"]["gdzie i ile"] == [("obszar", "twarz i ciało", "kategoria"), ("wielkosc", "długich", "nazwa"),
                                              ("dla_kogo", "damskie", "zabieg_booksy")]
    assert potrzebne_potwierdzenia(surowy)[0] == {(1, "twarz i ciało", "gdzie i ile", CECHA), (1, "damskie", "gdzie i ile", CECHA)}


def test_inny_zabieg_to_inna_a_nazwa_zabiegu_nie_jest_skladem():
    a = prof(1, "włosów|strzyżenie", [("zabieg", "cięcie", "nazwa")])
    b = prof(2, "włosów|modelowanie", [("zabieg", "modelowanie", "nazwa")])
    assert a["frazy"]["skład"] == []  # „cięcie” to synonim węzła
    assert porownaj_v14(a, b, {}, {}) == ("rozne", "inny zabieg", 1)


def test_fraza_zabiegu_nazywajaca_wezel_wg_typesafe_nie_jest_skladem():
    surowy = profil_v14(1, [("zabieg", "strzyżonko", "nazwa")], rek13("włosów|strzyżenie", zestaw=0.9), DRZEWO)
    _cechy, r = potrzebne_potwierdzenia(surowy)
    assert r == {klucz_relacji("strzyżenie", "zabieg", "strzyżonko", "strzyżenie")}
    assert potwierdz(surowy, {klucz_relacji("strzyżenie", "zabieg", "strzyżonko", "strzyżenie"): True}, {})["frazy"]["skład"] == []
    assert potwierdz(surowy, {}, {})["frazy"]["skład"] == [("zabieg", "strzyżonko", "nazwa")]


def test_dodatkowy_zabieg_w_zestawie():
    a = prof(1, "włosów|strzyżenie", [("zabieg", "strzyżenie", "nazwa"), ("zabieg", "modelowanie", "nazwa")], zestaw=0.9)
    b = prof(2, "włosów|strzyżenie", [("zabieg", "strzyżenie", "nazwa"), ("zabieg", "koloryzacja", "nazwa")], zestaw=0.9)
    r = rel({("strzyżenie", "zabieg", "koloryzacja", "modelowanie"): False})
    assert porownaj_v14(a, b, r, {})[:2] == ("powiazane", "inny poziom: skład")
    c = prof(3, "włosów|modelowanie", [("zabieg", "modelowanie", "nazwa"), ("zabieg", "strzyżenie", "nazwa")], zestaw=0.9)
    # zestaw tych samych zabiegów, choć drzewo wskazało inny węzeł główny → ta sama
    assert porownaj_v14(a, c, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_pozycje_poza_zabiegiem_i_brak_danych():
    a = prof(1, "paznokci|pedicure", [], poz="produkt")
    b = prof(2, "paznokci|pedicure", [])
    assert porownaj_v14(a, b, {}, {})[:2] == ("rozne", "nie porównujemy: produkt")
    assert porownaj_v14(None, b, {}, {})[:2] == ("niepelne", "brak destylacji")
    c = prof(3, "paznokci|pedicure", [], poz="pakiet")
    assert porownaj_v14(c, b, {}, {})[:2] == ("powiazane", "pakiet i pojedyncza wizyta")


def test_brak_odpowiedzi_typesafe_nie_daje_ta_sama():
    a = prof(1, "paznokci|pedicure", [("metoda", "podologiczny", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("metoda", "leczniczy", "nazwa")])
    assert porownaj_v14(a, b, {}, {})[0] != "tozsame"
    # fraza z kontekstu bez odpowiedzi TypeSafe nie wchodzi do porównania
    assert prof(3, "paznokci|pedicure", [("metoda", "hybrydowy", "kategoria")])["frazy"]["metoda"] == []


def test_usluga_bez_wezla_zabiegu_nie_wywraca_potwierdzenia():
    rek = {"pozycja": {"zabieg": 0.9}, "zestaw": 0.05, "rozszerzenie": 0.05, "zabiegi": [], "sciezki": []}
    surowy = profil_v14(1, [("zabieg", "coś", "nazwa")], rek, DRZEWO)
    assert potrzebne_potwierdzenia(surowy) == (set(), set())
    assert potwierdz(surowy, {}, {})["frazy"]["skład"] == [("zabieg", "coś", "nazwa")]


def test_poza_zestawem_slowo_zabiegu_nie_jest_skladem():
    # „Lip Flip” kontra „Lip Flip powiększenie ust”: „powiększenie” to cel tego samego zabiegu, nie drugi zabieg
    a = prof(1, "włosów|strzyżenie", [("metoda", "flip", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [("metoda", "flip", "nazwa"), ("zabieg", "powiększenie", "nazwa")])
    assert potrzebne_relacje(a, b) == set()
    assert porownaj_v14(a, b, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_zestaw_po_punkcie_neutralnym():
    a = prof(1, "włosów|strzyżenie", [], zestaw=0.34)
    b = prof(2, "włosów|strzyżenie", [("obszar", "broda", "nazwa")], zestaw=0.94)
    assert porownaj_v14(a, b, {}, {})[:2] == ("powiazane", "zestaw")


def test_dwa_pakiety_bez_skladu_to_za_malo_danych():
    a = prof(1, "włosów|strzyżenie", [], zestaw=0.93)
    b = prof(2, "włosów|strzyżenie", [], zestaw=0.97)
    assert porownaj_v14(a, b, {}, {})[:2] == ("niepelne", "skład nieznany")


def test_cudzyslowy_nie_rozrozniaja_fraz():
    a = prof(1, "włosów|strzyżenie", [("obszar", "uśmiechu „gummy smile", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [("obszar", 'uśmiechu "gummy smile', "nazwa")])
    assert porownaj_v14(a, b, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_poza_zestawem_slowo_zmieniajace_zabieg_zostaje():
    # „Remover brwi” w węźle makijażu permanentnego: TypeSafe mówi, że „remover” zmienia zabieg → nie ta sama
    surowy = profil_v14(2, [("zabieg", "remover", "nazwa"), ("obszar", "brwi", "nazwa")], rek13("włosów|strzyżenie"), DRZEWO)
    k = (2, "remover", "strzyżenie", ZMIANA)
    assert potrzebne_potwierdzenia(surowy) == ({k}, set())
    b = potwierdz(surowy, {}, {k: True})
    a = prof(1, "włosów|strzyżenie", [("obszar", "brwi", "nazwa")])
    assert porownaj_v14(a, b, {}, {})[:2] == ("powiazane", "inny poziom: skład")
    assert porownaj_v14(a, potwierdz(surowy, {}, {k: False}), {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")
