"""Drzewo v14 — kontrakt porównania: drzewo do poziomu zabiegu, niżej frazy obu usług wprost (zgoda Alexa 28.09).

Odpowiedzi TypeSafe (czy usługa jest właśnie taka, czy obejmuje wariant) dostarcza tu słownik —
w pomiarze rozstrzyga je TypeSafe. Poziomy Score „czy właśnie taka”: INNY / MOZE / TEN_SAM (cookbook entity_alignment). Zero sieci; żadnych reguł na nazwach (kod porównuje tylko identyczne frazy).
"""
from __future__ import annotations

from services.typesafe_drzewo.drzewo_v14 import (
    CECHA,
    DOKLADNIE,
    INNY,
    MOZE,
    TEN_SAM,
    WARIANT,
    Wariant,
    ZMIANA,
    klucz_dokladnie,
    klucz_relacji,
    porownaj_v14,
    potrzebne_domniemania,
    potrzebne_synonimy,
    potrzebne_potwierdzenia,
    potwierdz,
    poziom_score,
    profil_v14,
    pytanie_dla,
)
from typesafe_sdk import Score

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
    assert potrzebne_domniemania(a, b) == set()
    assert porownaj_v14(a, b, {}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_poziom_score_najblizszy_poziom():
    # route() z cookbooka entity_alignment: najbliższy poziom; rozdarcie „nie” / „tak” (0,37 / 0,63 → 1,26) = „milczy”;
    # brak odpowiedzi = „milczy”, nigdy „ta sama”
    assert [poziom_score(x) for x in (0.0, 0.49, 0.5, 1.26, 1.49, 1.5, 2.0)] == [INNY, INNY, MOZE, MOZE, MOZE, TEN_SAM, TEN_SAM]
    assert poziom_score(None) == MOZE


def test_inny_obszar_to_podobna():
    a = prof(1, "ciała|depilacja", [("metoda", "laserowa", "nazwa"), ("obszar", "pachy", "nazwa")])
    b = prof(2, "ciała|depilacja", [("metoda", "laserowa", "nazwa"), ("obszar", "bikini", "nazwa")])
    # obie podają obszar → pytanie do każdej: czy u niej to właśnie obszar drugiej (pełny kontekst usługi)
    kb, ka = klucz_dokladnie(2, "obszar", ["pachy"]), klucz_dokladnie(1, "obszar", ["bikini"])
    assert potrzebne_domniemania(a, b) == {ka, kb}
    assert porownaj_v14(a, b, {ka: INNY, kb: INNY}, {}) == ("powiazane", "inny poziom: gdzie i ile", 3)
    assert porownaj_v14(a, b, {ka: TEN_SAM, kb: MOZE}, {}) == ("niepelne", "gdzie i ile: nie wiadomo, czy to samo", 3)
    assert porownaj_v14(a, b, {ka: TEN_SAM, kb: TEN_SAM}, {})[0] == "tozsame"  # obie strony potwierdzają


def test_czesc_obszaru_to_podobna_bez_pytania_o_ceche():
    # sprawdzian 4 (28.09): „uda” kontra „nogi” — TypeSafe: inny zakres; dawny ratunek „nogi ma cechę uda” 0,84 dawał „ta sama”
    a = prof(1, "ciała|depilacja", [("metoda", "pastą cukrową", "nazwa"), ("obszar", "uda", "nazwa")])
    b = prof(2, "ciała|depilacja", [("metoda", "pastą cukrową", "nazwa"), ("obszar", "nogi", "nazwa")])
    kb, ka = klucz_dokladnie(2, "obszar", ["uda"]), klucz_dokladnie(1, "obszar", ["nogi"])
    assert potrzebne_domniemania(a, b) == {ka, kb}
    assert porownaj_v14(a, b, {ka: INNY, kb: TEN_SAM}, {})[:2] == ("powiazane", "inny poziom: gdzie i ile")


def test_pakiet_obszarow_z_wariantow_kontra_jeden_obszar_to_podobna():
    # „pachy + bikini” w wariantach kontra „pachy”: pytamy, czy druga OBEJMUJE wariant bikini — nie obejmuje
    a = prof(1, "ciała|depilacja", [("obszar", "pachy", "wariant"), ("obszar", "bikini", "wariant")])
    b = prof(2, "ciała|depilacja", [("obszar", "pachy", "nazwa")])
    # „pachy” zgodne; druga (bez nadwyżki) pytana o komplet obszarów pierwszej
    kb = klucz_dokladnie(2, "obszar", ["pachy", "bikini"])
    assert potrzebne_domniemania(a, b) == {kb}
    assert porownaj_v14(a, b, {kb: INNY}, {})[:2] == ("powiazane", "inny poziom: gdzie i ile")
    # w obie strony: pierwsza (pakiet) pytana też o samą „pachy” drugiej
    w = Wariant(obie_strony=True)
    ka = klucz_dokladnie(1, "obszar", ["pachy"], w)
    assert potrzebne_domniemania(a, b, w) == {kb, ka}
    assert porownaj_v14(a, b, {kb: TEN_SAM, ka: INNY}, {}, w)[:2] == ("powiazane", "inny poziom: gdzie i ile")


def test_nadwyzka_jednej_strony_pytana_w_obie_strony():
    # zbiór 3 (28.09): „żelem” kontra „żelem + frencz” — węższa potwierdzała komplet szerszej (1,93); szersza musi
    # też odpowiedzieć, czy jest samym „żelem”
    a = prof(1, "paznokci|pedicure", [("metoda", "żelem", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("metoda", "żelem", "nazwa"), ("metoda", "frencz", "nazwa")])
    ka = klucz_dokladnie(1, "metoda", ["żelem", "frencz"])
    assert potrzebne_domniemania(a, b) == {ka}  # W2: tylko węższa
    w = Wariant(obie_strony=True)
    ka, kb = klucz_dokladnie(1, "metoda", ["żelem", "frencz"], w), klucz_dokladnie(2, "metoda", ["żelem"], w)
    assert potrzebne_domniemania(a, b, w) == {ka, kb}
    assert porownaj_v14(a, b, {ka: TEN_SAM, kb: INNY}, {}, w)[:2] == ("powiazane", "inny poziom: metoda")


def test_rodzina_dlugosci_kontra_cena_zalezna_od_dlugosci():
    # warianty długości kontra usługa bez wariantów, która obejmuje każdą długość → ta sama
    a = prof(1, "włosów|strzyżenie", [("dla_kogo", "damskie", "nazwa"), ("wielkosc", "krótkie", "wariant"),
                                      ("wielkosc", "długie", "wariant")])
    b = prof(2, "włosów|strzyżenie", [("dla_kogo", "damskie", "nazwa")])
    d = {(2, "krótkie", "gdzie i ile", WARIANT): True, (2, "długie", "gdzie i ile", WARIANT): True}
    assert potrzebne_domniemania(a, b) == set(d)
    assert porownaj_v14(a, b, {}, d)[:2] == ("tozsame", "zgodne wszystkie poziomy")
    assert porownaj_v14(a, b, {}, {**d, (2, "krótkie", "gdzie i ile", WARIANT): False})[0] == "niepelne"


def test_synonim_rozstrzyga_typesafe():
    a = prof(1, "paznokci|pedicure", [("metoda", "podologiczny", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("metoda", "leczniczy", "nazwa")])
    kb, ka = klucz_dokladnie(2, "metoda", ["podologiczny"]), klucz_dokladnie(1, "metoda", ["leczniczy"])
    assert potrzebne_domniemania(a, b) == {ka, kb}
    assert porownaj_v14(a, b, {ka: TEN_SAM, kb: TEN_SAM}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")
    assert porownaj_v14(a, b, {ka: TEN_SAM, kb: INNY}, {})[:2] == ("powiazane", "inny poziom: metoda")


def test_ten_sam_zakres_innymi_slowami_to_ta_sama():
    # „Pedicure stopy” kontra „Pedicure paznokci”: każda na swoim opisie potwierdza frazę drugiej
    a = prof(1, "paznokci|pedicure", [("obszar", "stopy", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("obszar", "paznokci", "nazwa")])
    d = {klucz_dokladnie(2, "obszar", ["stopy"]): TEN_SAM, klucz_dokladnie(1, "obszar", ["paznokci"]): TEN_SAM}
    assert potrzebne_domniemania(a, b) == set(d)
    assert porownaj_v14(a, b, d, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")


def test_jedna_podaje_druga_nie_czy_wlasnie_taka():
    a = prof(1, "włosów|strzyżenie", [("dla_kogo", "męskie", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [])
    k = klucz_dokladnie(2, "dla_kogo", ["męskie"])
    assert k == (2, "„męskie”", "dla_kogo", DOKLADNIE)
    assert potrzebne_domniemania(a, b) == {k}
    # barber: z salonu wynika, że strzyżenie jest męskie → zgodne
    assert porownaj_v14(a, b, {k: TEN_SAM}, {})[:2] == ("tozsame", "zgodne wszystkie poziomy")
    # nic nie mówi → za mało danych; szersza albo inna → znana różnica
    assert porownaj_v14(a, b, {k: MOZE}, {}) == ("niepelne", "gdzie i ile: podaje tylko jedna", 3)
    assert porownaj_v14(a, b, {k: INNY}, {}) == ("powiazane", "inny poziom: gdzie i ile", 3)
    assert porownaj_v14(a, b, {}, {})[0] == "niepelne"


def test_frazy_jednej_roli_w_jednym_pytaniu():
    # „maszynka + nożyczki” to co innego niż sama „maszynką” — pytamy o komplet frazy roli naraz
    a = prof(1, "włosów|strzyżenie", [("metoda", "nożyczki", "nazwa"), ("metoda", "maszynka", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [])
    k = klucz_dokladnie(2, "metoda", ["nożyczki", "maszynka"])
    assert k[1] == "„maszynka”, „nożyczki”"
    assert potrzebne_domniemania(a, b) == {k}
    pyt = pytanie_dla(k)
    assert isinstance(pyt, Score) and len(pyt.criteria) == 3 and "wszystko razem" not in pyt.instructions
    # komplet „+”: model czytał listę po przecinku jak „którakolwiek”
    w = Wariant(razem=True)
    k = klucz_dokladnie(2, "metoda", ["nożyczki", "maszynka"], w)
    assert k[1] == "„maszynka” + „nożyczki”" and potrzebne_domniemania(a, b, w) == {k}
    assert "„maszynka” + „nożyczki” — wszystko razem" in pytanie_dla(k).instructions


def test_rozne_cechy_po_obu_stronach_to_za_malo_danych():
    a = prof(1, "paznokci|pedicure", [("metoda", "hybrydowy", "nazwa")])
    b = prof(2, "paznokci|pedicure", [("wielkosc", "długie", "nazwa")])
    ka, kb = klucz_dokladnie(2, "metoda", ["hybrydowy"]), klucz_dokladnie(1, "wielkosc", ["długie"])
    assert potrzebne_domniemania(a, b) == {ka, kb}
    assert porownaj_v14(a, b, {ka: MOZE, kb: MOZE}, {})[:2] == ("niepelne", "metoda: podaje tylko jedna")


def test_dodatek_tylko_w_jednej_to_podobna_bez_domniemania():
    a = prof(1, "paznokci|pedicure", [("skladnik", "zdobienie", "nazwa")])
    b = prof(2, "paznokci|pedicure", [])
    assert potrzebne_domniemania(a, b) == set()
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
    kb, ka = klucz_dokladnie(2, "zabieg", ["strzyżenie", "modelowanie"]), klucz_dokladnie(1, "zabieg", ["strzyżenie", "koloryzacja"])
    assert potrzebne_domniemania(a, b) == {ka, kb}
    assert porownaj_v14(a, b, {ka: INNY, kb: INNY}, {})[:2] == ("powiazane", "inny poziom: skład")
    assert porownaj_v14(a, b, {ka: MOZE, kb: MOZE}, {})[:2] == ("powiazane", "inny poziom: skład")  # skład: tylko „tak” zgadza
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
    assert potrzebne_domniemania(a, b) == set()
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


def test_synonimy_przed_pytaniem_o_opis():
    # zbiór 5 (28.09): „grzywka” / „grzywki” — pełny kontekst wahał się (0,84–0,92); pytanie o dwie frazy rozstrzyga odmianę
    a = prof(1, "włosów|strzyżenie", [("obszar", "grzywki", "nazwa")])
    b = prof(2, "włosów|strzyżenie", [("obszar", "grzywka", "nazwa")])
    w = Wariant(synonimy=True)
    k = klucz_relacji("strzyżenie", "obszar", "grzywka", "grzywki")
    assert potrzebne_synonimy(a, b) == set()  # W2 nie pyta
    assert potrzebne_synonimy(a, b, w) == {k}
    assert potrzebne_domniemania(a, b, w, {k: True}) == set()
    assert porownaj_v14(a, b, {}, {}, w, {k: True})[:2] == ("tozsame", "zgodne wszystkie poziomy")
    # „nie to samo” (albo brak odpowiedzi) → decyduje jak dotąd pytanie o pełny opis drugiej usługi
    kb = klucz_dokladnie(2, "obszar", ["grzywki"], w)
    assert kb in potrzebne_domniemania(a, b, w, {k: False})
    assert porownaj_v14(a, b, {kb: MOZE, klucz_dokladnie(1, "obszar", ["grzywka"], w): MOZE}, {}, w, {k: False})[0] == "niepelne"


def test_zrodlo_fraz_w_pytaniu():
    # zbiory 3–5 (28.09): lista po przecinku gubi spójnik — nazwa drugiej usługi mówi, czy to komplet, czy alternatywa
    w = Wariant(synonimy=True, zrodlo=True)
    a = profil_v14(1, [("obszar", "twarz", "nazwa")], rek13("ciała|depilacja"), DRZEWO, nazwa="Mezoterapia twarz")
    b = profil_v14(2, [("obszar", "twarzy", "nazwa"), ("obszar", "szyi", "nazwa")], rek13("ciała|depilacja"), DRZEWO,
                   nazwa="Mezoterapia twarzy + szyi")
    k = klucz_relacji("depilacja", "obszar", "twarz", "twarzy")
    klucze = potrzebne_domniemania(a, b, w, {k: True})
    assert klucze == {(1, "„szyi”, „twarzy” ⟨w nazwie tamtej usługi: „Mezoterapia twarzy + szyi”⟩", "obszar", DOKLADNIE)}
    pyt = pytanie_dla(next(iter(klucze)))
    assert "(w nazwie tamtej usługi: „Mezoterapia twarzy + szyi”)" in pyt.instructions and "⟨" not in pyt.instructions
    # frazy z wariantów: opcje, które druga musi objąć wszystkie
    c = profil_v14(3, [("obszar", "wąsik", "wariant"), ("obszar", "pachy", "wariant")], rek13("ciała|depilacja"), DRZEWO,
                   nazwa="Depilacja woskiem")
    d = profil_v14(4, [("obszar", "wąsika", "nazwa")], rek13("ciała|depilacja"), DRZEWO, nazwa="Depilacja wąsika")
    k2 = klucz_relacji("depilacja", "obszar", "wąsik", "wąsika")
    klucze = potrzebne_domniemania(c, d, w, {k2: True})
    assert any("musi obejmować każdą" in x[1] and x[0] == 4 for x in klucze)
