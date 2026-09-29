"""Katalog usług — kontrakt wyciągania cech oferty (bez sieci): oferty z usługi, prompt, sprawdzenie odpowiedzi modelu."""
from __future__ import annotations

from services.katalog_uslug.ekstrakcja import Oferta, oferty_z_uslugi, prompt, waliduj

USLUGA = {"id": 7, "typ_salonu": "Medycyna Estetyczna", "kategoria": "Zabiegi na twarz", "nazwa": "Mezoterapia igłowa",
          "zabieg_booksy": "Mezoterapia igłowa", "opis": "",
          "warianty": [{"label": "Mezoterapia igłowa twarz", "cena_zl": 450, "min": 60},
                       {"label": "Mezoterapia igłowa twarz + szyja", "cena_zl": 500, "min": 60}]}


def oferta(**kw: str) -> Oferta:
    base = {"id": "1", "typ_salonu": "Barber shop", "kategoria": "Broda", "nazwa": "Strzyżenie brody", "wariant": "",
            "zabieg_booksy": "Strzyżenie brody", "opis": "", "cena_zl": 50.0}
    return Oferta(**{**base, **kw})


def rekord(**kw: object) -> dict:
    base = {"id": "1", "nazwa": "Strzyżenie brody", "pozycja": "zabieg",
            "zabieg": {"fraza": "Strzyżenie", "zrodlo": "nazwa"},
            "cechy": [{"rola": "obszar", "fraza": "brody", "zrodlo": "nazwa"}], "szum": []}
    return {**base, **kw}


def test_warianty_z_etykieta_to_osobne_oferty() -> None:
    o = oferty_z_uslugi(USLUGA)
    assert [x.id for x in o] == ["7#0", "7#1"]
    assert [x.wariant for x in o] == ["Mezoterapia igłowa twarz", "Mezoterapia igłowa twarz + szyja"]
    assert [x.cena_zl for x in o] == [450, 500]


def test_usluga_bez_etykiet_wariantow_to_jedna_oferta() -> None:
    u = {**USLUGA, "warianty": [{"label": "", "cena_zl": 80, "min": 30}], "cena_gr": 8000}
    assert [(x.id, x.wariant, x.cena_zl) for x in oferty_z_uslugi(u)] == [("7", "", 80)]


def test_prompt_niesie_id_i_pola_ofert() -> None:
    p = prompt([oferta(id="a1"), oferta(id="a2", nazwa="Combo Premium")])
    assert '"id": "a1"' in p and '"id": "a2"' in p and "Combo Premium" in p


def test_poprawna_odpowiedz_przechodzi() -> None:
    wynik, bledy = waliduj([oferta()], {"oferty": [rekord()]})
    assert bledy == [] and wynik["1"]["zabieg"]["fraza"] == "Strzyżenie"


def test_zla_liczba_rekordow_odrzuca_paczke() -> None:
    wynik, bledy = waliduj([oferta(), oferta(id="2")], {"oferty": [rekord()]})
    assert wynik == {} and any("liczba" in b for b in bledy)


def test_nieznane_id_albo_inna_nazwa_to_blad_rekordu() -> None:
    _w, bledy = waliduj([oferta()], {"oferty": [rekord(id="9")]})
    assert any("id" in b for b in bledy)
    _w, bledy = waliduj([oferta()], {"oferty": [rekord(nazwa="Strzyżenie włosów")]})
    assert any("nazwa" in b for b in bledy)


def test_fraza_spoza_tekstu_oferty_zostaje_oznaczona_i_wypada() -> None:
    zly = rekord(cechy=[{"rola": "obszar", "fraza": "brody", "zrodlo": "nazwa"},
                        {"rola": "metoda", "fraza": "brzytwą", "zrodlo": "nazwa"}])
    wynik, bledy = waliduj([oferta()], {"oferty": [zly]})
    assert wynik["1"]["obce"] == ["brzytwą"] and [c["fraza"] for c in wynik["1"]["cechy"]] == ["brody"]
    assert any("spoza tekstu" in b for b in bledy)


def test_nieprzypisane_slowo_nazwy_jest_zaznaczone() -> None:
    o = oferta(nazwa="Strzyżenie brody UV")
    wynik, _b = waliduj([o], {"oferty": [rekord(nazwa="Strzyżenie brody UV")]})
    assert wynik["1"]["nieprzypisane"] == ["uv"]


def test_szum_pokrywa_slowa() -> None:
    o = oferta(nazwa="Strzyżenie brody Paweł")
    r = rekord(nazwa="Strzyżenie brody Paweł", szum=["Paweł"])
    wynik, _b = waliduj([o], {"oferty": [r]})
    assert wynik["1"]["nieprzypisane"] == []
