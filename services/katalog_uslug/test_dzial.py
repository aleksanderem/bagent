"""Katalog usług — rozbiór działu (decyzja Alexa 1.10): kontekst nagłówka i salonu przypisuje model, podpis go bierze
bez reguł o źródłach; walidacja przyjmuje frazę z nazwy salonu. Bez sieci."""
from __future__ import annotations

from services.katalog_uslug.ekstrakcja import Oferta, prompt_dzialu, waliduj
from services.katalog_uslug.podpis import TA_SAMA, Klasy, podpis, porownaj


def _rek(cechy: list[tuple[str, str, str]], zabieg: str = "Usuwanie prosaka", dzial: bool = True) -> dict:
    r = {"pozycja": "zabieg", "zabieg": {"fraza": zabieg, "zrodlo": "nazwa"}, "nieprzypisane": [],
         "cechy": [{"rola": rola, "fraza": f, "zrodlo": zr} for rola, f, zr in cechy]}
    return {**r, "dzial": True} if dzial else r


def test_metoda_z_naglowka_dzialu_rozroznia_usluge() -> None:
    laser = podpis(_rek([("metoda", "laser CO2", "dzial")]))
    recznie = podpis(_rek([]))
    assert {"laser", "co2"} <= laser.zbior and porownaj(laser, recznie, Klasy())[0] != TA_SAMA


def test_kontekst_kategorii_i_salonu_z_regul_nie_dziala_przy_rozbiorze_dzialu() -> None:
    kontekst = {"nazwa": "Depilacja woskiem", "zabieg": {"fraza": "Depilacja", "zrodlo": "nazwa"},
                "cechy": [{"rola": "metoda", "fraza": "woskiem", "zrodlo": "nazwa"}], "pozycja_kategorii": "produkt"}
    p = podpis(_rek([("obszar", "pachy", "nazwa")], zabieg="Depilacja"), kontekst=kontekst)
    assert "wosk" not in " ".join(p.zbior) and p.pozycja == "zabieg"  # model nie przypisał — reguła nie dokleja


def test_z_opisu_tylko_wylaczenia_i_liczby() -> None:
    p = podpis(_rek([("wylaczenie", "bez farbki", "opis"), ("sklad", "maska", "opis")], zabieg="Henna brwi"))
    assert "farbk" in p.zbior and not any(w.startswith("mask") for w in p.zbior)


def test_walidacja_przyjmuje_fraze_z_nazwy_salonu() -> None:
    o = Oferta(id="1", typ_salonu="Depilacja", kategoria="Ciało", nazwa="Pachy", wariant="", zabieg_booksy="", opis="",
               cena_zl=100)
    odp = {"oferty": [{"id": "1", "nazwa": "Pachy", "pozycja": "zabieg",
                       "zabieg": {"fraza": "Pachy", "zrodlo": "nazwa"},
                       "cechy": [{"rola": "metoda", "fraza": "Laser", "zrodlo": "salon"}], "szum": []}]}
    bez_salonu, _ = waliduj([o], odp)
    z_salonem, bledy = waliduj([o], odp, salony={"1": "Laser Studio"})
    assert bez_salonu["1"]["obce"] == ["Laser"] and z_salonem["1"]["obce"] == [] and not bledy


def test_prompt_dzialu_grupuje_oferty_pod_naglowkiem() -> None:
    o = Oferta(id="7", typ_salonu="Medycyna Estetyczna", kategoria="Usuwanie zmian (laser CO2)", nazwa="Usuwanie prosaka",
               wariant="", zabieg_booksy="", opis="", cena_zl=199)
    tekst = prompt_dzialu([("Usuwanie zmian (laser CO2)", "Klinika X", [o])])
    assert '"dzial": "Usuwanie zmian (laser CO2)"' in tekst and '"salon": "Klinika X"' in tekst
    assert '"kategoria"' not in tekst.split("Działy:")[1]  # nagłówek raz na dział, nie przy każdej ofercie
