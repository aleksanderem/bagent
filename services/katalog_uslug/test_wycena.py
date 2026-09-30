"""Katalog usług — wycena usług podmiotu podpisem: rodzaje wierszy i klucz pamięci rozbioru (bez sieci)."""
from __future__ import annotations

from services.katalog_uslug.ekstrakcja import Oferta
from services.katalog_uslug.podpis import Klasy, podpis
from services.katalog_uslug.wycena import (BRAK, TA_SAMA_MALO, TA_SAMA_RYNEK, TYLKO_PODOBNE, klucz_oferty,
                                           wycen_oferty)


def rek(zabieg: str, cechy: list[tuple[str, str]] | None = None) -> dict:
    return {"pozycja": "zabieg", "zabieg": {"fraza": zabieg, "zrodlo": "nazwa"},
            "cechy": [{"rola": r, "fraza": f, "zrodlo": "nazwa"} for r, f in (cechy or [])], "nieprzypisane": []}


def probka(salon: int, cena_zl: float, nazwa: str = "Strzyżenie męskie") -> dict:
    return {"service_id": salon * 10, "booksy_id": salon, "service_name": nazwa, "price_grosze": round(cena_zl * 100),
            "duration_minutes": 30, "similarity": 0.9, "is_package": False}


PODMIOT = {"service_name": "Strzyżenie męskie", "price_grosze": 6000, "duration_minutes": 30, "is_package": False}
MESKIE = podpis(rek("Strzyżenie", [("dla_kogo", "męskie")]))
DAMSKIE = podpis(rek("Strzyżenie", [("dla_kogo", "damskie")]))


def test_trzy_salony_ta_sama_to_mediana_rynku() -> None:
    w = wycen_oferty({"1": (PODMIOT, MESKIE)}, {"1": [(probka(s, c), MESKIE) for s, c in ((1, 50), (2, 60), (3, 70))]},
                     Klasy())["1"]
    assert w.rodzaj == TA_SAMA_RYNEK and w.wynik.market_price_grosze == 6000 and w.wynik.n_unique_salons == 3


def test_jeden_dwa_salony_to_ich_ceny_bez_mediany() -> None:
    w = wycen_oferty({"1": (PODMIOT, MESKIE)}, {"1": [(probka(1, 50), MESKIE), (probka(2, 70), MESKIE)]}, Klasy())["1"]
    assert w.rodzaj == TA_SAMA_MALO and w.wynik.market_price_grosze is None
    assert sorted(s["price_grosze"] for s in w.wynik.samples) == [5000, 7000]  # ceny do pokazania w wierszu


def test_tylko_podobne_i_brak() -> None:
    kand = [(probka(s, 100, "Strzyżenie damskie"), DAMSKIE) for s in (1, 2, 3)]
    assert wycen_oferty({"1": (PODMIOT, MESKIE)}, {"1": kand}, Klasy())["1"].rodzaj == TYLKO_PODOBNE
    assert wycen_oferty({"1": (PODMIOT, MESKIE)}, {}, Klasy())["1"].rodzaj == BRAK


def test_klucz_oferty_to_pola_widziane_przez_model_bez_kontaktow() -> None:
    o = Oferta(id="1", typ_salonu="Barber shop", kategoria="Włosy", nazwa="Strzyżenie", wariant="", zabieg_booksy="",
               opis="Zapisy: 600 100 200", cena_zl=60)
    zamaskowana = Oferta(**{**o.__dict__, "id": "2", "opis": "Zapisy: [telefon]", "cena_zl": 80})
    assert klucz_oferty(o) == klucz_oferty(zamaskowana)  # id i cena nie idą do modelu, kontakt jest maskowany
    assert klucz_oferty(o) != klucz_oferty(Oferta(**{**o.__dict__, "kategoria": "Broda"}))
