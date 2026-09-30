"""Katalog usług — wycena oferty podmiotu z werdyktów podpisu (etap 3 planu 29.09), bez sieci i bazy."""
from __future__ import annotations

from services.katalog_uslug.dopasowanie import wycen
from services.katalog_uslug.podpis import INNA, PODOBNA, TA_SAMA

PODMIOT = {"service_name": "Manicure hybrydowy", "price_grosze": 12000, "duration_minutes": 60, "is_package": False}


def probka(booksy_id: int, cena_zl: int, minuty: int = 60, sim: float = 0.9, **kw: object) -> dict:
    return {"service_id": booksy_id * 10, "booksy_id": booksy_id, "service_name": "Manicure hybrydowy",
            "price_grosze": cena_zl * 100, "duration_minutes": minuty, "similarity": sim, "is_package": False, **kw}


def test_cena_tylko_z_ta_sama_a_podobne_osobno() -> None:
    kand = [(probka(i, 100 + i), TA_SAMA, "równe podpisy") for i in range(1, 6)]
    kand += [(probka(9, 400), PODOBNA, "rdzen: „japonski” tylko po jednej stronie"), (probka(8, 20), INNA, "inny zabieg")]
    r = wycen(PODMIOT, kand)
    assert r.status == "sufficient" and r.n_unique_salons == 5
    assert r.market_price_grosze == 10300  # mediana zł/min × 60 min podmiotu: 103 zł
    assert [s["booksy_id"] for s in r.related_samples] == [9]
    assert r.related_samples[0]["related_reason"].startswith("podobna:")


def test_jeden_salon_liczy_sie_raz_a_mniej_niz_trzy_to_brak_ceny() -> None:
    kand = [(probka(1, 100, sim=0.95), TA_SAMA, ""), (probka(1, 300, sim=0.7), TA_SAMA, ""), (probka(2, 110), TA_SAMA, "")]
    r = wycen(PODMIOT, kand)
    assert r.n_unique_salons == 2 and r.status == "insufficient" and r.market_price_grosze is None


def test_pakiet_nie_wchodzi_do_ceny() -> None:
    kand = [(probka(i, 100), TA_SAMA, "") for i in range(1, 4)] + [(probka(7, 900, is_package=True), TA_SAMA, "")]
    assert wycen(PODMIOT, kand).market_price_grosze == 10000


def test_podobna_tylko_przy_tym_samym_rdzeniu() -> None:
    from services.katalog_uslug.dopasowanie import werdykty
    from services.katalog_uslug.podpis import Klasy, podpis

    def rek(zabieg: str, cechy: list[tuple[str, str]]) -> dict:
        return {"pozycja": "zabieg", "zabieg": {"fraza": zabieg, "zrodlo": "nazwa"},
                "cechy": [{"rola": r, "fraza": f, "zrodlo": "nazwa"} for r, f in cechy]}
    ps = podpis(rek("Manicure", [("metoda", "hybrydowy")]))
    z_frenchem = podpis(rek("Manicure", [("metoda", "hybrydowy"), ("sklad", "+ french")]))
    pedicure = podpis(rek("Pedicure", [("metoda", "hybrydowy")]))
    w = werdykty(ps, [(probka(1, 150), z_frenchem), (probka(2, 130), pedicure)], Klasy())
    assert [x[1] for x in w] == [PODOBNA, INNA]
