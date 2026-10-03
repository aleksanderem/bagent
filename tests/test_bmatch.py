"""services/bmatch — kontrakt z b-card/b-match, dobór z kart, polityka werdyktów, przełącznik w wycenie, punkt Runpod."""
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.bmatch import dobor, polityka
from services.bmatch.klucz import klucz_uslugi, para_klucz, wiadomosci_bcard

FIX = json.loads((Path(__file__).parent / "fixtures_bmatch_kontrakt.json").read_text())


# --- kontrakt: identyczny z b-card (wartości wyliczone kodem ~/projects/b-card/kontrakt/kontrakt.py) ---

@pytest.mark.parametrize("f", FIX)
def test_klucz_identyczny_z_bcard(f):
    assert klucz_uslugi(f["usluga"]) == f["klucz"]


@pytest.mark.parametrize("f", FIX)
def test_wejscie_bcard_identyczne(f):
    assert wiadomosci_bcard(f["usluga"], "Salon Kosmetyczny")[1]["content"] == f["wejscie_bcard"]


def test_para_klucz_niezalezny_od_kolejnosci():
    assert para_klucz("b", "a") == para_klucz("a", "b") == "a|b"


# --- dobór z kart ---

def _karta(*sk, marka=None):
    return {"skladniki": [{"zabieg": z, "metoda": m, "obszar": o} for z, m, o in sk], "marka": marka}


def test_zgodnosc_kart_puste_pole_jest_zgodne():
    a = dobor.skladniki(_karta(("manicure", "hybrydowy", "dłonie")))
    assert dobor.zgodne_karty(a, [["manicure", "", "dłonie"]])
    assert not dobor.zgodne_karty(a, [["manicure", "żelowy", "dłonie"]])
    assert not dobor.zgodne_karty(a, [["pedicure", "hybrydowy", "stopy"]])
    assert not dobor.zgodne_karty(a, [["manicure", "hybrydowy", "dłonie"], ["pedicure", "", ""]])


def _oferta(sid, booksy, nazwa, sk, marka=None):
    return {"service_id": sid, "booksy_id": booksy, "klucz": f"k{sid}", "nazwa": nazwa, "skladniki": sk, "marka": marka}


def test_wybierz_najpodobniejsze_i_limit_plus_wybrani():
    k = _karta(("manicure", "hybrydowy", "dłonie"))
    oferty = [_oferta(1, 10, "Manicure hybrydowy", [["manicure", "hybrydowy", "dłonie"]]),
              _oferta(2, 11, "Hybryda dłonie", [["manicure", "hybrydowy", "dłonie"]]),
              _oferta(3, 12, "Mani klasyczny", [["manicure", "klasyczny", "dłonie"]]),  # niezgodna metoda
              _oferta(4, 99, "Coś innego", [["manicure", "", ""]])]
    wynik = dobor.wybierz("Manicure hybrydowy", k, oferty, k=1, wybrani={99})
    assert [o["service_id"] for o in wynik] == [1, 4]


def test_wybierz_poza_beauty_nic():
    assert dobor.wybierz("Strzyżenie psa", _karta(("usługa dla zwierząt", "", "")), [], k=50) == []


# --- polityka ---

@pytest.mark.parametrize("p,oczekiwane", [
    ([0.02, 0.03, 0.95], "ta_sama"), ([0.05, 0.1, 0.85], "niepewna"), ([0.05, 0.9, 0.05], "odmiana"),
    ([0.85, 0.1, 0.05], "inna"), ([0.5, 0.45, 0.05], "niepewna"),
])
def test_decyzja(p, oczekiwane):
    assert polityka.decyzja(p) == oczekiwane


def _probka(booksy, cena, czas=60):
    return {"service_id": booksy * 10, "booksy_id": booksy, "salon_name": f"s{booksy}", "service_name": "x",
            "price_grosze": cena, "duration_minutes": czas, "is_package": False, "similarity": 0.95}


def test_wynik_rynkowy_mediana_odmiany_i_straznik_ceny():
    subject = {"service_name": "x", "price_grosze": 10000, "duration_minutes": 60, "is_package": False}
    oceny = [(_probka(1, 9000), "ta_sama", None), (_probka(2, 11000), "ta_sama", None),
             (_probka(3, 10000), "ta_sama", None), (_probka(4, 60000), "ta_sama", None),  # ≥5× → powiązana
             (_probka(5, 12000), "odmiana", "metoda"), (_probka(6, 5000), "inna", None)]
    r = polityka.wynik_rynkowy(subject, oceny)
    assert r.n_unique_salons == 3 and r.status == "thin" and r.market_price_grosze == 10000
    assert {x["booksy_id"] for x in r.related_samples} == {4, 5}
    assert {x["related_reason"] for x in r.related_samples} == {"zakres", "metoda"}
    assert r.provenance["matching"] == "bmatch"


def test_wynik_rynkowy_za_malo_salonow_bez_ceny():
    subject = {"service_name": "x", "price_grosze": 10000, "duration_minutes": 60}
    r = polityka.wynik_rynkowy(subject, [(_probka(1, 9000), "ta_sama", None)])
    assert r.status == "insufficient" and r.market_price_grosze is None and len(r.samples) == 1


def test_powod_odmiany():
    assert polityka.powod_odmiany([["mani", "hybryda", ""]], [["mani", "żel", ""]]) == "metoda"
    assert polityka.powod_odmiany([["mani", "hybryda", "dłonie"]], [["mani", "", "stopy"]]) == "zakres"


# --- przełącznik w wycenie: każdy błąd b-match = stary silnik ---

def _rows():
    return [{"treatment_name": "A", "market_median_grosze": 100, "sample_size": 5},
            {"treatment_name": "B", "market_median_grosze": None, "sample_size": 0}]


SUBJ = [{"id": 1, "name": "A", "price_grosze": 100}, {"id": 2, "name": "B", "price_grosze": 200}]


@pytest.mark.asyncio
async def test_blad_bmatch_zostawia_stary_silnik():
    from services.similarity_pricing import report_pricing as rp
    with patch("services.bmatch.wycena.wycen", AsyncMock(side_effect=RuntimeError("punkt padł"))):
        out = await rp._matching_bcard("bcard", MagicMock(), 7, SUBJ, _rows(), [1], set(), {}, None)
    assert out == _rows()


@pytest.mark.asyncio
async def test_bcard_podmienia_tylko_uslugi_z_wynikiem():
    from services.similarity_pricing import report_pricing as rp
    wynik = polityka.wynik_rynkowy({"service_name": "B", "price_grosze": 20000, "duration_minutes": 60},
                                   [(_probka(i, 20000), "ta_sama", None) for i in (1, 2, 3)])
    with patch("services.bmatch.wycena.wycen", AsyncMock(return_value=({2: wynik}, {"czas_s": 1}))):
        out = await rp._matching_bcard("bcard", MagicMock(), 7, SUBJ, _rows(), [1], set(), {}, None)
    assert out[0] == _rows()[0]
    assert out[1]["market_median_grosze"] == 20000 and out[1]["sample_size"] == 3


@pytest.mark.asyncio
async def test_cien_zostawia_stary_i_zapisuje_porownanie():
    from services.similarity_pricing import report_pricing as rp
    wynik = polityka.wynik_rynkowy({"service_name": "B", "price_grosze": 20000, "duration_minutes": 60},
                                   [(_probka(i, 20000), "ta_sama", None) for i in (1, 2, 3)])
    zapis = MagicMock()
    with patch("services.bmatch.wycena.wycen", AsyncMock(return_value=({2: wynik}, {"czas_s": 1}))), \
         patch("services.bmatch.pamiec.zapisz_cien", zapis):
        out = await rp._matching_bcard("bcard_cien", MagicMock(), 7, SUBJ, _rows(), [1], set(), {}, None)
    assert out == _rows()
    wiersze = zapis.call_args.args[1]
    assert [w["nowy_mediana_grosze"] for w in wiersze] == [None, 20000]


def test_domyslnie_stary():
    from config import Settings
    assert Settings().matching_source == "stary"


# --- punkt Runpod: zawsze gaszony, także przy błędzie ---

@pytest.mark.asyncio
async def test_punkt_gasi_pracownika_przy_bledzie():
    from services.bmatch.runpod import Punkt
    p = Punkt("ep", "klucz", "bmatch")
    p._ustaw = AsyncMock()
    with patch("asyncio.sleep", AsyncMock()), pytest.raises(ValueError):
        async with p:
            raise ValueError("coś padło w środku")
    wywolania = [c.kwargs for c in p._ustaw.await_args_list]
    assert wywolania[0] == {"workersMax": 1, "workersMin": 1}
    assert {"workersMin": 0, "workersMax": 0} in wywolania and wywolania[-1] == {"workersMax": 1}


@pytest.mark.asyncio
async def test_maly_brak_kart_nie_rozgrzewa_bcard():
    from services.bmatch import wycena
    uslugi = [{"id": i, "name": f"u{i}", "category_name": "", "description": "", "variants": []} for i in range(100)]
    znane = {klucz_uslugi(u): {"skladniki": [{"zabieg": "manicure"}]} for u in uslugi[:98]}
    with patch.object(wycena.pamiec, "karty", MagicMock(return_value=dict(znane))), \
         patch.object(wycena, "Punkt") as punkt, patch.object(wycena.settings, "bcard_endpoint_id", "ep"):
        karty = await wycena._karty_podmiotu(MagicMock(), uslugi, "")
    punkt.assert_not_called()
    assert len(karty) == 98
