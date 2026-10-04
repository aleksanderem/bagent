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
         patch.object(wycena, "punkt_dostawcy") as punkt, patch.object(wycena, "dostepny", return_value=True):
        karty = await wycena._karty_podmiotu(MagicMock(), uslugi, "")
    punkt.assert_not_called()
    assert len(karty) == 98


# --- nocne odświeżanie ofert (services/bmatch/odswiez.py) ---

def _klient_odswiezania(do):
    c = MagicMock()
    c.rpc.return_value.range.return_value.execute.return_value.data = do
    return c


def _uslugi(n, prefiks="u"):
    return [{"id": i, "name": f"{prefiks}{i}", "category_name": "", "description": "", "variants": []} for i in range(n)]


@pytest.mark.asyncio
async def test_odswiez_zapisuje_oferty_ze_znanych_kart_bez_rozgrzewania():
    from datetime import datetime, timezone

    from services.bmatch import odswiez
    us = _uslugi(10)
    znane = {klucz_uslugi(u): {"skladniki": [{"zabieg": "manicure"}]} for u in us[:9]}
    c = _klient_odswiezania([{"booksy_id": 7, "scrape_id": "s7"}])
    zapisane = []
    with patch.object(odswiez.oferty, "uslugi_skanu", MagicMock(return_value=us)), \
         patch.object(odswiez.pamiec, "karty", MagicMock(return_value=dict(znane))), \
         patch.object(odswiez.oferty, "zapisz", MagicMock(side_effect=lambda _c, b, w: zapisane.append((b, w)))), \
         patch.object(odswiez, "punkt_dostawcy") as punkt, patch.object(odswiez, "dostepny", return_value=True):
        stat = await odswiez.odswiez(c, teraz=datetime(2026, 10, 5, tzinfo=timezone.utc))  # poniedziałek
    punkt.assert_not_called()  # 1 brak < KART_MIN
    assert stat["brakow"] == 1 and stat["salonow"] == 1
    assert zapisane[0][0] == 7 and len(zapisane[0][1]) == 9
    stan = c.table.return_value.upsert.call_args.args[0]
    assert stan["scrape_id"] == "s7" and stan["ofert"] == 9 and stan["bez_karty"] == 1


@pytest.mark.asyncio
async def test_odswiez_niedziela_generuje_brakujace_karty():
    from datetime import datetime, timezone

    from services.bmatch import odswiez
    us = _uslugi(3)
    c = _klient_odswiezania([{"booksy_id": 7, "scrape_id": "s7"}])
    p = MagicMock(sekundy=12.0)
    p.__aenter__ = AsyncMock(return_value=p)
    p.__aexit__ = AsyncMock(return_value=None)
    p.karty = AsyncMock(return_value=[{"skladniki": [{"zabieg": "manicure"}]}, None, {"skladniki": []}])
    with patch.object(odswiez.oferty, "uslugi_skanu", MagicMock(return_value=us)), \
         patch.object(odswiez.pamiec, "karty", MagicMock(return_value={})), \
         patch.object(odswiez.pamiec, "zapisz_karty") as zk, patch.object(odswiez.oferty, "zapisz") as zo, \
         patch.object(odswiez, "punkt_dostawcy", MagicMock(return_value=p)), \
         patch.object(odswiez, "dostepny", return_value=True):
        stat = await odswiez.odswiez(c, teraz=datetime(2026, 10, 4, tzinfo=timezone.utc))  # niedziela
    assert stat["kart_nowych"] == 2 and stat["gpu_bcard_s"] == 12.0
    assert len(zk.call_args.args[1]) == 2
    assert len(zo.call_args.args[2]) == 2  # usługa bez karty (None) nie trafia do ofert


@pytest.mark.asyncio
async def test_odswiez_sucho_nic_nie_zapisuje():
    from services.bmatch import odswiez
    c = _klient_odswiezania([{"booksy_id": 7, "scrape_id": "s7"}])
    with patch.object(odswiez.oferty, "uslugi_skanu", MagicMock(return_value=_uslugi(500))), \
         patch.object(odswiez.pamiec, "karty", MagicMock(return_value={})), \
         patch.object(odswiez.oferty, "zapisz") as zo, patch.object(odswiez, "punkt_dostawcy") as punkt:
        stat = await odswiez.odswiez(c, sucho=True)
    assert stat["brakow"] == 500
    zo.assert_not_called()
    punkt.assert_not_called()
    c.table.assert_not_called()


@pytest.mark.asyncio
async def test_odswiez_cron_przy_starym_silniku_nic_nie_robi():
    from services.bmatch import odswiez
    with patch.object(odswiez.settings, "matching_source", "stary"), patch.object(odswiez, "odswiez") as o:
        assert await odswiez.odswiez_cron({}) == {"pominiete": "stary"}
    o.assert_not_called()


def test_odczyty_pamieci_w_porcjach_mieszczacych_sie_w_adresie():
    """Lista w adresie zapytania > ~6 KB = 414 na prod (500 kluczy kart / 500 par padało, 300 / 150 przechodzi)."""
    from services.bmatch import pamiec
    c = MagicMock()
    for f in ("table", "select", "in_", "eq"):
        getattr(c, f).return_value = c
    c.execute.return_value.data = []
    pamiec.karty(c, {f"{i:016x}" for i in range(450)})
    pamiec.werdykty(c, {f"{i:016x}|{i:016x}" for i in range(450)}, "v")
    pamiec.uslugi(c, list(range(700)))
    rozmiary = [len(call.args[1]) for call in c.in_.call_args_list]
    assert max(rozmiary) <= 300 and len(rozmiary) == 3 + 5 + 3


@pytest.mark.asyncio
async def test_wybrany_poza_doborem_liczy_sie_do_pokrycia_ale_nie_do_mediany():
    """Salon dołożony ręcznie (counts_in_aggregates=False) był wycinany z wyceny w całości → pokrycie oferty 0%
    (JetSet u Beauty4ever, 2026-10-03). Teraz b-match go porównuje, ale jego ceny nie wchodzą do mediany."""
    from services.bmatch import wycena
    u = {"id": 1, "name": "Botoks", "category_name": "", "description": "", "variants": [], "price_grosze": 100000}
    karta = {"skladniki": [{"zabieg": "toksyna botulinowa"}]}
    oferty = [{"service_id": 10 + b, "booksy_id": b, "klucz": f"k{b}", "nazwa": "Botoks", "zabiegi": ["toksyna botulinowa"],
               "skladniki": [["toksyna botulinowa", "", ""]], "marka": None} for b in (1, 2, 3, 99)]
    dane = {10 + b: {"id": 10 + b, "name": "Botoks", "price_grosze": 100000 if b != 99 else 900000, "is_active": True}
            for b in (1, 2, 3, 99)}
    werdykt = [0.01, 0.01, 0.98]
    punkt = MagicMock(sekundy=0.0)
    punkt.gotowy = AsyncMock()
    punkt.werdykty = AsyncMock(side_effect=lambda wej: [werdykt for _ in wej])
    kandydaci = MagicMock(return_value=oferty)
    with patch.object(wycena.pamiec, "karty", MagicMock(side_effect=lambda c, k: {x: karta for x in k})), \
         patch.object(wycena.pamiec, "kandydaci", kandydaci), \
         patch.object(wycena.pamiec, "uslugi", MagicMock(return_value=dane)), \
         patch.object(wycena.pamiec, "werdykty", MagicMock(return_value={})), \
         patch.object(wycena.pamiec, "zapisz_werdykty"):
        wyniki, stat = await wycena._wycen(punkt, MagicMock(), [u], [1, 2, 3], {1, 2, 3}, {}, None, "", {},
                                           frozenset({99}))
    assert 99 in kandydaci.call_args.args[1]
    assert {x["booksy_id"] for x in stat["pokrycie"][1]} == {1, 2, 3, 99}
    assert all(s["booksy_id"] != 99 for s in wyniki[1].samples + wyniki[1].related_samples)
    assert wyniki[1].p50_grosze == 100000 and wyniki[1].n_unique_salons == 3


# --- kolejka dostawców (services/bmatch/dostawcy.py) ---

def _punkt_atrapa(wstaje: bool, sek: float = 1.0):
    from services.bmatch.runpod import BladPunktu
    p = MagicMock(sekundy=sek)
    p.__aenter__ = AsyncMock(return_value=p)
    p.__aexit__ = AsyncMock(return_value=None)
    p.gotowy = AsyncMock(return_value=None) if wstaje else AsyncMock(side_effect=BladPunktu("brak karty"))
    p.werdykty = AsyncMock(return_value=[[0.0, 0.0, 1.0]])
    return p


@pytest.mark.asyncio
async def test_lancuch_przechodzi_do_nastepnego_gdy_pierwszy_nie_wstaje():
    from services.bmatch.dostawcy import Lancuch
    modal, runpod = _punkt_atrapa(False), _punkt_atrapa(True)
    async with Lancuch([("modal", lambda: modal), ("runpod", lambda: runpod)], 5) as ln:
        assert await ln.werdykty([([], [])]) == [[0.0, 0.0, 1.0]]
    assert ln.dostawca == "runpod" and ln.proby[0].startswith("modal: ")
    runpod.werdykty.assert_awaited_once()
    modal.__aexit__.assert_awaited_once()  # nieudany też jest zamykany (Runpod: gaszenie pracownika)
    runpod.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_lancuch_nie_budzi_zapasowego_gdy_pierwszy_wstaje():
    from services.bmatch.dostawcy import Lancuch
    modal = _punkt_atrapa(True)
    zapas = MagicMock()
    async with Lancuch([("modal", lambda: modal), ("runpod", zapas)], 5) as ln:
        await ln.gotowy()
    zapas.assert_not_called()
    assert ln.dostawca == "modal"


@pytest.mark.asyncio
async def test_lancuch_nikt_nie_wstaje_to_blad_i_stary_silnik():
    from services.bmatch.dostawcy import Lancuch
    from services.bmatch.runpod import BladPunktu
    with pytest.raises(BladPunktu, match="żaden dostawca"):
        async with Lancuch([("modal", lambda: _punkt_atrapa(False)), ("runpod", lambda: _punkt_atrapa(False))], 5) as ln:
            await ln.gotowy()


def test_kolejnosc_i_pomijanie_nieskonfigurowanych():
    from services.bmatch import dostawcy
    s = dostawcy.settings
    with patch.object(s, "bmatch_dostawcy", "modal,runpod"), patch.object(s, "modal_bmatch_token", ""), \
         patch.object(s, "runpod_api_key", "k"), patch.object(s, "bmatch_endpoint_id", "ep"):
        assert dostawcy.dostawcy("bmatch") == ["runpod"]  # Modal bez tokenu pominięty
    with patch.object(s, "bmatch_dostawcy", "modal,runpod"), patch.object(s, "modal_bmatch_token", "t"), \
         patch.object(s, "runpod_api_key", ""):
        assert dostawcy.dostawcy("bcard") == ["modal"]
