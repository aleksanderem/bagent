"""services/bmatch/konkurenci — dobór konkurentów z kart usług + b-match (MATCHING_SOURCE=bcard)."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.bmatch import konkurenci


def _karta(z, m="", o=""):
    return {"skladniki": [{"zabieg": z, "metoda": m, "obszar": o}]}


def _oferta(sid, b, z, m="", o="", nazwa="x"):
    return {"service_id": sid, "booksy_id": b, "klucz": f"k{sid}", "nazwa": nazwa, "zabiegi": [z],
            "skladniki": [[z, m, o]], "marka": None, "poza_beauty": False}


def test_siec_i_oddzialy():
    assert konkurenci.siec("ESTETICAN PREMIUM® - Mokotów") == konkurenci.siec("Estetican Premium (Wola)")
    assert konkurenci.siec("Beauty4ever - ul. Wołoska 16") == konkurenci.siec("Beauty 4 Ever")
    a = {"name": "ESTETICAN PREMIUM® - Mokotów", "website": "https://www.estetican.pl/mokotow"}
    b = {"name": "Estetican Premium", "website": "http://estetican.pl"}
    assert konkurenci.ta_sama_siec(a, b)
    # ten sam adres (< 1 km) bez wspólnych kontaktów = oddział
    assert konkurenci.ta_sama_siec({"name": "Beauty 4 Ever", "latitude": 52.2, "longitude": 21.0, "website": "a.pl"},
                                   {"name": "Beauty4ever - ul. Wołoska", "latitude": 52.2005, "longitude": 21.0,
                                    "website": "b.pl"})
    # niezależne salony o ogólnej nazwie, daleko, różne kontakty → nie sieć
    assert not konkurenci.ta_sama_siec({"name": "Studio Urody", "instagram_url": "x", "latitude": 52.0, "longitude": 21.0},
                                       {"name": "Studio Urody", "instagram_url": "y", "latitude": 52.3, "longitude": 21.0})


def test_pokrycie_z_kart():
    karty = {1: _karta("manicure", "hybrydowy", "dłonie"), 2: _karta("pedicure")}
    oferty = [_oferta(10, 7, "manicure", "hybrydowy", "dłonie"), _oferta(11, 8, "manicure", "żelowy", "dłonie"),
              _oferta(12, 8, "pedicure", "frezarkowy", "stopy")]
    p = konkurenci.pokrycie(karty, oferty)
    assert p[7] == {1} and p[8] == {2}  # żelowy ≠ hybrydowy; pusta metoda podmiotu pasuje do każdej


def test_pary_najpodobniejsza_oferta_na_usluge():
    uslugi = {1: {"id": 1, "name": "Manicure hybrydowy", "category_name": "", "description": "", "variants": []}}
    karty = {1: _karta("manicure", "hybrydowy")}
    oferty = [_oferta(10, 7, "manicure", "hybrydowy", nazwa="Manicure hybrydowy"),
              _oferta(11, 7, "manicure", "hybrydowy", nazwa="Coś"), _oferta(12, 9, "manicure", "hybrydowy")]
    pary = konkurenci._pary(uslugi, karty, oferty, {7})
    assert [o["service_id"] for _, _, o in pary.values()] == [10]  # tylko salon z listy, 1 najpodobniejsza


@pytest.mark.asyncio
async def test_kolejnosc_z_bmatch_a_bez_karty_z_kart():
    lista = [{"booksy_id": 7, "udzial": 0.6, "pokryte": 6, "name": "Duży katalog"},
             {"booksy_id": 8, "udzial": 0.3, "pokryte": 3, "name": "Prawdziwy rywal"}]
    with patch.object(konkurenci, "kandydaci", MagicMock(return_value=(lista, []))), \
         patch.object(konkurenci, "pokrycie_bmatch", AsyncMock(return_value=({7: {1}, 8: {1, 2, 3}}, {"uslug": 10}))):
        out = await konkurenci.kandydaci_bmatch(MagicMock(), {"booksy_id": 1}, "s", [7, 8])
    assert [x["booksy_id"] for x in out] == [8, 7] and out[0]["udzial"] == 0.3 and out[0]["zrodlo"] == "b-match"
    assert out[0]["pokrycie_kart"] == 0.3 and out[1]["pokrycie_kart"] == 0.6
    with patch.object(konkurenci, "kandydaci", MagicMock(return_value=(lista, []))), \
         patch.object(konkurenci, "pokrycie_bmatch", AsyncMock(side_effect=RuntimeError("brak karty"))):
        out = await konkurenci.kandydaci_bmatch(MagicMock(), {"booksy_id": 1}, "s", [7, 8])
    assert [x["booksy_id"] for x in out] == [7, 8] and out[0]["zrodlo"] == "karty"


@pytest.mark.asyncio
async def test_dobor_z_kart_w_selekcji_koszyki_i_dane_starego_doboru():
    from pipelines import competitor_selection as cs

    stary = cs.CompetitorCandidate(salon_id=5, booksy_id=8, name="R", city="W", primary_category_id=3, reviews_count=100,
                                   reviews_rank=4.9, distance_km=2.0, female_weight_diff=4.0, composite_score=50.0,
                                   bucket="cluster", counts_in_aggregates=True, similarity_scores={"focus_tid_sim": 0.2})
    lista = [{"booksy_id": 8, "salon_id": 5, "name": "R", "city": "W", "reviews_count": 100, "reviews_rank": 4.9,
              "distance_km": 2.1, "pokryte": 30, "wszystkie": 100, "udzial": 0.3, "pokrycie_kart": 0.5, "zrodlo": "b-match"},
             {"booksy_id": 9, "salon_id": 6, "name": "Nowy", "city": "W", "reviews_count": 5, "reviews_rank": 5.0,
              "distance_km": 3.0, "pokryte": 12, "wszystkie": 100, "udzial": 0.12, "zrodlo": "b-match"},
             {"booksy_id": 10, "salon_id": 7, "name": "Słaby", "city": "W", "reviews_count": 50, "reviews_rank": 4.0,
              "distance_km": 1.0, "pokryte": 1, "wszystkie": 100, "udzial": 0.01, "zrodlo": "b-match"}]
    with patch("services.bmatch.konkurenci.kandydaci_bmatch", AsyncMock(return_value=lista)), \
         patch("services.similarity_pricing.report_pricing._geo_competitor_booksy_ids", return_value=[8, 9, 10]):
        out = await cs._dobor_z_kart(MagicMock(), {"booksy_id": 1, "name": "P"}, "s", [stary], 15, 3)
    assert [c.booksy_id for c in out] == [8, 9]  # 1 usługa < MIN_COVERED → poza listą
    assert out[0].bucket == "direct" and out[0].female_weight_diff == 4.0
    assert out[0].similarity_scores["focus_tid_sim"] == 0.2 and out[0].similarity_scores["profile_overlap_sim"] == 0.3
    assert out[1].bucket == "new" and out[1].counts_in_aggregates is False  # < 20 opinii


def test_propozycje_werdykty_z_pamieci_i_szacunek_z_kart():
    # salon 7: werdykt „ta sama” z pamięci; salon 8: bez werdyktu, identyczna karta (0,85); salon 9: tylko zgodna (0,3)
    uslugi = [{"id": 1, "name": "Mani", "category_name": "", "description": "", "variants": [], "price_grosze": 1}]
    karta = _karta("manicure", "hybrydowy", "dłonie")
    lista = [{"booksy_id": b, "udzial": 1.0, "name": str(b)} for b in (7, 8, 9)]
    oferty = [_oferta(10, 7, "manicure", "hybrydowy", "dłonie"), _oferta(11, 8, "manicure", "hybrydowy", "dłonie"),
              _oferta(12, 9, "manicure", "hybrydowy", "")]
    c = MagicMock()
    c.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"id": "s1", "salon_name": "P", "salon_lat": 52.0, "salon_lng": 21.0}]
    from services.bmatch.klucz import klucz_uslugi, para_klucz
    k7 = para_klucz(klucz_uslugi(uslugi[0]), "k10")
    with patch.object(konkurenci, "kandydaci", MagicMock(return_value=(lista, oferty))), \
         patch.object(konkurenci, "_z_ostatniego_raportu", return_value=[]), \
         patch.object(konkurenci, "_uslugi_podmiotu", return_value=uslugi), \
         patch.object(konkurenci.pamiec, "karty", MagicMock(return_value={klucz_uslugi(uslugi[0]): karta})), \
         patch.object(konkurenci.pamiec, "werdykty", MagicMock(return_value={k7: [0.0, 0.0, 1.0]})), \
         patch.object(konkurenci, "_zapisz_propozycje") as zap:
        w = konkurenci.propozycje(c, 1, [7, 8, 9])
    po = {x["booksy_id"]: x for x in w["wyniki"]}
    assert po[7]["udzial"] == 1.0 and po[7]["zrodlo"] == "b-match"
    assert po[8]["udzial"] == 0.85 and po[8]["zrodlo"] == "szacunek"
    assert po[9]["udzial"] == 0.3
    assert [x["booksy_id"] for x in w["wyniki"]] == [7, 8, 9]
    zap.assert_called_once()


def test_endpoint_propozycji_statusy(monkeypatch):
    from fastapi.testclient import TestClient

    import server

    monkeypatch.setattr(server.settings, "api_key", "k")
    klient = TestClient(server.app)
    monkeypatch.setattr(server.settings, "matching_source", "stary")
    r = klient.post("/api/internal/bmatch/propozycje", json={"booksy_id": 1}, headers={"x-api-key": "k"})
    assert r.json() == {"status": "wylaczone"}
