"""Testy klasyfikacji kategorii neutralnych — na realnych kategoriach z prod."""
from __future__ import annotations

import pytest

from .layer_neutral import classify_neutral_category, is_neutral_category


@pytest.mark.parametrize("cat", [
    "Promocje", "Promocja", "promocje czerwiec", "Oferta specjalna", "Oferta miesiąca",
    "Popularne usługi", "Popularne", "Bestsellery", "Bestseller", "Nowości",
    "Polecane zabiegi", "Aktualne promocje", "Wiosenne promocje", "oferta",
    "Promocja czerwiec 12 zab w cenie 9 endermologia",  # mocny marker > domena
    "signature collections nasze bestsellery",
])
def test_event_categories(cat):
    assert classify_neutral_category(cat) == "event", cat


@pytest.mark.parametrize("cat", [
    "dla niej", "dla niego", "Dla Niej i Dla Niego",
])
def test_event_weak_bare(cat):
    assert classify_neutral_category(cat) == "event", cat


@pytest.mark.parametrize("cat", [
    "Konsultacja", "Konsultacje", "Pakiet", "Pakiety", "Voucher", "Vouchery",
    "Bon podarunkowy", "Diagnostyka", "Pierwsza wizyta", "Pakiety usług",
    "Pakiety zabiegowe", "Pakiety zabiegów", "Konsultacje kosmetologiczne",
])
def test_service_categories(cat):
    # "Konsultacje kosmetologiczne" — uwaga: 'kosmetolog' to domena => None.
    # ujmujemy tylko gołe formy; sprawdzamy resztę osobno niżej.
    if cat == "Konsultacje kosmetologiczne":
        assert classify_neutral_category(cat) is None
    else:
        assert classify_neutral_category(cat) == "service", cat


@pytest.mark.parametrize("cat", [
    "Konsultacja kosmetologiczna",      # ma domenę (kosmetolog)
    "Pakiety depilacji laserowej",      # ma domenę (depilacja/laser)
    "Pakiety depilacji laserowej diodowy laser aspiro",
    "depilacja laserowa dla niej",      # domena + słaby marker niegoły
    "makijaż okazjonalny",              # 'okazjonalny' to typ, nie 'okazja'; makijaż domena
])
def test_not_neutral_has_domain(cat):
    assert classify_neutral_category(cat) is None, cat


@pytest.mark.parametrize("cat", [
    "Manicure", "Pedicure", "Depilacja laserowa", "Medycyna estetyczna",
    "Stymulatory tkankowe", "Mezoterapia igłowa", "Zabiegi na twarz",
    "Stylizacja rzęs", "Podologia", "Masaż",
])
def test_domain_categories_are_none(cat):
    assert classify_neutral_category(cat) is None, cat


def test_empty_and_garbage():
    assert classify_neutral_category(None) is None
    assert classify_neutral_category("") is None
    assert classify_neutral_category("🌸🌸🌸") is None
    assert classify_neutral_category("—") is None


def test_is_neutral_shortcut():
    assert is_neutral_category("Promocje") is True
    assert is_neutral_category("Konsultacja") is True
    assert is_neutral_category("Manicure") is False
