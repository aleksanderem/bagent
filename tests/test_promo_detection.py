"""Promocja w nazwie usługi — przykłady z prawdziwych cenników (skan 10.2026)."""

import pytest

from services.promo_detection import promocja_w_nazwie


@pytest.mark.parametrize(
    "name",
    [
        "LASEROWY LIFTING SKÓRY FOTONA 4D - PROMOCJA  24...",
        "Koktajl MONACO - promocja miesiąca",
        "Depilacja laserowa bikini głębokie -20%",
        "Depilacja laserowa 1 zabieg - 50%",
        "RADIOFREKWENCJA MIKROIGŁOWA TWARZ-50%",
        "Masaż świecą (w październiku-20%)",
        "PAKIET 3 MASAŻY KOBIDO Z AMPUŁKĄ WIT(- 36% zniżki)",
        "PAKIETY 15-20% TANIEJ",
        "🍂JESIENNA PROMOCJA 🍂 rabat nawet do 50%",
        "Promo Ola Combo",
        "PROMO -50% BIKINI PACHWINY Nowy Klient",
    ],
)
def test_promocja_w_nazwie(name):
    assert promocja_w_nazwie(name)


@pytest.mark.parametrize(
    "name",
    [
        "Manicure hybrydowy",
        "Pakiet 5 zabiegów",
        "Kwas migdałowy 20-40%",
        "Light Maskara żagęszczanie rzęs 60-80%",
        "TCA15%-35% + maska",
        "Peeling kwasem 40%",
        "Promień lasera — konsultacja",
        "",
        None,
    ],
)
def test_bez_promocji(name):
    assert not promocja_w_nazwie(name)
