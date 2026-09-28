"""Klasyfikacja po drzewie-podziale (v8) i reguła rodziny (services/typesafe_drzewo/podzial.py).

Kontrakt: odmiana tylko przy wyraźnej większości (inaczej poziom ogólny);
rodzaj ogólny vs jego odmiana → niepełne; dwie odmiany tego samego korzenia →
powiązane; różne korzenie → różne. Atrapa klienta SDK — zero sieci.
"""
from __future__ import annotations

from types import SimpleNamespace

from services.typesafe_drzewo.podzial import dzieci, porownaj_v8, rodzaj_v8

PODZIAL = {
    "kanoniczne": ["manicure", "manicure hybrydowy", "manicure klasyczny", "masaż", "depilacja"],
    "rodzic": {"manicure hybrydowy": "manicure", "manicure klasyczny": "manicure"},
    "korzen": {"manicure": "manicure", "manicure hybrydowy": "manicure", "manicure klasyczny": "manicure",
               "masaż": "masaż", "depilacja": "depilacja"},
    "branze": {"Paznokcie": ["manicure"], "Masaż": ["masaż"], "Salon Kosmetyczny": ["depilacja", "manicure"]},
    "przyklady": {"manicure": ["Manicure"], "manicure hybrydowy": ["Manicure hybrydowy"], "masaż": ["Masaż klasyczny"]},
    "liczn": {"manicure": 100, "manicure hybrydowy": 80, "manicure klasyczny": 50, "masaż": 70, "depilacja": 40},
    "cechy": {},
}


class _Wybor(SimpleNamespace):
    pass


def _odp(choices: dict[str, tuple[str, dict]]):
    return SimpleNamespace(
        choices={k: _Wybor(choice=c, probabilities=p, confidence=max(p.values())) for k, (c, p) in choices.items()},
        usage=SimpleNamespace(input_tokens=100),
    )


class _Klient:
    def __init__(self, odmiana: tuple[str, float]):
        self.odmiana = odmiana
        self.pytania: list[dict] = []

    async def system_one(self, state, questions, model=None):
        self.pytania.append(questions)
        if "dziedzina" in questions:
            return _odp({"dziedzina": ("Paznokcie", {"Paznokcie": 0.9, "Masaż": 0.1})})
        if "o" in questions:
            c, p = self.odmiana
            return _odp({"o": (c, {c: p, "ogolnie": 1 - p})})
        return _odp({k: ("manicure", {"manicure": 0.95, "inny": 0.05}) for k in questions})


def u(rodzaj: str) -> dict:
    return {"rodzaj": rodzaj, "cechy": {rodzaj: {}}, "liczba_zabiegow": 1, "ilosc": None}


def test_dzieci_korzenia():
    assert dzieci(PODZIAL) == {"manicure": ["manicure hybrydowy", "manicure klasyczny"]}


async def test_odmiana_przy_wyraznej_wiekszosci():
    r = await rodzaj_v8(_Klient(("manicure hybrydowy", 0.8)), {}, PODZIAL, dzieci(PODZIAL), [0])
    assert (r["korzen"], r["rodzaj"]) == ("manicure", "manicure hybrydowy")


async def test_bez_wyraznej_odmiany_zostaje_poziom_ogolny():
    r = await rodzaj_v8(_Klient(("manicure hybrydowy", 0.45)), {}, PODZIAL, dzieci(PODZIAL), [0])
    assert r["rodzaj"] == "manicure"


async def test_opcje_korzenia_pokazuja_odmiany():
    k = _Klient(("ogolnie", 0.9))
    await rodzaj_v8(k, {}, PODZIAL, dzieci(PODZIAL), [0])
    opcje = k.pytania[1]["k|Paznokcie"].criteria
    assert opcje["manicure"]["odmiany"] == ["manicure hybrydowy", "manicure klasyczny"]
    assert "inny" in opcje


def test_ogolny_vs_odmiana_to_niepelne():
    assert porownaj_v8(u("manicure"), u("manicure hybrydowy"), PODZIAL)[0] == "niepelne"


def test_dwie_odmiany_tego_samego_korzenia_to_powiazane():
    assert porownaj_v8(u("manicure hybrydowy"), u("manicure klasyczny"), PODZIAL) == ("powiazane", "ta sama rodzina")


def test_rozne_korzenie_to_rozne():
    assert porownaj_v8(u("masaż"), u("depilacja"), PODZIAL)[0] == "rozne"


def test_ten_sam_rodzaj_bez_cech_to_ta_sama():
    assert porownaj_v8(u("masaż"), u("masaż"), PODZIAL)[0] == "tozsame"


def f(nazwa: str) -> dict:
    return {"nazwa": nazwa}


def test_identyczna_nazwa_w_rodzinie_to_ta_sama():
    assert porownaj_v8(u("manicure hybrydowy"), u("manicure klasyczny"), PODZIAL,
                       f("Manicure  męski"), f("manicure męski.")) == ("tozsame", "identyczna nazwa")


def test_identyczna_nazwa_z_roznych_rodzin_nie_przesadza():
    assert porownaj_v8(u("masaż"), u("depilacja"), PODZIAL, f("Botoks"), f("Botoks"))[0] == "rozne"


def test_zestaw_albo_dodatek_po_jednej_stronie_to_powiazane():
    a, b = {**u("masaż"), "zestaw": 0.9, "rozszerzenie": 0.1}, {**u("masaż"), "zestaw": 0.1, "rozszerzenie": 0.1}
    assert porownaj_v8(a, b, PODZIAL) == ("powiazane", "zestaw")
    a2, b2 = {**u("masaż"), "zestaw": 0.1, "rozszerzenie": 0.95}, {**u("masaż"), "zestaw": 0.1, "rozszerzenie": 0.05}
    assert porownaj_v8(a2, b2, PODZIAL) == ("powiazane", "dodatek")
