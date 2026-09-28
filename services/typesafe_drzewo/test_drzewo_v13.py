"""Drzewo v13 — kontrakt przejścia i porównania (model Alexa 28.09 + cookbook hierarchical_classification).

Ta sama usługa = ten sam węzeł zabiegu i te same poziomy 3–5; węzeł zabiegu może mieć kilku rodziców
(pedicure pod paznokciami i stopami). „inne” i „ogolnie” nigdy nie dają „ta sama”. Atrapa klienta — zero sieci.
"""
from __future__ import annotations

from types import SimpleNamespace

from services.typesafe_drzewo.drzewo_v13 import INNE, NIE_DOTYCZY, OGOLNIE, porownaj_v13, wiazka_v13

DRZEWO = {
    "wersja": 13, "dziedziny": ["paznokci", "stóp", "rzęs"],
    "dziedziny_opis": {"paznokci": {"miejsca": ["paznokci"]}, "stóp": {"miejsca": ["pięt"]}, "rzęs": {"miejsca": ["rzęs"]}},
    "zabiegi": {
        "paznokci|pedicure": {"etykieta": "pedicure", "rodzice": ["paznokci", "stóp"], "synonimy": ["pedicure"], "n": 50,
                              "examples": ["Pedicure"], "metoda": {"hybrydowy": {"n": 20}, "podologiczny": {"n": 10}},
                              "gdzie": {}, "etap": {}},
        "rzęs|przedłużanie": {"etykieta": "przedłużanie", "rodzice": ["rzęs"], "synonimy": ["przedłużanie"], "n": 40,
                              "examples": ["Przedłużanie rzęs"], "metoda": {}, "gdzie": {"2:1": {"n": 10}, "1:1": {"n": 10}},
                              "etap": {"uzupełnienie": {"n": 20}}},
        "paznokci|przedłużanie": {"etykieta": "przedłużanie", "rodzice": ["paznokci"], "synonimy": ["przedłużanie"], "n": 30,
                                  "examples": ["Przedłużanie paznokci"], "metoda": {"żelowe": {"n": 20}}, "gdzie": {}, "etap": {}},
    },
}


def _odp(choices: dict[str, dict[str, float]], nouls: dict[str, float] | None = None):
    return SimpleNamespace(choices={k: SimpleNamespace(choice=max(p, key=p.get), probabilities=p) for k, p in choices.items()},
                           nouls={k: SimpleNamespace(noul=v) for k, v in (nouls or {}).items()},
                           usage=SimpleNamespace(input_tokens=100))


class _Klient:
    def __init__(self, rozklady: dict[str, dict[str, float]]):
        self.rozklady, self.wywolania = rozklady, []

    async def system_one(self, state, questions, model=None):
        self.wywolania.append(questions)
        return _odp({k: self.rozklady[k] for k in questions if k in self.rozklady},
                    {k: 0.05 for k in ("zestaw", "rozszerzenie") if k in questions})


async def test_trzy_wywolania_i_sciezka_przez_dziedzine():
    k = _Klient({"pozycja": {"zabieg": 0.95, "dodatek": 0.05}, "dziedzina": {"stóp": 0.7, "paznokci": 0.3},
                 "z|stóp": {"paznokci|pedicure": 0.9, OGOLNIE: 0.1}, "z|paznokci": {"paznokci|pedicure": 0.8, "paznokci|przedłużanie": 0.2},
                 "metoda|paznokci|pedicure": {"podologiczny": 0.8, "hybrydowy": 0.2}})
    rek = await wiazka_v13(k, {}, DRZEWO, [0])
    assert len(k.wywolania) == 3
    assert rek["sciezki"][0][0] == ["stóp", "paznokci|pedicure", "podologiczny", NIE_DOTYCZY, NIE_DOTYCZY]
    assert "gdzie|paznokci|pedicure" not in k.wywolania[2]  # poziom bez opcji = nie dotyczy, bez pytania


def rek(sciezka: list, poz: str = "zabieg", **dod) -> dict:
    return {"pozycja": {poz: 0.95}, "sciezki": [[sciezka, 0.8, 0.9]], "zestaw": 0.05, "rozszerzenie": 0.05, **dod}


PED_STOPY = ["stóp", "paznokci|pedicure", "podologiczny", NIE_DOTYCZY, NIE_DOTYCZY]
PED_PAZN = ["paznokci", "paznokci|pedicure", "podologiczny", NIE_DOTYCZY, NIE_DOTYCZY]


def test_ten_sam_wezel_zabiegu_przez_inna_dziedzine_to_ta_sama_usluga():
    assert porownaj_v13(rek(PED_STOPY), rek(PED_PAZN)) == ("tozsame", "ta sama ścieżka", 5)


def test_ta_sama_nazwa_inny_wezel_to_inny_zabieg():
    a = rek(["rzęs", "rzęs|przedłużanie", NIE_DOTYCZY, "2:1", "uzupełnienie"])
    b = rek(["paznokci", "paznokci|przedłużanie", "żelowe", NIE_DOTYCZY, NIE_DOTYCZY])
    assert porownaj_v13(a, b) == ("rozne", "inny zabieg", 0)


def test_inna_metoda_to_podobna():
    b = rek(["paznokci", "paznokci|pedicure", "hybrydowy", NIE_DOTYCZY, NIE_DOTYCZY])
    assert porownaj_v13(rek(PED_STOPY), b) == ("powiazane", "inny poziom: metoda", 2)


def test_inne_po_obu_stronach_i_ogolnie_kontra_konkret_to_niepelne():
    a = rek(["rzęs", "rzęs|przedłużanie", NIE_DOTYCZY, INNE, "uzupełnienie"])
    assert porownaj_v13(a, a)[:2] == ("niepelne", "gdzie i ile spoza listy")
    b = rek(["rzęs", "rzęs|przedłużanie", NIE_DOTYCZY, OGOLNIE, "uzupełnienie"])
    assert porownaj_v13(b, rek(["rzęs", "rzęs|przedłużanie", NIE_DOTYCZY, "2:1", "uzupełnienie"]))[:2] == ("niepelne", "gdzie i ile nieustalony")
    assert porownaj_v13(rek(["rzęs", OGOLNIE, NIE_DOTYCZY, NIE_DOTYCZY, NIE_DOTYCZY]), b)[:2] == ("niepelne", "zabieg nieustalony")


def test_obie_bez_okreslenia_to_ten_sam_lisc():
    # „Manicure hybrydowy” w dwóch salonach: żaden nie podaje długości ani etapu → ta sama ścieżka
    a = rek(["paznokci", "paznokci|pedicure", "hybrydowy", OGOLNIE, OGOLNIE])
    assert porownaj_v13(a, rek(list(a["sciezki"][0][0]))) == ("tozsame", "ta sama ścieżka", 5)


def test_produkty_pakiety_i_dodatki():
    assert porownaj_v13(rek(PED_STOPY, "produkt"), rek(PED_STOPY))[:2] == ("rozne", "nie porównujemy: produkt")
    assert porownaj_v13(rek(PED_STOPY, "pakiet"), rek(PED_STOPY))[:2] == ("powiazane", "pakiet i pojedyncza wizyta")
    assert porownaj_v13(rek(PED_STOPY, rozszerzenie=0.8), rek(PED_STOPY))[:2] == ("powiazane", "dodatek")


def test_brak_reguly_na_nazwach():
    # identyczne nazwy nie wchodzą do porównania — decyduje wyłącznie ścieżka w drzewie
    import inspect

    from services.typesafe_drzewo import drzewo_v13
    assert "nazwa" not in inspect.signature(drzewo_v13.porownaj_v13).parameters
