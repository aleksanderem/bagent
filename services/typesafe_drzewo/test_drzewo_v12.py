"""Drzewo v12 — przejście po poziomach i poziom pokrycia ścieżek (model tej samej usługi, 28.09).

Kontrakt: dziedzina → zabieg → metoda → gdzie i ile (obszar, wielkość, dla kogo, cel) → etap;
„ta sama” = ten sam konkretny liść; inaczej werdykt z poziomu, na którym ścieżki się rozchodzą;
produkty, dodatki i informacje nie są porównywane. Atrapa klienta — zero sieci.
"""
from __future__ import annotations

from types import SimpleNamespace

from services.typesafe_drzewo.drzewo_v12 import FACETY, NIE_DOTYCZY, OGOLNIE, glebokosc, p_ta_sama, porownaj_v12, wiazka_v12

PUSTE = {w: {} for w in FACETY}
DRZEWO = {
    "wersja": "12b",
    "grupy": {},
    "zabiegi": {
        "brwi i rzęsy": {
            "przedłużanie": {"n": 40, "synonimy": ["przedłużanie"], "examples": ["Przedłużanie rzęs 1:1"], **PUSTE,
                             "wielkosc": {"1:1": {"n": 30, "examples": ["Przedłużanie rzęs 1:1"]},
                                          "2:1": {"n": 25, "examples": ["Przedłużanie rzęs 2:1"]}},
                             "etap": {"uzupełnienie": {"n": 30, "examples": ["Uzupełnienie rzęs 1:1"]}}},
            "henna": {"n": 50, "synonimy": ["henna"], "examples": ["Henna brwi"], **PUSTE,
                      "metoda": {"pudrowa": {"n": 20, "examples": ["Henna pudrowa"]}},
                      "obszar": {"brwi": {"n": 40, "examples": ["Henna brwi"]}, "rzęs": {"n": 30, "examples": ["Henna rzęs"]}}},
        },
    },
}


def _odp(choices: dict[str, dict[str, float]], nouls: dict[str, float] | None = None):
    return SimpleNamespace(
        choices={k: SimpleNamespace(choice=max(p, key=p.get), probabilities=p) for k, p in choices.items()},
        nouls={k: SimpleNamespace(noul=v) for k, v in (nouls or {}).items()},
        usage=SimpleNamespace(input_tokens=100),
    )


class _Klient:
    def __init__(self, rozklady: dict[str, dict[str, float]]):
        self.rozklady = rozklady
        self.wywolania: list[dict] = []

    async def system_one(self, state, questions, model=None):
        self.wywolania.append(questions)
        return _odp({k: self.rozklady[k] for k in questions if k in self.rozklady},
                    {k: 0.05 for k in ("zestaw", "rozszerzenie") if k in questions})


P01_RZESY = {"pozycja": {"zabieg": 0.95, "dodatek": 0.05}, "dziedzina": {"brwi i rzęsy": 0.97, "depilacja": 0.03}}


async def test_dwa_wywolania_z_gotowym_rozkladem_i_wymiary_osobno():
    k = _Klient({"z|brwi i rzęsy": {"przedłużanie": 0.9, "henna": 0.05, OGOLNIE: 0.05},
                 "wielkosc|brwi i rzęsy|przedłużanie": {"2:1": 0.8, "1:1": 0.1, OGOLNIE: 0.1},
                 "etap|brwi i rzęsy|przedłużanie": {"uzupełnienie": 0.85, OGOLNIE: 0.15},
                 "obszar|brwi i rzęsy|henna": {"brwi": 0.9, OGOLNIE: 0.1},
                 "metoda|brwi i rzęsy|henna": {"pudrowa": 0.5, OGOLNIE: 0.5}})
    rek = await wiazka_v12(k, {}, DRZEWO, [0], p01=P01_RZESY)
    assert rek["sciezki"][0][0] == ["brwi i rzęsy", "przedłużanie", NIE_DOTYCZY, NIE_DOTYCZY, "2:1", NIE_DOTYCZY, NIE_DOTYCZY, "uzupełnienie"]
    assert len(k.wywolania) == 2  # zabieg (+zestaw, dodatek) i wszystkie wymiary razem
    assert "zestaw" in k.wywolania[0] and "etap|brwi i rzęsy|przedłużanie" in k.wywolania[1]
    assert "obszar|brwi i rzęsy|przedłużanie" not in k.wywolania[1]  # wymiar bez opcji = nie dotyczy, bez pytania
    z = {tuple(x[:2]): x for x in rek["zabiegi"]}
    assert z[("brwi i rzęsy", "przedłużanie")][3]["obszar"] is None


def s(z="przedłużanie", metoda=NIE_DOTYCZY, obszar=NIE_DOTYCZY, wielkosc="2:1", dla=NIE_DOTYCZY, cel=NIE_DOTYCZY, etap="uzupełnienie"):
    return ["brwi i rzęsy", z, metoda, obszar, wielkosc, dla, cel, etap]


def rek(sciezka: list, p: float = 0.9, poz: str = "zabieg", **dod) -> dict:
    g, z, *w = sciezka
    rozk = {f: (None if v == NIE_DOTYCZY else {v: p, "inne": round(1 - p, 4)}) for f, v in zip(FACETY, w)}
    return {"pozycja": {poz: 0.95}, "zabiegi": [[g, z, p, rozk]], "sciezki": [[sciezka, p, p]],
            "zestaw": 0.05, "rozszerzenie": 0.05, **dod}


def test_poziom_pokrycia_liczy_poziomy_modelu():
    assert glebokosc(tuple(s()), tuple(s())) == 5
    assert glebokosc(tuple(s()), tuple(s(etap=OGOLNIE))) == 4
    assert glebokosc(tuple(s()), tuple(s(wielkosc="1:1"))) == 3  # „gdzie i ile” to jeden poziom modelu
    assert glebokosc(tuple(s()), tuple(s(z="henna"))) == 1


def test_ta_sama_sciezka_to_ta_sama_usluga():
    w = porownaj_v12(rek(s()), rek(s(), 0.75), {"nazwa": "Uzupełnienie rzęs 2:1"}, {"nazwa": "Uzup. 2D"})
    assert w == ("tozsame", "ta sama ścieżka", 5)


def test_inna_wielkosc_to_powiazane_z_nazwa_wymiaru():
    assert porownaj_v12(rek(s()), rek(s(wielkosc="1:1"))) == ("powiazane", "inny poziom: wielkość", 3)


def test_ogolnie_kontra_konkret_to_niepelne():
    assert porownaj_v12(rek(s()), rek(s(etap=OGOLNIE)))[:2] == ("niepelne", "etap nieustalony")


def test_inny_zabieg_to_rozne():
    assert porownaj_v12(rek(s()), rek(s(z="henna", obszar="brwi", wielkosc=NIE_DOTYCZY, etap=NIE_DOTYCZY))) == ("rozne", "inny zabieg", 1)


def test_nieustalony_zabieg_to_niepelne():
    assert porownaj_v12(rek(s()), rek(s(z=OGOLNIE)))[:2] == ("niepelne", "zabieg nieustalony")


def test_produktow_i_dodatkow_nie_porownujemy():
    assert porownaj_v12(rek(s(), poz="produkt"), rek(s()))[0] == "rozne"
    assert porownaj_v12(rek(s(), poz="dodatek"), rek(s()))[1] == "nie porównujemy: dodatek"


def test_pakiet_kontra_pojedyncza_wizyta():
    assert porownaj_v12(rek(s(), poz="pakiet"), rek(s()))[:2] == ("powiazane", "pakiet i pojedyncza wizyta")


def test_dodatek_po_jednej_stronie_to_powiazane():
    assert porownaj_v12(rek(s(), rozszerzenie=0.8), rek(s()))[:2] == ("powiazane", "dodatek")


def test_p_ta_sama_to_iloczyn_krawedzi_i_zgodnosci_wymiarow():
    assert p_ta_sama(rek(s()), rek(s())) == round(0.81 * 0.82 * 0.82, 4)  # dwa wymiary z opcjami
    assert p_ta_sama(rek(s()), rek(s(z="henna"))) == 0.0
