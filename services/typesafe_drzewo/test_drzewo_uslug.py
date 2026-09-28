"""Drzewo usług v11 (services/typesafe_drzewo/drzewo_uslug.py) — jak cookbook hierarchical_classification.

Kontrakt: każdy węzeł = jedno pytanie o bezpośrednie dzieci (+ „ogolnie” poniżej grup);
wiązka trzyma 3 ścieżki na każdym poziomie i naprawia niejasną decyzję na górze; ścieżki
porównuje średnia geometryczna; „ta sama” = ten sam konkretny liść z p ≥ 0,5, potem cechy.
Atrapa klienta — zero sieci.
"""
from __future__ import annotations

from types import SimpleNamespace

from services.typesafe_drzewo.drzewo_uslug import (
    OGOLNIE, dzieci, p_ten_sam, porownaj_v11, pytanie, relacja_drzewa, rozklad, wiazka,
)

DRZEWO = {
    "wersja": 11,
    "grupy": {
        "paznokcie": {"what": "manicure i pedicure", "not_for": "leczenie stóp", "examples": ["manicure hybrydowy"]},
        "brwi i rzęsy": {"what": "henna, rzęsy", "not_for": "makijaż permanentny", "examples": ["henna brwi"]},
        "stopy — podologia": {"what": "leczenie stóp", "not_for": "pedicure kosmetyczny", "examples": ["klamra"]},
    },
    "zabiegi": {
        "paznokcie": {
            "przedłużanie": {"n": 50, "examples": ["przedłużanie paznokci żelem"], "odmiany": {}},
            "manicure": {"n": 90, "examples": ["manicure"], "odmiany": {
                "manicure hybrydowy": {"n": 60, "examples": ["manicure hybrydowy"]},
                "manicure klasyczny": {"n": 20, "examples": ["manicure klasyczny"]}}},
        },
        "brwi i rzęsy": {"przedłużanie": {"n": 40, "examples": ["przedłużanie rzęs 1:1"], "odmiany": {}}},
        "stopy — podologia": {},
    },
}
PODZIAL_CECH = {"kanoniczne": ["manicure hybrydowy", "przedłużanie"], "rodzic": {}, "korzen": {},
                "cechy": {"manicure hybrydowy": {"etap": {"wartosci": ["założenie", "zdjęcie"], "domyslna": None}}}}


def _odp(rozklady: dict[str, dict[str, float]]):
    return SimpleNamespace(
        choices={k: SimpleNamespace(choice=max(p, key=p.get), probabilities=p) for k, p in rozklady.items()},
        usage=SimpleNamespace(input_tokens=100),
    )


class _Klient:
    """Odpowiada rozkładem przypisanym do klucza pytania (ścieżki węzła)."""

    def __init__(self, rozklady: dict[str, dict[str, float]]):
        self.rozklady = rozklady
        self.wywolania: list[dict] = []

    async def system_one(self, state, questions, model=None):
        self.wywolania.append(questions)
        return _odp({k: self.rozklady[k] for k in questions})


# ---------- węzły i pytania ----------
def test_dzieci_na_kazdym_poziomie():
    assert set(dzieci(DRZEWO, ())) == set(DRZEWO["grupy"])
    assert set(dzieci(DRZEWO, ("paznokcie",))) == {"przedłużanie", "manicure"}
    assert set(dzieci(DRZEWO, ("paznokcie", "manicure"))) == {"manicure hybrydowy", "manicure klasyczny"}
    assert dzieci(DRZEWO, ("paznokcie", "przedłużanie")) == {}
    assert dzieci(DRZEWO, ("paznokcie", OGOLNIE)) == {}


def test_grupy_z_opisem_wykluczeniem_i_przykladami():
    kr = pytanie(DRZEWO, ()).criteria
    assert kr["paznokcie"] == {"what": "manicure i pedicure", "not_for": "leczenie stóp", "examples": ["manicure hybrydowy"]}
    assert OGOLNIE not in kr


def test_ponizej_grup_jest_ogolnie():
    assert OGOLNIE in pytanie(DRZEWO, ("paznokcie",)).criteria
    assert OGOLNIE in pytanie(DRZEWO, ("paznokcie", "manicure")).criteria


def test_to_samo_slowo_w_dwoch_grupach_to_dwa_wezly():
    assert pytanie(DRZEWO, ("paznokcie",)).criteria["przedłużanie"]["examples"] == ["przedłużanie paznokci żelem"]
    assert pytanie(DRZEWO, ("brwi i rzęsy",)).criteria["przedłużanie"]["examples"] == ["przedłużanie rzęs 1:1"]


# ---------- wiązka ----------
async def test_wiazka_naprawia_niejasna_decyzje_na_gorze():
    k = _Klient({
        "_": {"brwi i rzęsy": 0.55, "paznokcie": 0.45},
        "brwi i rzęsy": {"przedłużanie": 0.3, OGOLNIE: 0.7},
        "paznokcie": {"manicure": 0.97, "przedłużanie": 0.02, OGOLNIE: 0.01},
        "paznokcie|manicure": {"manicure hybrydowy": 0.96, "manicure klasyczny": 0.03, OGOLNIE: 0.01},
    })
    beam = await wiazka(k, {}, DRZEWO, [0])
    assert beam[0]["sciezka"] == ("paznokcie", "manicure", "manicure hybrydowy")
    assert len(k.wywolania) == 3  # jedno wywołanie na poziom, wszystkie ścieżki naraz


async def test_wynik_to_srednia_geometryczna():
    k = _Klient({"_": {"paznokcie": 0.64, "brwi i rzęsy": 0.36},
                 "paznokcie": {"przedłużanie": 1.0, "manicure": 0.0, OGOLNIE: 0.0},
                 "brwi i rzęsy": {"przedłużanie": 1.0, OGOLNIE: 0.0}})
    beam = await wiazka(k, {}, DRZEWO, [0])
    assert beam[0]["sciezka"] == ("paznokcie", "przedłużanie")
    assert round(beam[0]["wynik"], 3) == 0.8  # sqrt(0,64 · 1,0)


async def test_plaski_rozklad_nie_gubi_sciezki():
    k = _Klient({"_": {"paznokcie": 0.04, "brwi i rzęsy": 0.03, "stopy — podologia": 0.03}})
    beam = await wiazka(k, {}, {**DRZEWO, "zabiegi": {}}, [0])
    assert beam and beam[0]["sciezka"] == ("paznokcie",)


# ---------- porównanie ----------
def rek(sciezki: list[tuple[tuple, float]], cechy: dict | None = None, **dod) -> dict:
    naj = sciezki[0][0]
    rodzaj = naj[2] if len(naj) > 2 and naj[2] != OGOLNIE else (naj[1] if len(naj) > 1 else "inny")
    return {"sciezki": [[list(s), p, p] for s, p in sciezki], "rodzaj": rodzaj, "cechy": {rodzaj: cechy or {}},
            "liczba_zabiegow": 1, "ilosc": None, "zestaw": 0.05, "rozszerzenie": 0.05, **dod}


HYBRYDA = ("paznokcie", "manicure", "manicure hybrydowy")


def f(nazwa: str) -> dict:
    return {"nazwa": nazwa}


def test_p_ten_sam_tylko_dla_konkretnych_lisci():
    assert p_ten_sam({HYBRYDA: 0.9}, {HYBRYDA: 0.8}) == 0.9 * 0.8
    assert p_ten_sam({("paznokcie", OGOLNIE): 0.9}, {("paznokcie", OGOLNIE): 0.9}) == 0


def test_ten_sam_lisc_i_te_same_cechy_to_ta_sama():
    a, b = rek([(HYBRYDA, 0.9)], {"etap": "założenie"}), rek([(HYBRYDA, 0.85)], {"etap": "założenie"})
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Manicure hybrydowy"), f("Hybryda na dłonie"))[0] == "tozsame"


def test_ten_sam_lisc_inna_cecha_to_powiazane():
    a, b = rek([(HYBRYDA, 0.9)], {"etap": "założenie"}), rek([(HYBRYDA, 0.85)], {"etap": "zdjęcie"})
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Manicure hybrydowy"), f("Zdjęcie hybrydy")) == ("powiazane", "etap")


def test_przedluzanie_paznokci_i_rzes_to_rozne():
    a = rek([(("paznokcie", "przedłużanie"), 0.9)])
    b = rek([(("brwi i rzęsy", "przedłużanie"), 0.9)])
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Przedłużanie paznokci"), f("Przedłużanie rzęs")) == ("rozne", "inna grupa")


def test_niepewne_drzewo_nie_daje_ta_sama():
    a = rek([(HYBRYDA, 0.6), (("paznokcie", "manicure", "manicure klasyczny"), 0.4)])
    b = rek([(("paznokcie", "manicure", "manicure klasyczny"), 0.6), (HYBRYDA, 0.4)])
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Manicure A"), f("Manicure B"))[0] != "tozsame"  # 0,24 + 0,24 < 0,5


def test_relacje_drzewa():
    assert relacja_drzewa(HYBRYDA, ("paznokcie", "manicure", "manicure klasyczny")) == ("powiazane", "inna odmiana")
    assert relacja_drzewa(HYBRYDA, ("paznokcie", "manicure", OGOLNIE)) == ("niepelne", "zabieg ogólny")
    assert relacja_drzewa(HYBRYDA, ("paznokcie", OGOLNIE)) == ("niepelne", "zabieg nieustalony")
    assert relacja_drzewa(HYBRYDA, ("paznokcie", "przedłużanie")) == ("rozne", "inny zabieg")


def test_identyczna_nazwa_w_tej_samej_grupie():
    a = rek([(HYBRYDA, 0.5)])
    b = rek([(("paznokcie", "manicure", "manicure klasyczny"), 0.5)])
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Manicure"), f("manicure")) == ("tozsame", "identyczna nazwa")


def test_dodatek_po_jednej_stronie():
    a, b = rek([(HYBRYDA, 0.9)], rozszerzenie=0.7), rek([(HYBRYDA, 0.9)], rozszerzenie=0.05)
    assert porownaj_v11(a, b, PODZIAL_CECH, f("Manicure hybrydowy + french"), f("Manicure hybrydowy")) == ("powiazane", "dodatek")


def test_rozklad_to_iloczyn_krawedzi():
    import math
    r = rozklad([{"sciezka": HYBRYDA, "log_p": math.log(0.5) + math.log(0.8), "decyzje": 2, "wynik": 0}])
    assert round(r[HYBRYDA], 3) == 0.4


async def test_gotowy_rozklad_grup_oszczedza_wywolanie():
    k = _Klient({"paznokcie": {"przedłużanie": 0.96, "manicure": 0.02, OGOLNIE: 0.02}})
    beam = await wiazka(k, {}, DRZEWO, [0], grupy={"paznokcie": 0.96, "brwi i rzęsy": 0.04})
    assert beam[0]["sciezka"] == ("paznokcie", "przedłużanie")
    assert all("_" not in q for q in k.wywolania)


def test_glebsze_poziomy_po_rozbiciu_worka():
    d = {**DRZEWO, "zabiegi": {**DRZEWO["zabiegi"], "stopy — podologia": {
        "klamra ortonyksyjna": {"n": 40, "examples": ["klamra"], "odmiany": {
            "tamponada": {"n": 5, "examples": ["tamponada"], "odmiany": {}},
            "podcięcie wrastającego paznokcia": {"n": 9, "examples": ["podcięcie wrastającego paznokcia"], "odmiany": {}}}}}}}
    assert set(dzieci(d, ("stopy — podologia", "klamra ortonyksyjna"))) == {"tamponada", "podcięcie wrastającego paznokcia"}
    assert dzieci(d, ("stopy — podologia", "klamra ortonyksyjna", "tamponada")) == {}
    a = ("stopy — podologia", "klamra ortonyksyjna", "tamponada")
    b = ("stopy — podologia", "klamra ortonyksyjna", "podcięcie wrastającego paznokcia")
    assert relacja_drzewa(a, b) == ("powiazane", "inna odmiana")
    assert relacja_drzewa(a, ("stopy — podologia", "klamra ortonyksyjna", OGOLNIE)) == ("niepelne", "zabieg ogólny")


def test_cechy_po_najblizszym_rodzaju_v9():
    from services.typesafe_drzewo.drzewo_uslug import zabieg_liscia
    s = ("paznokcie", "manicure", "manicure hybrydowy", "french")
    assert zabieg_liscia(s, {"manicure hybrydowy": {}, "manicure": {}}) == "manicure hybrydowy"
