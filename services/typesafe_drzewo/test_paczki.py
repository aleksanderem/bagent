"""Destylacja paczkami: te same pytania i PEŁNE opisy opcji co usługa po usłudze, tylko kilka usług
w jednym wywołaniu. Atrapa klienta SDK — zero sieci."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.typesafe_drzewo.paczki import destyluj_paczkami, destyluj_paczke
from services.typesafe_drzewo.podzial import dzieci, pytanie_dziedzina, pytanie_korzen, pytanie_odmiana

PODZIAL = {
    "kanoniczne": ["manicure", "manicure hybrydowy", "manicure klasyczny", "masaż"],
    "rodzic": {"manicure hybrydowy": "manicure", "manicure klasyczny": "manicure"},
    "korzen": {"manicure": "manicure", "manicure hybrydowy": "manicure", "manicure klasyczny": "manicure", "masaż": "masaż"},
    "branze": {"Paznokcie": ["manicure"], "Masaż": ["masaż"]},
    "przyklady": {"manicure": ["Manicure"], "manicure hybrydowy": ["Manicure hybrydowy"], "masaż": ["Masaż klasyczny"]},
    "liczn": {"manicure": 100, "manicure hybrydowy": 80, "manicure klasyczny": 50, "masaż": 70},
    "cechy": {"manicure hybrydowy": {"obszar": {"wartosci": ["paznokcie dłoni", "paznokcie stóp"], "domyslna": None}},
              "manicure": {"obszar": {"wartosci": ["paznokcie dłoni"], "domyslna": None}}},
}


def _wybor(c: str, p: dict) -> SimpleNamespace:
    return SimpleNamespace(choice=c, probabilities=p, confidence=max(p.values()))


class _Klient:
    def __init__(self):
        self.wywolania: list[tuple[dict, dict]] = []

    async def system_one(self, state, questions, model=None):
        self.wywolania.append((state, questions))
        ch, nl = {}, {}
        for k, q in questions.items():
            if k.startswith("d"):
                ch[k] = _wybor("Paznokcie", {"Paznokcie": 0.9, "Masaż": 0.1})
            elif k.startswith("k"):
                ch[k] = _wybor("manicure", {"manicure": 0.95, "inny": 0.05})
            elif k.startswith("o"):
                ch[k] = (_wybor("manicure hybrydowy", {"manicure hybrydowy": 0.8, "ogolnie": 0.2}) if k == "o0"
                         else _wybor("ogolnie", {"ogolnie": 0.9, "manicure hybrydowy": 0.1}))
            elif k.startswith("c"):
                v = next(iter(q.criteria))
                ch[k] = _wybor(v, {v: 0.9})
            else:
                nl[k] = SimpleNamespace(noul=0.1)
        return SimpleNamespace(choices=ch, nouls=nl, usage=SimpleNamespace(input_tokens=100))


def stan(nazwa: str) -> dict:
    return {"usluga": {"nazwa": nazwa, "kategoria_w_cenniku": "Manicure", "typ_salonu": "Paznokcie"}}


async def test_rekord_na_kazda_usluge_w_kolejnosci():
    rek = await destyluj_paczke(_Klient(), [stan("Manicure hybrydowy"), stan("Manicure")], PODZIAL, dzieci(PODZIAL), [0])
    assert [r["rodzaj"] for r in rek] == ["manicure hybrydowy", "manicure"]
    assert rek[0]["cechy"] == {"manicure hybrydowy": {"obszar": "paznokcie dłoni"}}
    assert rek[1]["zestaw"] == 0.1 and "rozszerzenie" in rek[1]


async def test_opcje_pelne_jak_usluga_po_usludze():
    k = _Klient()
    dz = dzieci(PODZIAL)
    await destyluj_paczke(k, [stan("Manicure hybrydowy"), stan("Manicure")], PODZIAL, dz, [0])
    q_d, q_k, q_o = k.wywolania[0][1], k.wywolania[1][1], k.wywolania[2][1]
    assert q_d["d1"].criteria == pytanie_dziedzina(PODZIAL)["dziedzina"].criteria
    assert q_k["k1|Paznokcie"].criteria == pytanie_korzen(PODZIAL, "Paznokcie", dz).criteria
    assert q_o["o0"].criteria == pytanie_odmiana(PODZIAL, "manicure", dz["manicure"]).criteria


async def test_pytanie_wskazuje_swoja_usluge():
    k = _Klient()
    await destyluj_paczke(k, [stan("A"), stan("B")], PODZIAL, dzieci(PODZIAL), [0])
    for _state, q in k.wywolania:
        for klucz, pyt in q.items():
            assert "`usluga`" not in pyt.instructions
            i = klucz.split("|")[0].split("#")[-1].lstrip("dkoc")
            assert f"`uslugi[{i}]`" in pyt.instructions


async def test_cztery_wywolania_na_paczke_i_dane_raz():
    k = _Klient()
    await destyluj_paczke(k, [stan("A"), stan("B"), stan("C")], PODZIAL, dzieci(PODZIAL), [0])
    assert len(k.wywolania) == 4
    assert all(len(st["uslugi"]) == 3 for st, _q in k.wywolania)


@pytest.mark.parametrize("paczka", [1, 2, 5])
async def test_wiele_paczek_daje_komplet(paczka):
    stany = {f"k{i}": stan(f"Manicure {i}") for i in range(7)}
    out = await destyluj_paczkami(_Klient(), stany, PODZIAL, dzieci(PODZIAL), [0], paczka=paczka)
    assert set(out) == set(stany)
