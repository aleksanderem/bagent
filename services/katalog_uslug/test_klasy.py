"""Katalog usług — pytanie o klasę różnicy jednostronnej (bez sieci)."""
from __future__ import annotations

from services.katalog_uslug.klasy import (NIE_WIADOMO, NIE_ZMIENIA, ZMIENIA, przyklad_czysty, pytanie_klasy,
                                          pytanie_zamiany, rozstrzygnij, zamiana_rownowazna)


def test_przyklad_klasy_odrzucony_gdy_druga_oferta_ma_dopisek_w_innej_postaci() -> None:
    # sprawdzian 9: „Strzyżenie Maszynka(1długość)” jako oferta BEZ „jedna długość” — model słusznie odpowiedział
    # „nie zmienia” dla tej pary, a pamięć przeniosła to na „Strzyżenie maszynką” bez długości
    assert not przyklad_czysty("dlugosc jedn", "Strzyżenie Maszynka(1długość)")
    assert przyklad_czysty("dlugosc jedn", "Strzyżenie maszynką")
    assert not przyklad_czysty("maszynk strzyzen", "Combo - buzzcut + Broda", {"buzzcut": "maszynk"})
    assert przyklad_czysty("u", "Hybryda na stopy")  # krótkie słowo tylko jako całe słowo, nie wewnątrz „hybryda”
    assert not przyklad_czysty("stop", "Hybryda — stopy")


def test_pytanie_nazywa_dopisek_zabieg_i_druga_oferte() -> None:
    q = pytanie_klasy("rdzen", "UV", "Uzupełnianie rzęs", "Uzupełnienie rzęs 4-6D")
    assert "„UV”" in q.instructions and "Uzupełnianie rzęs" in q.instructions and "Uzupełnienie rzęs 4-6D" in q.instructions
    assert len(q.criteria) == 3


def test_routing_najblizszym_poziomem_brak_odpowiedzi_nie_daje_nie_zmienia() -> None:
    assert rozstrzygnij(0.2) == NIE_ZMIENIA
    assert rozstrzygnij(0.6) == NIE_WIADOMO
    assert rozstrzygnij(1.7) == ZMIENIA
    assert rozstrzygnij(None) == NIE_WIADOMO


def test_pytanie_o_zamiane_podaje_obie_oferty_i_obie_strony_roznicy() -> None:
    q = pytanie_zamiany("Głowy", "włosów", "Strzyżenie Głowy i Brody", "Strzyżenie włosów i brody")
    assert all(s in q.instructions for s in ("„Głowy”", "„włosów”", "Strzyżenie Głowy i Brody", "Strzyżenie włosów i brody"))
    assert "`oferta_a" in q.instructions and "`oferta_b" in q.instructions
    assert set(q.criteria) == {"to_samo", "wezsze", "szersze", "inne", "nie_wiadomo"}  # relacja jak w słowniku + brak wiedzy


def test_zamiana_rownowazna_tylko_przy_pewnym_to_samo() -> None:
    assert zamiana_rownowazna({"relacja": "to_samo", "rozklad": {"to_samo": 0.9}})
    assert not zamiana_rownowazna({"relacja": "to_samo", "rozklad": {"to_samo": 0.6}})
    assert not zamiana_rownowazna({"relacja": "wezsze", "rozklad": {"to_samo": 0.1}})
    assert not zamiana_rownowazna({"relacja": None, "blad": "Timeout"})
