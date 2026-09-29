"""Katalog usług — pytanie o klasę różnicy jednostronnej (bez sieci)."""
from __future__ import annotations

from services.katalog_uslug.klasy import NIE_WIADOMO, NIE_ZMIENIA, ZMIENIA, pytanie_klasy, rozstrzygnij


def test_pytanie_nazywa_dopisek_zabieg_i_druga_oferte() -> None:
    q = pytanie_klasy("rdzen", "UV", "Uzupełnianie rzęs", "Uzupełnienie rzęs 4-6D")
    assert "„UV”" in q.instructions and "Uzupełnianie rzęs" in q.instructions and "Uzupełnienie rzęs 4-6D" in q.instructions
    assert len(q.criteria) == 3


def test_routing_najblizszym_poziomem_brak_odpowiedzi_nie_daje_nie_zmienia() -> None:
    assert rozstrzygnij(0.2) == NIE_ZMIENIA
    assert rozstrzygnij(0.6) == NIE_WIADOMO
    assert rozstrzygnij(1.7) == ZMIENIA
    assert rozstrzygnij(None) == NIE_WIADOMO
