"""Pokrycie menu podmiotu przez konkurentów raportu wg werdyktów b-match (Faza 8a w competitor_analysis).

Wycena b-match (report_pricing._matching_bcard) zna już, u którego konkurenta raportu jest ta sama usługa albo jej
odmiana — Faza 8a bierze to stąd zamiast osobnego wyszukiwania podobnych nazw. Oba kroki liczą się w tym samym
zadaniu raportu (ten sam proces workera), więc wystarczy pamięć procesu, kluczem jest id raportu.
Format jak wynik search_twins: {id usługi podmiotu: [{"booksy_id", "similarity"}]} — dalej te same funkcje koszyków.
"""
from __future__ import annotations

from typing import Any

POKRYWA = ("ta_sama", "odmiana")
_MAX_RAPORTOW = 50
_raporty: dict[int, dict[str, Any]] = {}


def zapamietaj(report_id: int, klastry: dict[int, list[dict[str, Any]]], uslug: int) -> None:
    while len(_raporty) >= _MAX_RAPORTOW:
        _raporty.pop(next(iter(_raporty)))
    _raporty[int(report_id)] = {"klastry": klastry, "uslug": uslug}


def wez(report_id: int) -> dict[str, Any] | None:
    """Jednorazowo: drugi odczyt tego samego raportu (np. ponowienie bez b-match) nie dostanie starych danych."""
    return _raporty.pop(int(report_id), None)
