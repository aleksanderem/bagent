"""Przeniesione do services/typesafe_profile/listy.py (produkcja, bd BEAUTY_AUDIT-8295).

Listy v1 (wybór jednej wartości z opcją „nie podano”) porzucone 21.09 na rzecz
pomiarów tak/nie i skal — patrz historia gita tego pliku w PR.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.typesafe_profile.listy import METODA, OBSZAR  # noqa: E402,F401
