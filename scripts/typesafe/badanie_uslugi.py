"""Przeniesione do services/typesafe_profile/pytania.py (produkcja, bd BEAUTY_AUDIT-8295).

Ten plik zostaje, żeby narzędzia pomiarowe w tym katalogu działały bez zmian.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.typesafe_profile.pytania import *  # noqa: E402,F401,F403
from services.typesafe_profile.pytania import _all_questions  # noqa: E402,F401
