"""Kontrolka „Kopia bazy poza serwerem" — czy ostatnia nocna kopia wyszła z tytana.

Nocny root-cron (04:00 UTC, /home/booksy/scripts/backup-supabase-tytan.sh)
robi zrzut i wysyła go na iDrive, a gdy ten padnie — na Google Drive Alexa.
Jedynym miejscem, gdzie widać wynik wysyłki, jest jego log. 16–24.09 Drive był
martwy, skrypt pisał „KEEPING local dump (jedyna kopia!)", a panel patrzył na
wyłączony celowo timer i nikt nie wiedział. Ta sekcja czyta wynik z logu.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

BACKUP_LOG = Path("/var/log/backup-supabase.log")

_START = "starting backup"
_DONE = re.compile(r"done at (\S+)")
_VERIFIED_DRIVE = "uploaded + byte-verified to Google Drive"
_VERIFIED_IDRIVE = "uploaded + byte-verified ("
_KEPT_LOCAL = "KEEPING local dump"


def parse_offsite(lines: list[str]) -> dict[str, Any]:
    """Wynik ostatniego ZAKOŃCZONEGO przebiegu: ok, cel wysyłki, czas zakończenia.

    Przebieg kończy linia „done at …"; trwający zrzut nie przykrywa poprzedniej
    nocy. ok=None, gdy w logu nie ma żadnego zakończonego przebiegu.
    """
    last: dict[str, Any] = {"ok": None, "cel": None, "zakonczono": None}
    ok: bool | None = None
    cel: str | None = None
    for line in lines:
        if _START in line:
            ok, cel = None, None
        elif _VERIFIED_DRIVE in line:
            ok, cel = True, "Google Drive"
        elif _VERIFIED_IDRIVE in line:
            ok, cel = True, "iDrive"
        elif _KEPT_LOCAL in line and ok is not True:
            ok = False
        elif match := _DONE.search(line):
            last = {"ok": ok if ok is not None else False, "cel": cel, "zakonczono": match.group(1)}
            ok, cel = None, None
    return last


def backup_offsite(path: Path = BACKUP_LOG) -> dict[str, Any]:
    """Wynik z logu; brak dostępu do pliku = sekcja z `error`, nie wyjątek."""
    from services.diagnostics import tail_lines

    if not path.exists():
        return {"ok": None, "cel": None, "zakonczono": None, "error": f"brak pliku {path}"}
    return parse_offsite(tail_lines(path))
