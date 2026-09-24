"""services/diag_backup.py — czy ostatnia nocna kopia bazy wyszła poza serwer.

Linie jak w prawdziwym /var/log/backup-supabase.log na tytanie.
"""
from __future__ import annotations

from services.diag_backup import parse_offsite

TAG = "[backup-supabase-tytan]"

NIEUDANY = [
    f"{TAG} starting backup 2026-09-24 via docker exec ba-supabase-db",
    f"{TAG} WARN cloud upload FAILED (iDrive) — próbuję Google Drive",
    f"{TAG} WARN Google Drive upload FAILED — KEEPING local dump /var/backups/supabase/2026-09-24.dump (jedyna kopia!)",
    f"{TAG} done at 2026-09-24T05:00:47+00:00",
]
UDANY_DRIVE = [
    f"{TAG} starting backup 2026-09-25 via docker exec ba-supabase-db",
    f"{TAG} WARN cloud upload FAILED (iDrive) — próbuję Google Drive",
    f"{TAG} uploaded + byte-verified to Google Drive (28387710328 B)",
    f"{TAG} done at 2026-09-25T05:10:00+00:00",
]


def test_ostatni_przebieg_bez_kopii_poza_serwerem():
    assert parse_offsite(NIEUDANY) == {"ok": False, "cel": None, "zakonczono": "2026-09-24T05:00:47+00:00"}


def test_kopia_na_google_drive_po_padzie_idrive():
    assert parse_offsite(NIEUDANY + UDANY_DRIVE) == {
        "ok": True,
        "cel": "Google Drive",
        "zakonczono": "2026-09-25T05:10:00+00:00",
    }


def test_kopia_na_idrive():
    linie = [
        f"{TAG} starting backup 2026-08-20 via docker exec ba-supabase-db",
        f"{TAG} uploaded + byte-verified (20000 B)",
        f"{TAG} done at 2026-08-20T06:30:00+00:00",
    ]
    assert parse_offsite(linie)["cel"] == "iDrive"


def test_trwajacy_przebieg_nie_przykrywa_ostatniego_zakonczonego():
    """Zrzut o 04:00 trwa — panel ma pokazywać wynik poprzedniej, skończonej nocy."""
    trwa = [f"{TAG} starting backup 2026-09-26 via docker exec ba-supabase-db"]
    assert parse_offsite(UDANY_DRIVE + trwa)["ok"] is True


def test_reczne_doslanie_zapisane_w_logu_liczy_sie_jako_kopia():
    doslane = [
        f"{TAG} uploaded + byte-verified to Google Drive (28387710328 B) — dosłane ręcznie po odnowieniu tokenu gdrive",
        f"{TAG} done at 2026-09-24T18:14:00+00:00 (ręczne dosłanie)",
    ]
    assert parse_offsite(NIEUDANY + doslane) == {
        "ok": True,
        "cel": "Google Drive",
        "zakonczono": "2026-09-24T18:14:00+00:00",
    }


def test_brak_zakonczonego_przebiegu():
    assert parse_offsite([]) == {"ok": None, "cel": None, "zakonczono": None}
