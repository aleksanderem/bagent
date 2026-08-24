"""Testy walidacji schematu wejścia run_corpus_regression.py (BEAUTY_AUDIT-f3rv).

CO: run_corpus_regression.py oczekuje korpusu zbudowanego przez build_corpus.py
(schemat {subject, twins_082, twins_075, error?}). fixtures/corpus_golden.json ma
INNY schemat ({case, subject, twins, expected}, przeznaczony dla
test_corpus_golden.py) — bez walidacji podanie go jako --corpus cicho wpadało w
gałąź "brak twins_082 => skip" dla WSZYSTKICH 8 rekordów, a compare_snapshots.py
potem fałszywie raportował "IDENTICAL: 8 / INVESTIGATE: 0", mimo że silnik nic
nie policzył.

PO CO: regresja incydentu z 2026-08-24 (weryfikacja BEAUTY_AUDIT-mol-gmbm) —
pusty snapshot wyglądał jak dowód zera regresji.
"""
from __future__ import annotations

import json
import pathlib
import sys

import pytest

from . import run_corpus_regression

_FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


def _run_main(monkeypatch, corpus_path, out_path):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_corpus_regression", "--corpus", str(corpus_path), "--out", str(out_path)],
    )
    run_corpus_regression.main()


def test_corpus_golden_json_rejected_not_silently_skipped(monkeypatch, tmp_path):
    """fixtures/corpus_golden.json (schemat golden-testu) musi wywalić błąd, nie
    cichy snapshot samych 'skip: no_data'."""
    out_path = tmp_path / "snap.json"
    with pytest.raises(SystemExit) as exc_info:
        _run_main(monkeypatch, _FIXTURES_DIR / "corpus_golden.json", out_path)

    message = str(exc_info.value)
    assert "twins_082" in message
    assert "test_corpus_golden.py" in message
    # Nic nie miało zostać zapisane - błąd wywalił się przed pętlą snapshotu.
    assert not out_path.exists()


def test_valid_schema_record_is_scored_not_skipped(monkeypatch, tmp_path):
    """Rekord z poprawnym schematem build_corpus.py (klucz twins_082 obecny,
    niepusta lista bliźniaków) przechodzi normalnie przez compute_market_price."""
    corpus = [
        {
            "subject": {
                "id": 1,
                "booksy_id": 100,
                "name": "Manicure hybrydowy",
                "price_grosze": 10000,
                "duration_minutes": 60,
                "category_name": "Manicure",
                "is_package": False,
            },
            "twins_082": [
                {
                    "service_id": 2,
                    "booksy_id": 200,
                    "salon_name": "Salon Testowy",
                    "service_name": "Manicure hybrydowy",
                    "price_grosze": 9500,
                    "duration_minutes": 55,
                    "category_name": "Manicure",
                    "is_package": False,
                    "similarity": 0.95,
                    "peer_max_sim": 0.95,
                }
            ],
            "twins_075": None,
        }
    ]
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus), encoding="utf-8")
    out_path = tmp_path / "snap.json"

    _run_main(monkeypatch, corpus_path, out_path)

    snap = json.loads(out_path.read_text())
    assert len(snap) == 1
    assert "skip" not in snap[0]
    assert snap[0]["id"] == 1


def test_legal_per_record_skip_still_silent(monkeypatch, tmp_path):
    """Korpus z DOBRYM schematem (klucz twins_082 obecny w co najmniej jednym
    rekordzie), gdzie POJEDYNCZY rekord ma error/twins_082=None, dalej ma cicho
    trafić do skip — walidacja nie może złapać legalnego per-rekord przypadku."""
    corpus = [
        {
            "subject": {
                "id": 1,
                "booksy_id": 100,
                "name": "Usługa bez konkurencji",
                "price_grosze": 5000,
                "duration_minutes": 30,
                "category_name": "Inne",
                "is_package": False,
            },
            "twins_082": None,
            "twins_075": None,
            "error": "no_competitors_in_radius",
        },
        {
            "subject": {
                "id": 2,
                "booksy_id": 200,
                "name": "Manicure hybrydowy",
                "price_grosze": 10000,
                "duration_minutes": 60,
                "category_name": "Manicure",
                "is_package": False,
            },
            "twins_082": [
                {
                    "service_id": 3,
                    "booksy_id": 300,
                    "salon_name": "Salon Testowy 2",
                    "service_name": "Manicure hybrydowy",
                    "price_grosze": 9000,
                    "duration_minutes": 55,
                    "category_name": "Manicure",
                    "is_package": False,
                    "similarity": 0.95,
                    "peer_max_sim": 0.95,
                }
            ],
            "twins_075": None,
        },
    ]
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text(json.dumps(corpus), encoding="utf-8")
    out_path = tmp_path / "snap.json"

    _run_main(monkeypatch, corpus_path, out_path)

    snap = json.loads(out_path.read_text())
    assert len(snap) == 2
    assert snap[0]["id"] == 1
    assert snap[0]["skip"] == "no_competitors_in_radius"
    assert snap[1]["id"] == 2
    assert "skip" not in snap[1]


def test_empty_corpus_does_not_raise(monkeypatch, tmp_path):
    """Pusty korpus ([]) to nie sygnatura złego pliku - nie ma czego walidować."""
    corpus_path = tmp_path / "corpus.json"
    corpus_path.write_text("[]", encoding="utf-8")
    out_path = tmp_path / "snap.json"

    _run_main(monkeypatch, corpus_path, out_path)

    snap = json.loads(out_path.read_text())
    assert snap == []
