"""Odzyskiwanie pełnych nazw usług uciętych przez Booksy (2026-07-22).

Booksy customer_api ucina długie service.name ("…" na końcu lub w środku);
pełna nazwa jest w variants[].label (usługi jednowariantowe) albo w 1. linii
description. _recover_full_name odzyskuje ją z twardymi guardami.
"""
from scripts.ingest_salon_jsons import _recover_full_name


def test_end_truncated_recovered_from_variant_label():
    svc = {
        "name": "Konsultacja kompleksowa Beauty Plan 360° z zabi...",
        "variants": [{"label": "Konsultacja kompleksowa Beauty Plan 360° z zabiegiem"}],
    }
    assert _recover_full_name(svc) == "Konsultacja kompleksowa Beauty Plan 360° z zabiegiem"


def test_middle_truncated_recovered():
    svc = {
        "name": "Onda Coolwaves - redukc...i modelowanie sylwetki",
        "variants": [{"label": "Onda Coolwaves - redukcja cellulitu i modelowanie sylwetki"}],
    }
    assert _recover_full_name(svc) == "Onda Coolwaves - redukcja cellulitu i modelowanie sylwetki"


def test_fallback_to_description_first_line():
    svc = {
        "name": "Depilacja laserowa - Pachy + Bikini pełne / bra...",
        "variants": [{"label": "60 min"}],  # multiwariant: label to NIE nazwa
        "description": "Depilacja laserowa - Pachy + Bikini pełne / brazylijskie\nOpis zabiegu...",
    }
    assert _recover_full_name(svc) == "Depilacja laserowa - Pachy + Bikini pełne / brazylijskie"


def test_no_match_keeps_original():
    svc = {
        "name": "Masaż twarzy Kobido dłu...",
        "variants": [{"label": "Standard"}],
        "description": "Relaksacyjny masaż liftingujący.",
    }
    assert _recover_full_name(svc) == "Masaż twarzy Kobido dłu..."


def test_clean_name_untouched():
    svc = {"name": "Manicure hybrydowy", "variants": [{"label": "cokolwiek innego"}]}
    assert _recover_full_name(svc) == "Manicure hybrydowy"


def test_short_prefix_too_weak_anchor():
    # Prefiks < 8 znaków = ryzyko false positive — zostaje oryginał.
    svc = {"name": "Masaż...", "variants": [{"label": "Masaż gorącymi kamieniami"}]}
    assert _recover_full_name(svc) == "Masaż..."


def test_unicode_ellipsis_handled():
    svc = {
        "name": "Mezoterapia igłowa twarzy i szyi z ampułką prem…",
        "variants": [{"label": "Mezoterapia igłowa twarzy i szyi z ampułką premium"}],
    }
    assert _recover_full_name(svc) == "Mezoterapia igłowa twarzy i szyi z ampułką premium"
