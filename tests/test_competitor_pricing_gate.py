"""Bramka wejścia do silnika cenowego w Etapie 4 (BEAUTY_AUDIT-cbnt).

Kontekst regresji: bramka early-exit pytała `_active_services_with_variant`,
czyli o `variant_id` — wymóg STAREJ funkcji `_compute_pricing_comparisons`.
Od S0078 w gałęzi `else` stoi `compute_pricing_comparisons_v2`, która
`variant_id` w ogóle nie czyta (services/similarity_pricing/report_pricing.py
:201-204 filtruje po `is_active` + `price_grosze` + `id`). Subject 98814 miał
229 usług, 220 aktywnych z ceną i tylko 25 wariantów — z czego promocje,
pakiety i konsultacje bez ceny wypadały w filtrze wariantowym, więc bramka
schodziła do zera i odcinała 220 dobrych usług od silnika, który ich wariantów
nie potrzebuje. Raport 250 dostał 24 puste wiersze "tylko Ty na rynku" zamiast
203 z cenami rynkowymi.

Trzy warstwy testów:
1. Regresja: subject BEZ wariantów, ale z aktywnymi usługami z ceną → silnik
   v2 jest wołany, ścieżka subject_only NIE.
2. Zabezpieczenie zostaje: subject bez ani jednej usługi aktywnej z ceną →
   early-exit jak dotąd (v2 pominięty, ciężkie tiery też).
3. Sam predykat kwalifikacji — zgodność z filtrem v2, pole po polu.

Zero sieci, zero Supabase, zero Qdranta: warstwa pipeline'u jedzie na tych
samych mockach AsyncMock, co tests/test_competitor_analysis.py (stamtąd
importujemy fabrykę mocka i runner ze szpiegiem), predykat testujemy wprost.
"""

from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock

import pytest

from pipelines.competitor_analysis import _v2_eligible_subject_services
from tests.test_competitor_analysis import (
    _mock_supabase_for_e2e,
    _run_pipeline_with_pricing_spy,
)


async def _subject_with(services: list[dict[str, Any]]) -> dict[str, Any]:
    """Domyślny subject z e2e mocka, z podmienioną listą usług."""
    base = _mock_supabase_for_e2e()
    data = copy.deepcopy(await base.get_subject_full_data())
    data["services"] = services
    return data


def _svc(**overrides: Any) -> dict[str, Any]:
    """Wiersz salon_scrape_services w kształcie, jaki widzi pipeline."""
    svc: dict[str, Any] = {
        "id": 5001,
        "booksy_treatment_id": 1,
        "booksy_service_id": 11,
        "price_grosze": 10000,
        "name": "Test",
        "treatment_name": "Test",
        "is_active": True,
        "is_from_price": False,
        "duration_minutes": 30,
        "treatment_parent_id": None,
        "description_type": "M",
        "photos": [],
        "category_name": "A",
        "is_promo": False,
        "combo_type": None,
        "omnibus_price_grosze": None,
        "description": None,
    }
    svc.update(overrides)
    return svc


# ---------------------------------------------------------------------------
# 1. Regresja BEAUTY_AUDIT-cbnt — brak wariantów nie może odcinać od v2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subject_without_variant_ids_still_reaches_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Subject bez ani jednego `variant_id`, ale z aktywnymi usługami z ceną
    i z `id` → silnik v2 dostaje robotę, ścieżka subject_only nie rusza.

    To dokładnie kształt subjecta 98814 z raportu 250: wariantów tyle co nic,
    ale 220 usług, na których v2 ma na czym pracować.
    """
    import pipelines.competitor_analysis as ca

    emit_spy = MagicMock(return_value=[])
    monkeypatch.setattr(ca, "_emit_subject_only_rows_no_variants", emit_spy)

    subject_data = await _subject_with([
        _svc(id=5001, name="Botoks 1 okolica", price_grosze=60000),
        _svc(id=5002, name="Mezoterapia igłowa", price_grosze=45000),
    ])
    # Ani jedna usługa nie ma variant_id — stara bramka zjechałaby do zera.
    assert all("variant_id" not in s for s in subject_data["services"])

    tracer, pricing_spy, _mock, embed = await _run_pipeline_with_pricing_spy(
        monkeypatch, subject_full_data=subject_data, pricing_return=[],
    )
    # Warstwa embeddingow taksonomii jedzie na deterministycznym stubie, nie na
    # pustym srodowisku — i patch nie jest martwy (BEAUTY_AUDIT-elbp).
    embed.assert_used()

    assert pricing_spy.await_count == 1, (
        "subject z usługami aktywnymi i z ceną musi wejść do "
        "compute_pricing_comparisons_v2 mimo zerowej liczby variant_id"
    )
    assert emit_spy.call_count == 0, (
        "ścieżka subject_only nie może przejąć raportu, gdy silnik v2 ma "
        "na czym pracować"
    )
    early_exit = [
        c for c in tracer.of("pricing.computed")
        if c["data"].get("pricing_type") == "subject_only_early_exit"
    ]
    assert early_exit == [], f"early-exit nie powinien wystąpić; got {early_exit}"


# ---------------------------------------------------------------------------
# 2. Zabezpieczenie zostaje — subject, na którym v2 nie miałby czego liczyć
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subject_without_priced_services_takes_early_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same konsultacje 0 zł i usługi nieaktywne → v2 zwróciłaby [] (jej filtr
    odrzuca każdą z tych usług), więc bramka zostaje i idziemy ścieżką
    subject_only, pomijając ciężkie tiery."""
    import pipelines.competitor_analysis as ca

    emit_spy = MagicMock(return_value=[])
    monkeypatch.setattr(ca, "_emit_subject_only_rows_no_variants", emit_spy)

    subject_data = await _subject_with([
        _svc(id=5001, name="Konsultacja kosmetologiczna", price_grosze=0),
        _svc(id=5002, name="Konsultacja online", price_grosze=None),
        _svc(id=5003, name="Laser (wycofany)", price_grosze=30000, is_active=False),
    ])

    tracer, pricing_spy, _mock, embed = await _run_pipeline_with_pricing_spy(
        monkeypatch, subject_full_data=subject_data,
    )
    # Jak wyzej: sciezka embeddingow wybrana jawnie, patch zywy.
    embed.assert_used()

    assert pricing_spy.await_count == 0, (
        "subject bez usług aktywnych z ceną nie ma po co wchodzić do v2"
    )
    assert emit_spy.call_count == 1, "early-exit musi wyemitować wiersze subject_only"

    early_exit = [
        c for c in tracer.of("pricing.computed")
        if c["data"].get("pricing_type") == "subject_only_early_exit"
    ]
    assert len(early_exit) == 1, f"oczekiwano jednego śladu early-exit; got {early_exit}"
    data = early_exit[0]["data"]
    # Powód musi mówić prawdę: to NIE jest "subject bez wariantów".
    assert data["skip_reason"] == "subject_has_no_priced_services"
    assert data["services_eligible_for_v2"] == 0
    assert data["total_services"] == 3
    for tier in ("variant", "treatment", "structured", "method", "sub_variant"):
        assert tier in data["tiers_skipped"]


# ---------------------------------------------------------------------------
# 3. Predykat kwalifikacji — zgodność z filtrem v2
# ---------------------------------------------------------------------------


def _v2_filter_verbatim(services: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filtr subjecta przepisany DOSŁOWNIE z compute_pricing_comparisons_v2
    (services/similarity_pricing/report_pricing.py:201-204). Kopia jest celowa:
    testujemy zgodność bramki z silnikiem, więc wzorzec musi być czytelny na
    miejscu, a rozjazd z oryginałem ma boleć tutaj."""
    return [
        s for s in services
        if s.get("is_active", True) and s.get("price_grosze") and s.get("id") is not None
    ]


class TestV2EligibilityPredicate:
    """`_v2_eligible_subject_services` — to, o co bramka ma pytać."""

    def test_active_priced_service_with_id_qualifies(self) -> None:
        svc = _svc(id=1, price_grosze=100)
        assert _v2_eligible_subject_services([svc]) == [svc]

    def test_is_active_defaults_to_true(self) -> None:
        """Brak klucza `is_active` = usługa aktywna (tak samo jak w v2)."""
        svc = _svc(id=1, price_grosze=100)
        del svc["is_active"]
        assert _v2_eligible_subject_services([svc]) == [svc]

    def test_inactive_service_drops(self) -> None:
        assert _v2_eligible_subject_services([_svc(is_active=False)]) == []

    @pytest.mark.parametrize("price", [None, 0, -100])
    def test_service_without_real_price_drops(self, price: Any) -> None:
        assert _v2_eligible_subject_services([_svc(price_grosze=price)]) == []

    def test_service_without_id_drops(self) -> None:
        assert _v2_eligible_subject_services([_svc(id=None)]) == []

    def test_missing_id_key_drops(self) -> None:
        svc = _svc()
        del svc["id"]
        assert _v2_eligible_subject_services([svc]) == []

    def test_variant_id_is_irrelevant(self) -> None:
        """Sedno naprawy: v2 nie czyta `variant_id`, więc predykat też nie."""
        without = _svc(id=1)
        with_variant = _svc(id=2, variant_id=7)
        assert _v2_eligible_subject_services([without, with_variant]) == [
            without, with_variant,
        ]

    def test_matches_v2_filter_on_realistic_mix(self) -> None:
        """Na danych, jakie realnie przychodzą ze scrape'u, predykat daje to
        samo, co filtr v2 — usługa po usłudze."""
        services = [
            _svc(id=1, price_grosze=10000),
            _svc(id=2, price_grosze=0),
            _svc(id=3, price_grosze=None),
            _svc(id=4, price_grosze=20000, is_active=False),
            _svc(id=None, price_grosze=30000),
            _svc(id=6, price_grosze=40000, variant_id=7),
        ]
        assert _v2_eligible_subject_services(services) == _v2_filter_verbatim(services)

    def test_qualified_set_is_a_subset_of_what_v2_accepts(self) -> None:
        """Kierunek, na którym stoi bramka: cokolwiek przepuścimy do v2, v2
        musi mieć na czym pracować. Cena ujemna to jedyny rozjazd (v2 sprawdza
        prawdziwościowo, więc -100 gr by przepuściła) i jest tu przypięty:
        jesteśmy odrobinę ostrzejsi, nigdy luźniejsi."""
        services = [
            _svc(id=1, price_grosze=10000),
            _svc(id=2, price_grosze=-100),
            _svc(id=3, price_grosze=0),
            _svc(id=4, is_active=False),
        ]
        ours = _v2_eligible_subject_services(services)
        theirs = _v2_filter_verbatim(services)
        assert all(s in theirs for s in ours)
        # Ujemna cena: v2 ją bierze, my nie. Świadomy, przypięty rozjazd.
        assert _svc(id=2, price_grosze=-100) in theirs
        assert _svc(id=2, price_grosze=-100) not in ours
