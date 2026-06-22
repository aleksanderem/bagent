"""Testy dla layer_unit.py — normalizacja jednostki / pakietu.

Scenariusze:
  - klaster z pakietami → n_packages_excluded poprawne, cena z pojedynczych
  - normalizacja zł/min → market_price = median(zł/min) * duration_subject
  - subject bez duration → fallback do median_raw
  - wszystkie pakiety → market_price_grosze=None, n_used=0
  - deviation_pct → poprawne obliczenie
  - sample z duration=0/None → pomijany w zł/min, liczony w n_zero_duration
  - is_package brak w dict → traktowany jako False (pojedynczy)
  - percentyle p25<=p50<=p75
  - immutability: wejście nie jest mutowane
"""
from __future__ import annotations

import copy
import math

import pytest

from .layer_unit import normalize_unit


# ---------------------------------------------------------------------------
# Fixtures — realistyczne dane (presoterapia ~100zł/40min)
# ---------------------------------------------------------------------------

def make_subject(
    price_grosze: int = 15000,
    duration_minutes: int | None = 40,
    is_package: bool = False,
    service_name: str = "Presoterapia",
) -> dict:
    return {
        "service_id": 1,
        "service_name": service_name,
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "category_name": "Drenaż",
        "is_package": is_package,
    }


def make_sample(
    service_id: int,
    price_grosze: int,
    duration_minutes: int | None,
    is_package: bool = False,
    similarity: float = 0.85,
) -> dict:
    return {
        "service_id": service_id,
        "booksy_id": 50 + service_id,
        "salon_name": f"Salon {service_id}",
        "service_name": "Presoterapia",
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "similarity": similarity,
        "category_name": "Pielęgnacja ciała",
        "is_package": is_package,
    }


# ---------------------------------------------------------------------------
# Test 1: klaster z pakietami — wykluczenie pakietów
# ---------------------------------------------------------------------------

class TestPackageExclusion:
    def test_n_packages_excluded_and_n_used(self):
        """2 pakiety + 5 pojedynczych: n_packages_excluded=2, n_used=5."""
        samples = [
            make_sample(1, price_grosze=70000, duration_minutes=None, is_package=True),
            make_sample(2, price_grosze=65000, duration_minutes=None, is_package=True),
            make_sample(3, price_grosze=9900,  duration_minutes=60),
            make_sample(4, price_grosze=10000, duration_minutes=60),
            make_sample(5, price_grosze=11000, duration_minutes=55),
            make_sample(6, price_grosze=8500,  duration_minutes=30),
            make_sample(7, price_grosze=12000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert meta["n_packages_excluded"] == 2
        assert meta["n_in"] == 7
        assert stats["n_used"] == 5

    def test_packages_do_not_affect_price(self):
        """Cena rynkowa liczona TYLKO z 5 pojedynczych sampli."""
        # Pojedyncze: ceny w groszach dla 60min
        # zł/min: 9900/60=165, 10000/60=166.67, 11000/55=200, 8500/30=283.33, 12000/60=200
        # median(165, 166.67, 200, 200, 283.33) = 200 gr/min
        # market_price = 200 * 40 = 8000 groszy
        samples = [
            make_sample(1, price_grosze=70000, duration_minutes=None, is_package=True),
            make_sample(2, price_grosze=65000, duration_minutes=None, is_package=True),
            make_sample(3, price_grosze=9900,  duration_minutes=60),
            make_sample(4, price_grosze=10000, duration_minutes=60),
            make_sample(5, price_grosze=11000, duration_minutes=55),
            make_sample(6, price_grosze=8500,  duration_minutes=30),
            make_sample(7, price_grosze=12000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        # Oczekiwana: zł/min [165.0, 166.67, 200.0, 283.33, 200.0]
        # sorted: [165.0, 166.67, 200.0, 200.0, 283.33]
        # median (index 2) = 200.0 gr/min
        assert meta["used_per_minute"] is True
        assert stats["zl_per_min_median"] == pytest.approx(200.0, rel=0.01)
        assert stats["market_price_grosze"] == 8000


# ---------------------------------------------------------------------------
# Test 2: normalizacja zł/min — ręcznie policzony oczekiwany wynik
# ---------------------------------------------------------------------------

class TestPerMinuteNormalization:
    def test_market_price_equals_median_zl_per_min_times_subject_duration(self):
        """Przykład presoterapii: 5 sampli, subject 40min, 15000 groszy.

        Ręczne obliczenie:
          sample A: 9900 gr / 60min = 165.0 gr/min
          sample B: 9900 gr / 40min = 247.5 gr/min
          sample C: 12000 gr / 60min = 200.0 gr/min
          sample D: 7200 gr / 30min = 240.0 gr/min
          sample E: 9000 gr / 45min = 200.0 gr/min
        Posortowane: [165.0, 200.0, 200.0, 240.0, 247.5]
        Median (index 2) = 200.0 gr/min
        market_price = round(200.0 * 40) = 8000
        """
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=9900,  duration_minutes=40),
            make_sample(3, price_grosze=12000, duration_minutes=60),
            make_sample(4, price_grosze=7200,  duration_minutes=30),
            make_sample(5, price_grosze=9000,  duration_minutes=45),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert meta["used_per_minute"] is True
        assert stats["zl_per_min_median"] == pytest.approx(200.0, abs=0.01)
        assert stats["market_price_grosze"] == 8000

    def test_market_price_even_number_of_samples(self):
        """Parzysty klaster: median z interpolacji.

        Ręczne obliczenie:
          sample A: 6000 / 30 = 200.0
          sample B: 9000 / 45 = 200.0
          sample C: 12000 / 60 = 200.0
          sample D: 15000 / 50 = 300.0
          statistics.median([200, 200, 200, 300]) = (200+200)/2 = 200
          market_price = round(200 * 40) = 8000
        """
        samples = [
            make_sample(1, price_grosze=6000,  duration_minutes=30),
            make_sample(2, price_grosze=9000,  duration_minutes=45),
            make_sample(3, price_grosze=12000, duration_minutes=60),
            make_sample(4, price_grosze=15000, duration_minutes=50),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert stats["market_price_grosze"] == 8000
        assert meta["used_per_minute"] is True

    def test_median_raw_always_present_when_samples_exist(self):
        """median_raw_grosze zawsze policzony (nawet gdy używamy zł/min)."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=15000, duration_minutes=40),
            make_sample(3, price_grosze=12000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        # median_raw: [9900, 12000, 15000] → 12000
        assert stats["median_raw_grosze"] == 12000


# ---------------------------------------------------------------------------
# Test 3: subject bez duration → fallback
# ---------------------------------------------------------------------------

class TestFallbackNoDuration:
    def test_subject_duration_none_uses_median_raw(self):
        """Subject z duration_minutes=None → used_per_minute=False, fallback."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=10000, duration_minutes=60),
            make_sample(3, price_grosze=11000, duration_minutes=55),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=None)
        stats, meta = normalize_unit(subject, samples)

        assert meta["used_per_minute"] is False
        # median([9900, 10000, 11000]) = 10000
        assert stats["market_price_grosze"] == 10000
        assert stats["median_raw_grosze"] == 10000

    def test_subject_duration_zero_uses_median_raw(self):
        """Subject z duration_minutes=0 → traktowany jak None."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=11000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=0)
        stats, meta = normalize_unit(subject, samples)

        assert meta["used_per_minute"] is False
        assert stats["market_price_grosze"] == 10450  # median([9900, 11000])


# ---------------------------------------------------------------------------
# Test 4: wszystkie samples pakietowe → None
# ---------------------------------------------------------------------------

class TestAllPackages:
    def test_all_packages_returns_none(self):
        """Gdy wszystkie samples są pakietami → market_price_grosze=None."""
        samples = [
            make_sample(1, price_grosze=70000, duration_minutes=None, is_package=True),
            make_sample(2, price_grosze=50000, duration_minutes=None, is_package=True),
            make_sample(3, price_grosze=80000, duration_minutes=None, is_package=True),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert stats["market_price_grosze"] is None
        assert stats["deviation_pct"] is None
        assert stats["n_used"] == 0
        assert meta["n_packages_excluded"] == 3
        assert stats["p25_grosze"] is None
        assert stats["p75_grosze"] is None

    def test_empty_samples_returns_none(self):
        """Pusty klaster → market_price_grosze=None."""
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, [])

        assert stats["market_price_grosze"] is None
        assert stats["n_used"] == 0
        assert meta["n_in"] == 0


# ---------------------------------------------------------------------------
# Test 5: deviation_pct
# ---------------------------------------------------------------------------

class TestDeviationPct:
    def test_deviation_pct_above_market(self):
        """Subject 15000 groszy (150zł), market 9800 gr (98zł).

        Ręczne obliczenie:
          Klaster 5 sampli × 60 min, ceny: 8000,9900,10000,10100,11000
          zł/min: 133.33, 165.0, 166.67, 168.33, 183.33
          sorted: [133.33, 165.0, 166.67, 168.33, 183.33]
          median (idx 2) = 166.67 gr/min
          market_price = round(166.67 * 40) = 6667 groszy (≈66.67 zł)

        Używamy prostszego zestawu:
          4 samples × 40 min = po 100 gr/min, 150 gr/min, 200 gr/min, 250 gr/min
          ceny: 4000, 6000, 8000, 10000
          zł/min sorted: [100, 150, 200, 250], median = (150+200)/2 = 175
          market_price = round(175 * 40) = 7000
          deviation = (15000 - 7000) / 7000 * 100 = 114.28...%
        """
        samples = [
            make_sample(1, price_grosze=4000,  duration_minutes=40),
            make_sample(2, price_grosze=6000,  duration_minutes=40),
            make_sample(3, price_grosze=8000,  duration_minutes=40),
            make_sample(4, price_grosze=10000, duration_minutes=40),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        # zł/min sorted: [100, 150, 200, 250], n=4, median=175.0
        assert stats["zl_per_min_median"] == pytest.approx(175.0, abs=0.1)
        assert stats["market_price_grosze"] == 7000
        expected_dev = (15000 - 7000) / 7000 * 100
        assert stats["deviation_pct"] == pytest.approx(expected_dev, rel=1e-5)

    def test_deviation_pct_below_market(self):
        """Subject tańszy od rynku → ujemne deviation_pct."""
        samples = [
            make_sample(1, price_grosze=20000, duration_minutes=40),
            make_sample(2, price_grosze=20000, duration_minutes=40),
            make_sample(3, price_grosze=20000, duration_minutes=40),
        ]
        subject = make_subject(price_grosze=10000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        # market = 20000, deviation = (10000-20000)/20000*100 = -50%
        assert stats["deviation_pct"] == pytest.approx(-50.0, abs=0.01)

    def test_deviation_pct_with_real_presoterapia_example(self):
        """Przykład z dokumentacji: subject 150zł (40min), market ~98zł.

        Klaster: 5 sampli 60-min @ 9900 gr każdy
        zł/min = 165.0, median = 165.0
        market = round(165.0 * 40) = 6600 groszy (66zł)
        deviation = (15000 - 6600) / 6600 * 100 ≈ +127.27%

        Żeby przetestować ~+53% z dokumentacji (150 vs 98):
        Potrzebujemy market = 9800 gr przy subject 40 min
        → zł/min = 245, samples 60 min po 14700 gr
        deviation = (15000 - 9800) / 9800 * 100 = 53.06...%
        """
        samples = [
            make_sample(i, price_grosze=14700, duration_minutes=60)
            for i in range(1, 6)
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        # zł/min = 245.0, market = round(245 * 40) = 9800
        assert stats["market_price_grosze"] == 9800
        expected_dev = (15000 - 9800) / 9800 * 100
        assert stats["deviation_pct"] == pytest.approx(expected_dev, rel=1e-5)
        # ≈ 53.06%
        assert 53.0 < stats["deviation_pct"] < 54.0


# ---------------------------------------------------------------------------
# Test 6: samples z duration=0/None → n_zero_duration
# ---------------------------------------------------------------------------

class TestZeroDuration:
    def test_zero_and_none_duration_counted_in_meta(self):
        """Samples z duration=0 i None pominięte w zł/min, zliczone."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=9900,  duration_minutes=0),
            make_sample(3, price_grosze=9900,  duration_minutes=None),
            make_sample(4, price_grosze=12000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert meta["n_zero_duration"] == 2
        assert stats["n_used"] == 4  # all 4 are non-package
        # zł/min tylko z 2 sampli (60 min): 9900/60 i 12000/60
        assert meta["used_per_minute"] is True
        assert stats["zl_per_min_median"] == pytest.approx(
            (9900 / 60 + 12000 / 60) / 2, abs=0.1
        )

    def test_all_non_package_samples_have_zero_duration_falls_back(self):
        """Gdy żaden sample nie ma duration>0 → fallback do median_raw."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=0),
            make_sample(2, price_grosze=11000, duration_minutes=None),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)

        assert meta["used_per_minute"] is False
        assert stats["zl_per_min_median"] is None
        # fallback: median([9900, 11000]) = 10450
        assert stats["market_price_grosze"] == 10450


# ---------------------------------------------------------------------------
# Test 7: is_package brak w dict → traktowany jako False
# ---------------------------------------------------------------------------

class TestMissingIsPackageKey:
    def test_missing_is_package_treated_as_false(self):
        """Sample bez klucza is_package jest policzony jako pojedynczy."""
        samples_without_key = [
            {
                "service_id": 1,
                "booksy_id": 51,
                "salon_name": "Salon A",
                "service_name": "Presoterapia",
                "price_grosze": 9900,
                "duration_minutes": 60,
                "similarity": 0.9,
                "category_name": "Pielęgnacja ciała",
                # brak is_package
            },
            {
                "service_id": 2,
                "booksy_id": 52,
                "salon_name": "Salon B",
                "service_name": "Presoterapia",
                "price_grosze": 12000,
                "duration_minutes": 60,
                "similarity": 0.85,
                "category_name": "Pielęgnacja ciała",
                # brak is_package
            },
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples_without_key)

        assert meta["n_packages_excluded"] == 0
        assert stats["n_used"] == 2


# ---------------------------------------------------------------------------
# Test 8: percentyle p25<=p50<=p75
# ---------------------------------------------------------------------------

class TestPercentiles:
    def test_percentile_ordering(self):
        """p25 <= p50 <= p75 zawsze."""
        # 7 sampli, różne czasy — żeby p25/p50/p75 były różne
        samples = [
            make_sample(1, price_grosze=5000,  duration_minutes=30),
            make_sample(2, price_grosze=8000,  duration_minutes=40),
            make_sample(3, price_grosze=9900,  duration_minutes=60),
            make_sample(4, price_grosze=12000, duration_minutes=60),
            make_sample(5, price_grosze=14000, duration_minutes=70),
            make_sample(6, price_grosze=18000, duration_minutes=90),
            make_sample(7, price_grosze=25000, duration_minutes=120),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)

        assert stats["p25_grosze"] is not None
        assert stats["p50_grosze"] is not None
        assert stats["p75_grosze"] is not None
        assert stats["p25_grosze"] <= stats["p50_grosze"] <= stats["p75_grosze"]

    def test_percentiles_single_sample(self):
        """Jeden sample → p25 = p50 = p75."""
        samples = [make_sample(1, price_grosze=10000, duration_minutes=40)]
        subject = make_subject(price_grosze=10000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)

        assert stats["p25_grosze"] == stats["p50_grosze"] == stats["p75_grosze"]

    def test_percentile_values_fallback_to_raw(self):
        """Gdy subject bez duration → percentyle z surowych cen."""
        samples = [
            make_sample(1, price_grosze=8000,  duration_minutes=60),
            make_sample(2, price_grosze=10000, duration_minutes=40),
            make_sample(3, price_grosze=12000, duration_minutes=30),
            make_sample(4, price_grosze=14000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=11000, duration_minutes=None)
        stats, meta = normalize_unit(subject, samples)

        assert meta["used_per_minute"] is False
        # Surowe ceny: [8000, 10000, 12000, 14000]
        # p25: index 0.75 → 8000 + 0.75*(10000-8000) = 9500
        # p50: index 1.5 → 10000 + 0.5*(12000-10000) = 11000
        # p75: index 2.25 → 12000 + 0.25*(14000-12000) = 12500
        assert stats["p25_grosze"] == 9500
        assert stats["p50_grosze"] == 11000
        assert stats["p75_grosze"] == 12500


# ---------------------------------------------------------------------------
# Test 9: immutability
# ---------------------------------------------------------------------------

class TestImmutability:
    def test_subject_not_mutated(self):
        """Funkcja nie mutuje słownika subject."""
        samples = [make_sample(1, price_grosze=9900, duration_minutes=60)]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        original_subject = copy.deepcopy(subject)
        normalize_unit(subject, samples)
        assert subject == original_subject

    def test_samples_not_mutated(self):
        """Funkcja nie mutuje żadnego sample z listy."""
        samples = [
            make_sample(1, price_grosze=9900, duration_minutes=60),
            make_sample(2, price_grosze=12000, duration_minutes=60, is_package=True),
        ]
        original_samples = copy.deepcopy(samples)
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        normalize_unit(subject, samples)
        assert samples == original_samples


# ---------------------------------------------------------------------------
# Test 10: edge cases rounding i typy
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_market_price_is_int(self):
        """market_price_grosze jest int, nie float."""
        samples = [make_sample(1, price_grosze=9900, duration_minutes=60)]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)
        assert isinstance(stats["market_price_grosze"], int)

    def test_percentiles_are_int(self):
        """p25, p50, p75 są int."""
        samples = [
            make_sample(1, price_grosze=9900,  duration_minutes=60),
            make_sample(2, price_grosze=12000, duration_minutes=60),
        ]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)
        assert isinstance(stats["p25_grosze"], int)
        assert isinstance(stats["p50_grosze"], int)
        assert isinstance(stats["p75_grosze"], int)

    def test_zl_per_min_is_float(self):
        """zl_per_min_median jest float gdy dostępny."""
        samples = [make_sample(1, price_grosze=9900, duration_minutes=60)]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)
        assert isinstance(stats["zl_per_min_median"], float)

    def test_meta_keys_complete(self):
        """meta zawiera wszystkie wymagane klucze."""
        samples = [make_sample(1, price_grosze=9900, duration_minutes=60)]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, meta = normalize_unit(subject, samples)
        required_meta = {"n_in", "n_packages_excluded", "n_zero_duration", "used_per_minute"}
        assert required_meta == set(meta.keys())

    def test_stats_keys_complete(self):
        """market_stats zawiera wszystkie wymagane klucze."""
        samples = [make_sample(1, price_grosze=9900, duration_minutes=60)]
        subject = make_subject(price_grosze=15000, duration_minutes=40)
        stats, _ = normalize_unit(subject, samples)
        required_stats = {
            "market_price_grosze", "median_raw_grosze",
            "p25_grosze", "p50_grosze", "p75_grosze",
            "zl_per_min_median", "deviation_pct", "n_used",
        }
        assert required_stats == set(stats.keys())
