"""Testy jednostkowe dla layer_dedup.dedup_by_salon."""
from __future__ import annotations

import copy

import pytest

from services.similarity_pricing.layer_dedup import dedup_by_salon


# ---------------------------------------------------------------------------
# Fixtures — realistyczne kształty sample (wyjście fn_find_related_v2)
# ---------------------------------------------------------------------------

def make_sample(
    service_id: int,
    booksy_id: int | None,
    salon_id: int,
    salon_name: str,
    service_name: str,
    price_grosze: int,
    duration_minutes: int,
    similarity: float,
    category_name: str = "Kategoria testowa",
    is_package: bool = False,
) -> dict:
    return {
        "service_id": service_id,
        "booksy_id": booksy_id,
        "salon_id": salon_id,
        "salon_name": salon_name,
        "service_name": service_name,
        "price_grosze": price_grosze,
        "duration_minutes": duration_minutes,
        "similarity": similarity,
        "category_name": category_name,
        "is_package": is_package,
    }


# Salon A (booksy_id=100) — 3 warianty
SALON_A_1 = make_sample(1, 100, 10, "Salon Aneta", "Manicure hybrydowy", 5000, 60, 0.92)
SALON_A_2 = make_sample(2, 100, 10, "Salon Aneta", "Manicure hybryda klasyczna", 5500, 60, 0.85)
SALON_A_3 = make_sample(3, 100, 10, "Salon Aneta", "Hybrid manicure", 6000, 60, 0.78)

# Salon B (booksy_id=200) — 2 warianty
SALON_B_1 = make_sample(4, 200, 20, "Beauty Room B", "Manicure gel", 6500, 75, 0.90)
SALON_B_2 = make_sample(5, 200, 20, "Beauty Room B", "Manicure żelowy", 7000, 75, 0.95)

FIVE_SAMPLES = [SALON_A_1, SALON_A_2, SALON_A_3, SALON_B_1, SALON_B_2]


# ---------------------------------------------------------------------------
# Testy podstawowe
# ---------------------------------------------------------------------------

class TestBasicCounts:
    def test_two_salons_five_samples_returns_two_reps(self):
        deduped, meta = dedup_by_salon(FIVE_SAMPLES)
        assert len(deduped) == 2

    def test_meta_n_unique_salons(self):
        _, meta = dedup_by_salon(FIVE_SAMPLES)
        assert meta["n_unique_salons"] == 2

    def test_meta_n_raw(self):
        _, meta = dedup_by_salon(FIVE_SAMPLES)
        assert meta["n_raw"] == 5

    def test_meta_n_dropped(self):
        _, meta = dedup_by_salon(FIVE_SAMPLES)
        assert meta["n_dropped"] == 3

    def test_empty_input(self):
        deduped, meta = dedup_by_salon([])
        assert deduped == []
        assert meta == {"n_raw": 0, "n_unique_salons": 0, "n_dropped": 0}


# ---------------------------------------------------------------------------
# strategy="closest"
# ---------------------------------------------------------------------------

class TestStrategyClosest:
    def test_default_strategy_is_closest(self):
        deduped, _ = dedup_by_salon(FIVE_SAMPLES)
        booksy_ids_with_sims = {
            (s["booksy_id"], s["similarity"]) for s in deduped
        }
        # Salon A: max sim = 0.92 (SALON_A_1)
        assert (100, 0.92) in booksy_ids_with_sims
        # Salon B: max sim = 0.95 (SALON_B_2)
        assert (200, 0.95) in booksy_ids_with_sims

    def test_explicit_closest_picks_max_similarity_per_salon(self):
        deduped, _ = dedup_by_salon(FIVE_SAMPLES, strategy="closest")
        salon_a_rep = next(s for s in deduped if s["booksy_id"] == 100)
        assert salon_a_rep["service_id"] == 1  # similarity 0.92

        salon_b_rep = next(s for s in deduped if s["booksy_id"] == 200)
        assert salon_b_rep["service_id"] == 5  # similarity 0.95

    def test_single_sample_salon_returns_that_sample(self):
        samples = [SALON_A_1, SALON_B_1]
        deduped, _ = dedup_by_salon(samples, strategy="closest")
        assert len(deduped) == 2
        service_ids = {s["service_id"] for s in deduped}
        assert service_ids == {1, 4}


# ---------------------------------------------------------------------------
# strategy="median_price"
# ---------------------------------------------------------------------------

class TestStrategyMedianPrice:
    def test_median_price_salon_a_odd_count(self):
        # Salon A: ceny [5000, 5500, 6000] → median_low = 5500 → SALON_A_2
        deduped, _ = dedup_by_salon(FIVE_SAMPLES, strategy="median_price")
        salon_a_rep = next(s for s in deduped if s["booksy_id"] == 100)
        assert salon_a_rep["price_grosze"] == 5500
        assert salon_a_rep["service_id"] == 2

    def test_median_price_salon_b_even_count(self):
        # Salon B: ceny [6500, 7000] → median_low = 6500 → SALON_B_1
        deduped, _ = dedup_by_salon(FIVE_SAMPLES, strategy="median_price")
        salon_b_rep = next(s for s in deduped if s["booksy_id"] == 200)
        assert salon_b_rep["price_grosze"] == 6500
        assert salon_b_rep["service_id"] == 4

    def test_median_price_tie_takes_first(self):
        # 4 próbki z cenami [100, 200, 300, 400] → median_low = 200 (indeks 1)
        samples = [
            make_sample(10, 300, 30, "Salon C", "Usługa A", 100, 30, 0.80),
            make_sample(11, 300, 30, "Salon C", "Usługa B", 200, 30, 0.79),
            make_sample(12, 300, 30, "Salon C", "Usługa C", 300, 30, 0.78),
            make_sample(13, 300, 30, "Salon C", "Usługa D", 400, 30, 0.77),
        ]
        deduped, _ = dedup_by_salon(samples, strategy="median_price")
        assert len(deduped) == 1
        # median_low([100,200,300,400]) = 200 → sample z service_id=11
        assert deduped[0]["service_id"] == 11


# ---------------------------------------------------------------------------
# booksy_id = None
# ---------------------------------------------------------------------------

class TestNoneBookyId:
    def test_none_booksy_id_each_treated_as_own_salon(self):
        samples = [
            make_sample(20, None, 40, "Anonimowy 1", "Usługa X", 3000, 45, 0.88),
            make_sample(21, None, 41, "Anonimowy 2", "Usługa Y", 3500, 45, 0.82),
        ]
        deduped, meta = dedup_by_salon(samples, strategy="closest")
        # Dwa różne service_id → dwa reprezentantów
        assert len(deduped) == 2
        assert meta["n_unique_salons"] == 2
        assert meta["n_dropped"] == 0

    def test_none_booksy_id_not_grouped_with_real_salons(self):
        samples = [
            make_sample(30, None, 50, "Anonimowy", "Usługa Z", 4000, 60, 0.70),
            make_sample(31, 100, 10, "Salon Aneta", "Manicure", 5000, 60, 0.90),
        ]
        deduped, meta = dedup_by_salon(samples, strategy="closest")
        assert len(deduped) == 2
        assert meta["n_unique_salons"] == 2

    def test_same_service_id_none_is_single_salon(self):
        # Dwa sample z booksy_id=None ale TYM SAMYM service_id → nie powinno
        # się zdarzać w praktyce, ale dedup powinien zwrócić 1 reprezentanta.
        samples = [
            make_sample(40, None, 60, "X", "Usługa", 2000, 30, 0.80),
            make_sample(40, None, 60, "X", "Usługa duplikat", 2500, 30, 0.75),
        ]
        deduped, meta = dedup_by_salon(samples, strategy="closest")
        assert len(deduped) == 1
        assert meta["n_dropped"] == 1


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------

class TestImmutability:
    def test_input_list_not_mutated(self):
        original = copy.deepcopy(FIVE_SAMPLES)
        dedup_by_salon(FIVE_SAMPLES, strategy="closest")
        assert FIVE_SAMPLES == original

    def test_input_dicts_not_mutated(self):
        sample = make_sample(99, 999, 99, "Test", "Usługa", 1000, 30, 0.99)
        original = copy.deepcopy(sample)
        dedup_by_salon([sample], strategy="closest")
        assert sample == original

    def test_returned_dicts_are_copies(self):
        deduped, _ = dedup_by_salon(FIVE_SAMPLES, strategy="closest")
        # Mutowanie zwróconego dict nie powinno wpływać na oryginał
        rep = deduped[0]
        original_price = SALON_A_1["price_grosze"]
        rep["price_grosze"] = -1
        assert SALON_A_1["price_grosze"] == original_price


# ---------------------------------------------------------------------------
# Walidacja strategii
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_unknown_strategy_raises_value_error(self):
        with pytest.raises(ValueError, match="Nieznana strategia"):
            dedup_by_salon(FIVE_SAMPLES, strategy="unknown_strategy")
