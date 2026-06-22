"""Tests for layer_sufficiency.assess_sufficiency."""

import pytest

from services.similarity_pricing.layer_sufficiency import assess_sufficiency


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_sample(
    service_id: int,
    booksy_id: int | None,
    salon_id: int = 1,
    salon_name: str = "Salon X",
    service_name: str = "Masaż klasyczny",
    price_grosze: int = 15000,
    duration_minutes: int = 60,
    similarity: float = 0.90,
    category_name: str = "Masaż",
    is_package: bool = False,
) -> dict:
    """Return a fully-shaped sample matching fn_find_related_v2 output."""
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


def _make_cluster(n_salons: int, samples_per_salon: int = 1) -> list[dict]:
    """Create a cluster with n_salons distinct booksy_ids, each with samples_per_salon rows."""
    cluster: list[dict] = []
    svc_id = 1
    for salon_idx in range(n_salons):
        booksy_id = 1000 + salon_idx
        for _ in range(samples_per_salon):
            cluster.append(
                _make_sample(
                    service_id=svc_id,
                    booksy_id=booksy_id,
                    salon_id=100 + salon_idx,
                    salon_name=f"Salon {salon_idx}",
                    service_name="Masaż klasyczny",
                    price_grosze=15000 + salon_idx * 100,
                )
            )
            svc_id += 1
    return cluster


# ---------------------------------------------------------------------------
# Core classification tests
# ---------------------------------------------------------------------------


def test_sufficient_5_unique_salons_6_samples() -> None:
    """6 samples across 5 unique salons → 'sufficient', confidence=1.0."""
    cluster = _make_cluster(n_salons=4, samples_per_salon=1)
    # 5th salon contributes 2 samples
    cluster.append(_make_sample(service_id=99, booksy_id=1099, salon_id=199))
    cluster.append(_make_sample(service_id=100, booksy_id=1099, salon_id=199))

    status, meta = assess_sufficiency(cluster)

    assert status == "sufficient"
    assert meta["n_unique_salons"] == 5
    assert meta["n_samples"] == 6
    assert meta["confidence"] == 1.0


def test_thin_3_unique_salons_4_samples() -> None:
    """4 samples with 3 unique salons → 'thin'."""
    cluster = _make_cluster(n_salons=2, samples_per_salon=1)
    # 3rd salon contributes 2 samples
    cluster.append(_make_sample(service_id=50, booksy_id=2000, salon_id=200))
    cluster.append(_make_sample(service_id=51, booksy_id=2000, salon_id=200))

    status, meta = assess_sufficiency(cluster)

    assert status == "thin"
    assert meta["n_unique_salons"] == 3
    assert meta["n_samples"] == 4


def test_insufficient_1_unique_salon_2_samples_low_confidence() -> None:
    """2 samples from 1 salon → 'insufficient', confidence < 0.5."""
    cluster = [
        _make_sample(service_id=1, booksy_id=9001),
        _make_sample(service_id=2, booksy_id=9001),
    ]

    status, meta = assess_sufficiency(cluster)

    assert status == "insufficient"
    assert meta["n_unique_salons"] == 1
    assert meta["n_samples"] == 2
    assert meta["confidence"] < 0.5


def test_empty_list_insufficient() -> None:
    """Empty list → 'insufficient', n_unique_salons=0, confidence=0.0."""
    status, meta = assess_sufficiency([])

    assert status == "insufficient"
    assert meta["n_unique_salons"] == 0
    assert meta["n_samples"] == 0
    assert meta["confidence"] == 0.0


# ---------------------------------------------------------------------------
# Configurable thresholds
# ---------------------------------------------------------------------------


def test_custom_threshold_changes_status() -> None:
    """Same 5-salon cluster → 'sufficient' at default, 'thin' at min_salons_sufficient=10."""
    cluster = _make_cluster(n_salons=5, samples_per_salon=2)

    status_default, _ = assess_sufficiency(cluster)
    status_strict, meta_strict = assess_sufficiency(
        cluster, min_salons_sufficient=10, min_salons_thin=5
    )

    assert status_default == "sufficient"
    assert status_strict == "thin"
    assert meta_strict["n_unique_salons"] == 5


def test_custom_thin_threshold() -> None:
    """2 unique salons is 'insufficient' at default min_salons_thin=3,
    but 'thin' when min_salons_thin=2."""
    cluster = _make_cluster(n_salons=2, samples_per_salon=1)

    status_default, _ = assess_sufficiency(cluster)
    status_relaxed, _ = assess_sufficiency(
        cluster, min_salons_sufficient=5, min_salons_thin=2
    )

    assert status_default == "insufficient"
    assert status_relaxed == "thin"


# ---------------------------------------------------------------------------
# booksy_id=None (own salon) counted as distinct entries per service_id
# ---------------------------------------------------------------------------


def test_none_booksy_id_counted_as_own_salon() -> None:
    """Samples with booksy_id=None are own-salon rows.
    Each distinct service_id with None booksy_id is a separate identity bucket.
    """
    cluster = [
        _make_sample(service_id=1, booksy_id=None),
        _make_sample(service_id=2, booksy_id=None),
        _make_sample(service_id=3, booksy_id=None),
        _make_sample(service_id=4, booksy_id=None),
        _make_sample(service_id=5, booksy_id=None),
    ]

    status, meta = assess_sufficiency(cluster)

    assert meta["n_unique_salons"] == 5
    assert status == "sufficient"


def test_none_booksy_id_same_service_id_counted_once() -> None:
    """Multiple samples with booksy_id=None and the same service_id should count as one."""
    cluster = [
        _make_sample(service_id=1, booksy_id=None),
        _make_sample(service_id=1, booksy_id=None),
        _make_sample(service_id=1, booksy_id=None),
    ]

    _, meta = assess_sufficiency(cluster)

    assert meta["n_unique_salons"] == 1


def test_mixed_none_and_real_booksy_ids() -> None:
    """Mix of None booksy_id (own salon) and real ones is counted correctly."""
    cluster = [
        _make_sample(service_id=1, booksy_id=None),        # own salon, svc 1
        _make_sample(service_id=2, booksy_id=None),        # own salon, svc 2
        _make_sample(service_id=10, booksy_id=1001),       # external salon A
        _make_sample(service_id=11, booksy_id=1001),       # external salon A (duplicate)
        _make_sample(service_id=12, booksy_id=1002),       # external salon B
    ]

    status, meta = assess_sufficiency(cluster)

    # 2 own-salon identities + 2 external booksy_ids = 4 unique salons
    assert meta["n_unique_salons"] == 4
    assert status == "thin"


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_does_not_mutate_input() -> None:
    """assess_sufficiency must not mutate the input list or its dicts."""
    original = [
        _make_sample(service_id=1, booksy_id=1001),
        _make_sample(service_id=2, booksy_id=1002),
    ]
    snapshot_ids = [id(s) for s in original]
    snapshot_copies = [s.copy() for s in original]

    assess_sufficiency(original)

    assert [id(s) for s in original] == snapshot_ids
    for original_sample, copy in zip(original, snapshot_copies):
        assert original_sample == copy


# ---------------------------------------------------------------------------
# Confidence monotonicity
# ---------------------------------------------------------------------------


def test_confidence_monotonically_increases_with_salons() -> None:
    """Confidence must increase (or stay equal) as n_unique_salons grows."""
    confidences = []
    for n in range(0, 8):
        cluster = _make_cluster(n_salons=n)
        _, meta = assess_sufficiency(cluster)
        confidences.append(meta["confidence"])

    for i in range(1, len(confidences)):
        assert confidences[i] >= confidences[i - 1], (
            f"Confidence dropped from {confidences[i-1]} to {confidences[i]} "
            f"going from {i-1} to {i} salons"
        )


def test_confidence_caps_at_1() -> None:
    """Confidence must never exceed 1.0 regardless of salon count."""
    cluster = _make_cluster(n_salons=20, samples_per_salon=3)
    _, meta = assess_sufficiency(cluster)
    assert meta["confidence"] <= 1.0
