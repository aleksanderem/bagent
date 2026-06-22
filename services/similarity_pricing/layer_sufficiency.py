"""Layer B — Data sufficiency assessment for similarity-based pricing clusters.

Classifies whether a cluster of similar service samples has enough unique salons
to produce a trustworthy market price. Returns a status label and metadata dict.
"""


def assess_sufficiency(
    samples: list[dict],
    min_salons_sufficient: int = 5,
    min_salons_thin: int = 3,
) -> tuple[str, dict]:
    """Oceń czy klaster ma dość unikalnych salonów na wiarygodną cenę rynkową.

    Liczy unikalne salony po booksy_id (None => własny salon, klucz fallback service_id).

    status:
      "sufficient"   gdy n_unique_salons >= min_salons_sufficient
      "thin"         gdy min_salons_thin <= n_unique_salons < min_salons_sufficient
      "insufficient" gdy n_unique_salons < min_salons_thin

    Zwraca (status, meta) gdzie meta = {"n_unique_salons": int, "n_samples": int, "confidence": float}.
    confidence: 0.0-1.0 monotonicznie rosnące z liczbą salonów
    (min(1.0, n_unique_salons / min_salons_sufficient)).

    NIE mutuje wejścia.
    """
    n_samples = len(samples)

    if n_samples == 0:
        return "insufficient", {
            "n_unique_salons": 0,
            "n_samples": 0,
            "confidence": 0.0,
        }

    seen: set[object] = set()
    for sample in samples:
        booksy_id = sample.get("booksy_id")
        if booksy_id is None:
            # Own salon: use service_id as a unique fallback key so that
            # multiple own-salon rows don't collapse into a single None bucket.
            seen.add(("__own__", sample.get("service_id")))
        else:
            seen.add(booksy_id)

    n_unique_salons = len(seen)
    confidence = min(1.0, n_unique_salons / min_salons_sufficient)

    if n_unique_salons >= min_salons_sufficient:
        status = "sufficient"
    elif n_unique_salons >= min_salons_thin:
        status = "thin"
    else:
        status = "insufficient"

    return status, {
        "n_unique_salons": n_unique_salons,
        "n_samples": n_samples,
        "confidence": confidence,
    }
