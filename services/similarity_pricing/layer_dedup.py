"""Warstwa A — deduplikacja klastra bliźniaków per salon.

Problem: jeden salon może mieć wiele wariantów tej samej usługi
(np. "Przedłużanie 5 paznokci" ×24 z jednego salonu), co zawyża
licznik salonów i zniekształca statystyki cenowe. Siłą rynku jest
liczba UNIKALNYCH SALONÓW — nie liczba wpisów.

Rozwiązanie: zachowaj jednego reprezentanta per salon (booksy_id),
wybieranego wg strategii. Funkcja jest czysta (immutable — nie mutuje
wejścia) i nie ma zewnętrznych zależności (tylko stdlib).
"""
from __future__ import annotations

import statistics
from typing import Any


def dedup_by_salon(
    samples: list[dict[str, Any]],
    strategy: str = "closest",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Zostaw jednego reprezentanta per salon (booksy_id).

    strategy="closest":
        Reprezentant = sample o najwyższym ``similarity`` w salonie.
        Przy remisach zachowuje pierwszy napotkany w kolejności wejścia.

    strategy="median_price":
        Reprezentant = sample o cenie najbliższej medianie ``price_grosze``
        w salonie. Przy parzystej liczbie próbek przyjmuje dolną medianę
        (statistics.median_low). Przy remisach (kilka próbek tak samo
        daleko od mediany) zachowuje pierwszy napotkany o niższej
        medianie, lub pierwszy napotkany w ogóle gdy odległości są równe.

    Klucz dedup: ``booksy_id``. Jeśli ``booksy_id`` jest None, każdy
    taki sample traktowany jest jako osobny "salon" — klucz grupowania
    to wtedy ``service_id``.

    Args:
        samples: Lista słowników w kształcie wyjścia fn_find_related_v2.
        strategy: "closest" (domyślnie) lub "median_price".

    Returns:
        Krotka (deduped_samples, meta) gdzie:
          - deduped_samples: nowa lista z reprezentantami (kopie oryginalnych
            słowników — wejście nie jest mutowane).
          - meta: słownik {"n_raw": int, "n_unique_salons": int, "n_dropped": int}.

    Raises:
        ValueError: Gdy ``strategy`` jest nieznana.
    """
    if strategy not in ("closest", "median_price"):
        raise ValueError(
            f"Nieznana strategia: {strategy!r}. Dostępne: 'closest', 'median_price'."
        )

    if not samples:
        return [], {"n_raw": 0, "n_unique_salons": 0, "n_dropped": 0}

    # Grupowanie po booksy_id; None traktujemy jako unikalny per service_id.
    groups: dict[Any, list[dict[str, Any]]] = {}
    for sample in samples:
        booksy_id = sample.get("booksy_id")
        if booksy_id is None:
            key = ("__none__", sample.get("service_id"))
        else:
            key = ("__bid__", booksy_id)
        groups.setdefault(key, []).append(sample)

    deduped: list[dict[str, Any]] = []
    for group in groups.values():
        if strategy == "closest":
            representative = _pick_closest(group)
        else:
            representative = _pick_median_price(group)
        # Immutable: zwracamy płytką kopię słownika, nie referencję.
        deduped.append(dict(representative))

    n_raw = len(samples)
    n_unique = len(deduped)
    meta: dict[str, Any] = {
        "n_raw": n_raw,
        "n_unique_salons": n_unique,
        "n_dropped": n_raw - n_unique,
    }
    return deduped, meta


# ---------------------------------------------------------------------------
# Pomocniki prywatne
# ---------------------------------------------------------------------------

def _pick_closest(group: list[dict[str, Any]]) -> dict[str, Any]:
    """Zwraca sample z najwyższym similarity (przy remisie — pierwszy)."""
    return max(group, key=lambda s: (s.get("similarity") or 0.0, -group.index(s)))


def _pick_median_price(group: list[dict[str, Any]]) -> dict[str, Any]:
    """Zwraca sample o cenie najbliższej dolnej medianie (przy remisie — pierwszy)."""
    prices = [s.get("price_grosze") or 0 for s in group]
    median = statistics.median_low(prices)
    # Szukamy próbki o minimalnej odległości od mediany; przy remisie — pierwszy.
    best = min(
        enumerate(group),
        key=lambda idx_s: (abs((idx_s[1].get("price_grosze") or 0) - median), idx_s[0]),
    )
    return best[1]
