"""Warstwa D — normalizacja jednostki / pakietu.

Problem: mediana ceny klastra kłamie z dwóch powodów:
  1. Pakiety: "Presoterapia 5 zabiegów 700zł" zawyża klaster obok
     "Presoterapia 150zł". Flaga is_package pozwala je wykluczyć.
  2. Różny czas: presoterapia bywa 25/30/55/60 min — porównanie
     surowych cen myli. Normalizujemy do zł/min i mnożymy przez czas
     subjectu.

Funkcja jest czysta (immutable — nie mutuje wejścia) i używa wyłącznie
stdlib (statistics). Grosze (int) są jednostką; zaokrąglenia jawnie
udokumentowane.
"""
from __future__ import annotations

import statistics
from typing import Any


def normalize_unit(
    subject: dict[str, Any],
    samples: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Policz cenę rynkową z klastra bliźniaków, odporną na pakiety i różny czas.

    Kroki:
      1. Wyklucz samples z is_package=True (pakiety zaburzają cenę
         pojedynczej usługi). Brak klucza is_package traktujemy jako False.
      2. Z pozostałych policz: median_raw (median price_grosze), oraz zł/min
         (price_grosze/duration_minutes) dla sampli z duration>0.
      3. market_price_grosze = median(zł/min) * subject.duration_minutes
         (gdy subject ma duration>0 i są dane zł/min); inaczej
         fallback = median_raw.
      4. Policz percentyle (p25, p50, p75) ceny znormalizowanej do czasu
         subjectu (każdy sample: zł/min_sample * subject.duration) gdy mamy
         zł/min; inaczej percentyle surowych cen.
      5. deviation_pct subjectu = (subject.price_grosze - market_price_grosze)
         / market_price_grosze * 100.

    Args:
        subject: Słownik opisujący analizowaną usługę salonu klienta.
            Wymagane/opcjonalne pola:
              - price_grosze (int): cena w groszach (15000 = 150,00 zł)
              - duration_minutes (int|None): czas trwania; None lub 0 →
                fallback do median_raw
              - is_package (bool, opcjonalne): domyślnie False
        samples: Lista słowników bliźniaków z klastra embedding.
            Pola per sample:
              - price_grosze (int): cena w groszach
              - duration_minutes (int|None): czas trwania; None lub 0 →
                sample nie wchodzi do zł/min, liczony do n_zero_duration
              - is_package (bool, opcjonalne): domyślnie False

    Returns:
        Krotka (market_stats, meta) gdzie:

        market_stats = {
          "market_price_grosze": int|None,   # cena rynkowa (None gdy 0 sampli)
          "median_raw_grosze":   int|None,   # mediana surowych cen (bez pakietów)
          "p25_grosze":          int|None,   # 25-ty percentyl znorm./surowy
          "p50_grosze":          int|None,   # 50-ty percentyl (mediana)
          "p75_grosze":          int|None,   # 75-ty percentyl
          "zl_per_min_median":   float|None, # mediana zł/min; None gdy brak danych
          "deviation_pct":       float|None, # odchylenie ceny subjectu od rynku
          "n_used":              int,        # liczba nie-pakietowych sampli
        }

        meta = {
          "n_in":               int,   # liczba sampli na wejściu
          "n_packages_excluded": int,  # ile pakietów wykluczone
          "n_zero_duration":    int,   # ile sampli z duration=0/None
          "used_per_minute":    bool,  # True gdy użyto normalizacji zł/min
        }

    Raises:
        Nie rzuca wyjątków dla prawidłowych wejść. Brakujące pola
        traktowane jako None/False.

    Note:
        Zaokrąglenia: market_price_grosze i percentyle zaokrąglone przez
        round() do int (grosze). zł/min_median to float bez dodatkowego
        zaokrąglenia — precyzja potrzebna przy mnożeniu.
        Immutable: funkcja nigdy nie mutuje wejściowych słowników.
    """
    n_in = len(samples)

    # --- Krok 1: wykluczenie pakietów ---
    non_packages: list[dict[str, Any]] = []
    n_packages_excluded = 0
    for s in samples:
        if s.get("is_package", False):
            n_packages_excluded += 1
        else:
            non_packages.append(s)

    n_used = len(non_packages)

    # Gdy zero sampli po filtrowaniu — brak ceny rynkowej.
    if n_used == 0:
        market_stats: dict[str, Any] = {
            "market_price_grosze": None,
            "median_raw_grosze": None,
            "p25_grosze": None,
            "p50_grosze": None,
            "p75_grosze": None,
            "zl_per_min_median": None,
            "deviation_pct": None,
            "n_used": 0,
        }
        meta: dict[str, Any] = {
            "n_in": n_in,
            "n_packages_excluded": n_packages_excluded,
            "n_zero_duration": 0,
            "used_per_minute": False,
        }
        return market_stats, meta

    # --- Krok 2: median_raw i zbieranie zł/min ---
    raw_prices = [s.get("price_grosze") or 0 for s in non_packages]
    median_raw_grosze = round(statistics.median(raw_prices))

    zl_per_min_list: list[float] = []
    n_zero_duration = 0
    for s in non_packages:
        dur = s.get("duration_minutes")
        if not dur:  # None lub 0
            n_zero_duration += 1
        else:
            price = s.get("price_grosze") or 0
            zl_per_min_list.append(price / dur)

    # --- Krok 3: market_price_grosze ---
    subject_duration = subject.get("duration_minutes")
    subject_price = subject.get("price_grosze") or 0
    used_per_minute = False
    zl_per_min_median: float | None = None

    if subject_duration and zl_per_min_list:
        zl_per_min_median = statistics.median(zl_per_min_list)
        # Zaokrąglenie do grosza — int.
        market_price_grosze: int | None = round(zl_per_min_median * subject_duration)
        used_per_minute = True
    else:
        market_price_grosze = median_raw_grosze

    # --- Krok 4: percentyle ---
    # Jeśli używamy per-minute: znormalizuj każdy sample do czasu subjectu.
    # Inaczej: użyj surowych cen.
    if used_per_minute and subject_duration:
        normalized_prices = [
            round(zpm * subject_duration) for zpm in zl_per_min_list
        ]
    else:
        normalized_prices = raw_prices

    # Percentyle ręcznie (statistics.quantiles wymaga Pythona 3.8+ i n>=1).
    # Używamy metody inclusive (linear interpolation) zgodnej z konwencją Excel/numpy.
    p25_grosze = round(_percentile(normalized_prices, 25))
    p50_grosze = round(_percentile(normalized_prices, 50))
    p75_grosze = round(_percentile(normalized_prices, 75))

    # --- Krok 5: deviation_pct ---
    deviation_pct: float | None
    if market_price_grosze is not None and market_price_grosze != 0:
        deviation_pct = (subject_price - market_price_grosze) / market_price_grosze * 100
    else:
        deviation_pct = None

    market_stats = {
        "market_price_grosze": market_price_grosze,
        "median_raw_grosze": median_raw_grosze,
        "p25_grosze": p25_grosze,
        "p50_grosze": p50_grosze,
        "p75_grosze": p75_grosze,
        "zl_per_min_median": zl_per_min_median,
        "deviation_pct": deviation_pct,
        "n_used": n_used,
    }
    meta = {
        "n_in": n_in,
        "n_packages_excluded": n_packages_excluded,
        "n_zero_duration": n_zero_duration,
        "used_per_minute": used_per_minute,
    }
    return market_stats, meta


# ---------------------------------------------------------------------------
# Pomocniki prywatne
# ---------------------------------------------------------------------------

def _percentile(data: list[int | float], pct: float) -> float:
    """Percentyl metodą linear interpolation (inclusive).

    Zgodna z numpy percentile (method='linear') i Excel PERCENTILE.INC.
    Zakłada że data jest niepusta.

    Args:
        data: Lista wartości liczbowych (nie musi być posortowana).
        pct: Percentyl w przedziale [0, 100].

    Returns:
        Wartość percentyla (float).
    """
    sorted_data = sorted(data)
    n = len(sorted_data)
    if n == 1:
        return float(sorted_data[0])
    # Indeks float: pct/100 * (n-1)
    index = pct / 100.0 * (n - 1)
    lower = int(index)
    upper = lower + 1
    if upper >= n:
        return float(sorted_data[-1])
    frac = index - lower
    return sorted_data[lower] + frac * (sorted_data[upper] - sorted_data[lower])
