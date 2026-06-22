"""Warstwa C — korekta klastra bliźniaków przez kontekst kategorii właściciela.

Problem: embedding nazwy usługi grupuje po semantyce słowa, nie po sensie zabiegu.
Dla nazw GENERYCZNYCH (Konsultacja, Wizyta, Pakiet…) kategoria właściciela niesie sens,
którego sama nazwa nie ma. Dla nazw SPECYFICZNYCH (Presoterapia drenaż limfatyczny,
Botoks na włosy) embedding wystarcza i kategoria prawie nie zmienia klastra.

Rozwiązanie: parametryczne „pokrętło similarity↔taksonomia" (weight 0.0–1.0).
  weight=0  → czyste embedding, brak filtrowania
  weight=1  → twardy filtr kategorii (tylko zgodne)
  pośrednie → dla nazw generycznych usuwa niezgodne gdy weight >= 0.5, inaczej
              tylko oznacza; dla nazw specyficznych jedynie dodaje boost informacyjnie.

Funkcja jest czysta — nie mutuje wejścia. Tylko stdlib.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

# ---------------------------------------------------------------------------
# Zestaw wzorców generycznych nazw (lower-case, bez diakrytyków)
# ---------------------------------------------------------------------------

# Dokładne dopasowanie (cała nazwa = jeden z tych tokenów lub krótka fraza)
_GENERIC_EXACT: frozenset[str] = frozenset(
    [
        "konsultacja",
        "wizyta",
        "wizyta kontrolna",
        "zabieg",
        "pakiet",
        "sesja",
        "kontrola",
        "porada",
        "diagnostyka",
        "dobor",          # dobór bez diakrytyki
        "spotkanie",
        "voucher",
        "bon",
        "bestseller",
        "promocja",
        "oferta",
        "dla niego",
        "dla niej",
        "dzien dobry",    # dzień dobry
        "wizyta wstepna", # wstępna
        "wizyta poczatkowa",
    ]
)

# Wzorzec: nazwa zaczyna się od słowa generycznego i jest "krótka" (<=4 tokeny)
# bez żadnego konkretnego zabiegu/części ciała/marki po nim.
_GENERIC_PREFIX_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^konsultacja$"),
    re.compile(r"^konsultacja\s+(wstepna|poczatkowa|kontrolna|indywidualna|pierwsza|bezplatna|free)$"),
    re.compile(r"^wizyta(\s+(kontrolna|wstepna|poczatkowa|indywidualna))?$"),
    re.compile(r"^zabieg$"),
    re.compile(r"^pakiet$"),
    re.compile(r"^sesja$"),
    re.compile(r"^kontrola$"),
    re.compile(r"^porada$"),
    re.compile(r"^diagnostyka$"),
    re.compile(r"^dobor$"),
    re.compile(r"^spotkanie$"),
    re.compile(r"^voucher$"),
    re.compile(r"^bon(\s+podarunkowy)?$"),
    re.compile(r"^bestseller$"),
    re.compile(r"^promocja$"),
    re.compile(r"^oferta$"),
    re.compile(r"^dla\s+(niego|niej)$"),
    re.compile(r"^dzien\s+dobry$"),
)

# Minimalna długość nazwy (w tokenach) żeby w ogóle być specyficzną
_MIN_SPECIFIC_TOKENS = 3


def _normalize(text: str) -> str:
    """Sprowadź do lower-case ASCII bez diakrytyków, usuń emoji i znaki porządkowe.

    Usuwa prefiksy w stylu "01. ", "1. ", "🌸", "—", "–".
    """
    if not text:
        return ""
    # Usuń prefiksy numeryczne: "01. ", "1. ", "I. "
    text = re.sub(r"^\s*[\dIVXivx]+\.\s*", "", text)
    # Usuń emoji i niestandardowe znaki (niebędące literami/cyframi/spacją)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    # Normalizacja Unicode → ASCII (usuwa diakrytyki)
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = nfkd.encode("ascii", "ignore").decode("ascii")
    # Znormalizuj białe znaki
    return re.sub(r"\s+", " ", ascii_text).strip().lower()


def is_generic_name(name: str) -> bool:
    """Zwraca True gdy nazwa usługi jest generyczna/niespecyficzna.

    Generyczne: konsultacja, wizyta, wizyta kontrolna, zabieg, pakiet, sesja,
    kontrola, porada, diagnostyka, dobór, spotkanie, voucher, bon, dzień dobry,
    bestseller, promocja, oferta, dla niego/dla niej (gdy to całość nazwy).

    Specyficzne (zwraca False): nazwy zawierające konkretny zabieg, część ciała,
    markę lub deskryptor — np. "Konsultacja kosmetologiczna z analizą skóry",
    "Presoterapia drenaż limfatyczny", "Botoks na włosy", "Manicure hybrydowy".

    Case- i diacritic-insensitive. Dopasowanie na całości lub dominującej części
    krótkiej nazwy (≤ _MIN_SPECIFIC_TOKENS tokenów).
    """
    if not name or not name.strip():
        return False

    normalized = _normalize(name)
    if not normalized:
        return False

    # Krótka nazwa: sprawdź dokładne dopasowanie
    tokens = normalized.split()

    # Sprawdź w zbiorze dokładnych fraz
    if normalized in _GENERIC_EXACT:
        return True

    # Sprawdź wzorce regex (pokrywają drobne warianty)
    for pattern in _GENERIC_PREFIX_PATTERNS:
        if pattern.match(normalized):
            return True

    # Długa nazwa (> _MIN_SPECIFIC_TOKENS tokenów) z generycznym początkiem jest
    # uważana za specyficzną (ma deskryptor — "Konsultacja kosmetologiczna z…")
    # Wyjątek: dokładne wzorce powyżej już ją odrzuciły.
    _ = tokens  # używamy tokens tylko do długości; wzorce regex robią całą pracę
    return False


# ---------------------------------------------------------------------------
# Normalizacja kategorii do porównania
# ---------------------------------------------------------------------------

def _normalize_category(cat: str | None) -> str | None:
    """Normalizuj kategorię: lower-case, bez diakrytyków, bez emoji/cyfr porządkowych.

    Zwraca None gdy wynik jest pusty (np. wejście "🌸🌸🌸" lub "—").
    """
    if not cat:
        return None
    normalized = _normalize(cat)
    if not normalized or normalized in ("-", "—", "–"):
        return None
    return normalized


def _categories_match(cat_a: str | None, cat_b: str | None) -> bool:
    """Sprawdź czy dwie znormalizowane kategorie są zgodne.

    Dopasowanie przez podzbiór tokenów (substring/overlap), żeby "medycyna estetyczna"
    pasowało do "zabiegi medycyny estetycznej", "gabinet medycyny estetycznej" itp.
    """
    if cat_a is None or cat_b is None:
        return False
    # Podzbiór tokenów: każdy token z krótszej kategorii musi być w tokenach dłuższej
    tokens_a = set(cat_a.split())
    tokens_b = set(cat_b.split())
    # Oblicz nakładanie się
    overlap = tokens_a & tokens_b
    if not overlap:
        return False
    # Wymagamy co najmniej 50% tokenów krótszej frazy w dłuższej
    shorter = min(len(tokens_a), len(tokens_b))
    return len(overlap) / shorter >= 0.5


# ---------------------------------------------------------------------------
# Główna funkcja warstwy C
# ---------------------------------------------------------------------------

def apply_category_context(
    subject: dict[str, Any],
    samples: list[dict[str, Any]],
    weight: float = 0.5,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Skoryguj klaster bliźniaków o zgodność kategorii właściciela.

    Parametry:
        subject: Słownik usługi właściciela (musi mieć "service_name", opcjonalnie
                 "category_name").
        samples: Lista potencjalnych bliźniaków (każdy z "category_name" i innymi
                 polami jak w kształcie wyjścia fn_find_related_v2).
        weight:  Siła korekty 0.0–1.0.
                   0.0 → brak filtrowania (wszystkie samples, boost informacyjnie)
                   1.0 → twardy filtr (tylko zgodne kategorie; safety: jeśli 0 zostałoby,
                          zwróć wszystkie z adnotacją w meta)
                   0.5 → domyślnie: miękkie — dla nazw generycznych usuwa niezgodne,
                          dla specyficznych tylko oznacza.

    Zwraca:
        (samples', meta) gdzie:
          samples' — nowa lista słowników z dodanym polem "category_boost" (1.0/0.0)
          meta     — {"was_generic": bool, "weight": float, "n_in": int, "n_out": int,
                       "n_category_match": int, "safety_fallback": bool}

    Nie mutuje wejścia.
    """
    weight = max(0.0, min(1.0, float(weight)))

    service_name: str = subject.get("service_name") or ""
    subject_cat_raw: str | None = subject.get("category_name")
    subject_cat = _normalize_category(subject_cat_raw)

    was_generic = is_generic_name(service_name)
    n_in = len(samples)

    # Oznacz każdy sample polem category_boost (nie mutujemy — tworzymy nowe dicty)
    annotated: list[dict[str, Any]] = []
    for s in samples:
        sample_cat = _normalize_category(s.get("category_name"))
        boost = 1.0 if _categories_match(subject_cat, sample_cat) else 0.0
        annotated.append({**s, "category_boost": boost})

    n_category_match = sum(1 for s in annotated if s["category_boost"] == 1.0)

    # Decyzja o filtrowaniu
    safety_fallback = False

    if weight == 0.0:
        # Brak filtrowania — wszystkie samples, boost informacyjnie
        result = annotated

    elif weight == 1.0 and was_generic:
        # Twardy filtr — zostają tylko zgodne
        matched = [s for s in annotated if s["category_boost"] == 1.0]
        if matched:
            result = matched
        else:
            # Safety: nie zwracaj pustej listy z winy filtra
            result = annotated
            safety_fallback = True

    elif was_generic and weight >= 0.5:
        # Miękkie filtrowanie — usuń niezgodne dla generycznej nazwy
        matched = [s for s in annotated if s["category_boost"] == 1.0]
        if matched:
            result = matched
        else:
            # Safety: nie zwracaj pustej listy z winy filtra
            result = annotated
            safety_fallback = True

    else:
        # Dla nazw niespecyficznych z weight < 0.5 LUB nazw specyficznych (dowolny weight)
        # Zachowaj wszystkie — boost tylko informacyjny
        result = annotated

    meta: dict[str, Any] = {
        "was_generic": was_generic,
        "weight": weight,
        "n_in": n_in,
        "n_out": len(result),
        "n_category_match": n_category_match,
        "safety_fallback": safety_fallback,
    }

    return result, meta
