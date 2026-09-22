"""Cennik modeli — jedyne miejsce, gdzie tokeny zamieniają się w dolary.

Zasada: **nie zgadujemy cen**. Model bez wpisu ma koszt „nie wiadomo" (None) —
panel pokaże wtedy tokeny i powie wprost, że cennika brakuje, zamiast wyświetlać
wymyśloną kwotę. Wpis oznaczony `szacunek=True` to cena z publicznego cennika
dostawcy, nieskonfrontowana z naszym rachunkiem; panel dopisuje przy niej „≈".

Abonament (Z.ai): opłata jest stała, więc pojedyncze wywołanie nie kosztuje nic
dodatkowo — wpis ma cenę zero i komentarz, skąd to zero się bierze. To NIE jest
to samo co brak cennika.

Gdzie to dopisać: nowy dostawca albo nowy model = nowy wpis tutaj, w tym samym
PR co wpięcie modelu (patrz zasada „autodiagnostyka pokazuje wszystko").
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Cennik:
    """Cena za milion tokenów, osobno wejście i wyjście."""

    usd_wej_za_mln: float
    usd_wyj_za_mln: float
    zrodlo: str
    szacunek: bool = False


# Klucz: (dostawca, model) — dokładnie te wartości, które lecą do pomiaru.
CENNIK: dict[tuple[str, str], Cennik] = {
    ("typesafe", "jev-1.13.0"): Cennik(
        usd_wej_za_mln=0.042,
        usd_wyj_za_mln=0.0,
        # Sprawdzone w konsoli TypeSafe 21.09.2026: 0,3832 USD = 9,124 mln tokenów
        # wejścia (zgodność z naszym licznikiem 0,01%), wyjście nierozliczane.
        zrodlo="konsola TypeSafe, 21.09.2026",
    ),
    ("openai", "gpt-4o-mini"): Cennik(
        usd_wej_za_mln=0.15,
        usd_wyj_za_mln=0.60,
        zrodlo="publiczny cennik OpenAI — NIE potwierdzone naszym rachunkiem",
        szacunek=True,
    ),
    ("openai", "text-embedding-3-small"): Cennik(
        usd_wej_za_mln=0.02,
        usd_wyj_za_mln=0.0,
        zrodlo="publiczny cennik OpenAI — NIE potwierdzone naszym rachunkiem",
        szacunek=True,
    ),
    ("zai", "glm-5.3-flash"): Cennik(
        usd_wej_za_mln=0.0,
        usd_wyj_za_mln=0.0,
        # Abonament Z.ai: wywołanie nie kosztuje osobno, więc zero jest prawdą
        # o koszcie krańcowym, a nie brakiem danych.
        zrodlo="abonament Z.ai — koszt stały, wywołanie nie dolicza się osobno",
    ),
}

# MiniMax-M3 świadomie BEZ wpisu: nie mamy potwierdzonej ceny za tokeny na naszym
# planie, a wpisanie „mniej więcej" zrobiłoby z panelu źródło fałszywych kwot.
# Do czasu potwierdzenia panel pokazuje dla niego tokeny i „cennik nieznany".


def koszt_usd(
    dostawca: str, model: str, tokeny_wej: int | None, tokeny_wyj: int | None
) -> tuple[float | None, bool]:
    """(koszt w USD, czy to szacunek). Brak wpisu w cenniku => (None, False)."""
    c = CENNIK.get((dostawca or "", model or ""))
    if c is None:
        return None, False
    usd = (tokeny_wej or 0) * c.usd_wej_za_mln / 1_000_000 + (tokeny_wyj or 0) * c.usd_wyj_za_mln / 1_000_000
    return round(usd, 6), c.szacunek
