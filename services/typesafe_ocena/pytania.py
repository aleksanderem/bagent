"""Ocena pary usług — czy usługa konkurenta to TA SAMA usługa co usługa salonu.

Przepis TypeSafe „entity_alignment” (docs.typesafe.ai/cookbooks/entity_alignment):
jedna skala o trzech poziomach = trzy decyzje (inna / powiązana / ta sama) plus
trzy pytania tak/nie, które mówią, CZYM się para różni. Poziomy opisują
sytuacje, więc decyzja nie wymaga progu dopasowanego do naszych danych —
zgodne z bramką uniwersalności silnika.

To jest SĘDZIA, nie sito: pytanie jest inne niż bateria destylacji
(typesafe_profile/pytania.py), żeby pomiar nie sprawdzał własnej pracy.
Liczby (cena, czas) zostają poza modelem — porównuje je kod.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from typesafe_sdk import Noul, Score

# Zmiana treści pytań albo modelu = nowa wersja. Pamięć ocen trzyma wersję
# w kluczu, więc stare oceny nie mieszają się z nowymi.
# v2 (25.09.2026, scripts/typesafe/warianty_sedziego.py, wariant v8): instrukcje
# po angielsku (główny język treningowy Jev; dane zostają po polsku), poziom
# środkowy wymaga NAZWANEJ różnicy, a klauzula „nazwa ogólna” działa tylko, gdy
# nazwy się różnią — v1 czytał ją dosłownie i identyczne nazwy („Kolorowe rzęsy”
# po obu stronach) lądowały w „powiązanych”. Pomiar na nieoglądanej połowie
# holdoutu: trafność tak/nie 92,3% → 94,3%, wychwycone tożsame 70% → 80%,
# precyzja 97% → 95%, 5 par poprawionych / 1 pogorszona, żadna branża w dół;
# na 49 ocenach człowieka bez zmian (89,8%, 0 fałszywie tożsamych).
WERSJA = 2
MODEL = "jev-1.13.0"

A = "`usluga_a`"
B = "`usluga_b`"

POZIOMY = [
    "Two different services: a different treatment, body part or purpose — a client looking for "
    "one would not book the other instead.",
    "The same kind of treatment, but the names or categories state a concrete difference that "
    "changes what the client gets: a different area or scope, length, size or volume, stage (e.g. "
    "first application vs refill), number of sessions, a package, or a different technique. Also "
    "when the two names differ and one of them is so generic that it could mean several different "
    "treatments. Identical names are not a difference.",
    "The same service: both names describe the same treatment and differ only in spelling, word "
    "order, marketing additions or details that do not change what the client gets.",
]
WERDYKTY = ("rozne", "powiazane", "tozsame")


def _pytania() -> dict[str, Any]:
    # Pytania tak/nie są diagnostyczne (czym para się różni) — oceniane niezależnie,
    # nie wpływają na skalę; decyzję daje wyłącznie skala.
    return {
        "relacja": Score(
            instructions=(
                f"How do service {A} and service {B}, taken from the price lists of two beauty salons "
                "(names in Polish), relate to each other, considering the name, the price-list category "
                "and the salon type?"
            ),
            criteria=POZIOMY,
        ),
        "ten_sam_zabieg": Noul(
            instructions=f"Are {A} and {B} the same kind of treatment performed with the same technique?"
        ),
        "ten_sam_obszar": Noul(
            instructions=f"Do {A} and {B} cover the same body area and the same scope?"
        ),
        "ten_sam_wariant": Noul(
            instructions=(
                f"Are {A} and {B} the same variant: the same size or length, the same stage (e.g. first "
                "application, refill, removal) and the same number of sessions included?"
            )
        ),
    }


PYTANIA = _pytania()


def _norm(text: str | None) -> str:
    return " ".join((text or "").lower().split())


def strona(nazwa: str | None, kategoria: str | None, typ_salonu: str | None) -> dict[str, str]:
    """Jedna strona pary w postaci, którą widzi model (oryginalna pisownia)."""
    return {
        "nazwa": (nazwa or "").strip(),
        "kategoria_w_cenniku": (kategoria or "").strip(),
        "typ_salonu": (typ_salonu or "").strip(),
    }


def _klucz_strony(s: dict[str, str]) -> tuple[str, str, str]:
    return (_norm(s["nazwa"]), _norm(s["kategoria_w_cenniku"]), _norm(s["typ_salonu"]))


def uporzadkuj(a: dict[str, str], b: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """Kolejność stron niezależna od tego, która była „podmiotem” — pytanie jest
    symetryczne, więc para (A,B) i (B,A) to jedna ocena i jeden wpis w pamięci."""
    return (a, b) if _klucz_strony(a) <= _klucz_strony(b) else (b, a)


def para_klucz(a: dict[str, str], b: dict[str, str]) -> str:
    """Klucz pamięci = dokładnie to, co widzi model (po normalizacji wielkości
    liter i spacji). Inna kategoria albo typ salonu = inna ocena."""
    x, y = uporzadkuj(a, b)
    raw = json.dumps([_klucz_strony(x), _klucz_strony(y)], ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def stan(a: dict[str, str], b: dict[str, str]) -> dict[str, Any]:
    x, y = uporzadkuj(a, b)
    return {"usluga_a": x, "usluga_b": y}


def werdykt(score: float) -> str:
    """Najbliższy poziom skali nazywa decyzję — bez progu strojonego na danych."""
    return WERDYKTY[min(max(int(score + 0.5), 0), len(WERDYKTY) - 1)]


def wynik(result: Any) -> dict[str, Any]:
    """Odpowiedź SDK → zapisywalny słownik (liczby, bez obiektów SDK)."""
    rel = result.scores["relacja"]
    return {
        "werdykt": werdykt(rel.score),
        "score": round(rel.score, 3),
        "pewnosc": round(rel.confidence, 3),
        # SDK: probabilities poziomu skali jako {indeks: p}
        "rozklad": [round(rel.probabilities.get(i, 0.0), 3) for i in range(len(POZIOMY))],
        "cechy": {k: round(a.noul, 3) for k, a in result.nouls.items()},
    }
