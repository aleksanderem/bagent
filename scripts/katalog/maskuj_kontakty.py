"""Maskowanie telefonów i adresów e-mail w plikach danych katalogu usług, zanim trafią do repo (30.09).

Opisy usług z Booksy bywają z numerem salonu („tel. 600 100 200”), a GLM kopiuje go do fraz. Maskujemy WARTOŚCI
tekstowe w JSON-ie (klucze to identyfikatory albo skróty tekstu — zostają), pomijając pola z identyfikatorami
i napisy złożone z samych cyfr (numery usług). Ta sama funkcja na opisie i na frazie z niego daje ten sam wynik,
więc sprawdzenie „fraza jest w tekście oferty” działa po maskowaniu tak samo.

  python scripts/katalog/maskuj_kontakty.py plik.json [plik2.json ...]   # zapis w miejscu, wypisuje liczby
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B)]
from services.katalog_uslug.normalizacja import bez_kontaktow  # noqa: E402

POLA_ID = frozenset({"id", "a", "b", "oferta", "usluga_a", "usluga_b", "id_oferty"})  # „salon” bywa nazwą salonu (z telefonem!)
SAME_CYFRY = re.compile(r"\d+(?:#\d+)?")


def maskuj(tekst: str) -> str:
    return bez_kontaktow(tekst)


def _przejdz(x: Any, klucz: str | None = None) -> Any:
    if isinstance(x, dict):
        return {k: _przejdz(v, k) for k, v in x.items()}
    if isinstance(x, list):
        return [_przejdz(v, klucz) for v in x]
    if isinstance(x, str) and klucz not in POLA_ID and not SAME_CYFRY.fullmatch(x):
        return maskuj(x)
    return x


def maskuj_plik(plik: Path) -> int:
    """→ liczba zmienionych wartości; plik zapisany tylko, gdy coś się zmieniło (bez zmiany formatowania reszty)."""
    tekst = plik.read_text(encoding="utf-8")
    dane = json.loads(tekst)
    nowe = _przejdz(dane)
    if nowe == dane:
        return 0
    zmian = sum(1 for a, b in zip(_wartosci(dane), _wartosci(nowe), strict=True) if a != b)
    wciecie = 1 if tekst.lstrip().startswith(("{\n", "[\n")) else None
    plik.write_text(json.dumps(nowe, ensure_ascii=False, indent=wciecie), encoding="utf-8")
    return zmian


def _wartosci(x: Any):
    if isinstance(x, dict):
        for v in x.values():
            yield from _wartosci(v)
    elif isinstance(x, list):
        for v in x:
            yield from _wartosci(v)
    else:
        yield x


if __name__ == "__main__":
    for p in map(Path, sys.argv[1:]):
        if p.exists():
            print(f"{p}: zamaskowano {maskuj_plik(p)} wartości")
