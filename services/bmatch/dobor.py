"""Dobór kandydatów do porównania z KART (etap 7, faza 1–2 planu b-card).

Kandydat = oferta z salonu w promieniu, której karta jest zgodna z kartą usługi salonu (te same zabiegi; metoda
i obszar zgodne, gdzie oba podane), a spośród zgodnych bierzemy K najbardziej podobnych (wspólne słowa nazwy,
identyczne składniki, ta sama marka). Pomiar 2026-10-03: „najpodobniejsze” dają 3–5× więcej par „ta sama”
niż „najbliższe”. Wybrani przez klienta konkurenci przechodzą zawsze (ponad limit), o ile karta zgodna.
"""
from __future__ import annotations

import re
from typing import Any

SLOWO = re.compile(r"[a-ząćęłńóśźż0-9]+")
POZA_BEAUTY = {"nie usługa", "usługa dla zwierząt", "usługa motoryzacyjna"}


def norm(t: Any) -> str:
    return " ".join(str(t or "").lower().split())


def skladniki(karta: dict[str, Any]) -> list[tuple[str, str, str]]:
    return [(norm(s.get("zabieg")), norm(s.get("metoda")), norm(s.get("obszar")))
            for s in (karta.get("skladniki") or []) if s.get("zabieg")]


def zabiegi(karta: dict[str, Any]) -> list[str]:
    return sorted({s[0] for s in skladniki(karta)})


def poza_beauty(karta: dict[str, Any]) -> bool:
    return any(z in POZA_BEAUTY for z in zabiegi(karta))


def _zgodne(a: tuple[str, str, str], b: tuple[str, str, str]) -> bool:
    return a[0] == b[0] and (not a[1] or not b[1] or a[1] == b[1]) and (not a[2] or not b[2] or a[2] == b[2])


def zgodne_karty(sk_a: list[Any], sk_b: list[Any]) -> bool:
    """Ta sama liczba składników i każdy składnik usługi ma zgodny odpowiednik u kandydata."""
    sk_a, sk_b = [tuple(x) for x in sk_a], [tuple(x) for x in sk_b]
    return bool(sk_a) and len(sk_a) == len(sk_b) and all(any(_zgodne(a, b) for b in sk_b) for a in sk_a)


def _slowa(t: str) -> set[str]:
    return {w for w in SLOWO.findall(str(t).lower()) if len(w) > 2}


def podobienstwo(nazwa_a: str, sk_a: list[Any], marka_a: str | None, nazwa_b: str, sk_b: list[Any], marka_b: str | None) -> float:
    a, b = _slowa(nazwa_a), _slowa(nazwa_b)
    j = len(a & b) / len(a | b) if a and b else 0.0
    rowne = [tuple(x) for x in sk_a] == [tuple(x) for x in sk_b]
    marka = bool(marka_a) and norm(marka_a) == norm(marka_b)
    return j + 0.5 * rowne + 0.3 * marka


def wybierz(usluga_nazwa: str, karta: dict[str, Any], oferty: list[dict[str, Any]], *, k: int,
            wybrani: set[int] | None = None) -> list[dict[str, Any]]:
    """oferty: wiersze fn_bcard_kandydaci (service_id, booksy_id, klucz, nazwa, skladniki, marka).
    Zwraca do k najpodobniejszych zgodnych + wszystkie zgodne od wybranych konkurentów."""
    sk = skladniki(karta)
    if not sk or poza_beauty(karta):
        return []
    zg = [o for o in oferty if zgodne_karty(sk, o.get("skladniki") or [])]
    ocena = {o["service_id"]: podobienstwo(usluga_nazwa, sk, karta.get("marka"), o.get("nazwa") or "",
                                           o.get("skladniki") or [], o.get("marka")) for o in zg}
    zg.sort(key=lambda o: -ocena[o["service_id"]])
    wynik = zg[:k]
    if wybrani:
        mam = {o["service_id"] for o in wynik}
        wynik += [o for o in zg[k:] if o["booksy_id"] in wybrani and o["service_id"] not in mam]
    return wynik
