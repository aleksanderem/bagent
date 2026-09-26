"""Werdykt pary z dwóch klasyfikacji — cała polityka w kodzie, bez modelu.

* ten sam zbiór rodzajów i brak sprzeczności na osiach wariantu → ta sama,
* ten sam zbiór rodzajów, ale oś wariantu podana tylko po JEDNEJ stronie
  („Depilacja woskiem” ↔ „Depilacja woskiem bikini”) → powiązane: różnica jest
  nazwana, a tożsamości nie da się wykazać. (W medianie silnika ta reguła
  została odrzucona 28.08 — tam cisza jednej strony kosztowała cenę; tu
  pytanie brzmi „czy to na pewno ta sama usługa”, więc cisza nie jest dowodem.)
* twarde dane (kod, bez modelu; progi = te same stałe co silnik wyceny):
  cena za minutę różna ≥ PRICE_RATIO_AGAINST, czas ≥ DEMOTION_DURATION_RATIO,
  inna liczba zabiegów w cenie (pakiet Booksy albo „10 zabiegów” w nazwie/opisie)
  → powiązane,
* dodatek w nazwie po jednej stronie (french, drugi preparat, exclusive) →
  powiązane (decyzja Alexa 25.09.2026),
* ten sam zbiór rodzajów, ale sprzeczna oś (obszar, etap, długość, objętość,
  zakres, pakiet, odbiorca — weto.py, te same progi co w silniku) → powiązane,
* zbiory częściowo wspólne (zestaw vs pojedynczy zabieg) → powiązane,
* rozłączne, ale z tej samej grupy pokrewieństwa → powiązane,
* rozłączne z różnych grup → różne,
* któraś strona bez rodzaju: pewne różne rodziny → różne, inaczej brak werdyktu.
"""

from __future__ import annotations

from typing import Any

import re

from services.similarity_pricing.layer_identity import DEMOTION_DURATION_RATIO, PRICE_RATIO_AGAINST
from services.typesafe_profile.weto import NIE, REGULY, TAK, veto

from .drzewo import grupa
from .klasyfikacja import PEWNA_RODZINA

OSIE_WARIANTU = tuple(c for c in REGULY if c != "metoda")


ODBIORCY_JEDNOSTRONNI = ("odb:kobiety", "odb:mezczyzni", "odb:dzieci", "odb:para")
ETAPY = ("etap:zalozenie", "etap:uzupelnienie", "etap:zdjecie", "etap:korekta", "etap:konsultacja")


def _jest(p: dict, k: str) -> bool:
    return p.get(k, 0) >= TAK


def _brak(p: dict, k: str) -> bool:
    return p.get(k, 1) <= NIE


def jednostronne(pa: dict, pb: dict) -> list[str]:
    """Osie, które jedna strona pewnie podaje, a druga pewnie pomija."""
    out = []
    for p, q in ((pa, pb), (pb, pa)):
        ob = [k for k in p if k.startswith("ob:") and _jest(p, k)]
        if ob and all(_brak(q, k) for k in ob) and not any(_jest(q, k) for k in q if k.startswith("ob:")):
            out.append("obszar")
        for os_, klucz in (("dlugosc", "dl:obecna"), ("objetosc", "obj:obecna"), ("zakres", "zakres:obecny")):
            if _jest(p, klucz) and _brak(q, klucz):
                out.append(os_)
        if any(_jest(p, k) for k in ETAPY) and all(_brak(q, k) for k in ETAPY):
            out.append("etap")
        if any(_jest(p, k) and _brak(q, k) for k in ODBIORCY_JEDNOSTRONNI):
            out.append("odbiorca")
    return sorted(set(out))


_SESJE = re.compile(r"(\d{1,2})\s*(?:x\s*)?(?:zabieg|sesj|wizyt|spotka|serii)|(?:pakiet|seria|x)\s*(\d{1,2})\b", re.I)


def liczba_sesji(fakty: dict | None) -> int | None:
    if not fakty:
        return None
    for tekst in (fakty.get("nazwa"), fakty.get("opis")):
        m = _SESJE.search(tekst or "")
        if m:
            return int(m.group(1) or m.group(2))
    return None


def _stosunek(a: float | None, b: float | None) -> float | None:
    if not a or not b or a <= 0 or b <= 0:
        return None
    return max(a, b) / min(a, b)


def twarde_roznice(fa: dict | None, fb: dict | None) -> list[str]:
    """Różnice z danych cennika, liczone kodem."""
    if not fa or not fb:
        return []
    out = []
    pa = (fa.get("price_grosze") or 0) / fa["duration_minutes"] if fa.get("duration_minutes") else None
    pb = (fb.get("price_grosze") or 0) / fb["duration_minutes"] if fb.get("duration_minutes") else None
    r = _stosunek(pa, pb)
    if r and r >= PRICE_RATIO_AGAINST:
        out.append("cena")
    r = _stosunek(fa.get("duration_minutes"), fb.get("duration_minutes"))
    if r and r >= DEMOTION_DURATION_RATIO:
        out.append("czas")
    sa, sb = liczba_sesji(fa), liczba_sesji(fb)
    if bool(fa.get("is_package")) != bool(fb.get("is_package")) or (sa or 1) != (sb or 1):
        out.append("pakiet")
    return out


def werdykt(ka: dict[str, Any] | None, kb: dict[str, Any] | None,
            pa: dict[str, Any] | None = None, pb: dict[str, Any] | None = None,
            fa: dict[str, Any] | None = None, fb: dict[str, Any] | None = None) -> tuple[str | None, str]:
    """→ (werdykt, powód). werdykt None = za mało informacji.

    pa/pb — profile osi (gdy klasyfikacja ich nie niesie); fa/fb — fakty z cennika
    (nazwa, opis, price_grosze, duration_minutes, is_package)."""
    if not ka or not kb:
        return None, "brak klasyfikacji"
    pa, pb = ka.get("osie") or pa, kb.get("osie") or pb
    ra, rb = set(ka["rodzaje"]), set(kb["rodzaje"])
    if not ra or not rb:
        pewne = ka["rodzina_pewnosc"] >= PEWNA_RODZINA and kb["rodzina_pewnosc"] >= PEWNA_RODZINA
        if pewne and ka["rodzina"] != kb["rodzina"]:
            return "rozne", "inna rodzina"
        return None, "rodzaj nieokreślony"
    if ra == rb:
        sprzeczne = veto(pa, pb, OSIE_WARIANTU) if pa and pb else []
        if sprzeczne:
            return "powiazane", "oś: " + ",".join(sprzeczne)
        jedna = jednostronne(pa, pb) if pa and pb else []
        jedna = [*jedna, *twarde_roznice(fa, fb)]
        da, db = ka.get("rozszerzenie", ka.get("dodatek")), kb.get("rozszerzenie", kb.get("dodatek"))
        if da is not None and db is not None and ((da >= TAK and db <= NIE) or (db >= TAK and da <= NIE)):
            jedna = [*jedna, "dodatek"]
        if jedna:
            return "powiazane", "jedna strona: " + ",".join(jedna)
        return "tozsame", "ten sam rodzaj"
    if ra & rb:
        return "powiazane", "zestaw"
    ga, gb = {grupa(r) for r in ra}, {grupa(r) for r in rb}
    if ga & gb:
        return "powiazane", "ta sama grupa"
    return "rozne", "inny rodzaj"
