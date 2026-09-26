"""Destylacja usługi według schematu z danych i porównanie „wszystkie cechy równe”.

Schemat (scripts/typesafe/schemat_osi.py) powstaje automatycznie, tymi samymi
regułami dla każdej branży: branża → rodzaje, rodzaj → cechy → wartości.

Destylacja (TypeSafe, dwa wywołania na usługę):
  1. rodzaj — wybór spośród rodzajów branży salonu (+ „inny”); gdy zwycięzca
     niepewny (< PEWNY), krok 2 idzie też dla drugiego (wiązka, cookbook
     hierarchical_classification),
  2. cechy rodzaju — dla każdej cechy wybór spośród jej wartości
     + „nie podano” + „inna”. Cechy liczbowe (liczba zabiegów, ilość) wyciąga kod.
Stan: nazwa, opis, kategoria w cenniku, typ salonu, zabieg wybrany w Booksy.
Cena i czas nie idą do modelu.

Porównanie (kod):
  * różny rodzaj → różne,
  * cecha podana po obu stronach i różna → powiązane (z nazwą cechy),
  * cecha niepodana = wartość domyślna rodzaju, jeśli ≥ 80% usług tego rodzaju,
    które ją podają, ma tę samą wartość (schemat_osi.py),
  * cecha podana tylko po jednej stronie albo „inna” → niepełne
    (brak danych to nie zgodność),
  * wszystkie cechy równe (także obie „nie podano”) i zgodne liczby → ta sama.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from typesafe_sdk import Choice

MODEL = "jev-1.13.0"
WERSJA = 7
# v6: rodzaj wybierany po WSZYSTKICH rodzajach: najpierw dziedzina (listy rodzajów
# branż GLM jako gałęzie), potem rodzaj w BEAM najbardziej prawdopodobnych
# dziedzinach, ścieżka = średnia geometryczna (cookbook hierarchical_classification).
# v5 brał listę z branży SALONU → 24% usług „inny” (henna rzęs w salonie paznokci).
BEAM = 3
PEWNY = 0.8
NIE_PODANO = "nie_podano"
INNA = "inna"
NIEUSTALONE = "nieustalone"  # model niepewny (< 0,5) — to brak wiedzy, nie „inna wartość”
OPIS_MAX = 800
ROWNOLEGLE = 6
CZAS_STOSUNEK = 2.0  # jak DEMOTION_DURATION_RATIO silnika

_SESJE = re.compile(r"(\d{1,2})\s*(?:x\s*)?(?:zabieg|sesj|wizyt|spotka)|(?:pakiet|seria|x)\s*(\d{1,2})\b", re.I)
_ILOSC = re.compile(r"(\d+(?:[.,]\d+)?)\s*(ml|okolic|partii|partie|stref|jednost|j\.|u\b|cm)", re.I)


def stan(nazwa: str, kategoria: str | None, typ: str | None, opis: str | None, zabieg_booksy: str | None) -> dict:
    u: dict[str, Any] = {"nazwa": nazwa}
    if kategoria:
        u["kategoria_w_cenniku"] = kategoria
    if typ:
        u["typ_salonu"] = typ
    if zabieg_booksy:
        u["zabieg_wybrany_przez_salon_w_booksy"] = zabieg_booksy
    if opis and opis.strip():
        u["opis"] = " ".join(opis.split())[:OPIS_MAX]
    return {"usluga": u}


def _pytanie_rodzaj(rodzaje: list[str]) -> dict:
    return {"rodzaj": Choice(
        instructions="Jaki zabieg wykonuje się w usłudze `usluga`? Rozstrzyga nazwa i opis; typ salonu to tylko kontekst.",
        criteria={**{r: None for r in rodzaje}, "inny": {"what": "zabieg spoza tej listy"}},
    )}


def _pytania_cech(rodzaj: str, cechy: dict[str, dict]) -> dict:
    q: dict[str, Any] = {}
    for c, d in cechy.items():
        if not d["wartosci"]:
            continue
        q[f"{rodzaj}|{c}"] = Choice(
            instructions=f"Jaka jest cecha „{c}” usługi `usluga` (zabieg: {rodzaj})?",
            criteria={**{v: None for v in d["wartosci"][:240]},
                      NIE_PODANO: {"what": "nazwa i opis tego nie podają"},
                      INNA: {"what": "podana wartość spoza listy"}},
        )
    return q


def liczby(nazwa: str | None, opis: str | None) -> dict[str, Any]:
    tekst = f"{nazwa or ''} | {opis or ''}"
    m = _SESJE.search(nazwa or "") or _SESJE.search(opis or "")
    il = _ILOSC.findall(nazwa or "")
    return {
        "liczba_zabiegow": int(m.group(1) or m.group(2)) if m else 1,
        "ilosc": sorted({f"{a.replace(',', '.')} {b.lower()[:5]}" for a, b in il}) or None,
        "_tekst_len": len(tekst),
    }


def _pytanie_dziedzina(branze: dict[str, list[str]]) -> dict:
    return {"dziedzina": Choice(
        instructions="Do jakiej dziedziny usług należy zabieg `usluga`? Rozstrzyga nazwa i opis, nie typ salonu.",
        criteria={b: {"examples": r[:15]} for b, r in branze.items()},
    )}


async def _rodzaj(client: Any, st: dict, schemat: dict, tok: list) -> tuple[str, float, list]:
    r1 = await client.system_one(st, _pytanie_dziedzina(schemat["branze"]), model=MODEL)
    tok[0] += r1.usage.input_tokens or 0
    dz = sorted(r1.choices["dziedzina"].probabilities.items(), key=lambda kv: -kv[1])[:BEAM]
    q = {f"r|{b}": _pytanie_rodzaj(schemat["branze"][b][:240])["rodzaj"] for b, p in dz if p >= 0.05}
    r2 = await client.system_one(st, q, model=MODEL)
    tok[0] += r2.usage.input_tokens or 0
    sciezki = []
    for b, p in dz:
        a = r2.choices.get(f"r|{b}")
        if a is None:
            continue
        pr = a.probabilities.get(a.choice, 0.0)
        sciezki.append([b, a.choice, round((p * pr) ** 0.5, 3), round(pr, 3)])
    sciezki.sort(key=lambda x: (x[1] == "inny", -x[2]))
    naj = sciezki[0] if sciezki else [None, "inny", 0.0, 0.0]
    return naj[1], naj[2], sciezki


async def cechy_dla(client: Any, st: dict, rodzaj: str, schemat: dict, tok: list) -> dict[str, str]:
    q = _pytania_cech(rodzaj, schemat["rodzaje"].get(rodzaj, {}).get("cechy", {}))
    if not q:
        return {}
    r2 = await client.system_one(st, q, model=MODEL)
    tok[0] += r2.usage.input_tokens or 0
    return {key.split("|", 1)[1]: (a.choice if a.probabilities.get(a.choice, 0) >= 0.5 else NIEUSTALONE)
            for key, a in r2.choices.items()}


async def destyluj(client: Any, schemat: dict, uslugi: dict[str, tuple]) -> tuple[dict[str, dict], int]:
    """klucz → (nazwa, kategoria, typ, opis, zabieg_booksy, branża_do_listy)."""
    sem = asyncio.Semaphore(ROWNOLEGLE)
    out: dict[str, dict] = {}
    tok = [0]

    async def jedna(k: str, n: str, kat: str | None, typ: str | None, opis: str | None, zb: str | None, br: str | None) -> None:
        st = stan(n, kat, typ, opis, zb)
        async with sem:
            try:
                rodzaj, pewnosc, sciezki = await _rodzaj(client, st, schemat, tok)
                top = [[x[1], x[2]] for x in sciezki]
                wiazka = [rodzaj] if rodzaj != "inny" else []
                q: dict[str, Any] = {}
                for r in wiazka:
                    q.update(_pytania_cech(r, schemat["rodzaje"][r]["cechy"]))
                cechy: dict[str, dict[str, str]] = {r: {} for r in wiazka}
                if q:
                    r2 = await client.system_one(st, q, model=MODEL)
                    tok[0] += r2.usage.input_tokens or 0
                    for key, a in r2.choices.items():
                        r, c = key.split("|", 1)
                        cechy[r][c] = a.choice if a.probabilities.get(a.choice, 0) >= 0.5 else NIEUSTALONE
            except Exception as e:  # noqa: BLE001
                print(f"destylacja {n[:40]!r}: {type(e).__name__}: {str(e)[:100]}")
                return
        out[k] = {"wersja": WERSJA, "rodzaj": rodzaj, "pewnosc": pewnosc,
                  "top": top[:3], "cechy": cechy, **liczby(n, opis)}

    await asyncio.gather(*[jedna(k, *v) for k, v in uslugi.items()])
    return out, tok[0]


def porownaj(a: dict | None, b: dict | None, schemat: dict, fa: dict | None = None, fb: dict | None = None) -> tuple[str, str]:
    """→ (werdykt, powód). werdykt: tozsame | powiazane | rozne | niepelne."""
    if not a or not b:
        return "niepelne", "brak destylacji"
    ra, rb = a["rodzaj"], b["rodzaj"]
    if "inny" in (ra, rb):
        return "niepelne", "rodzaj spoza schematu"
    if ra != rb:
        rodzic = schemat.get("rodzice", {})
        def przodkowie(x: str) -> set[str]:
            out: set[str] = set()
            while x in rodzic and rodzic[x] not in out and len(out) < 10:
                x = rodzic[x]
                out.add(x)
            return out
        if ra in przodkowie(rb) or rb in przodkowie(ra):
            return "niepelne", "rodzaj ogólny"
        return "rozne", "inny rodzaj"
    # ta sama nazwa w tym samym rodzaju = ten sam dowód tekstowy → ta sama usługa
    if fa and fb and fa.get("nazwa") and " ".join(fa["nazwa"].lower().split()) == " ".join((fb.get("nazwa") or "").lower().split()):
        return "tozsame", "identyczna nazwa"
    ca, cb = a["cechy"].get(ra, {}), b["cechy"].get(rb, {})
    rozne, niepelne = [], []
    for c, d in schemat["rodzaje"][ra]["cechy"].items():
        if c not in ca and c not in cb:
            continue
        dom = d.get("domyslna")
        va, vb = ca.get(c, NIE_PODANO), cb.get(c, NIE_PODANO)
        va = NIE_PODANO if va == NIEUSTALONE else va
        vb = NIE_PODANO if vb == NIEUSTALONE else vb
        if dom:  # brak podania = wartość domyślna rodzaju (z danych)
            va = dom if va == NIE_PODANO else va
            vb = dom if vb == NIE_PODANO else vb
        if INNA in (va, vb) or (NIE_PODANO in (va, vb) and va != vb):
            niepelne.append(c)
        elif va != vb:
            rozne.append(c)
    if a.get("liczba_zabiegow") != b.get("liczba_zabiegow"):
        rozne.append("liczba_zabiegow")
    if a.get("ilosc") and b.get("ilosc") and a["ilosc"] != b["ilosc"]:
        rozne.append("ilosc")
    elif bool(a.get("ilosc")) != bool(b.get("ilosc")):
        niepelne.append("ilosc")
    if fa and fb and fa.get("duration_minutes") and fb.get("duration_minutes"):
        d1, d2 = fa["duration_minutes"], fb["duration_minutes"]
        if max(d1, d2) / max(min(d1, d2), 1) >= CZAS_STOSUNEK:
            rozne.append("czas")
    if rozne:
        return "powiazane", ",".join(rozne)
    if niepelne:
        return "niepelne", ",".join(niepelne)
    return "tozsame", "wszystkie cechy równe"
