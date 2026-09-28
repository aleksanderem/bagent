"""Klasyfikacja rodzaju po drzewie-podziale (v8) i porównanie z regułą rodziny (bd BEAUTY_AUDIT-qbe4).

Drzewo (podzial.json) buduje automatycznie scripts/typesafe/podzial_rodzajow.py:
korzenie = zabiegi bez rodzica, dzieci = odmiany. Klasyfikacja jak w dokumentacji
TypeSafe („Walking a taxonomy”, cookbook hierarchical_classification):
  1. dziedzina — wybór spośród branż (przykłady: ich korzenie),
  2. korzeń — wybór w BEAM najprawdopodobniejszych dziedzinach; każda opcja ma
     przykłady nazw z cenników i listę swoich odmian (model widzi, co leży pod
     gałęzią, zanim ją wybierze),
  3. odmiana — wybór spośród dzieci wybranego korzenia + „ogólnie”: nazwa, która
     nie mówi, o którą odmianę chodzi („Manicure”, „Depilacja nóg”), zostaje na
     poziomie ogólnym zamiast zgadywać (v7 zgadywał — 10% niespójności dla
     identycznych nazw w różnych salonach),
  4. cechy wybranego rodzaju — jak v7 (schemat.cechy_dla), z cech podziału.

Porównanie = schemat.porownaj na drzewie podziału + trzy reguły:
  * rodzina: dwa różne rodzaje pod tym samym korzeniem → powiązane, nie różne;
  * identyczna nazwa (bez interpunkcji i odstępów) w tej samej rodzinie → ta sama
    — ten sam tekst, a różnica rodzaju pochodzi tylko z kontekstu salonu
    (26.09: „Masaż pleców” ↔ „Masaż  pleców” wychodziło „niepełne”);
  * zestaw albo dodatek w nazwie tylko po jednej stronie → powiązane (decyzja
    Alexa 25.09); pytania o nie zgubiły się przy przejściu na schemat (v5–v7),
    stąd „Laminowanie rzęs + farbowanie” = „Laminacja rzęs”.
"""

from __future__ import annotations

from typing import Any

from typesafe_sdk import Choice

import re

from services.typesafe_profile.weto import NIE, TAK

from .klasyfikacja import PYTANIA_WSPOLNE
from .schemat import MODEL, PEWNY, _pytania_cech, porownaj

WERSJA = 8
BEAM = 3
MIN_KRAWEDZ = 0.05
OGOLNIE = "ogolnie"
MAX_OPCJI = 240


def dzieci(podzial: dict) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for dz, ro in podzial["rodzic"].items():
        out.setdefault(podzial["korzen"].get(ro, ro), []).append(dz)
    return {k: sorted(v, key=lambda x: -podzial["liczn"].get(x, 0)) for k, v in out.items()}


def _opis(podzial: dict, r: str, odmiany: list[str] | None = None) -> dict[str, Any]:
    o: dict[str, Any] = {"examples": podzial["przyklady"].get(r, [])[:3]}
    if odmiany:
        o["odmiany"] = odmiany[:12]
    return o


def pytanie_dziedzina(podzial: dict) -> dict:
    return {"dziedzina": Choice(
        instructions="Do jakiej dziedziny usług należy zabieg `usluga`? Rozstrzyga nazwa i opis, nie typ salonu.",
        criteria={b: {"examples": sorted(rs, key=lambda r: -podzial["liczn"].get(r, 0))[:15]}
                  for b, rs in podzial["branze"].items() if rs},
    )}


def pytanie_korzen(podzial: dict, dziedzina: str, dz: dict[str, list[str]]) -> Choice:
    korzenie = sorted(podzial["branze"][dziedzina], key=lambda r: -podzial["liczn"].get(r, 0))[:MAX_OPCJI]
    return Choice(
        instructions="Jaki zabieg wykonuje się w usłudze `usluga`? Wybierz najbliższą grupę zabiegów; "
                     "odmiany grupy są wymienione przy niej.",
        criteria={**{r: _opis(podzial, r, dz.get(r)) for r in korzenie},
                  "inny": {"what": "zabieg spoza tej listy"}},
    )


def pytanie_odmiana(podzial: dict, korzen: str, odmiany: list[str]) -> Choice:
    return Choice(
        instructions=f"Którą odmianą zabiegu „{korzen}” jest usługa `usluga`? "
                     "Jeśli nazwa i opis nie mówią, o którą odmianę chodzi, wybierz „ogólnie”.",
        criteria={**{r: _opis(podzial, r) for r in odmiany[:MAX_OPCJI]},
                  OGOLNIE: {"what": f"{korzen} bez wskazania odmiany",
                            "not_for": "nazwa lub opis wprost wskazują jedną z odmian"}},
    )


async def rodzaj_v8(client: Any, st: dict, podzial: dict, dz: dict[str, list[str]], tok: list) -> dict[str, Any]:
    r1 = await client.system_one(st, pytanie_dziedzina(podzial), model=MODEL)
    tok[0] += r1.usage.input_tokens or 0
    dzd = sorted(r1.choices["dziedzina"].probabilities.items(), key=lambda kv: -kv[1])[:BEAM]
    q = {f"k|{b}": pytanie_korzen(podzial, b, dz) for b, p in dzd if p >= MIN_KRAWEDZ}
    r2 = await client.system_one(st, q, model=MODEL)
    tok[0] += r2.usage.input_tokens or 0
    sciezki = []
    for b, p in dzd:
        a = r2.choices.get(f"k|{b}")
        if a is not None:
            pr = a.probabilities.get(a.choice, 0.0)
            sciezki.append([b, a.choice, round((p * pr) ** 0.5, 3)])
    sciezki.sort(key=lambda x: (x[1] == "inny", -x[2]))
    korzen = sciezki[0][1] if sciezki else "inny"
    pewnosc = sciezki[0][2] if sciezki else 0.0
    rodzaj, p_odm = korzen, None
    if korzen in dz:
        r3 = await client.system_one(st, {"o": pytanie_odmiana(podzial, korzen, dz[korzen])}, model=MODEL)
        tok[0] += r3.usage.input_tokens or 0
        o = r3.choices["o"]
        p_odm = round(o.probabilities.get(o.choice, 0.0), 3)
        # odmiana tylko przy wyraźnej większości — inaczej zostaje poziom ogólny
        if o.choice != OGOLNIE and p_odm >= 0.5:
            rodzaj = o.choice
    return {"rodzaj": rodzaj, "korzen": korzen, "pewnosc": pewnosc, "p_odmiany": p_odm, "sciezki": sciezki[:3]}


async def cechy_v8(client: Any, st: dict, rodzaj: str, podzial: dict, tok: list) -> dict[str, str]:
    from .schemat import NIEUSTALONE

    q = _pytania_cech(rodzaj, podzial["cechy"].get(rodzaj, {}))
    if not q or rodzaj == "inny":
        return {}
    r = await client.system_one(st, q, model=MODEL)
    tok[0] += r.usage.input_tokens or 0
    return {k.split("|", 1)[1]: (a.choice if a.probabilities.get(a.choice, 0) >= 0.5 else NIEUSTALONE)
            for k, a in r.choices.items()}


async def dodatki_v8(client: Any, st: dict, tok: list) -> dict[str, float]:
    """Zestaw kilku zabiegów i dodatek w nazwie — dwa pytania tak/nie (jak w klasyfikacja.py)."""
    r = await client.system_one(st, PYTANIA_WSPOLNE, model=MODEL)
    tok[0] += r.usage.input_tokens or 0
    return {k: round(a.noul, 3) for k, a in r.nouls.items()}


def _nazwa(n: str | None) -> str:
    return " ".join(re.sub(r"[^\w]+", " ", (n or "").lower()).split())


def _jednostronne(a: dict, b: dict, klucz: str) -> bool:
    pa, pb = a.get(klucz), b.get(klucz)
    return pa is not None and pb is not None and ((pa >= TAK and pb <= NIE) or (pb >= TAK and pa <= NIE))


def jako_schemat(podzial: dict) -> dict:
    """Podział w kształcie, którego oczekuje schemat.porownaj."""
    return {"rodzaje": {k: {"cechy": podzial["cechy"].get(k, {})} for k in podzial["kanoniczne"]},
            "rodzice": podzial["rodzic"]}


def porownaj_v8(a: dict | None, b: dict | None, podzial: dict, fa: dict | None = None, fb: dict | None = None,
                _sch: dict | None = None) -> tuple[str, str]:
    sch = _sch or jako_schemat(podzial)
    if a and b and "inny" not in (a["rodzaj"], b["rodzaj"]):
        ka = podzial["korzen"].get(a["rodzaj"], a["rodzaj"])
        kb = podzial["korzen"].get(b["rodzaj"], b["rodzaj"])
        if fa and fb and ka == kb and _nazwa(fa.get("nazwa")) and _nazwa(fa.get("nazwa")) == _nazwa(fb.get("nazwa")):
            return "tozsame", "identyczna nazwa"
        for klucz, powod in (("zestaw", "zestaw"), ("rozszerzenie", "dodatek")):
            if _jednostronne(a, b, klucz):
                return "powiazane", powod
    w, pw = porownaj(a, b, sch, fa, fb)
    if w == "rozne" and pw == "inny rodzaj" and a and b:
        if podzial["korzen"].get(a["rodzaj"], a["rodzaj"]) == podzial["korzen"].get(b["rodzaj"], b["rodzaj"]):
            return "powiazane", "ta sama rodzina"
    return w, pw


__all__ = ["WERSJA", "PEWNY", "rodzaj_v8", "cechy_v8", "dodatki_v8", "porownaj_v8", "jako_schemat", "dzieci"]
