"""Drzewo v14 — próba i sprawdzian: drzewo do poziomu zabiegu, niżej frazy obu usług wprost (bd BEAUTY_AUDIT-asrk).

Zgoda Alexa 28.09 (warunkowa: ma działać uniwersalnie, poprawnie wyciągać taksonomię i dopasowywać).
Na usługę: przejście drzewa v13b (poziomy 1–2: dziedzina, zabieg; pozycja, tak/nie zestaw i dodatek) + jedno
wywołanie „rola każdego słowa” w pełnym kontekście (v13_role). Na parę: frazy porównane wprost; różne frazy →
TypeSafe (to samo / inna wartość / inna cecha, pamięć per zabieg i para fraz); fraza tylko w jednej usłudze →
TypeSafe na pełnym kontekście drugiej (czy i tak ją obejmuje; pamięć per usługa i fraza).

Tryby:
  --proba N     ≤ N par z danych sprawdzianu v13 (salony JUŻ UŻYTE — tylko do obejrzenia mechanizmu, nie dowód);
  (domyślnie)   NOWE salony (bramka uniwersalności), szeroka sieć, bilans z v13b na tych samych parach.
Baza produkcyjna: tylko odczyt.

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v14_sprawdzian.py --proba 40 --budzet 0.1
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v14_sprawdzian.py --tylko-zbierz
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v14_sprawdzian.py --budzet 4.5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v12_budowa as tk  # noqa: E402
import v12_sprawdzian as v12s  # noqa: E402
import v12b_budowa as vb  # noqa: E402
import v13_role as vr  # noqa: E402
import v13_sprawdzian as v13s  # noqa: E402

from services.typesafe_drzewo.drzewo_v13 import porownaj_v13, wiazka_v13  # noqa: E402
from services.typesafe_drzewo import drzewo_v14e as e14  # noqa: E402 — zamrożona v14e, tylko do bilansu
from services.typesafe_drzewo.drzewo_v14 import (  # noqa: E402
    DOKLADNIE,
    W2,
    Wariant,
    porownaj_v14,
    potrzebne_domniemania,
    potrzebne_synonimy,
    PRZYIMKI,
    poziom_score,
    profil_v14,
    potrzebne_potwierdzenia,
    potwierdz,
    pytanie_dla,
    pytanie_tozsamosci,
    stan_relacji,
)
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DZ = v13s.DZ
STARE = DZ / "v13_sprawdzian"
SEED_NOWE = 20261005  # inne losowanie niż sprawdzian v13 (20261004)
CENA_TOK = 0.042 / 1e6
TOK_ROLE = 5970   # zmierzone 28.09 (v13_role, 20 590 usług)
TOK_V13 = 12400   # zmierzone 28.09
WERSJA_PYTAN = 2  # „czy właśnie taka” = w2 (Score z przykładami); Wariant.razem używa tej samej pamięci — klucz z „+” to inne pytanie (w1 bez przykładów, w3 Choice, w4 za ściśle — odrzucone)
WARIANTY = {"w2": W2, "razem": Wariant(razem=True), "obie": Wariant(obie_strony=True),
            "razem_obie": Wariant(razem=True, obie_strony=True), "syn": Wariant(synonimy=True),
            "syn_razem": Wariant(synonimy=True, razem=True), "syn_obie": Wariant(synonimy=True, obie_strony=True),
            "syn_razem_obie": Wariant(synonimy=True, razem=True, obie_strony=True),
            "syn_zrodlo": Wariant(synonimy=True, zrodlo=True), "syn_zrodlo_obie": Wariant(synonimy=True, zrodlo=True, obie_strony=True)}
TOK_PARA = 300    # szacunek na parę: pytania o frazy i domniemania (większość par bez pytań)
ROWNOLEGLE = 30
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"


# „pakiet”, „karnet” mówią, ile wizyt obejmuje usługa (model: „gdzie i ile” — liczba). Sprawdzian 28.09: bez nich
# „PEELING WĘGLOWY” z wariantami „Pakiet 3 / 5 zabiegów” wychodził tą samą usługą co pojedynczy peeling.
STOP_V14 = (tk.STOP - {"pakiet", "pakiety", "karnet", "karnety"}) | {"albo"}  # „albo” to łącznik, jak „lub”


def slowa_v14(u: dict) -> list[str]:
    """Słowa usługi do pytania o role — te same źródła i kolejność co v13_role, bez pomijania słów pakietu."""
    out: list[str] = []
    for _zr, tekst in vb.teksty_zrodel(u):
        for m in tk.TOKEN.finditer(tekst):
            w = " ".join(m.group(0).lower().split())
            if w not in STOP_V14 and w not in out:
                out.append(w)
    return out[:vb.MAX_SLOW]


LACZNIKI = re.compile(r"[+/,;|()\[\]]|\s[-–—]\s|[–—]|\s(?:i|oraz|lub|albo)\s", re.I)
LICZBA = re.compile(r"^\d+(?:[.,]\d+)?$")


def _dolaczalne(slowo: str) -> bool:
    s = slowo.lower().strip(".:")
    return s in PRZYIMKI or bool(LICZBA.match(s))


def frazy_v14(u: dict, role: dict[str, list]) -> list[tuple[str, str, str]]:
    """Fraza = sąsiednie słowa tej samej roli w ORYGINALNYM brzmieniu, z przyimkiem i liczbą tuż przed nimi
    („do ramion”, „bez starej stylizacji”, „do 2 lat”). Próba 28.09: same słowa gubiły sens („ramion” wobec
    „średnie”, „starej stylizacji” bez „bez”). Zabieg bez przyimka: „z regulacją” kontra „regulacja” TypeSafe
    czytał jako dodatek kontra zabieg (sprawdzian 2, 28.09).
    Role słów te same co w v13_role (te same słowa, te same odpowiedzi). → [(rola, fraza, źródło)]"""
    out: list[tuple[str, str, str]] = []
    rola = lambda t: (role.get(t) or [None])[0]  # noqa: E731
    for zr, tekst in vb.teksty_zrodel(u):
        for kaw in LACZNIKI.split(f" {tekst} "):
            toks = [(m.start(), m.end(), " ".join(m.group(0).lower().split())) for m in tk.TOKEN.finditer(kaw)]
            toks = [t for t in toks if t[2] not in STOP_V14]
            i = 0
            while i < len(toks):
                r, j = rola(toks[i][2]), i
                while (j + 1 < len(toks) and rola(toks[j + 1][2]) == r
                       and all(_dolaczalne(w) or len(w) < 3 for w in kaw[toks[j][1]:toks[j + 1][0]].split())):
                    # słowo pominięte (np. „min”) przerywa frazę: sprawdzian 3 (28.09) — „włosów do 15 min twarz” jako jeden
                    # obszar, a czas trwania nie rozróżnia usług (decyzja Alexa 28.09)
                    j += 1
                if r not in (None, "nieistotne"):
                    przed = kaw[:toks[i][0]].split()
                    k = len(przed)
                    while r != "zabieg" and k > 0 and len(przed) - k < 3 and _dolaczalne(przed[k - 1]):
                        k -= 1
                    fraza = " ".join((" ".join(przed[k:]) + " " + kaw[toks[i][0]:toks[j][1]]).lower().split())
                    if (r, fraza, zr) not in out:
                        out.append((r, fraza, zr))
                i = j + 1
    return out


def pytania_rol(sl: list[str]) -> dict[str, Any]:
    """Tylko role słów (v13_role bez pozycji i tak/nie — te daje już przejście drzewa, pytanie dwa razy to strata)."""
    return {f"r{i}": q for i, q in enumerate(vr.pytania(sl)[f"r{i}"] for i in range(len(sl)))}


class Pytania:
    """TypeSafe z pamięcią w plikach; 402 = stop; budżet liczony na tokenach wejściowych."""

    def __init__(self, client: Any, out: Path, budzet: float):
        self.client, self.out, self.budzet = client, out, budzet
        self.tok, self.bledy = [0], Counter()
        self.sem = asyncio.Semaphore(ROWNOLEGLE)
        self.role = self._wczytaj("role.json")
        self.rel = self._wczytaj("tozsamosc.json")
        self.dom = self._wczytaj("cechy.json")
        self.dok = self._wczytaj(f"dokladnie_w{WERSJA_PYTAN}.json")   # v14f: Score „czy właśnie taka”

    def _wczytaj(self, nazwa: str) -> dict:
        f = self.out / nazwa
        return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}

    def zapisz(self) -> None:
        for nazwa, d in (("role.json", self.role), ("tozsamosc.json", self.rel), ("cechy.json", self.dom),
                         (f"dokladnie_w{WERSJA_PYTAN}.json", self.dok)):
            (self.out / nazwa).write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")

    async def _wolaj(self, rodzaj: str, stan: dict, pyt: dict) -> Any:
        async with self.sem:
            try:
                r = await self.client.system_one(stan, pyt, model=MODEL)
            except Exception as e:  # noqa: BLE001 — jedno pytanie bez odpowiedzi, reszta dalej
                if " 402 " in str(e):
                    # 28.09: SystemExit z wnętrza gather zawiesił proces i role policzone przed 402 przepadły —
                    # najpierw zapis pamięci, potem twarde wyjście
                    self.zapisz()
                    print(f"TypeSafe: brak kredytów — pamięć zapisana, przerywam. {str(e)[:160]}", flush=True)
                    os._exit(2)
                self.bledy[rodzaj] += 1
                return None
        self.tok[0] += r.usage.input_tokens or 0
        if self.tok[0] * CENA_TOK > self.budzet:
            raise SystemExit(f"budżet {self.budzet} USD wyczerpany")
        return r

    async def role_uslug(self, uslugi: dict[int, dict]) -> None:
        async def jedna(i: int, u: dict) -> None:
            sl = slowa_v14(u)
            r = await self._wolaj("role", stan_v12(u), pytania_rol(sl))
            if r is not None:
                self.role[str(i)] = {"role": {w: [r.choices[f"r{k}"].choice, round(r.choices[f"r{k}"].probabilities[r.choices[f"r{k}"].choice], 3)]
                                              for k, w in enumerate(sl)}}
        brak = [(i, u) for i, u in uslugi.items()
                if slowa_v14(u) and (str(i) not in self.role or any(w not in self.role[str(i)]["role"] for w in slowa_v14(u)))]
        await asyncio.gather(*[jedna(i, u) for i, u in brak])

    async def relacje(self, klucze: set[tuple[str, str, str, str]]) -> None:
        async def jedna(k: tuple[str, str, str, str]) -> None:
            r = await self._wolaj("tozsamosc", stan_relacji(k), {"r": pytanie_tozsamosci(k[1])})
            if r is not None:
                self.rel[json.dumps(k, ensure_ascii=False)] = round(r.nouls["r"].noul, 3)
        await asyncio.gather(*[jedna(k) for k in sorted(klucze) if json.dumps(k, ensure_ascii=False) not in self.rel])

    async def cechy(self, klucze: set[tuple[int, str, str, str]], uslugi: dict[int, dict]) -> None:
        """Czy usługa ma cechę / obejmuje wariant / jest właśnie taka — wszystkie pytania o jedną usługę w jednym wywołaniu."""
        po_usludze: dict[int, list[tuple[int, str, str, str]]] = defaultdict(list)
        for k in sorted(klucze):
            if json.dumps(list(k), ensure_ascii=False) not in (self.dok if k[3] == DOKLADNIE else self.dom):
                po_usludze[k[0]].append(k)

        async def jedna(uid: int, lista: list[tuple[int, str, str, str]]) -> None:  # jedna usługa na wywołanie
            r = await self._wolaj("cechy", stan_v12(uslugi[uid]), {f"d{n}": pytanie_dla(k) for n, k in enumerate(lista)})
            if r is not None:
                for n, k in enumerate(lista):
                    if k[3] == DOKLADNIE:
                        self.dok[json.dumps(list(k), ensure_ascii=False)] = round(r.scores[f"d{n}"].score, 3)
                    else:
                        self.dom[json.dumps(list(k), ensure_ascii=False)] = round(r.nouls[f"d{n}"].noul, 3)
        await asyncio.gather(*[jedna(uid, lst) for uid, lst in po_usludze.items()])

    def slownik_rel(self) -> dict:
        return {tuple(json.loads(k)): v >= 0.5 for k, v in self.rel.items()}

    def slownik_dom(self) -> dict:
        return {tuple(json.loads(k)): v >= 0.5 for k, v in self.dom.items()}

    def slownik_poz(self) -> dict:
        """Score v14f → najbliższy poziom (INNY / MOZE / TEN_SAM)."""
        return {tuple(json.loads(k)): poziom_score(v) for k, v in self.dok.items()}


def mapuj(rek: dict | None, mapa: dict[str, str]) -> dict | None:
    """Przejście drzewa v13b przepisane na węzły scalone (v14_scalanie): ta sama wiązka, węzeł zabiegu → grupa."""
    if not rek:
        return rek
    return {**rek, "zabiegi": [[d, mapa.get(g, g), pr, r] for d, g, pr, r in rek.get("zabiegi") or []],
            "sciezki": [[[s[0], mapa.get(s[1], s[1]), *s[2:]], pr, w] for s, pr, w in rek.get("sciezki") or []]}


async def _profile(p: Pytania, pary: list[dict], uslugi: dict[int, dict], r13: dict, drzewo: dict) -> dict[int, dict]:
    """Profile po rundzie 0 (wspólnej dla v14e i v14f): frazy z kontekstu potwierdzone na własnej usłudze,
    frazy zabiegu sprawdzone z węzłem."""
    await p.role_uslug({i: uslugi[i] for i in {x for q in pary for x in (int(q["a"]), int(q["b"]))}})
    surowe = {}
    for i in {x for q in pary for x in (int(q["a"]), int(q["b"]))}:
        z = p.role.get(str(i))
        surowe[i] = profil_v14(i, frazy_v14(uslugi[i], z["role"]) if z else [], r13.get(str(i)), drzewo, uslugi[i]["nazwa"])
    potw = [potrzebne_potwierdzenia(x) for x in surowe.values()]
    await asyncio.gather(p.cechy(set().union(*[c for c, _r in potw]), uslugi), p.relacje(set().union(*[r for _c, r in potw])))
    return {i: potwierdz(x, p.slownik_rel(), p.slownik_dom()) for i, x in surowe.items()}


async def porownaj_pary(p: Pytania, pary: list[dict], uslugi: dict[int, dict], r13: dict, drzewo: dict,
                        pole: str = "v14", w: Wariant = W2) -> dict[int, dict]:
    """v14f: role, w których frazy się różnią → „czy u drugiej właśnie tak” na jej pełnym kontekście (jedna usługa na wywołanie)."""
    prof = await _profile(p, pary, uslugi, r13, drzewo)
    pr = [(prof[int(q["a"])], prof[int(q["b"])]) for q in pary]
    await p.relacje(set().union(*[potrzebne_synonimy(a, b, w) for a, b in pr]))  # pytanie_tozsamosci, pamięć tozsamosc.json
    rel = p.slownik_rel()
    await p.cechy(set().union(*[potrzebne_domniemania(a, b, w, rel) for a, b in pr]), uslugi)
    poz, dom = p.slownik_poz(), p.slownik_dom()
    for q, (a, b) in zip(pary, pr):
        q[pole], q[f"{pole}_powod"], q[f"{pole}_poziom"] = porownaj_v14(a, b, poz, dom, w, rel)
    return prof


async def porownaj_pary_v14e(p: Pytania, pary: list[dict], uslugi: dict[int, dict], r13: dict, drzewo: dict,
                             pole: str = "v14e") -> None:
    """Zamrożona v14e (sprawdziany 3–4) na tych samych parach — bilans z v14f."""
    prof = await _profile(p, pary, uslugi, r13, drzewo)
    await p.relacje(set().union(*[e14.potrzebne_relacje(prof[int(q["a"])], prof[int(q["b"])]) for q in pary]))
    rel = p.slownik_rel()
    await p.cechy(set().union(*[e14.potrzebne_domniemania(prof[int(q["a"])], prof[int(q["b"])], rel) for q in pary]), uslugi)
    dom = p.slownik_dom()
    for q in pary:
        q[pole], q[f"{pole}_powod"], q[f"{pole}_poziom"] = e14.porownaj_v14(prof[int(q["a"])], prof[int(q["b"])], rel, dom)


def opis(u: dict, pr: dict | None) -> str:
    fr = " | ".join(f"{k}: {', '.join(f'{x} ({r[:4]}/{zr[:3]})' for r, x, zr in v)}" for k, v in (pr or {}).get("frazy", {}).items() if v)
    w = (pr or {}).get("wezel")
    return f"{u['nazwa'][:60]!r} [{(u.get('kategoria') or '')[:28]}] → {w[1] if w else '—'} :: {fr}"


def wybierz_probe(pary: list[dict], n: int) -> list[dict]:
    """Połowa z moich ocenionych par (181, v13b/v12b), połowa z „za mało danych” v13b przy tym samym zabiegu — po równo z branż."""
    rng = random.Random(28)
    ocenione = {(o["a"], o["b"]): o["ocena_claude"] for o in json.loads((STARE / "ocena_claude.json").read_text(encoding="utf-8"))}
    branze = sorted({q["branza"] for q in pary})
    wynik: list[dict] = []
    for zrodlo, ile in ((lambda q: (q["a"], q["b"]) in ocenione, n // 2), (lambda q: q["v13"] == "niepelne" and q["v13_poziom"] >= 2, n - n // 2)):
        na_br = max(1, ile // len(branze))
        for br in branze:
            kand = [q for q in pary if q["branza"] == br and zrodlo(q) and q not in wynik]
            wynik += rng.sample(kand, min(na_br, len(kand)))
    for q in wynik:
        q["ocena_claude"] = ocenione.get((q["a"], q["b"]))
    return wynik[:n]


async def proba(a: argparse.Namespace, client: Any) -> None:
    out = DZ / "v14_proba"
    out.mkdir(parents=True, exist_ok=True)
    d = json.loads((STARE / "uslugi.json").read_text(encoding="utf-8"))
    uslugi = {int(k): v for k, v in d["uslugi"].items()}
    pary = wybierz_probe(json.loads((STARE / "pary.json").read_text(encoding="utf-8")), a.proba)
    r13 = json.loads((STARE / "sciezki_drzewo_v13b.json").read_text(encoding="utf-8"))
    drzewo = json.loads((DZ / "v13" / "drzewo_v13b.json").read_text(encoding="utf-8"))
    szac = len({x for q in pary for x in (q["a"], q["b"])}) * TOK_ROLE * CENA_TOK + len(pary) * TOK_PARA * CENA_TOK
    print(f"par {len(pary)}, szac. {szac:.3f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet")
    p = Pytania(client, out, a.budzet)
    try:
        prof = await porownaj_pary(p, pary, uslugi, r13, drzewo)
    finally:
        p.zapisz()
    print(f"koszt {p.tok[0] * CENA_TOK:.4f} USD, błędy pytań {dict(p.bledy)}\n")
    for q in sorted(pary, key=lambda q: (q["branza"], q["v14"])):
        print(f"[{q['branza']}] v13b={q['v13']} ({q['v13_powod']}) | v14={q['v14']} ({q['v14_powod']})"
              f"{' | moja ocena: ' + q['ocena_claude'] if q.get('ocena_claude') else ''}")
        print(f"   A {opis(uslugi[int(q['a'])], prof.get(int(q['a'])))}")
        print(f"   B {opis(uslugi[int(q['b'])], prof.get(int(q['b'])))}")
    (out / "pary_proby.json").write_text(json.dumps(pary, ensure_ascii=False, indent=1), encoding="utf-8")


_UZYTE_PRZED_V14 = v13s.uzyte_salony


def uzyte_salony_v14() -> set[int]:
    """Wszystkie salony użyte dotąd: diagnozy, budowa drzew, sprawdziany v12–v13 i wszystkie wcześniejsze sprawdziany v14."""
    u = _UZYTE_PRZED_V14()
    for f in [STARE / "uslugi.json", *sorted(DZ.glob("v14_sprawdzian*/uslugi.json"))]:
        if f.exists():
            u |= {b for _br, b, _m in json.loads(f.read_text(encoding="utf-8"))["salony"]}
    return u


async def sprawdzian(a: argparse.Namespace, client: Any) -> None:
    out = DZ / a.wyjscie
    out.mkdir(parents=True, exist_ok=True)
    plik_u = out / "uslugi.json"
    if plik_u.exists():
        d = json.loads(plik_u.read_text(encoding="utf-8"))
        uslugi, pary, salony = {int(k): v for k, v in d["uslugi"].items()}, d["pary"], d["salony"]
    else:
        from services.supabase import SupabaseService
        v13s.SEED, v13s.uzyte_salony = a.ziarno, uzyte_salony_v14  # nowe losowanie, bez wszystkich użytych salonów
        uslugi, pary, salony = await v13s.zbierz(SupabaseService(), a)
        plik_u.write_text(json.dumps({"uslugi": uslugi, "pary": pary, "salony": salony}, ensure_ascii=False), encoding="utf-8")
    po_a: dict[int, list[dict]] = defaultdict(list)
    for q in pary:
        po_a[q["a"]].append(q)
    pary = [q for lst in po_a.values() for q in sorted(lst, key=lambda q: -q["sim"])[:a.kandydatow_pomiar]]
    uzyte = {q["a"] for q in pary} | {q["b"] for q in pary}
    uslugi = {i: u for i, u in uslugi.items() if i in uzyte}
    # szacunek bez tego, co już jest w pamięci (przejście drzewa i role z poprzedniego przebiegu)
    f13, frol = out / "sciezki_drzewo_v13b.json", out / "role.json"
    zrob13 = json.loads(f13.read_text(encoding="utf-8")) if f13.exists() else {}
    zrob_role = json.loads(frol.read_text(encoding="utf-8")) if frol.exists() else {}
    bez13 = sum(str(i) not in zrob13 for i in uslugi)
    bez_rol = sum(str(i) not in zrob_role or any(w not in zrob_role[str(i)]["role"] for w in slowa_v14(u)) for i, u in uslugi.items())
    szac = (bez13 * TOK_V13 + bez_rol * TOK_ROLE + len(pary) * TOK_PARA * (2 if a.mapa else 1)) * CENA_TOK
    print(f"salonów {len(salony)}, usług {len(uslugi)} (bez przejścia {bez13}, bez ról {bez_rol}), par {len(pary)}; "
          f"szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if a.tylko_zbierz:
        return
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz --kandydatow-pomiar")
    drzewo = json.loads((DZ / "v13" / "drzewo_v13b.json").read_text(encoding="utf-8"))
    p = Pytania(client, out, a.budzet)
    t13 = [0]
    try:
        r13 = await v13s.przejdz(client, uslugi, drzewo, out / "sciezki_drzewo_v13b.json", wiazka_v13, t13)
        p.tok[0] += t13[0]
        if a.mapa:  # drzewo ze scalonymi węzłami; bilans z zamrożoną v14e na tych samych parach
            mapa = json.loads((DZ / a.mapa).read_text(encoding="utf-8"))
            drzewo_e = json.loads((DZ / "v13" / a.drzewo_scalone).read_text(encoding="utf-8"))
            r13e = {i: mapuj(r, mapa) for i, r in r13.items()}
            if a.poprzednia == "v14e":  # zbiór 5: zamrożona v14e
                await porownaj_pary_v14e(p, pary, uslugi, r13e, drzewo_e)
            else:  # zbiór 6+: poprzednia wersja v14f jako „v14p”
                await porownaj_pary(p, pary, uslugi, r13e, drzewo_e, pole="v14p", w=WARIANTY[a.poprzednia])
            await porownaj_pary(p, pary, uslugi, r13e, drzewo_e, w=WARIANTY[a.wariant])
        else:
            await porownaj_pary(p, pary, uslugi, r13, drzewo)
    finally:
        p.zapisz()
    for q in pary:
        q["v13"], q["v13_powod"], q["v13_poziom"] = porownaj_v13(r13.get(str(q["a"])), r13.get(str(q["b"])))
    (out / "pary.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    wynik = miary_v14(pary, salony)
    wynik["koszt_usd"] = round(p.tok[0] * CENA_TOK, 3)
    wynik["bledy_pytan"] = dict(p.bledy)
    (out / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(wynik, ensure_ascii=False, indent=1)[:8000])


def miary_v14(pary: list[dict], salony: list) -> dict:
    grupy = {"RAZEM": pary, **{b: [q for q in pary if q["branza"] == b] for b in sorted({q["branza"] for q in pary})}}

    def pokrycie(z: list[dict], w: str) -> dict:
        return v12s.pokrycie([{**q, "werdykt": q[w], "poziom": q[f"{w}_poziom"]} for q in z])
    return {"salony": salony, "par": len(pary),
            "per_branza": {g: {"par": len(z),
                               "v14": {"werdykty": dict(Counter(q["v14"] for q in z)), "pokrycie": pokrycie(z, "v14"),
                                       "powody": dict(Counter(q["v14_powod"] for q in z).most_common(10))},
                               "v13b": {"werdykty": dict(Counter(q["v13"] for q in z)), "pokrycie": pokrycie(z, "v13")},
                               "bilans_tozsame": {"tylko_v14": sum(q["v14"] == "tozsame" != q["v13"] for q in z),
                                                  "tylko_v13b": sum(q["v13"] == "tozsame" != q["v14"] for q in z),
                                                  "obie": sum(q["v13"] == q["v14"] == "tozsame" for q in z)},
                               **{f"{w}_poprzednia": {"werdykty": dict(Counter(q[w] for q in z)), "pokrycie": pokrycie(z, w),
                                                      "tylko_nowa": sum(q["v14"] == "tozsame" != q[w] for q in z),
                                                      "tylko_poprzednia": sum(q[w] == "tozsame" != q["v14"] for q in z)}
                                  for w in ("v14d", "v14e", "v14p") if z and w in z[0]}}
                           for g, z in grupy.items()}}


async def przelicz(a: argparse.Namespace, client: Any) -> None:
    """v14f na parach UŻYTEGO zbioru (sprawdzenie mechanizmu, nie dowód) — pamięć odpowiedzi tego zbioru, drzewo scalone.
    --proba-uslug N: tylko pary z moją oceną, do N usług, ze śladem pytań; wynik do pary_v14f[_proba].json."""
    out = DZ / a.przelicz
    d = json.loads((out / "uslugi.json").read_text(encoding="utf-8"))
    uslugi = {int(k): v for k, v in d["uslugi"].items()}
    pary = json.loads((out / "pary.json").read_text(encoding="utf-8"))
    ocena = {(o["a"], o["b"]): o["ocena_claude"] for o in json.loads((out / "ocena_claude.json").read_text(encoding="utf-8"))}
    if a.proba_uslug:  # po kolei z każdej branży, żeby próba objęła wszystkie
        kolejki = defaultdict(list)
        for q in pary:
            if (q["a"], q["b"]) in ocena:
                kolejki[q["branza"]].append(q)
        wyb, us = [], set()
        while any(kolejki.values()):
            for br in sorted(kolejki):
                if kolejki[br]:
                    q = kolejki[br].pop(0)
                    if len(us | {q["a"], q["b"]}) <= a.proba_uslug:
                        wyb.append(q)
                        us |= {q["a"], q["b"]}
        pary = wyb
    pary = [{k: q[k] for k in ("a", "b", "branza", "v14", "v13") if k in q} for q in pary]
    r13 = json.loads((out / "sciezki_drzewo_v13b.json").read_text(encoding="utf-8"))
    mapa = json.loads((DZ / a.mapa).read_text(encoding="utf-8"))
    drzewo_e = json.loads((DZ / "v13" / a.drzewo_scalone).read_text(encoding="utf-8"))
    p = Pytania(client, out, a.budzet)
    try:
        prof = await porownaj_pary(p, pary, uslugi, {i: mapuj(r, mapa) for i, r in r13.items()}, drzewo_e, pole="v14f",
                                   w=WARIANTY[a.wariant])
    finally:
        p.zapisz()
    poz, dom = p.slownik_poz(), p.slownik_dom()
    for q in pary:
        q["ocena_claude"] = ocena.get((q["a"], q["b"]))
        if a.proba_uslug:
            pa, pb = prof[int(q["a"])], prof[int(q["b"])]
            print(f"[{q['branza'][:10]}] ocena={q['ocena_claude']} v14e={q['v14']} → v14f={q['v14f']} ({q['v14f_powod']})")
            print(f"   {opis(uslugi[int(q['a'])], pa)}\n   {opis(uslugi[int(q['b'])], pb)}")
            for k in sorted(potrzebne_domniemania(pa, pb, WARIANTY[a.wariant], p.slownik_rel()), key=str):
                print(f"   {k} → {poz.get(k, dom.get(k))}")
    (out / f"pary_v14f_{a.wariant}{'_proba' if a.proba_uslug else ''}.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    print(f"koszt {p.tok[0] * CENA_TOK:.4f} USD, błędy pytań {dict(p.bledy)}")


async def main_async(a: argparse.Namespace) -> None:
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, timeout=60.0, retry=RetryPolicy(max_retries=4, timeout=180.0)) as client:
        await (przelicz(a, client) if a.przelicz else proba(a, client) if a.proba else sprawdzian(a, client))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Drzewo v14: próba i sprawdzian na nowych salonach")
    ap.add_argument("--proba", type=int, default=0, help="próba na N parach z danych sprawdzianu v13 (bez nowych salonów)")
    ap.add_argument("--budzet", type=float, default=0.1)
    ap.add_argument("--uslug", type=int, default=3, help="usług salonu podmiotowego (koszt)")
    ap.add_argument("--na-branze", type=int, default=2)
    ap.add_argument("--kandydatow", type=int, default=150)
    ap.add_argument("--podobienstwo", type=float, default=0.6)
    ap.add_argument("--kandydatow-pomiar", type=int, default=120, help="najwyżej N najbardziej podobnych kandydatów na usługę")
    ap.add_argument("--tylko-zbierz", action="store_true")
    ap.add_argument("--wyjscie", default="v14_sprawdzian", help="katalog w dane/2026-09-28/ (nowy = nowe salony)")
    ap.add_argument("--ziarno", type=int, default=SEED_NOWE, help="losowanie salonów (każdy sprawdzian inne)")
    ap.add_argument("--mapa", default="", help="mapa węzłów scalonych w dane/2026-09-28/ (v14_scalanie); pusta = bez scalenia")
    ap.add_argument("--drzewo-scalone", default="drzewo_v14e_w2.json", help="drzewo ze scalonymi węzłami w dane/2026-09-28/v13/")
    ap.add_argument("--przelicz", default="", help="katalog UŻYTEGO zbioru: v14f na jego parach (sprawdzenie mechanizmu)")
    ap.add_argument("--proba-uslug", type=int, default=0, help="z --przelicz: tylko ocenione pary, do N usług, ze śladem")
    ap.add_argument("--wariant", default="w2", choices=sorted(WARIANTY), help="sposób pytania „czy właśnie taka” (drzewo_v14.Wariant)")
    ap.add_argument("--poprzednia", default="v14e", choices=["v14e", *sorted(WARIANTY)],
                    help="sprawdzian z --mapa: poprzednia wersja do bilansu na tych samych parach")
    asyncio.run(main_async(ap.parse_args()))
