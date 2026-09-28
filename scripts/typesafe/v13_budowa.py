"""Drzewo v13 — budowa z ról słów (v13_role.py) według modelu Alexa i cookbooka hierarchical_classification.

Poziomy (model 28.09): 1 dziedzina (czego dotyczy) → 2 zabieg (co się robi) → 3 metoda → 4 gdzie i ile → 5 etap.
Zasady (dokument „Matching usług — stan”, Zasada niezmienna):
  * etykiety z danych — frazy wybrane przez TypeSafe z nazw usług, żadnej listy pisanej ręcznie (zasada 5);
  * rodzeństwo się nie nakłada (zasada 2); ten sam zabieg może mieć kilku rodziców — jak MeSH w cookbooku
    (graf rozwinięty w ścieżki): „pedicure” pod paznokciami i stopami to jeden węzeł, a „przedłużanie” rzęs
    i paznokci — dwa; rozstrzyga TypeSafe (zasada 6);
  * poziom 1 = obiekt usługi, jedna zasada podziału (lista odchyleń: dwie zasady naraz rozcięły 68 z 206 zabiegów);
  * poziomy 3–5 zależą tylko od zabiegu, więc dzieci każdego węzła metody są te same (drzewo-iloczyn);
    poziom 4 to JEDNO pytanie (model), a jego opcja = zestaw fraz usługi (obszar, wielkość, dla kogo), więc
    opcje się nie nakładają; „ogolnie” = usługa tego nie mówi, „inne” = mówi coś spoza listy (wzorzec TypeSafe:
    wybór z opcją „brak dopasowania” — bez niej „włosy do ramion” szły w jedyną opcję „długie”).
Kod liczy (salony, zestawy fraz); TypeSafe decyduje (dziedzina czy miejsce, rodzic miejsca, synonimy, ten sam zabieg).
Próg etykiety: ≥ 3 różne salony (jak w wycenie). Zestawy zabiegów nie budują drzewa (porównuje się je jako zbiory).

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_budowa.py --budzet 0.6
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v12b_budowa as vb  # noqa: E402
import v13_dziedziny as vd  # noqa: E402

from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, NoulCriteria, RetryPolicy  # noqa: E402

DZ = vb.DZ
OUT = DZ.parent / "v13"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
MIN_SALONOW = 3
CZESTE_SALONOW = 10
ROWNOLEGLE = 30
POROWNYWANE = ("zabieg", "pakiet")
ZRODLA_WLASNE = ("nazwa", "zabieg_booksy", "kategoria")  # warianty to inne usługi z tej samej karty
WYMIARY_4 = ("obszar", "wielkosc", "dla_kogo")

TEN_SAM_ZABIEG = {"to_samo": Noul(
    instructions="Czy zabieg `zabieg_a` i zabieg `zabieg_b` to ten sam zabieg — to samo, co się robi — mimo innej "
                 "dziedziny? Obok każdego: dziedzina i przykłady usług z cenników różnych salonów.",
    criteria=NoulCriteria(true="ten sam zabieg, tylko wykonywany na innym obiekcie albo opisany w innej dziedzinie",
                          false="różne zabiegi, które mają tylko tę samą nazwę"),
)}


def naj(d: dict[str, float]) -> str:
    return max(d, key=d.get)


def wlasne(fr: dict, rola: str) -> list[str]:
    return sorted({f for f, z in fr.get(rola, []) if z in ZRODLA_WLASNE})


def wszystkie(fr: dict, rola: str) -> list[str]:
    return sorted({f for f, _z in fr.get(rola, [])})


def zabieg_uslugi(fr: dict) -> str | None:
    return next((f for zr in vb.ZRODLA for f, z in fr.get("zabieg", []) if z == zr), None)


def licz(uslugi: list[dict], wart: dict[str, list[str]]) -> dict[str, dict]:
    """Etykieta → salony, przykłady (tylko etykiety z ≥ MIN_SALONOW salonów)."""
    sal: dict[str, set] = defaultdict(set)
    przyk: dict[str, list[str]] = defaultdict(list)
    for u in uslugi:
        for e in wart.get(str(u["id"]), []):
            if u["booksy_id"] not in sal[e] and len(przyk[e]) < 6:
                przyk[e].append(f"{u['nazwa']} [{u.get('kategoria') or '—'}]")
            sal[e].add(u["booksy_id"])
    return {e: {"n": len(s), "examples": przyk[e]} for e, s in sal.items() if len(s) >= MIN_SALONOW}


class Pytania:
    """Pytania do TypeSafe z pamięcią w pliku (powtórka nie płaci drugi raz) i ostrożną odpowiedzią przy błędzie:
    nieudane tak/nie = 0 (nie łączy węzłów), nieudany wybór = ostatnia opcja („zadna”/„inne”) — liczone w raporcie."""

    def __init__(self, client: Any, budzet: float, plik: Path):
        self.client, self.budzet, self.tok, self.plik = client, budzet, [0], plik
        self.sem = asyncio.Semaphore(ROWNOLEGLE)
        self.pamiec: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
        self.bledy = 0

    def zapisz(self) -> None:
        self.plik.write_text(json.dumps(self.pamiec, ensure_ascii=False), encoding="utf-8")

    async def __call__(self, st: dict, q: dict) -> Any:
        klucz = hashlib.sha1((json.dumps(st, sort_keys=True, ensure_ascii=False) + repr(q)).encode()).hexdigest()
        if klucz not in self.pamiec:
            async with self.sem:
                try:
                    r = await self.client.system_one(st, q, model=MODEL)
                except Exception as e:  # noqa: BLE001 — jedno pytanie bez odpowiedzi nie zatrzymuje budowy
                    if " 402 " in str(e):
                        raise SystemExit(f"TypeSafe: brak kredytów — {str(e)[:160]}") from e
                    self.bledy += 1
                    return _odpowiedz({k: {"noul": 0.0} if isinstance(v, Noul) else {"choice": list(v.criteria)[-1],
                                                                                    "probabilities": {list(v.criteria)[-1]: 1.0}}
                                       for k, v in q.items()})
            self.tok[0] += r.usage.input_tokens or 0
            if self.tok[0] * CENA_TOK > self.budzet:
                self.zapisz()
                raise SystemExit("budżet wyczerpany")
            self.pamiec[klucz] = {**{k: {"noul": a.noul} for k, a in r.nouls.items()},
                                  **{k: {"choice": a.choice, "probabilities": dict(a.probabilities)} for k, a in r.choices.items()}}
            if len(self.pamiec) % 500 == 0:
                self.zapisz()
        return _odpowiedz(self.pamiec[klucz])

    @property
    def koszt(self) -> float:
        return self.tok[0] * CENA_TOK


def _odpowiedz(zapis: dict) -> Any:
    return SimpleNamespace(nouls={k: SimpleNamespace(noul=v["noul"]) for k, v in zapis.items() if "noul" in v},
                           choices={k: SimpleNamespace(choice=v["choice"], probabilities=v["probabilities"])
                                    for k, v in zapis.items() if "choice" in v})


async def dziedziny(pytaj: Pytania, uslugi: list[dict], fr: dict[str, dict]) -> tuple[dict[str, str], set[str]]:
    """Poziom 1: fraza obszaru → dziedzina (obiekt), plus frazy, które SĄ dziedziną (każda forma słowa).
    Jak v13_dziedziny próba 3 (dziedzina czy miejsce)."""
    ob = {}
    zab: dict[str, Counter] = defaultdict(Counter)
    for u in uslugi:
        f = fr[str(u["id"])]
        z = zabieg_uslugi(f)
        for o in wszystkie(f, "obszar"):
            if z:
                zab[o][z] += 1
    for o, d in licz(uslugi, {k: wszystkie(v, "obszar") for k, v in fr.items()}).items():
        ob[o] = {"n": d["n"], "zabiegi": [z for z, _ in zab[o].most_common(6)], "przyklady": d["examples"][:4]}
    lista = sorted(ob, key=lambda o: (-ob[o]["n"], o))

    def opis(o: str) -> dict:
        return {"nazwa": o, "zabiegi": ob[o]["zabiegi"], "przyklady": ob[o]["przyklady"]}
    pary = [(x, y) for i, x in enumerate(lista) for y in lista[i + 1:] if vd.podobne(x, y)]
    w: dict = {}

    async def syn(x: str, y: str) -> None:
        r = await pytaj({"obiekt_a": opis(x), "obiekt_b": opis(y)}, vd.TEN_SAM_OBIEKT)
        w[(x, y)] = w[(y, x)] = r.nouls["to_samo"].noul
    await asyncio.gather(*[syn(x, y) for x, y in pary])
    rep = vd.laczenie_pelne(lista, {o: ob[o]["n"] for o in lista}, pary, w)
    glowne = sorted(set(rep.values()), key=lambda o: (-ob[o]["n"], o))
    czy: dict[str, float] = {}

    async def dz(o: str) -> None:
        czy[o] = (await pytaj({"obiekt": opis(o)}, vd.CZY_DZIEDZINA)).nouls["dziedzina"].noul
    await asyncio.gather(*[dz(o) for o in glowne])
    korz = [o for o in glowne if czy[o] >= 0.5]
    wk: dict = {}

    async def para_k(x: str, y: str) -> None:
        r = await pytaj({"dziedzina_a": opis(x), "dziedzina_b": opis(y)}, vd.TA_SAMA_NAZWA_DZIEDZINY)
        wk[(x, y)] = wk[(y, x)] = r.nouls["to_samo"].noul
    await asyncio.gather(*[para_k(x, y) for i, x in enumerate(korz) for y in korz[i + 1:]])
    rep_k = vd.laczenie_srednie(korz, {o: ob[o]["n"] for o in korz}, wk)
    dz_lista = sorted(set(rep_k.values()), key=lambda o: (-ob[o]["n"], o))
    q = {"rodzic": Choice(instructions="W której dziedzinie usług leży miejsce `obiekt` — częścią której z tych dziedzin jest?",
                          criteria={**{d: {"examples": ob[d]["przyklady"][:3]} for d in dz_lista},
                                    "zadna": "żadna z tych dziedzin — to osobny obszar usług"})}
    rodzic = {o: rep_k[o] for o in korz}

    async def miejsce(o: str) -> None:
        c = (await pytaj({"obiekt": opis(o)}, q)).choices["rodzic"].choice
        rodzic[o] = o if c == "zadna" else c
    await asyncio.gather(*[miejsce(o) for o in glowne if o not in rodzic])
    return {o: rodzic[rep[o]] for o in lista}, {o for o in lista if rep[o] in korz}


async def zabiegi(pytaj: Pytania, uslugi: list[dict], fr: dict[str, dict], dz_obszaru: dict[str, str]) -> tuple[dict, dict]:
    """Poziom 2: zabiegi w dziedzinach (synonimy w dziedzinie), potem ten sam zabieg w kilku dziedzinach (graf)."""
    korz_uslugi: dict[str, set] = {}
    for u in uslugi:
        korz_uslugi[str(u["id"])] = {dz_obszaru[o] for o in wlasne(fr[str(u["id"])], "obszar") if o in dz_obszaru}
    zab = {str(u["id"]): zabieg_uslugi(fr[str(u["id"])]) for u in uslugi}
    lokalne: dict[str, dict[str, str]] = {}  # dziedzina → fraza → reprezentant
    przyk: dict[tuple[str, str], list[str]] = {}
    for d in sorted({r for rs in korz_uslugi.values() for r in rs}):
        us = [u for u in uslugi if d in korz_uslugi[str(u["id"])] and zab[str(u["id"])]]
        et = licz(us, {str(u["id"]): [zab[str(u["id"])]] for u in us})
        if not et:
            continue
        rep = await vb.scal(pytaj_noul(pytaj), d, "zabieg", et, kazda_para=True)
        lokalne[d] = rep
        for e, r in rep.items():
            przyk[(d, r)] = et[r]["examples"][:3]
    # ten sam zabieg w kilku dziedzinach: węzły lokalne o wspólnej frazie
    wezly = [(d, r) for d, rep in lokalne.items() for r in sorted(set(rep.values()))]
    syn = {(d, r): {e for e, x in lokalne[d].items() if x == r} for d, r in wezly}
    pary = [(a, b) for i, a in enumerate(wezly) for b in wezly[i + 1:] if a[0] != b[0] and syn[a] & syn[b]]
    w: dict = {}

    async def ten_sam(a: tuple, b: tuple) -> None:
        r = await pytaj({"zabieg_a": {"nazwa": a[1], "dziedzina": a[0], "przyklady": przyk[a]},
                         "zabieg_b": {"nazwa": b[1], "dziedzina": b[0], "przyklady": przyk[b]}}, TEN_SAM_ZABIEG)
        w[(a, b)] = w[(b, a)] = r.nouls["to_samo"].noul
    await asyncio.gather(*[ten_sam(a, b) for a, b in pary])
    klucze = [f"{d}|{r}" for d, r in wezly]
    waga = {f"{d}|{r}": len(przyk[(d, r)]) for d, r in wezly}
    wk = {(f"{a[0]}|{a[1]}", f"{b[0]}|{b[1]}"): v for (a, b), v in w.items()}
    # łączenie średnie (jak na poziomie 1): pełne blokowało grupę jednym „różne” — masaż został osobnym węzłem
    # w czterech dziedzinach, choć to ten sam zabieg na innym obiekcie
    rep_g = vd.laczenie_srednie(klucze, waga, wk) if wk else {k: k for k in klucze}
    wezel_globalny: dict[str, dict] = {}
    for k in klucze:
        g = rep_g[k]
        d, r = k.split("|", 1)
        wz = wezel_globalny.setdefault(g, {"etykieta": g.split("|", 1)[1], "rodzice": [], "synonimy": set()})
        wz["rodzice"].append(d)
        wz["synonimy"] |= syn[(d, r)]
    # usługa → węzeł zabiegu: po dziedzinie i frazie; bez dziedziny — gdy fraza jest w dokładnie jednym węźle
    fraza_wezly: dict[str, set] = defaultdict(set)
    for g, wz in wezel_globalny.items():
        for e in wz["synonimy"]:
            fraza_wezly[e].add(g)
    usl_wezel: dict[str, str] = {}
    for u in uslugi:
        i, z = str(u["id"]), zab[str(u["id"])]
        if not z:
            continue
        kand = {rep_g[f"{d}|{lokalne[d][z]}"] for d in korz_uslugi[i] if d in lokalne and z in lokalne[d]}
        if not kand and len(fraza_wezly.get(z, ())) == 1:
            kand = set(fraza_wezly[z])
        if len(kand) == 1:
            usl_wezel[i] = kand.pop()
    return wezel_globalny, usl_wezel


def pytaj_noul(pytaj: Pytania):
    async def f(st: dict, q: dict) -> Any:
        return await pytaj(st, q)
    return f


async def poziomy_3_5(pytaj: Pytania, uslugi: list[dict], fr: dict[str, dict], poziom_1: set[str],
                      usl_wezel: dict[str, str]) -> dict[str, dict]:
    """Pod każdym zabiegiem: metody, „gdzie i ile” (zestaw fraz usługi) i etapy z ≥ 3 salonów; synonymy metod i etapów."""
    po_wezle: dict[str, list[dict]] = defaultdict(list)
    for u in uslugi:
        if str(u["id"]) in usl_wezel:
            po_wezle[usl_wezel[str(u["id"])]].append(u)
    out: dict[str, dict] = {}
    for g, us in po_wezle.items():
        wezel: dict[str, Any] = {}
        for poz, rola in (("metoda", "metoda"), ("etap", "etap")):
            et = licz(us, {str(u["id"]): wszystkie(fr[str(u["id"])], rola) for u in us})
            rep = await vb.scal(pytaj_noul(pytaj), g.split("|")[0], rola, et, kazda_para=False) if len(et) > 1 else {e: e for e in et}
            wezel[poz] = {r: {"n": sum(et[e]["n"] for e in et if rep[e] == r), "synonimy": sorted(e for e in et if rep[e] == r),
                              "examples": et[r]["examples"][:3]} for r in sorted(set(rep.values()))}
        # poziom 4: jeden wybór; opcja = zestaw fraz obszaru (bez fraz samej dziedziny), wielkości i dla kogo
        zestawy = {}
        for u in us:
            f = fr[str(u["id"])]
            frazy = [o for o in wlasne(f, "obszar") if o not in poziom_1] + wlasne(f, "wielkosc") + wlasne(f, "dla_kogo")
            if frazy:
                zestawy[str(u["id"])] = [" · ".join(sorted(set(frazy)))]
        et4 = licz(us, zestawy)
        wezel["gdzie"] = {e: {"n": d["n"], "synonimy": [e], "examples": d["examples"][:3]} for e, d in et4.items()}
        out[g] = wezel
    return out


async def main_async(a: argparse.Namespace) -> None:
    uslugi = [json.loads(x) for x in (DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    role = json.loads((OUT / "role_v13.json").read_text(encoding="utf-8"))
    uslugi = [u for u in uslugi if (r := role.get(str(u["id"]))) and r.get("pozycja") and naj(r["pozycja"]) in POROWNYWANE
              and r.get("zestaw", 0) < 0.5]
    fr = {str(u["id"]): vb.frazy_rol(u, role[str(u["id"])]["role"]) for u in uslugi}
    print(f"usług porównywalnych bez zestawów: {len(uslugi)}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, timeout=60.0, retry=RetryPolicy(max_retries=4, timeout=180.0)) as client:
        pytaj = Pytania(client, a.budzet, OUT / "pamiec_budowy_v13.json")
        try:
            dz_obszaru, poziom_1 = await dziedziny(pytaj, uslugi, fr)
            print(f"dziedzin {len(set(dz_obszaru.values()))}; koszt {pytaj.koszt:.3f} USD, błędów {pytaj.bledy}", flush=True)
            wezly, usl_wezel = await zabiegi(pytaj, uslugi, fr, dz_obszaru)
            print(f"zabiegów {len(wezly)} (z kilkoma rodzicami {sum(len(w['rodzice']) > 1 for w in wezly.values())}); "
                  f"usług przypisanych {len(usl_wezel)}; koszt {pytaj.koszt:.3f} USD, błędów {pytaj.bledy}", flush=True)
            nizej = await poziomy_3_5(pytaj, uslugi, fr, poziom_1, usl_wezel)
        finally:
            pytaj.zapisz()
    drzewo = {"wersja": 13, "dziedzina_obszaru": dz_obszaru, "frazy_dziedzin": sorted(poziom_1),
              "dziedziny": sorted(set(dz_obszaru.values())),
              "zabiegi": {g: {"etykieta": w["etykieta"], "rodzice": sorted(w["rodzice"]), "synonimy": sorted(w["synonimy"]),
                              "n": len({u["booksy_id"] for u in uslugi if usl_wezel.get(str(u["id"])) == g}),
                              "examples": [u["nazwa"] for u in uslugi if usl_wezel.get(str(u["id"])) == g][:3],
                              **nizej.get(g, {"metoda": {}, "gdzie": {}, "etap": {}})}
                          for g, w in wezly.items()},
              "koszt_usd": round(pytaj.koszt, 4), "bledow_pytan": pytaj.bledy}
    drzewo["dziedziny_opis"] = {d: {"miejsca": [o for o, r in dz_obszaru.items() if r == d][:8],
                                    "zabiegi": [w["etykieta"] for w in sorted(drzewo["zabiegi"].values(), key=lambda w: -w["n"])
                                                if d in w["rodzice"]][:8]}
                                for d in drzewo["dziedziny"]}
    (OUT / "drzewo_v13.json").write_text(json.dumps(drzewo, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt łącznie {pytaj.koszt:.3f} USD")
    raport(drzewo)


def raport(drzewo: dict) -> None:
    po_dz: dict[str, list] = defaultdict(list)
    for g, w in drzewo["zabiegi"].items():
        for d in w["rodzice"]:
            po_dz[d].append((g, w))
    for d, zs in sorted(po_dz.items(), key=lambda kv: -sum(w["n"] for _g, w in kv[1])):
        print(f"\n{d}: zabiegów {len(zs)}")
        for _g, w in sorted(zs, key=lambda x: -x[1]["n"])[:8]:
            rodz = f" (też: {', '.join(r for r in w['rodzice'] if r != d)})" if len(w["rodzice"]) > 1 else ""
            osie = " | ".join(f"{p}: {', '.join(sorted(w[p], key=lambda e: -w[p][e]['n'])[:4])}" for p in ("metoda", "gdzie", "etap") if w.get(p))
            print(f"  {w['etykieta']} ({w['n']} sal.){rodz} | {osie}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v13: budowa z ról słów według modelu i cookbooka")
    p.add_argument("--budzet", type=float, default=0.6)
    asyncio.run(main_async(p.parse_args()))
