"""Drzewo v14e: scalenie węzłów zabiegu, które przejście po drzewie myli (bd BEAUTY_AUDIT-asrk, 28.09 noc).

Zasada 6 (dokument „Matching usług”): błędy liczymy na węzłach we wszystkich branżach naraz, o zmianie węzła
decyduje TypeSafe, nigdy łatka. Jak v13_scalanie, ale dowody z trzech sprawdzianów v14 (ok. 15 tys. usług
z 54 salonów i ich konkurencji): pomyłka = usługi o identycznej nazwie z różnych salonów przypisane do różnych
węzłów zabiegu. Nazwa jest wyłącznie DOWODEM pomyłki węzła, nie regułą porównania usług. Para węzłów trafia do
TypeSafe, gdy pomylone nazwy pochodzą z ≥ 3 różnych salonów (próg jak w wycenie). Przy każdym węźle TypeSafe
widzi etykietę, dziedziny i przykładowe usługi z pełnym kontekstem (nazwa, zabieg z Booksy, kategoria) —
przykłady z drzewa bywały mylące („Easyshare” w węźle botoksu).
Scalone grupy → nowy plik drzewa (scal_wezly z v13_scalanie) i mapa stary węzeł → węzeł scalony.
Sprawdziany 1–3 to dane diagnozy; dowodem jest dopiero sprawdzian na nowych salonach.

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v14_scalanie.py --budzet 0.2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v13_scalanie as sc  # noqa: E402

from typesafe_sdk import Noul, NoulCriteria  # noqa: E402

from services.typesafe_drzewo.drzewo_v13 import POROWNYWANE  # noqa: E402
from services.typesafe_drzewo.podzial import _nazwa  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DZ = Path(__file__).resolve().parent / "dane" / "2026-09-28"
ZBIORY = ("v14_sprawdzian", "v14_sprawdzian2", "v14_sprawdzian3")
MIN_SALONOW = 3
PRZYKLADOW = 6
CENA_TOK = 0.042 / 1e6
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"

# Wariant 2 (28.09 noc): pytanie o to, CO SIĘ ROBI, bez dziedzin przy węźle (wariant 1 z dziedzinami nie scalał
# masażu kobido spod „twarzy” i „twarz szyja”: 0,42) i z najczęstszymi usługami węzła zamiast pierwszych z brzegu.
TEN_SAM_ZABIEG_V2 = {"to_samo": Noul(
    instructions="Czy w grupie usług `wezel_a` i w grupie `wezel_b` robi się ten sam zabieg? Obok każdej grupy: nazwa "
                 "zabiegu i najczęstsze usługi z cenników różnych salonów, a także nazwy usług, które w różnych salonach "
                 "trafiły raz do jednej, raz do drugiej grupy. Obszar, metoda i dodatki mogą się różnić.",
    criteria=NoulCriteria(true="ten sam zabieg — w obu grupach robi się to samo",
                          false="różne zabiegi — w grupach robi się co innego, a pomylone nazwy są wieloznaczne"),
)}


def najlepszy(rek: dict | None) -> str | None:
    if not rek or not rek.get("zabiegi"):
        return None
    return max(rek["zabiegi"], key=lambda z: z[2])[1]


def pozycja(rek: dict | None) -> str:
    p = (rek or {}).get("pozycja") or {}
    return max(p, key=p.get) if p else "zabieg"


def zbierz() -> tuple[dict[str, dict[str, set]], dict[str, list[dict]]]:
    """→ (nazwa → węzeł → salony, węzeł → przykładowe usługi z pełnym kontekstem)."""
    po_nazwie: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    przyklady: dict[str, list[dict]] = defaultdict(list)
    czeste: dict[str, Counter] = defaultdict(Counter)
    for kat in ZBIORY:
        uslugi = json.loads((DZ / kat / "uslugi.json").read_text(encoding="utf-8"))["uslugi"]
        sciezki = json.loads((DZ / kat / "sciezki_drzewo_v13b.json").read_text(encoding="utf-8"))
        for i in sorted(sciezki, key=int):
            rek, u = sciezki[i], uslugi.get(i)
            g = najlepszy(rek)
            if not u or not g or pozycja(rek) not in POROWNYWANE:
                continue
            n = _nazwa(u["nazwa"])
            if n:
                po_nazwie[n][g].add(u["booksy_id"])
                czeste[g][u["nazwa"].strip()] += 1
            if len(przyklady[g]) < PRZYKLADOW and all(p["nazwa"] != u["nazwa"] for p in przyklady[g]):
                przyklady[g].append({"nazwa": u["nazwa"], "zabieg_w_booksy": u.get("zabieg_booksy") or "",
                                     "kategoria": u.get("kategoria") or ""})
    najczestsze = {g: [n for n, _k in c.most_common(PRZYKLADOW + 2)] for g, c in czeste.items()}
    return po_nazwie, {"pierwsze": przyklady, "najczestsze": najczestsze}


def pomylki(po_nazwie: dict[str, dict[str, set]]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for nazwa, wezly in po_nazwie.items():
        ws = sorted(wezly)
        for i, x in enumerate(ws):
            for y in ws[i + 1:]:
                d = out.setdefault((x, y), {"salony": set(), "nazwy": []})
                d["salony"] |= wezly[x] | wezly[y]
                if len(d["nazwy"]) < 6 and nazwa not in d["nazwy"]:
                    d["nazwy"].append(nazwa)
    return out


async def main_async(a: argparse.Namespace) -> None:
    drzewo = json.loads((DZ / "v13" / "drzewo_v13b.json").read_text(encoding="utf-8"))
    po_nazwie, przyklady = zbierz()
    pom = pomylki(po_nazwie)
    pary = sorted((x, y) for (x, y), d in pom.items() if len(d["salony"]) >= MIN_SALONOW
                  and x in drzewo["zabiegi"] and y in drzewo["zabiegi"])
    print(f"par węzłów mylonych przez ≥ {MIN_SALONOW} salony: {len(pary)} (szac. {len(pary) * 1800 * CENA_TOK:.3f} USD)", flush=True)

    def opis(g: str) -> dict:
        z = drzewo["zabiegi"][g]
        if a.wariant == 2:
            return {"nazwa": z["etykieta"], "najczestsze_uslugi": przyklady["najczestsze"].get(g) or z["examples"][:PRZYKLADOW]}
        return {"nazwa": z["etykieta"], "dziedziny": z["rodzice"],
                "przyklady_uslug": przyklady["pierwsze"].get(g) or z["examples"][:PRZYKLADOW]}
    pytanie = TEN_SAM_ZABIEG_V2 if a.wariant == 2 else sc.TEN_SAM_WEZEL
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    plik = DZ / ("v14_scalanie.json" if a.wariant == 1 else f"v14_scalanie_w{a.wariant}.json")
    pamiec: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    tok = [0]
    sem = asyncio.Semaphore(20)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, timeout=60.0, retry=RetryPolicy(max_retries=4, timeout=180.0)) as client:
        async def jedna(x: str, y: str) -> None:
            k = f"{x} ~ {y}"
            if k in pamiec:
                return
            async with sem:
                r = await client.system_one({"wezel_a": opis(x), "wezel_b": opis(y), "pomylone_nazwy": pom[(x, y)]["nazwy"]},
                                            pytanie, model=MODEL)
            tok[0] += r.usage.input_tokens or 0
            if tok[0] * CENA_TOK > a.budzet:
                raise SystemExit("budżet wyczerpany")
            pamiec[k] = round(r.nouls["to_samo"].noul, 3)
        try:
            await asyncio.gather(*[jedna(x, y) for x, y in pary])
        finally:
            plik.write_text(json.dumps(pamiec, ensure_ascii=False, indent=1), encoding="utf-8")
    rodzic = {g: g for g in drzewo["zabiegi"]}

    def koniec(g: str) -> str:
        while rodzic[g] != g:
            g = rodzic[g]
        return g
    # pełne wiązanie: grupy łączymy tylko, gdy żadna para zapytana wprost między nimi nie mówi „różne” (< 0,5)
    # — pierwsze uruchomienie łączyło łańcuchem: henna ~ stylizacja 0,53, regulacja ~ stylizacja 0,52, a henna ~ regulacja 0,12
    werdykt = {tuple(k.split(" ~ ")): v for k, v in pamiec.items()}
    czlonkowie: dict[str, set] = {g: {g} for g in drzewo["zabiegi"]}
    for (x, y), v in sorted(werdykt.items(), key=lambda kv: (-kv[1], kv[0])):
        if v < 0.5 or x not in rodzic or y not in rodzic:  # punkt neutralny tak/nie (docs TypeSafe)
            continue
        gx, gy = koniec(x), koniec(y)
        if gx == gy:
            continue
        sprzeczne = any(werdykt.get(tuple(sorted((m, n))), 1.0) < 0.5 for m in czlonkowie[gx] for n in czlonkowie[gy])
        if not sprzeczne:
            rodzic[gy] = gx
            czlonkowie[gx] |= czlonkowie.pop(gy)
    grupy_d: dict[str, set] = defaultdict(set)
    for g in drzewo["zabiegi"]:
        grupy_d[koniec(g)].add(g)
    grupy = [g for g in grupy_d.values() if len(g) > 1]
    nowe = sc.scal_wezly(drzewo, grupy)
    mapa = {g: next(k for k, w in nowe["zabiegi"].items() if g == k or g in w.get("scalone", [])) for g in drzewo["zabiegi"]}
    nowe["wersja"] = "14e"
    nowe["scalanie_v14"] = {"par": len(pary), "grup": len(grupy), "koszt_usd": round(tok[0] * CENA_TOK, 4),
                            "werdykty": {k: v for k, v in sorted(pamiec.items(), key=lambda kv: -kv[1])},
                            "pomylone_nazwy": {f"{x} ~ {y}": pom[(x, y)]["nazwy"] for x, y in pary}}
    (DZ / "v13" / f"drzewo_v14e_w{a.wariant}.json").write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
    (DZ / f"v14_mapa_wezlow_w{a.wariant}.json").write_text(json.dumps(mapa, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt {tok[0] * CENA_TOK:.4f} USD; scalonych grup {len(grupy)}; zabiegów {len(drzewo['zabiegi'])} → {len(nowe['zabiegi'])}")
    print("rozkład odpowiedzi:", dict(Counter("≥0,5" if v >= 0.5 else "0,3–0,5" if v >= 0.3 else "<0,3" for v in pamiec.values())))
    for g in sorted(grupy, key=len, reverse=True):
        print("  scalone: " + " + ".join(sorted(g)))
    print("pary blisko progu (0,3–0,5):")
    for k, v in sorted(pamiec.items(), key=lambda kv: -kv[1]):
        if 0.3 <= v < 0.5:
            print(f"  {v:.2f} {k} | {pom.get(tuple(k.split(' ~ ')), {}).get('nazwy', [])[:3]}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v14e: scalanie mylonych węzłów zabiegu na dowodach ze sprawdzianów")
    p.add_argument("--budzet", type=float, default=0.2)
    p.add_argument("--wariant", type=int, default=2, help="1 = pytanie z dziedzinami (v13), 2 = co się robi, najczęstsze usługi")
    asyncio.run(main_async(p.parse_args()))
