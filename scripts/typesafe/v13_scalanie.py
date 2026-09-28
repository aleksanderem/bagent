"""Drzewo v13b: scalenie węzłów zabiegu, które budowa myli (zasada 6: błędy na węzłach, decyduje TypeSafe).

Tak samo jak v11b („scalone rodzeństwo, które przejście po drzewie myli”, zaakceptowane 28.09): kod liczy
pomyłki, TypeSafe rozstrzyga. Pomyłka = usługi o identycznej nazwie z różnych salonów przypisane do różnych
węzłów zabiegu (np. „Manicure hybrydowy” raz pod „dłonie”, raz pod „paznokci” — bo górny poziom ma obie
dziedziny). Nazwa jest tu wyłącznie DOWODEM pomyłki węzła, nie regułą porównania usług.
Para węzłów trafia do TypeSafe, gdy pomylone nazwy pochodzą z ≥ 3 różnych salonów (próg jak w wycenie).

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_scalanie.py --budzet 0.1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import v12b_budowa as vb  # noqa: E402
import v13_budowa as b  # noqa: E402

from services.typesafe_drzewo.podzial import _nazwa  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, Noul, NoulCriteria, RetryPolicy  # noqa: E402

TEN_SAM_WEZEL = {"to_samo": Noul(
    instructions="Czy węzły `wezel_a` i `wezel_b` drzewa usług to ten sam zabieg? Obok każdego: dziedzina, przykłady "
                 "usług i nazwy usług, które w różnych salonach trafiły raz do jednego, raz do drugiego węzła.",
    criteria=NoulCriteria(true="ten sam zabieg — usługi z obu węzłów to to samo, co się robi",
                          false="różne zabiegi — pomylone nazwy są wieloznaczne albo przypadkowe"),
)}


class _BezSieci:
    async def system_one(self, *a, **k):
        raise RuntimeError("pytanie spoza pamięci budowy")


def scal_wezly(drzewo: dict, grupy: list[set[str]]) -> dict:
    """Łączy węzły w grupach: rodzice, synonimy i opcje poziomów 3–5 sumowane (liczby salonów — przybliżenie z góry)."""
    zab = dict(drzewo["zabiegi"])
    for g in grupy:
        glowny = max(g, key=lambda x: zab[x]["n"])
        w = {**zab[glowny], "rodzice": sorted({r for x in g for r in zab[x]["rodzice"]}),
             "synonimy": sorted({s for x in g for s in zab[x]["synonimy"]}), "n": sum(zab[x]["n"] for x in g),
             "scalone": sorted(g)}
        for poz in ("metoda", "gdzie", "etap"):
            opcje: dict[str, dict] = {}
            for x in g:
                for e, d in (zab[x].get(poz) or {}).items():
                    o = opcje.setdefault(e, {"n": 0, "synonimy": [], "examples": []})
                    o["n"] += d.get("n", 0)
                    o["synonimy"] = sorted(set(o["synonimy"]) | set(d.get("synonimy", [e])))
                    o["examples"] = (o["examples"] + d.get("examples", []))[:3]
            w[poz] = opcje
        for x in g:
            zab.pop(x)
        zab[glowny] = w
    return {**drzewo, "wersja": "13b", "zabiegi": zab}


async def main_async(a: argparse.Namespace) -> None:
    uslugi = [json.loads(x) for x in (b.DZ / "probka.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    role = json.loads((b.OUT / "role_v13.json").read_text(encoding="utf-8"))
    uslugi = [u for u in uslugi if (r := role.get(str(u["id"]))) and r.get("pozycja") and b.naj(r["pozycja"]) in b.POROWNYWANE
              and r.get("zestaw", 0) < 0.5]
    fr = {str(u["id"]): vb.frazy_rol(u, role[str(u["id"])]["role"]) for u in uslugi}
    drzewo = json.loads((b.OUT / "drzewo_v13.json").read_text(encoding="utf-8"))
    p0 = b.Pytania(_BezSieci(), 1.0, b.OUT / "pamiec_budowy_v13.json")  # przypisanie usług z budowy, bez sieci
    dz, _p1 = await b.dziedziny(p0, uslugi, fr)
    _wezly, usl_wezel = await b.zabiegi(p0, uslugi, fr, dz)
    if p0.bledy:
        raise SystemExit(f"{p0.bledy} pytań spoza pamięci — najpierw przebuduj drzewo")
    po_nazwie: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for u in uslugi:
        g = usl_wezel.get(str(u["id"]))
        if g and _nazwa(u["nazwa"]):
            po_nazwie[_nazwa(u["nazwa"])][g].add(u["booksy_id"])
    pomylki: dict[tuple[str, str], dict] = {}
    for nazwa, wezly in po_nazwie.items():
        ws = sorted(wezly)
        for i, x in enumerate(ws):
            for y in ws[i + 1:]:
                d = pomylki.setdefault((x, y), {"salony": set(), "nazwy": []})
                d["salony"] |= wezly[x] | wezly[y]
                if len(d["nazwy"]) < 6:
                    d["nazwy"].append(nazwa)
    pary = [(x, y) for (x, y), d in pomylki.items() if len(d["salony"]) >= b.MIN_SALONOW
            and x in drzewo["zabiegi"] and y in drzewo["zabiegi"]]
    print(f"par węzłów mylonych przez ≥ {b.MIN_SALONOW} salony: {len(pary)}", flush=True)
    key = (os.environ.get("TYPESAFE_API_KEY") or b.KEY_FILE.read_text(encoding="utf-8")).strip()
    w: dict[tuple[str, str], float] = {}
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, timeout=60.0, retry=RetryPolicy(max_retries=4, timeout=180.0)) as client:
        pytaj = b.Pytania(client, a.budzet, b.OUT / "pamiec_scalania_v13.json")

        def opis(g: str) -> dict:
            z = drzewo["zabiegi"][g]
            return {"nazwa": z["etykieta"], "dziedziny": z["rodzice"], "przyklady": z["examples"]}

        async def jedna(x: str, y: str) -> None:
            r = await pytaj({"wezel_a": opis(x), "wezel_b": opis(y), "pomylone_nazwy": pomylki[(x, y)]["nazwy"]}, TEN_SAM_WEZEL)
            w[(x, y)] = r.nouls["to_samo"].noul
        try:
            await asyncio.gather(*[jedna(x, y) for x, y in pary])
        finally:
            pytaj.zapisz()
    rodzic = {g: g for g in drzewo["zabiegi"]}

    def koniec(g: str) -> str:
        while rodzic[g] != g:
            g = rodzic[g]
        return g
    for (x, y), v in sorted(w.items(), key=lambda kv: -kv[1]):
        if v >= 0.5:
            rodzic[koniec(y)] = koniec(x)
    grupy_d: dict[str, set] = defaultdict(set)
    for g in drzewo["zabiegi"]:
        grupy_d[koniec(g)].add(g)
    grupy = [g for g in grupy_d.values() if len(g) > 1]
    nowe = scal_wezly(drzewo, grupy)
    nowe["scalanie"] = {"par": len(pary), "scalonych_grup": len(grupy), "koszt_usd": round(pytaj.koszt, 4),
                        "werdykty": {f"{x} ~ {y}": round(v, 3) for (x, y), v in w.items()}}
    (b.OUT / "drzewo_v13b.json").write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"scalonych grup {len(grupy)}; zabiegów {len(drzewo['zabiegi'])} → {len(nowe['zabiegi'])}; koszt {pytaj.koszt:.3f} USD")
    for g in sorted(grupy, key=len, reverse=True)[:25]:
        print("  " + " + ".join(sorted(g)))
    for (x, y), v in sorted(w.items(), key=lambda kv: -kv[1])[:0]:
        print(x, y, v)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Drzewo v13b: scalanie mylonych węzłów zabiegu")
    p.add_argument("--budzet", type=float, default=0.1)
    asyncio.run(main_async(p.parse_args()))
