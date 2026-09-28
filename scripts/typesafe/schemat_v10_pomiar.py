"""Schemat v10 na zbiorach roboczych — BEZ odczytu bazy i bez nowych destylacji (bd BEAUTY_AUDIT-asrk, 28.09).

Zbiory robocze = salony, na których diagnozowano (warianty 27.09: 10 salonów; diagnoza
27.09: 8 salonów). To NIE jest dowód w rozumieniu bramki uniwersalności — dowód to pomiar
na nowych, losowych salonach (schemat_v10_sprawdzian.py). Tu: kierunek i rozkład zysku.

v10 zmienia tylko porównanie, więc wszystkie wersje liczą się na TYCH SAMYCH destylacjach v9:
  v9            porównanie v8,
  v10           dodatek od 0,5 + liczby sztuk + poziom z nazwy + liczba osób domyślnie jedna,
  v10_bez_czasu to samo bez reguły czasu (decyzja Alexa 28.09 — do potwierdzenia pomiarem).

Miernik: sędzia par v3 (czas trwania nie jest różnicą); oceny z pliku sedzia_v3.json, brakujące
pary ocenia API TypeSafe (bez zapisu do bazy).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/schemat_v10_pomiar.py --budzet 0.3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.podzial import jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from services.typesafe_drzewo.schemat_v10 import porownaj_v10, schemat_v10  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import WERSJA_V3, para_klucz  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

DANE = Path(__file__).resolve().parent / "dane"
OUT = DANE / "2026-09-28" / "v10_robocze"
ZBIORY = {"warianty": DANE / "2026-09-27" / "warianty", "diagnoza": DANE / "2026-09-27" / "diagnoza"}
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_PARA = 900
WERSJE = ("v9", "v10", "v10_bez_czasu")
MIERNIK = "sedzia_v3"


def decyzje(pary: list[dict], rek: dict, p9: dict, p10: dict) -> None:
    """Decyzje wszystkich wersji na tych samych destylacjach (rek: klucz usługi → destylacja v9)."""
    s9, s10 = jako_schemat(p9), jako_schemat(p10)
    for p in pary:
        a, b = rek.get(p["ka"]), rek.get(p["kb"])
        fa = {"nazwa": p["a"]["nazwa"], "duration_minutes": p.get("czas_a")}
        fb = {"nazwa": p["b"]["nazwa"], "duration_minutes": p.get("czas_b")}
        p["v9"] = porownaj_v8(a, b, p9, fa, fb, _sch=s9)[0]
        p["v10"] = porownaj_v10(a, b, p10, fa, fb, _sch=s10)[0]
        p["v10_bez_czasu"] = porownaj_v10(a, b, p10, fa, fb, _sch=s10, regula_czasu=False)[0]


def metryki(pary: list[dict], w: str, miernik: str = MIERNIK) -> dict:
    z = [p for p in pary if p.get(miernik) in ("tozsame", "powiazane", "rozne") and p.get(w)]
    tp = sum(1 for p in z if p[w] == "tozsame" and p[miernik] == "tozsame")
    fp = sum(1 for p in z if p[w] == "tozsame" and p[miernik] != "tozsame")
    fn = sum(1 for p in z if p[w] != "tozsame" and p[miernik] == "tozsame")
    return {"par": len(z), "precyzja": round(tp / max(tp + fp, 1) * 100, 1), "czulosc": round(tp / max(tp + fn, 1) * 100, 1),
            "trafnosc": round((len(z) - fp - fn) / max(len(z), 1) * 100, 1), "mowi_ta_sama": tp + fp}


def bilans(pary: list[dict], stara: str, nowa: str, miernik: str = MIERNIK) -> dict:
    """Ile par zmieniło decyzję „ta sama / nie ta sama” na zgodną z miernikiem, a ile na niezgodną."""
    lepiej = gorzej = 0
    for p in pary:
        m = p.get(miernik)
        if m not in ("tozsame", "powiazane", "rozne") or not p.get(stara) or not p.get(nowa):
            continue
        ok_s, ok_n = (p[stara] == "tozsame") == (m == "tozsame"), (p[nowa] == "tozsame") == (m == "tozsame")
        lepiej += ok_n and not ok_s
        gorzej += ok_s and not ok_n
    return {"lepiej": lepiej, "gorzej": gorzej}


def raport(pary: list[dict]) -> dict:
    grupy = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    return {g: {**{w: metryki(z, w) for w in WERSJE},
                **{f"bilans_v9_{w}": bilans(z, "v9", w) for w in WERSJE[1:]},
                "salonow": len({p["salon"] for p in z})} for g, z in grupy.items()}


async def ocen_brakujace(pary: list[dict], sedzia_v3: dict, budzet: float) -> float:
    brak = [(p["a"], p["b"]) for p in pary if para_klucz(p["a"], p["b"]) not in sedzia_v3]
    if not brak:
        return 0.0
    if len(brak) * TOK_PARA * CENA_TOK > budzet:
        raise SystemExit(f"{len(brak)} par bez oceny — ponad budżet")
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        s = OcenaPar(None, client, budzet_usd=budzet, wersja=WERSJA_V3)
        sedzia_v3.update(await s.ocen(brak))
    return s.koszt_usd


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    p9 = json.loads((DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    p10 = schemat_v10(p9)
    plik = OUT / "sedzia_v3.json"
    sedzia_v3: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    wszystkie: list[dict] = []
    for z, d in ZBIORY.items():
        pary = json.loads((d / "pary.json").read_text(encoding="utf-8"))
        rek = json.loads((d / "destylacje_v9.json").read_text(encoding="utf-8"))
        decyzje(pary, rek, p9, p10)
        wszystkie += [{**p, "zbior": z} for p in pary]
    koszt = await ocen_brakujace(wszystkie, sedzia_v3, a.budzet)
    plik.write_text(json.dumps(sedzia_v3, ensure_ascii=False), encoding="utf-8")
    for p in wszystkie:
        p[MIERNIK] = (sedzia_v3.get(para_klucz(p["a"], p["b"])) or {}).get("werdykt")
    wynik = {"koszt_usd": round(koszt, 4),
             "oba_zbiory": raport(wszystkie),
             **{z: raport([p for p in wszystkie if p["zbior"] == z]) for z in ZBIORY},
             "zmiana_miernika_v2_v3": dict(Counter(f"{p.get('sedzia')}→{p.get(MIERNIK)}" for p in wszystkie
                                                   if p.get("sedzia") != p.get(MIERNIK)))}
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "pary.json").write_text(json.dumps(wszystkie, ensure_ascii=False), encoding="utf-8")
    for g, d in wynik["oba_zbiory"].items():
        print(f"{g:<20} sal {d['salonow']:>2} par {d['v9']['par']:>4} | "
              + " | ".join(f"{w}: P{d[w]['precyzja']} C{d[w]['czulosc']}" for w in WERSJE)
              + " | " + " ".join(f"{w}: +{d[f'bilans_v9_{w}']['lepiej']}/-{d[f'bilans_v9_{w}']['gorzej']}" for w in WERSJE[1:]))


def main() -> None:
    p = argparse.ArgumentParser(description="Schemat v10 na zbiorach roboczych (bez bazy)")
    p.add_argument("--budzet", type=float, default=0.3, help="USD na oceny sędziego brakujących par")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
