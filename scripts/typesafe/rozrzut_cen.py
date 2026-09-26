"""Miara bez etykiet: rozrzut ceny za minutę w grupie uznanej za „tę samą usługę”.

Jeśli metoda dobrze wybiera tożsame usługi, ich ceny za minutę leżą bliżej
siebie niż w surowym klastrze silnika (tam siedzą warianty i obce zabiegi).
Porównujemy trzy grupy dla każdego wiersza wyceny:
  * silnik — wszystkie próbki, z których liczy się dziś cena,
  * sędzia — próbki uznane za tożsame przez sędziego par v2,
  * drzewo — próbki tożsame z klasyfikacji po drzewie (v4: nazwa, opis,
    kategoria, typ salonu, zabieg Booksy; czas i pakiet w kodzie).
Reguła cenowa drzewa jest WYŁĄCZONA — cena nie może oceniać sama siebie.

Rozrzut = (p75 − p25) / p50 ceny za minutę, tylko grupy z ≥ 3 salonami.
Wynik per branża: mediana rozrzutu, pokrycie wierszy i bilans wiersz po
wierszu (ile grup ciaśniejszych, ile luźniejszych niż silnik).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/rozrzut_cen.py --wiersze <ocena_trafnosci_wiersze.json> --out <katalog>
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
if not os.environ.get("TYPESAFE_API_KEY") and KEY_FILE.exists():
    os.environ["TYPESAFE_API_KEY"] = KEY_FILE.read_text(encoding="utf-8").strip()

from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo import porownanie  # noqa: E402
from services.typesafe_drzewo.drzewo import WERSJA  # noqa: E402
from services.typesafe_drzewo.klasyfikacja import KLASYFIKACJA_WERSJA, MODEL, klasyfikuj_wiele  # noqa: E402
from services.typesafe_profile.destylacja import nk  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

porownanie.PRICE_RATIO_AGAINST = math.inf  # cena nie ocenia sama siebie
MIN_SALONOW = 3


def klucz(n: str, kat: str | None, typ: str | None, opis: str | None, zb: str | None) -> str:
    h = hashlib.sha1(f"{nk(opis)}|{nk(zb)}".encode()).hexdigest()[:10]
    return f"{nk(n)}|{nk(kat)}|{nk(typ)}|{h}"


def rozrzut(probki: list[dict[str, Any]]) -> float | None:
    ok = [p for p in probki if not p.get("pakiet") and p.get("cena") and p.get("czas")]
    if len({p["booksy_id"] for p in ok}) < MIN_SALONOW:
        return None
    ppm = sorted(p["cena"] / p["czas"] for p in ok)
    q = st.quantiles(ppm, n=4) if len(ppm) >= 2 else [ppm[0]] * 3
    return (q[2] - q[0]) / q[1] if q[1] else None


def _paczki(xs: list, n: int):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    dane: dict[str, list[dict]] = json.loads(Path(args.wiersze).read_text(encoding="utf-8"))
    branze = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    wiersze = [(b, w) for b, ws in dane.items() for w in ws]
    ids = sorted({p["service_id"] for _, w in wiersze for p in w["probki"] if p.get("service_id")})
    bids = sorted({p["booksy_id"] for _, w in wiersze for p in w["probki"] if p.get("booksy_id")} | {w["salon"] for _, w in wiersze})
    sv: dict[int, dict] = {}
    for part in _paczki(ids, 200):
        for r in cli.table("salon_scrape_services").select("id,description,treatment_name,category_name").in_("id", part).execute().data or []:
            sv[r["id"]] = r
    typ: dict[int, str] = {}
    for part in _paczki(bids, 200):
        for r in cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", part).execute().data or []:
            typ[r["booksy_id"]] = branze.get(r.get("primary_category_id"), "")
    podmioty: dict[int, dict[str, dict]] = {}
    for part in _paczki(sorted({w["salon"] for _, w in wiersze}), 40):
        for bid, c in (await sb.get_competitor_full_data(part)).items():
            podmioty[bid] = {nk(s.get("name")): s for s in c.get("services") or []}

    uslugi: dict[str, tuple] = {}
    fakty: dict[str, dict] = {}
    zestawy = []
    for branza, w in wiersze:
        s = podmioty.get(w["salon"], {}).get(nk(w["usluga"]), {})
        ta = (w["usluga"], s.get("category_name"), typ.get(w["salon"], ""), s.get("description"), s.get("treatment_name"))
        ka = klucz(*ta)
        uslugi[ka] = ta
        fakty[ka] = {"nazwa": w["usluga"], "opis": s.get("description"), "price_grosze": w.get("cena_podmiotu"),
                     "duration_minutes": w.get("czas_podmiotu"), "is_package": s.get("is_package")}
        kb_list = []
        for p in w["probki"]:
            r = sv.get(p.get("service_id"), {})
            tb = (p["nazwa"], p.get("kategoria") or r.get("category_name"), typ.get(p.get("booksy_id"), ""), r.get("description"), r.get("treatment_name"))
            kb = klucz(*tb)
            uslugi[kb] = tb
            fakty[kb] = {"nazwa": p["nazwa"], "opis": r.get("description"), "price_grosze": p.get("cena"),
                         "duration_minutes": p.get("czas"), "is_package": p.get("pakiet")}
            kb_list.append(kb)
        zestawy.append((branza, w, ka, kb_list))

    plik = out / f"klasyfikacje_d{WERSJA}_k{KLASYFIKACJA_WERSJA}.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = {k: v for k, v in uslugi.items() if k not in pamiec}
    print(f"usług {len(uslugi)}, do klasyfikacji {len(brak)} (szac. {len(brak) * 0.00026:.2f} USD)", flush=True)
    if len(brak) * 0.00026 > args.budzet:
        raise SystemExit(f"szacunek ponad budżet {args.budzet} USD — przerwano")
    async with AsyncTypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"], model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        nowe, tok = await klasyfikuj_wiele(client, brak)
    pamiec.update(nowe)
    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    print(f"sklasyfikowano {len(nowe)}, koszt {tok * 0.042 / 1e6:.3f} USD", flush=True)

    per_b: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for branza, w, ka, kb_list in zestawy:
        drzewo = [porownanie.werdykt(pamiec.get(ka), pamiec.get(kb), fa=fakty[ka], fb=fakty[kb])[0] for kb in kb_list]
        grupy = {
            "silnik": w["probki"],
            "sedzia": [p for p in w["probki"] if p.get("werdykt") == "tozsame"],
            "drzewo": [p for p, d in zip(w["probki"], drzewo) if d == "tozsame"],
        }
        r = {k: rozrzut(v) for k, v in grupy.items()}
        for k, v in r.items():
            per_b[branza][k].append(v)
        per_b[branza]["tozsame_drzewo"].append(sum(1 for d in drzewo if d == "tozsame") / max(len(drzewo), 1))

    def podsumuj(d: dict[str, list]) -> dict[str, Any]:
        n = len(d["silnik"])
        wynik: dict[str, Any] = {"wierszy": n, "tozsame_drzewo_proc": round(st.mean(d["tozsame_drzewo"]) * 100, 1)}
        for m in ("silnik", "sedzia", "drzewo"):
            vals = [x for x in d[m] if x is not None]
            wynik[m] = {"pokrycie_proc": round(len(vals) / max(n, 1) * 100, 1),
                        "mediana_rozrzutu": round(st.median(vals), 3) if vals else None}
        for m in ("sedzia", "drzewo"):
            pary = [(a, b) for a, b in zip(d["silnik"], d[m]) if a is not None and b is not None]
            wynik[m]["ciasniej_niz_silnik"] = sum(1 for a, b in pary if b < a - 1e-9)
            wynik[m]["luzniej_niz_silnik"] = sum(1 for a, b in pary if b > a + 1e-9)
        return wynik

    lacznie: dict[str, list] = defaultdict(list)
    for d in per_b.values():
        for k, v in d.items():
            lacznie[k].extend(v)
    raport = {"lacznie": podsumuj(lacznie), "per_branza": {b: podsumuj(d) for b, d in per_b.items()}}
    (out / "rozrzut.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Rozrzut cen w grupach tożsamych: silnik vs sędzia vs drzewo")
    p.add_argument("--wiersze", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--budzet", type=float, default=3.0)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
