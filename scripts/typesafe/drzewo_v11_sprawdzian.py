"""Sprawdzian drzewa usług v11b na NOWYCH, losowych salonach — dowód wg bramki uniwersalności (bd BEAUTY_AUDIT-asrk).

Zasada niezmienna (Alex, 28.09): matching = cookbook TypeSafe hierarchical_classification — drzewo,
wiązka 3 na każdym poziomie, „ta sama” rozstrzyga drzewo. Ten skrypt tylko mierzy.

Salony: po NA_BRANZE z każdej z 9 branż Booksy, różne miasta, inne niż użyte w pomiarach 25–27.09.
Te same pary dla obu wersji: usługa salonu × kandydaci z dzisiejszej sieci (80 najbliższych, ≥ 0,68, 15 km).
  v9        destylacja v9 (rodzaj, cechy, zestaw/dodatek), porównanie v8,
  v11b      przejście po drzewie drzewo_v11b.json, porównanie drzewem (bez reguły czasu — decyzja 28.09),
  v11b_czas to samo z regułą czasu (do rozstrzygnięcia).
Drzewo zbudowano z nazw innych salonów (budowa 28.09) — nowe salony są poza nim.
Miernik: sędzia par v3, bez zapisu do bazy. Baza produkcyjna: tylko odczyt (zgoda Alexa 28.09).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_v11_sprawdzian.py --na-branze 2 --uslug 5 --budzet 3.0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import drzewo_pomiar as dp  # noqa: E402
import schemat_v10_sprawdzian as sv  # noqa: E402
import siec_kandydatow as sk  # noqa: E402
import warianty_destylacji as wd  # noqa: E402

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.drzewo_uslug import porownaj_v11  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci, jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from services.typesafe_drzewo.schemat_v10 import schemat_v10  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import WERSJA_V3, para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20260930
DZ = sk.DANE / "2026-09-28"
OUT = DZ / "sprawdzian_drzewo"
TOK_V9, TOK_V11, TOK_PARA = 8300, 8100, 900
WERSJE = ("v9", "v11b", "v11b_czas")


def pamiec_v9() -> dict:
    out: dict = {}
    for f in (sk.DANE / "2026-09-27" / "warianty" / "destylacje_v9.json", sk.DANE / "2026-09-27" / "diagnoza" / "destylacje_v9.json"):
        out.update(json.loads(f.read_text(encoding="utf-8")))
    return out


async def pary_salonow(sb: SupabaseService, salony: list, uslug: int, rng: random.Random) -> tuple[dict, list, dict]:
    cli = sb.client
    branze_nazwy = wd.ot._nazwy_branz(cli)
    do_dest, pary, ceny = {}, [], {}
    for branza, bid, miasto, dane in salony:
        us = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        ceny[bid] = round(statistics.median(s["price_grosze"] / 100 for s in us)) if us else None
        us, _ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, us, [int(s["id"]) for s in us], bid)
        us = [s for s in us if int(s["id"]) in emb]
        if not us:
            continue
        us = rng.sample(us, min(uslug, len(us)))
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, sk.PROMIEN_KM) if b != bid]
        kand = search_twins([int(s["id"]) for s in us], pula, subject_embeddings=emb, limit=wd.NET[0], min_similarity=wd.NET[1], exact=True)
        typy = wd.ot._typy_salonow(cli, {c["booksy_id"] for cs in kand.values() for c in cs} | {bid}, branze_nazwy)
        for s in us:
            ka = sk.klucz(s.get("name"), s.get("category_name"), typy.get(bid))
            do_dest[ka] = (s.get("name") or "", s.get("category_name"), typy.get(bid))
            for c in kand.get(int(s["id"]), []):
                t = typy.get(c["booksy_id"])
                kb = sk.klucz(c["service_name"], c.get("category_name"), t)
                do_dest[kb] = (c["service_name"], c.get("category_name"), t)
                pary.append({"branza": branza, "salon": bid, "ka": ka, "kb": kb, "cand_salon": c["booksy_id"], "sim": c["similarity"],
                             "a": strona(s.get("name"), s.get("category_name"), typy.get(bid)),
                             "b": strona(c["service_name"], c.get("category_name"), t),
                             "czas_a": s.get("duration_minutes"), "czas_b": c.get("duration_minutes")})
        print(f"  {branza:<20} {miasto:<18} mediana ceny {ceny[bid] or 0:>5} zł | pula {len(pula):>5} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    return do_dest, pary, ceny


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    rng = random.Random(SEED)
    salony = await sk.losuj(sb, a.na_branze * len(wd.ot.BRANZE), rng, sv.uzyte())
    (OUT / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print("salony:", ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)
    do_dest, pary, ceny = await pary_salonow(sb, salony, a.uslug, rng)
    drzewo = json.loads((DZ / "drzewo" / "drzewo_v11b.json").read_text(encoding="utf-8"))
    grupy = json.loads((DZ / "drzewo" / "grupy_uslug.json").read_text(encoding="utf-8"))
    p9 = json.loads((sk.DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    p10 = schemat_v10(p9)
    v9 = {k: r for k, r in pamiec_v9().items() if k in do_dest}
    v11 = {k: r for k, r in json.loads((DZ / "drzewo" / "destylacje_v11b.json").read_text(encoding="utf-8")).items() if k in do_dest}
    brak9, brak11 = sum(k not in v9 for k in do_dest), sum(k not in v11 for k in do_dest)
    szac = (brak9 * TOK_V9 + brak11 * TOK_V11 + len(pary) * TOK_PARA) * wd.CENA_TOK
    print(f"usług {len(do_dest)} (bez v9: {brak9}, bez drzewa: {brak11}), par {len(pary)}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz --uslug albo --na-branze")
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    tr, tc, t11 = [0], [0], [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        await wd.destyluj(client, "A", do_dest, p9, dzieci(p9), v9, tr, tc)
        uslugi = {k: strona(*v) for k, v in do_dest.items()}
        await dp.destyluj(client, uslugi, drzewo, p10, v9, grupy, v11, t11)
        sedzia = OcenaPar(None, client, budzet_usd=0.8, wersja=WERSJA_V3)  # bez bazy: tylko odczyt prod
        oc = await sedzia.ocen([(p["a"], p["b"]) for p in pary])
    s9, s10 = jako_schemat(p9), jako_schemat(p10)
    for p in pary:
        p["sedzia_v3"] = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("werdykt")
        fa = {"nazwa": p["a"]["nazwa"], "duration_minutes": p.get("czas_a")}
        fb = {"nazwa": p["b"]["nazwa"], "duration_minutes": p.get("czas_b")}
        p["v9"] = porownaj_v8(v9.get(p["ka"]), v9.get(p["kb"]), p9, fa, fb, _sch=s9)[0]
        p["v11b"] = porownaj_v11(v11.get(p["ka"]), v11.get(p["kb"]), p10, fa, fb, _sch=s10)[0]
        p["v11b_czas"] = porownaj_v11(v11.get(p["ka"]), v11.get(p["kb"]), p10, fa, fb, _sch=s10, regula_czasu=True)[0]
    for nazwa, d in (("destylacje_v9", v9), ("destylacje_v11b", v11), ("pary", pary)):
        (OUT / f"{nazwa}.json").write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
    grupy_par = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    wynik = {
        "koszt_usd": {"v9": round((tr[0] + tc[0]) * wd.CENA_TOK, 3), "drzewo": round(t11[0] * wd.CENA_TOK, 3), "sedzia_v3": round(sedzia.koszt_usd, 3)},
        "salony": [{"branza": b, "salon": i, "miasto": m, "mediana_ceny_zl": ceny.get(i)} for b, i, m, _ in salony],
        "per_branza": {g: {**{w: dp.metryki(z, w) for w in WERSJE},
                           **{f"bilans_v9_{w}": dp.bilans(z, "v9", w) for w in WERSJE[1:]},
                           "pokrycie": {w: {"wg_wersji": sv.pokrycie(z, w), "potwierdzone": sv.pokrycie(z, w, "sedzia_v3")} for w in WERSJE[:2]},
                           "salonow": len({p["salon"] for p in z}), "par": len(z)} for g, z in grupy_par.items()},
        "bledy_wezlow": dp.bledy_wezlow(pary, v11, "v11b"),
    }
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"koszt: {wynik['koszt_usd']}")
    for g, d in wynik["per_branza"].items():
        pk = d["pokrycie"]
        print(f"{g:<20} sal {d['salonow']:>2} par {d['par']:>4} | "
              + " | ".join(f"{w}: P{d[w]['precyzja']} C{d[w]['czulosc']}" for w in WERSJE)
              + f" | v11b vs v9 +{d['bilans_v9_v11b']['lepiej']}/-{d['bilans_v9_v11b']['gorzej']}"
              + f" | z ceną (potw.) v9 {pk['v9']['potwierdzone']['z_cena_proc']}% v11b {pk['v11b']['potwierdzone']['z_cena_proc']}%")
    print("\nwęzły z największą liczbą błędów (węzeł, par, błędów):")
    for wz, n, z in wynik["bledy_wezlow"]:
        print(f"  {wz[:60]:<60} {n:>5} {z:>4}")


def main() -> None:
    p = argparse.ArgumentParser(description="Sprawdzian drzewa v11b na nowych, losowych salonach")
    p.add_argument("--na-branze", type=int, default=2)
    p.add_argument("--uslug", type=int, default=5)
    p.add_argument("--budzet", type=float, default=3.0, help="USD łącznie")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
