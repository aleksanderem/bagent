"""Sprawdzian schematu v10 na NOWYCH, losowych salonach — dowód wg bramki uniwersalności (bd BEAUTY_AUDIT-asrk).

Salony: po 2 z każdej z 9 branż Booksy (ocena_trafnosci.BRANZE), różne miasta, inne niż 48
użytych w pomiarach 25–27.09. Te same pary dla obu wersji: usługa salonu × kandydaci z dzisiejszej
sieci (80 najbliższych, ≥ 0,68, 15 km). Jedna usługa na wywołanie TypeSafe (decyzja Alexa 27.09).

  v9             destylacja v9 (rodzaj, cechy, zestaw/dodatek), porównanie v8,
  v10            ta sama destylacja, porównanie v10 (dodatek od 0,5, liczby sztuk, poziom z nazwy,
                 liczba osób domyślnie jedna) — v10 nie zmienia pytań, więc nie kosztuje więcej,
  v10_bez_czasu  jak v10, bez reguły czasu (decyzja Alexa 28.09 — do potwierdzenia pomiarem).

Miernik: sędzia par v3 (czas trwania nie jest różnicą). Wynik per branża: precyzja i czułość
„ta sama”, bilans par w obie strony, pokrycie wierszy (≥ 3 salony z tą samą usługą) i liczba salonów.
Czyta bazę produkcyjną (salony, usługi, wektory) — tylko odczyt; oceny sędziego nie są zapisywane.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/schemat_v10_sprawdzian.py --na-branze 2 --uslug 7 --budzet 2.5
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
import schemat_v10_pomiar as rob  # noqa: E402
import siec_kandydatow as sk  # noqa: E402
import warianty_destylacji as wd  # noqa: E402

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import dzieci  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from services.typesafe_drzewo.schemat_v10 import schemat_v10  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import WERSJA_V3, para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20260929
OUT = sk.DANE / "2026-09-28" / "v10_sprawdzian"
TOK_USLUGA = 8_300  # destylacja v9 (zmierzone 27.09)
MIN_SALONOW = 3  # ten sam próg co silnik (min_salons_thin)


def uzyte() -> set[int]:
    u = sk.uzyte_salony()
    for f in (sk.DANE / "2026-09-26" / "siec" / "salony.json", sk.DANE / "2026-09-27" / "warianty" / "salony.json",
              sk.DANE / "2026-09-27" / "diagnoza" / "salony.json"):
        if f.exists():
            u |= {b for _br, b, _m in json.loads(f.read_text(encoding="utf-8"))}
    return u


async def losuj(sb: SupabaseService, na_branze: int, rng: random.Random, pomin: set[int]) -> list[tuple]:
    return await sk.losuj(sb, na_branze * len(wd.ot.BRANZE), rng, pomin)


def pokrycie(pary: list[dict], w: str, miernik: str | None = None) -> dict:
    """Udział usług salonu, dla których ≥ 3 inne salony mają tę samą usługę (wg wersji / potwierdzone miernikiem)."""
    salony: dict[str, set] = {}
    for p in pary:
        salony.setdefault(p["ka"], set())
        if p.get(w) == "tozsame" and (miernik is None or p.get(miernik) == "tozsame"):
            salony[p["ka"]].add(p["cand_salon"])
    n = max(len(salony), 1)
    return {"uslug": len(salony), "z_cena_proc": round(sum(len(s) >= MIN_SALONOW for s in salony.values()) / n * 100, 1)}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    rng = random.Random(SEED)
    salony = await losuj(sb, a.na_branze, rng, uzyte())
    (OUT / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print("salony:", ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)
    branze_nazwy = wd.ot._nazwy_branz(cli)
    do_dest, pary, ceny = {}, [], {}
    for branza, bid, miasto, dane in salony:
        uslugi = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        ceny[bid] = statistics.median(s["price_grosze"] / 100 for s in uslugi) if uslugi else None
        uslugi, _ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, uslugi, [int(s["id"]) for s in uslugi], bid)
        uslugi = [s for s in uslugi if int(s["id"]) in emb]
        if not uslugi:
            continue
        uslugi = rng.sample(uslugi, min(a.uslug, len(uslugi)))
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, sk.PROMIEN_KM) if b != bid]
        kand = search_twins([int(s["id"]) for s in uslugi], pula, subject_embeddings=emb, limit=wd.NET[0], min_similarity=wd.NET[1], exact=True)
        typy = wd.ot._typy_salonow(cli, {c["booksy_id"] for cs in kand.values() for c in cs} | {bid}, branze_nazwy)
        for s in uslugi:
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
        print(f"  {branza:<20} {miasto:<18} mediana ceny {ceny[bid] or 0:>6.0f} zł | pula {len(pula):>5} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    szac = len(do_dest) * TOK_USLUGA * wd.CENA_TOK + len(pary) * rob.TOK_PARA * wd.CENA_TOK
    print(f"usług {len(do_dest)}, par {len(pary)}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz --uslug albo --na-branze")
    p9 = json.loads((sk.DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    p10 = schemat_v10(p9)
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    v9, tr, tc = {}, [0], [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        await wd.destyluj(client, "A", do_dest, p9, dzieci(p9), v9, tr, tc)
        sedzia = OcenaPar(None, client, budzet_usd=0.6, wersja=WERSJA_V3)
        oc = await sedzia.ocen([(p["a"], p["b"]) for p in pary])
    for p in pary:
        p["sedzia_v3"] = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("werdykt")
    rob.decyzje(pary, v9, p9, p10)
    for nazwa, d in (("destylacje_v9", v9), ("pary", pary)):
        (OUT / f"{nazwa}.json").write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")
    grupy = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    wynik = {
        "koszt_usd": {"destylacja_v9": round((tr[0] + tc[0]) * wd.CENA_TOK, 3), "sedzia_v3": round(sedzia.koszt_usd, 3)},
        "salony": [{"branza": b, "salon": i, "miasto": m, "mediana_ceny_zl": ceny.get(i)} for b, i, m, _ in salony],
        "per_grupa": {g: {**{w: rob.metryki(z, w) for w in rob.WERSJE},
                          **{f"bilans_v9_{w}": rob.bilans(z, "v9", w) for w in rob.WERSJE[1:]},
                          "pokrycie": {w: {"wg_wersji": pokrycie(z, w), "potwierdzone": pokrycie(z, w, "sedzia_v3")} for w in rob.WERSJE},
                          "salonow": len({p["salon"] for p in z}), "par": len(z)} for g, z in grupy.items()},
    }
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(wynik, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Sprawdzian v10 na nowych, losowych salonach")
    p.add_argument("--na-branze", type=int, default=2)
    p.add_argument("--uslug", type=int, default=7)
    p.add_argument("--budzet", type=float, default=2.5, help="USD łącznie")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
