"""Sprawdzian drzewa v12 BEZ sędziego na 18 NOWYCH salonach (bd BEAUTY_AUDIT-asrk, 28.09).

Decyzja Alexa 28.09: sędzia par wyłączony; wynikiem jest poziom pokrycia ścieżek w drzewie.
Miary:
  * pokrycie — udział usług salonu, dla których ≥ 3 inne salony mają tę samą ścieżkę (tak liczy wycena),
    oraz rozkład poziomu, na którym ścieżki się rozchodzą;
  * spójność — pary o identycznej nazwie (po normalizacji) muszą mieć tę samą ścieżkę;
  * przegląd — dla każdej branży kilka usług salonu z listą konkurentów, werdyktem i ścieżką: do oceny przez Alexa.
Usługi mają pełny kontekst Booksy (kategoria, nazwa, opis, warianty, zabieg z Booksy, salon).
Baza produkcyjna: tylko odczyt.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v12_sprawdzian.py --uslug 6 --budzet 2.0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import schemat_v10_sprawdzian as sv  # noqa: E402
import siec_kandydatow as sk  # noqa: E402
import v12_eksport as ve  # noqa: E402

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.drzewo_v12 import p_ta_sama, porownaj_v12, wiazka_v12  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402
from services.typesafe_drzewo.podzial import _nazwa  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from services.typesafe_drzewo.schemat_v10 import liczby_v10  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20261002
DZ = sk.DANE / "2026-09-28"
OUT = DZ / "v12_sprawdzian"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_USLUGA = 10000
ROWNOLEGLE = 30
MIN_SALONOW = 3
POLA = ("id,booksy_id,category_name,name,description,variants,treatment_name,treatment_parent_id,combo_type,"
        "is_package,is_promo,price_grosze,duration_minutes")


def wiersz(r: dict, salon: str, typ: str) -> dict:
    return {"booksy_id": r.get("booksy_id"), "salon": salon, "typ_salonu": typ, "id": r.get("id"),
            "kategoria": r.get("category_name") or "", "nazwa": r.get("name") or "",
            "opis": " ".join((r.get("description") or "").split())[:ve.OPIS_MAX], "warianty": ve.warianty(r.get("variants")),
            "zabieg_booksy": r.get("treatment_name") or "", "pakiet": bool(r.get("is_package")),
            "cena_gr": r.get("price_grosze"), "min": r.get("duration_minutes")}


async def zbierz(sb: SupabaseService, uslug: int) -> tuple[dict, list]:
    cli = sb.client
    kategorie = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    salony = await salony_sprawdzianu(sb)
    rng = random.Random(SEED)
    uslugi, pary = {}, []
    for branza, bid, miasto in salony:
        dane = (await sb.get_competitor_full_data([bid])).get(bid) or {}
        sc = dane.get("scrape") or {}
        us = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        us, _ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, us, [int(s["id"]) for s in us], bid)
        us = [s for s in us if int(s["id"]) in emb]
        if not us:
            continue
        us = rng.sample(us, min(uslug, len(us)))
        for s in us:
            uslugi[int(s["id"])] = wiersz({**s, "booksy_id": bid}, sc.get("salon_name") or "", branza)
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, sk.PROMIEN_KM) if b != bid]
        kand = search_twins([int(s["id"]) for s in us], pula, subject_embeddings=emb, limit=80, min_similarity=0.68, exact=True)
        for s in us:
            for c in kand.get(int(s["id"]), []):
                pary.append({"branza": branza, "miasto": miasto, "salon": bid, "a": int(s["id"]), "b": c["service_id"], "cand_salon": c["booksy_id"]})
        print(f"  {branza:<20} {miasto:<18} usług {len(us)} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    brak = sorted({p["b"] for p in pary} - set(uslugi))
    sal_ids = set()
    for i in range(0, len(brak), 200):
        for r in cli.table("salon_scrape_services").select(POLA).in_("id", brak[i:i + 200]).execute().data or []:
            uslugi[int(r["id"])] = wiersz(r, "", "")
            sal_ids.add(r.get("booksy_id"))
    ids = sorted(i for i in sal_ids if i)
    for i in range(0, len(ids), 200):
        for r in cli.table("salons").select("booksy_id,name,primary_category_id").in_("booksy_id", ids[i:i + 200]).execute().data or []:
            for u in uslugi.values():
                if u["booksy_id"] == r["booksy_id"] and not u["salon"]:
                    u["salon"], u["typ_salonu"] = r.get("name") or "", kategorie.get(r.get("primary_category_id"), "")
    return uslugi, pary


async def salony_sprawdzianu(sb: SupabaseService, na_branze: int = 2) -> list:
    """Nowe salony, nieużyte nigdzie wcześniej: ani w diagnozie, ani w sprawdzianie v11b, ani w budowie drzewa v12.
    Bramka uniwersalności: dowód na salonach innych niż te, na których diagnozowano."""
    plik = OUT / "salony.json"
    if plik.exists():
        return json.loads(plik.read_text(encoding="utf-8"))
    pomin = sv.uzyte() | ve.odlozone() | {s["booksy_id"] for s in json.loads((DZ / "v12" / "salony.json").read_text(encoding="utf-8"))}
    wybrane = await sv.losuj(sb, na_branze, random.Random(SEED + 1), pomin)
    out = [[b, i, m] for b, i, m, _d in wybrane]
    plik.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"nowe salony: {len(out)} (pominięto {len(pomin)} użytych)", flush=True)
    return out


def pokrycie(pary: list[dict], progi: tuple[int, ...] = (5, 4, 3, 2)) -> dict:
    """Udział usług salonu z ≥ 3 innymi salonami na tej samej ścieżce (5) albo zgodnych do poziomu 4, 3, 2."""
    na_poziomie: dict[int, dict[int, set]] = {p: defaultdict(set) for p in progi}
    uslugi = {p["a"] for p in pary}
    for p in pary:
        for prog in progi:
            if (p["werdykt"] == "tozsame") if prog == 5 else (p["poziom"] >= prog):
                na_poziomie[prog][p["a"]].add(p["cand_salon"])
    return {f"poziom_{prog}": round(sum(len(na_poziomie[prog][u]) >= MIN_SALONOW for u in uslugi) / max(len(uslugi), 1) * 100, 1)
            for prog in progi} | {"uslug": len(uslugi)}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    drzewo = json.loads((DZ / "v12" / a.drzewo).read_text(encoding="utf-8"))
    plik_u = OUT / "uslugi.json"
    if plik_u.exists():
        dane = json.loads(plik_u.read_text(encoding="utf-8"))
        uslugi, pary = {int(k): v for k, v in dane["uslugi"].items()}, dane["pary"]
    else:
        uslugi, pary = await zbierz(SupabaseService(), a.uslug)
        plik_u.write_text(json.dumps({"uslugi": uslugi, "pary": pary}, ensure_ascii=False), encoding="utf-8")
    plik_r = OUT / f"destylacje_{Path(a.drzewo).stem}.json"
    rek: dict = json.loads(plik_r.read_text(encoding="utf-8")) if plik_r.exists() else {}
    brak = [i for i in uslugi if str(i) not in rek]
    szac = len(brak) * TOK_USLUGA * CENA_TOK
    print(f"usług {len(uslugi)}, par {len(pary)}, do przejścia {len(brak)}, szac. {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz --uslug")
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok, bledy = [0], [0]
    sem = asyncio.Semaphore(ROWNOLEGLE)
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        async def jedna(i: int) -> None:
            u = uslugi[i]
            async with sem:
                try:
                    r = await wiazka_v12(client, stan_v12(u), drzewo, tok)
                except Exception as e:  # noqa: BLE001 — jedna usługa bez ścieżki, reszta dalej
                    if " 402 " in str(e):
                        raise SystemExit(f"TypeSafe: brak kredytów — {str(e)[:160]}") from e
                    bledy[0] += 1
                    if bledy[0] <= 5:
                        print(f"  {u['nazwa'][:40]!r}: {type(e).__name__}: {str(e)[:100]}", flush=True)
                    return
            rek[str(i)] = {**r, **liczby_v10(u["nazwa"], None)}
        try:
            await asyncio.gather(*[jedna(i) for i in brak])
        finally:
            plik_r.write_text(json.dumps(rek, ensure_ascii=False), encoding="utf-8")
    plik_r.write_text(json.dumps(rek, ensure_ascii=False), encoding="utf-8")
    for p in pary:
        ua, ub = uslugi[int(p["a"])], uslugi[int(p["b"])]
        p["werdykt"], p["powod"], p["poziom"] = porownaj_v12(rek.get(str(p["a"])), rek.get(str(p["b"])),
                                                              {"nazwa": ua["nazwa"]}, {"nazwa": ub["nazwa"]})
        p["ta_sama_nazwa"] = bool(_nazwa(ua["nazwa"])) and _nazwa(ua["nazwa"]) == _nazwa(ub["nazwa"])
        p["p"] = p_ta_sama(rek.get(str(p["a"])) or {}, rek.get(str(p["b"])) or {})
    grupy = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    wynik = {"koszt_usd": round(tok[0] * CENA_TOK, 3), "bledow": bledy[0],
             "pozycja": dict(Counter(max(r["pozycja"], key=r["pozycja"].get) for r in rek.values() if r.get("pozycja"))),
             "per_branza": {g: {"par": len(z), "werdykty": dict(Counter(p["werdykt"] for p in z)),
                                "poziom_pokrycia": dict(sorted(Counter(p["poziom"] for p in z).items())),
                                "pokrycie": pokrycie(z),
                                "spojnosc_tej_samej_nazwy": f"{sum(p['werdykt'] == 'tozsame' for p in z if p['ta_sama_nazwa'])}/{sum(p['ta_sama_nazwa'] for p in z)}"}
                            for g, z in grupy.items()}}
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "pary.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    przeglad(pary, uslugi, rek)
    print(json.dumps(wynik, ensure_ascii=False, indent=1)[:4000])


def przeglad(pary: list[dict], uslugi: dict, rek: dict, na_branze: int = 3) -> None:
    """Próbka do oceny przez Alexa: usługa salonu → konkurenci z werdyktem, poziomem pokrycia i ścieżką."""
    rng = random.Random(SEED)
    out = []
    for br in sorted({p["branza"] for p in pary}):
        subj = sorted({p["a"] for p in pary if p["branza"] == br})
        for a_id in rng.sample(subj, min(na_branze, len(subj))):
            pa = [p for p in pary if p["a"] == a_id]
            s = rek.get(str(a_id), {}).get("sciezki", [[[]]])[0][0]
            out.append({"branza": br, "usluga": uslugi[int(a_id)]["nazwa"], "kategoria": uslugi[int(a_id)]["kategoria"],
                        "sciezka": " › ".join(s), "konkurenci": [
                            {"nazwa": uslugi[int(p["b"])]["nazwa"], "kategoria": uslugi[int(p["b"])]["kategoria"],
                             "werdykt": p["werdykt"], "poziom": p["poziom"], "powod": p["powod"],
                             "sciezka": " › ".join(rek.get(str(p["b"]), {}).get("sciezki", [[[]]])[0][0])}
                            for p in sorted(pa, key=lambda p: -p["poziom"])[:12]]})
    (OUT / "przeglad.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sprawdzian drzewa v12 bez sędziego")
    p.add_argument("--uslug", type=int, default=6)
    p.add_argument("--budzet", type=float, default=2.0)
    p.add_argument("--drzewo", default="drzewo_v12b.json", help="plik drzewa w dane/2026-09-28/v12/")
    asyncio.run(main_async(p.parse_args()))
