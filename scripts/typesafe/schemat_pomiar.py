"""Pomiar destylacji według schematu z danych: pokrycie taksonomii i rozstrzygalność par.

Wejścia: schemat.json (schemat_osi.py), pary z wyceny 18 salonów
(ocena_trafnosci_wiersze.json z pełnymi danymi próbek) i holdout (mig 188).
Miary główne — bez niczyich ocen:
  * pokrycie: udział usług z rozpoznanym rodzajem i udział cech z konkretną wartością,
  * rozstrzygalność: udział par, które da się rozstrzygnąć (nie „niepełne”),
  * rozkład werdyktów w próbkach, z których silnik liczy dziś cenę.
Pomocniczo: zgodność z etykietami holdoutu (tylko pary rozstrzygnięte).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/schemat_pomiar.py --schemat <schemat.json> --wiersze <wiersze.json> --out <katalog>
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

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"

from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.schemat import INNA, MODEL, NIE_PODANO, WERSJA, _rodzaj, cechy_dla, destyluj, liczby, porownaj, stan  # noqa: E402
from services.typesafe_profile.destylacja import nk  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402
from warianty_sedziego import binarnie  # noqa: E402

KLASY = ("tozsame", "powiazane", "rozne")


def klucz(t: tuple) -> str:
    n, kat, typ, opis, zb, br = t
    return f"{nk(n)}|{nk(kat)}|{nk(typ)}|{hashlib.sha1(f'{nk(opis)}|{nk(zb)}|{br}'.encode()).hexdigest()[:10]}"


def _paczki(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


async def dane(sb: SupabaseService, wiersze_plik: Path):
    cli = sb.client
    br_id = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    wiersze = [(b, w) for b, ws in json.loads(wiersze_plik.read_text(encoding="utf-8")).items() for w in ws]
    hold = cli.table("matching_holdout").select("*").order("id").execute().data or []
    ids = sorted({p["service_id"] for _, w in wiersze for p in w["probki"] if p.get("service_id")})
    bids = sorted({p["booksy_id"] for _, w in wiersze for p in w["probki"] if p.get("booksy_id")} | {w["salon"] for _, w in wiersze}
                  | {r["subject_booksy_id"] for r in hold} | {r["cand_booksy_id"] for r in hold if r.get("cand_booksy_id")})
    sv = {}
    for part in _paczki(ids, 200):
        for r in cli.table("salon_scrape_services").select("id,description,treatment_name,category_name").in_("id", part).execute().data or []:
            sv[r["id"]] = r
    typ = {}
    for part in _paczki(bids, 200):
        for r in cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", part).execute().data or []:
            typ[r["booksy_id"]] = br_id.get(r.get("primary_category_id"), "")
    cennik = {}
    for part in _paczki(bids, 40):
        for bid, c in (await sb.get_competitor_full_data(part)).items():
            for s in c.get("services") or []:
                cennik.setdefault((bid, nk(s.get("name"))), s)
    return wiersze, hold, sv, typ, cennik


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    schemat = json.loads(Path(args.schemat).read_text(encoding="utf-8"))
    sb = SupabaseService()
    wiersze, hold, sv, typ, cennik = await dane(sb, Path(args.wiersze))
    uslugi, fakty = {}, {}

    def dodaj(n, kat, typ_s, s: dict, cena, czas, pakiet) -> str:
        t = (n, kat or s.get("category_name"), typ_s, s.get("description"), s.get("treatment_name"), typ_s)
        k = klucz(t)
        uslugi[k] = t
        fakty[k] = {"nazwa": n, "price_grosze": cena, "duration_minutes": czas or s.get("duration_minutes"), "is_package": pakiet}
        return k

    pary = []  # (źródło, branża, ka, kb, etykieta_modeli, etykieta_czlowieka, id)
    for b, w in wiersze:
        s = cennik.get((w["salon"], nk(w["usluga"])), {})
        ka = dodaj(w["usluga"], s.get("category_name"), typ.get(w["salon"], ""), s, w.get("cena_podmiotu"), w.get("czas_podmiotu"), s.get("is_package"))
        for p in w["probki"]:
            kb = dodaj(p["nazwa"], p.get("kategoria"), typ.get(p.get("booksy_id"), ""), sv.get(p.get("service_id"), {}), p.get("cena"), p.get("czas"), p.get("pakiet"))
            pary.append(("salony", b, ka, kb, None, None, None))
    for r in hold:
        sa = cennik.get((r["subject_booksy_id"], nk(r["subject_name"])), {})
        sbv = cennik.get((r.get("cand_booksy_id"), nk(r["cand_name"])), {})
        ka = dodaj(r["subject_name"], r.get("subject_category"), r["branza"], sa, r.get("subject_price_grosze"), None, sa.get("is_package"))
        kb = dodaj(r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), ""), sbv, r.get("cand_price_grosze"), None, sbv.get("is_package"))
        pary.append(("holdout", r["branza"], ka, kb, r.get("label_final"), r.get("label_human"), r["id"]))

    plik = out / "destylacje_v5.json"
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    # v7: rodzaj od nowa tylko, gdy stary jest nieważny (spoza listy, zestaw, „inny”,
    # niepewny); cechy od nowa dla wszystkich (niepewne = „nieustalone”, nie „inna”).
    lista = set(schemat["rodzaje"])
    do_rodzaju = {k for k, v in uslugi.items() if k not in pamiec or pamiec[k]["rodzaj"] not in lista or pamiec[k]["pewnosc"] < 0.8}
    do_cech = {k for k in uslugi if pamiec.get(k, {}).get("wersja", 0) < WERSJA}
    szac = (len(do_rodzaju) * 4500 + len(do_cech) * 2000) * 0.042 / 1e6
    print(f"usług {len(uslugi)}, rodzaj od nowa {len(do_rodzaju)}, cechy od nowa {len(do_cech)} (szac. {szac:.2f} USD)", flush=True)
    if szac > args.budzet:
        raise SystemExit("ponad budżet")
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    tok = [0]
    sem = asyncio.Semaphore(6)

    async def jedna(k: str) -> None:
        n, kat, typ_s, opis, zb, _br = uslugi[k]
        st = stan(n, kat, typ_s, opis, zb)
        stary = pamiec.get(k, {})
        async with sem:
            try:
                if k in do_rodzaju:
                    rodzaj, pewnosc, sciezki = await _rodzaj(client, st, schemat, tok)
                    top = [[x[1], x[2]] for x in sciezki][:3]
                else:
                    rodzaj, pewnosc, top = stary["rodzaj"], stary["pewnosc"], stary.get("top", [])
                cechy = await cechy_dla(client, st, rodzaj, schemat, tok) if rodzaj != "inny" else {}
            except Exception as e:  # noqa: BLE001
                print(f"v7 {n[:40]!r}: {type(e).__name__}: {str(e)[:100]}")
                return
        pamiec[k] = {"wersja": WERSJA, "rodzaj": rodzaj, "pewnosc": pewnosc, "top": top,
                     "cechy": {rodzaj: cechy}, **liczby(n, opis)}

    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        await asyncio.gather(*[jedna(k) for k in do_rodzaju | do_cech])
    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    print(f"przeliczono {len(do_rodzaju | do_cech)}, koszt {tok[0] * 0.042 / 1e6:.3f} USD", flush=True)

    # pokrycie taksonomii per typ salonu
    pokr = defaultdict(lambda: Counter())
    for k, t in uslugi.items():
        d = pamiec.get(k)
        c = pokr[t[2] or "?"]
        c["uslug"] += 1
        if not d or d["rodzaj"] == "inny":
            continue
        c["z_rodzajem"] += 1
        c["pewny_rodzaj"] += d["pewnosc"] >= 0.8
        cechy = d["cechy"].get(d["rodzaj"], {})
        c["cech"] += len(cechy)
        c["cech_konkretnych"] += sum(1 for v in cechy.values() if v not in (NIE_PODANO, INNA))
        c["cech_inna"] += sum(1 for v in cechy.values() if v == INNA)
    werd = defaultdict(Counter)
    powody = Counter()
    hold_w = []
    for zr, b, ka, kb, lf, lh, i in pary:
        w, pw = porownaj(pamiec.get(ka), pamiec.get(kb), schemat, fakty[ka], fakty[kb])
        if zr == "salony":
            werd[b][w] += 1
            powody[pw.split(",")[0] if w != "tozsame" else w] += 1
        else:
            hold_w.append((w, lf, lh, i))
    rozstrz = [(lf, w) for w, lf, lh, i in hold_w if w != "niepelne" and lf in KLASY]
    raport = {
        "pokrycie_per_typ_salonu": {t: {"uslug": c["uslug"], "rodzaj_proc": round(c["z_rodzajem"] / c["uslug"] * 100, 1),
                                        "pewny_rodzaj_proc": round(c["pewny_rodzaj"] / c["uslug"] * 100, 1),
                                        "cechy_konkretne_proc": round(c["cech_konkretnych"] / max(c["cech"], 1) * 100, 1),
                                        "cechy_inna_proc": round(c["cech_inna"] / max(c["cech"], 1) * 100, 1)}
                                    for t, c in sorted(pokr.items(), key=lambda kv: -kv[1]["uslug"]) if c["uslug"] >= 30},
        "werdykty_w_probkach_ceny": {b: {k: round(v / sum(c.values()) * 100, 1) for k, v in c.most_common()} for b, c in werd.items()},
        "powody_nie_tozsamych": dict(powody.most_common(15)),
        "holdout_rozstrzygniete_proc": round(len(rozstrz) / max(sum(1 for _, lf, _, _ in hold_w if lf in KLASY), 1) * 100, 1),
        "holdout_vs_modele_rozstrzygniete": binarnie(rozstrz),
        "holdout_vs_czlowiek_rozstrzygniete": binarnie([(lh, w) for w, lf, lh, i in hold_w if w != "niepelne" and lh in KLASY]),
    }
    (out / "schemat_raport.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--schemat", required=True)
    p.add_argument("--wiersze", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--budzet", type=float, default=3.0)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
