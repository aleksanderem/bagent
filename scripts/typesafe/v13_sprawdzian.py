"""Sprawdzian drzewa v13 na NOWYCH salonach, z bilansem względem v12b na tych samych usługach (bd BEAUTY_AUDIT-asrk).

Zasady pomiaru (dokument „Matching usług — stan”, zasada 9 i założenia):
  * salony nowe — nieużyte w żadnej diagnozie, sprawdzianie ani budowie drzewa (bramka uniwersalności);
  * osobno każda branża; wektory tylko podsuwają kandydatów, SZEROKO (domyślnie 150 kandydatów, podobieństwo ≥ 0,6);
  * bilans: na tych samych parach werdykt v13 i v12b (poprzednia zmierzona wersja); sędzia par wyłączony,
    więc „lepiej/gorzej” rozstrzyga przegląd Alexa — próbka stawia na pary, w których wersje się różnią;
  * miary bez sędziego: pokrycie (≥ 3 inne salony z tą samą usługą), spójność (identyczne nazwy → ten sam liść),
    rozkład poziomu, na którym ścieżki się rozchodzą.
Baza produkcyjna: tylko odczyt.

Użycie:
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_sprawdzian.py --tylko-zbierz
  ZASADY_OK=1 bagent/.venv/bin/python bagent/scripts/typesafe/v13_sprawdzian.py --budzet 5
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
import v12_sprawdzian as v12s  # noqa: E402

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.qdrant_search import search_twins  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.drzewo_v12 import porownaj_v12, wiazka_v12  # noqa: E402
from services.typesafe_drzewo.drzewo_v13 import porownaj_v13, wiazka_v13  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402
from services.typesafe_drzewo.podzial import _nazwa  # noqa: E402
from services.typesafe_drzewo.schemat import MODEL  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20261004
DZ = sk.DANE / "2026-09-28"
OUT = DZ / "v13_sprawdzian"
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
CENA_TOK = 0.042 / 1e6
TOK_V12B = 8300   # zmierzone 28.09 w sprawdzianie v12b
TOK_V13 = 12400   # zmierzone 28.09 na 20 usługach z danych budowy
ROWNOLEGLE = 30


def uzyte_salony() -> set[int]:
    u = sv.uzyte() | ve.odlozone()
    u |= {s["booksy_id"] for s in json.loads((DZ / "v12" / "salony.json").read_text(encoding="utf-8"))}
    f = DZ / "v12_sprawdzian" / "salony.json"
    if f.exists():
        u |= {b for _br, b, _m in json.loads(f.read_text(encoding="utf-8"))}
    return u


async def zbierz(sb: SupabaseService, a: argparse.Namespace) -> tuple[dict, list, list]:
    cli = sb.client
    kategorie = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    pomin = uzyte_salony()
    wybrane = await sv.losuj(sb, a.na_branze, random.Random(SEED), pomin)
    salony = [[b, i, m] for b, i, m, _d in wybrane]
    print(f"nowe salony: {len(salony)} (pominięto {len(pomin)} użytych)", flush=True)
    rng = random.Random(SEED + 1)
    uslugi, pary = {}, []
    for (branza, bid, miasto), (_b, _i, _m, dane) in zip(salony, wybrane):
        sc = dane.get("scrape") or {}
        us = [s for s in dane.get("services") or [] if s.get("is_active", True) and s.get("price_grosze") and s.get("id")]
        us, _ids, emb = await report_pricing._fetch_subject_embeddings_with_chain_head_fallback(sb, us, [int(s["id"]) for s in us], bid)
        us = [s for s in us if int(s["id"]) in emb]
        if not us:
            continue
        us = rng.sample(us, min(a.uslug, len(us)))
        for s in us:
            uslugi[int(s["id"])] = v12s.wiersz({**s, "booksy_id": bid}, sc.get("salon_name") or "", branza)
        pula = [b for b in report_pricing._geo_competitor_booksy_ids(sb, bid, sk.PROMIEN_KM) if b != bid]
        kand = search_twins([int(s["id"]) for s in us], pula, subject_embeddings=emb, limit=a.kandydatow,
                            min_similarity=a.podobienstwo, exact=True)
        for s in us:
            for c in kand.get(int(s["id"]), []):
                pary.append({"branza": branza, "miasto": miasto, "salon": bid, "a": int(s["id"]), "b": c["service_id"],
                             "cand_salon": c["booksy_id"], "sim": round(c["similarity"], 3)})
        print(f"  {branza:<20} {miasto:<18} usług {len(us)} | par {sum(1 for p in pary if p['salon'] == bid)}", flush=True)
    brak = sorted({p["b"] for p in pary} - set(uslugi))
    sal_ids = set()
    for i in range(0, len(brak), 200):
        for r in cli.table("salon_scrape_services").select(v12s.POLA).in_("id", brak[i:i + 200]).execute().data or []:
            uslugi[int(r["id"])] = v12s.wiersz(r, "", "")
            sal_ids.add(r.get("booksy_id"))
    ids = sorted(i for i in sal_ids if i)
    nazwy: dict[int, tuple[str, str]] = {}
    for i in range(0, len(ids), 200):
        for r in cli.table("salons").select("booksy_id,name,primary_category_id").in_("booksy_id", ids[i:i + 200]).execute().data or []:
            nazwy[r["booksy_id"]] = (r.get("name") or "", kategorie.get(r.get("primary_category_id"), ""))
    for u in uslugi.values():
        if not u["salon"] and u["booksy_id"] in nazwy:
            u["salon"], u["typ_salonu"] = nazwy[u["booksy_id"]]
    return uslugi, pary, salony


async def przejdz(client, uslugi: dict, drzewo: dict, plik: Path, funkcja, tok: list) -> dict:
    rek: dict = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    sem = asyncio.Semaphore(ROWNOLEGLE)
    bledy = [0]

    async def jedna(i: int) -> None:
        async with sem:
            try:
                rek[str(i)] = await funkcja(client, stan_v12(uslugi[i]), drzewo, tok)
            except Exception as e:  # noqa: BLE001 — jedna usługa bez ścieżki, reszta dalej
                if " 402 " in str(e):
                    raise SystemExit(f"TypeSafe: brak kredytów — {str(e)[:160]}") from e
                bledy[0] += 1
    try:
        await asyncio.gather(*[jedna(i) for i in uslugi if str(i) not in rek])
    finally:
        plik.write_text(json.dumps(rek, ensure_ascii=False), encoding="utf-8")
    print(f"  {plik.name}: {len(rek)} ścieżek, błędów {bledy[0]}", flush=True)
    return rek


def lisc13(r: dict | None) -> tuple | None:
    return tuple(r["sciezki"][0][0][1:]) if r and r.get("sciezki") else None


def miary(pary: list[dict], w: str) -> dict:
    return {"werdykty": dict(Counter(p[w] for p in pary)), "pokrycie": v12s.pokrycie([{**p, "werdykt": p[w], "poziom": p[f"{w}_poziom"]} for p in pary])}


async def main_async(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    plik_u = OUT / "uslugi.json"
    if plik_u.exists():
        d = json.loads(plik_u.read_text(encoding="utf-8"))
        uslugi, pary, salony = {int(k): v for k, v in d["uslugi"].items()}, d["pary"], d["salony"]
    else:
        uslugi, pary, salony = await zbierz(SupabaseService(), a)
        plik_u.write_text(json.dumps({"uslugi": uslugi, "pary": pary, "salony": salony}, ensure_ascii=False), encoding="utf-8")
    if a.uslug_pomiar:  # mniej usług salonu w pomiarze (koszt), te same salony i ta sama szeroka sieć
        rng = random.Random(SEED + 2)
        po_salonie: dict[int, list[int]] = defaultdict(list)
        for p in pary:
            if p["a"] not in po_salonie[p["salon"]]:
                po_salonie[p["salon"]].append(p["a"])
        wybrane = {x for s in po_salonie.values() for x in rng.sample(sorted(s), min(a.uslug_pomiar, len(s)))}
        pary = [p for p in pary if p["a"] in wybrane]
    if a.kandydatow_pomiar:  # najbardziej podobni kandydaci z już zebranej, szerokiej sieci
        po_a: dict[int, list[dict]] = defaultdict(list)
        for p in pary:
            po_a[p["a"]].append(p)
        pary = [p for lst in po_a.values() for p in sorted(lst, key=lambda p: -p["sim"])[:a.kandydatow_pomiar]]
    if a.uslug_pomiar or a.kandydatow_pomiar:
        uzyte = {p["a"] for p in pary} | {p["b"] for p in pary}
        uslugi = {i: u for i, u in uslugi.items() if i in uzyte}
    szac = len(uslugi) * (TOK_V13 + TOK_V12B) * CENA_TOK
    print(f"salonów {len(salony)}, usług {len(uslugi)}, par {len(pary)}; szac. v13+v12b {szac:.2f} USD (budżet {a.budzet})", flush=True)
    if a.tylko_zbierz:
        return
    if szac > a.budzet:
        raise SystemExit("ponad budżet — zmniejsz --uslug albo --kandydatow")
    d13 = json.loads((DZ / "v13" / a.drzewo).read_text(encoding="utf-8"))
    d12 = json.loads((DZ / "v12" / "drzewo_v12b.json").read_text(encoding="utf-8"))
    key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    t13, t12 = [0], [0]
    async with AsyncTypeSafeClient(api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=90.0)) as client:
        r13 = await przejdz(client, uslugi, d13, OUT / f"sciezki_{Path(a.drzewo).stem}.json", wiazka_v13, t13)
        r12 = await przejdz(client, uslugi, d12, OUT / "sciezki_v12b.json", wiazka_v12, t12)
    for p in pary:
        ua, ub = uslugi[int(p["a"])], uslugi[int(p["b"])]
        p["v13"], p["v13_powod"], p["v13_poziom"] = porownaj_v13(r13.get(str(p["a"])), r13.get(str(p["b"])))
        p["v12b"], p["v12b_powod"], p["v12b_poziom"] = porownaj_v12(r12.get(str(p["a"])), r12.get(str(p["b"])),
                                                                    {"nazwa": ua["nazwa"]}, {"nazwa": ub["nazwa"]})
        p["ta_sama_nazwa"] = bool(_nazwa(ua["nazwa"])) and _nazwa(ua["nazwa"]) == _nazwa(ub["nazwa"])
        la, lb = lisc13(r13.get(str(p["a"]))), lisc13(r13.get(str(p["b"])))
        p["v13_ten_sam_lisc"] = bool(la and lb and la == lb)
    grupy = {"RAZEM": pary, **{b: [p for p in pary if p["branza"] == b] for b in sorted({p["branza"] for p in pary})}}
    wynik = {"koszt_usd": {"v13": round(t13[0] * CENA_TOK, 3), "v12b": round(t12[0] * CENA_TOK, 3)},
             "salony": salony, "par": len(pary), "uslug": len(uslugi),
             "per_branza": {g: {"par": len(z), "v13": miary(z, "v13"), "v12b": miary(z, "v12b"),
                                "bilans_tozsame": {"tylko_v13": sum(p["v13"] == "tozsame" != p["v12b"] for p in z),
                                                   "tylko_v12b": sum(p["v12b"] == "tozsame" != p["v13"] for p in z)},
                                "spojnosc_v13": f"{sum(p['v13_ten_sam_lisc'] for p in z if p['ta_sama_nazwa'])}/{sum(p['ta_sama_nazwa'] for p in z)}",
                                "powody_v13": dict(Counter(p["v13_powod"] for p in z).most_common(8))}
                            for g, z in grupy.items()}}
    (OUT / "wynik.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    (OUT / "pary.json").write_text(json.dumps(pary, ensure_ascii=False), encoding="utf-8")
    przeglad(pary, uslugi, r13, r12)
    print(json.dumps({k: v for k, v in wynik.items() if k != "salony"}, ensure_ascii=False, indent=1)[:6000])


def przeglad(pary: list[dict], uslugi: dict, r13: dict, r12: dict, na_branze: int = 8) -> None:
    """Próbka do oceny Alexa: w każdej branży pary, w których v13 i v12b różnią się co do „ta sama”, plus kilka zgodnych."""
    rng = random.Random(SEED)
    out = []

    def sciezka(r: dict | None, od: int) -> str:
        s = (r or {}).get("sciezki", [[[]]])[0][0]
        return " › ".join(x for x in s[od:] if x not in ("—", "ogolnie"))
    for br in sorted({p["branza"] for p in pary}):
        z = [p for p in pary if p["branza"] == br]
        rozne = [p for p in z if (p["v13"] == "tozsame") != (p["v12b"] == "tozsame")]
        zgodne = [p for p in z if p["v13"] == p["v12b"] == "tozsame"]
        for p in rng.sample(rozne, min(na_branze, len(rozne))) + rng.sample(zgodne, min(2, len(zgodne))):
            ua, ub = uslugi[int(p["a"])], uslugi[int(p["b"])]
            out.append({"branza": br, "a": f"{ua['nazwa']} [{ua['kategoria']}]", "b": f"{ub['nazwa']} [{ub['kategoria']}]",
                        "v13": f"{p['v13']} ({p['v13_powod']})", "v12b": f"{p['v12b']} ({p['v12b_powod']})",
                        "sciezka_a_v13": sciezka(r13.get(str(p["a"])), 1).replace("|", ": "),
                        "sciezka_b_v13": sciezka(r13.get(str(p["b"])), 1).replace("|", ": ")})
    (OUT / "przeglad.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sprawdzian drzewa v13 z bilansem względem v12b")
    p.add_argument("--uslug", type=int, default=6)
    p.add_argument("--na-branze", type=int, default=2)
    p.add_argument("--kandydatow", type=int, default=150)
    p.add_argument("--podobienstwo", type=float, default=0.6)
    p.add_argument("--budzet", type=float, default=5.0)
    p.add_argument("--drzewo", default="drzewo_v13b.json", help="plik drzewa w dane/2026-09-28/v13/")
    p.add_argument("--uslug-pomiar", type=int, default=0, help="najwyżej N usług salonu w pomiarze (0 = wszystkie zebrane)")
    p.add_argument("--kandydatow-pomiar", type=int, default=0, help="najwyżej N najbardziej podobnych kandydatów na usługę salonu")
    p.add_argument("--tylko-zbierz", action="store_true", help="tylko salony, usługi i pary (odczyt bazy, bez TypeSafe)")
    asyncio.run(main_async(p.parse_args()))
