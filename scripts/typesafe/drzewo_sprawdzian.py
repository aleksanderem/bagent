"""Sprawdzian drzewa: werdykt pary z klasyfikacji w krokach vs oceny człowieka.

Każda nazwa z holdoutu (mig 188) jest klasyfikowana po drzewie
(services/typesafe_drzewo), osie wariantu biorą się z zapisanych profili
(service_profile), werdykt pary liczy kod (porownanie.py). Obok: sędzia par v2
na tych samych parach. Klasyfikacje zapisują się lokalnie (plik pamięci),
więc powtórny przebieg nic nie kosztuje.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/drzewo_sprawdzian.py --out <katalog> --sedzia <kalibracja_pary.json>
"""

from __future__ import annotations

import argparse
import hashlib
import asyncio
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
if not os.environ.get("TYPESAFE_API_KEY") and KEY_FILE.exists():
    os.environ["TYPESAFE_API_KEY"] = KEY_FILE.read_text(encoding="utf-8").strip()

from kalibracja_sedziego import KLASY, zgodnosc  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.drzewo import WERSJA  # noqa: E402
from services.typesafe_drzewo.klasyfikacja import KLASYFIKACJA_WERSJA, MODEL, klasyfikuj_wiele  # noqa: E402
from services.typesafe_drzewo.porownanie import werdykt  # noqa: E402
from services.typesafe_profile.destylacja import ProfileSession, nk  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402
from warianty_sedziego import binarnie  # noqa: E402


def klucz(n: str, kat: str | None, typ: str | None, opis: str | None = None, zb: str | None = None) -> str:
    h = hashlib.sha1(f"{nk(opis)}|{nk(zb)}".encode()).hexdigest()[:10]
    return f"{nk(n)}|{nk(kat)}|{nk(typ)}|{h}"


async def cenniki(sb: SupabaseService, bids: list[int]) -> dict[tuple[int, str], dict]:
    """(booksy_id, klucz nazwy) → usługa z aktualnego cennika (opis, cena, czas, pakiet)."""
    out: dict[tuple[int, str], dict] = {}
    for i in range(0, len(bids), 40):
        dane = await sb.get_competitor_full_data(bids[i : i + 40])
        for bid, c in dane.items():
            for s in c.get("services") or []:
                out.setdefault((bid, nk(s.get("name"))), s)
    return out


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    rows = cli.table("matching_holdout").select("*").order("id").execute().data or []
    branze = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    bids = sorted({r["cand_booksy_id"] for r in rows if r.get("cand_booksy_id")})
    typ: dict[int, str] = {}
    for i in range(0, len(bids), 200):
        for s in cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", bids[i : i + 200]).execute().data or []:
            typ[s["booksy_id"]] = branze.get(s.get("primary_category_id"), "")
    cennik = await cenniki(sb, sorted({r["subject_booksy_id"] for r in rows} | set(bids)))
    pary = []
    uslugi: dict[str, tuple] = {}
    fakty: dict[str, dict] = {}
    for r in rows:
        strony = []
        for bid, n, kat, typ_s, cena in (
            (r["subject_booksy_id"], r["subject_name"], r.get("subject_category"), r["branza"], r.get("subject_price_grosze")),
            (r.get("cand_booksy_id"), r["cand_name"], r.get("cand_category"), typ.get(r.get("cand_booksy_id"), ""), r.get("cand_price_grosze")),
        ):
            sv = cennik.get((bid, nk(n))) or {}
            t = (n, kat, typ_s, sv.get("description"), sv.get("treatment_name"))
            k = klucz(*t)
            uslugi[k] = t
            fakty[k] = {"nazwa": n, "opis": sv.get("description"), "price_grosze": cena or sv.get("price_grosze"),
                        "duration_minutes": sv.get("duration_minutes"), "is_package": sv.get("is_package")}
            strony.append(t)
        pary.append((r, strony[0], strony[1]))
    print(f"usług z cennikiem {sum(1 for f in fakty.values() if f['duration_minutes'])}/{len(fakty)}, z opisem {sum(1 for f in fakty.values() if (f['opis'] or '').strip())}", flush=True)

    pamiec_plik = out / f"klasyfikacje_d{WERSJA}_k{KLASYFIKACJA_WERSJA}.json"
    pamiec = json.loads(pamiec_plik.read_text(encoding="utf-8")) if pamiec_plik.exists() else {}
    brak = {k: v for k, v in uslugi.items() if k not in pamiec}
    api_key = os.environ["TYPESAFE_API_KEY"]
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        nowe, tokeny = await klasyfikuj_wiele(client, brak)
    pamiec.update(nowe)
    pamiec_plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
    print(f"usług {len(uslugi)}, nowych klasyfikacji {len(nowe)}, koszt {tokeny * 0.042 / 1e6:.4f} USD", flush=True)

    # osie wariantu: zapisane profile (klucz: branża podmiotu, jak w wycenie)
    per_branza: dict[str, dict[str, str | None]] = defaultdict(dict)
    for r, a, b in pary:
        for n, k, *_ in (a, b):
            per_branza[r["branza"]].setdefault(nk(n), k)
    profile: dict[tuple[str, str], dict] = {}
    for br, meta in per_branza.items():
        s = ProfileSession(sb)
        got = await s.profiles_for(br, [{"name": n, "category_name": k} for n, k in meta.items()], {})
        profile.update({(br, n): p for n, p in got.items()})

    sedzia = {w["id"]: w["sedzia"] for w in json.loads(Path(args.sedzia).read_text(encoding="utf-8"))}
    wyniki = []
    for r, a, b in pary:
        ka, kb = pamiec.get(klucz(*a)), pamiec.get(klucz(*b))
        w, powod = werdykt(ka, kb, profile.get((r["branza"], nk(a[0]))), profile.get((r["branza"], nk(b[0]))),
                           fakty.get(klucz(*a)), fakty.get(klucz(*b)))
        wyniki.append({"id": r["id"], "branza": r["branza"], "a": a[0], "b": b[0], "human": r.get("label_human"),
                       "final": r.get("label_final"), "drzewo": w, "powod": powod, "sedzia_v2": sedzia.get(r["id"]),
                       "ra": (ka or {}).get("rodzaje"), "rb": (kb or {}).get("rodzaje")})

    def metryki(pole: str) -> dict:
        h = [(w["human"], w[pole]) for w in wyniki if w[pole] and w["human"] in KLASY]
        f = [(w["final"], w[pole]) for w in wyniki if w[pole] and w["final"] in KLASY]
        return {"czlowiek_tak_nie": binarnie(h), "czlowiek_3": zgodnosc(h)["zgodnosc_proc"], "czlowiek_par": len(h),
                "modele_tak_nie": binarnie(f), "modele_3": zgodnosc(f)["zgodnosc_proc"], "modele_par": len(f)}

    bez_rodzaju = sum(1 for k in uslugi if not (pamiec.get(k) or {}).get("rodzaje"))
    raport = {
        "drzewo": metryki("drzewo"),
        "sedzia_v2": metryki("sedzia_v2"),
        # sprawdzian czysty: pary o nieparzystym id (diagnoza błędów tylko na parzystych)
        "nieparzyste_drzewo_modele": binarnie([(w["final"], w["drzewo"]) for w in wyniki if w["id"] % 2 and w["drzewo"] and w["final"] in KLASY]),
        "nieparzyste_sedzia_modele": binarnie([(w["final"], w["sedzia_v2"]) for w in wyniki if w["id"] % 2 and w["sedzia_v2"] and w["final"] in KLASY]),
        "pary_bez_werdyktu_drzewa": sum(1 for w in wyniki if not w["drzewo"]),
        "uslugi_bez_rodzaju_proc": round(bez_rodzaju / max(len(uslugi), 1) * 100, 1),
        "powody": dict(Counter(w["powod"].split(":")[0] for w in wyniki)),
        "drzewo_per_branza_modele_tak_nie": {
            br: binarnie([(w["final"], w["drzewo"]) for w in wyniki if w["branza"] == br and w["drzewo"] and w["final"] in KLASY])["trafnosc_tak_nie_proc"]
            for br in sorted({w["branza"] for w in wyniki})},
        "sedzia_per_branza_modele_tak_nie": {
            br: binarnie([(w["final"], w["sedzia_v2"]) for w in wyniki if w["branza"] == br and w["sedzia_v2"] and w["final"] in KLASY])["trafnosc_tak_nie_proc"]
            for br in sorted({w["branza"] for w in wyniki})},
    }
    (out / "drzewo_raport.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "drzewo_pary.json").write_text(json.dumps(wyniki, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Sprawdzian drzewa zabiegów na holdoucie")
    p.add_argument("--out", required=True)
    p.add_argument("--sedzia", required=True)
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
