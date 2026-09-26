"""Pomiar trafności matchingu: ile próbek w cenach rynkowych to TA SAMA usługa.

Dla wylosowanych salonów (po 2 z 9 branż, różne miasta) uruchamia prawdziwy
silnik wyceny (report_pricing.compute_pricing_comparisons_v2, rynek z promienia
jak w raporcie) BEZ zapisu wyników i bez pomostu GLM (pomost pisze do
service_taxonomy). Każdą próbkę, z której liczy się cena, ocenia sędzia
TypeSafe (services/typesafe_ocena): ta sama / powiązana / inna. Oceny zapisują
się w matching_pair_verdict (mig 200) — kolejny pomiar płaci tylko za nowe pary.

Wynik per branża i łącznie:
  * pary: udział tożsamych / powiązanych / różnych w próbkach ceny,
  * wiersze: ile ma większość tożsamych, ile ma choć jedną różną,
  * cena: przesunięcie ceny rynkowej, gdy liczyć ją TYLKO z tożsamych
    (tym samym kodem co silnik — layer_unit.normalize_unit).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/ocena_trafnosci.py --out <katalog> --budzet 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics as st
import sys
import time
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
for _line in (BAGENT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    _k, _, _v = _line.partition("=")
    if _k.strip() in ("QDRANT_URL", "QDRANT_API_KEY") and _v.strip():
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

from services.similarity_pricing import report_pricing  # noqa: E402
from services.similarity_pricing.layer_unit import normalize_unit  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import MODEL, para_klucz, strona  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

SEED = 20260925
BRANZE = {
    "Fryzjer": 6, "Paznokcie": 10, "Medycyna Estetyczna": 11, "Masaż": 4, "Depilacja": 190,
    "Podologia": 189, "Salon Kosmetyczny": 7, "Barber shop": 16, "Brwi i rzęsy": 188,
}
SALONOW_NA_BRANZE = 2
MIN_USLUG = 15
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"
MIN_SALONOW_CENY = 3  # ten sam próg co silnik (min_salons_thin)


async def _bez_pomostu(*_a: Any, **_k: Any) -> dict:
    return {}  # pomost GLM woła model i ZAPISUJE — pomiar ma być tylko odczytem


report_pricing._bridge_distill_missing = _bez_pomostu


def _nazwy_branz(cli: Any) -> dict[int, str]:
    return {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}


def _typy_salonow(cli: Any, booksy_ids: set[int], branze: dict[int, str]) -> dict[int, str]:
    out: dict[int, str] = {}
    ids = sorted(i for i in booksy_ids if i)
    for i in range(0, len(ids), 200):
        rows = cli.table("salons").select("booksy_id,primary_category_id").in_("booksy_id", ids[i : i + 200]).execute().data or []
        out.update({r["booksy_id"]: branze.get(r.get("primary_category_id"), "") for r in rows})
    return out


def _kategorie_uslug(cli: Any, service_ids: set[int]) -> dict[int, str]:
    out: dict[int, str] = {}
    ids = sorted(i for i in service_ids if i)
    for i in range(0, len(ids), 200):
        rows = cli.table("salon_scrape_services").select("id,category_name").in_("id", ids[i : i + 200]).execute().data or []
        out.update({r["id"]: r.get("category_name") or "" for r in rows})
    return out


async def losuj_salony(sb: SupabaseService, rng: random.Random) -> list[tuple[str, int, str, dict[str, Any]]]:
    """(branża, booksy_id, miasto, dane cennika) — różne miasta w całej próbce."""
    cli = sb.client
    wybrane: list[tuple[str, int, str, dict[str, Any]]] = []
    miasta: set[str] = set()
    for branza, cat in BRANZE.items():
        kandydaci = (
            cli.table("salons").select("booksy_id,city").eq("primary_category_id", cat)
            .not_.is_("city", "null").order("booksy_id").limit(3000).execute().data or []
        )
        rng.shuffle(kandydaci)
        n = 0
        for s in kandydaci:
            if n >= SALONOW_NA_BRANZE:
                break
            bid, city = s.get("booksy_id"), s.get("city")
            if not bid or city in miasta:
                continue
            dane = (await sb.get_competitor_full_data([bid])).get(bid) or {}
            aktywne = [x for x in dane.get("services") or [] if x.get("is_active", True) and x.get("price_grosze")]
            if len(aktywne) < MIN_USLUG:
                continue
            wybrane.append((branza, bid, city, {**dane, "booksy_id": bid}))
            miasta.add(city)
            n += 1
    return wybrane


def _cena(row: dict[str, Any], probki: list[dict[str, Any]]) -> int | None:
    if len({p.get("booksy_id") for p in probki}) < MIN_SALONOW_CENY:
        return None
    subj = {"price_grosze": row.get("subject_price_grosze"), "duration_minutes": row.get("subject_duration_minutes")}
    stats, _ = normalize_unit(subj, probki)
    return stats["market_price_grosze"]


def ocen_wiersz(row: dict[str, Any], strona_a: dict[str, str], strony_b: list[dict[str, str]],
                oceny: dict[str, dict[str, Any]]) -> dict[str, Any]:
    probki = row.get("competitor_samples") or []
    werdykty = [(oceny.get(para_klucz(strona_a, b)) or {}).get("werdykt") for b in strony_b]
    tozsame = [p for p, w in zip(probki, werdykty) if w == "tozsame"]
    bez_roznych = [p for p, w in zip(probki, werdykty) if w in ("tozsame", "powiazane")]
    pokazana = row.get("market_median_grosze")
    c_t, c_bz = _cena(row, tozsame), _cena(row, bez_roznych)
    return {
        "usluga": row.get("treatment_name"),
        "status": row.get("verification_status"),
        "probek": len(probki),
        "ocenionych": sum(1 for w in werdykty if w),
        "tozsame": werdykty.count("tozsame"),
        "powiazane": werdykty.count("powiazane"),
        "rozne": werdykty.count("rozne"),
        "cena_pokazana": pokazana,
        "cena_z_tozsamych": c_t,
        "cena_bez_roznych": c_bz,
        "przesuniecie_tozsame_proc": round((pokazana - c_t) / c_t * 100, 1) if pokazana and c_t else None,
        "przesuniecie_bez_roznych_proc": round((pokazana - c_bz) / c_bz * 100, 1) if pokazana and c_bz else None,
        "cena_podmiotu": row.get("subject_price_grosze"),
        "czas_podmiotu": row.get("subject_duration_minutes"),
        "probki": [
            {"nazwa": b["nazwa"], "kategoria": b["kategoria_w_cenniku"], "werdykt": w, "cena": p.get("price_grosze"),
             "czas": p.get("duration_minutes"), "service_id": p.get("service_id"), "booksy_id": p.get("booksy_id"),
             "pakiet": p.get("is_package")}
            for p, b, w in zip(probki, strony_b, werdykty)
        ],
    }


def podsumuj(wiersze: list[dict[str, Any]]) -> dict[str, Any]:
    t = sum(w["tozsame"] for w in wiersze)
    p = sum(w["powiazane"] for w in wiersze)
    r = sum(w["rozne"] for w in wiersze)
    ocen = max(t + p + r, 1)
    przes = [abs(w["przesuniecie_tozsame_proc"]) for w in wiersze if w["przesuniecie_tozsame_proc"] is not None]
    przes_bz = [abs(w["przesuniecie_bez_roznych_proc"]) for w in wiersze if w["przesuniecie_bez_roznych_proc"] is not None]
    n = max(len(wiersze), 1)
    return {
        "wierszy_z_cena": len(wiersze),
        "par_ocenionych": t + p + r,
        "pary_tozsame_proc": round(t / ocen * 100, 1),
        "pary_powiazane_proc": round(p / ocen * 100, 1),
        "pary_rozne_proc": round(r / ocen * 100, 1),
        "wiersze_wiekszosc_tozsamych_proc": round(sum(1 for w in wiersze if w["ocenionych"] and w["tozsame"] * 2 > w["ocenionych"]) / n * 100, 1),
        "wiersze_z_choc_jedna_rozna_proc": round(sum(1 for w in wiersze if w["rozne"]) / n * 100, 1),
        "wiersze_bez_ceny_z_samych_tozsamych": sum(1 for w in wiersze if w["cena_z_tozsamych"] is None),
        "przesuniecie_mediana_proc": round(st.median(przes), 1) if przes else None,
        "przesuniecie_ponad_10proc": sum(1 for x in przes if x > 10),
        "przesuniecie_ponad_25proc": sum(1 for x in przes if x > 25),
        "wierszy_z_cena_z_tozsamych": len(przes),
        "bez_roznych_przesuniecie_mediana_proc": round(st.median(przes_bz), 1) if przes_bz else None,
        "bez_roznych_ponad_10proc": sum(1 for x in przes_bz if x > 10),
    }


async def main_async(args: argparse.Namespace) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    sb = SupabaseService()
    cli = sb.client
    branze = _nazwy_branz(cli)
    salony = await losuj_salony(sb, random.Random(SEED))
    # bez order() baza zwracała kandydatów w innej kolejności przy każdym przebiegu,
    # więc to samo ziarno dawało inne salony (25.09: dwa przebiegi, dwie próbki)
    (out / "salony.json").write_text(json.dumps([[b, i, m] for b, i, m, _ in salony], ensure_ascii=False), encoding="utf-8")
    print(f"wylosowano {len(salony)} salonów: " + ", ".join(f"{b}/{m}" for b, _, m, _ in salony), flush=True)
    api_key = (os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")).strip()
    wyniki: dict[str, list[dict[str, Any]]] = {}
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:
        sesja = OcenaPar(None if args.bez_zapisu else cli, client, budzet_usd=args.budzet)
        for branza, bid, miasto, dane in salony:
            t0 = time.monotonic()
            try:
                rows = await report_pricing.compute_pricing_comparisons_v2(sb, 0, dane, [])
            except Exception as e:  # noqa: BLE001 — jeden salon nie zatrzymuje pomiaru
                print(f"BŁĄD wyceny {branza} {bid}: {type(e).__name__}: {str(e)[:160]}", flush=True)
                continue
            rows = [r for r in rows if r.get("market_median_grosze") and r.get("competitor_samples")]
            kat_podmiotu = {" ".join((s.get("name") or "").lower().split()): s.get("category_name") for s in dane.get("services") or []}
            probki = [p for r in rows for p in r["competitor_samples"]]
            typy = _typy_salonow(cli, {p.get("booksy_id") for p in probki} | {bid}, branze)
            kategorie = _kategorie_uslug(cli, {p.get("service_id") for p in probki})
            zestawy = []
            for r in rows:
                a = strona(r["treatment_name"], kat_podmiotu.get(" ".join(r["treatment_name"].lower().split())), typy.get(bid))
                bs = [strona(p.get("service_name"), kategorie.get(p.get("service_id")), typy.get(p.get("booksy_id"))) for p in r["competitor_samples"]]
                zestawy.append((r, a, bs))
            oceny = await sesja.ocen([(a, b) for _, a, bs in zestawy for b in bs])
            wiersze = [{"salon": bid, "miasto": miasto, **ocen_wiersz(r, a, bs, oceny)} for r, a, bs in zestawy]
            wyniki.setdefault(branza, []).extend(wiersze)
            print(f"{branza:<20} {bid:>7} {miasto:<18} wierszy z ceną {len(wiersze):>3}  "
                  f"{json.dumps(podsumuj(wiersze), ensure_ascii=False)}  "
                  f"[{time.monotonic() - t0:.0f} s, łącznie {sesja.koszt_usd:.3f} USD, z pamięci {sesja.z_pamieci}, nowych {sesja.nowych}]",
                  flush=True)
    raport = {
        "lacznie": podsumuj([w for ws in wyniki.values() for w in ws]),
        "per_branza": {b: podsumuj(ws) for b, ws in wyniki.items()},
        "salonow": len({w["salon"] for ws in wyniki.values() for w in ws}),
        "koszt_usd": round(sesja.koszt_usd, 4),
        "par_z_pamieci": sesja.z_pamieci, "par_nowych": sesja.nowych, "par_pominietych": sesja.pominietych,
    }
    (out / "ocena_trafnosci.json").write_text(json.dumps(raport, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "ocena_trafnosci_wiersze.json").write_text(json.dumps(wyniki, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(raport, ensure_ascii=False, indent=1))


def main() -> None:
    p = argparse.ArgumentParser(description="Pomiar trafności matchingu sędzią TypeSafe")
    p.add_argument("--out", required=True)
    p.add_argument("--budzet", type=float, default=3.0)
    p.add_argument("--bez-zapisu", action="store_true", help="nie zapisuj ocen do matching_pair_verdict")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()
