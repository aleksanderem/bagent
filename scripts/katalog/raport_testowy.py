"""Katalog usług — TESTOWY raport konkurencji dla losowego salonu z produkcji (odczyt bazy, zapis tylko lokalnie).

Kroki (każdy wznawialny, wyniki w dane/2026-09-29/raport/<booksy_id>/):
  --wybierz    losuje salon (ziarno), pula = fn_competitors_in_radius 15 km (jak produkcja), 25 najbliższych
               z aktualnym cennikiem; zapisuje usługi z pełnym kontekstem Booksy                        0 USD
  --wyciagnij  cechy wszystkich ofert (usługa albo wariant z własną ceną) GLM z abonamentu, paczki po 12     0 USD
  --klasy      różnice jednostronne „oferta salonu × oferta konkurencji” → TypeSafe Score raz na klasę    grosze
  --licz       ceny „tej samej” usługi (≥ 3 salony) i „podobnych” (ten sam zabieg i metoda) → raport.json/md
Silnik produkcyjny nie jest dotykany; nic nie jest zapisywane w bazie.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import math
import os
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug.ekstrakcja import Oferta, oferty_z_uslugi  # noqa: E402
from services.katalog_uslug.podpis import INNA, TA_SAMA, Klasy, podpis, porownaj, roznica_do_pytania  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402


def _modul(nazwa: str, plik: Path):
    spec = importlib.util.spec_from_file_location(nazwa, plik)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


tp = _modul("test_paczek", B / "scripts" / "katalog" / "test_paczek.py")
kr = _modul("klasy_roznic", B / "scripts" / "katalog" / "klasy_roznic.py")
km = _modul("kategorie", B / "scripts" / "katalog" / "kategorie.py")
SLOWNIK_DEV = B / "scripts" / "katalog" / "dane" / "2026-09-29" / "w2" / "slownik.json"

DANE = B / "scripts" / "katalog" / "dane" / "2026-09-29" / "raport"
ZIARNO = 20260929
PROMIEN_KM = 15
KONKURENTOW = 25
MIN_USLUG, MAX_USLUG = 20, 120
SWIEZOSC_DNI = 120
OPIS_MAX = 800


def warianty(v) -> list[dict]:
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return []
    return [{"label": (w.get("label") or "").strip(), "cena_zl": w.get("price"), "min": w.get("duration")}
            for w in v or [] if isinstance(w, dict)]


def uzyte_salony() -> set[int]:
    """Salony ze wszystkich dotychczasowych zbiorów pomiarowych — podmiot ma być nowy."""
    wynik: set[int] = set()
    for plik in (B / "scripts" / "typesafe" / "dane").rglob("uslugi.json"):
        try:
            us = json.loads(plik.read_text(encoding="utf-8")).get("uslugi", {})
        except (ValueError, AttributeError):
            continue
        wynik |= {int(u["booksy_id"]) for u in us.values() if u.get("booksy_id")}
    return wynik


def odleglosc_km(a: dict, b: dict) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a["latitude"], a["longitude"], b["latitude"], b["longitude"]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def uslugi_salonu(sb, booksy_id: int, typ: str) -> tuple[dict | None, list[dict]]:
    sc = (sb.client.table("salon_scrapes").select("id,booksy_id,salon_name,scraped_at").eq("booksy_id", booksy_id)
          .order("scraped_at", desc=True).limit(1).execute().data or [])
    if not sc:
        return None, []
    s = sc[0]
    rows = [r for r in sb._load_services_for_scrape(s["id"]) if r.get("is_active", True)]
    return s, [{"booksy_id": booksy_id, "salon": s.get("salon_name") or "", "typ_salonu": typ, "id": r.get("id"),
                "kategoria": r.get("category_name") or "", "nazwa": r.get("name") or "",
                "opis": " ".join((r.get("description") or "").split())[:OPIS_MAX], "warianty": warianty(r.get("variants")),
                "zabieg_booksy": r.get("treatment_name") or "", "cena_gr": r.get("price_grosze"),
                "min": r.get("duration_minutes")} for r in rows]


def wybierz(a: argparse.Namespace) -> Path:
    from services.supabase import SupabaseService
    sb = SupabaseService()
    cli = sb.client
    kategorie = {r["id"]: r["name"] for r in cli.table("business_categories").select("id,name").execute().data or []}
    pomin = uzyte_salony()
    rng = random.Random(a.ziarno)
    granica = (datetime.now(timezone.utc) - timedelta(days=SWIEZOSC_DNI)).isoformat()
    kolejnosc = sorted(kategorie)
    rng.shuffle(kolejnosc)
    for kid in kolejnosc:
        rows = (cli.table("salons").select("booksy_id,name,city,latitude,longitude,last_scraped_at,deleted_at")
                .eq("primary_category_id", kid).order("booksy_id").limit(3000).execute().data or [])
        rows = [r for r in rows if r.get("booksy_id") and r["booksy_id"] not in pomin and not r.get("deleted_at")
                and r.get("latitude") and r.get("longitude") and (r.get("last_scraped_at") or "") >= granica]
        rng.shuffle(rows)
        for kand in rows[:15]:
            s, us = uslugi_salonu(sb, kand["booksy_id"], kategorie[kid])
            if not (MIN_USLUG <= len(us) <= MAX_USLUG):
                continue
            res = cli.rpc("fn_competitors_in_radius", {"p_subject_booksy_id": int(kand["booksy_id"]),
                                                        "p_radius_km": PROMIEN_KM}).limit(20000).execute()
            pula = [int(r if isinstance(r, int) else r.get("fn_competitors_in_radius")) for r in res.data or []]
            pula = [b for b in pula if b != kand["booksy_id"]]
            if len(pula) < KONKURENTOW:
                continue
            print(f"podmiot: {kand['name']} ({kategorie[kid]}, {kand['city']}), usług {len(us)}, pula {PROMIEN_KM} km: "
                  f"{len(pula)} salonów", flush=True)
            geo: list[dict] = []
            for i in range(0, len(pula), 200):
                geo += (cli.table("salons").select("booksy_id,name,city,latitude,longitude,primary_category_id,deleted_at")
                        .in_("booksy_id", pula[i:i + 200]).execute().data or [])
            # konkurencja = najbliższe salony tej samej branży (raport konkurencji), potem pozostałe najbliższe
            geo = sorted((g for g in geo if g.get("latitude") and not g.get("deleted_at")),
                         key=lambda g: (g.get("primary_category_id") != kid, odleglosc_km(kand, g)))
            konkurenci, uslugi = [], list(us)
            for g in geo:
                if len(konkurenci) >= KONKURENTOW:
                    break
                sg, ug = uslugi_salonu(sb, g["booksy_id"], kategorie.get(g.get("primary_category_id"), ""))
                if len(ug) < 5:
                    continue
                konkurenci.append({"booksy_id": g["booksy_id"], "nazwa": g["name"], "km": round(odleglosc_km(kand, g), 2),
                                   "typ": kategorie.get(g.get("primary_category_id"), ""), "uslug": len(ug)})
                uslugi += ug
            out = DANE / str(kand["booksy_id"])
            out.mkdir(parents=True, exist_ok=True)
            (out / "salon.json").write_text(json.dumps({"podmiot": {**kand, "typ": kategorie[kid], "uslug": len(us)},
                                                        "pula_15km": len(pula), "konkurenci": konkurenci},
                                                       ensure_ascii=False, indent=1), encoding="utf-8")
            (out / "uslugi.json").write_text(json.dumps({"uslugi": {str(u["id"]): u for u in uslugi}}, ensure_ascii=False),
                                             encoding="utf-8")
            print(f"konkurentów {len(konkurenci)} (do {konkurenci[-1]['km']} km), usług razem {len(uslugi)} → {out}")
            return out
    sys.exit("nie znaleziono salonu spełniającego warunki")


def katalog(a: argparse.Namespace) -> Path:
    if a.salon:
        return DANE / str(a.salon)
    kat = sorted(DANE.iterdir()) if DANE.exists() else []
    if not kat:
        sys.exit("najpierw --wybierz")
    return kat[-1]


def dane(out: Path) -> tuple[dict, dict[int, dict], list[Oferta]]:
    salon = json.loads((out / "salon.json").read_text(encoding="utf-8"))
    us = {int(k): v for k, v in json.loads((out / "uslugi.json").read_text(encoding="utf-8"))["uslugi"].items()}
    oferty = sorted((o for u in us.values() for o in oferty_z_uslugi(u)), key=lambda o: o.id)
    return salon, us, oferty


def _usluga(o: Oferta) -> int:
    return int(o.id.split("#")[0])


async def wyciagnij(out: Path, rownolegle: int) -> None:
    from taxonomy_backfill import KlientGLM
    _s, _us, oferty = dane(out)
    klucz = (os.environ.get("ZAI_API_KEY") or (tp.KLUCZ.read_text(encoding="utf-8") if tp.KLUCZ.exists() else "")).strip()
    if not klucz:
        sys.exit(f"Brak klucza Z.ai ({tp.KLUCZ})")
    tp.OUT = out
    print(f"ofert {len(oferty)}, paczek {len(tp.paczki(oferty, 'p12'))}", flush=True)
    await tp.przebieg(KlientGLM(klucz, temperature=0.0), "p12", oferty, rownolegle)
    _r, st = tp.rekordy("p12", oferty)
    print("wyciąganie: " + ", ".join(f"{k}: {v}" for k, v in st.items()))


USTAWIENIA = {"slownik": None, "kategorie": False}


def podpisy(out: Path) -> tuple[dict, dict[int, dict], list[Oferta], dict[str, dict], dict]:
    salon, us, oferty = dane(out)
    tp.OUT = out
    rek, _ = tp.rekordy("p12", oferty)
    kon = km.kontekst(oferty, out / "kategorie.json") if USTAWIENIA["kategorie"] else {}
    return salon, us, oferty, rek, {o.id: podpis(rek[o.id], USTAWIENIA["slownik"], kon.get(o.id)) for o in oferty if o.id in rek}


def klasy_raportu(out: Path, budzet: float) -> None:
    salon, us, oferty, rek, pod = podpisy(out)
    bid = salon["podmiot"]["booksy_id"]
    wlasne = [o for o in oferty if us[_usluga(o)]["booksy_id"] == bid and o.id in pod]
    obce = [o for o in oferty if us[_usluga(o)]["booksy_id"] != bid and o.id in pod]
    klasy: dict[str, dict] = {}
    for s in wlasne:
        for c in obce:
            for klasa, strona in roznica_do_pytania(pod[s.id], pod[c.id]):
                k = json.dumps(klasa, ensure_ascii=False)
                if k in klasy:
                    klasy[k]["par"] += 1
                    continue
                o_z, o_bez = (s, c) if strona is pod[s.id] else (c, s)
                u = us[_usluga(o_z)]
                klasy[k] = {"klasa": klasa, "par": 1, "oferta": o_z.id,
                            "dopisek": kr._dopisek(rek[o_z.id], klasa[1], set(klasa[2].split())),
                            "zabieg": (rek[o_z.id].get("zabieg") or {}).get("fraza") or "", "druga": o_bez.nazwa,
                            "stan": stan_v12({**u, "warianty": [{"label": o_z.wariant}] if o_z.wariant else []})}
    koszt = asyncio.run(kr.zapytaj(klasy, budzet, out / f"klasy_p{kr.WERSJA_PYTANIA}.json"))
    print(f"klas {len(klasy)}, koszt {koszt:.4f} USD")


def minuty(o: Oferta, us: dict[int, dict]) -> float | None:
    """Czas oferty: wariant z etykietą → jego czas; pojedyncza oferta → pierwszy wariant albo czas usługi."""
    u = us[_usluga(o)]
    war = [w for w in u.get("warianty") or [] if (w.get("label") or "").strip()]
    if "#" in o.id:
        m = war[int(o.id.split("#")[1])].get("min")
    else:
        m = (u.get("warianty") or [{}])[0].get("min") or u.get("min")
    return float(m) if m else None


def _statystyki(ceny: list[float]) -> dict:
    ceny = sorted(ceny)
    q = statistics.quantiles(ceny, n=4, method="inclusive") if len(ceny) >= 2 else [ceny[0]] * 3
    return {"n": len(ceny), "mediana": round(statistics.median(ceny)), "p25": round(q[0]), "p75": round(q[2])}


def licz(out: Path) -> None:
    from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij
    salon, us, oferty, rek, pod = podpisy(out)
    plik_klas = out / f"klasy_p{kr.WERSJA_PYTANIA}.json"
    rozstrz = json.loads(plik_klas.read_text(encoding="utf-8")) if plik_klas.exists() else {}
    klasy = Klasy(opisowe={tuple(v["klasa"]) for v in rozstrz.values() if rozstrzygnij(v.get("score")) == NIE_ZMIENIA})
    bid = salon["podmiot"]["booksy_id"]
    nazwy = {k["booksy_id"]: k["nazwa"] for k in salon["konkurenci"]}
    wlasne = [o for o in oferty if us[_usluga(o)]["booksy_id"] == bid]
    obce = [o for o in oferty if us[_usluga(o)]["booksy_id"] != bid and o.id in pod]
    wiersze = []
    for s in wlasne:
        w = {"id": s.id, "kategoria": s.kategoria, "nazwa": s.nazwa, "wariant": s.wariant, "cena": s.cena_zl,
             "pozycja": (rek.get(s.id) or {}).get("pozycja"), "ta_sama": [], "podobne": []}
        if s.id in pod:
            ps = pod[s.id]
            rdzen_s = {x for p, x in ps.poziomy if p == "rdzen"}
            for c in obce:
                werdykt, powod = porownaj(ps, pod[c.id], klasy)
                cena = c.cena_zl if c.cena_zl else None
                if not cena:
                    continue
                mc = minuty(c, us)
                wpis = {"salon": nazwy.get(us[_usluga(c)]["booksy_id"], ""), "booksy_id": us[_usluga(c)]["booksy_id"],
                        "id_oferty": c.id, "nazwa": c.nazwa, "wariant": c.wariant, "cena": cena, "min": mc,
                        "zl_min": round(cena / mc, 3) if mc else None, "powod": powod}
                if werdykt == TA_SAMA:
                    w["ta_sama"].append(wpis)
                elif werdykt != INNA and rdzen_s and rdzen_s == {x for p, x in pod[c.id].poziomy if p == "rdzen"}:
                    w["podobne"].append(wpis)
        w["min"] = minuty(s, us)
        for klucz in ("ta_sama", "podobne"):
            per_salon, per_salon_min = defaultdict(list), defaultdict(list)
            for m in w[klucz]:
                per_salon[m["booksy_id"]].append(m["cena"])
                if m["zl_min"]:
                    per_salon_min[m["booksy_id"]].append(m["zl_min"])
            ceny = [statistics.median(v) for v in per_salon.values()]
            st = _statystyki(ceny) if ceny else {"n": 0}
            # jak produkcja (layer_unit.normalize_unit): mediana zł/min × czas oferty salonu, inaczej mediana surowa
            if ceny and w["min"] and per_salon_min:
                zm = sorted(statistics.median(v) * w["min"] for v in per_salon_min.values())
                st["rynkowa"] = round(statistics.median(zm))
                st["rynkowa_z"] = "zł/min × czas"
                q = statistics.quantiles(zm, n=4, method="inclusive") if len(zm) >= 2 else [zm[0]] * 3
                st["p25"], st["p75"] = round(q[0]), round(q[2])  # przedział w tym samym przeliczeniu co cena rynkowa
            elif ceny:
                st["rynkowa"], st["rynkowa_z"] = st["mediana"], "mediana surowa"
            w[f"{klucz}_stat"] = st
        wiersze.append(w)
    (out / "raport.json").write_text(json.dumps({"salon": salon, "wiersze": wiersze}, ensure_ascii=False, indent=1),
                                     encoding="utf-8")
    z_cena = [w for w in wiersze if w["ta_sama_stat"]["n"] >= 3]
    z_podobnymi = [w for w in wiersze if w["ta_sama_stat"]["n"] < 3 and w["podobne_stat"]["n"] >= 3]
    print(f"podmiot {salon['podmiot']['name']} ({salon['podmiot']['typ']}, {salon['podmiot']['city']}); ofert "
          f"{len(wlasne)}; z ceną „ta sama” (≥ 3 salony) {len(z_cena)}; tylko „podobne” {len(z_podobnymi)}; "
          f"bez porównania {len(wiersze) - len(z_cena) - len(z_podobnymi)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    for k in ("wybierz", "wyciagnij", "klasy", "licz", "kategorie", "slownik"):
        ap.add_argument(f"--{k}", action="store_true")
    ap.add_argument("--salon", type=int, default=0)
    ap.add_argument("--ziarno", type=int, default=ZIARNO)
    ap.add_argument("--rownolegle", type=int, default=3)
    ap.add_argument("--budzet", type=float, default=0.2)
    a = ap.parse_args()
    out = wybierz(a) if a.wybierz else katalog(a)
    if a.wyciagnij:
        asyncio.run(wyciagnij(out, a.rownolegle))
    if a.slownik:
        USTAWIENIA["slownik"] = json.loads(SLOWNIK_DEV.read_text(encoding="utf-8"))
    if a.kategorie:
        _s, _us, oferty = dane(out)
        asyncio.run(km.wyciagnij(km.kategorie_ofert(oferty), out / "kategorie.json", a.rownolegle))
        USTAWIENIA["kategorie"] = True
    if a.klasy:
        klasy_raportu(out, a.budzet)
    if a.licz:
        licz(out)


if __name__ == "__main__":
    main()
