"""Katalog usług, etap 3 — suchy przebieg: wycena podpisem obok starego silnika na salonach ze sprawdzianów 7 i 8.

Gotowych raportów z wyceną jest na produkcji 5 (3 salony), więc porównujemy na 36 salonach sprawdzianów (9 branż,
3 usługi na salon). Te same usługi podmiotu, ta sama pula (15 km):
  stary — `compute_pricing_comparisons_v2` lokalnie, tylko odczyt: pomost destylacji i profile TypeSafe wyłączone
          (bez zapisu do service_taxonomy, bez płatnych pytań; weto na osiach GLM jak dziś na produkcji),
  podpis — kandydaci z wektorów szeroko (0,6, do 120 — jak w sprawdzianie), werdykty podpisu, cena
          `katalog_uslug.dopasowanie.wycen` (dedup per salon, ≥ 3 salony, zł/min × czas — warstwy produkcji).
Porównanie na ofercie reprezentującej usługę (pierwszy wariant — tę cenę pokazuje stary silnik); pozostałe warianty
podpis wycenia osobno (decyzja Alexa: każdy wariant z własną ceną to osobna pozycja).

  python scripts/katalog/porownanie_silnikow.py --wyjscie sprawdzian8 --stary      # odczyt prod, 0 USD
  python scripts/katalog/porownanie_silnikow.py --wyjscie sprawdzian8 --porownaj   # 0 USD
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
import sprawdzian7 as s7  # noqa: E402
from services.katalog_uslug import klasy as _klasy  # noqa: E402
from services.katalog_uslug.wycena import wycen_oferty  # noqa: E402
from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij, zamiana_rownowazna  # noqa: E402
from services.katalog_uslug.podpis import Klasy  # noqa: E402

POLA = ("id,booksy_id,category_name,name,description,variants,treatment_name,treatment_parent_id,booksy_treatment_id,"
        "is_package,is_active,price_grosze,duration_minutes")


def _ustaw(wyjscie: str) -> None:
    s7.OUT = s7.OUT.parent / wyjscie
    s7.V14 = B / "scripts" / "typesafe" / "dane" / "2026-09-28" / f"v14_{wyjscie}" / "pary.json"


async def _stary() -> None:
    """Wiersz starego silnika dla każdej usługi podmiotu — wywołanie na jedną usługę (drugi przebieg adaptacyjny
    i tak nie rusza: próg zapasowy = próg główny 0,68), więc wiersz jednoznacznie należy do usługi."""
    from dotenv import load_dotenv
    load_dotenv(B / ".env")  # adres i klucz bazy wektorów (atlas) — klient Qdrant czyta je z os.environ
    from services.similarity_pricing import report_pricing as rp
    from services.supabase import SupabaseService
    rp._bridge_glm_client = lambda: None  # bez zapisu do service_taxonomy
    rp._profile_session = lambda _s: None  # bez płatnych pytań TypeSafe
    sb = SupabaseService()
    us, pary, _salony = s7.dane()
    podmiot = sorted({(q["salon"], q["a"]) for q in pary})
    plik = s7.OUT / "stary_silnik.json"
    wynik = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    for bid, sid in podmiot:
        if str(sid) in wynik:
            continue
        r = sb.client.table("salon_scrape_services").select(POLA).eq("id", sid).execute().data or []
        if not r:
            wynik[str(sid)] = {"blad": "brak usługi w bazie"}
            continue
        try:
            wiersze = await rp.compute_pricing_comparisons_v2(sb, 0, {"booksy_id": bid, "services": r}, [], radius_km=15)
        except Exception as e:  # noqa: BLE001 — błąd jednej usługi nie przerywa porównania, zostaje w pliku
            wynik[str(sid)] = {"blad": f"{type(e).__name__}: {str(e)[:160]}"}
            continue
        w = wiersze[0] if wiersze else {}
        wynik[str(sid)] = {k: w.get(k) for k in ("market_median_grosze", "sample_size", "verification_status",
                                                 "subject_price_grosze", "subject_duration_minutes")}
        wynik[str(sid)]["probki"] = [x.get("service_id") for x in w.get("competitor_samples") or []]
        plik.write_text(json.dumps(wynik, ensure_ascii=False, indent=0), encoding="utf-8")
        print(f"  {sid}: {w.get('verification_status')} {w.get('sample_size')} salonów "
              f"{(w.get('market_median_grosze') or 0) / 100:.0f} zł", flush=True)
    plik.write_text(json.dumps(wynik, ensure_ascii=False, indent=0), encoding="utf-8")


def _probka(o, u: dict, sim: float) -> dict:
    """Oferta konkurenta w kształcie bliźniaka starego silnika (cena i czas wariantu, gdy oferta jest wariantem)."""
    war = [w for w in u.get("warianty") or [] if (w.get("label") or "").strip()]
    minuty = u.get("min")
    if "#" in o.id and len(war) >= 2:
        minuty = war[int(o.id.split("#")[1])].get("min") or minuty
    return {"service_id": int(o.id.split("#")[0]), "oferta": o.id, "booksy_id": u.get("booksy_id"),
            "service_name": f"{o.nazwa} {o.wariant}".strip(), "price_grosze": round((o.cena_zl or 0) * 100),
            "duration_minutes": minuty, "similarity": sim, "is_package": bool(u.get("pakiet"))}


def _podpis_wiersze() -> dict[str, dict]:
    """Oferta podmiotu → wynik wyceny podpisem (ta sama / podobne)."""
    pary, oferty, _rek, x = s7.podpisy()
    pod = x["pod"]
    roz = json.loads(s7.kr.PLIK.read_text(encoding="utf-8"))
    zam = json.loads(s7.kr.PLIK_ZAMIAN.read_text(encoding="utf-8")) if s7.kr.PLIK_ZAMIAN.exists() else {}
    klasy = Klasy(opisowe={tuple(v["klasa"]) for v in roz.values() if _klasy.klasa_nieistotna(v)},
                  rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
    us, _p, _s = s7.dane()
    kand: dict[str, list] = defaultdict(list)
    for q in pary:
        if q["bez_wspolnych"] or q["a"] not in pod or q["b"] not in pod:
            continue
        ob = oferty[q["b"]]
        kand[q["a"]].append((_probka(ob, us[int(q["b"].split("#")[0])], q["sim"]), pod[q["b"]]))
    podmiot = {oa: (_probka(oferty[oa], us[int(oa.split("#")[0])], 1.0), pod[oa]) for oa in kand}
    # ten sam kod co ścieżka raportu za przełącznikiem (etap 3) — suchy przebieg sprawdza dokładnie ją
    return {oa: {"cena": w.wynik.market_price_grosze, "salonow": w.wynik.n_unique_salons, "status": w.wynik.status,
                 "podobne_cena": w.podobne.market_price_grosze, "podobne_salonow": w.podobne.n_unique_salons,
                 "ta_sama": [s["oferta"] for s in w.wynik.samples], "rodzaj": w.rodzaj}
            for oa, w in wycen_oferty(podmiot, kand, klasy).items()}


def _porownaj() -> None:
    stary = json.loads((s7.OUT / "stary_silnik.json").read_text(encoding="utf-8"))
    nowy = _podpis_wiersze()
    us, pary, _s = s7.dane()
    branza = {q["a"]: q["branza"] for q in pary}
    oferty = {o.id: o for u in us.values() for o in s7.oferty_z_uslugi(u)}
    po_br: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for sid, st in stary.items():
        if "blad" in st:
            continue
        oa = s7._pierwsza(int(sid), oferty)
        nw = nowy.get(oa) or {}
        wiersz = {"stary": st.get("market_median_grosze"), "podpis": nw.get("cena"), "podobne": nw.get("podobne_cena"),
                  "salonow": nw.get("salonow") or 0, "cena_podmiotu": (oferty[oa].cena_zl or 0) * 100}
        for br in (branza[int(sid)], "RAZEM"):
            po_br[br]["wiersze"].append(wiersz)
    warianty = [k for k in nowy if k not in {s7._pierwsza(int(s), oferty) for s in stary}]
    print(f"{'branża':<20} {'usług':>5} {'stary z ceną':>13} {'podpis „ta sama”':>17} {'„ta sama” 1–2':>14} "
          f"{'tylko „podobne”':>16} {'bez ceny':>9} {'mediana różnicy':>16}")
    wynik = {}
    for br in sorted(po_br, key=lambda b: (b == "RAZEM", b)):
        w = po_br[br]["wiersze"]
        n = len(w)
        st_c = sum(x["stary"] is not None for x in w)
        pd_c = sum(x["podpis"] is not None for x in w)
        # decyzja Alexa 30.09: 1–2 salony „ta sama” = wiersz z ich cenami, bez mediany rynku
        malo = sum(x["podpis"] is None and x["salonow"] > 0 for x in w)
        tylko_pod = sum(x["podpis"] is None and not x["salonow"] and x["podobne"] is not None for x in w)
        bez = sum(x["podpis"] is None and not x["salonow"] and x["podobne"] is None for x in w)
        oba = [abs(x["podpis"] - x["stary"]) / x["stary"] for x in w if x["podpis"] and x["stary"]]
        roznica = statistics.median(oba) if oba else None
        wynik[br] = {"uslug": n, "stary_z_cena": st_c, "podpis_ta_sama": pd_c, "ta_sama_1_2": malo, "tylko_podobne": tylko_pod,
                     "bez_ceny": bez, "mediana_roznicy": round(roznica, 3) if roznica is not None else None,
                     "par_z_cena_w_obu": len(oba)}
        print(f"{br:<20} {n:>5} {st_c:>8} ({st_c / n:>3.0%}) {pd_c:>11} ({pd_c / n:>3.0%}) {malo:>8} ({malo / n:>3.0%}) "
              f"{tylko_pod:>10} ({tylko_pod / n:>3.0%}) "
              f"{bez:>4} ({bez / n:>3.0%}) {('%.0f%%' % (roznica * 100)) if roznica is not None else '—':>10} (n={len(oba)})")
    zw = [nowy[k] for k in warianty]
    wynik["warianty"] = {"ofert": len(zw), "z_cena_ta_sama": sum(x["cena"] is not None for x in zw)}
    print(f"pozostałe warianty podmiotu (osobne wiersze podpisu): {len(zw)}, z ceną „ta sama”: "
          f"{wynik['warianty']['z_cena_ta_sama']}")
    (s7.OUT / "porownanie_silnikow.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wyjscie", default="sprawdzian8")
    ap.add_argument("--stary", action="store_true")
    ap.add_argument("--porownaj", action="store_true")
    a = ap.parse_args()
    _ustaw(a.wyjscie)
    if a.stary:
        asyncio.run(_stary())
    if a.porownaj:
        _porownaj()


if __name__ == "__main__":
    main()
