"""Etap 3, krok A — suchy przebieg na PRAWDZIWYM raporcie konkurencji (tylko odczyt produkcji, zapis lokalny).

Wszystkie usługi podmiotu z audytu raportu i ta sama pula salonów co raport (promień 15 km), kandydaci jak
w sprawdzianach (wektory ≥ 0,6, do 120 na usługę) → format sprawdzianu w dane/2026-09-29/raport_<id>/, plus
wiersze wyceny, które raport pokazał klientce (stary silnik) → wycena_raportu.json. Dalej zwykły potok sprawdzianu:

  python scripts/katalog/zbierz_raport.py --raport 279                        # zbieranie              0 USD
  python scripts/katalog/sprawdzian7.py --wyjscie raport_279 --wyciagnij      # rozbiór ofert GLM      0 USD
  python scripts/katalog/sprawdzian7.py --wyjscie raport_279 --salony
  python scripts/katalog/kategorie.py scripts/katalog/dane/2026-09-29/raport_279 --pamiec .../wszystkie/pozycje_kategorii_v2.json
  python scripts/katalog/sprawdzian7.py --wyjscie raport_279 --klasy          # klasy różnic TypeSafe  grosze
  python scripts/katalog/zbierz_raport.py --raport 279 --porownaj             # podpis obok raportu
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "typesafe")]
from dotenv import load_dotenv  # noqa: E402

load_dotenv(B / ".env")
from services.supabase import SupabaseService  # noqa: E402

POLA_WYCENY = ("treatment_name,subject_price_grosze,subject_duration_minutes,market_median_grosze,sample_size,"
               "verification_status,comparison_tier,sub_variant_label")


def _modul(nazwa: str, plik: Path):
    spec = importlib.util.spec_from_file_location(nazwa, plik)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def zbierz(raport: int) -> None:
    s7 = _modul("sprawdzian7", B / "scripts" / "katalog" / "sprawdzian7.py")
    s7.OUT = s7.OUT.parent / f"raport_{raport}"
    sb = SupabaseService()
    cli = sb.client
    r = cli.table("competitor_reports").select("id,convex_audit_id,subject_salon_id").eq("id", raport).execute().data[0]
    s = cli.table("salons").select("booksy_id,city,primary_category_id").eq("id", r["subject_salon_id"]).execute().data[0]
    kat = {k["id"]: k["name"] for k in cli.table("business_categories").select("id,name").execute().data or []}
    dane = asyncio.run(sb.get_subject_full_data(r["convex_audit_id"]))
    salon = (kat.get(s["primary_category_id"], ""), s["booksy_id"], s.get("city") or "", {**dane, "booksy_id": s["booksy_id"]})

    async def tylko_ten(*_a, **_k) -> list[tuple]:
        return [salon]

    import schemat_v10_sprawdzian as sv  # ten sam moduł, z którego losuje sprawdzian
    sv.losuj = tylko_ten
    s7.zbierz(argparse.Namespace(ziarno=s7.ZIARNO, uslug=9999))  # wszystkie usługi podmiotu; własne asyncio.run w środku
    wiersze = (cli.table("competitor_pricing_comparisons").select(POLA_WYCENY).eq("report_id", raport)
               .limit(2000).execute().data or [])
    (s7.OUT / "wycena_raportu.json").write_text(json.dumps(wiersze, ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"raport {raport}: salon {salon[1]} ({salon[0]}, {salon[2]}), wierszy wyceny w raporcie {len(wiersze)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raport", type=int, required=True)
    ap.add_argument("--porownaj", action="store_true")
    a = ap.parse_args()
    if a.porownaj:
        porownaj(a.raport)
    else:
        zbierz(a.raport)


def porownaj(raport: int) -> None:
    """Wiersz raportu (stary silnik, to widziała klientka) obok wyceny podpisem — po nazwie usługi i cenie podmiotu."""
    ps = _modul("porownanie_silnikow", B / "scripts" / "katalog" / "porownanie_silnikow.py")
    ps._ustaw(f"raport_{raport}")
    s7 = ps.s7
    us, _pary, _s = s7.dane()
    oferty = {o.id: o for u in us.values() for o in s7.oferty_z_uslugi(u)}
    nowy = ps._podpis_wiersze()
    stare = json.loads((s7.OUT / "wycena_raportu.json").read_text(encoding="utf-8"))
    po_nazwie = {((w["treatment_name"] or "").strip().lower(), w["subject_price_grosze"]): w for w in stare}
    wiersze = []
    for oid, nw in nowy.items():
        o = oferty[oid]
        st = po_nazwie.get((o.nazwa.strip().lower(), round((o.cena_zl or 0) * 100))) or {}
        rodzaj = ("ta sama ≥3" if nw["cena"] is not None else "ta sama 1–2" if nw["salonow"] else
                  "tylko podobne" if nw["podobne_cena"] is not None else "brak")
        wiersze.append({"oferta": oid, "nazwa": o.nazwa, "wariant": o.wariant, "cena": o.cena_zl, "rodzaj": rodzaj,
                        "podpis": nw["cena"], "salonow": nw["salonow"], "podobne": nw["podobne_cena"],
                        "stary": st.get("market_median_grosze"), "stary_status": st.get("verification_status"),
                        "stary_probka": st.get("sample_size")})
    n = len(wiersze)
    from collections import Counter
    import statistics
    ile = Counter(w["rodzaj"] for w in wiersze)
    stary_c = sum(w["stary"] is not None for w in wiersze)
    print(f"ofert podmiotu z kandydatami {n}; stary silnik z ceną {stary_c} ({stary_c / max(n, 1):.0%})")
    for k in ("ta sama ≥3", "ta sama 1–2", "tylko podobne", "brak"):
        print(f"  podpis {k:<14} {ile[k]:4} ({ile[k] / max(n, 1):.0%})")
    oba = [abs(w["podpis"] - w["stary"]) / w["stary"] for w in wiersze if w["podpis"] and w["stary"]]
    if oba:
        print(f"  gdzie obie metody mają cenę: {len(oba)} wierszy, mediana różnicy {statistics.median(oba):.0%}")
    (s7.OUT / "porownanie_raportu.json").write_text(json.dumps(wiersze, ensure_ascii=False, indent=0), encoding="utf-8")


if __name__ == "__main__":
    main()
