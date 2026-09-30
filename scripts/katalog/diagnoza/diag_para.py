"""Diagnoza pojedynczych par zbioru 1042 (tylko odczyt pamięci, 0 USD)."""
import sys, json
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog")]
import ocena_podpisu as op
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU
from services.katalog_uslug.podpis import podpis, porownaj, Klasy
import importlib.util
op.tp.OUT = op.tp.OUT.parent / f"w{WERSJA_PROMPTU}" / "wszystkie"
pary = op.pary_ocenione()
rek, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
D = B / "scripts/katalog/dane/2026-09-29/w2"
slownik = json.loads((D / "slownik.json").read_text())
spec = importlib.util.spec_from_file_location("kategorie", B / "scripts/katalog/kategorie.py")
km = importlib.util.module_from_spec(spec); spec.loader.exec_module(km)
kon = km.kontekst(op.tp.oferty_probki(0), D / "kategorie.json")
slowa = op.slownictwo_rynku(rek, slownik)
szukane = [x.lower() for x in sys.argv[1:]]
for q in pary:
    n = f"{q['oa'].nazwa} || {q['ob'].nazwa}".lower()
    if all(s in n for s in szukane):
        for o in (q["oa"], q["ob"]):
            r = rek.get(o.id)
            p = podpis(r, slownik, kon.get(o.id), slowa)
            print(f"{o.nazwa!r} / {o.wariant!r} [{o.kategoria}] booksy={o.zabieg_booksy!r}")
            print("   rekord:", json.dumps({k: r.get(k) for k in ("zabieg", "cechy", "szum", "nieprzypisane")}, ensure_ascii=False)[:600])
            print("   kontekst:", json.dumps({k: (kon.get(o.id) or {}).get(k) for k in ("zabieg", "cechy")}, ensure_ascii=False)[:300])
            print("   podpis:", sorted(p.poziomy))
        print("   ocena:", q["ocena"])
