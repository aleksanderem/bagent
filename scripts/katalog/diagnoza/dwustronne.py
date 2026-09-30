"""Rozkład różnic dwustronnych na zbiorze 1042 par: wielkość różnicy vs moja ocena."""
import importlib.util, json, sys
from collections import Counter
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts")]
def mod(n, p):
    s = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(s); sys.modules[n] = m; s.loader.exec_module(m); return m
op = mod("ocena_podpisu", B / "scripts/katalog/ocena_podpisu.py")
from services.katalog_uslug.podpis import podpis, porownaj, Klasy, TA_SAMA
W = B / "scripts/katalog/dane/2026-09-29/w2"
op.tp.OUT = W / "wszystkie"
pary = op.pary_ocenione()
rek, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
sl = json.loads((W / "slownik.json").read_text())
km = mod("kategorie", B / "scripts/katalog/kategorie.py")
kon = km.kontekst(op.tp.oferty_probki(0), W / "kategorie.json")
tab = Counter(); przyk = {}
for q in pary:
    ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
    if not (ra and rb): continue
    pa, pb = podpis(ra, sl, kon.get(q["oa"].id)), podpis(rb, sl, kon.get(q["ob"].id))
    da, db = pa.zbior - pb.zbior, pb.zbior - pa.zbior
    if da and db and pa.zbior & pb.zbior:
        k = (min(len(da), 3), min(len(db), 3)); k = tuple(sorted(k))
        tab[(k, q["ocena"])] += 1
        przyk.setdefault((k, q["ocena"]), []).append(f"{sorted(da)} / {sorted(db)} | {q['oa'].nazwa} vs {q['ob'].nazwa}")
for k in sorted({k for k, _ in tab}):
    print(k, {o: tab[(k, o)] for o in "TPI"})
for o in "TPI":
    print(f"== (1,1) {o}:"); [print("  ", x) for x in przyk.get(((1, 1), o), [])[:14]]
print("\n=== strona różnicy złożona tylko ze słów kontekstu (kategoria / Booksy / opis)")
tab2 = Counter()
for q in pary:
    ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
    if not (ra and rb): continue
    pa, pb = podpis(ra, sl, kon.get(q["oa"].id)), podpis(rb, sl, kon.get(q["ob"].id))
    da, db = pa.zbior - pb.zbior, pb.zbior - pa.zbior
    if da and db and pa.zbior & pb.zbior:
        ka, kb = not (da & pa.wlasne), not (db & pb.wlasne)
        tab2[("kontekst po jednej stronie" if ka != kb else "kontekst po obu" if ka else "własne po obu", q["ocena"])] += 1
for k in sorted({k for k, _ in tab2}):
    print(k, {o: tab2[(k, o)] for o in "TPI"})
