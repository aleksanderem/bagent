"""Zgubione pary T w wybranej branży: powód i obie oferty (podpis z kontekstem, słownikiem, klasami i zamianami)."""
import importlib.util, json, sys
from collections import Counter
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts")]
def mod(n, p):
    s = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(s); sys.modules[n] = m; s.loader.exec_module(m); return m
op = mod("ocena_podpisu", B / "scripts/katalog/ocena_podpisu.py")
from services.katalog_uslug.podpis import podpis, porownaj, Klasy, TA_SAMA
from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij, zamiana_rownowazna
W = B / "scripts/katalog/dane/2026-09-29/w2"
op.tp.OUT = W / "wszystkie"
pary = op.pary_ocenione(); rek, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
sl = json.loads((W / "slownik.json").read_text())
km = mod("kategorie", B / "scripts/katalog/kategorie.py"); kon = km.kontekst(op.tp.oferty_probki(0), W / "kategorie.json")
roz = json.loads((W / "klasy_p2.json").read_text()); zam = json.loads((W / "zamiany_p1.json").read_text())
kl = Klasy(opisowe={tuple(v["klasa"]) for v in roz.values() if rozstrzygnij(v.get("score")) == NIE_ZMIENIA},
           rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
br = sys.argv[1]; pow = Counter()
for q in pary:
    if q["branza"] != br or q["ocena"] != "T": continue
    ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
    if not (ra and rb): print("  brak rekordu", q["oa"].nazwa, "|", q["ob"].nazwa); continue
    pa, pb = podpis(ra, sl, kon.get(q["oa"].id)), podpis(rb, sl, kon.get(q["ob"].id))
    w, p = porownaj(pa, pb, kl)
    if w != TA_SAMA:
        pow[p.split(":")[0]] += 1
        print(f"  {p[:70]:70} | {q['oa'].nazwa!r}/{q['oa'].wariant!r} vs {q['ob'].nazwa!r}/{q['ob'].wariant!r}")
print(pow)
