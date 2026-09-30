"""Bilans: stare uzupełnianie z kategorii (HEAD) vs nowe — te same pary, kontekst, słownik, klasy."""
import importlib.util, json, sys
from collections import Counter
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent"); S = Path(sys.argv[1])
sys.path[:0] = [str(B), str(B / "scripts")]
def mod(n, p):
    s = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(s); sys.modules[n] = m; s.loader.exec_module(m); return m
op = mod("ocena_podpisu", B / "scripts/katalog/ocena_podpisu.py")
stary = mod("podpis_stary", S / "podpis_stary.py")
import services.katalog_uslug.podpis as nowy
from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij
W = B / "scripts/katalog/dane/2026-09-29/w2"
op.tp.OUT = W / "wszystkie"
pary = op.pary_ocenione()
rek, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
sl = json.loads((W / "slownik.json").read_text())
km = mod("kategorie", B / "scripts/katalog/kategorie.py")
kon = km.kontekst(op.tp.oferty_probki(0), W / "kategorie.json")
roz = json.loads((W / "klasy_p2.json").read_text())
opis = {tuple(v["klasa"]) for v in roz.values() if rozstrzygnij(v.get("score")) == NIE_ZMIENIA}
def w(m, q):
    ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
    if not (ra and rb): return "brak"
    return m.porownaj(m.podpis(ra, sl, kon.get(q["oa"].id)), m.podpis(rb, sl, kon.get(q["ob"].id)), m.Klasy(opisowe=opis))[0]
zm = Counter(); przyk = []
for q in pary:
    a, b = w(stary, q), w(nowy, q)
    if a != b:
        zm[(a == "ta_sama", b == "ta_sama", q["ocena"])] += 1
        if (a == "ta_sama") != (b == "ta_sama"):
            przyk.append(f"  {q['ocena']} {a}→{b} [{q['branza'][:10]}] {q['oa'].nazwa!r}/{q['oa'].wariant!r} ({q['oa'].kategoria}) vs {q['ob'].nazwa!r}/{q['ob'].wariant!r} ({q['ob'].kategoria})")
print("zmiany (stara ta_sama, nowa ta_sama, ocena):", dict(zm))
print("\n".join(sorted(przyk)))
