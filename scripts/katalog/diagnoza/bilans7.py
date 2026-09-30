"""Bilans werdyktów sprawdzianu 7 po zmianie vs werdykty z próby (moje oceny)."""
import json, sys, importlib.util
from collections import Counter
sys.path[:0] = ["/Users/alex/Desktop/MOJE_PROJEKTY/bagent", "/Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts", "/Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts/typesafe"]
s = importlib.util.spec_from_file_location("s7", "/Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts/katalog/sprawdzian7.py"); m = importlib.util.module_from_spec(s); sys.modules["s7"] = m; s.loader.exec_module(m)
D = m.OUT
stare = {(q["a"], q["b"]): q for q in json.load(open(D / "probka_oceny.json"))["pary"]}
oc = {(o["a"], o["b"]): o["ocena"] for o in json.load(open(D / "ocena_claude.json"))}
pary, oferty = m.werdykty()
po = {(q["a"], q["b"]): q for q in pary}
us, pu, _ = m.dane()
zm = Counter(); nowe = []; ts = []
for q in pu:
    k = (m._pierwsza(q["a"], oferty), m._pierwsza(q["b"], oferty))
    teraz = po[k]["podpis"] == "ta_sama"; przed = stare.get(k, {}).get("podpis") == "ta_sama"
    if teraz: ts.append((k, q["branza"]))
    if teraz != przed:
        lab = oc.get(k, "?"); zm[(przed, teraz, lab)] += 1
        if lab == "?": nowe.append(k)
print("zmiany (przed, teraz, ocena):", dict(zm))
t = sum(oc.get(k) == "T" for k, _b in ts); n = len(ts); bez = sum(k not in oc for k, _b in ts)
print(f"teraz ta sama {n}, T {t} ({t / max(n - bez, 1):.1%} z ocenionych), bez oceny {bez}")
for br in sorted({b for _k, b in ts}):
    z = [k for k, b in ts if b == br]; tt = sum(oc.get(k) == "T" for k in z)
    print(f"  {br:<20} {tt}/{len(z)}")
for q in pu:
    k = (m._pierwsza(q["a"], oferty), m._pierwsza(q["b"], oferty))
    if stare.get(k, {}).get("podpis") == "ta_sama" and po[k]["podpis"] != "ta_sama" and oc.get(k) == "T":
        a, b = oferty[k[0]], oferty[k[1]]
        print(f"  STRATA {a.nazwa!r} [{a.kategoria[:22]}] | {b.nazwa!r}/{b.wariant!r} [{b.kategoria[:28]}] | {po[k]['powod'][:70]}")
for k in nowe[:40]:
    a, b = oferty[k[0]], oferty[k[1]]
    print(f"  NOWA {k} | {a.nazwa!r}/{a.wariant!r} [{a.kategoria[:28]}] | {b.nazwa!r}/{b.wariant!r} [{b.kategoria[:28]}] | {po[k]['powod'][:60]}")
