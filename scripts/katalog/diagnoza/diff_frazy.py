"""Werdykty sprawdzianu z poprawką źródeł fraz i bez niej — które ocenione pary się zmieniają (0 USD)."""
import sys, json
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
import sprawdzian7 as s7
from services.katalog_uslug import podpis as pm
wy = sys.argv[1]
s7.OUT = s7.OUT.parent / wy
s7.V14 = B / "scripts/typesafe/dane/2026-09-28" / f"v14_{wy}" / "pary.json"
oc = {(q["a"], q["b"]): q["ocena"] for q in json.loads((s7.OUT / "ocena_claude.json").read_text())}
nowe, _ = s7.werdykty()
nowe = {(q["a"], q["b"]): (q["podpis"], q["powod"]) for q in nowe}
oryg = pm._frazy
def stare(rek):
    z = rek.get("zabieg") or {}
    return [("zabieg", z.get("zrodlo") or "nazwa", z.get("fraza") or "")] + [(c.get("rola") or "", c.get("zrodlo") or "nazwa", c.get("fraza") or "") for c in rek.get("cechy") or []]
pm._frazy = stare
st, of = s7.werdykty()
pm._frazy = oryg
for q in st:
    k = (q["a"], q["b"])
    if k in oc and (q["podpis"] == "ta_sama") != (nowe[k][0] == "ta_sama"):
        print(oc[k], q["podpis"], "→", nowe[k][0], "|", nowe[k][1][:80], "|", of[q["a"]].nazwa, "[", of[q["a"]].kategoria, "] ||", of[q["b"]].nazwa, "[", of[q["b"]].kategoria, "]")
