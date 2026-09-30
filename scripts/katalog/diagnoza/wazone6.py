"""Zbiór 6: ważone szacunki trafności i liczby trafnych par dla v14f-w2 (v14), syn (v14p) i v13b — z moich ocen próbki.

Grupy rozłączne w branży: S14 (v14 „ta sama”), Sp\\S14 (tylko syn), S13\\S14\\Sp (tylko v13b); każda ważona swoją liczebnością.
"""
import json
from collections import defaultdict
from pathlib import Path

OUT = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts/typesafe/dane/2026-09-28/v14_sprawdzian6")
pary = json.loads((OUT / "pary.json").read_text())
oc = json.loads((OUT / "ocena_claude.json").read_text())
T = lambda q: q["ocena_claude"] == "T"


def frac(lst):
    return (sum(map(T, lst)) / len(lst)) if lst else 0.0


wyn = defaultdict(lambda: defaultdict(float))
for br in sorted({q["branza"] for q in pary}):
    z = [q for q in pary if q["branza"] == br]
    s14 = [q for q in z if q["v14"] == "tozsame"]
    str_ = [q for q in z if q.get("v14p") == "tozsame" and q["v14"] != "tozsame"]
    t13 = [q for q in z if q["v13"] == "tozsame" and q["v14"] != "tozsame" and q.get("v14p") != "tozsame"]
    g14 = [q for q in oc if q["branza"] == br and q["grupa"] in ("v14", "tylko_v14")]
    gst = [q for q in oc if q["branza"] == br and q["grupa"] == "stracone_poprawka"]
    g13 = [q for q in oc if q["branza"] == br and q["grupa"] == "tylko_v13b"]
    w = wyn[br]
    # v14 (w2)
    w["v14_n"] = len(s14); w["v14_T"] = len(s14) * frac(g14)
    # syn: część wspólna z v14 (z próbki v14, gdzie syn też „ta sama”) + tylko syn
    wsp = [q for q in s14 if q.get("v14p") == "tozsame"]
    g14p = [q for q in g14 if q.get("v14p") == "tozsame"]
    w["syn_n"] = len(wsp) + len(str_); w["syn_T"] = len(wsp) * frac(g14p) + len(str_) * frac(gst)
    # v13b: z v14 + ze „stracone” + tylko v13b
    a = [q for q in s14 if q["v13"] == "tozsame"]; ga = [q for q in g14 if q["v13"] == "tozsame"]
    b = [q for q in str_ if q["v13"] == "tozsame"]; gb = [q for q in gst if q["v13"] == "tozsame"]
    w["v13_n"] = len(a) + len(b) + len(t13)
    w["v13_T"] = len(a) * frac(ga) + len(b) * (frac(gb) if gb else frac(gst)) + len(t13) * frac(g13)
    w["suma_T"] = w["v14_T"] + len(str_) * frac(gst) + len(t13) * frac(g13)
    w["probka"] = len(g14) + len(gst) + len(g13)

razem = defaultdict(float)
for br, w in wyn.items():
    for k, v in w.items():
        razem[k] += v
wyn["RAZEM"] = razem
print(f"{'branża':<20} {'v14f-w2: ta sama / trafne / traf%':>34} | {'syn':>22} | {'v13b':>22} | wszystkie trafne (szac.)")
for br, w in wyn.items():
    f = lambda p: f"{w[p + '_n']:4.0f} / {w[p + '_T']:5.1f} / {w[p + '_T'] / w[p + '_n'] if w[p + '_n'] else 0:4.0%}"
    print(f"{br:<20} {f('v14'):>34} | {f('syn'):>22} | {f('v13'):>22} | {w['suma_T']:6.1f}  "
          f"(v14 znajduje {w['v14_T'] / w['suma_T'] if w['suma_T'] else 0:.0%}, syn {w['syn_T'] / w['suma_T'] if w['suma_T'] else 0:.0%}, "
          f"v13b {w['v13_T'] / w['suma_T'] if w['suma_T'] else 0:.0%})")
