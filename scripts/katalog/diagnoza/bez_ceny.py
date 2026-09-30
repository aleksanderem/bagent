"""Wiersze bez ceny „ta sama”: najbliżsi kandydaci z werdyktem i powodem (0 USD)."""
import sys, json
from collections import defaultdict
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
import porownanie_silnikow as ps
from services.katalog_uslug.dopasowanie import werdykty
from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij, zamiana_rownowazna
from services.katalog_uslug.podpis import Klasy, TA_SAMA
ps._ustaw(sys.argv[1])
s7 = ps.s7
pary, oferty, _rek, x = s7.podpisy()
pod = x["pod"]
roz = json.loads(s7.kr.PLIK.read_text()); zam = json.loads(s7.kr.PLIK_ZAMIAN.read_text())
kl = Klasy(opisowe={tuple(v["klasa"]) for v in roz.values() if rozstrzygnij(v.get("score")) == NIE_ZMIENIA},
           rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
us, pary_u, _ = s7.dane()
kand = defaultdict(list)
for q in pary:
    if q["bez_wspolnych"] or q["a"] not in pod or q["b"] not in pod: continue
    kand[q["a"]].append((q, pod[q["b"]]))
for sid in sorted({q["a"] for q in pary_u}):
    oa = s7._pierwsza(sid, oferty)
    w = werdykty(pod[oa], [({"q": q}, p) for q, p in kand.get(oa, [])], kl) if oa in pod else []
    salony = {x[0]["q"]["cand_salon"] for x in w if x[1] == TA_SAMA}
    if len(salony) >= 3: continue
    o = oferty[oa]
    print(f"\n{o.nazwa!r} [{o.kategoria}] {o.cena_zl} zł — „ta sama” {len(salony)} sal.; podpis {sorted(pod[oa].zbior) if oa in pod else '-'}")
    for (d, v, powod) in sorted(w, key=lambda t: -t[0]["q"]["sim"])[:6]:
        ob = oferty[d["q"]["b"]]
        print(f"    {v:8} {d['q']['sim']:.2f} {ob.nazwa[:40]!r}/{ob.wariant[:20]!r} [{ob.kategoria[:25]}] — {powod[:70]}")
