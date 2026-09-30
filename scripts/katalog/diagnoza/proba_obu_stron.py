"""Próba 40 nowych klas z różnic dwustronnych (zbiór 1042 par) — do obejrzenia przed pełnym przebiegiem."""
import asyncio, json, sys, importlib.util
from collections import Counter
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog")]
import klasy_roznic as kr
from services.katalog_uslug.podpis import klasy_roznicy, klasy_obu_stron, podpis
from services.katalog_uslug.klasy import rozstrzygnij
slownik = json.loads((kr.PLIK.parent / "slownik.json").read_text())
spec = importlib.util.spec_from_file_location("kategorie", B / "scripts/katalog/kategorie.py")
km = importlib.util.module_from_spec(spec); spec.loader.exec_module(km)
kon = km.kontekst(kr.op.tp.oferty_probki(0), kr.PLIK.parent / "kategorie.json")
wszystkie = kr.klasy_z_par("p12", slownik, kon)
pamiec = json.loads(kr.PLIK.read_text()) if kr.PLIK.exists() else {}
# które klasy pochodzą z różnic dwustronnych: przelicz pary jeszcze raz
op = kr.op
pary = op.pary_ocenione()
rek, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
slowa = kr.slownictwo_rynku(rek, slownik)
z_obu = Counter()
for q in pary:
    ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
    if not (ra and rb): continue
    pa, pb = podpis(ra, slownik, kon.get(q["oa"].id), slowa), podpis(rb, slownik, kon.get(q["ob"].id), slowa)
    if klasy_roznicy(pa, pb) is None and (obie := klasy_obu_stron(pa, pb)):
        for k, _z in obie:
            z_obu[json.dumps(k, ensure_ascii=False)] += 1
nowe = [k for k, _n in z_obu.most_common() if k not in pamiec and k in wszystkie]
print(f"klas z różnic dwustronnych {len(z_obu)}, nowych {len(nowe)}; wszystkich nowych klas {sum(k not in pamiec for k in wszystkie)}")
proba = {k: wszystkie[k] for k in nowe[:40]}
koszt = asyncio.run(kr.zapytaj(proba, 0.05))
pamiec = json.loads(kr.PLIK.read_text())
for k in proba:
    v = pamiec[k]
    print(f"  {rozstrzygnij(v.get('score'))} ({None if v.get('score') is None else round(v['score'], 2)}) [{v['klasa'][1]}] „{v['dopisek']}” przy „{v['zabieg']}” vs „{v['druga']}”")
print(f"koszt {koszt:.4f} USD")
