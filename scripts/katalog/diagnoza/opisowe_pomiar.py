"""Czy „cecha opisowa” z planu (żaden salon nie sprzedaje obu wersji w różnych cenach) oddziela moje T od P/I
na parach z różnicą jednostronną (zbiór 1042 par). Rynek = wszystkie wyciągnięte oferty (14 tys.). 0 USD."""
import sys, json, importlib.util
from collections import defaultdict, Counter
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog")]
import synonimy as sy
import ocena_podpisu as op
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU
from services.katalog_uslug.podpis import podpis, klasy_roznicy, klasy_obu_stron, KOLEJNOSC, TA_SAMA, porownaj, Klasy
from services.katalog_uslug.klasy import NIE_ZMIENIA, rozstrzygnij
D = B / "scripts/katalog/dane/2026-09-29/w2"
slownik = json.loads((D / "slownik.json").read_text())
oferty, rek, salon = sy.dane_rynku()
rynek = []  # (salon, cena, zbiór, poziomy)
for o in oferty:
    r = rek.get(o.id)
    if r:
        p = podpis(r, slownik)
        rynek.append((str(salon.get(o.id.split("#")[0])), o.cena_zl, p.zbior, p.poziomy))
po_slowie = defaultdict(list)
for i, (_s, _c, z, _p) in enumerate(rynek):
    for w in z:
        po_slowie[w].append(i)
def domyslna(C: frozenset, poz: str, Dw: frozenset, prog: float = 0.8):
    """Udział salonów, których oferty ⊇ C z czymkolwiek na poziomie poz mają dokładnie dopisek Dw (wartość domyślna)."""
    if not C:
        return 0.0, 0
    kand = set.intersection(*(set(po_slowie.get(w, [])) for w in C))
    z_czyms, z_d = set(), set()
    for i in kand:
        s, c, z, pz = rynek[i]
        na_poz = {w for q, w in pz if q == poz and w not in C}
        if na_poz:
            z_czyms.add(s)
            if Dw <= z:
                z_d.add(s)
    return (len(z_d) / len(z_czyms) if z_czyms else 0.0), len(z_czyms)
def dowody(C: frozenset, poz: str, Dw: frozenset):
    if not C:
        return 0, 0, 0
    kand = set.intersection(*(set(po_slowie.get(w, [])) for w in C))
    z_d, bez_d = defaultdict(set), defaultdict(set)
    for i in kand:
        s, c, z, pz = rynek[i]
        if Dw <= z:
            z_d[s].add(c)
        elif not (z & Dw) and not any(q == poz and w not in C for q, w in pz):
            bez_d[s].add(c)
    oba = sum(1 for s in z_d if s in bez_d and (z_d[s] - bez_d[s] or bez_d[s] - z_d[s]))
    return len(z_d), len(bez_d), oba
# pary ocenione z pełnym podpisem (jak ocena_podpisu)
spec = importlib.util.spec_from_file_location("kategorie", B / "scripts/katalog/kategorie.py")
km = importlib.util.module_from_spec(spec); spec.loader.exec_module(km)
op.tp.OUT = op.tp.OUT.parent / f"w{WERSJA_PROMPTU}" / "wszystkie"
pary = op.pary_ocenione()
rek2, _ = op.tp.rekordy("p12", op.tp.oferty_probki(0))
kon = km.kontekst(op.tp.oferty_probki(0), D / "kategorie.json")
slowa = op.slownictwo_rynku(rek2, slownik)
roz = json.loads((D / "klasy_p2.json").read_text())
decyzja = {tuple(v["klasa"]): rozstrzygnij(v.get("score")) for v in roz.values()}
tab = Counter()
przyklady = defaultdict(list)
for q in pary:
    ra, rb = rek2.get(q["oa"].id), rek2.get(q["ob"].id)
    if not (ra and rb): continue
    pa, pb = podpis(ra, slownik, kon.get(q["oa"].id), slowa), podpis(rb, slownik, kon.get(q["ob"].id), slowa)
    if porownaj(pa, pb, Klasy())[1].startswith(("inny zabieg", "poza", "nieprzypisane", "pozycja", "nazwa bez")):
        continue
    kl = klasy_roznicy(pa, pb) or klasy_obu_stron(pa, pb)
    if not kl: continue
    ocena = "T" if q["ocena"] == "T" else "N"
    dom = all((lambda e: e[0] >= 0.8 and e[1] >= 5)(domyslna(frozenset(klasa[0].split()), klasa[1], frozenset(klasa[2].split()))) for klasa, _z in kl if klasa[1] != "sklad")
    ts0 = all(decyzja.get(tuple(klasa)) == NIE_ZMIENIA for klasa, _z in kl)
    tab[("dom", ocena, "domyslna" if dom else "-", "TS nie zmienia" if ts0 else "TS inaczej")] += 1
    if dom and not ts0 and len(przyklady["dom" + ocena]) < 12:
        przyklady["dom" + ocena].append(f"{q['oa'].nazwa} || {q['ob'].nazwa} — {[kk[2] for kk, _ in kl]}")
    for k in (1, 2, 3):
        opis = all((lambda e: e[2] == 0 and min(e[0], e[1]) >= k)(dowody(frozenset(klasa[0].split()), klasa[1], frozenset(klasa[2].split()))) for klasa, _z in kl if klasa[1] != "sklad")
        ts = all(decyzja.get(tuple(klasa)) == NIE_ZMIENIA for klasa, _z in kl)
        tab[(k, ocena, "opisowa" if opis else "-", "TS nie zmienia" if ts else "TS inaczej")] += 1
        if k == 2 and opis and len(przyklady[ocena]) < 10:
            przyklady[ocena].append(f"{q['oa'].nazwa} || {q['ob'].nazwa} — {[kk[2] for kk, _ in kl]}")
print("domyślna (≥80%, ≥5 salonów):", {f"{o}|{a}|{b}": n for (kk, o, a, b), n in sorted(tab.items(), key=str) if kk == "dom"})
for k in (1, 2, 3):
    print(f"k={k}:", {f"{o}|{a}|{b}": n for (kk, o, a, b), n in sorted(tab.items(), key=str) if kk == k})
for o, l in przyklady.items():
    print("==", o)
    for x in l: print("   ", x)
