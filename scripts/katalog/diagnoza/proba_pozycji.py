"""Próba pytania o pozycję kategorii na ≤ 40 kategoriach (do obejrzenia oczami) — wybór próby tylko tutaj."""
import asyncio, json, random, re, sys
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog")]
import kategorie as km
from services.katalog_uslug.ekstrakcja import Oferta, prompt_pozycji_kategorii, waliduj_pozycje
D = B / "scripts/katalog/dane/2026-09-29"
nazwy = {}
for d in ("sprawdzian7", "sprawdzian8", "w2", "raport/234429"):
    for kid, r in json.loads((D / d / "kategorie.json").read_text()).items():
        nazwy.setdefault(kid, r.get("nazwa") or "")
podejrz = [k for k, n in nazwy.items() if re.search(r"szkol|kurs|voucher|instru|modelk|model|dodat|wkładk|przystawk|kosmetyka", n, re.I)]
random.seed(7)
reszta = random.sample([k for k in nazwy if k not in podejrz], 40 - min(len(podejrz), 34))
wyb = sorted(podejrz)[:34] + reszta
kat = [Oferta(id=k, typ_salonu="", kategoria="", nazwa=nazwy[k], wariant="", zabieg_booksy="", opis="", cena_zl=None) for k in wyb]
out = Path(sys.argv[1])
w = asyncio.run(km.wyciagnij(kat, out, 3, prompt_pozycji_kategorii, waliduj_pozycje, 40))
for k in wyb:
    v = w.get(k, {})
    print(f"{v.get('pozycja','BRAK'):10} | {nazwy[k]!r} | fraza={v.get('fraza','')!r}")
