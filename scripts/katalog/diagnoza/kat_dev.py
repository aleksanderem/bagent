"""Rozkład kategorii dla zbioru 1042 par (GLM z abonamentu, 0 USD)."""
import asyncio, importlib.util, sys
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts")]
def mod(n, p):
    s = importlib.util.spec_from_file_location(n, p); m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
tp = mod("test_paczek", B / "scripts/katalog/test_paczek.py")
km = mod("kategorie", B / "scripts/katalog/kategorie.py")
KAT = B / "scripts/katalog/dane/2026-09-29/w2"
tp.OUT = KAT / "wszystkie"
kat = km.kategorie_ofert(tp.oferty_probki(0))
if "--licz" in sys.argv:
    print("unikalnych kategorii", len(kat)); sys.exit()
pam = asyncio.run(km.wyciagnij(kat, KAT / "kategorie.json", 3))
print("rozłożonych", sum(k.id in pam for k in kat), "z", len(kat))
