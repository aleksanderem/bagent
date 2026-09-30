"""Reguła „nie zmienia”: poziom najbliższy średniej (dziś) vs P(„nie zmienia”) > p — na tych samych danych (0 USD)."""
import sys, io, contextlib
from pathlib import Path
B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug import klasy as K
import ocena_podpisu as op, sprawdzian7 as s7, porownanie_silnikow as ps
prog = None if sys.argv[1] == "none" else float(sys.argv[1])
K.PROG_NIE_ZMIENIA = prog
def zlap(f):
    b = io.StringIO()
    with contextlib.redirect_stdout(b):
        f()
    return b.getvalue()
sys.argv = ["x", "--klasy", "--slownik", "--kategorie"]
out = zlap(op.main)
print(f"== próg {prog}: zbiór 1042:", [l.strip() for l in out.splitlines() if "podpis    „ta sama”" in l][0])
for wy in ("sprawdzian8", "sprawdzian7"):
    ps._ustaw(wy)
    o1 = zlap(s7.przelicz)
    o2 = zlap(ps._porownaj)
    print(f"   {wy}:", [l for l in o1.splitlines() if l.startswith("RAZEM")][0], "|", [l for l in o2.splitlines() if l.startswith("RAZEM")][0][20:80])
