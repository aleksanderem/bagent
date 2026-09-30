"""Wypisuje pary sprawdzianu 7 do mojej oceny (kawałkami), bez werdyktów metod — oceniam na ślepo."""
import json, sys
d = json.load(open("/Users/alex/Desktop/MOJE_PROJEKTY/bagent/scripts/katalog/dane/2026-09-29/sprawdzian8/probka_oceny.json"))
pary = sorted(d["pary"], key=lambda q: (q["branza"], q["oa"]["nazwa"], q["a"], q["b"]))
od, do = int(sys.argv[1]), int(sys.argv[2])
def o(x, w):
    s = f'{x["nazwa"]!r}' + (f' / {x["wariant"]!r}' if x["wariant"] else "") + f' [{x["kategoria"][:40]}]' + f' {x["cena"]} zł'
    if w and len(w) > 1: s += f' | warianty: {w}'
    if x["opis"]: s += f' | opis: {x["opis"][:170]!r}'
    return s + f' | typ: {x["typ"][:18]} | booksy: {x["zabieg_booksy"][:30]}'
for i, q in enumerate(pary[od:do], start=od):
    print(f'#{i} [{q["branza"][:10]}]\n  A {o(q["oa"], q["warianty_a"])}\n  B {o(q["ob"], q["warianty_b"])}')
