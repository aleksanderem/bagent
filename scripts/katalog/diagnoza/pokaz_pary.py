"""Pary sprawdzianu do mojej oceny na ślepo (bez werdyktów metod i bez grupy), kawałkami, oraz zapis ocen.

  python scripts/katalog/diagnoza/pokaz_pary.py sprawdzian9 0 50          # wypisz pary 0–49
  python scripts/katalog/diagnoza/pokaz_pary.py sprawdzian9 --zapisz "0:T 1:P 2:I"   # dopisz oceny (indeks:ocena)
  python scripts/katalog/diagnoza/pokaz_pary.py sprawdzian9 --zamknij     # oceny → ocena_claude.json

Kolejność: branża, nazwa oferty podmiotu, id — grupy („ta sama” podpisu, reszta, warianty) wymieszane w obrębie
usługi. Oceny T/P/I według modelu tej samej usługi; robocze oceny w oceny_indeks.json obok próbki (nie w /tmp).
"""
import json
import sys
from pathlib import Path

B = Path(__file__).resolve().parents[3]
KAT = B / "scripts" / "katalog" / "dane" / "2026-09-29" / sys.argv[1]
d = json.loads((KAT / "probka_oceny.json").read_text(encoding="utf-8"))
pary = sorted(d["pary"], key=lambda q: (q["branza"], q["oa"]["nazwa"], q["a"], q["b"]))
PLIK = KAT / "oceny_indeks.json"


def _o(x: dict, w: list) -> str:
    s = f'{x["nazwa"]!r}' + (f' / {x["wariant"]!r}' if x["wariant"] else "") + f' [{x["kategoria"][:40]}]' + f' {x["cena"]} zł'
    if w and len(w) > 1:
        s += f" | warianty: {w}"
    if x["opis"]:
        s += f' | opis: {x["opis"][:170]!r}'
    return s + f' | typ: {x["typ"][:18]} | booksy: {x["zabieg_booksy"][:30]}'


if len(sys.argv) > 2 and sys.argv[2] == "--zapisz":
    oceny = json.loads(PLIK.read_text(encoding="utf-8")) if PLIK.exists() else {}
    for t in sys.argv[3].split():
        i, o = t.split(":")
        assert o in ("T", "P", "I"), t
        oceny[str(int(i))] = o
    PLIK.write_text(json.dumps(oceny, ensure_ascii=False), encoding="utf-8")
    print(f"ocen {len(oceny)} z {len(pary)}")
elif len(sys.argv) > 2 and sys.argv[2] == "--zamknij":
    oceny = json.loads(PLIK.read_text(encoding="utf-8"))
    brak = [i for i in range(len(pary)) if str(i) not in oceny]
    assert not brak, f"brak ocen: {brak[:20]}"
    wynik = [{"a": q["a"], "b": q["b"], "branza": q["branza"], "grupa": q["grupa"], "ocena": oceny[str(i)]}
             for i, q in enumerate(pary)]
    (KAT / "ocena_claude.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=0), encoding="utf-8")
    print(f"zapisano {len(wynik)} ocen")
else:
    od, do = int(sys.argv[2]), int(sys.argv[3])
    for i, q in enumerate(pary[od:do], start=od):
        print(f'#{i} [{q["branza"][:10]}]\n  A {_o(q["oa"], q["warianty_a"])}\n  B {_o(q["ob"], q["warianty_b"])}')
