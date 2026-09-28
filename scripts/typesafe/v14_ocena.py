"""Próbka par sprawdzianu v14 do MOJEJ oceny (Claude) według modelu tej samej usługi — bez TypeSafe, bez kosztu.

Alex 28.09: pary oceniam sam i podaję liczby; sędzia par wyłączony. Próbka w każdej branży:
  * „v14” — pary, które v14 uznał za tę samą usługę (trafność v14);
  * „tylko_v13b” — v13b mówi „ta sama”, v14 nie (czy v14 coś przeoczył, czy słusznie odrzucił);
  * „tylko_v14” liczone w „v14” (pole grupa), żeby bilans był na tych samych parach.
Ocena: T = ta sama usługa, P = podobna (ten sam zabieg, inny szczegół), I = inna. Zapis: ocena_claude.json.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/v14_ocena.py --pokaz          # wypisz próbkę
  bagent/.venv/bin/python bagent/scripts/typesafe/v14_ocena.py --policz         # liczby z ocena_claude.json
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter, defaultdict
from pathlib import Path

DZ = Path(__file__).resolve().parent / "dane" / "2026-09-28"
OUT = DZ / "v14_sprawdzian"
NA_BRANZE_V14 = 10
NA_BRANZE_V13 = 6


def probka(pary: list[dict]) -> list[dict]:
    rng = random.Random(20261005)
    wynik = []
    for br in sorted({q["branza"] for q in pary}):
        z = [q for q in pary if q["branza"] == br]
        v14 = [q for q in z if q["v14"] == "tozsame"]
        # grupy rozłączne (ważenie w szacunku trafności każdej wersji): v14 / tylko poprzednia / tylko v13b
        tylko13 = [q for q in z if q["v13"] == "tozsame" and q["v14"] != "tozsame" and q.get("v14e") != "tozsame"]
        # bilans z poprzednią wersją na tych samych parach: co traci nowa (v14e — zbiór 5+, v14d — scalenie w zbiorze 4)
        pop = "v14e" if any("v14e" in q for q in z) else "v14d"
        stracone = [q for q in z if q.get(pop) == "tozsame" and q["v14"] != "tozsame" and (pop == "v14e" or q["v13"] != "tozsame")]
        nazwa = "stracone_poprawka" if pop == "v14e" else "stracone_scaleniem"
        for grupa, lst, n in (("v14", v14, NA_BRANZE_V14), ("tylko_v13b", tylko13, NA_BRANZE_V13), (nazwa, stracone, 6 if pop == "v14e" else 4)):
            for q in rng.sample(lst, min(n, len(lst))):
                wynik.append({**q, "grupa": grupa if grupa != "v14" or q["v13"] == "tozsame" else "tylko_v14"})
    return wynik


def opis(u: dict) -> str:
    war = [w.get("label") for w in u.get("warianty") or [] if w.get("label")]
    return (f"{u['nazwa']!r} [kat: {u.get('kategoria') or '—'}] [Booksy: {u.get('zabieg_booksy') or '—'}]"
            f"{' [warianty: ' + '; '.join(war[:6]) + ']' if war else ''}{' [opis: ' + u['opis'][:140] + ']' if u.get('opis') else ''}"
            f" <{u.get('typ_salonu') or '?'}> {u.get('cena_gr', 0) // 100 if u.get('cena_gr') else '?'} zł")


def pokaz() -> None:
    pary = json.loads((OUT / "pary.json").read_text(encoding="utf-8"))
    uslugi = {int(k): v for k, v in json.loads((OUT / "uslugi.json").read_text(encoding="utf-8"))["uslugi"].items()}
    pr = probka(pary)
    (OUT / "probka_oceny.json").write_text(json.dumps(pr, ensure_ascii=False, indent=1), encoding="utf-8")
    for i, q in enumerate(pr):
        d = f" | v14e={q['v14e']}" if "v14e" in q else (f" | bez scalenia={q['v14d']}" if "v14d" in q else "")
        print(f"#{i} [{q['branza']}] {q['grupa']} | v14={q['v14']} ({q['v14_powod']}) | v13b={q['v13']} ({q['v13_powod']}){d}")
        print(f"   A {opis(uslugi[int(q['a'])])}")
        print(f"   B {opis(uslugi[int(q['b'])])}")


def policz() -> None:
    oc = json.loads((OUT / "ocena_claude.json").read_text(encoding="utf-8"))
    per: dict[str, Counter] = defaultdict(Counter)
    for q in oc:
        per[q["branza"]][(q["grupa"], q["ocena_claude"])] += 1
        per["RAZEM"][(q["grupa"], q["ocena_claude"])] += 1
    for br, c in sorted(per.items(), key=lambda kv: (kv[0] == "RAZEM", kv[0])):
        v14 = sum(n for (g, o), n in c.items() if g in ("v14", "tylko_v14"))
        v14_t = sum(n for (g, o), n in c.items() if g in ("v14", "tylko_v14") and o == "T")
        t13 = sum(n for (g, o), n in c.items() if g == "tylko_v13b")
        t13_t = sum(n for (g, o), n in c.items() if g == "tylko_v13b" and o == "T")
        st = sum(n for (g, o), n in c.items() if g.startswith("stracone"))
        st_t = sum(n for (g, o), n in c.items() if g.startswith("stracone") and o == "T")
        extra = f" | stracone względem poprzedniej: trafne {st_t}/{st}" if st else ""
        print(f"{br:<22} v14 „ta sama” trafne {v14_t}/{v14} | tylko v13b „ta sama”: trafne {t13_t}/{t13} (= przeoczone przez v14){extra}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pokaz", action="store_true")
    ap.add_argument("--policz", action="store_true")
    ap.add_argument("--wyjscie", default="v14_sprawdzian", help="katalog sprawdzianu w dane/2026-09-28/")
    a = ap.parse_args()
    OUT = DZ / a.wyjscie
    pokaz() if a.pokaz else policz()
