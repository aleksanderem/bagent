"""Za darmo (bez TypeSafe): z czego składa się koszt przejścia drzewa v13b na usługach zbioru 6.

Odtwarza pytania trzech wywołań dokładnie tak, jak wiazka_v13 (poziom 1, poziom 2, poziomy 3–5), z węzłów zapisanych
w sciezki_drzewo_v13b.json, i mierzy długość tekstu wysyłanego w każdym pytaniu. Udziały skaluje do zmierzonych
12 400 tokenów na usługę (TOK_V13).
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

B = Path("/Users/alex/Desktop/MOJE_PROJEKTY/bagent")
sys.path.insert(0, str(B))
from services.typesafe_drzewo import drzewo_v13 as d13  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402

DZ = B / "scripts/typesafe/dane/2026-09-28"
drzewo = json.loads((DZ / "v13/drzewo_v13b.json").read_text())
us = {int(k): v for k, v in json.loads((DZ / "v14_sprawdzian6/uslugi.json").read_text())["uslugi"].items()}
r13 = json.loads((DZ / "v14_sprawdzian6/sciezki_drzewo_v13b.json").read_text())


def dl(x) -> int:
    if hasattr(x, "model_dump"):
        x = x.model_dump(exclude_none=True)
    elif hasattr(x, "__dict__") and not isinstance(x, dict):
        x = {k: v for k, v in vars(x).items() if v is not None}
    return len(json.dumps(x, ensure_ascii=False, default=str))


czesci = defaultdict(list)
opcje = defaultdict(list)
for uid, rek in list(r13.items())[:600]:
    u = us.get(int(uid))
    if not u or not rek:
        continue
    stan = dl(stan_v12(u))
    q1 = d13.pytania_1(drzewo)
    czesci["stan usługi (raz na wywołanie ×3)"].append(stan * 3)
    czesci["poz.1: dziedzina"].append(dl(q1["dziedzina"]))
    czesci["poz.1: pozycja + pytania tak/nie"].append(sum(dl(q) for k, q in q1.items() if k != "dziedzina"))
    dz = []
    for d, g, _p, _r in rek.get("zabiegi", []):
        if d not in dz:
            dz.append(d)
    q2 = [q for d in dz[:3] if (q := d13.pytanie_zabiegu(drzewo, d)) is not None]
    czesci["poz.2: zabieg (do 3 dziedzin)"].append(sum(dl(q) for q in q2))
    opcje["poz.2"].append(sum(len(q.criteria) for q in q2))
    q3 = [q for _d, g, _p, _r in rek.get("zabiegi", [])[:3] for poz in d13.NIZEJ if (q := d13.pytanie_nizej(drzewo, g, poz)) is not None]
    czesci["poz.3–5: metoda / gdzie / etap (do 9 pytań)"].append(sum(dl(q) for q in q3))
    opcje["poz.3–5"].append(sum(len(q.criteria) for q in q3))
    opcje["pytań poz.3–5"].append(len(q3))
    opcje["dziedzin w pytaniu"].append(len(q1["dziedzina"].criteria))

suma = {k: statistics.mean(v) for k, v in czesci.items()}
razem = sum(suma.values())
print(f"usług w próbce: {len(czesci['poz.1: dziedzina'])}")
for k, v in sorted(suma.items(), key=lambda kv: -kv[1]):
    print(f"  {k:<45} {v / razem:5.0%}  ≈ {12400 * v / razem:6.0f} tok.")
for k, v in opcje.items():
    print(f"  średnio {k}: {statistics.mean(v):.1f}")
