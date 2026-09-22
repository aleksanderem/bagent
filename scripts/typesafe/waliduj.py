"""Walidacja weta GLM vs TypeSafe na NOWEJ partii par (nowa_partia.py).

Etykieta pary = zgoda dwóch niezależnych sędziów (spór = 'niepewne', pomijane),
jak w mig 188. Dwa porównania:
* JAKOŚĆ — tylko pary, w których GLM ma destylację po obu stronach
  (czy wartości TypeSafe są lepsze od wartości GLM),
* PRAKTYKA — wszystkie pary; tam, gdzie GLM nie ma destylacji, jego weto
  milczy (tak jak dziś w raporcie), a TypeSafe ma pokrycie pełne.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/waliduj.py <katalog> profile_walid_v2.jsonl
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import weto_profil  # noqa: E402
from holdout_weto import BAD, GOOD, load_axes_db, name_key, veto  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

GLM_PROD = ("obszar", "odbiorca", "etap", "rozmiar", "dlugosc")
TS = {
    "TypeSafe + metoda + objętość": ("obszar", "odbiorca", "etap", "dlugosc", "metoda", "objetosc"),
    "TypeSafe wszystkie cechy": ("obszar", "odbiorca", "etap", "dlugosc", "metoda", "objetosc", "zakres", "pakiet"),
}


def line(pairs: list[dict[str, Any]], cut) -> str:
    bad = [p for p in pairs if p["gold"] in BAD]
    good = [p for p in pairs if p["gold"] == GOOD]
    c, lost = sum(1 for p in bad if cut(p)), sum(1 for p in good if cut(p))
    return f"{c:>3}/{len(bad):<3} ({100 * c / max(len(bad), 1):4.1f}%)   straconych tożsamych {lost}/{len(good)}"


def main() -> None:
    base = Path(sys.argv[1])
    pairs = [json.loads(x) for x in (base / "nowa_partia.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    j1 = json.loads((base / "etykiety_j1.json").read_text(encoding="utf-8"))
    j2 = json.loads((base / "etykiety_j2.json").read_text(encoding="utf-8"))
    for p in pairs:
        a, b = j1.get(p["id"]), j2.get(p["id"])
        p["gold"] = a if a == b else "niepewne"
    agree = sum(1 for p in pairs if p["gold"] != "niepewne")
    print(f"par {len(pairs)}; zgoda sędziów {agree} ({100 * agree / len(pairs):.0f}%); etykiety: {dict(Counter(p['gold'] for p in pairs))}")

    prof = {(r["branza"], r["name_key"]): r["profil"] for r in map(json.loads, (base / sys.argv[2]).read_text(encoding="utf-8").splitlines()) if "profil" in r}
    glm = load_axes_db(SupabaseService().client, pairs)

    def keys(p):
        return (p["branza"], name_key(p["subject_name"])), (p["branza"], name_key(p["cand_name"]))

    cut = {"GLM produkcja (5 osi)": lambda p: bool(veto(glm.get(keys(p)[0]), glm.get(keys(p)[1]), GLM_PROD))}
    for label, axes in TS.items():
        cut[label] = lambda p, axes=axes: bool(weto_profil.veto(prof.get(keys(p)[0]), prof.get(keys(p)[1]), axes))

    scored = [p for p in pairs if p["gold"] in (GOOD, *BAD)]
    covered = [p for p in scored if all(k in glm for k in keys(p))]
    print(f"z etykietą: {len(scored)}; z destylacją GLM po obu stronach: {len(covered)}\n")
    print(f"{'wariant':<32}{'JAKOŚĆ (pary pokryte przez GLM)':<52}PRAKTYKA (wszystkie pary)")
    for label, fn in cut.items():
        print(f"{label:<32}{line(covered, fn):<52}{line(scored, fn)}")

    print("\nper branża — PRAKTYKA (wszystkie pary):")
    for branch in sorted({p["branza"] for p in scored}):
        grp = [p for p in scored if p["branza"] == branch]
        print(f"  {branch}")
        for label in ("GLM produkcja (5 osi)", "TypeSafe wszystkie cechy"):
            print(f"     {label:<32}{line(grp, cut[label])}")


if __name__ == "__main__":
    main()
