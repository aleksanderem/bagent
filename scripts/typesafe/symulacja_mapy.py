"""Czy przycięcie pytań mapą (mapa_pytan.json) zmienia decyzje weta?

Pytania w jednym wywołaniu są oceniane niezależnie (docs: cookbooks/
parallel_questions), więc wycięcie jednych nie zmienia odpowiedzi na pozostałe.
Symulacja: z zebranych profili usuwamy odpowiedzi na pytania spoza mapy i
liczymy weto ponownie — bez nowych wywołań. Prawdziwy przebieg z mapą
(destylacja_badanie.py --mapa) potwierdza to i mierzy tokeny.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/symulacja_mapy.py <katalog>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import weto_profil  # noqa: E402
from holdout_weto import BAD, GOOD, load_holdout, name_key  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

WSZYSTKIE = ("obszar", "odbiorca", "etap", "dlugosc", "metoda", "objetosc", "zakres", "pakiet")
MAPA = json.loads((Path(__file__).resolve().parents[2] / "services" / "typesafe_profile" / "mapa_pytan.json").read_text(encoding="utf-8"))


def prune(profile: dict[str, Any] | None, branch: str) -> dict[str, Any] | None:
    if profile is None or branch not in MAPA:
        return profile
    allowed = set(MAPA[branch])
    return {k: v for k, v in profile.items() if k.split("#")[0] in allowed}


def load_profiles(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    return {(r["branza"], r["name_key"]): r["profil"] for r in map(json.loads, path.read_text(encoding="utf-8").splitlines()) if "profil" in r}


def compare(label: str, pairs: list[dict[str, Any]], prof: dict[tuple[str, str], dict[str, Any]]) -> None:
    scored = [p for p in pairs if p["gold"] in (GOOD, *BAD)]
    changed = []
    stats = {"przed": [0, 0], "po": [0, 0]}
    for p in scored:
        ka, kb = (p["branza"], name_key(p["subject_name"])), (p["branza"], name_key(p["cand_name"]))
        before = weto_profil.veto(prof.get(ka), prof.get(kb), WSZYSTKIE)
        after = weto_profil.veto(prune(prof.get(ka), p["branza"]), prune(prof.get(kb), p["branza"]), WSZYSTKIE)
        for tag, v in (("przed", before), ("po", after)):
            if v and p["gold"] in BAD:
                stats[tag][0] += 1
            if v and p["gold"] == GOOD:
                stats[tag][1] += 1
        if bool(before) != bool(after):
            changed.append((p, before, after))
    n_bad = sum(p["gold"] in BAD for p in scored)
    n_good = sum(p["gold"] == GOOD for p in scored)
    print(f"{label}: par {len(scored)}, zmienionych decyzji {len(changed)}")
    for tag in ("przed", "po"):
        c, lost = stats[tag]
        print(f"   {tag:<6} odcięte złe {c}/{n_bad} ({100 * c / n_bad:.1f}%), stracone tożsame {lost}/{n_good}")
    for p, b, a in changed[:8]:
        print(f"   ZMIANA [{p['branza']}] {p['gold']}: {p['subject_name'][:35]} ↔ {p['cand_name'][:35]}  weto {b} → {a}")


def main() -> None:
    base = Path(sys.argv[1])
    holdout = load_holdout(SupabaseService().client)
    compare("HOLDOUT (563 par)", holdout, load_profiles(base / "profile_v2_1.jsonl"))
    fresh = [json.loads(x) for x in (base / "nowa_partia.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    j1 = json.loads((base / "etykiety_j1.json").read_text(encoding="utf-8"))
    j2 = json.loads((base / "etykiety_j2.json").read_text(encoding="utf-8"))
    for p in fresh:
        p["gold"] = j1[p["id"]] if j1[p["id"]] == j2[p["id"]] else "niepewne"
    compare("NOWA PARTIA (320 par)", fresh, load_profiles(base / "profile_walid_v2.jsonl"))


if __name__ == "__main__":
    main()
