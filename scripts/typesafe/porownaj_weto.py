"""GLM (dzisiejsza destylacja) vs TypeSafe (badanie) na zamrożonym holdoucie.

Ta sama miara co holdout_weto.py: ile par złych (powiązane + różne) weto
odcina i ile tożsamych traci — ogółem, na parach przyjmowanych przez silnik
(tam weto realnie działa) i per branża. Opcjonalnie powtarzalność: dwa
przebiegi badania tych samych nazw.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/porownaj_weto.py <katalog> profile_1.jsonl [profile_2.jsonl]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import weto_profil  # noqa: E402
from holdout_weto import BAD, GOOD, load_axes_db, load_holdout, name_key, veto  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

GLM_WARIANTY = {
    "GLM produkcja (5 osi)": ("obszar", "odbiorca", "etap", "rozmiar", "dlugosc"),
    "GLM 4 osie (bez rozmiaru)": ("obszar", "odbiorca", "etap", "dlugosc"),
    "GLM 4 osie + metoda": ("obszar", "odbiorca", "etap", "dlugosc", "metoda"),
}
TS_WARIANTY = {
    "TypeSafe 4 osie": ("obszar", "odbiorca", "etap", "dlugosc"),
    "TypeSafe 4 osie + metoda": ("obszar", "odbiorca", "etap", "dlugosc", "metoda"),
    "TypeSafe + metoda + objętość": ("obszar", "odbiorca", "etap", "dlugosc", "metoda", "objetosc"),
    "TypeSafe wszystkie cechy": ("obszar", "odbiorca", "etap", "dlugosc", "metoda", "objetosc", "zakres", "pakiet"),
}


def load_profiles(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {(r["branza"], r["name_key"]): r["profil"] for r in rows if "profil" in r}


def score(pairs: list[dict[str, Any]], cut) -> str:
    bad = [p for p in pairs if p["gold"] in BAD]
    good = [p for p in pairs if p["gold"] == GOOD]
    c = sum(1 for p in bad if cut(p))
    lost = sum(1 for p in good if cut(p))
    return f"{c:>3}/{len(bad):<3} ({100 * c / max(len(bad), 1):4.1f}%)   straconych tożsamych {lost}/{len(good)}"


def main() -> None:
    base = Path(sys.argv[1])
    prof = load_profiles(base / sys.argv[2])
    cli = SupabaseService().client
    pairs = [p for p in load_holdout(cli) if p["gold"] in (GOOD, *BAD)]
    glm = load_axes_db(cli, pairs)

    def keys(p):
        return (p["branza"], name_key(p["subject_name"])), (p["branza"], name_key(p["cand_name"]))

    cutters = {}
    for label, axes in GLM_WARIANTY.items():
        cutters[label] = lambda p, axes=axes: bool(veto(glm.get(keys(p)[0]), glm.get(keys(p)[1]), axes))
    for label, axes in TS_WARIANTY.items():
        cutters[label] = lambda p, axes=axes: bool(weto_profil.veto(prof.get(keys(p)[0]), prof.get(keys(p)[1]), axes))
    cutters["Hybryda: TypeSafe 4 osie + rozmiar z GLM"] = lambda p: cutters["TypeSafe 4 osie"](p) or bool(
        veto(glm.get(keys(p)[0]), glm.get(keys(p)[1]), ("rozmiar",))
    )

    accepted = [p for p in pairs if p["decyzja_silnika"] == "przyjety"]
    covered = sum(1 for p in pairs if all(k in prof for k in keys(p)))
    print(f"par z etykietą: {len(pairs)} (przyjmowanych przez silnik: {len(accepted)}); obie strony zbadane TypeSafe: {covered}\n")
    print(f"{'wariant':<42}{'WSZYSTKIE: odcięte złe':<48}PRZYJMOWANE PRZEZ SILNIK")
    for label, cut in cutters.items():
        print(f"{label:<42}{score(pairs, cut):<48}{score(accepted, cut)}")

    print("\nper branża (odcięte złe / stracone tożsame, wszystkie pary):")
    for branch in sorted({p["branza"] for p in pairs}):
        grp = [p for p in pairs if p["branza"] == branch]
        print(f"  {branch}")
        for label in ("GLM produkcja (5 osi)", "TypeSafe + metoda + objętość", "TypeSafe wszystkie cechy"):
            print(f"     {label:<42}{score(grp, cutters[label])}")

    if len(sys.argv) > 3:
        prof2 = load_profiles(base / sys.argv[3])
        common = [k for k in prof if k in prof2]
        diffs = []
        same_method = 0
        for k in common:
            a, b = prof[k], prof2[k]
            nums = [abs(a[x] - b[x]) for x in a if isinstance(a.get(x), (int, float)) and isinstance(b.get(x), (int, float))]
            diffs.append(max(nums) if nums else 0.0)
            same_method += a.get("metoda") == b.get("metoda")
        identical = sum(1 for d in diffs if d == 0)
        print(f"\nPOWTARZALNOŚĆ (dwa przebiegi, {len(common)} nazw):")
        print(f"  profile identyczne co do liczby: {identical}/{len(common)}")
        print(f"  największa różnica jakiejkolwiek wartości: {max(diffs):.3f}; mediana największych różnic: {sorted(diffs)[len(diffs) // 2]:.3f}")
        print(f"  ta sama metoda: {same_method}/{len(common)}")
        for label, axes in TS_WARIANTY.items():
            same = sum(
                1 for p in pairs
                if bool(weto_profil.veto(prof.get(keys(p)[0]), prof.get(keys(p)[1]), axes))
                == bool(weto_profil.veto(prof2.get(keys(p)[0]), prof2.get(keys(p)[1]), axes))
            )
            print(f"  ta sama decyzja weta [{label}]: {same}/{len(pairs)}")


if __name__ == "__main__":
    main()
