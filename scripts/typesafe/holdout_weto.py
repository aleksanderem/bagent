"""Pomiar weta osi taksonomii na zamrożonym holdoucie par (mig 188).

Liczy to, po co destylacja istnieje: ile par ZŁYCH (powiązane + różne) weto
osi odcina od mediany ceny i ile par TOŻSAMYCH traci. Używa produkcyjnych
funkcji weta (layer_identity.vote_taxonomy_axis, TAXONOMY_VETO_AXES), więc
mierzy dokładnie to, co działa w raporcie — bez kopiowania logiki.

Etykieta pary = label_human, a gdy go nie ma — label_final (zgoda dwóch
sędziów-modeli; spór = 'niepewne', pomijane w metrykach).
Osie dociągane jak w report_pricing._taxonomy_axes: po (branża podmiotu,
nazwa małymi literami z pojedynczymi spacjami).

Źródło osi:
  * domyślnie tabela service_taxonomy (dzisiejsza destylacja, GLM/M3),
  * --osie-plik X.jsonl — wiersze {"branza", "name_key", "osie"}, np. wynik
    destylacji TypeSafe; porównanie obu źródeł na tej samej mierze.

Tylko odczyt. Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/holdout_weto.py --out <katalog> [--osie-plik X.jsonl] [--tag nazwa]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))

from services.similarity_pricing.layer_identity import (  # noqa: E402
    TAXONOMY_VETO_AXES,
    vote_taxonomy_axis,
)
from services.supabase import SupabaseService  # noqa: E402

GOOD = "tozsame"
BAD = ("powiazane", "rozne")
# Hipoteza do wyceny: co by dało dołączenie osi "metoda" do weta (dziś poza
# wetem, bo w destylacji GLM niestabilna — 45% rozjazdu przy powtórce).
WITH_METHOD = (*TAXONOMY_VETO_AXES, "metoda")


def name_key(name: str | None) -> str:
    return " ".join((name or "").lower().split())


def load_holdout(cli) -> list[dict[str, Any]]:
    rows = (
        cli.table("matching_holdout")
        .select("id,branza,subject_name,cand_name,decyzja_silnika,label_final,label_human")
        .execute()
        .data
        or []
    )
    return [{**r, "gold": r.get("label_human") or r.get("label_final")} for r in rows]


def load_axes_db(cli, pairs: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    by_branch: dict[str, set[str]] = defaultdict(set)
    for p in pairs:
        by_branch[p["branza"]] |= {name_key(p["subject_name"]), name_key(p["cand_name"])}
    axes: dict[tuple[str, str], dict[str, Any]] = {}
    for branch, names in by_branch.items():
        keys = sorted(names - {""})
        for i in range(0, len(keys), 40):
            for r in (
                cli.table("service_taxonomy")
                .select("name_key,osie")
                .eq("branza", branch)
                .in_("name_key", keys[i : i + 40])
                .execute()
                .data
                or []
            ):
                axes[(branch, r["name_key"])] = r["osie"] or {}
    return axes


def load_axes_file(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {(r["branza"], r["name_key"]): r["osie"] or {} for r in rows}


def veto(subject_axes: dict | None, cand_axes: dict | None, axes: tuple[str, ...]) -> list[str]:
    s, c = {"_tax": subject_axes or {}}, {"_tax": cand_axes or {}}
    return [a for a in axes if vote_taxonomy_axis(s, c, a) == "against"]


def rates(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    bad = [r for r in rows if r["gold"] in BAD]
    good = [r for r in rows if r["gold"] == GOOD]
    cut = sum(1 for r in bad if r[key])
    lost = sum(1 for r in good if r[key])
    return {
        "zlych": len(bad),
        "zlych_odcietych": cut,
        "odciete_proc": round(100 * cut / len(bad), 1) if bad else None,
        "tozsamych": len(good),
        "tozsamych_straconych": lost,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Weto osi taksonomii na holdoucie (tylko odczyt)")
    p.add_argument("--out", required=True)
    p.add_argument("--osie-plik", default=None)
    p.add_argument("--tag", default="baza")
    args = p.parse_args()

    cli = SupabaseService().client
    pairs = load_holdout(cli)
    axes = load_axes_file(Path(args.osie_plik)) if args.osie_plik else load_axes_db(cli, pairs)

    rows: list[dict[str, Any]] = []
    for pr in pairs:
        sa = axes.get((pr["branza"], name_key(pr["subject_name"])))
        ca = axes.get((pr["branza"], name_key(pr["cand_name"])))
        rows.append({
            **pr,
            "obie_zdestylowane": sa is not None and ca is not None,
            "weto": veto(sa, ca, TAXONOMY_VETO_AXES),
            "weto_z_metoda": veto(sa, ca, WITH_METHOD),
        })

    scored = [r for r in rows if r["gold"] in (GOOD, *BAD)]
    summary = {
        "zrodlo": args.osie_plik or "service_taxonomy",
        "par": len(rows),
        "par_z_etykieta": len(scored),
        "niepewnych_pominietych": sum(1 for r in rows if r["gold"] == "niepewne"),
        "obie_strony_zdestylowane": sum(1 for r in scored if r["obie_zdestylowane"]),
        "weto_dzis": rates(scored, "weto"),
        "weto_z_metoda": rates(scored, "weto_z_metoda"),
        "osie_ktore_ciely": dict(Counter(a for r in scored for a in r["weto"])),
        "per_branza": {
            b: {"dzis": rates(g, "weto"), "z_metoda": rates(g, "weto_z_metoda")}
            for b in sorted({r["branza"] for r in scored})
            for g in [[r for r in scored if r["branza"] == b]]
        },
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"weto_{args.tag}.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8"
    )
    (out / f"weto_{args.tag}_podsumowanie.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
