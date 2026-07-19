"""Bramka regresji: porównanie snapshotów korpusu (baseline vs candidate).

Reguły klasyfikacji per subject (patrz run_corpus_regression.py):
  IDENTICAL   — pełna zgodność (status, mediana, kept_ids).
  IMPROVED    — candidate ma n_coherence_dropped>0 LUB kept schudł przy
                niezmienionym/zmienionym medianie — czyli zmiana wynika z
                NOWEJ ochrony; wymagane: kept_ids(candidate) ⊆ kept_ids(baseline)
                (nowa ochrona może tylko ODEJMOWAĆ sample, nigdy dodawać).
  INVESTIGATE — wszystko inne (dodane sample, zmiana bez ochrony) = blokada.

Exit code 1 gdy jakikolwiek INVESTIGATE — do użycia w CI/skryptach.
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--show", type=int, default=12)
    args = ap.parse_args()

    base = {s["id"]: s for s in json.load(open(args.baseline))}
    cand = {s["id"]: s for s in json.load(open(args.candidate))}
    assert set(base) == set(cand), "snapshoty z różnych korpusów"

    identical, improved, investigate = [], [], []
    for sid, b in base.items():
        c = cand[sid]
        if "skip" in b or "skip" in c:
            if b.get("skip") == c.get("skip"):
                identical.append(sid)
            else:
                investigate.append((sid, "skip-mismatch", b, c))
            continue
        same = (b["status"] == c["status"] and b["median"] == c["median"]
                and b["kept_ids"] == c["kept_ids"])
        if same:
            identical.append(sid)
            continue
        subset = set(c["kept_ids"]) <= set(b["kept_ids"])
        protected = c.get("n_coherence_dropped", 0) > 0 or len(c["kept_ids"]) < len(b["kept_ids"])
        if subset and protected:
            improved.append((sid, b, c))
        else:
            investigate.append((sid, "unexplained-change", b, c))

    print(f"IDENTICAL:   {len(identical)}")
    print(f"IMPROVED:    {len(improved)}   (nowa ochrona odjęła obce sample)")
    print(f"INVESTIGATE: {len(investigate)}  (blokada gdy >0)")
    for sid, b, c in improved[:args.show]:
        print(f"  ~ {c['name'][:44]:44} kept {len(b['kept_ids'])}→{len(c['kept_ids'])} "
              f"coh_drop={c.get('n_coherence_dropped',0)} median {b['median']}→{c['median']} "
              f"status {b['status']}→{c['status']}")
    for sid, why, b, c in investigate[:args.show]:
        print(f"  ! {why}: {c.get('name', sid)}")
        print(f"      base: status={b.get('status')} median={b.get('median')} kept={len(b.get('kept_ids', []))}")
        print(f"      cand: status={c.get('status')} median={c.get('median')} kept={len(c.get('kept_ids', []))} coh={c.get('n_coherence_dropped')}")
    sys.exit(1 if investigate else 0)


if __name__ == "__main__":
    main()
