"""Runner regresji korpusowej silnika similarity_pricing (REALNE DANE).

CO: uruchamia compute_market_price na ZAMROŻONYM korpusie wejść z produkcji
(250 subjectów, twins@0.82/0.75 + peer_max_sim; build_corpus.py 2026-07-19)
i zapisuje deterministyczny snapshot wyników per subject.

PO CO: twarda bramka regresji na prawdziwych danych. Porównanie snapshotów
dwóch wersji silnika (baseline = origin/main, candidate = working tree)
klasyfikuje każdy subject:
  IDENTICAL — wynik bit-w-bit ten sam (wymagane dla subjectów bez obcych
              bloków — inaczej REGRESJA),
  IMPROVED  — zmiana wyjaśniona nową ochroną (n_coherence_dropped>0 albo
              nowe weto params odnotowane w provenance),
  INVESTIGATE — każda inna zmiana (blokuje merge do wyjaśnienia).

JAK URUCHOMIĆ:
  .venv/bin/python -m services.similarity_pricing.run_corpus_regression \
      --corpus /tmp/regression_corpus.json --out /tmp/snap_after.json
  (baseline: to samo z --repo <ścieżka worktree main>)

Selekcja twins odtwarza logikę report_pricing per subject: primary 0.82,
fallback 0.75 gdy <3 twins (uproszczenie per-subject globalnego triggera
verified_rate<0.20 — udokumentowane; identyczne w obu snapshotach, więc
nie wpływa na diff).

UWAGA: `fixtures/corpus_golden.json` (8 zamrożonych golden case'ów, inny schemat:
{case, subject, twins, expected}) NIE jest wejściem dla tego skryptu — to
fixture dla `test_corpus_golden.py` (pytest, pinned assertions). Podanie go
jako --corpus kończy się teraz kontrolowanym błędem zamiast cichego "skip"
dla wszystkich rekordów.
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--repo", default=None,
                    help="ścieżka repo bagent do importu silnika (default: to repo)")
    args = ap.parse_args()

    if args.repo:
        sys.path.insert(0, args.repo)
    from services.similarity_pricing.engine import compute_market_price  # noqa: WPS433

    corpus = json.load(open(args.corpus))

    # Fail-fast na złym schemacie wejścia (BEAUTY_AUDIT-f3rv): jeśli korpus jest
    # niepusty a ŻADEN rekord nie ma klucza "twins_082", to nie jest korpus
    # build_corpus.py — leciałoby dalej i cicho produkowało sam "skip: no_data"
    # dla wszystkich rekordów (compare_snapshots.py wtedy fałszywie raportuje
    # "IDENTICAL: N", mimo że nic nie zostało policzone). Legalny per-rekord
    # skip (error / twins_082=None w PRAWDZIWYM korpusie) nie jest tu łapany —
    # to sprawdza tylko brak klucza we WSZYSTKICH rekordach naraz.
    if corpus and not any("twins_082" in rec for rec in corpus):
        sys.exit(
            "Zły schemat korpusu: żaden rekord nie ma klucza 'twins_082'.\n"
            "To NIE jest korpus zbudowany przez build_corpus.py "
            "(oczekiwany schemat: {subject, twins_082, twins_075, error?}).\n"
            "Jeśli to fixtures/corpus_golden.json — to wejście dla innego testu, "
            "nie dla tego skryptu. Użyj zamiast tego:\n"
            "  uv run pytest services/similarity_pricing/test_corpus_golden.py\n"
            "(8 pinned golden case'ów)."
        )

    snap = []
    for rec in corpus:
        sub = rec["subject"]
        if rec.get("error") or rec.get("twins_082") is None:
            snap.append({"id": sub["id"], "skip": rec.get("error", "no_data")})
            continue
        twins, sim_used = rec["twins_082"], 0.82
        if len(twins) < 3 and len(rec.get("twins_075") or []) > len(twins):
            twins, sim_used = rec["twins_075"], 0.75
        if not twins:
            snap.append({"id": sub["id"], "skip": "no_twins"})
            continue
        subject = {"service_name": sub["name"], "price_grosze": sub["price_grosze"],
                   "duration_minutes": sub["duration_minutes"],
                   "category_name": sub["category_name"], "is_package": sub["is_package"]}
        r = compute_market_price(subject, twins, None)
        snap.append({
            "id": sub["id"], "name": sub["name"], "sim_used": sim_used,
            "n_in": len(twins),
            "status": r.status,
            "median": r.market_price_grosze,
            "deviation_pct": r.deviation_pct,
            "n_unique_salons": r.n_unique_salons,
            "strictness": r.identity_strictness,
            "kept_ids": sorted(s["service_id"] for s in r.samples),
            "n_coherence_dropped": getattr(r, "n_coherence_dropped", 0),
        })
    json.dump(snap, open(args.out, "w"), ensure_ascii=False)
    done = [s for s in snap if "skip" not in s]
    print(f"snapshot: {len(done)} scored / {len(snap)} subjects → {args.out}")


if __name__ == "__main__":
    main()
