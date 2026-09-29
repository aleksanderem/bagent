"""Katalog usług, etap 1 — poprzeczka B0 bez modelu: „ta sama” = identyczna nazwa po normalizacji (0 USD).

Na 1042 parach ocenionych przeze mnie według modelu tej samej usługi (7 zbiorów, dane/2026-09-28/*/ocena_claude.json).
Warianty B0: sama nazwa; nazwa bez rodzin wariantów (≥ 2 warianty z etykietą po którejkolwiek stronie).
Liczby surowe (bez ważenia grup) — poprzeczka orientacyjna; ważenie w porównaniu z v14f (etap 1, krok 4).

Użycie: bagent/.venv/bin/python bagent/scripts/katalog/b0_identyczne_nazwy.py [--bledy]
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402

DZ = Path(__file__).resolve().parents[1] / "typesafe" / "dane" / "2026-09-28"
ZBIORY = ["v13_sprawdzian", "v14_sprawdzian", *(f"v14_sprawdzian{i}" for i in range(2, 7))]


def rodzina(u: dict) -> bool:
    return sum(1 for w in u.get("warianty") or [] if (w.get("label") or "").strip()) >= 2


def pary() -> list[dict]:
    wynik = []
    for z in ZBIORY:
        us = {int(k): v for k, v in json.loads((DZ / z / "uslugi.json").read_text(encoding="utf-8"))["uslugi"].items()}
        for q in json.loads((DZ / z / "ocena_claude.json").read_text(encoding="utf-8")):
            a, b = us[int(q["a"])], us[int(q["b"])]
            wynik.append({"zbior": z, "branza": q["branza"], "ocena": q["ocena_claude"], "a": a, "b": b,
                          "ta_sama_nazwa": normalizuj(a["nazwa"]) == normalizuj(b["nazwa"]),
                          "rodzina": rodzina(a) or rodzina(b)})
    return wynik


def main() -> None:
    p = pary()
    wszystkie_t = Counter(q["branza"] for q in p if q["ocena"] == "T")
    for nazwa, war in (("B0 nazwa", lambda q: q["ta_sama_nazwa"]),
                       ("B0 nazwa, bez rodzin wariantów", lambda q: q["ta_sama_nazwa"] and not q["rodzina"])):
        tak = [q for q in p if war(q)]
        per = defaultdict(Counter)
        for q in tak:
            per[q["branza"]][q["ocena"]] += 1
        t = sum(q["ocena"] == "T" for q in tak)
        print(f"\n{nazwa}: „ta sama” {len(tak)}, trafne {t} ({t / max(len(tak), 1):.1%}), "
              f"odzysk {t}/{sum(wszystkie_t.values())} ({t / sum(wszystkie_t.values()):.0%})")
        for br in sorted(wszystkie_t):
            c = per[br]
            n = sum(c.values())
            print(f"  {br:<20} trafne {c['T']:3}/{n:<3} ({c['T'] / n if n else 0:4.0%}) | odzysk {c['T']:3}/{wszystkie_t[br]}")
    if "--bledy" in sys.argv:
        print("\nBłędy B0 (nazwa identyczna, ocena ≠ T):")
        for q in p:
            if q["ta_sama_nazwa"] and q["ocena"] != "T":
                a, b = q["a"], q["b"]
                print(f"  [{q['branza']}] {q['ocena']} rodzina={q['rodzina']} | {a['nazwa']!r} ({a.get('kategoria')}) "
                      f"{[w.get('label') for w in a.get('warianty') or [] if w.get('label')][:4]} "
                      f"opis={(a.get('opis') or '')[:70]!r}\n      vs {b['nazwa']!r} ({b.get('kategoria')}) "
                      f"{[w.get('label') for w in b.get('warianty') or [] if w.get('label')][:4]} opis={(b.get('opis') or '')[:70]!r}")


if __name__ == "__main__":
    main()
