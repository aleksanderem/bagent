"""Zestawia decyzje kilku przebiegów proba_pass5.py grupa po grupie.

Łączy po kluczu grupy (marka, metoda, okolice), nie po numerze — numer grupy
zależy od kolejności danych. Wypisuje wzorzec (kotwica gpt-4o) i wybór każdego
przebiegu, a dla v2 wartości, które zadecydowały.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/porownaj.py <katalog> 250 v1:250 pl:250_v2_pl en:250_v2_en
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[tuple, dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return {(r["marka"], r["metoda"], tuple(r["okolice"])): r for r in rows}


def short(text: str | None, n: int = 42) -> str:
    return (text or "-")[:n]


def reference_name(r: dict[str, Any]) -> str:
    ref = r.get("wzorzec")
    if ref in (None, "brak"):
        return "-"
    if ref == "wlasna_kategoria":
        return f"WŁASNA: {r.get('wzorzec_nazwa') or ''}"
    return r["kandydaci"].get(ref, f"SPOZA LISTY {ref}")


def v2_reason(r: dict[str, Any]) -> str:
    win = next((c for c in r.get("top3_rankingu", []) if c["klucz"] == r["wybor"]), None)
    if r["wybor"] == "wlasna_kategoria":
        return f"kwalifikujących {r.get('kwalifikujacych')}, pewność {r['pewnosc']}"
    if win is None:
        return f"pasuje {r['pewnosc']} (poza top3 rankingu)"
    return f"pasuje {win['pasuje']:.2f} metoda {win['konflikt_metody']} okolica {win['konflikt_okolicy']}"


def main() -> None:
    base, runs = Path(sys.argv[1]), [a.split(":", 1) for a in sys.argv[3:]]
    data = {label: load(base / f"wiersze_{tag}.jsonl") for label, tag in runs}
    first = data[runs[0][0]]
    for key, r0 in sorted(first.items(), key=lambda kv: kv[1]["grupa"]):
        print(f"#{r0['grupa']} [{key[0] or '-'} | {key[1]} | {','.join(key[2]) or '-'}]")
        print(f"   usługi: {'; '.join(n for n in r0['przyklady'][:3] if n)[:140]}")
        print(f"   gpt-4o : {short(reference_name(r0), 60)}")
        for label, _ in runs:
            r = data[label].get(key)
            if r is None:
                print(f"   {label:<7}: (brak w przebiegu)")
                continue
            mark = "=" if r.get("werdykt") == "zgodne" else "≠"
            reason = f"   [{v2_reason(r)}]" if "top3_rankingu" in r else ""
            print(f"   {label:<7}{mark} {short(r['wybor_nazwa'])}{reason}")


if __name__ == "__main__":
    main()
