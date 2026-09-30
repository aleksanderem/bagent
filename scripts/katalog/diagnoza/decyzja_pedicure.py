"""Decyzja Alexa (30.09): „Hybryda na stopy” = „Pedicure hybrydowy” — ta sama usługa. Przepisuje moje oceny par.

Na ślepo oceniałem je jako podobne (pedicure = też opracowanie stóp); Alex rozstrzygnął definicję. Zmieniam TYLKO
pary, w których jedyną różnicą jest ta definicja: hybryda na stopach bez słowa „pedicure” po jednej stronie
i „pedicure” + „hybryda” po drugiej, bez innego dopisku w nazwie (frezowanie, opracowanie podeszwy, „pełny”
zostają podobne — to inny zakres niezależnie od definicji). Idempotentny; wypisuje każdą zmianę.

  python scripts/katalog/diagnoza/decyzja_pedicure.py          # podgląd
  python scripts/katalog/diagnoza/decyzja_pedicure.py --zapisz # zapis ocen
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

B = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(B))
from services.katalog_uslug.normalizacja import bez_polskich_znakow, normalizuj  # noqa: E402

INNY_ZAKRES = ("frezow", "podeszw", "pelny")  # dopiski, które zostawiają parę podobną niezależnie od decyzji
ZBIORY_1042 = B / "scripts" / "typesafe" / "dane" / "2026-09-28"
SPRAWDZIANY = B / "scripts" / "katalog" / "dane" / "2026-09-29"


def _t(nazwa: str) -> str:
    return bez_polskich_znakow(normalizuj(nazwa))


def dotyczy(a: str, b: str) -> bool:
    def stopy(n: str) -> bool:
        s = _t(n)
        return "hybryd" in s and "stop" in s and "pedicur" not in s

    def pedicure(n: str) -> bool:
        s = _t(n)
        return "pedicur" in s and "hybryd" in s and not any(w in s for w in INNY_ZAKRES)

    return (stopy(a) and pedicure(b)) or (stopy(b) and pedicure(a))


def main(zapisz: bool) -> None:
    for f in sorted(ZBIORY_1042.glob("v1[34]_sprawdzian*/ocena_claude.json")):
        us = json.loads((f.parent / "uslugi.json").read_text(encoding="utf-8"))["uslugi"]
        oceny = json.loads(f.read_text(encoding="utf-8"))
        zmian = 0
        for q in oceny:
            a, b = us.get(str(q["a"])), us.get(str(q["b"]))
            if a and b and q["ocena_claude"] != "T" and dotyczy(a["nazwa"], b["nazwa"]):
                print(f"{f.parent.name}: {q['ocena_claude']}→T | {a['nazwa']} || {b['nazwa']}")
                q["ocena_claude"], zmian = "T", zmian + 1
        if zapisz and zmian:
            f.write_text(json.dumps(oceny, ensure_ascii=False, indent=1), encoding="utf-8")
    for d in sorted(SPRAWDZIANY.glob("sprawdzian*")):
        if not (d / "ocena_claude.json").exists():
            continue
        pary = {(q["a"], q["b"]): q for q in json.loads((d / "probka_oceny.json").read_text(encoding="utf-8"))["pary"]}
        oceny = json.loads((d / "ocena_claude.json").read_text(encoding="utf-8"))
        zmian = 0
        for q in oceny:
            p = pary.get((q["a"], q["b"]))
            if p is None or q["ocena"] == "T":
                continue
            na, nb = (f"{p[k]['nazwa']} {p[k]['wariant']}" for k in ("oa", "ob"))
            if dotyczy(na, nb):
                print(f"{d.name}: {q['ocena']}→T | {na} || {nb}")
                q["ocena"], zmian = "T", zmian + 1
        if zapisz and zmian:
            (d / "ocena_claude.json").write_text(json.dumps(oceny, ensure_ascii=False, indent=0), encoding="utf-8")  # jak pokaz_pary


if __name__ == "__main__":
    main("--zapisz" in sys.argv)
