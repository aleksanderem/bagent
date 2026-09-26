"""Schemat destylacji z danych: branża → rodzaje; rodzaj → cechy → wartości.

Bez udziału człowieka i bez modelu — tylko liczenie na 334 tys. nazw GLM
przełożonych na słownik kanoniczny (slownik_osi.py). Te same reguły dla każdej
branży i każdego rodzaju:
  * rodzaj w branży: co najmniej 1 na 1000 nazw branży (min. 3) — jak w słowniku,
  * cecha rodzaju: podana w ≥ 5% nazw tego rodzaju (rzadziej nie odróżnia
    większości jego usług, więc nie może być warunkiem tożsamości),
  * wartość cechy: co najmniej 3 nazwy tego rodzaju.

Użycie:
  python bagent/scripts/typesafe/schemat_osi.py --dir <katalog słownika>
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

BAGENT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BAGENT_ROOT))

from services.typesafe_drzewo.osie import LICZBOWE  # noqa: E402

MIN_UDZIAL_CECHY = 0.05
MIN_WARTOSC = 3
# Wartość domyślna cechy rodzaju: gdy ≥ 80% usług, które cechę PODAJĄ, ma tę
# samą wartość, brak podania znaczy tę wartość (manicure → paznokcie dłoni).
# 80% = ten sam próg „TAK” co w regułach weta (docs TypeSafe, przykład progu).
DOMYSLNA_UDZIAL = 0.8
# …i co najmniej 20 usług podaje cechę — przy mniejszej próbie 80% to przypadek
# (dolna granica przedziału ufności spada poniżej połowy).
DOMYSLNA_MIN = 20


ZESTAW = ("," , "+", "/", "&")  # wartość z separatorem = zestaw kilku zabiegów, nie rodzaj
RODZAJ_TEZ = 0.3  # krok 1 niepewny: rodzaj_zabiegu z p ≥ 0,3 też trafia na listę rodzajów


def norm(v) -> str:
    return " ".join(str(v).lower().split()).strip(" .,;")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dir", required=True)
    d = Path(p.parse_args().dir)
    przyp = json.loads((d / "krok1_cechy.json").read_text(encoding="utf-8"))
    mapy = {k: v for k, v in json.loads((d / "krok2_kotwice.json").read_text(encoding="utf-8")).items() if not k.startswith("_")}

    def kanon(cecha: str, v: str) -> str | None:
        m = mapy.get(cecha, {})
        if v not in m:
            return v if cecha == "rodzaj_zabiegu" else None
        k = v
        for _ in range(20):
            nxt = m.get(k, {}).get("kotwica", k)
            if nxt == k:
                break
            k = nxt
        return k

    nazw_b: Counter = Counter()
    rodz_b: dict[str, Counter] = defaultdict(Counter)
    cechy_r: dict[str, Counter] = defaultdict(Counter)
    wart_rc: dict[tuple[str, str], Counter] = defaultdict(Counter)
    nazw_r: Counter = Counter()
    for b, _n, o in csv.reader(gzip.open(d / "taksonomia.csv.gz", "rt")):
        nazw_b[b] += 1
        try:
            osie = json.loads(o)
        except ValueError:
            continue
        pary = []
        for k, v in osie.items():
            for x in (v if isinstance(v, list) else [v]):
                if x in (None, "", []):
                    continue
                wpis = przyp.get(f"{k.lower()}|{norm(x)}", {})
                c = wpis.get("cecha")
                if c and c != "nie_cecha":
                    pary.append((c, norm(x)))
                if c != "rodzaj_zabiegu" and any(a == "rodzaj_zabiegu" and p >= RODZAJ_TEZ for a, p in wpis.get("top", [])):
                    pary.append(("rodzaj_zabiegu", norm(x)))
        rodzaje = {kanon("rodzaj_zabiegu", v) for c, v in pary if c == "rodzaj_zabiegu"} - {None}
        rodzaje = {r for r in rodzaje if not any(z in r for z in ZESTAW) and not r.startswith("tak ")}
        for r in rodzaje:
            rodz_b[b][r] += 1
            nazw_r[r] += 1
            for c in {c for c, _ in pary if c != "rodzaj_zabiegu"}:
                cechy_r[r][c] += 1
            for c, v in pary:
                if c == "rodzaj_zabiegu":
                    continue
                kv = v if c in LICZBOWE else kanon(c, v)
                if kv:
                    wart_rc[(r, c)][kv] += 1

    branze = {}
    for b, c in rodz_b.items():
        prog = max(3, nazw_b[b] // 1000)
        branze[b] = sorted((r for r, n in c.items() if n >= prog), key=lambda r: -c[r])
    rodzaje_all = {r for rs in branze.values() for r in rs}
    schemat = {}
    for r in rodzaje_all:
        cechy = {}
        for c, n in cechy_r[r].items():
            if n / nazw_r[r] < MIN_UDZIAL_CECHY:
                continue
            licz = wart_rc[(r, c)]
            wart = [v for v, m in licz.most_common() if m >= MIN_WARTOSC]
            if c in LICZBOWE or wart:
                razem = sum(licz.values())
                dom = wart[0] if wart and c not in LICZBOWE and razem >= DOMYSLNA_MIN and licz[wart[0]] / razem >= DOMYSLNA_UDZIAL else None
                cechy[c] = {"udzial": round(n / nazw_r[r], 3), "wartosci": [] if c in LICZBOWE else wart,
                            "domyslna": dom}
        schemat[r] = {"n": nazw_r[r], "cechy": cechy}
    rodzic = {v: w.get("rodzic") for v, w in mapy.get("rodzaj_zabiegu", {}).items() if w.get("rodzic")}
    (d / "schemat.json").write_text(json.dumps({"branze": branze, "rodzaje": schemat, "rodzice": rodzic}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"branż {len(branze)}, rodzajów {len(schemat)}; rodzajów na branżę: "
          + ", ".join(f"{b}={len(r)}" for b, r in sorted(branze.items(), key=lambda kv: -len(kv[1]))[:10]))
    for r in ("depilacja laserowa", "manicure hybrydowy", "przedłużanie rzęs", "strzyżenie", "mezoterapia igłowa"):
        s = schemat.get(r)
        if s:
            print(f"\n{r} (n={s['n']}):")
            for c, v in sorted(s["cechy"].items(), key=lambda kv: -kv[1]["udzial"]):
                print(f"   {c:<20} {v['udzial']*100:>4.0f}%  {', '.join(v['wartosci'][:10])}")


if __name__ == "__main__":
    main()
