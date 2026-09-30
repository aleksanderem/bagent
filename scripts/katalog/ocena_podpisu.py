"""Katalog usług, etap 1 — podpis oferty na moich ocenionych parach (0 USD: liczy z pamięci wyciągania).

Para jest oceniona na poziomie usługi; rodzinę wariantów reprezentuje wariant, którego cenę pokazuje ogłoszenie
(pierwszy — tak oceniałem w zbiorach 4–6). Porównanie z v14f i z B0 (identyczna nazwa) na TYCH SAMYCH parach:
zbiory 3–6, gdzie v14f policzono (pary_v14f_w2.json; w zbiorze 6 pole v14 w pary.json). Liczby surowe (bez wag
grup) — porównanie wersji na tej samej próbce.

Użycie: bagent/.venv/bin/python bagent/scripts/katalog/ocena_podpisu.py --wariant p12 [--bledy 40]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts")]
from services.katalog_uslug import klasy as _klasy  # noqa: E402
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU, oferty_z_uslugi  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402
from services.katalog_uslug.podpis import TA_SAMA, Klasy, podpis, porownaj  # noqa: E402
from services.katalog_uslug.dopasowanie import straznik_ceny  # noqa: E402
from services.katalog_uslug import podpis as _podpis_mod  # noqa: E402

_spec = importlib.util.spec_from_file_location("test_paczek", B / "scripts" / "katalog" / "test_paczek.py")
tp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tp)

DZ = B / "scripts" / "typesafe" / "dane" / "2026-09-28"
V14F = {"v14_sprawdzian3": ("pary_v14f_w2.json", "v14f"), "v14_sprawdzian4": ("pary_v14f_w2.json", "v14f"),
        "v14_sprawdzian5": ("pary_v14f_w2.json", "v14f"), "v14_sprawdzian6": ("pary.json", "v14")}


def pary_ocenione() -> list[dict]:
    wynik = []
    for z in tp.ZBIORY:
        us = {int(k): v for k, v in json.loads((DZ / z / "uslugi.json").read_text(encoding="utf-8"))["uslugi"].items()}
        v14f = {}
        if z in V14F:
            plik, pole = V14F[z]
            v14f = {(int(q["a"]), int(q["b"])): q.get(pole) for q in json.loads((DZ / z / plik).read_text(encoding="utf-8"))}
        for q in json.loads((DZ / z / "ocena_claude.json").read_text(encoding="utf-8")):
            a, b = us[int(q["a"])], us[int(q["b"])]
            wynik.append({"zbior": z, "branza": q["branza"], "ocena": q["ocena_claude"],
                          "oa": oferty_z_uslugi(a)[0], "ob": oferty_z_uslugi(b)[0],
                          "v14f": v14f.get((int(q["a"]), int(q["b"]))) if z in V14F else None,
                          "b0": normalizuj(a["nazwa"]) == normalizuj(b["nazwa"])})
    return wynik


def rekordy_rynku(rek: dict[str, dict]) -> dict[str, dict]:
    """WSZYSTKIE znane rekordy wyciągania (zbiór par, raporty, sprawdziany) — przybliżenie rynku, bez moich ocen."""
    wszystkie = dict(rek)
    for plik in (B / "scripts" / "katalog" / "dane" / "2026-09-29").glob("**/p12.json"):
        if "paczki" in plik.parts:
            continue
        for v in json.loads(plik.read_text(encoding="utf-8")).values():
            for r in (v.get("odp") or {}).get("oferty") or [] if isinstance(v, dict) else []:
                if isinstance(r, dict):
                    wszystkie.setdefault(f"{plik.parent.name}:{r.get('id')}", r)
    return wszystkie


def slownictwo_rynku(rek: dict[str, dict], slownik: dict | None) -> frozenset[str]:
    return _podpis_mod.slownictwo(rekordy_rynku(rek).values(), slownik)


def metody_rynku(rek: dict[str, dict], slownik: dict | None) -> frozenset[str]:
    return _podpis_mod.metody_rynku(rekordy_rynku(rek).values(), slownik)


def metryki(pary: list[dict], klucz) -> tuple[int, int, int, int]:
    tak = [q for q in pary if klucz(q)]
    t = sum(q["ocena"] == "T" for q in tak)
    return len(tak), t, sum(q["ocena"] == "T" for q in pary), len(pary)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wariant", default="p12")
    ap.add_argument("--bledy", type=int, default=0)
    ap.add_argument("--klasy", action="store_true", help="użyj rozstrzygnięć klas różnic (klasy_roznic.py)")
    ap.add_argument("--slownik", action="store_true", help="użyj słownika synonimów (synonimy.py)")
    ap.add_argument("--kategorie", action="store_true", help="kontekst z rozkładu kategorii (kategorie.py)")
    a = ap.parse_args()
    tp.OUT = tp.OUT.parent / f"w{WERSJA_PROMPTU}" / "wszystkie"
    pary = pary_ocenione()
    oferty = {o.id: o for q in pary for o in (q["oa"], q["ob"])}
    rek, stat = tp.rekordy(a.wariant, tp.oferty_probki(0))  # walidacja wobec WSZYSTKICH ofert paczek (też wariantów)
    print(f"ofert w parach {len(oferty)}, z rekordem {len(rek)}; " + ", ".join(f"{k}: {v}" for k, v in stat.items()))
    slownik = {}
    if a.slownik:
        slownik = json.loads((B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}" / "slownik.json").read_text(encoding="utf-8"))
        print(f"słownik synonimów: {len(slownik)} scaleń")
    kon: dict = {}
    if a.kategorie:
        kat_mod = importlib.util.spec_from_file_location("kategorie", B / "scripts" / "katalog" / "kategorie.py")
        km = importlib.util.module_from_spec(kat_mod); kat_mod.loader.exec_module(km)
        kon = km.kontekst(tp.oferty_probki(0), B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}" / "kategorie.json")
        print(f"kontekst kategorii dla {sum(v is not None for v in kon.values())} ofert")
    klasy = Klasy()
    if a.klasy:  # rozstrzygnięcia TypeSafe raz na klasę (klasy_roznic.py): nieistotna = „nie zmienia”
        from services.katalog_uslug.klasy import NIE_ZMIENIA, WERSJA_PYTANIA, rozstrzygnij
        plik = B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}" / f"klasy_p{WERSJA_PYTANIA}.json"
        rozstrz = json.loads(plik.read_text(encoding="utf-8"))
        from services.katalog_uslug.klasy import WERSJA_ZAMIANY, zamiana_rownowazna
        pz = plik.parent / f"zamiany_p{WERSJA_ZAMIANY}.json"
        zam = json.loads(pz.read_text(encoding="utf-8")) if pz.exists() else {}
        klasy = Klasy(opisowe={tuple(v["klasa"]) for v in rozstrz.values() if _klasy.klasa_nieistotna(v)},
                      rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
        print(f"klasy nieistotne (TypeSafe „nie zmienia”): {len(set(klasy.opisowe))} z {len(rozstrz)}; "
              f"zamiany słów „to samo”: {len(set(klasy.rownowazne))} z {len(zam)}")
    slowa = slownictwo_rynku(rek, slownik)
    for q in pary:
        ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
        q["podpis"], q["powod"] = (straznik_ceny(*porownaj(podpis(ra, slownik, kon.get(q["oa"].id), slowa),
                                                          podpis(rb, slownik, kon.get(q["ob"].id), slowa), klasy),
                                                  q["oa"].cena_zl, q["ob"].cena_zl)  # cena ≥ 5× → nie „ta sama” (Alex 30.09)
                                    if ra and rb else ("brak", "brak rekordu"))
    for nazwa, zbior in (("wszystkie 7 zbiorów", pary), ("zbiory 3–6 (z v14f)", [q for q in pary if q["zbior"] in V14F])):
        print(f"\n{nazwa}: par {len(zbior)}")
        for wersja, klucz in (("podpis", lambda q: q["podpis"] == TA_SAMA), ("B0 nazwa", lambda q: q["b0"]),
                              ("v14f", lambda q: q["v14f"] == "tozsame")):
            if wersja == "v14f" and zbior is pary:
                continue
            n, t, wszystkie_t, _ = metryki(zbior, klucz)
            print(f"  {wersja:<9} „ta sama” {n:4}, trafne {t:4} ({t / max(n, 1):5.1%}), odzysk {t}/{wszystkie_t} "
                  f"({t / max(wszystkie_t, 1):.0%})")
        per = defaultdict(list)
        for q in zbior:
            per[q["branza"]].append(q)
        for br in sorted(per):
            s = metryki(per[br], lambda q: q["podpis"] == TA_SAMA)
            v = metryki(per[br], lambda q: q["v14f"] == "tozsame") if zbior is not pary else None
            print(f"    {br:<20} podpis {s[1]}/{s[0]} trafnych, odzysk {s[1]}/{s[2]}"
                  + (f" | v14f {v[1]}/{v[0]}, odzysk {v[1]}/{v[2]}" if v else ""))
    przyczyny = Counter(q["powod"].split(":")[0] for q in pary if q["ocena"] == "T" and q["podpis"] != TA_SAMA)
    print("\nDlaczego podpis gubi prawdziwe pary (T):", dict(przyczyny.most_common(12)))
    if a.bledy:
        print("\nBłędne „ta sama” (ocena ≠ T):")
        for q in [q for q in pary if q["podpis"] == TA_SAMA and q["ocena"] != "T"][: a.bledy]:
            print(f"  [{q['branza']}] {q['ocena']} | {q['oa'].nazwa!r} / {q['oa'].wariant!r} ({q['oa'].kategoria}) "
                  f"vs {q['ob'].nazwa!r} / {q['ob'].wariant!r} ({q['ob'].kategoria})")
        print("\nZgubione T (przykłady):")
        for q in [q for q in pary if q["ocena"] == "T" and q["podpis"] != TA_SAMA][: a.bledy]:
            print(f"  [{q['branza']}] {q['powod'][:90]} | {q['oa'].nazwa!r} vs {q['ob'].nazwa!r}")


if __name__ == "__main__":
    main()
