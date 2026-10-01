"""Pamięć katalogu z pomiarów → wiersze tabel mig 201 (katalog_rozbior, katalog_klasa, katalog_slowo), pliki JSONL.

Bez zapisu w bazie — wgranie to osobny krok po „tak” Alexa. `--sprawdz` odtwarza podpisy i werdykty zbiorów WYŁĄCZNIE
z wierszy po kluczach (jak czytnik raportu na produkcji) i porównuje z werdyktami pomiaru.

  python scripts/katalog/eksport_pamieci.py --do DIR                 # wiersze z wszystkich zbiorów
  python scripts/katalog/eksport_pamieci.py --do DIR --sprawdz sprawdzian12
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "katalog")]
import sprawdzian7 as s7  # noqa: E402
from services.katalog_uslug import klasy as _klasy  # noqa: E402
from services.katalog_uslug.dopasowanie import straznik_ceny  # noqa: E402
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU  # noqa: E402
from services.katalog_uslug.klasy import WERSJA_PYTANIA, WERSJA_ZAMIANY, zamiana_rownowazna  # noqa: E402
from services.katalog_uslug.podpis import INNA, PODOBNA, Klasy, podpis, porownaj, slownictwo, wykonawcy  # noqa: E402
from services.katalog_uslug.wycena import klucz_oferty  # noqa: E402

km, kr, tp = s7.km, s7.kr, s7.tp
ZBIORY = ("sprawdzian7", "sprawdzian8", "sprawdzian9", "sprawdzian10", "sprawdzian11", "sprawdzian12", "raport_279")
MODEL_GLM, MODEL_TS, BUDOWA = "glm-5.3-flash", "jev", "2026-10-01"


def _zbior(z: str):
    s7.OUT = s7.OUT.parent / z
    us, _pary, oferty = s7.pary_ofert()
    tp.OUT = s7.OUT
    rek, _ = tp.rekordy("p12", list(oferty.values()))
    return us, oferty, rek


def wiersze() -> tuple[dict, dict, dict, Counter]:
    """(rodzaj, klucz) → wiersz; pierwszy rozbiór danego tekstu wygrywa (konflikty liczone)."""
    rozbior: dict[tuple[str, str], dict] = {}
    st: Counter = Counter()

    def dodaj(rodzaj: str, klucz: str, rekord: dict, zrodlo: str) -> None:
        k = (rodzaj, klucz)
        if k in rozbior:
            st[f"{rodzaj}: ten sam klucz" + (", inny rekord" if rozbior[k]["rekord"] != rekord else "")] += 1
            return
        rozbior[k] = {"rodzaj": rodzaj, "klucz": klucz, "rekord": rekord, "model": MODEL_GLM,
                      "wersja_promptu": WERSJA_PROMPTU, "zrodlo": zrodlo}

    for z in ZBIORY:
        if not (s7.OUT.parent / z / "p12.json").exists():
            continue
        _us, oferty, rek = _zbior(z)
        for oid, r in rek.items():
            if oid in oferty:
                dodaj("oferta", klucz_oferty(oferty[oid]), r, z)
        for plik, rodzaj in (("kategorie.json", "kategoria"), (km.PLIK_POZYCJI, "pozycja_kategorii"), ("salony.json", "salon")):
            p = s7.OUT / plik
            for k, r in (json.loads(p.read_text(encoding="utf-8")).items() if p.exists() else ()):
                dodaj(rodzaj, k, r, z)
    klasa: dict[tuple[str, str], dict] = {}
    for plik, rodzaj, wersja in ((kr.PLIK, "dopisek", WERSJA_PYTANIA), (kr.PLIK_ZAMIAN, "zamiana", WERSJA_ZAMIANY)):
        for k, v in json.loads(plik.read_text(encoding="utf-8")).items():
            klasa[(rodzaj, k)] = {"rodzaj": rodzaj, "klucz": k, "wpis": v, "wersja_pytania": wersja, "model": MODEL_TS}
    s = s7._slownik()
    rek_rynku = [w["rekord"] for (r, _k), w in rozbior.items() if r == "oferta"]
    cechy, wyk = slownictwo(rek_rynku, s), wykonawcy(rek_rynku, s)
    slowa = {w: {"budowa": BUDOWA, "slowo": w, "forma": s.get(w), "cecha_rynku": w in cechy, "wykonawca": w in wyk}
             for w in set(s) | cechy | wyk}
    return rozbior, klasa, slowa, st


def sprawdz(z: str, rozbior: dict, klasa: dict, slowa: dict) -> Counter:
    """Werdykty z samych wierszy (klucz oferty, kategorii, salonu) vs werdykty pomiaru tego zbioru."""
    us, oferty, _rek = _zbior(z)
    s = {w: v["forma"] for w, v in slowa.items() if v["forma"]}
    cechy = frozenset(w for w, v in slowa.items() if v["cecha_rynku"])
    wyk = frozenset(w for w, v in slowa.items() if v["wykonawca"])
    kl = Klasy(opisowe={tuple(v["wpis"]["klasa"]) for (r, _k), v in klasa.items() if r == "dopisek"
                        and _klasy.klasa_nieistotna(v["wpis"])},
               rownowazne={tuple(v["wpis"]["klasa"]) for (r, _k), v in klasa.items() if r == "zamiana"
                           and zamiana_rownowazna(v["wpis"])})

    def kontekst(o) -> dict | None:
        kid = km.id_kategorii(o.kategoria) if s7.normalizuj(o.kategoria) else None
        r = (rozbior.get(("kategoria", kid)) or {}).get("rekord") if kid else None
        poz = ((rozbior.get(("pozycja_kategorii", kid)) or {}).get("rekord") or {}).get("pozycja", "uslugi") if kid else "uslugi"
        if poz != "uslugi":
            r = {**(r or {"nazwa": o.kategoria, "zabieg": {"fraza": ""}, "cechy": [], "szum": []}), "pozycja_kategorii": poz}
        return r

    def salon(o) -> dict | None:
        n = us[int(o.id.split("#")[0])].get("salon") or ""
        r = (rozbior.get(("salon", km.id_salonu(n))) or {}).get("rekord") if s7.normalizuj(n) else None
        return {**r, "nazwa": km.rozdziel_zlepki(n)} if r else None

    pod = {}
    for oid, o in oferty.items():
        w = rozbior.get(("oferta", klucz_oferty(o)))
        if w:
            pod[oid] = podpis(w["rekord"], s, kontekst(o), cechy, salon(o), wyk)
    pary, _o = s7.werdykty()
    st: Counter = Counter()
    for q in pary:
        pa, pb = pod.get(q["a"]), pod.get(q["b"])
        if q["bez_wspolnych"]:
            w = INNA
        elif pa is None or pb is None:
            w = PODOBNA
        else:
            w = straznik_ceny(*porownaj(pa, pb, kl), oferty[q["a"]].cena_zl, oferty[q["b"]].cena_zl)[0]
        st["zgodne" if w == q["podpis"] else f"różne: pomiar {q['podpis']} / z wierszy {w}"] += 1
    return st


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--do", type=Path, required=True, help="katalog na pliki JSONL (poza repo)")
    ap.add_argument("--sprawdz", nargs="*", default=[], help="zbiory do sprawdzenia werdyktów z samych wierszy")
    a = ap.parse_args()
    rozbior, klasa, slowa, st = wiersze()
    a.do.mkdir(parents=True, exist_ok=True)
    for nazwa, dane in (("katalog_rozbior", rozbior), ("katalog_klasa", klasa), ("katalog_slowo", slowa)):
        with (a.do / f"{nazwa}.jsonl").open("w", encoding="utf-8") as f:
            for w in dane.values():
                f.write(json.dumps(w, ensure_ascii=False) + "\n")
    print("wierszy:", {"rozbior": Counter(r for r, _k in rozbior), "klasa": Counter(r for r, _k in klasa),
                       "slowo": len(slowa)}, "| konflikty:", dict(st))
    for z in a.sprawdz:
        print(z, dict(sprawdz(z, rozbior, klasa, slowa)))


if __name__ == "__main__":
    main()
