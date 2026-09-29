"""Katalog usług, etap 2 — sprawdzian 7: podpis oferty na NOWYCH salonach (bramka uniwersalności, plan 29.09).

Projekt jak sprawdziany v14 5–6 (porównywalny z ich wynikami v14f): 2 salony podmiotowe na branżę (9 branż), po
3 usługi, kandydaci = do 120 najbardziej podobnych usług konkurencji w promieniu 15 km (szeroka sieć wektorów).
Jednostka = para OFERT (usługa albo wariant z własną ceną — tak liczy raport). Pełnego przebiegu v14f nie ma
(~4 USD, plan zakłada < 1 USD na etapy 1–2); odniesienie: v14f ze sprawdzianów 5–6 i B0 (ta sama nazwa) na
tych samych parach.

Kroki (wznawialne, dane/2026-09-29/sprawdzian7/):
  --zbierz     losowanie salonów (pomija wszystkie dotąd użyte), usługi i kandydaci z bazy prod (odczyt)   0 USD
  --wyciagnij  cechy ofert GLM z abonamentu, paczki po 12; kategorie raz na tekst                         0 USD
  --klasy      klasy różnic jednostronnych i zamian słów → TypeSafe raz na klasę (pamięć wspólna z w2)     grosze
  --probka     werdykty na wszystkich parach, próba warstwowa do mojej oceny → probka_oceny.json          0 USD
  --wynik      po ocenie: trafność i odzysk ważone warstwami, per branża                                  0 USD
Para bez wspólnego słowa (po słowniku synonimów) w nazwach, kategorii i etykiecie Booksy nie może dostać „ta sama”
ani „podobna” — dostaje „inna” bez wyciągania cech kandydata (skrót, nie zmiana werdyktu).
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug.ekstrakcja import Oferta, oferty_z_uslugi  # noqa: E402
from services.katalog_uslug.klasy import NIE_ZMIENIA, WERSJA_PYTANIA, WERSJA_ZAMIANY, rozstrzygnij, zamiana_rownowazna  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa  # noqa: E402
from services.katalog_uslug.podpis import (INNA, PODOBNA, TA_SAMA, Klasy, podpis, porownaj, roznica_do_pytania,  # noqa: E402
                                           slownictwo)


def _modul(nazwa: str, plik: Path):
    spec = importlib.util.spec_from_file_location(nazwa, plik)
    m = importlib.util.module_from_spec(spec)
    sys.modules[nazwa] = m
    spec.loader.exec_module(m)
    return m


tp = _modul("test_paczek", B / "scripts" / "katalog" / "test_paczek.py")
km = _modul("kategorie", B / "scripts" / "katalog" / "kategorie.py")
kr = _modul("klasy_roznic", B / "scripts" / "katalog" / "klasy_roznic.py")

W2 = B / "scripts" / "katalog" / "dane" / "2026-09-29" / "w2"
OUT = B / "scripts" / "katalog" / "dane" / "2026-09-29" / "sprawdzian7"
ZIARNO = 20261007  # inne niż sprawdziany v14 (20261005 i kolejne --ziarno)
RAPORT_TESTOWY = 234429  # salon z raportu testowego 29.09 — oglądany, więc pomijany jako podmiot
NA_WARSTWE = {TA_SAMA: 400, PODOBNA: 150, INNA: 60}  # próba do oceny: „ta sama” prawie w całości (trafność)


def _slownik() -> dict[str, str]:
    return json.loads((W2 / "slownik.json").read_text(encoding="utf-8"))


def zbierz(a: argparse.Namespace) -> None:
    import v13_sprawdzian as v13s
    import v14_sprawdzian as v14s
    from services.supabase import SupabaseService
    OUT.mkdir(parents=True, exist_ok=True)
    plik = OUT / "uslugi.json"
    if plik.exists():
        print("uslugi.json już jest — pomijam losowanie")
        return
    v13s.SEED = a.ziarno
    v13s.uzyte_salony = lambda: v14s.uzyte_salony_v14() | {RAPORT_TESTOWY}
    ns = argparse.Namespace(na_branze=2, uslug=3, kandydatow=150, podobienstwo=0.6)
    uslugi, pary, salony = asyncio.run(v13s.zbierz(SupabaseService(), ns))
    po_a: dict[int, list[dict]] = defaultdict(list)
    for q in pary:
        po_a[q["a"]].append(q)
    pary = [q for lst in po_a.values() for q in sorted(lst, key=lambda q: -q["sim"])[:120]]
    uzyte = {q["a"] for q in pary} | {q["b"] for q in pary}
    uslugi = {i: {**u, "id": i} for i, u in uslugi.items() if i in uzyte}
    plik.write_text(json.dumps({"uslugi": uslugi, "pary": pary, "salony": salony}, ensure_ascii=False), encoding="utf-8")
    print(f"salonów {len(salony)}, usług podmiotu {len(po_a)}, par usług {len(pary)}, usług razem {len(uslugi)}")


def dane() -> tuple[dict[int, dict], list[dict], list]:
    d = json.loads((OUT / "uslugi.json").read_text(encoding="utf-8"))
    return {int(k): {**v, "id": int(k)} for k, v in d["uslugi"].items()}, d["pary"], d["salony"]


def _rdzenie_tekstu(u: dict, s: dict[str, str]) -> set[str]:
    tekst = " ".join([u.get("nazwa") or "", u.get("kategoria") or "", u.get("zabieg_booksy") or "",
                      *((w.get("label") or "") for w in u.get("warianty") or [])])
    return {s.get(r, r) for t in normalizuj(tekst).split() if (r := rdzen_slowa(t))}


def pary_ofert() -> tuple[dict[int, dict], list[dict], dict[str, Oferta]]:
    """Pary usług → pary ofert (każdy wariant z ceną osobno); znacznik `bez_wspolnych` = skrót „inna”."""
    us, pary, _s = dane()
    s = _slownik()
    rdz = {i: _rdzenie_tekstu(u, s) for i, u in us.items()}
    oferty = {o.id: o for u in us.values() for o in oferty_z_uslugi(u)}
    po_uslugi: dict[int, list[str]] = defaultdict(list)
    for o in oferty.values():
        po_uslugi[int(o.id.split("#")[0])].append(o.id)
    wynik = []
    for q in pary:
        wspolne = bool(rdz[q["a"]] & rdz[q["b"]])
        for oa in sorted(po_uslugi[q["a"]]):
            for ob in sorted(po_uslugi[q["b"]]):
                wynik.append({"a": oa, "b": ob, "branza": q["branza"], "salon": q["salon"], "cand_salon": q["cand_salon"],
                              "sim": q["sim"], "bez_wspolnych": not wspolne})
    return us, wynik, oferty


def do_wyciagniecia() -> list[Oferta]:
    _us, pary, oferty = pary_ofert()
    ids = {x for q in pary if not q["bez_wspolnych"] for x in (q["a"], q["b"])}
    return sorted((oferty[i] for i in ids), key=lambda o: o.id)


async def wyciagnij(rownolegle: int) -> None:
    from taxonomy_backfill import KlientGLM
    oferty = do_wyciagniecia()
    klucz = (os.environ.get("ZAI_API_KEY") or tp.KLUCZ.read_text(encoding="utf-8")).strip()
    tp.OUT = OUT
    print(f"ofert do wyciągnięcia {len(oferty)}, paczek {len(tp.paczki(oferty, 'p12'))}", flush=True)
    await tp.przebieg(KlientGLM(klucz, temperature=0.0), "p12", oferty, rownolegle)
    _r, st = tp.rekordy("p12", oferty)
    print("wyciąganie: " + ", ".join(f"{k}: {v}" for k, v in st.items()), flush=True)
    await km.wyciagnij(km.kategorie_ofert(oferty), OUT / "kategorie.json", rownolegle)


def podpisy() -> tuple[list[dict], dict[str, Oferta], dict[str, dict], dict[str, object]]:
    _us, pary, oferty = pary_ofert()
    wyc = do_wyciagniecia()
    tp.OUT = OUT
    rek, _ = tp.rekordy("p12", wyc)
    kon = km.kontekst(wyc, OUT / "kategorie.json")
    s = _slownik()
    slowa = kr.slownictwo_rynku(rek, s)
    pod = {o.id: podpis(rek[o.id], s, kon.get(o.id), slowa) for o in wyc if o.id in rek}
    x_slowa = slowa
    return pary, oferty, rek, {"kon": kon, "pod": pod, "slownik": s, "slowa": x_slowa}


def klasy(budzet: float) -> None:
    pary, oferty, rek, x = podpisy()
    pod, kon, s = x["pod"], x["kon"], x["slownik"]
    kl: dict[str, dict] = {}
    for q in pary:
        pa, pb = pod.get(q["a"]), pod.get(q["b"])
        if q["bez_wspolnych"] or pa is None or pb is None:
            continue
        for klasa, strona in roznica_do_pytania(pa, pb):
            k = json.dumps(klasa, ensure_ascii=False)
            if k in kl:
                kl[k]["par"] += 1
                continue
            o_z, o_bez = (oferty[q["a"]], oferty[q["b"]]) if strona is pa else (oferty[q["b"]], oferty[q["a"]])
            kl[k] = {"klasa": klasa, "par": 1, "oferta": o_z.id,
                     "dopisek": kr._dopisek(rek[o_z.id], klasa[1], set(klasa[2].split()), kon.get(o_z.id), s),
                     "zabieg": kr._nazwa(o_z), "druga": kr._nazwa(o_bez), "stan": kr._stan(o_z)}
    koszt = asyncio.run(kr.zapytaj(kl, budzet))  # pamięć wspólna z w2 — ta sama klasa nie jest pytana drugi raz
    zam = kr.zamiany_ofert([(oferty[q["a"]], oferty[q["b"]]) for q in pary if not q["bez_wspolnych"]], rek, s, kon, x["slowa"])
    koszt_z = asyncio.run(kr.zapytaj_zamiany(zam, budzet))
    print(f"klas {len(kl)}, zamian {len(zam)}; koszt {koszt + koszt_z:.4f} USD")


def werdykty() -> tuple[list[dict], dict[str, Oferta]]:
    pary, oferty, _rek, x = podpisy()
    pod = x["pod"]
    roz = json.loads(kr.PLIK.read_text(encoding="utf-8"))
    zam = json.loads(kr.PLIK_ZAMIAN.read_text(encoding="utf-8")) if kr.PLIK_ZAMIAN.exists() else {}
    kl = Klasy(opisowe={tuple(v["klasa"]) for v in roz.values() if rozstrzygnij(v.get("score")) == NIE_ZMIENIA},
               rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
    for q in pary:
        pa, pb = pod.get(q["a"]), pod.get(q["b"])
        if q["bez_wspolnych"]:
            q["podpis"], q["powod"] = INNA, "bez wspólnych słów (skrót)"
        elif pa is None or pb is None:
            q["podpis"], q["powod"] = PODOBNA, "brak rekordu wyciągania"  # nigdy „ta sama” bez rozkładu
        else:
            q["podpis"], q["powod"] = porownaj(pa, pb, kl)
        q["b0"] = normalizuj(f"{oferty[q['a']].nazwa} {oferty[q['a']].wariant}") == normalizuj(f"{oferty[q['b']].nazwa} {oferty[q['b']].wariant}")
    return pary, oferty


def probka() -> None:
    pary, oferty = werdykty()
    rng = random.Random(ZIARNO + 1)
    warstwy: dict[str, list[dict]] = defaultdict(list)
    for q in pary:
        warstwy[q["podpis"]].append(q)
    wynik = []
    for w, lst in sorted(warstwy.items()):
        wyb = rng.sample(lst, min(NA_WARSTWE[w], len(lst)))
        waga = len(lst) / max(len(wyb), 1)
        wynik += [{**q, "warstwa": w, "waga": round(waga, 4),
                   "oa": {"nazwa": oferty[q["a"]].nazwa, "wariant": oferty[q["a"]].wariant, "kategoria": oferty[q["a"]].kategoria,
                          "opis": oferty[q["a"]].opis, "cena": oferty[q["a"]].cena_zl, "typ": oferty[q["a"]].typ_salonu},
                   "ob": {"nazwa": oferty[q["b"]].nazwa, "wariant": oferty[q["b"]].wariant, "kategoria": oferty[q["b"]].kategoria,
                          "opis": oferty[q["b"]].opis, "cena": oferty[q["b"]].cena_zl, "typ": oferty[q["b"]].typ_salonu}}
                  for q in sorted(wyb, key=lambda q: (q["branza"], q["a"], q["b"]))]
    (OUT / "probka_oceny.json").write_text(json.dumps(wynik, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"par ofert {len(pary)}; warstwy: " + ", ".join(f"{w} {len(l)}" for w, l in sorted(warstwy.items()))
          + f"; do oceny {len(wynik)}")
    print("„ta sama” per branża: " + ", ".join(f"{b} {n}" for b, n in sorted(Counter(q["branza"] for q in warstwy[TA_SAMA]).items())))


def wynik() -> None:
    """Trafność i odzysk ważone warstwami (waga = liczność warstwy / próba), per branża; B0 dla porównania."""
    oc = {(q["a"], q["b"]): q["ocena"] for q in json.loads((OUT / "ocena_claude.json").read_text(encoding="utf-8"))}
    pr = [q for q in json.loads((OUT / "probka_oceny.json").read_text(encoding="utf-8")) if (q["a"], q["b"]) in oc]
    grupy = {"RAZEM": pr, **{b: [q for q in pr if q["branza"] == b] for b in sorted({q["branza"] for q in pr})}}
    out = {}
    for g, z in grupy.items():
        t_all = sum(q["waga"] for q in z if oc[(q["a"], q["b"])] == "T")
        wiersz = {"ocenionych": len(z)}
        for nazwa, klucz in (("podpis", lambda q: q["podpis"] == TA_SAMA), ("B0", lambda q: q["b0"])):
            tak = [q for q in z if klucz(q)]
            n, t = sum(q["waga"] for q in tak), sum(q["waga"] for q in tak if oc[(q["a"], q["b"])] == "T")
            wiersz[nazwa] = {"ta_sama_ocenione": len(tak), "trafnosc": round(t / n, 3) if n else None,
                             "odzysk": round(t / t_all, 3) if t_all else None}
        out[g] = wiersz
        print(f"{g:<20} {json.dumps(wiersz, ensure_ascii=False)}")
    (OUT / "wynik.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    for k in ("zbierz", "wyciagnij", "klasy", "probka", "wynik"):
        ap.add_argument(f"--{k}", action="store_true")
    ap.add_argument("--ziarno", type=int, default=ZIARNO)
    ap.add_argument("--rownolegle", type=int, default=4)
    ap.add_argument("--budzet", type=float, default=0.3)
    ap.add_argument("--licz", action="store_true", help="tylko liczby par i ofert do wyciągnięcia")
    a = ap.parse_args()
    if a.zbierz:
        zbierz(a)
    if a.licz:
        _us, pary, oferty = pary_ofert()
        print(f"par ofert {len(pary)}, bez wspólnych słów {sum(q['bez_wspolnych'] for q in pary)}, "
              f"ofert razem {len(oferty)}, do wyciągnięcia {len(do_wyciagniecia())}")
    if a.wyciagnij:
        asyncio.run(wyciagnij(a.rownolegle))
    if a.klasy:
        klasy(a.budzet)
    if a.probka:
        probka()
    if a.wynik:
        wynik()


if __name__ == "__main__":
    main()
