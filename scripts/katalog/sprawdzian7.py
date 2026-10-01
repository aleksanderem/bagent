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
from services.katalog_uslug import klasy as _klasy  # noqa: E402
from services.katalog_uslug.ekstrakcja import Oferta, oferty_z_uslugi  # noqa: E402
from services.katalog_uslug.klasy import NIE_ZMIENIA, WERSJA_PYTANIA, WERSJA_ZAMIANY, rozstrzygnij, zamiana_rownowazna  # noqa: E402
from services.katalog_uslug.dopasowanie import straznik_ceny  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa  # noqa: E402
from services.katalog_uslug.podpis import (INNA, PODOBNA, TA_SAMA, Klasy, klasy_do_rozstrzygniecia, podpis,  # noqa: E402
                                           porownaj, roznica_do_pytania,
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


def _slownik() -> dict[str, str]:
    """Słownik synonimów z całego znanego rynku (synonimy.py --rynek, po moim przeglądzie); zbiór par nie znał piercingu."""
    return json.loads((W2 / "slownik_rynek.json").read_text(encoding="utf-8"))


def zbierz(a: argparse.Namespace) -> None:
    """Losowanie nowych salonów; kopia usług do dane/2026-09-28/v14_<wyjście>/, żeby kolejne sprawdziany je pomijały."""
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
    ns = argparse.Namespace(na_branze=2, uslug=a.uslug, kandydatow=150, podobienstwo=0.6)
    uslugi, pary, salony = asyncio.run(v13s.zbierz(SupabaseService(), ns))
    po_a: dict[int, list[dict]] = defaultdict(list)
    for q in pary:
        po_a[q["a"]].append(q)
    pary = [q for lst in po_a.values() for q in sorted(lst, key=lambda q: -q["sim"])[:120]]
    uzyte = {q["a"] for q in pary} | {q["b"] for q in pary}
    uslugi = {i: {**u, "id": i} for i, u in uslugi.items() if i in uzyte}
    plik.write_text(json.dumps({"uslugi": uslugi, "pary": pary, "salony": salony}, ensure_ascii=False), encoding="utf-8")
    kopia = B / "scripts" / "typesafe" / "dane" / "2026-09-28" / f"v14_{OUT.name}"
    kopia.mkdir(parents=True, exist_ok=True)
    if not (kopia / "uslugi.json").exists():
        (kopia / "uslugi.json").write_text(plik.read_text(encoding="utf-8"), encoding="utf-8")
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


def salon_ofert(us: dict[int, dict], oferty: dict[str, Oferta]) -> dict[str, str]:
    return {oid: us[int(oid.split("#")[0])].get("salon") or "" for oid in oferty}


async def wyciagnij_salony(rownolegle: int) -> None:
    from services.katalog_uslug.ekstrakcja import prompt_salonow
    us, _pary, oferty = pary_ofert()
    nazwy = sorted(set(salon_ofert(us, oferty).values()))
    await km.wyciagnij(km.salony_ofert(nazwy), OUT / "salony.json", rownolegle, prompt=prompt_salonow)


def podpisy() -> tuple[list[dict], dict[str, Oferta], dict[str, dict], dict[str, object]]:
    us, pary, oferty = pary_ofert()
    wyc = do_wyciagniecia()
    tp.OUT = OUT
    # Paczka z pamięci jest walidowana w całości (liczba rekordów = liczba ofert paczki): gdy oferta wypadła z bieżącej
    # listy do rozbioru, cała paczka przepadała (1.10: łączenie „20 ml” zmieniło, które pary mają wspólne słowa, i w teście
    # E zniknęły rekordy ~990 par). Walidacja na WSZYSTKICH ofertach zbioru — rekord jest przypisany po id i nazwie.
    rek, _ = tp.rekordy("p12", list(oferty.values()))
    if os.environ.get("KATALOG_ROZBIOR") == "dzial":  # rozbiór działu (dzialy.py, 1.10) zamiast p12 + reguł kontekstu
        dz = _modul("dzialy", B / "scripts" / "katalog" / "dzialy.py")
        rek, _ = dz.rekordy(OUT / dz.PLIK, oferty, salon_ofert(us, oferty))
    elif os.environ.get("KATALOG_ROZBIOR") == "stosowalnosc":  # p12 + kontekst rozstrzygnięty przez TypeSafe (1.10)
        from types import SimpleNamespace
        st = _modul("stosowalnosc", B / "scripts" / "katalog" / "stosowalnosc.py")
        rek = st.rekordy(SimpleNamespace(OUT=OUT, pary_ofert=pary_ofert, do_wyciagniecia=do_wyciagniecia, tp=tp, km=km,
                                         salon_ofert=salon_ofert, _slownik=_slownik))
    kon = km.kontekst(wyc, OUT / "kategorie.json")
    s = _slownik()
    slowa = kr.slownictwo_rynku(rek, s)
    sal = km.kontekst_salonu(salon_ofert(us, oferty), OUT / "salony.json") if (OUT / "salony.json").exists() else {}
    wyk = kr.wykonawcy_rynku(rek, s)
    pod = {o.id: podpis(rek[o.id], s, kon.get(o.id), slowa, sal.get(o.id), wyk) for o in wyc if o.id in rek}
    return pary, oferty, rek, {"kon": kon, "pod": pod, "slownik": s, "slowa": slowa, "sal": sal, "wyk": wyk}


def klasy(budzet: float, proba: int = 0) -> None:
    pary, oferty, rek, x = podpisy()
    pod, kon, s = x["pod"], x["kon"], x["slownik"]
    kl: dict[str, dict] = {}

    def przyklad(z: str, bez: str, klasa) -> dict:
        return {"oferta": z, "dopisek": kr._dopisek(rek[z], klasa[1], set(klasa[2].split()), kon.get(z), s),
                "zabieg": kr._nazwa(oferty[z]), "druga": kr._nazwa(oferty[bez]), "stan": kr._stan(oferty[z])}

    for q in pary:
        pa, pb = pod.get(q["a"]), pod.get(q["b"])
        if q["bez_wspolnych"] or pa is None or pb is None:
            continue
        for klasa, strona in roznica_do_pytania(pa, pb):
            o_z, o_bez = (oferty[q["a"]], oferty[q["b"]]) if strona is pa else (oferty[q["b"]], oferty[q["a"]])
            kr.dodaj_przyklad(kl, klasa, o_bez, s, lambda: przyklad(o_z.id, o_bez.id, klasa))
    potrzebne = {json.dumps(k, ensure_ascii=False) for q in pary if not q["bez_wspolnych"] and q["a"] in pod and q["b"] in pod
                 for k in klasy_do_rozstrzygniecia(pod[q["a"]], pod[q["b"]])}
    us, _p, _o = pary_ofert()
    z_puli = kr.przyklady_z_puli(kl, potrzebne, pod, oferty, salon_ofert(us, oferty), s, przyklad)
    print(f"klas potrzebnych w porównaniach {len(potrzebne)}, przykładów z puli rynku {z_puli}", flush=True)
    koszt = asyncio.run(kr.zapytaj(kl, budzet))  # pamięć wspólna z w2 — ta sama klasa nie jest pytana drugi raz
    zam = kr.zamiany_ofert([(oferty[q["a"]], oferty[q["b"]]) for q in pary if not q["bez_wspolnych"]], rek, s, kon, x["slowa"], x["sal"], x["wyk"])
    koszt_z = asyncio.run(kr.zapytaj_zamiany(zam, budzet, proba=proba))
    if proba:  # próba zamian do obejrzenia przed pełnym przebiegiem
        pz = json.loads(kr.PLIK_ZAMIAN.read_text(encoding="utf-8"))
        for k in sorted((k for k in zam if k in pz), key=lambda k: -zam[k]["par"])[:proba]:
            v = pz[k]
            print(f"  {'=' if zamiana_rownowazna(v) else ' '} {v.get('relacja')} {(v.get('rozklad') or {}).get('to_samo', 0):.2f} "
                  f"„{v['slowa_a']}” / „{v['slowa_b']}” | {v['a']} vs {v['b']} — par {zam[k]['par']}")
    print(f"klas {len(kl)}, zamian {len(zam)}; koszt {koszt + koszt_z:.4f} USD")


def werdykty() -> tuple[list[dict], dict[str, Oferta]]:
    pary, oferty, _rek, x = podpisy()
    pod = x["pod"]
    roz = json.loads(kr.PLIK.read_text(encoding="utf-8"))
    zam = json.loads(kr.PLIK_ZAMIAN.read_text(encoding="utf-8")) if kr.PLIK_ZAMIAN.exists() else {}
    kl = Klasy(opisowe={tuple(v["klasa"]) for v in roz.values() if _klasy.klasa_nieistotna(v)},
               rownowazne={tuple(v["klasa"]) for v in zam.values() if zamiana_rownowazna(v)})
    for q in pary:
        pa, pb = pod.get(q["a"]), pod.get(q["b"])
        if q["bez_wspolnych"]:
            q["podpis"], q["powod"] = INNA, "bez wspólnych słów (skrót)"
        elif pa is None or pb is None:
            q["podpis"], q["powod"] = PODOBNA, "brak rekordu wyciągania"  # nigdy „ta sama” bez rozkładu
        else:
            # decyzja Alexa 30.09: cena ≥ 5× → nie „ta sama” (jak w wycenie raportu)
            w0, p0 = porownaj(pa, pb, kl)
            q["podpis"], q["powod"] = straznik_ceny(w0, p0, oferty[q["a"]].cena_zl, oferty[q["b"]].cena_zl)
        q["b0"] = normalizuj(f"{oferty[q['a']].nazwa} {oferty[q['a']].wariant}") == normalizuj(f"{oferty[q['b']].nazwa} {oferty[q['b']].wariant}")
    return pary, oferty


V14 = B / "scripts" / "typesafe" / "dane" / "2026-09-28" / "v14_sprawdzian7" / "pary.json"
NA_GRUPE = {"obie": 999, "tylko_podpis": 999, "tylko_v14f": 999, "warianty": 999, "reszta": 150}  # wszystkie: 225 „ta sama” podpisu to całość, bez błędu próby


def _pierwsza(sid: int, oferty: dict[str, Oferta]) -> str:
    """Oferta reprezentująca usługę = wariant, którego cenę pokazuje ogłoszenie (pierwszy) — jak w zbiorach 4–6."""
    return str(sid) if str(sid) in oferty else f"{sid}#0"


def _opis_oferty(o: Oferta) -> dict:
    return {"nazwa": o.nazwa, "wariant": o.wariant, "kategoria": o.kategoria, "opis": o.opis, "cena": o.cena_zl,
            "typ": o.typ_salonu, "zabieg_booksy": o.zabieg_booksy}


def probka() -> None:
    """Pary usług sprawdzianu (jak zbiory 5–6) z werdyktem v14f; podpis i B0 na ofercie reprezentującej każdą
    usługę. Grupy rozłączne w branży: obie metody „ta sama”, tylko podpis, tylko v14f — każda ważona liczebnością.
    Osobno: „ta sama” podpisu na pozostałych wariantach (raport pokazuje każdy wariant)."""
    pary, oferty = werdykty()
    po_ofertach = {(q["a"], q["b"]): q for q in pary}
    v14 = {(q["a"], q["b"]): q["v14"] for q in json.loads(V14.read_text(encoding="utf-8"))} if V14.exists() else {}
    reszta: list[dict] = []  # bez v14f (sprawdzian 8+): losowa próba pozostałych par — szacunek zgubionych prawdziwych
    us, pary_uslug, _s = dane()
    rng = random.Random(ZIARNO + 1)
    grupy: dict[tuple[str, str], list[dict]] = defaultdict(list)
    reprez = set()
    for q in pary_uslug:
        oa, ob = _pierwsza(q["a"], oferty), _pierwsza(q["b"], oferty)
        w = po_ofertach[(oa, ob)]
        reprez.add((oa, ob))
        wpis = {**w, "v14f": v14.get((q["a"], q["b"])), "usluga_a": q["a"], "usluga_b": q["b"]}
        p_ts, v_ts = w["podpis"] == TA_SAMA, wpis["v14f"] == "tozsame"
        if p_ts or v_ts:
            grupy[(q["branza"], "obie" if p_ts and v_ts else "tylko_podpis" if p_ts else "tylko_v14f")].append(wpis)
        elif not v14:
            reszta.append(wpis)
    warianty = [q for k, q in po_ofertach.items() if k not in reprez and q["podpis"] == TA_SAMA]
    wynik, liczebnosci = [], {}
    for (br, g), lst in sorted(grupy.items()):
        wyb = rng.sample(lst, min(NA_GRUPE[g], len(lst)))
        liczebnosci[f"{br}|{g}"] = len(lst)
        wynik += [{**q, "grupa": g, "waga": round(len(lst) / len(wyb), 4)} for q in wyb]
    if reszta:
        wr = rng.sample(reszta, min(NA_GRUPE["reszta"], len(reszta)))
        liczebnosci["RAZEM|reszta"] = len(reszta)
        wynik += [{**q, "grupa": "reszta", "waga": round(len(reszta) / len(wr), 4)} for q in wr]
    wyb = rng.sample(warianty, min(NA_GRUPE["warianty"], len(warianty)))
    liczebnosci["RAZEM|warianty"] = len(warianty)
    wynik += [{**q, "grupa": "warianty", "waga": round(len(warianty) / max(len(wyb), 1), 4)} for q in wyb]
    for q in wynik:
        q["oa"], q["ob"] = _opis_oferty(oferty[q["a"]]), _opis_oferty(oferty[q["b"]])
        q["warianty_a"] = [w.get("label") for w in us[int(q["a"].split("#")[0])].get("warianty") or [] if w.get("label")][:8]
        q["warianty_b"] = [w.get("label") for w in us[int(q["b"].split("#")[0])].get("warianty") or [] if w.get("label")][:8]
    (OUT / "probka_oceny.json").write_text(json.dumps({"liczebnosci": liczebnosci, "pary": wynik}, ensure_ascii=False, indent=1),
                                           encoding="utf-8")
    print(f"do oceny {len(wynik)}; grupy: " + ", ".join(f"{k} {v}" for k, v in sorted(liczebnosci.items())))


def wynik() -> None:
    """Ważone liczebnością grup: trafność „ta sama” podpisu i v14f, prawdziwe pary znane (suma trafnych z grup),
    odzysk = trafne metody / znane; per branża. Warianty osobno (trafność podpisu na wariantach)."""
    d = json.loads((OUT / "probka_oceny.json").read_text(encoding="utf-8"))
    oc = {f"{q['a']}|{q['b']}": q["ocena"] for q in json.loads((OUT / "ocena_claude.json").read_text(encoding="utf-8"))}
    licz = d["liczebnosci"]
    pr = [q for q in d["pary"] if f"{q['a']}|{q['b']}" in oc]
    frac = lambda z: sum(oc[f"{q['a']}|{q['b']}"] == "T" for q in z) / len(z) if z else 0.0
    out = {}
    for br in sorted({q["branza"] for q in pr if q["grupa"] != "warianty"}) + ["RAZEM"]:
        t = {g: 0.0 for g in ("obie", "tylko_podpis", "tylko_v14f")}
        n = dict(t)
        for g in t:
            klucze = [k for k in licz if k.endswith(f"|{g}") and (br == "RAZEM" or k.startswith(f"{br}|"))]
            for k in klucze:
                z = [q for q in pr if q["grupa"] == g and q["branza"] == k.split("|")[0]]
                n[g] += licz[k]
                t[g] += licz[k] * frac(z)
        znane = sum(t.values())
        pod_n, pod_t = n["obie"] + n["tylko_podpis"], t["obie"] + t["tylko_podpis"]
        v_n, v_t = n["obie"] + n["tylko_v14f"], t["obie"] + t["tylko_v14f"]
        out[br] = {"podpis": {"ta_sama": round(pod_n), "trafnosc": round(pod_t / pod_n, 3) if pod_n else None,
                              "trafnych": round(pod_t, 1), "odzysk": round(pod_t / znane, 3) if znane else None},
                   "v14f": {"ta_sama": round(v_n), "trafnosc": round(v_t / v_n, 3) if v_n else None,
                            "trafnych": round(v_t, 1), "odzysk": round(v_t / znane, 3) if znane else None},
                   "znane_prawdziwe": round(znane, 1)}
        print(f"{br:<20} " + json.dumps(out[br], ensure_ascii=False))
    zr = [q for q in pr if q["grupa"] == "reszta"]
    if zr:  # szacunek prawdziwych par, których podpis nie znalazł (losowa próba reszty, waga = liczność / próba)
        zgub = licz["RAZEM|reszta"] * frac(zr)
        pod = out["RAZEM"]["podpis"]
        out["RAZEM"]["szac_zgubionych_prawdziwych"] = round(zgub, 1)
        out["RAZEM"]["szac_odzysk_podpisu"] = round(pod["trafnych"] / (pod["trafnych"] + zgub), 3) if pod["trafnych"] + zgub else None
        print(f"reszta: oceniono {len(zr)} z {licz['RAZEM|reszta']}, trafnych w próbie {frac(zr):.1%} → zgubionych ~{zgub:.0f}; "
              f"odzysk podpisu ~{out['RAZEM']['szac_odzysk_podpisu']}")
    zw = [q for q in pr if q["grupa"] == "warianty"]
    out["warianty"] = {"ocenionych": len(zw), "trafnosc": round(frac(zw), 3) if zw else None}
    print("warianty (poza ofertą reprezentującą): " + json.dumps(out["warianty"], ensure_ascii=False))
    (OUT / "wynik.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


def przelicz() -> None:
    """Werdykty od nowa na TYCH SAMYCH parach (bez nowego losowania) — bilans zmiany reguły na ocenionej próbie.
    Grupy liczone jak w `probka`, ale z nowymi werdyktami; próbą grupy są ocenione pary, które do niej teraz należą.
    Wypisuje każdą ocenioną parę, której „ta sama” się zmieniło (z moją oceną), i trafność ważoną na nowej populacji."""
    d = json.loads((OUT / "probka_oceny.json").read_text(encoding="utf-8"))
    oc = {(q["a"], q["b"]): q["ocena"] for q in json.loads((OUT / "ocena_claude.json").read_text(encoding="utf-8"))}
    pary, oferty = werdykty()
    po_ofertach = {(q["a"], q["b"]): q for q in pary}
    v14 = {(q["a"], q["b"]): q["v14"] for q in json.loads(V14.read_text(encoding="utf-8"))} if V14.exists() else {}
    _us, pary_uslug, _s = dane()
    grupa: dict[tuple[str, str], str] = {}
    populacja: Counter = Counter()
    for q in pary_uslug:
        k = (_pierwsza(q["a"], oferty), _pierwsza(q["b"], oferty))
        p_ts, v_ts = po_ofertach[k]["podpis"] == TA_SAMA, v14.get((q["a"], q["b"])) == "tozsame"
        if p_ts or v_ts:
            grupa[k] = "obie" if p_ts and v_ts else "tylko_podpis" if p_ts else "tylko_v14f"
            populacja[(q["branza"], grupa[k])] += 1
    zmiany: Counter = Counter()
    for q in d["pary"]:
        k = (q["a"], q["b"])
        stary, nowy = q["podpis"], po_ofertach[k]["podpis"]
        if (stary == TA_SAMA) != (nowy == TA_SAMA):
            zmiany[(q["branza"], stary, nowy, oc.get(k))] += 1
            print(f"  {oc.get(k)} {stary}→{nowy} ({po_ofertach[k]['powod']}) | {q['oa']['nazwa']} [{q['oa']['kategoria']}]"
                  f" || {q['ob']['nazwa']} [{q['ob']['kategoria']}]")
    ocenione = [q for q in d["pary"] if (q["a"], q["b"]) in oc and q["grupa"] != "warianty"]
    wynik_br = {}
    for br in sorted({q["branza"] for q in ocenione}) + ["RAZEM"]:
        n = t = 0.0
        for (b, g), liczba in populacja.items():
            if g == "tylko_v14f" or (br != "RAZEM" and b != br):
                continue
            z = [oc[(q["a"], q["b"])] == "T" for q in ocenione if q["branza"] == b and grupa.get((q["a"], q["b"])) == g]
            n += liczba
            t += liczba * (sum(z) / len(z) if z else 0.0)
        wynik_br[br] = {"ta_sama": round(n), "trafnosc": round(t / n, 3) if n else None}
        print(f"{br:<20} " + json.dumps(wynik_br[br], ensure_ascii=False))
    zw = [q for q in d["pary"] if q["grupa"] == "warianty" and (q["a"], q["b"]) in oc
          and po_ofertach[(q["a"], q["b"])]["podpis"] == TA_SAMA]
    print(f"warianty: {sum(oc[(q['a'], q['b'])] == 'T' for q in zw)}/{len(zw)} trafnych")
    print("zmiany na ocenionych (branża, stary, nowy, ocena): " + json.dumps(
        {"|".join(map(str, k)): v for k, v in sorted(zmiany.items())}, ensure_ascii=False))


def main() -> None:
    ap = argparse.ArgumentParser()
    for k in ("zbierz", "wyciagnij", "salony", "klasy", "probka", "wynik", "przelicz"):
        ap.add_argument(f"--{k}", action="store_true")
    ap.add_argument("--ziarno", type=int, default=ZIARNO)
    ap.add_argument("--rownolegle", type=int, default=4)
    ap.add_argument("--budzet", type=float, default=0.3)
    ap.add_argument("--licz", action="store_true", help="tylko liczby par i ofert do wyciągnięcia")
    ap.add_argument("--proba", type=int, default=0, help="z --klasy: zapytaj tylko o N najczęstszych nowych zamian")
    ap.add_argument("--wyjscie", default="sprawdzian7", help="katalog w dane/2026-09-29/ (nowy = nowe salony)")
    ap.add_argument("--na-grupe", type=int, default=999, help="--probka: najwyżej N par z grupy na branżę (reszta ważona)")
    ap.add_argument("--uslug", type=int, default=3, help="--zbierz: usług podmiotu na salon (sprawdzian 9: 4 — pokrycie wierszy)")
    a = ap.parse_args()
    global OUT, V14
    OUT = OUT.parent / a.wyjscie
    V14 = B / "scripts" / "typesafe" / "dane" / "2026-09-28" / f"v14_{a.wyjscie}" / "pary.json"
    if a.zbierz:
        zbierz(a)
    if a.licz:
        _us, pary, oferty = pary_ofert()
        print(f"par ofert {len(pary)}, bez wspólnych słów {sum(q['bez_wspolnych'] for q in pary)}, "
              f"ofert razem {len(oferty)}, do wyciągnięcia {len(do_wyciagniecia())}")
    if a.wyciagnij:
        asyncio.run(wyciagnij(a.rownolegle))
    if a.salony:
        asyncio.run(wyciagnij_salony(a.rownolegle))
    if a.klasy:
        klasy(a.budzet, a.proba)
    if a.probka:
        NA_GRUPE.update({g: a.na_grupe for g in ("obie", "tylko_podpis", "tylko_v14f")})
        probka()
    if a.wynik:
        wynik()
    if a.przelicz:
        przelicz()


if __name__ == "__main__":
    main()
