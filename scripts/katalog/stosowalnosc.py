"""Katalog usług — kontekst oferty bez reguł (decyzja Alexa 1.10, druga wersja rozbioru działu).

Pierwsza wersja (dzialy.py: GLM czyta dział i sam dopisuje kontekst) na teście F: −68 prawdziwych par „ta sama” za −3
błędne — model dopisywał części nagłówka niekonsekwentnie (ten sam nagłówek „Depilacja pastą cukrową kobiety” u dwóch
salonów: raz „kobiety”, raz „pastą cukrową”). Tu każda część kontekstu jest STAŁA — nagłówek działu, nazwa salonu
i etykieta zabiegu Booksy są rozłożone raz na tekst (kategorie.json, salony.json, w2/etykiety_booksy.json) — a o tym,
czy część dotyczy oferty, rozstrzyga TypeSafe Noul raz na (część, źródło, nazwa oferty) z pamięcią: ta sama odpowiedź
dla tego samego nagłówka i tej samej usługi w każdym salonie. Bez reguł o liście w nagłówku i o nazwie salonu.
Zabieg z kontekstu tylko, gdy sama nazwa nie mówi, co to za zabieg — w modelu usługa ma JEDEN zabieg; o tym też
rozstrzyga TypeSafe (raz na nazwę), nie rola przypisana przez GLM („Brwi” bywało zabiegiem).
Słowa własne oferty: rozbiór p12 (nazwa, wariant; z opisu tylko wyłączenia i liczby).

  KATALOG_ROZBIOR=stosowalnosc w sprawdzian7.py / bilansach — podpisy z tych rekordów.
  ZASADY_OK=1 python scripts/katalog/stosowalnosc.py --wyjscie sprawdzian12 --budzet 0.2   # pytania (płatne)
  python scripts/katalog/stosowalnosc.py --wyjscie sprawdzian12 --licz                     # ile pytań, bez zapytań
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts"), str(B / "scripts" / "typesafe")]
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402
from services.katalog_uslug.podpis import POZA_POROWNANIEM, POZIOM_ROLI, _frazy, _slowa  # noqa: E402

DANE = B / "scripts" / "katalog" / "dane" / "2026-09-29"
PLIK = DANE / "w2" / "stosowalnosc_p2.json"  # pamięć wspólna dla zbiorów, jak klasy_p3.json; numer = WERSJA_PYTANIA
ETYKIETY = DANE / "w2" / "etykiety_booksy.json"  # rozkład etykiet zabiegu Booksy (GLM raz na tekst, jak kategorie)
KLUCZ = Path.home() / ".config" / "typesafe" / "api_key"
MODEL = "jev-1.13.0"  # jak klasy_roznic.py
CENA_TOK = 0.042 / 1e6
TOK_NA_PYTANIE = 750  # zmierzone: test F 1.10, 746 tokenów na pytanie (próba: 650)
PROG = 0.5  # Noul to prawdopodobieństwo „tak”: rozstrzyga „tak”, gdy jest bardziej prawdopodobne niż „nie”
WLASNE = ("nazwa", "wariant")
ZRODLO = {"dzial": "z nagłówka działu cennika", "salon": "z nazwy salonu", "zabieg_booksy": "z zabiegu wybranego w Booksy"}
POMIJANE_ROLE = frozenset({"specjalista", "inne"})  # wykonawca poza podpisem (plan p. 5); „inne” to cel albo marketing
WERSJA_PYTANIA = 2  # v1 („czy mówi coś prawdziwego o usłudze”): próba 1.10 — „Medycyna Estetyczna”, „Kosmetologia”,
# „Hair” z nazwy salonu i „brwi i rzęs” przy hennie rzęs dostawały „tak” (prawdziwe, ale nic nie odróżniają)


def _modul(nazwa: str, plik: Path):
    spec = importlib.util.spec_from_file_location(nazwa, plik)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def id_etykiety(tekst: str) -> str:
    return "b:" + hashlib.sha1(normalizuj(tekst).encode("utf-8")).hexdigest()[:12]


def czesci(o, rek: dict[str, Any], kat: dict[str, Any] | None, sal: dict[str, Any] | None, ks: dict[str, Any] | None,
           slownik: dict[str, str]) -> list[dict[str, str]]:
    """Części kontekstu oferty: frazy rozkładu nagłówka działu, nazwy salonu i etykiety Booksy (bez wykonawcy i „inne”)
    — poza tymi, których słowa oferta już ma (nic by nie dodały)."""
    wlasne = {w for r, zr, f in _frazy(rek) if zr in WLASNE and f for w in _slowa(f, slownik)}
    zrodla = [("dzial", kat, o.kategoria), ("salon", sal, (sal or {}).get("nazwa") or ""), ("zabieg_booksy", ks, o.zabieg_booksy)]
    wynik = [{"zrodlo": z, "tekst": t, "rola": r, "fraza": f} for z, rozklad, t in zrodla if rozklad
             for r, _zr, f in _frazy(rozklad) if f and r in POZIOM_ROLI and r not in POMIJANE_ROLE]
    return [c for c in wynik if not set(_slowa(c["fraza"], slownik)) <= wlasne]


def klucz(c: dict[str, str], o) -> str:
    tekst = " | ".join(normalizuj(x) for x in (c["zrodlo"], c["tekst"], c["fraza"], o.nazwa, o.wariant))
    return hashlib.sha1(tekst.encode("utf-8")).hexdigest()[:16]


def klucz_nazwy(o) -> str:
    return "z:" + hashlib.sha1(normalizuj(f"{o.nazwa} | {o.wariant}").encode("utf-8")).hexdigest()[:16]


def pytanie(c: dict[str, str]):
    from typesafe_sdk import Noul
    return Noul(
        instructions=(f"Usługa `usluga.nazwa` (wariant `usluga.wariant`) jest w cenniku salonu `usluga.salon` w dziale "
                      f"`usluga.dzial`; w Booksy salon przypisał jej zabieg `usluga.zabieg_booksy`. Fraza „{c['fraza']}” "
                      f"pochodzi {ZRODLO[c['zrodlo']]}, nie z nazwy usługi. Czy ta fraza dopowiada o TEJ usłudze coś, czego "
                      "jej nazwa nie mówi, a co odróżnia ją od innych usług o tej samej nazwie: co to za zabieg (gdy nazwa "
                      "tego nie mówi), jaką metodą, urządzeniem albo preparatem się ją wykonuje, na jakim obszarze, dla kogo "
                      "albo ile zabiegów obejmuje?"),
        criteria={"true": "tak — fraza dopowiada taką informację i dotyczy właśnie tej usługi (opisuje wszystkie usługi "
                          "działu albo salonu, albo wymienia kilka rzeczy i ta usługa jest jedną z nich)",
                  "false": "nie — fraza dotyczy innych usług działu, powtarza to, co mówi nazwa, nazywa tylko branżę, "
                           "rodzaj gabinetu albo salon, jest hasłem marketingowym, albo nazwa usługi mówi co innego"})


def pytanie_nazwy():
    """Czy sama nazwa mówi, co to za zabieg — jedno pytanie na nazwę (bez działu i salonu, żeby ocenić samą nazwę)."""
    from typesafe_sdk import Noul
    return Noul(
        instructions=("Czy z samej nazwy usługi `usluga.nazwa` (z wariantem `usluga.wariant`) z cennika salonu beauty wiadomo, "
                      "jaki to zabieg — co się robi klientce (np. strzyżenie, depilacja, makijaż permanentny, masaż, "
                      "mezoterapia, usuwanie zmian skórnych)?"),
        criteria={"true": "tak — z samej nazwy wiadomo, jaki to zabieg",
                  "false": "nie — nazwa podaje tylko obszar, urządzenie, preparat, technikę, wariant albo nazwę własną "
                           "i bez działu cennika nie wiadomo, co to za zabieg"})


def stan(o, sal_nazwa: str) -> dict[str, Any]:
    return {"usluga": {"nazwa": o.nazwa, "wariant": o.wariant, "dzial": o.kategoria, "salon": sal_nazwa,
                       "typ_salonu": o.typ_salonu, "zabieg_booksy": o.zabieg_booksy, "opis": (o.opis or "")[:200]}}


async def zapytaj(pytania: dict[str, tuple[dict[str, Any], Any, dict[str, Any]]], budzet: float) -> float:
    """pytania: klucz → (stan, pytanie Noul, metadane do pamięci). Pamięć: klucz → metadane + „tak” (prawdopodobieństwo)."""
    from typesafe_sdk import AsyncTypeSafeClient
    pamiec = json.loads(PLIK.read_text(encoding="utf-8")) if PLIK.exists() else {}
    nowe = [k for k in pytania if k not in pamiec]
    szac = len(nowe) * TOK_NA_PYTANIE * CENA_TOK
    print(f"pytań {len(pytania)}, nowych {len(nowe)}, szac. {szac:.3f} USD (budżet {budzet})", flush=True)
    if szac > budzet:
        sys.exit("szacunek ponad budżet — przerwane")
    tok, sem = [0], asyncio.Semaphore(6)
    async with AsyncTypeSafeClient(api_key=KLUCZ.read_text(encoding="utf-8").strip(), model=MODEL, timeout=60.0) as cl:
        async def jedno(k: str) -> None:
            st, q, meta = pytania[k]
            async with sem:
                try:
                    r = await cl.system_one(st, {"s": q}, model=MODEL)
                    tok[0] += r.usage.input_tokens or 0
                    pamiec[k] = {**meta, "tak": r.nouls["s"].noul}
                except Exception as e:  # noqa: BLE001 — brak odpowiedzi zapisany jawnie (część nie wchodzi)
                    pamiec[k] = {**meta, "tak": None, "blad": f"{type(e).__name__}: {str(e)[:120]}"}
        await asyncio.gather(*(jedno(k) for k in nowe))
    PLIK.write_text(json.dumps(pamiec, ensure_ascii=False, indent=0), encoding="utf-8")
    koszt = tok[0] * CENA_TOK
    print(f"koszt {koszt:.4f} USD, tokenów na pytanie {tok[0] / max(len(nowe), 1):.0f}", flush=True)
    return koszt


def _dane(s7):
    us, _pary, oferty = s7.pary_ofert()
    wyc = s7.do_wyciagniecia()
    s7.tp.OUT = s7.OUT
    rek, _ = s7.tp.rekordy("p12", list(oferty.values()))
    kon = s7.km.kontekst(wyc, s7.OUT / "kategorie.json")
    sal_of = s7.salon_ofert(us, oferty)
    sal = s7.km.kontekst_salonu(sal_of, s7.OUT / "salony.json") if (s7.OUT / "salony.json").exists() else {}
    et = json.loads(ETYKIETY.read_text(encoding="utf-8")) if ETYKIETY.exists() else {}
    ks = {o.id: et.get(id_etykiety(o.zabieg_booksy)) for o in wyc if normalizuj(o.zabieg_booksy)}
    return wyc, rek, kon, sal, ks, sal_of, s7._slownik()


def _tak(pamiec: dict[str, Any], k: str) -> float:
    return (pamiec.get(k) or {}).get("tak") or 0.0


def rekordy(s7) -> dict[str, dict]:
    """Rekordy p12 z dołączonym kontekstem, który TypeSafe uznał za dopowiadający coś o ofercie (flaga `dzial` — podpis
    bierze je wprost, bez reguł o źródłach). Zabieg z kontekstu tylko przy nazwie, z której nie wiadomo, co to za zabieg.
    Część bez odpowiedzi w pamięci nie wchodzi."""
    wyc, rek, kon, sal, ks, _sal_of, s = _dane(s7)
    pamiec = json.loads(PLIK.read_text(encoding="utf-8")) if PLIK.exists() else {}
    wynik: dict[str, dict] = {}
    for o in wyc:
        r = rek.get(o.id)
        if r is None:
            continue
        nazwa_mowi = _tak(pamiec, klucz_nazwy(o)) >= PROG
        wlasne = [c for c in r.get("cechy") or [] if c.get("zrodlo") in (*WLASNE, "opis")]
        zab = r.get("zabieg") or {}
        dolaczone = [{"rola": c["rola"], "fraza": c["fraza"], "zrodlo": c["zrodlo"]}
                     for c in czesci(o, r, kon.get(o.id), sal.get(o.id), ks.get(o.id), s)
                     if _tak(pamiec, klucz(c, o)) >= PROG and not (c["rola"] == "zabieg" and nazwa_mowi)]
        poz_kat = (kon.get(o.id) or {}).get("pozycja_kategorii")  # sekcja „nie usługi” (rozkład raz na nagłówek)
        wynik[o.id] = {**r, "zabieg": zab if zab.get("zrodlo") in WLASNE else {"fraza": "", "zrodlo": ""},
                       "cechy": wlasne + dolaczone, "dzial": True,
                       "pozycja": poz_kat if poz_kat in POZA_POROWNANIEM else r.get("pozycja")}
    return wynik


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wyjscie", required=True)
    ap.add_argument("--budzet", type=float, default=0.2)
    ap.add_argument("--licz", action="store_true")
    a = ap.parse_args()
    s7 = _modul("sprawdzian7", B / "scripts" / "katalog" / "sprawdzian7.py")
    s7.OUT = DANE / a.wyjscie
    wyc, rek, kon, sal, ks, sal_of, s = _dane(s7)
    cz = {o.id: czesci(o, rek[o.id], kon.get(o.id), sal.get(o.id), ks.get(o.id), s) for o in wyc if o.id in rek}
    po_id = {o.id: o for o in wyc}
    nazwy = {klucz_nazwy(po_id[i]): ({"usluga": {"nazwa": po_id[i].nazwa, "wariant": po_id[i].wariant}}, pytanie_nazwy(),
                                     {"oferta": f"{po_id[i].nazwa} / {po_id[i].wariant}".strip(" /"), "pytanie": "nazwa_mowi_zabieg"})
             for i, c in cz.items() if any(x["rola"] == "zabieg" for x in c)}
    print(f"nazw do pytania „czy mówi, co to za zabieg”: {len(nazwy)}", flush=True)
    if not a.licz:
        asyncio.run(zapytaj(nazwy, a.budzet))
    pamiec = json.loads(PLIK.read_text(encoding="utf-8")) if PLIK.exists() else {}
    pytania: dict[str, tuple[dict[str, Any], Any, dict[str, Any]]] = {}
    for i, c in cz.items():
        o = po_id[i]
        nazwa_mowi = _tak(pamiec, klucz_nazwy(o)) >= PROG
        for x in c:
            if x["rola"] == "zabieg" and (nazwa_mowi or a.licz and klucz_nazwy(o) not in pamiec):
                continue
            pytania.setdefault(klucz(x, o), (stan(o, sal_of.get(o.id, "")), pytanie(x),
                                              {**x, "oferta": f"{o.nazwa} / {o.wariant}".strip(" /")}))
    print("części wg źródła:", dict(Counter(m["zrodlo"] for _s, _q, m in pytania.values())),
          "wg roli:", dict(Counter(m["rola"] for _s, _q, m in pytania.values()).most_common(6)), flush=True)
    if not a.licz:
        asyncio.run(zapytaj(pytania, a.budzet))


if __name__ == "__main__":
    main()
