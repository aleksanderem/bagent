"""Katalog usług, etap 1 — klasy różnic jednostronnych z ocenionych par → TypeSafe Score raz na klasę (grosze).

Zbiera z par (podpisy z pamięci wyciągania) różnice „dopisek tylko po jednej stronie”, dla każdej klasy
(rdzeń, poziom, dopisek) bierze pierwszą ofertę z dopiskiem jako reprezentanta i pyta TypeSafe na jej pełnym
kontekście, czy dopisek zmienia usługę (services/katalog_uslug/klasy.py). Wynik w pamięci: ocena_podpisu.py --klasy.

Klucz TypeSafe: ~/.config/typesafe/api_key. Użycie (bramka płatnych uruchomień: wpis w preflight.log + ZASADY_OK=1):
  bagent/.venv/bin/python bagent/scripts/katalog/klasy_roznic.py --wariant p12 --budzet 0.2
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts")]
from services.katalog_uslug.ekstrakcja import WERSJA_PROMPTU  # noqa: E402
from services.katalog_uslug.klasy import (NIE_ZMIENIA, WERSJA_PYTANIA, WERSJA_ZAMIANY, przyklad_czysty,  # noqa: E402
                                          pytanie_klasy, pytanie_zamiany, rozstrzygnij, zamiana_rownowazna)
from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa  # noqa: E402
from services.katalog_uslug.podpis import (_wybrane_frazy, klucz_roznicy, podpis, roznica_do_pytania,  # noqa: E402
                                           zamiana_do_pytania)


def slownictwo_rynku(rek: dict, slownik: dict | None) -> frozenset[str]:
    return op.slownictwo_rynku(rek, slownik)


def wykonawcy_rynku(rek: dict, slownik: dict | None) -> frozenset[str]:
    return op.wykonawcy_rynku(rek, slownik)


def metody_rynku(rek: dict, slownik: dict | None) -> frozenset[str]:
    return op.metody_rynku(rek, slownik)
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402

_spec = importlib.util.spec_from_file_location("ocena_podpisu", B / "scripts" / "katalog" / "ocena_podpisu.py")
op = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(op)

KLUCZ = Path.home() / ".config" / "typesafe" / "api_key"
MODEL = "jev-1.13.0"
CENA_TOK = 0.042 / 1e6
PLIK = B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}" / f"klasy_p{WERSJA_PYTANIA}.json"
PLIK_ZAMIAN = PLIK.parent / f"zamiany_p{WERSJA_ZAMIANY}.json"


def _dopisek(rek: dict, poziom: str, slowa: set[str], kontekst: dict | None = None, slownik: dict | None = None) -> str:
    """Tylko słowa różnicy, w kolejności i brzmieniu z oferty albo jej kategorii (bez słów wspólnych z drugą ofertą)."""
    z = (rek.get("zabieg") or {}).get("fraza") or ""
    s = slownik or {}
    frazy = [z] + [f for _poz, f in _wybrane_frazy(rek, kontekst, s)] + list(rek.get("nieprzypisane") or [])
    wynik: list[str] = []
    for f in frazy:
        for tok in f.split():
            if {s.get(r, r) for t in normalizuj(tok).split() if (r := rdzen_slowa(t))} & slowa and tok not in wynik:
                wynik.append(tok)
    return " ".join(wynik) or " ".join(sorted(slowa))


def klasy_z_par(wariant: str, slownik: dict[str, str] | None = None, kon: dict | None = None) -> dict[str, dict]:
    kon = kon or {}
    op.tp.OUT = op.tp.OUT.parent / f"w{WERSJA_PROMPTU}" / "wszystkie"
    pary = op.pary_ocenione()
    rek, _ = op.tp.rekordy(wariant, op.tp.oferty_probki(0))
    slowa = slownictwo_rynku(rek, slownik)
    klasy: dict[str, dict] = {}
    for q in pary:
        ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
        if not (ra and rb):
            continue
        pa, pb = podpis(ra, slownik, kon.get(q["oa"].id), slowa), podpis(rb, slownik, kon.get(q["ob"].id), slowa)
        for klasa, strona in roznica_do_pytania(pa, pb):
            o_z, r_z, o_bez = (q["oa"], ra, q["ob"]) if strona is pa else (q["ob"], rb, q["oa"])
            dodaj_przyklad(klasy, klasa, o_bez, slownik, lambda: {
                "oferta": o_z.id, "dopisek": _dopisek(r_z, klasa[1], set(klasa[2].split()), kon.get(o_z.id), slownik),
                "zabieg": _nazwa(o_z), "druga": _nazwa(o_bez), "stan": _stan(o_z)})
    return klasy


def dodaj_przyklad(klasy: dict[str, dict], klasa, o_bez, slownik: dict | None, przyklad) -> None:
    """Klasa → licznik par i pierwszy CZYSTY przykład do pytania (klasy.przyklad_czysty, sprawdzian 9). Klasa bez
    czystego przykładu nie jest pytana — zostaje istotna (zła para gorsza niż brak porównania)."""
    w = klasy.setdefault(json.dumps(klasa, ensure_ascii=False), {"klasa": klasa, "par": 0})
    w["par"] += 1
    if "oferta" not in w and przyklad_czysty(klasa[2], _nazwa(o_bez), slownik):
        w.update(przyklad())


def przyklady_z_puli(klasy: dict[str, dict], potrzebne: set[str], pod: dict, oferty: dict, salon: dict[str, str],
                     slownik: dict | None, przyklad) -> int:
    """Klasa potrzebna w porównaniu (podpis.klasy_do_rozstrzygniecia), a bez czystego przykładu w parach podmiot–kandydat
    → czysty przykład z pary dwóch innych ofert puli (różne salony) o DOKŁADNIE tej różnicy. Znaczenie słów nie zależy
    od tego, kto jest podmiotem; bez tego klasy stron różnicy dwustronnej zostawały bez przykładu (sprawdzian 9 po
    poprawce: −19 prawdziwych par). przyklad(a, b, klasa) → pola przykładu. Zwraca liczbę znalezionych przykładów."""
    po_zbiorze: dict[frozenset[str], list[str]] = {}
    for oid, p in pod.items():
        po_zbiorze.setdefault(frozenset(p.zbior), []).append(oid)
    znalezione = 0
    for k in potrzebne:
        if "oferta" in klasy.get(k, {}):
            continue
        klasa = tuple(json.loads(k))
        wspolne, dopisek = frozenset(klasa[0].split()), frozenset(klasa[2].split())
        for a in po_zbiorze.get(wspolne | dopisek, []):
            b = next((b for b in po_zbiorze.get(wspolne, []) if salon.get(b) != salon.get(a)
                      and przyklad_czysty(klasa[2], _nazwa(oferty[b]), slownik)
                      and any(kl == klasa and z is pod[a] for kl, z in roznica_do_pytania(pod[a], pod[b]))), None)
            if b is not None:
                klasy.setdefault(k, {"klasa": klasa, "par": 0}).update(przyklad(a, b, klasa))
                znalezione += 1
                break
    return znalezione


def _nazwa(o) -> str:
    return o.nazwa + (f" — {o.wariant}" if o.wariant else "")


def _stan(o) -> dict:
    return stan_v12({"nazwa": o.nazwa, "kategoria": o.kategoria, "opis": o.opis,
                     "warianty": [{"label": o.wariant}] if o.wariant else [], "zabieg_booksy": o.zabieg_booksy,
                     "typ_salonu": o.typ_salonu})


def zamiany_z_par(wariant: str, slownik: dict[str, str] | None = None, kon: dict | None = None) -> dict[str, dict]:
    """Klasy zamiany słów (różnica po obu stronach, podpis.zamiana_slow) z ocenionych par."""
    op.tp.OUT = PLIK.parent / "wszystkie"  # ścieżka bezwzględna — klasy_z_par już ją przestawiło
    rek, _ = op.tp.rekordy(wariant, op.tp.oferty_probki(0))
    return zamiany_ofert([(q["oa"], q["ob"]) for q in op.pary_ocenione()], rek, slownik, kon or {},
                         slownictwo_rynku(rek, slownik))


def _w_slowniku(slowa: str, slownik: dict[str, str]) -> set[str]:
    return {slownik.get(w, w) for w in slowa.split()}


def przenies_pamiec(slownik: dict[str, str]) -> dict[str, int]:
    """Pamięć klas i zamian w formach głównych BIEŻĄCEGO słownika — plan: klasa różnicy rozstrzygana RAZ. Po przebudowie
    słownika klucze w starych formach („rzes zdjec”) nie pasowały do nowych („rzes usuwan”) i ta sama klasa była pytana
    ponownie; odpowiedź na progu (0,48 → 0,51) przewracała 36 par „Ściągnięcie rzęs” (bilans słownika 1.10). Przy dwóch
    odpowiedziach pod jednym kluczem zostaje starsza (plik zapisuje chronologicznie). Klasa, której dopisek po
    przemapowaniu jest już wśród wspólnych słów, i zamiana, której strony się zrównały, znikają — para jest równa."""
    licz = {"klasy_przed": 0, "klasy_po": 0, "zamiany_przed": 0, "zamiany_po": 0}
    if PLIK.exists():
        stare = json.loads(PLIK.read_text(encoding="utf-8"))
        nowe: dict[str, dict] = {}
        for v in stare.values():
            wsp, poz, dop = v["klasa"]
            w = _w_slowniku(wsp, slownik)
            d = _w_slowniku(dop, slownik) - w
            if d:
                kl = [klucz_roznicy(w), poz, klucz_roznicy(d)]
                nowe.setdefault(json.dumps(kl, ensure_ascii=False), {**v, "klasa": kl})
        PLIK.write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
        licz.update(klasy_przed=len(stare), klasy_po=len(nowe))
    if PLIK_ZAMIAN.exists():
        stare = json.loads(PLIK_ZAMIAN.read_text(encoding="utf-8"))
        nowe = {}
        for v in stare.values():
            wsp, x, y = v["klasa"]
            w, sx, sy = (_w_slowniku(s, slownik) for s in (wsp, x, y))
            da, db = sx - w - sy, sy - w - sx
            if da and db:
                kx, ky = sorted((klucz_roznicy(da), klucz_roznicy(db)))
                kl = [klucz_roznicy(w | (sx & sy)), kx, ky]
                nowe.setdefault(json.dumps(kl, ensure_ascii=False), {**v, "klasa": kl})
        PLIK_ZAMIAN.write_text(json.dumps(nowe, ensure_ascii=False, indent=1), encoding="utf-8")
        licz.update(zamiany_przed=len(stare), zamiany_po=len(nowe))
    return licz


def zamiany_ofert(pary: list, rek: dict, slownik: dict | None, kon: dict, slowa: frozenset[str] | None = None,
                  sal: dict | None = None, wyk: frozenset[str] = frozenset()) -> dict[str, dict]:
    """Klucz klasy → reprezentant (pierwsza para z tą klasą): nazwy i słowa różnicy w brzmieniu z ofert, stan obu ofert."""
    zamiany: dict[str, dict] = {}
    for oa, ob in pary:
        ra, rb = rek.get(oa.id), rek.get(ob.id)
        if not (ra and rb):
            continue
        sal = sal or {}
        pa, pb = (podpis(ra, slownik, kon.get(oa.id), slowa, sal.get(oa.id), wyk),
                  podpis(rb, slownik, kon.get(ob.id), slowa, sal.get(ob.id), wyk))
        if (z := zamiana_do_pytania(pa, pb)) is None:
            continue
        klucz = json.dumps(z, ensure_ascii=False)
        if klucz in zamiany:
            zamiany[klucz]["par"] += 1
            continue
        if " ".join(sorted(pa.zbior - pb.zbior)) != z[1]:  # strona A = ta ze słowami z[1]
            (oa, ra), (ob, rb) = (ob, rb), (oa, ra)
        zamiany[klucz] = {"klasa": list(z), "par": 1, "a": _nazwa(oa), "b": _nazwa(ob),
                          "slowa_a": _dopisek(ra, "", set(z[1].split()), kon.get(oa.id), slownik),
                          "slowa_b": _dopisek(rb, "", set(z[2].split()), kon.get(ob.id), slownik),
                          "stan": {"oferta_a": _stan(oa), "oferta_b": _stan(ob)}}
    return zamiany


async def zapytaj_zamiany(zamiany: dict[str, dict], budzet: float, plik: Path | None = None, proba: int = 0) -> float:
    plik = plik or PLIK_ZAMIAN
    from typesafe_sdk import AsyncTypeSafeClient
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    nowe = sorted((k for k in zamiany if k not in pamiec), key=lambda k: -zamiany[k]["par"])
    nowe = nowe[:proba] if proba else nowe  # próba: najczęstsze klasy, obejrzane przed pełnym przebiegiem
    szac = len(nowe) * 1300 * CENA_TOK
    print(f"zamian {len(zamiany)}, nowych do pytania {len(nowe)}, szac. {szac:.3f} USD (budżet {budzet})", flush=True)
    if szac > budzet:
        sys.exit("szacunek ponad budżet — przerwane")
    tok, sem = [0], asyncio.Semaphore(6)
    async with AsyncTypeSafeClient(api_key=KLUCZ.read_text(encoding="utf-8").strip(), model=MODEL, timeout=60.0) as c:
        async def jedna(k: str) -> None:
            z = zamiany[k]
            opis = {x: z[x] for x in ("klasa", "a", "b", "slowa_a", "slowa_b")}
            async with sem:
                try:
                    r = await c.system_one(z["stan"], {"s": pytanie_zamiany(z["slowa_a"], z["slowa_b"], z["a"], z["b"])},
                                           model=MODEL)
                    tok[0] += r.usage.input_tokens or 0
                    ch = r.choices["s"]
                    pamiec[k] = {**opis, "relacja": ch.choice, "rozklad": dict(ch.probabilities)}
                except Exception as e:  # noqa: BLE001 — brak odpowiedzi = zamiana nierozstrzygnięta, zapisane jawnie
                    pamiec[k] = {**opis, "relacja": None, "blad": f"{type(e).__name__}: {str(e)[:120]}"}
        await asyncio.gather(*(jedna(k) for k in nowe))
    plik.parent.mkdir(parents=True, exist_ok=True)
    plik.write_text(json.dumps(pamiec, ensure_ascii=False, indent=1), encoding="utf-8")
    return tok[0] * CENA_TOK


def _te_same_pytania(klasy: dict[str, dict], pamiec: dict, plik: Path) -> dict[str, dict]:
    """Odpowiedzi poprzedniej wersji pamięci na DOKŁADNIE to samo pytanie (ten sam przykład, dopisek, obie nazwy) —
    v3 zmieniła tylko wybór przykładu, więc takie odpowiedzi są ważne bez ponownego płacenia."""
    stary = plik.with_name(plik.name.replace(f"_p{WERSJA_PYTANIA}", f"_p{WERSJA_PYTANIA - 1}"))
    if stary == plik or not stary.exists():
        return {}
    poprz = json.loads(stary.read_text(encoding="utf-8"))
    pola = ("oferta", "dopisek", "zabieg", "druga")
    return {k: poprz[k] for k, v in klasy.items() if k not in pamiec and "oferta" in v and k in poprz
            and poprz[k].get("score") is not None and all(poprz[k].get(p) == v.get(p) for p in pola)}


async def zapytaj(klasy: dict[str, dict], budzet: float, plik: Path | None = None) -> float:
    plik = plik or PLIK
    from typesafe_sdk import AsyncTypeSafeClient
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    pamiec.update(_te_same_pytania(klasy, pamiec, plik))
    nowe = [k for k in klasy if k not in pamiec and "oferta" in klasy[k]]  # bez czystego przykładu — nie pytamy
    szac = len(nowe) * 700 * CENA_TOK
    bez = sum("oferta" not in v for v in klasy.values())
    print(f"klas {len(klasy)} (bez czystego przykładu {bez}), nowych do pytania {len(nowe)}, szac. {szac:.3f} USD "
          f"(budżet {budzet})", flush=True)
    if szac > budzet:
        sys.exit("szacunek ponad budżet — przerwane")
    tok = [0]
    sem = asyncio.Semaphore(6)
    async with AsyncTypeSafeClient(api_key=KLUCZ.read_text(encoding="utf-8").strip(), model=MODEL, timeout=60.0) as c:
        async def jedna(k: str) -> None:
            k_ = klasy[k]
            async with sem:
                try:
                    r = await c.system_one(k_["stan"], {"s": pytanie_klasy(k_["klasa"][1], k_["dopisek"], k_["zabieg"], k_["druga"])},
                                           model=MODEL)
                    tok[0] += r.usage.input_tokens or 0
                    sc = r.scores["s"]
                    pamiec[k] = {**{x: k_[x] for x in ("klasa", "dopisek", "zabieg", "druga", "oferta")},
                                 "score": sc.score, "rozklad": dict(sc.probabilities)}
                except Exception as e:  # noqa: BLE001 — brak odpowiedzi = klasa istotna, zapisane jawnie
                    pamiec[k] = {**{x: k_[x] for x in ("klasa", "dopisek", "zabieg", "druga", "oferta")},
                                 "score": None, "blad": f"{type(e).__name__}: {str(e)[:120]}"}
        await asyncio.gather(*(jedna(k) for k in nowe))
    plik.parent.mkdir(parents=True, exist_ok=True)
    plik.write_text(json.dumps(pamiec, ensure_ascii=False, indent=1), encoding="utf-8")
    return tok[0] * CENA_TOK


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wariant", default="p12")
    ap.add_argument("--budzet", type=float, default=0.2)
    ap.add_argument("--slownik", action="store_true")
    ap.add_argument("--kategorie", action="store_true")
    ap.add_argument("--zamiany", action="store_true", help="także różnice po obu stronach (inna nazwa tej samej rzeczy)")
    ap.add_argument("--proba", type=int, default=0, help="z --zamiany: zapytaj tylko o N najczęstszych nowych klas")
    a = ap.parse_args()
    slownik = json.loads((PLIK.parent / "slownik.json").read_text(encoding="utf-8")) if a.slownik else None
    kon = {}
    if a.kategorie:
        km = importlib.util.module_from_spec(s_ := importlib.util.spec_from_file_location("kategorie", B / "scripts" / "katalog" / "kategorie.py"))
        s_.loader.exec_module(km)
        kon = km.kontekst(op.tp.oferty_probki(0), PLIK.parent / "kategorie.json")
    klasy = klasy_z_par(a.wariant, slownik, kon)
    koszt = asyncio.run(zapytaj(klasy, a.budzet))
    pamiec = json.loads(PLIK.read_text(encoding="utf-8"))
    rozk = Counter(rozstrzygnij(v.get("score")) for k, v in pamiec.items() if k in klasy)
    print(f"koszt {koszt:.4f} USD; klasy: nie zmienia {rozk[NIE_ZMIENIA]}, nie wiadomo {rozk[1]}, zmienia {rozk[2]}; "
          f"błędów {sum(1 for k, v in pamiec.items() if k in klasy and v.get('blad'))}")
    for k, v in sorted(pamiec.items(), key=lambda kv: -klasy.get(kv[0], {}).get("par", 0))[:25]:
        if k in klasy:
            print(f"  {rozstrzygnij(v.get('score'))} ({v.get('score') if v.get('score') is None else round(v['score'], 2)}) "
                  f"[{v['klasa'][1]}] „{v['dopisek']}” przy „{v['zabieg']}” vs „{v['druga']}” — par {klasy[k]['par']}")
    if a.zamiany:
        zam = zamiany_z_par(a.wariant, slownik, kon)
        koszt_z = asyncio.run(zapytaj_zamiany(zam, a.budzet, proba=a.proba))
        pz = json.loads(PLIK_ZAMIAN.read_text(encoding="utf-8"))
        rz = Counter(v.get("relacja") for k, v in pz.items() if k in zam)
        print(f"koszt zamian {koszt_z:.4f} USD; relacje {dict(rz)}; równoważnych (to samo ≥ próg) "
              f"{sum(zamiana_rownowazna(v) for k, v in pz.items() if k in zam)}")
        for k, v in sorted(pz.items(), key=lambda kv: -zam.get(kv[0], {}).get("par", 0))[:40]:
            if k in zam:
                print(f"  {'=' if zamiana_rownowazna(v) else ' '} {v.get('relacja')} {(v.get('rozklad') or {}).get('to_samo', 0):.2f} "
                      f"„{v['slowa_a']}” / „{v['slowa_b']}” | {v['a']} vs {v['b']} — par {zam[k]['par']}")


if __name__ == "__main__":
    main()
