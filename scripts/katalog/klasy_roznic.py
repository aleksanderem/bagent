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
from services.katalog_uslug.klasy import NIE_ZMIENIA, WERSJA_PYTANIA, pytanie_klasy, rozstrzygnij  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj, rdzen_slowa  # noqa: E402
from services.katalog_uslug.podpis import _wybrane_frazy, podpis, roznica_do_pytania  # noqa: E402
from services.typesafe_drzewo.kontekst_v12 import stan_v12  # noqa: E402

_spec = importlib.util.spec_from_file_location("ocena_podpisu", B / "scripts" / "katalog" / "ocena_podpisu.py")
op = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(op)

KLUCZ = Path.home() / ".config" / "typesafe" / "api_key"
MODEL = "jev-1.13.0"
CENA_TOK = 0.042 / 1e6
PLIK = B / "scripts" / "katalog" / "dane" / "2026-09-29" / f"w{WERSJA_PROMPTU}" / f"klasy_p{WERSJA_PYTANIA}.json"


def _dopisek(rek: dict, poziom: str, slowa: set[str]) -> str:
    """Tylko słowa różnicy, w kolejności i brzmieniu z oferty (bez słów wspólnych z drugą ofertą)."""
    z = (rek.get("zabieg") or {}).get("fraza") or ""
    frazy = [z] + [f for _poz, f in _wybrane_frazy(rek)] + list(rek.get("nieprzypisane") or [])
    wynik: list[str] = []
    for f in frazy:
        for tok in f.split():
            if {rdzen_slowa(t) for t in normalizuj(tok).split()} & slowa and tok not in wynik:
                wynik.append(tok)
    return " ".join(wynik) or " ".join(sorted(slowa))


def klasy_z_par(wariant: str, slownik: dict[str, str] | None = None, kon: dict | None = None) -> dict[str, dict]:
    kon = kon or {}
    op.tp.OUT = op.tp.OUT.parent / f"w{WERSJA_PROMPTU}" / "wszystkie"
    pary = op.pary_ocenione()
    rek, _ = op.tp.rekordy(wariant, op.tp.oferty_probki(0))
    klasy: dict[str, dict] = {}
    for q in pary:
        ra, rb = rek.get(q["oa"].id), rek.get(q["ob"].id)
        if not (ra and rb):
            continue
        pa, pb = podpis(ra, slownik, kon.get(q["oa"].id)), podpis(rb, slownik, kon.get(q["ob"].id))
        for klasa, strona in roznica_do_pytania(pa, pb):
            klucz = json.dumps(klasa, ensure_ascii=False)
            if klucz in klasy:
                klasy[klucz]["par"] += 1
                continue
            o_z, r_z, o_bez = (q["oa"], ra, q["ob"]) if strona is pa else (q["ob"], rb, q["oa"])
            klasy[klucz] = {"klasa": klasa, "par": 1, "oferta": o_z.id, "dopisek": _dopisek(r_z, klasa[1], set(klasa[2].split())),
                            "zabieg": o_z.nazwa + (f" — {o_z.wariant}" if o_z.wariant else ""), "druga": o_bez.nazwa + (f" — {o_bez.wariant}" if o_bez.wariant else ""),
                            "stan": stan_v12({"nazwa": o_z.nazwa, "kategoria": o_z.kategoria, "opis": o_z.opis,
                                              "warianty": [{"label": o_z.wariant}] if o_z.wariant else [],
                                              "zabieg_booksy": o_z.zabieg_booksy, "typ_salonu": o_z.typ_salonu})}
    return klasy


async def zapytaj(klasy: dict[str, dict], budzet: float, plik: Path | None = None) -> float:
    plik = plik or PLIK
    from typesafe_sdk import AsyncTypeSafeClient
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    nowe = [k for k in klasy if k not in pamiec]
    szac = len(nowe) * 700 * CENA_TOK
    print(f"klas {len(klasy)}, nowych do pytania {len(nowe)}, szac. {szac:.3f} USD (budżet {budzet})", flush=True)
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


if __name__ == "__main__":
    main()
