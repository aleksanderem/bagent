"""Katalog usług — rozkład KATEGORII cennika raz na unikalny tekst kategorii (GLM z abonamentu, 0 USD).

Kontekst oferty (zabieg, gdy nazwa go nie mówi; dla kogo; miejsce) bierzemy z rozkładu jej kategorii — ten sam
dla wszystkich ofert kategorii, zamiast frazy z kategorii wyciąganej przy każdej ofercie (niespójne między
przebiegami; 4 z 6 błędów „ta sama” na 1042 parach, pomiar 29.09). Pamięć po id kategorii (skrót tekstu), wznawialna.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

B = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(B), str(B / "scripts")]
from services.katalog_uslug.ekstrakcja import (WERSJA_POZYCJI, Oferta, prompt_kategorii, prompt_pozycji_kategorii,  # noqa: E402
                                               prompt_salonow, rozdziel_zlepki, waliduj, waliduj_pozycje)
from services.katalog_uslug.normalizacja import bez_kontaktow, normalizuj  # noqa: E402

KLUCZ = Path.home() / ".config" / "zai" / "api_key"
PACZKA = 20
PACZKA_POZYCJI = 40  # odpowiedź to jedno słowo i fraza na kategorię
PLIK_POZYCJI = f"pozycje_kategorii_v{WERSJA_POZYCJI}.json"  # obok kategorie.json: co salon sprzedaje w sekcji


def id_kategorii(tekst: str) -> str:
    """Skrót tekstu kategorii bez telefonów i e-maili — dane w repo są maskowane (maskuj_kontakty.py), a klucz
    musi być ten sam dla tekstu przed maskowaniem i po nim."""
    return "k:" + hashlib.sha1(normalizuj(bez_kontaktow(tekst)).encode("utf-8")).hexdigest()[:12]


def kategorie_ofert(oferty: list[Oferta]) -> list[Oferta]:
    wynik: dict[str, Oferta] = {}
    for o in oferty:
        if normalizuj(o.kategoria) and (k := id_kategorii(o.kategoria)) not in wynik:
            wynik[k] = Oferta(id=k, typ_salonu="", kategoria="", nazwa=o.kategoria, wariant="", zabieg_booksy="", opis="",
                              cena_zl=None)
    return sorted(wynik.values(), key=lambda k: k.id)


async def wyciagnij(kategorie: list[Oferta], plik: Path, rownolegle: int = 3, prompt=prompt_kategorii,
                    walidator=waliduj, paczka_n: int = PACZKA) -> dict[str, dict]:
    import openai
    from taxonomy_backfill import KlientGLM
    pamiec: dict[str, dict] = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = [k for k in kategorie if k.id not in pamiec]
    klucz = (os.environ.get("ZAI_API_KEY") or KLUCZ.read_text(encoding="utf-8")).strip()
    klient, sem = KlientGLM(klucz, temperature=0.0), asyncio.Semaphore(rownolegle)
    print(f"kategorii {len(kategorie)}, do rozkładu {len(brak)} ({-(-len(brak) // paczka_n)} wywołań)", flush=True)

    async def paczka(p: list[Oferta]) -> None:
        async with sem:
            for _proba in range(2):
                try:
                    wynik, _bledy = walidator(p, await klient.generate_json(prompt(p), max_tokens=12000))
                    pamiec.update(wynik)
                    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
                    return
                except openai.RateLimitError:
                    raise
                except Exception as e:  # noqa: BLE001 — ponowienie raz; brak rekordu = kontekst z oferty
                    print(f"  paczka kategorii: {type(e).__name__}: {str(e)[:120]}", flush=True)

    await asyncio.gather(*(paczka(brak[i:i + paczka_n]) for i in range(0, len(brak), paczka_n)))
    return pamiec


def kontekst(oferty: list[Oferta], plik: Path) -> dict[str, dict | None]:
    """id oferty → rozkład jej kategorii; sekcja z czymś, co nie jest usługą (plik pozycji obok), dokłada
    `pozycja_kategorii` — taka oferta jest poza porównaniem (podpis.POZA_POROWNANIEM)."""
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    pp = plik.with_name(PLIK_POZYCJI)
    pozycje = json.loads(pp.read_text(encoding="utf-8")) if pp.exists() else {}
    wynik: dict[str, dict | None] = {}
    for o in oferty:
        kid = id_kategorii(o.kategoria) if normalizuj(o.kategoria) else None
        rek = pamiec.get(kid) if kid else None
        poz = (pozycje.get(kid) or {}).get("pozycja", "uslugi") if kid else "uslugi"
        if poz != "uslugi":
            rek = {**(rek or {"nazwa": o.kategoria, "zabieg": {"fraza": ""}, "cechy": [], "szum": []}),
                   "pozycja_kategorii": poz}
        wynik[o.id] = rek
    return wynik


async def wyciagnij_pozycje(katalogi: list[Path], pamiec: Path, rownolegle: int = 3) -> None:
    """Pozycja każdej kategorii z kategorie.json podanych katalogów (jedna pamięć), potem kopia do każdego katalogu."""
    nazwy: dict[str, str] = {}
    for d in katalogi:
        for kid, r in json.loads((d / "kategorie.json").read_text(encoding="utf-8")).items():
            nazwy.setdefault(kid, str(r.get("nazwa") or ""))
    kategorie = [Oferta(id=kid, typ_salonu="", kategoria="", nazwa=n, wariant="", zabieg_booksy="", opis="", cena_zl=None)
                 for kid, n in sorted(nazwy.items()) if normalizuj(n)]
    wynik = await wyciagnij(kategorie, pamiec, rownolegle, prompt_pozycji_kategorii, waliduj_pozycje, PACZKA_POZYCJI)
    for d in katalogi:
        wlasne = json.loads((d / "kategorie.json").read_text(encoding="utf-8"))
        (d / PLIK_POZYCJI).write_text(json.dumps({k: v for k, v in wynik.items() if k in wlasne}, ensure_ascii=False,
                                                 indent=0), encoding="utf-8")
    inne = sorted((v["pozycja"], nazwy[k]) for k, v in wynik.items() if v["pozycja"] != "uslugi")
    print(f"kategorii z pozycją {len(wynik)}/{len(kategorie)}; nie usługi: {len(inne)}")
    for poz, n in inne:
        print(f"  {poz:10} {n}")


def id_salonu(nazwa: str) -> str:
    return "s:" + hashlib.sha1(normalizuj(rozdziel_zlepki(bez_kontaktow(nazwa))).encode("utf-8")).hexdigest()[:12]


def salony_ofert(nazwy: list[str]) -> list[Oferta]:
    """Nazwa salonu rozkładana raz (jak kategoria): metoda, którą salon pracuje, gdy oferty jej nie podają."""
    wynik: dict[str, Oferta] = {}
    for n in nazwy:
        if normalizuj(n) and (k := id_salonu(n)) not in wynik:
            wynik[k] = Oferta(id=k, typ_salonu="", kategoria="", nazwa=rozdziel_zlepki(n), wariant="", zabieg_booksy="",
                              opis="", cena_zl=None)
    return sorted(wynik.values(), key=lambda k: k.id)


def kontekst_salonu(salon_oferty: dict[str, str], plik: Path) -> dict[str, dict | None]:
    """id oferty → rozkład nazwy jej salonu (z nazwą po rozdzieleniu zlepków), albo None."""
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    return {oid: ({**pamiec[id_salonu(n)], "nazwa": rozdziel_zlepki(n)} if normalizuj(n) and id_salonu(n) in pamiec else None)
            for oid, n in salon_oferty.items()}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Pozycja kategorii cennika (GLM z abonamentu, 0 USD)")
    ap.add_argument("katalogi", nargs="+", type=Path, help="katalogi z kategorie.json")
    ap.add_argument("--pamiec", type=Path, required=True, help="wspólna pamięć odpowiedzi (json)")
    ap.add_argument("--rownolegle", type=int, default=3)
    a = ap.parse_args()
    asyncio.run(wyciagnij_pozycje(a.katalogi, a.pamiec, a.rownolegle))
