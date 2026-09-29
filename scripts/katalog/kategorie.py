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
from services.katalog_uslug.ekstrakcja import Oferta, prompt_kategorii, prompt_salonow, rozdziel_zlepki, waliduj  # noqa: E402
from services.katalog_uslug.normalizacja import normalizuj  # noqa: E402

KLUCZ = Path.home() / ".config" / "zai" / "api_key"
PACZKA = 20


def id_kategorii(tekst: str) -> str:
    return "k:" + hashlib.sha1(normalizuj(tekst).encode("utf-8")).hexdigest()[:12]


def kategorie_ofert(oferty: list[Oferta]) -> list[Oferta]:
    wynik: dict[str, Oferta] = {}
    for o in oferty:
        if normalizuj(o.kategoria) and (k := id_kategorii(o.kategoria)) not in wynik:
            wynik[k] = Oferta(id=k, typ_salonu="", kategoria="", nazwa=o.kategoria, wariant="", zabieg_booksy="", opis="",
                              cena_zl=None)
    return sorted(wynik.values(), key=lambda k: k.id)


async def wyciagnij(kategorie: list[Oferta], plik: Path, rownolegle: int = 3, prompt=prompt_kategorii) -> dict[str, dict]:
    import openai
    from taxonomy_backfill import KlientGLM
    pamiec: dict[str, dict] = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    brak = [k for k in kategorie if k.id not in pamiec]
    klucz = (os.environ.get("ZAI_API_KEY") or KLUCZ.read_text(encoding="utf-8")).strip()
    klient, sem = KlientGLM(klucz, temperature=0.0), asyncio.Semaphore(rownolegle)
    print(f"kategorii {len(kategorie)}, do rozkładu {len(brak)} ({-(-len(brak) // PACZKA)} wywołań)", flush=True)

    async def paczka(p: list[Oferta]) -> None:
        async with sem:
            for _proba in range(2):
                try:
                    wynik, _bledy = waliduj(p, await klient.generate_json(prompt(p), max_tokens=12000))
                    pamiec.update(wynik)
                    plik.write_text(json.dumps(pamiec, ensure_ascii=False), encoding="utf-8")
                    return
                except openai.RateLimitError:
                    raise
                except Exception as e:  # noqa: BLE001 — ponowienie raz; brak rekordu = kontekst z oferty
                    print(f"  paczka kategorii: {type(e).__name__}: {str(e)[:120]}", flush=True)

    await asyncio.gather(*(paczka(brak[i:i + PACZKA]) for i in range(0, len(brak), PACZKA)))
    return pamiec


def kontekst(oferty: list[Oferta], plik: Path) -> dict[str, dict | None]:
    pamiec = json.loads(plik.read_text(encoding="utf-8")) if plik.exists() else {}
    return {o.id: pamiec.get(id_kategorii(o.kategoria)) if normalizuj(o.kategoria) else None for o in oferty}


def id_salonu(nazwa: str) -> str:
    return "s:" + hashlib.sha1(normalizuj(rozdziel_zlepki(nazwa)).encode("utf-8")).hexdigest()[:12]


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
