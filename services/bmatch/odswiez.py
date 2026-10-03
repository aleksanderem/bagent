"""Nocne odświeżanie ofert b-card po nowych skanach salonów (cron w workers/main.py, mig 206 w BEAUTY_AUDIT).

Salon z nowym skanem ma nowe id usług i czasem nowe opisy. Bez odświeżania dobór kandydatów widzi oferty z poprzedniego
skanu, a nowe usługi nie mają karty — raport liczy je wtedy starym silnikiem.
Kroki: salony do przeliczenia (fn_bcard_do_odswiezenia) → usługi aktualnego skanu → karty z pamięci → brakujące
z punktu b-card (jedno rozgrzanie na noc, limit kart) → oferty salonu + stan w bcard_skan.
Działa tylko przy MATCHING_SOURCE=bcard|bcard_cien. Zapis wyłącznie do bcard_karta, bcard_oferta, bcard_skan.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from config import settings

from . import oferty, pamiec
from .klucz import klucz_uslugi, wiadomosci_bcard
from .runpod import Punkt
from .wycena import uzupelnij_obszar

logger = logging.getLogger(__name__)

DNI = 14            # skany starsze nie wracają do kolejki (stan bcard_skan i tak pamięta ostatni przeliczony)
SALONOW = 2000      # na noc; reszta następnej nocy
KART_MAX = 20000    # na noc; ~7 min karty graficznej (~48 kart/s)
KART_MIN = 300      # mniej braków = bez rozgrzewania (~2,5 min); czekają do większej paczki albo niedzieli
NIEDZIELA = 6


def _czy_generowac(brakow: int, teraz: datetime) -> bool:
    return bool(brakow and settings.bcard_endpoint_id and settings.runpod_api_key
                and (brakow >= KART_MIN or teraz.weekday() == NIEDZIELA))


async def _generuj(client: Any, brak: dict[str, dict[str, Any]], stat: dict[str, Any]) -> dict[str, dict[str, Any]]:
    klucze = list(brak)[:KART_MAX]
    p = Punkt(settings.bcard_endpoint_id, settings.runpod_api_key, "bcard")
    async with p:
        wynik = await p.karty([wiadomosci_bcard(brak[k]) for k in klucze])
    stat["gpu_bcard_s"] = p.sekundy
    nowe = {k: uzupelnij_obszar(c) for k, c in zip(klucze, wynik, strict=True) if c}
    await asyncio.to_thread(pamiec.zapisz_karty, client, [{"klucz": k, "karta": c, "wersja_modelu": settings.bcard_wersja,
                                  "wersja_slownika": settings.bcard_wersja_slownika} for k, c in nowe.items()])
    stat["kart_nowych"] = len(nowe)
    return nowe


def _zbierz(client: Any, salonow: int) -> tuple[list[tuple[int, str, list[dict[str, Any]]]],
                                                 dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """→ (salony z lekkimi usługami, karty z pamięci, usługi bez karty po kluczu). Pełne dane tylko dla braków."""
    do = client.rpc("fn_bcard_do_odswiezenia", {"p_dni": DNI, "p_limit": salonow}).execute().data or []
    salony: list[tuple[int, str, list[dict[str, Any]]]] = []
    karty: dict[str, dict[str, Any]] = {}
    brak: dict[str, dict[str, Any]] = {}
    for r in do:
        uslugi = oferty.uslugi_skanu(client, r["scrape_id"])
        lekkie = [{"id": u["id"], "name": u["name"], "klucz": klucz_uslugi(u)} for u in uslugi]
        karty.update(pamiec.karty(client, {x["klucz"] for x in lekkie} - set(karty)))
        brak.update({x["klucz"]: u for x, u in zip(lekkie, uslugi, strict=True) if x["klucz"] not in karty})
        salony.append((int(r["booksy_id"]), r["scrape_id"], lekkie))
    return salony, karty, brak


def _zapisz(client: Any, salony: list[tuple[int, str, list[dict[str, Any]]]], karty: dict[str, dict[str, Any]],
            teraz: datetime) -> None:
    for bid, sid, lekkie in salony:
        gotowe = [oferty.wiersz_oferty(x, bid, x["klucz"], karty[x["klucz"]]) for x in lekkie if x["klucz"] in karty]
        oferty.zapisz(client, bid, gotowe)
        client.table("bcard_skan").upsert({"booksy_id": bid, "scrape_id": sid, "ofert": len(gotowe),
                                           "bez_karty": len(lekkie) - len(gotowe),
                                           "odswiezono": teraz.isoformat()}, on_conflict="booksy_id").execute()


async def odswiez(client: Any, *, sucho: bool = False, salonow: int = SALONOW,
                  teraz: datetime | None = None) -> dict[str, Any]:
    """→ statystyka przebiegu. `sucho` = tylko liczy (bez b-card i bez zapisu). Klient bazy jest synchroniczny —
    praca z bazą w wątku, żeby nie blokować innych cronów workera (np. pobierania skanów co 15 s)."""
    t0 = time.time()
    teraz = teraz or datetime.now(timezone.utc)
    salony, karty, brak = await asyncio.to_thread(_zbierz, client, salonow)
    stat: dict[str, Any] = {"salonow": len(salony), "uslug": sum(len(x[2]) for x in salony), "brakow": len(brak),
                            "kart_nowych": 0, "gpu_bcard_s": 0.0, "sucho": sucho}
    if not sucho and _czy_generowac(len(brak), teraz):
        karty.update(await _generuj(client, brak, stat))
    if not sucho:
        await asyncio.to_thread(_zapisz, client, salony, karty, teraz)
    stat["czas_s"] = round(time.time() - t0, 1)
    return stat


async def odswiez_cron(ctx: dict[str, Any]) -> dict[str, Any]:
    """Cron arq. Przy MATCHING_SOURCE=stary nic nie robi (oferty nie są wtedy czytane)."""
    zrodlo = (settings.matching_source or "stary").strip().lower()
    if zrodlo not in ("bcard", "bcard_cien"):
        return {"pominiete": zrodlo}
    from services.supabase import SupabaseService

    stat = await odswiez(SupabaseService().client)
    logger.info("bcard odświeżanie ofert: %s", stat)
    return stat
