"""Wycena raportu konkurencji przez b-card + b-match (za przełącznikiem MATCHING_SOURCE).

Wejście = to samo, co ma stary silnik w compute_pricing_comparisons_v2 (usługi podmiotu, booksy_id salonów z promienia,
wybrani konkurenci, salony). Wyjście = {id usługi podmiotu: MarketResult} — wiersz raportu buduje dalej ten sam
_build_row. Usługa bez karty albo bez kandydatów NIE ma wpisu → wywołujący zostawia dla niej wynik starego silnika.
Kroki: karty podmiotu (pamięć → b-card dla brakujących) → kandydaci z kart (fn_bcard_kandydaci + dobor.wybierz)
→ werdykty par (pamięć → b-match dla brakujących) → polityka.wynik_rynkowy.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from config import settings

from . import dobor, pamiec, polityka
from .klucz import klucz_uslugi, para_klucz, strona_bmatch, wiadomosci_bcard, wiadomosci_bmatch
from .runpod import Punkt

logger = logging.getLogger(__name__)
OBSZAR_DOMYSLNY: dict[str, str] = json.loads((Path(__file__).parent / "obszar_domyslny.json").read_text())


def uzupelnij_obszar(karta: dict[str, Any]) -> dict[str, Any]:
    """Obszar domyślny (manicure → dłonie…) — tak samo jak w kartach, na których uczył się b-match."""
    sk = [{**s, "obszar": s.get("obszar") or OBSZAR_DOMYSLNY.get(s.get("zabieg") or "")}
          for s in (karta.get("skladniki") or [])]
    return {**karta, "skladniki": [{k: v for k, v in s.items() if v is not None} for s in sk]}


async def _karty_podmiotu(client: Any, uslugi: list[dict[str, Any]], typ_salonu: str) -> dict[str, dict[str, Any]]:
    klucze = {klucz_uslugi(u): u for u in uslugi}
    karty = pamiec.karty(client, set(klucze))
    brak = [u for k, u in klucze.items() if k not in karty]
    if brak and settings.bcard_endpoint_id:
        async with Punkt(settings.bcard_endpoint_id, settings.runpod_api_key, "bcard") as p:
            nowe = await p.karty([wiadomosci_bcard(u, typ_salonu) for u in brak])
        zapis = []
        for u, k in zip(brak, nowe, strict=True):
            if k:
                karty[klucz_uslugi(u)] = uzupelnij_obszar(k)
                zapis.append({"klucz": klucz_uslugi(u), "karta": karty[klucz_uslugi(u)],
                              "wersja_modelu": settings.bcard_wersja, "wersja_slownika": settings.bcard_wersja_slownika})
        pamiec.zapisz_karty(client, zapis)
    return karty


async def wycen(service: Any, subject_services: list[dict[str, Any]], all_booksy: list[int],
                selected_booksy: set[int], salons_by_booksy: dict[int, dict[str, Any]],
                config: dict[str, Any] | None = None, typ_salonu: str = "") -> tuple[dict[int, Any], dict[str, Any]]:
    t0 = time.time()
    client = service.client
    karty = await _karty_podmiotu(client, subject_services, typ_salonu)
    karta_uslugi = {int(u["id"]): karty.get(klucz_uslugi(u)) for u in subject_services}
    zab = sorted({z for k in karta_uslugi.values() if k for z in dobor.zabiegi(k)})
    oferty = pamiec.kandydaci(client, all_booksy, zab) if zab else []
    po_zabiegu: dict[str, list[dict[str, Any]]] = {}
    for o in oferty:
        for z in o.get("zabiegi") or []:
            po_zabiegu.setdefault(z, []).append(o)

    wybor: dict[int, list[dict[str, Any]]] = {}
    for u in subject_services:
        k = karta_uslugi[int(u["id"])]
        if not k:
            continue
        pula = {o["service_id"]: o for z in dobor.zabiegi(k) for o in po_zabiegu.get(z, [])}
        kand = dobor.wybierz(u["name"], k, list(pula.values()), k=settings.bmatch_kandydatow, wybrani=selected_booksy)
        if kand:
            wybor[int(u["id"])] = kand

    ids = sorted({o["service_id"] for kand in wybor.values() for o in kand})
    dane = pamiec.uslugi(client, ids)
    karty_kand = pamiec.karty(client, {o["klucz"] for kand in wybor.values() for o in kand})

    # pary do oceny: (usługa podmiotu, oferta) → klucz pary; pamięć werdyktów, reszta do b-match
    pary: dict[str, tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]] = {}
    for u in subject_services:
        ku = karta_uslugi.get(int(u["id"]))
        if not ku:
            continue
        for o in wybor.get(int(u["id"]), []):
            d, kk = dane.get(int(o["service_id"])), karty_kand.get(o["klucz"])
            if d and kk and d.get("is_active", True):
                pary.setdefault(para_klucz(klucz_uslugi(u), o["klucz"]), (u, ku, d, kk))
    znane = pamiec.werdykty(client, set(pary), settings.bmatch_wersja)
    brak = [k for k in pary if k not in znane]
    if brak:
        async with Punkt(settings.bmatch_endpoint_id, settings.runpod_api_key, "bmatch") as p:
            wej = []
            for k in brak:
                u, ku, d, kk = pary[k]
                a, b = strona_bmatch(u, ku), strona_bmatch(d, kk)
                wej.append((wiadomosci_bmatch(a, b), wiadomosci_bmatch(b, a)))
            nowe = dict(zip(brak, await p.werdykty(wej), strict=True))
        pamiec.zapisz_werdykty(client, nowe, settings.bmatch_wersja)
        znane.update(nowe)

    wyniki: dict[int, Any] = {}
    for u in subject_services:
        sid = int(u["id"])
        ku = karta_uslugi[sid]
        if sid not in wybor or not ku:
            continue
        oceny = []
        for o in wybor[sid]:
            d = dane.get(int(o["service_id"]))
            pk = para_klucz(klucz_uslugi(u), o["klucz"])
            if not d or pk not in znane:
                continue
            p = znane[pk]
            dec = polityka.decyzja(p)
            info = salons_by_booksy.get(o["booksy_id"]) or {}
            probka = {"service_id": o["service_id"], "booksy_id": o["booksy_id"], "salon_name": info.get("name", ""),
                      "salon_id": info.get("id"), "service_name": d.get("name"), "price_grosze": d.get("price_grosze"),
                      "duration_minutes": d.get("duration_minutes"), "category_name": d.get("category_name"),
                      "is_package": bool(d.get("is_package")), "similarity": p[2],
                      "is_selected": o["booksy_id"] in selected_booksy}
            oceny.append((probka, dec, polityka.powod_odmiany(dobor.skladniki(ku), o.get("skladniki") or [])))
        subject = {"service_name": u.get("name") or "", "price_grosze": u.get("price_grosze"),
                   "duration_minutes": u.get("duration_minutes"), "category_name": u.get("category_name"),
                   "is_package": bool(u.get("is_package", False))}
        wyniki[sid] = polityka.wynik_rynkowy(subject, oceny, config, {"wersja_bmatch": settings.bmatch_wersja,
                                                                     "wersja_bcard": settings.bcard_wersja})
    stat = {"czas_s": round(time.time() - t0, 1), "uslug": len(subject_services), "z_karta": sum(1 for k in karta_uslugi.values() if k),
            "z_kandydatami": len(wybor), "par": len(pary), "par_nowych": len(brak), "ofert_w_puli": len(oferty)}
    logger.info("bmatch: %s", stat)
    return wyniki, stat
