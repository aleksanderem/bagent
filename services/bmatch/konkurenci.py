"""Dobór konkurentów z kart usług (MATCHING_SOURCE=bcard; pipelines/competitor_selection.select_competitors).

Stary dobór brał 200 najbliższych salonów tej samej kategorii Booksy i oceniał je po profilu kategorii i podobieństwie
nazw. Pomiar Beauty4ever (2026-10-04, werdykty b-match z pamięci): górę trafiał, ale pomijał salony z pokryciem menu
16–25% (Imperial Clinic, City Beauty, GK Expert…) i brał takie z 2–11% (dwa oddziały ESTETICAN, Laser DeLux).
Tu: wszystkie salony beauty z promienia, pokrycie = ile usług podmiotu ma u salonu ofertę o zgodnej karcie (zabieg,
metoda, obszar — jak dobór kandydatów do b-match), bez oddziałów samego podmiotu i bez dubli tej samej sieci.
Same karty to za mało (zgodność kart vs werdykty b-match: korelacja ~0,75; salony z ogromnym menu i ogólnikowymi kartami
wychodzą za wysoko, a część prawdziwych konkurentów spada poza setkę), więc dwa stopnie: krótka lista = PULA najlepszych
z kart + najlepsi ze starego doboru (ta sama kategoria — tych karty gubią), a kolejność ustala b-match
na parach podmiot × ich zgodne oferty (do NA_USLUGE najpodobniejszych na usługę). Werdykty trafiają do pamięci, więc
wycena raportu ich nie liczy drugi raz. Brak karty graficznej = ranking z samych kart.
Koszyk liczy się z tych samych progów co Faza 8a (pipelines/competitor_buckets). Za mało kart podmiotu albo błąd =
pusta lista → dotychczasowy dobór.
"""
from __future__ import annotations

import logging
import math
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from config import settings

from . import dobor, pamiec, polityka
from .dostawcy import punkt as punkt_dostawcy
from .klucz import klucz_uslugi, para_klucz, strona_bmatch, wiadomosci_bmatch

logger = logging.getLogger(__name__)

PULA = 60            # salonów o największym pokryciu z kart (+ najlepsi ze starego doboru) ocenia b-match
NA_USLUGE = 1        # par na (usługę podmiotu, salon): najpodobniejsza zgodna oferta
PROPOZYCJI = 30       # ile propozycji widzi okno wyboru konkurentów
MAX_NOWYCH_PAR = 8000  # ~4 min karty graficznej; reszta par → ranking tego salonu z kart
MIN_KART = 10        # mniej kart podmiotu = pokrycie niewiarygodne → stary dobór
MIN_UDZIAL_KART = 0.5
POLA_SALONU = ("id,booksy_id,name,city,reviews_count,reviews_rank,latitude,longitude,deleted_at,website,instagram_url,"
               "facebook_url")


def siec(nazwa: str | None) -> str:
    """Klucz sieci: „ESTETICAN PREMIUM® - Mokotów” i „Estetican Premium (Wola)” → „esteticanpremium”."""
    s = unicodedata.normalize("NFKD", (nazwa or "").lower())
    s = re.split(r"\s+[-–—|/]\s+|\(|,", s)[0]
    return re.sub(r"[^a-z0-9]", "", s.encode("ascii", "ignore").decode())


def _host(url: str | None) -> str:
    u = re.sub(r"^https?://(www\.)?", "", (url or "").strip().lower())
    return u.split("/")[0] if "." in u else ""


def ta_sama_siec(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Oddziały jednej sieci: ta sama nazwa (bez dopisku o lokalizacji) i wspólna strona / Instagram / Facebook albo
    ten sam adres (< 1 km). Bez danych kontaktowych — sama nazwa, ale tylko wyrazista (≥ 10 znaków), żeby nie sklejać
    niezależnych „Studio Urody”."""
    ka, kb = siec(a.get("name")), siec(b.get("name"))
    if not ka or ka != kb:
        return False
    if 0 < odleglosc_km(a.get("latitude"), a.get("longitude"), b.get("latitude"), b.get("longitude")) < 1.0:
        return True
    wspolne = [(_host(a.get("website")), _host(b.get("website"))),
               ((a.get("instagram_url") or "").lower().rstrip("/"), (b.get("instagram_url") or "").lower().rstrip("/")),
               ((a.get("facebook_url") or "").lower().rstrip("/"), (b.get("facebook_url") or "").lower().rstrip("/"))]
    if any(x and x == y for x, y in wspolne):
        return True
    return not any(x or y for x, y in wspolne) and len(ka) >= 10


def odleglosc_km(lat: float | None, lng: float | None, lat2: float | None, lng2: float | None) -> float:
    if lat is None or lng is None or lat2 is None or lng2 is None:
        return 0.0
    f1, f2 = math.radians(float(lat)), math.radians(float(lat2))
    df, dl = f2 - f1, math.radians(float(lng2) - float(lng))
    a = math.sin(df / 2) ** 2 + math.cos(f1) * math.cos(f2) * math.sin(dl / 2) ** 2
    return round(2 * 6371.0 * math.asin(math.sqrt(a)), 2)


def pokrycie(karty_podmiotu: dict[int, dict[str, Any]], oferty: list[dict[str, Any]]) -> dict[int, set[int]]:
    """{booksy_id: zbiór usług podmiotu, do których salon ma ofertę o zgodnej karcie}."""
    po_zabiegu: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for o in oferty:
        if not o.get("poza_beauty"):
            for z in o.get("zabiegi") or []:
                po_zabiegu[z].append(o)
    out: dict[int, set[int]] = defaultdict(set)
    for sid, karta in karty_podmiotu.items():
        sk = dobor.skladniki(karta)
        for z in dobor.zabiegi(karta):
            for o in po_zabiegu.get(z, []):
                b = int(o["booksy_id"])
                if sid not in out[b] and dobor.zgodne_karty(sk, o.get("skladniki") or []):
                    out[b].add(sid)
    return out


def _uslugi_podmiotu(client: Any, scrape_id: str) -> list[dict[str, Any]]:
    return [u for u in (client.table("salon_scrape_services").select(pamiec.POLA_USLUGI).eq("scrape_id", scrape_id)
                        .execute().data or []) if u.get("is_active", True) and u.get("price_grosze")]


def _karty_podmiotu(client: Any, scrape_id: str) -> tuple[dict[int, dict[str, Any]], int]:
    uslugi = _uslugi_podmiotu(client, scrape_id)
    karty = pamiec.karty(client, {klucz_uslugi(u) for u in uslugi})
    out = {int(u["id"]): karty[klucz_uslugi(u)] for u in uslugi
           if klucz_uslugi(u) in karty and not dobor.poza_beauty(karty[klucz_uslugi(u)])}
    return out, len(uslugi)


def _salony(client: Any, booksy_ids: list[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(booksy_ids), pamiec.W_ADRESIE_ID):
        for r in (client.table("salons").select(POLA_SALONU).in_("booksy_id", booksy_ids[i:i + pamiec.W_ADRESIE_ID])
                  .execute().data or []):
            out[int(r["booksy_id"])] = r
    return out


def kandydaci(client: Any, podmiot: dict[str, Any], scrape_id: str, promien: list[int],
              dodatkowi: list[int] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(salony z promienia wg pokrycia menu podmiotu z kart, malejąco: {booksy_id, salon_id, name, city,
    reviews_count, reviews_rank, distance_km, pokryte, wszystkie, udzial}; oferty z promienia do drugiego stopnia).
    `podmiot` = {booksy_id, name, salon_lat, salon_lng}; `dodatkowi` = booksy_id spoza czołówki z kart, które i tak
    mają trafić na listę (najlepsi ze starego doboru)."""
    karty, uslug = _karty_podmiotu(client, scrape_id)
    if len(karty) < MIN_KART or len(karty) < MIN_UDZIAL_KART * max(1, uslug):
        logger.warning("dobór z kart: za mało kart podmiotu (%d z %d usług) — stary dobór", len(karty), uslug)
        return [], []
    zabiegi = sorted({z for k in karty.values() for z in dobor.zabiegi(k)})
    bid = int(podmiot["booksy_id"])
    dod = [int(b) for b in (dodatkowi or []) if int(b) != bid]
    oferty = pamiec.kandydaci(client, sorted((set(promien) | set(dod)) - {bid}), zabiegi)
    pok = pokrycie(karty, oferty)
    ranking = sorted(((b, s) for b, s in pok.items() if b != bid and s), key=lambda kv: -len(kv[1]))[:PULA * 3]
    ranking = [(b, pok.get(b, set())) for b in dict.fromkeys(dod)] + [x for x in ranking if x[0] not in set(dod)]
    salony = _salony(client, [bid] + [b for b, _ in ranking])
    wziete: list[dict[str, Any]] = [salony.get(bid) or {"name": podmiot.get("name")}]
    out: list[dict[str, Any]] = []
    for b, s in ranking:
        r = salony.get(b)
        if not r or r.get("deleted_at"):
            continue
        if any(ta_sama_siec(r, w) for w in wziete):  # oddział podmiotu albo drugi oddział sieci już wziętej
            continue
        wziete.append(r)
        out.append({"booksy_id": b, "salon_id": int(r["id"]), "name": r.get("name") or "", "city": r.get("city"),
                    "reviews_count": int(r.get("reviews_count") or 0), "reviews_rank": r.get("reviews_rank"),
                    "distance_km": odleglosc_km(podmiot.get("salon_lat"), podmiot.get("salon_lng"),
                                                r.get("latitude"), r.get("longitude")),
                    "pokryte": len(s), "wszystkie": len(karty), "udzial": round(len(s) / len(karty), 4)})
        if len(out) >= PULA + len(dod):
            break
    logger.info("dobór z kart: %d kart podmiotu, %d ofert w promieniu, %d salonów z pokryciem, najlepszy %s",
                len(karty), len(oferty), len(pok), out[0]["udzial"] if out else None)
    return out, oferty


def _pary(uslugi: dict[int, dict[str, Any]], karty: dict[int, dict[str, Any]], oferty: list[dict[str, Any]],
          salony: set[int]) -> dict[str, tuple[int, int, dict[str, Any]]]:
    """{klucz pary: (usługa podmiotu, booksy_id, oferta)} — do NA_USLUGE najpodobniejszych zgodnych ofert salonu."""
    po_salonie: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for o in oferty:
        if int(o["booksy_id"]) in salony and not o.get("poza_beauty"):
            po_salonie[int(o["booksy_id"])].append(o)
    out: dict[str, tuple[int, int, dict[str, Any]]] = {}
    for sid, k in karty.items():
        sk, zab, u = dobor.skladniki(k), set(dobor.zabiegi(k)), uslugi[sid]
        for b, lista in po_salonie.items():
            zg = [o for o in lista if zab & set(o.get("zabiegi") or []) and dobor.zgodne_karty(sk, o.get("skladniki") or [])]
            zg.sort(key=lambda o: -dobor.podobienstwo(u["name"], sk, k.get("marka"), o.get("nazwa") or "",
                                                      o.get("skladniki") or [], o.get("marka")))
            for o in zg[:NA_USLUGE]:
                out.setdefault(para_klucz(klucz_uslugi(u), o["klucz"]), (sid, b, o))
    return out


async def pokrycie_bmatch(client: Any, scrape_id: str, oferty: list[dict[str, Any]], salony: set[int]
                          ) -> tuple[dict[int, set[int]], dict[str, Any]]:
    """{booksy_id: usługi podmiotu z werdyktem „ta sama” / „odmiana”} dla salonów z krótkiej listy + statystyka.
    Werdykty z pamięci; brakujące liczy pierwszy dostawca, który wstanie (zapis do pamięci). Błąd → wyjątek."""
    uslugi = {int(u["id"]): u for u in _uslugi_podmiotu(client, scrape_id)}
    karty_map = pamiec.karty(client, {klucz_uslugi(u) for u in uslugi.values()})
    karty = {sid: karty_map[klucz_uslugi(u)] for sid, u in uslugi.items()
             if klucz_uslugi(u) in karty_map and not dobor.poza_beauty(karty_map[klucz_uslugi(u)])}
    pary = _pary(uslugi, karty, oferty, salony)
    znane = pamiec.werdykty(client, set(pary), settings.bmatch_wersja)
    brak = [k for k in pary if k not in znane][:MAX_NOWYCH_PAR]
    stat: dict[str, Any] = {"par": len(pary), "par_nowych": len(brak), "dostawca": None}
    if brak:
        ids = sorted({int(pary[k][2]["service_id"]) for k in brak})
        dane = pamiec.uslugi(client, ids)
        karty_ofert = pamiec.karty(client, {pary[k][2]["klucz"] for k in brak})
        gotowe = [k for k in brak if int(pary[k][2]["service_id"]) in dane and pary[k][2]["klucz"] in karty_ofert]
        wej = []
        for k in gotowe:
            sid, _, o = pary[k]
            a = strona_bmatch(uslugi[sid], karty[sid])
            b = strona_bmatch(dane[int(o["service_id"])], karty_ofert[o["klucz"]])
            wej.append((wiadomosci_bmatch(a, b), wiadomosci_bmatch(b, a)))
        p = punkt_dostawcy("bmatch")
        async with p:
            nowe = dict(zip(gotowe, await p.werdykty(wej), strict=True))
        stat.update({"dostawca": p.dostawca, "gpu_s": p.sekundy})
        pamiec.zapisz_werdykty(client, nowe, settings.bmatch_wersja)
        znane.update(nowe)
    out: dict[int, set[int]] = defaultdict(set)
    for k, (sid, b, _) in pary.items():
        if k in znane and polityka.decyzja(znane[k]) in ("ta_sama", "odmiana"):
            out[b].add(sid)
    stat["uslug"] = len(uslugi)
    return out, stat


async def kandydaci_bmatch(client: Any, podmiot: dict[str, Any], scrape_id: str, promien: list[int],
                           dodatkowi: list[int] | None = None) -> list[dict[str, Any]]:
    """Krótka lista (karty + `dodatkowi`) → kolejność z werdyktów b-match (pole „zrodlo”: „b-match” albo „karty”)."""
    lista, oferty = kandydaci(client, podmiot, scrape_id, promien, dodatkowi)
    if not lista:
        return []
    try:
        pok, stat = await pokrycie_bmatch(client, scrape_id, oferty, {x["booksy_id"] for x in lista})
    except Exception as e:  # noqa: BLE001 — bez karty graficznej zostaje ranking z kart
        logger.warning("dobór z kart: b-match niedostępny (%s: %s) — ranking z samych kart", type(e).__name__, str(e)[:200])
        return [{**x, "zrodlo": "karty"} for x in lista]
    wszystkie = max(1, int(stat["uslug"]))
    out = [{**x, "pokryte": len(pok.get(x["booksy_id"], ())), "wszystkie": wszystkie,
            "udzial": round(len(pok.get(x["booksy_id"], ())) / wszystkie, 4), "pokrycie_kart": x["udzial"],
            "zrodlo": "b-match"} for x in lista]
    out.sort(key=lambda x: (-x["pokryte"], -x["pokrycie_kart"]))
    logger.info("dobór z kart + b-match: %s; najlepszy %s (%s)", stat, out[0]["name"], out[0]["udzial"])
    _zapisz_propozycje(client, int(podmiot["booksy_id"]), scrape_id, out)
    return out


def _zapisz_propozycje(client: Any, booksy_id: int, scrape_id: str, wyniki: list[dict[str, Any]]) -> None:
    """Okno wyboru konkurentów pokazuje to samo, co wybrał dobór w raporcie (bcard_propozycje, mig 208)."""
    try:
        client.table("bcard_propozycje").upsert({"booksy_id": booksy_id, "scrape_id": scrape_id,
                                                 "wyniki": wyniki[:PROPOZYCJI],
                                                 "policzono": datetime.now(timezone.utc).isoformat()},
                                                on_conflict="booksy_id").execute()
    except Exception as e:  # noqa: BLE001 — brak tabeli (mig 208) albo błąd zapisu nie rusza raportu
        logger.warning("propozycje konkurentów %s: zapis nieudany: %s", booksy_id, str(e)[:200])


# ── Podpowiedzi w oknie wyboru konkurentów (bez karty graficznej) ──────────────

# Para bez werdyktu: identyczna karta ≈ prawie pewne pokrycie, tylko zgodna ≈ 0,3 (Beauty4ever, 21 salonów: korelacja
# z b-match 0,83 wobec 0,78 przy jednej wadze dla wszystkich zgodnych).
WAGA_ROWNE = 0.85
WAGA_ZGODNE = 0.3
MIN_ZNANYCH_BMATCH = 0.8  # od tylu znanych par salon ma źródło „b-match”, niżej „szacunek”


def _z_ostatniego_raportu(client: Any, booksy_id: int) -> list[int]:
    """booksy_id konkurentów z ostatniego raportu tego salonu (wybrani przez b-match) — na krótką listę."""
    s = client.table("salons").select("id").eq("booksy_id", booksy_id).limit(1).execute().data or []
    if not s:
        return []
    r = (client.table("competitor_reports").select("id").eq("subject_salon_id", s[0]["id"])
         .order("updated_at", desc=True).limit(1).execute().data or [])
    if not r:
        return []
    ids = [m["competitor_salon_id"] for m in (client.table("competitor_matches").select("competitor_salon_id")
                                               .eq("report_id", r[0]["id"]).execute().data or [])]
    return [int(x["booksy_id"]) for x in (client.table("salons").select("booksy_id").in_("id", ids).execute().data
                                          or [])] if ids else []


def propozycje(client: Any, booksy_id: int, promien: list[int]) -> dict[str, Any] | None:
    """Ranking konkurentów do okna wyboru: krótka lista z kart (+ konkurenci z ostatniego raportu), kolejność
    z werdyktów b-match z pamięci, a gdzie ich brak — szacunek z kart (WAGA_KART). Zapis do bcard_propozycje.
    None = brak skanu albo za mało kart (okno zostaje przy dotychczasowych podpowiedziach)."""
    sk = (client.table("salon_scrapes").select("id,salon_name,salon_lat,salon_lng").eq("booksy_id", booksy_id)
          .eq("is_chain_head", True).limit(1).execute().data or [])
    if not sk:
        return None
    scrape_id = sk[0]["id"]
    podmiot = {"booksy_id": booksy_id, "name": sk[0]["salon_name"], "salon_lat": sk[0]["salon_lat"],
               "salon_lng": sk[0]["salon_lng"]}
    lista, oferty = kandydaci(client, podmiot, scrape_id, promien, _z_ostatniego_raportu(client, booksy_id))
    if not lista:
        return None
    uslugi = {int(u["id"]): u for u in _uslugi_podmiotu(client, scrape_id)}
    km = pamiec.karty(client, {klucz_uslugi(u) for u in uslugi.values()})
    karty = {sid: km[klucz_uslugi(u)] for sid, u in uslugi.items()
             if klucz_uslugi(u) in km and not dobor.poza_beauty(km[klucz_uslugi(u)])}
    pary = _pary(uslugi, karty, oferty, {x["booksy_id"] for x in lista})
    znane = pamiec.werdykty(client, set(pary), settings.bmatch_wersja)
    pokryte: dict[int, set[int]] = defaultdict(set)
    nieznane: dict[int, dict[int, float]] = defaultdict(dict)
    par_salonu: dict[int, int] = defaultdict(int)
    znanych_salonu: dict[int, int] = defaultdict(int)
    for k, (sid, b, o) in pary.items():
        par_salonu[b] += 1
        if k in znane:
            znanych_salonu[b] += 1
            if polityka.decyzja(znane[k]) in ("ta_sama", "odmiana"):
                pokryte[b].add(sid)
        else:
            rowne = dobor.skladniki(karty[sid]) == [tuple(x) for x in (o.get("skladniki") or [])]
            nieznane[b][sid] = max(nieznane[b].get(sid, 0.0), WAGA_ROWNE if rowne else WAGA_ZGODNE)
    n = max(1, len(uslugi))
    wyniki = []
    for x in lista:
        b = x["booksy_id"]
        szac = (len(pokryte[b]) + sum(w for sid, w in nieznane[b].items() if sid not in pokryte[b])) / n
        znanych = znanych_salonu[b] / par_salonu[b] if par_salonu[b] else 0.0
        wyniki.append({**x, "udzial": round(szac, 4), "pokrycie_kart": x["udzial"],
                       "zrodlo": "b-match" if znanych >= MIN_ZNANYCH_BMATCH else "szacunek"})
    wyniki.sort(key=lambda x: -x["udzial"])
    wyniki = wyniki[:PROPOZYCJI]
    _zapisz_propozycje(client, booksy_id, scrape_id, wyniki)
    logger.info("propozycje konkurentów %s: %d (b-match %d, szacunek %d)", booksy_id, len(wyniki),
                sum(1 for w in wyniki if w["zrodlo"] == "b-match"), sum(1 for w in wyniki if w["zrodlo"] != "b-match"))
    return {"scrape_id": scrape_id, "wyniki": wyniki}
