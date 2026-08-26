#!/usr/bin/env python3
"""Backfill taksonomii usług — destylacja nazw przez MiniMax M3.

CO ROBI: dla każdej unikalnej nazwy usługi w regionie wyciąga osie (metoda, obszar,
marka preparatu, ...) i zapisuje do `service_taxonomy`. Klucz = znormalizowana nazwa,
więc ta sama nazwa z wielu salonów destyluje się raz.

DLACZEGO TAK OSTROŻNIE — HARD GATE #1 (CLAUDE.md):
Ten skrypt wywołuje model dziesiątki tysięcy razy i DZIELI SUBSKRYPCJĘ Z PRODUKCJĄ
(bagent syntetyzuje raporty tym samym kluczem). Stąd twarde wymagania:
  * kursor zapisywany po KAŻDEJ paczce — przerwanie nie cofa pracy
  * limit dobowy wywołań — domyślnie poniżej pułapu subskrypcji
  * ograniczona równoległość — subskrypcja daje 3-4 sloty, zostawiamy je produkcji
  * log postępu z tempem i ETA — brak tego był realnym problemem 2026-08-25
  * graceful stop na SIGTERM/SIGINT — dokańcza bieżące paczki i zapisuje kursor

POMIARY, NA KTÓRYCH OPARTO DOMYŚLNE USTAWIENIA (docs/research/destylacja-proba-500-2026-08-25.md):
  * rozumowanie `thinking=adaptive` podnosi pokrycie osi z 64% na 81% i zeruje śmieci,
    ale wymaga max_tokens=16000 i paczek po 12 (przy 6000 urywa się 59% odpowiedzi)
  * branża pochodzi z business_categories salonu (pokrycie 100%, mediana 3 na salon)
    i zawęża przestrzeń osi — bez tego model szuka masy psa w salonie kosmetycznym
  * enum nazw pól usuwa chaos nazewniczy (marka_preparatu vs "marka linii") w 100%

UŻYCIE:
    python scripts/taxonomy_backfill.py --region mazowieckie --limit 200 --dry-run
    python scripts/taxonomy_backfill.py --region mazowieckie
    python scripts/taxonomy_backfill.py --region mazowieckie --resume
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import settings  # noqa: E402
from services.minimax import MiniMaxClient  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402

log = logging.getLogger("taxonomy_backfill")

PROMPT_VER = 1

# Prostokąty obejmujące województwa (przybliżenie po współrzędnych).
REGIONY: dict[str, tuple[float, float, float, float]] = {
    # region: (lat_min, lat_max, lon_min, lon_max)
    "mazowieckie": (51.0, 53.6, 19.2, 23.2),
    "malopolskie": (49.1, 50.6, 19.1, 21.5),
    "slaskie": (49.4, 51.1, 18.0, 19.9),
    "wielkopolskie": (51.6, 53.7, 15.8, 19.0),
    "dolnoslaskie": (50.1, 51.8, 14.8, 17.9),
    "pomorskie": (53.5, 54.9, 16.7, 19.7),
    "lodzkie": (51.0, 52.4, 18.1, 20.6),
    # pseudo-region: salony, ktore juz pojawily sie w wygenerowanych raportach.
    # Kolejke buduje sie recznie z competitor_matches, prostokat nieuzywany.
    "raporty": (0.0, 0.0, 0.0, 0.0),
    "raport259": (0.0, 0.0, 0.0, 0.0),
    "pary48": (0.0, 0.0, 0.0, 0.0),
    "test186": (0.0, 0.0, 0.0, 0.0),
}

# Zamknięty słownik nazw osi. Bez niego model produkuje warianty
# ("marka_preparatu" obok "marka linii", "cel" obok "cel_problem").
OSIE_ENUM = (
    "obszar, metoda, pakiet_sesji, rozmiar, marka_preparatu, odbiorca, etap, "
    "dlugosc, technika, objetosc, material, powierzchnia_cm2, typ_siersci, "
    "masa_zwierzecia, liczba_osob, poziom_specjalisty, forma_realizacji, "
    "typ_okazji, specjalizacja, patologia"
)

# Osie własne branż — wyprowadzone z danych, patrz docs/research/taksonomia-uslug-2026-08.json.
# Klucze to nazwy z business_categories Booksy.
def branza_glowna(it: dict) -> str:
    """Branża pozycji — grupuje paczki i wybiera osie mapy.

    Mig 186: branżę USŁUGI rozstrzyga budowa kolejki kaskadą
    (treatment_branch_map -> salon jednobranżowy), nie "pierwsza branża salonu" —
    tamta wersja zatruwała globalny name_key kontekstem przypadkowego wystąpienia
    (przegląd adwersaryjny 2026-08-26). Pusta branża = kaskada nie rozstrzygnęła;
    taka pozycja idzie do grupy "_do_wyboru", gdzie model wybiera branżę podczas
    destylacji, WYŁĄCZNIE z listy branż salonu (2-3 opcje, nie 19).
    """
    return it.get("branza") or "_do_wyboru"


OSIE_BRANZ: dict[str, str] = {
    "Paznokcie": "dlugosc, technika, etap",
    "Brwi i rzęsy": "objetosc, etap, poziom_specjalisty",
    "Fryzjer": "dlugosc, poziom_specjalisty, odbiorca",
    "Barber shop": "dlugosc, odbiorca",
    "Depilacja": "odbiorca, rozmiar",
    "Medycyna Estetyczna": "marka_preparatu, rozmiar",
    "Salon Kosmetyczny": "marka_preparatu",
    "Masaż": "liczba_osob, odbiorca, forma_realizacji",
    "Fizjoterapia": "patologia, forma_realizacji",
    "Podologia": "patologia, etap",
    "Makijaż": "typ_okazji, etap",
    "Stomatolog": "material",
    "Tatuaż i Piercing": "powierzchnia_cm2",
    "Zwierzaki": "typ_siersci, masa_zwierzecia",
    "SPA i wellness": "liczba_osob",
    "Trening i Dieta": "specjalizacja",
    "Zdrowie": "specjalizacja",
    "Medycyna Naturalna": "patologia",
    "Psychoterapia": "liczba_osob, forma_realizacji",
}


def norm_key(name: str) -> str:
    return " ".join((name or "").lower().split())


def strip_pl(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9 ]", " ", s).strip()


@dataclass
class Licznik:
    """Twardy limit dobowy — chroni subskrypcję dzieloną z produkcją."""

    limit: int
    dzien: date = field(default_factory=date.today)
    zuzyte: int = 0

    def wolno(self, ile: int = 1) -> bool:
        if date.today() != self.dzien:  # nowa doba resetuje pulę
            self.dzien, self.zuzyte = date.today(), 0
        return self.zuzyte + ile <= self.limit

    def zapisz(self, ile: int = 1) -> None:
        self.zuzyte += ile


class Backfill:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.svc = SupabaseService()
        self.cli = self.svc.client
        self.model = MiniMaxClient(
            settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model
        )
        # Rozumowanie jest warunkiem jakości (64% -> 81% pokrycia osi). Ustawiamy je
        # w pamięci procesu, NIE dotykamy .env produkcji.
        if args.thinking:
            settings.minimax_thinking = args.thinking
        self.licznik = Licznik(limit=args.daily_calls)
        self.stop = False
        self.zapisane = 0
        self.bledy = 0
        self.w_tej_sesji = 0
        self.t0 = time.time()

    # ── dane ────────────────────────────────────────────────────────────────
    def nazwy_do_zrobienia(self) -> list[dict]:
        """Kolejna porcja z materializowanej kolejki, po kursorze.

        Czytamy z taxonomy_queue, a NIE liczymy na żywo z salon_scrape_services —
        ta ma ponad 100 GB i zapytanie przez PostgREST kończy się timeoutem 57014.
        Kolejkę buduje raz `SELECT fn_taxonomy_queue_build(region, lat_min, lat_max,
        lon_min, lon_max)` (minuty), potem odczyt idzie po indeksie.
        """
        # PostgREST zwraca maksymalnie 1000 wierszy na żądanie niezależnie od .limit()
        # — pierwszy przebieg 2026-08-25 zatrzymał się po 1000 nazwach mimo --limit 9000.
        # Stąd stronicowanie po kluczu.
        # UWAGA: przy paczkowaniu branżowym kolejność przetwarzania NIE jest
        # alfabetyczna, więc kursor po name_key przestaje być wiarygodny jako
        # znacznik postępu. Wznawianie opiera się na fladze done w taxonomy_queue
        # (odhaczanej po każdej paczce), kursor zostaje wyłącznie do logu.
        wynik: list[dict] = []
        # Mig 186: kursor postępu ZLIKWIDOWANY w odczycie — źródłem prawdy jest
        # done w kolejce. Kursor z --resume przeskakiwał klucze sprzed swojej
        # pozycji, więc nieudane pozycje (attempt+1) nigdy nie wracały, jeśli
        # sortowały się wcześniej (test 2026-08-26: nazwy od cyfr). Kursor niżej
        # to wyłącznie stronicowanie W RAMACH jednego odczytu (PostgREST tnie
        # do 1000 wierszy niezależnie od .limit()).
        kursor = ""
        while len(wynik) < self.args.limit:
            porcja = min(1000, self.args.limit - len(wynik))
            q = (
                self.cli.table("taxonomy_queue")
                .select("name_key,name_sample,category_name,description,branze,branza,attempt")
                .eq("region", self.args.region)
                .eq("done", False)
                # po 3 nieudanych próbach wiersz zostaje z last_error do ręcznego
                # przejrzenia zamiast wracać w nieskończoność
                .lt("attempt", 3)
            )
            if kursor:
                q = q.gt("name_key", kursor)
            dane = (q.order("name_key").limit(porcja).execute().data) or []
            if not dane:
                break
            wynik.extend(dane)
            kursor = dane[-1]["name_key"]
            if len(dane) < porcja:
                break
        return wynik

    def ile_zostalo(self) -> int:
        """Ile nazw czeka jeszcze w kolejce regionu — prawdziwy mianownik postępu.
        Bez tego log pokazywał „72/12 (600%)", bo mylił narastającą liczbę zrobionych
        z limitem pojedynczego uruchomienia."""
        r = (
            self.cli.table("taxonomy_queue")
            .select("name_key", count="exact")
            .eq("region", self.args.region).eq("done", False).limit(1).execute()
        )
        return r.count or 0

    def _oznacz_zrobione(self, paczka: list[dict], zapisane: set[tuple[str, str]]) -> None:
        """done WYŁĄCZNIE dla pozycji potwierdzonych udanym upsertem (mig 186).

        Poprzednia wersja odhaczała całą paczkę — pusta/częściowa odpowiedź modelu
        zostawiała trwałą, cichą lukę: done=true bez wpisu w service_taxonomy,
        niewidoczną, bo --resume filtruje done=false (zarzut [high] przeglądu
        adwersaryjnego 2026-08-26). Pozycja bez potwierdzenia dostaje attempt+1
        i wraca w następnym przebiegu; po 3 próbach zostaje z last_error.
        """
        if not paczka or self.args.dry_run:
            return
        for it in paczka:
            kq = (it["name_key"], it.get("branza") or "")
            if kq in zapisane or (it["name_key"], "*") in zapisane:
                dane = {"done": True}
            else:
                dane = {
                    "attempt": int(it.get("attempt") or 0) + 1,
                    "last_error": "model nie zwrócił pozycji albo zapis się nie powiódł",
                }
            (
                self.cli.table("taxonomy_queue").update(dane)
                .eq("region", self.args.region)
                .eq("name_key", it["name_key"])
                .eq("branza", it.get("branza") or "")
                .execute()
            )

    def wczytaj_postep(self) -> None:
        r = (
            self.cli.table("taxonomy_backfill_progress")
            .select("*").eq("region", self.args.region).execute()
        )
        rec = (r.data or [None])[0]
        if rec and self.args.resume:
            self.kursor = rec.get("last_key") or ""
            self.zrobione = rec.get("done") or 0
            if rec.get("calls_day") == str(date.today()):
                self.licznik.zuzyte = rec.get("calls_today") or 0
            log.info("wznowienie od klucza %r, zrobione %d, dziś zużyte %d wywołań",
                     self.kursor[:40], self.zrobione, self.licznik.zuzyte)
        else:
            self.kursor, self.zrobione = "", 0

    def zapisz_postep(self, last_key: str, total: int | None = None) -> None:
        self.cli.table("taxonomy_backfill_progress").upsert(
            {
                "region": self.args.region, "last_key": last_key,
                "done": self.zrobione, "total": total,
                "calls_day": str(self.licznik.dzien), "calls_today": self.licznik.zuzyte,
                "updated_at": "now()",
            },
            on_conflict="region",
        ).execute()

    # ── destylacja ──────────────────────────────────────────────────────────
    def _prompt(self, paczka: list[dict]) -> str:
        # --minimal-axes: bez mapy branż. Pomiar 2026-08-25 — mapa nie kosztuje na
        # WEJŚCIU (1426 vs 1096 tokenów), ale zleca modelowi więcej osi, przez co
        # odpowiedź rośnie 4x (8306 vs 2139 tokenów wyjścia) i paczka trwa 55 s
        # zamiast 11 s. Oś `metoda` — jedyna ze zmierzonym wpływem na raport —
        # jest w zestawie uniwersalnym, więc zostaje w obu trybach.
        if self.args.minimal_axes:
            return (
                "OSIE: obszar, metoda, pakiet_sesji, rozmiar, marka_preparatu, odbiorca\n"
                "ZASADA: wypelniaj wylacznie to, co DOSLOWNIE wynika z nazwy, kategorii "
                "lub opisu. ZAKAZ wnioskowania. Nie wiadomo -> POMIN os. ZAKAZ wartosci: "
                "nieokreslony, brak, inny, standard, null.\n"
                'Os "metoda" = rodzajowa nazwa zabiegu po polsku, BEZ marki i BEZ obszaru.\n\n'
                'Zwroc JSON pod kluczem result: [{"i":numer,"osie":{klucz:wartosc}}]\n\nPOZYCJE:\n'
                + json.dumps(
                    [
                        {"i": i, "nazwa": it["name_sample"], "kat": it.get("category_name"),
                         "opis": (it.get("description") or "")[: self.args.desc_chars]}
                        for i, it in enumerate(paczka)
                    ],
                    ensure_ascii=False,
                )
            )
        # Paczka jest JEDNORODNA branżowo (patrz branza_glowna) — wysyłamy osie
        # WYŁĄCZNIE tej jednej branży. Pomiar 2026-08-26 na 12 pozycjach:
        #   minimal (6 osi uniwersalnych)   39.0 s | 8628 tok | 27 osi wypełnionych
        #   mapa MIESZANA (11 branż)        82.3 s | 9966 tok | 28 osi
        #   mapa JEDNORODNA (1 branża)      75.8 s | 6960 tok | 38 osi   <- ta
        # Jednorodna daje 41% więcej osi przy 30% mniejszym wyjściu niż mieszana.
        # To ma znaczenie dla osi ROZDZIELAJĄCYCH (odbiorca, dlugosc, etap), które
        # w trybie minimal miały 13% pokrycia — a bez nich silnik nie odróżnia
        # strzyżenia męskiego od damskiego (pomiar trafności: 15/48 par błędnych).
        branza = branza_glowna(paczka[0])
        if branza == "_do_wyboru":
            # Kaskada nie rozstrzygnęła branży (mig 186): model wybiera ją per
            # pozycja, WYŁĄCZNIE z listy branż salonu podanej przy pozycji.
            # Osie tylko uniwersalne — osi branżowych nie znamy przed wyborem.
            return (
                "OSIE: obszar, metoda, pakiet_sesji, rozmiar, marka_preparatu, odbiorca\n"
                "Kazda pozycja ma pole 'kandydaci_branzy' — WYBIERZ dokladnie jedna "
                "z tej listy (przepisz doslownie) i zwroc w polu 'branza'.\n"
                "ZASADA NADRZEDNA: wypelniaj wylacznie to, co DOSLOWNIE wynika z "
                "nazwy, kategorii lub opisu. Nie wiadomo -> POMIN os.\n"
                'Zwroc JSON pod kluczem result: '
                '[{"i":numer,"branza":"...","osie":{klucz:wartosc}}]\n\nPOZYCJE:\n'
                + json.dumps(
                    [
                        {"i": i, "nazwa": it["name_sample"], "kat": it.get("category_name"),
                         "kandydaci_branzy": it.get("branze") or [],
                         "opis": (it.get("description") or "")[: self.args.desc_chars]}
                        for i, it in enumerate(paczka)
                    ],
                    ensure_ascii=False,
                )
            )
        wlasne = f"  {OSIE_BRANZ[branza]}" if branza in OSIE_BRANZ else ""
        return f"""Uslugi z branzy: {branza}.

OSIE UNIWERSALNE: obszar, metoda, pakiet_sesji, rozmiar, marka_preparatu, odbiorca
OSIE WLASNE TEJ BRANZY:
{wlasne or '  (tylko uniwersalne)'}

NAZWY OSI — uzywaj WYLACZNIE tych kluczy: {OSIE_ENUM}
ZASADA NADRZEDNA: wypelniaj wylacznie to, co DOSLOWNIE wynika z nazwy, kategorii lub opisu.
ZAKAZ wnioskowania i domyslania sie. Nie wiadomo -> POMIN os calkowicie.
ZAKAZ wartosci: nieokreslony, brak, inny, standard, nie dotyczy, null, pusty string.
Lepiej zwrocic puste osie niz zgadnac.
Os "metoda" = rodzajowa nazwa zabiegu po polsku, BEZ marki i BEZ obszaru
(np. "depilacja laserowa", "mezoterapia iglowa", "manicure hybrydowy").

Zwroc JSON pod kluczem result: [{{"i":numer,"osie":{{klucz:wartosc}}}}]

POZYCJE:
""" + json.dumps(
            [
                {"i": i, "nazwa": it["name_sample"], "kat": it.get("category_name"),
                 "opis": (it.get("description") or "")[: self.args.desc_chars]}
                for i, it in enumerate(paczka)
            ],
            ensure_ascii=False,
        )

    async def _paczka(self, paczka: list[dict]) -> list[dict]:
        if not self.licznik.wolno():
            raise RuntimeError("limit dobowy wyczerpany")
        self.licznik.zapisz()
        try:
            out = await self.model.generate_json(
                self._prompt(paczka), max_tokens=self.args.max_tokens
            )
            res = out.get("result") if isinstance(out, dict) else out
        except Exception as e:  # noqa: BLE001
            # generate_json zakłada kopertę {"result": [...]}. M2.7 zwraca często GOŁĄ
            # TABLICĘ, czasem w bloku ```json, czasem jako NDJSON — i wtedy pada.
            # Model dostarczył poprawne dane, więc parsujemy sami zamiast tracić paczkę.
            res = self._ratuj(getattr(e, "raw", None) or str(e))
            if res is None:
                self.bledy += 1
                log.warning("paczka nieudana (%s): %s", type(e).__name__, str(e)[:120])
                return []
        if not isinstance(res, list):
            self.bledy += 1
            return []
        wynik = []
        for r in res:
            i = r.get("i") if isinstance(r, dict) else None
            osie = (r.get("osie") or {}) if isinstance(r, dict) else {}
            if not isinstance(i, int) or i >= len(paczka) or not osie:
                continue
            it = paczka[i]
            # mig 186: branża rozstrzygnięta w kolejce; dla grupy "_do_wyboru"
            # bierzemy wybór modelu, ale WYŁĄCZNIE z listy branż salonu — spoza
            # listy oznacza halucynację i pozycja zostaje bez branży (retry).
            branza = it.get("branza") or ""
            if not branza:
                wybor = str((r.get("branza") if isinstance(r, dict) else "") or "").strip()
                if wybor and wybor in (it.get("branze") or []):
                    branza = wybor
                else:
                    continue
            wynik.append(
                {
                    "name_key": it["name_key"], "name_sample": it["name_sample"],
                    "branza": branza,
                    "branze": it.get("branze") or [], "osie": osie,
                    "model": settings.minimax_model,
                    "thinking": settings.minimax_thinking or None,
                    "prompt_ver": PROMPT_VER,
                }
            )
        return wynik

    @staticmethod
    def _ratuj(surowe: str | None) -> list | None:
        """Wyciągnij tablicę obiektów z odpowiedzi, której klient nie strawił.

        Trzy zaobserwowane formaty poza kontraktem:
          * goła tablica  [{...},{...}]           (najczęstsze w M2.7)
          * blok markdown ```json [...] ```
          * NDJSON — obiekty rozdzielone nową linią, bez nawiasów tablicy
        """
        if not surowe:
            return None
        txt = str(surowe)
        m = re.search(r"```(?:json)?\s*(.+?)```", txt, re.S)
        if m:
            txt = m.group(1)
        for kand in re.findall(r"\[\s*\{.*?\}\s*\]", txt, re.S):
            try:
                v = json.loads(kand)
                if isinstance(v, list) and v:
                    return v
            except Exception:  # noqa: BLE001, S112
                continue
        obiekty = []
        for linia in re.findall(r"\{[^{}]*\"osie\"[^{}]*\{[^{}]*\}[^{}]*\}", txt, re.S):
            try:
                obiekty.append(json.loads(linia))
            except Exception:  # noqa: BLE001, S112
                continue
        return obiekty or None

    def _zapisz(self, rekordy: list[dict]) -> set[tuple[str, str]]:
        """Upsert osi; zwraca POTWIERDZONE (name_key, branza) — tylko one wolno
        odhaczyć w kolejce (mig 186). Nieudany blok -> jego pozycje wracają."""
        potwierdzone: set[tuple[str, str]] = set()
        if not rekordy or self.args.dry_run:
            return potwierdzone
        for i in range(0, len(rekordy), 200):
            blok = rekordy[i : i + 200]
            try:
                self.cli.table("service_taxonomy").upsert(
                    blok, on_conflict="name_key,branza"
                ).execute()
                potwierdzone.update((r["name_key"], r["branza"]) for r in blok)
            except Exception as e:  # noqa: BLE001
                self.bledy += 1
                log.warning("zapis bloku nieudany (%s): %s", type(e).__name__, str(e)[:120])
        return potwierdzone

    # ── pętla główna ────────────────────────────────────────────────────────
    async def uruchom(self) -> int:
        self.wczytaj_postep()
        nazwy = self.nazwy_do_zrobienia()
        if not nazwy:
            log.info("nic do zrobienia w regionie %s", self.args.region)
            return 0
        log.info(
            "region %s: %d nazw w tym uruchomieniu | paczka %d | równolegle %d | "
            "limit dobowy %d wywołań | rozumowanie %s%s",
            self.args.region, len(nazwy), self.args.batch, self.args.concurrency,
            self.args.daily_calls, settings.minimax_thinking or "WYŁĄCZONE",
            "  [DRY-RUN, bez zapisu]" if self.args.dry_run else "",
        )
        zostalo0 = self.ile_zostalo()
        total = self.zrobione + zostalo0          # narastająco względem całego regionu
        # Paczki JEDNORODNE branżowo — inaczej mapa branż rozszerza zakres pracy
        # zamiast go zawężać (pomiar: 11,3 branży na paczkę przy losowym podziale).
        grupy: dict[str, list[dict]] = {}
        for it in nazwy:
            grupy.setdefault(branza_glowna(it), []).append(it)
        paczki = [
            g[i : i + self.args.batch]
            for g in grupy.values()
            for i in range(0, len(g), self.args.batch)
        ]
        log.info(
            "paczek %d z %d branż (największa grupa: %d nazw)",
            len(paczki), len(grupy), max((len(g) for g in grupy.values()), default=0),
        )
        kolejka: asyncio.Queue = asyncio.Queue()
        for idx, pk in enumerate(paczki):
            kolejka.put_nowait((idx, pk))
        ostatni = self.kursor
        zapis_lock = asyncio.Lock()

        async def robotnik(nr: int) -> None:
            """Pobiera paczkę, gdy tylko zwolni się slot — bez bariery na blok.

            Poprzednia wersja czekała na `asyncio.gather` całego bloku, więc wszystkie
            sloty stały, aż skończy się najwolniejsza paczka. Pomiar 2026-08-25:
            240 nazw w 819 s przy teoretycznych ~270 s — wykorzystanie slotów 33%.
            """
            nonlocal ostatni
            while not self.stop:
                try:
                    _, pk = kolejka.get_nowait()
                except asyncio.QueueEmpty:
                    return
                if not self.licznik.wolno():
                    log.warning("robotnik %d: limit dobowy wyczerpany", nr)
                    return
                rek = await self._paczka(pk)
                async with zapis_lock:
                    potwierdzone: set[tuple[str, str]] = set()
                    if rek:
                        potwierdzone = self._zapisz(rek)
                        self.zapisane += len(potwierdzone)
                    # mig 186: pozycje z grupy "_do_wyboru" zapisują się pod branżą
                    # wybraną przez model, a w kolejce mają branza='' — dopasowanie
                    # po samym name_key ('*') odhacza je poprawnie.
                    zap_gwiazdka = {(k, "*") for k, _ in potwierdzone}
                    self._oznacz_zrobione(pk, potwierdzone | zap_gwiazdka)
                    klucz = pk[-1]["name_key"]
                    if klucz > ostatni:
                        ostatni = klucz
                    n = len(pk)
                    self.zrobione += n
                    self.w_tej_sesji += n
                    if self.w_tej_sesji % (self.args.batch * self.args.concurrency * 3) < n:
                        if not self.args.dry_run:
                            self.zapisz_postep(ostatni, total=total)
                        self._log_postep(total)

        await asyncio.gather(*[robotnik(i) for i in range(self.args.concurrency)])
        if not self.args.dry_run:
            self.zapisz_postep(ostatni, total=total)
        self._log_koniec(total)
        return 0

    def _log_postep(self, total: int) -> None:
        dt = max(time.time() - self.t0, 1)
        tempo = self.w_tej_sesji / dt          # tempo z bieżącego uruchomienia
        zostalo = (total - self.zrobione) / tempo if tempo > 0 else 0
        log.info(
            "postęp %d/%d (%d%%) | zapisanych %d | błędów %d | %.1f nazw/s | "
            "wywołań dziś %d/%d | ETA %.1f h",
            self.zrobione, total, round(100 * self.zrobione / max(total, 1)),
            self.zapisane, self.bledy, tempo,
            self.licznik.zuzyte, self.licznik.limit, zostalo / 3600,
        )

    def _log_koniec(self, total: int) -> None:
        dt = time.time() - self.t0
        log.info(
            "KONIEC: przetworzonych %d/%d, zapisanych %d, błędów %d, czas %.0f s, "
            "wywołań %d",
            self.zrobione, total, self.zapisane, self.bledy, dt, self.licznik.zuzyte,
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region", required=True, choices=sorted(REGIONY))
    p.add_argument("--limit", type=int, default=500, help="ile nazw wziąć w tym uruchomieniu")
    p.add_argument("--batch", type=int, default=12, help="pozycji na wywołanie (12 przy rozumowaniu)")
    p.add_argument("--concurrency", type=int, default=2, help="równoległych wywołań; subskrypcja daje 3-4, zostawiamy sloty produkcji")
    p.add_argument("--daily-calls", type=int, default=3000, help="twardy limit wywołań na dobę")
    p.add_argument("--max-tokens", type=int, default=16000, help="16000 wymagane przy rozumowaniu")
    p.add_argument("--desc-chars", type=int, default=400, help="ile znaków opisu podać modelowi")
    p.add_argument("--thinking", default="adaptive", help="'adaptive' albo puste, żeby wyłączyć")
    p.add_argument("--minimal-axes", action="store_true", help="tylko osie uniwersalne — 5x szybciej, bez osi branżowych")
    p.add_argument("--resume", action="store_true", help="wznów od zapisanego kursora")
    p.add_argument("--dry-run", action="store_true", help="nie zapisuj do bazy")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bf = Backfill(args)

    def zatrzymaj(signum, _frame):  # noqa: ANN001
        log.warning("sygnał %s — kończę bieżące paczki i zapisuję kursor", signum)
        bf.stop = True

    signal.signal(signal.SIGINT, zatrzymaj)
    signal.signal(signal.SIGTERM, zatrzymaj)
    return asyncio.run(bf.uruchom())


if __name__ == "__main__":
    raise SystemExit(main())
