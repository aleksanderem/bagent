"""Destylacja na żądanie przy wycenie raportu — profile TypeSafe w service_profile.

Zastępuje pomost GLM (report_pricing._bridge_distill_missing), gdy
TAXONOMY_VETO_SOURCE=typesafe. Każda nazwa z wyceny (usługi podmiotu +
bliźniaki z promienia) dostaje profil liczbowy z badania (pytania.py); profil
zapisuje się TRWALE, więc pamięć rośnie sama i kolejny raport w tej branży
płaci tylko za nowe nazwy. Backfill niepotrzebny: jedynym konsumentem
taksonomii jest ta wycena (grep + pg_proc, 22.09.2026).

Różnice wobec pomostu GLM, obie zmierzone:
* bez wyłączonych branż — „model purity” (mig 189) dotyczył mieszania wartości
  M3 i GLM w jednym słowniku; profile to osobne źródło z własną tabelą;
* bez limitu 40 wywołań / 75 s — Jev odpowiada na nazwę w ~0,3 s i kosztuje
  ~0,00013 USD; test w cieniu (9 podmiotów, pusta pamięć): 0,003–0,69 USD
  i ~20 nazw/s na raport. Zostają bezpieczniki, które w normalnej pracy nie
  zadziałają: budżet USD i limit czasu na raport.

Każdy błąd => log i profil pominięty; brak profilu po którejkolwiek stronie
pary = weto milczy (abstain), czyli zachowanie sprzed weta.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from config import settings
from services.posthog_analytics import capture_ai_generation

from .pytania import WERSJA, build_state, profile, questions_for_branch

logger = logging.getLogger(__name__)

# Model przypięty do wersji: zmiana modelu zmienia odpowiedzi tak samo jak
# zmiana pytań, więc to decyzja z pomiarem, nie automatyczna aktualizacja.
MODEL = "jev-1.13.0"
# Cena zweryfikowana w konsoli TypeSafe 21.09.2026: 0,3832 USD = 9,124 mln
# tokenów wejścia; wyjście nierozliczane.
USD_ZA_TOKEN = 0.042 / 1_000_000
# Budżet raportu: 3× najdroższy raport testu w cieniu (0,69 USD przy pustej
# pamięci). Normalnie nieosiągalny — chroni przed raportem-anomalią.
BUDZET_USD_NA_RAPORT = 2.0
# Limit czasu destylacji na raport (oba przebiegi progu razem). Najcięższy
# raport testu (~5 tys. nazw) to ~4,5 min przy limicie API 1200 zapytań/min.
LIMIT_CZASU_S = 600.0
# Równoległość zmierzona w teście w cieniu: 6 zapytań naraz ≈ 20 nazw/s, czyli
# sufit limitu API. Więcej nie przyspiesza, tylko produkuje 429.
ROWNOLEGLE = 6
TABELA = "service_profile"
_ODCZYT_PACZKA = 40
_ZAPIS_PACZKA = 200

Profil = dict[str, Any]


def nk(name: str | None) -> str:
    """Klucz nazwy — ta sama normalizacja co service_taxonomy.name_key."""
    return " ".join((name or "").lower().split())


def _client() -> Any | None:
    """Klient SDK albo None (brak klucza). Wydzielone dla testów."""
    key = getattr(settings, "typesafe_api_key", "")
    if not key:
        return None
    from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy

    # 429 i 5xx SDK ponawia samo (z Retry-After); 4 próby, bo kilka raportów
    # naraz dzieli jeden limit zapytań na minutę.
    return AsyncTypeSafeClient(
        api_key=key, model=MODEL, retry=RetryPolicy(max_retries=4, backoff_max=10.0)
    )


def _is_auth_error(exc: BaseException) -> bool:
    from typesafe_sdk import TypeSafeAuthenticationError, TypeSafePermissionDeniedError

    return isinstance(exc, (TypeSafeAuthenticationError, TypeSafePermissionDeniedError))


def _names_meta(
    subject_services: list[dict[str, Any]], clusters: dict[int, list[dict[str, Any]]]
) -> dict[str, tuple[str, str | None]]:
    """name_key -> (pisownia, kategoria). Najpierw usługi podmiotu: przy budżecie
    na wyczerpaniu ich profil liczy się dla każdej pary tej usługi."""
    meta: dict[str, tuple[str, str | None]] = {}
    for s in subject_services:
        meta.setdefault(nk(s.get("name")), (s.get("name") or "", s.get("category_name")))
    for lst in clusters.values():
        for x in lst:
            meta.setdefault(nk(x.get("service_name")), (x.get("service_name") or "", x.get("category_name")))
    meta.pop("", None)
    return meta


class ProfileSession:
    """Jedna na wycenę raportu — oba przebiegi progu dzielą pamięć, budżet i zegar."""

    def __init__(self, service: Any) -> None:
        self.service = service
        self.cache: dict[tuple[str, str], Profil] = {}
        self.tokens = 0
        self.skipped = 0
        self.started = time.monotonic()

    async def profiles_for(
        self,
        branza: str | None,
        subject_services: list[dict[str, Any]],
        clusters: dict[int, list[dict[str, Any]]],
    ) -> dict[str, Profil]:
        """Profile wszystkich nazw z wyceny: z bazy, a brakujące destylowane teraz."""
        if not branza:
            return {}
        meta = _names_meta(subject_services, clusters)
        need = [n for n in meta if (branza, n) not in self.cache]
        for n, prof in self._load_stored(branza, need).items():
            self.cache[(branza, n)] = prof
        missing = [n for n in need if (branza, n) not in self.cache]
        if missing:
            self._store(await self._distill(branza, missing, meta))
        return {n: self.cache[(branza, n)] for n in meta if (branza, n) in self.cache}

    def _load_stored(self, branza: str, names: list[str]) -> dict[str, Profil]:
        cli = getattr(self.service, "client", None)
        out: dict[str, Profil] = {}
        if cli is None or not names:
            return out
        try:
            for i in range(0, len(names), _ODCZYT_PACZKA):
                rows = (
                    cli.table(TABELA)
                    .select("name_key,profil")
                    .eq("branza", branza)
                    .eq("wersja", WERSJA)
                    .in_("name_key", names[i : i + _ODCZYT_PACZKA])
                    .execute()
                    .data
                    or []
                )
                for r in rows:
                    if r.get("profil"):
                        out[r["name_key"]] = r["profil"]
        except Exception as e:  # noqa: BLE001 — brak pamięci = destylacja od zera, nie awaria raportu
            logger.warning("%s: odczyt profili niedostępny (%s): %s", TABELA, type(e).__name__, str(e)[:160])
        return out

    def _store(self, rows: list[dict[str, Any]]) -> None:
        cli = getattr(self.service, "client", None)
        if not rows or cli is None:
            return
        try:
            for i in range(0, len(rows), _ZAPIS_PACZKA):
                cli.table(TABELA).upsert(
                    rows[i : i + _ZAPIS_PACZKA], on_conflict="name_key,branza,wersja"
                ).execute()
        except Exception as e:  # noqa: BLE001 — profil działa w tym raporcie i bez zapisu
            logger.warning(
                "%s: zapis %d profili nieudany (%s): %s — kolejny raport zapłaci za nie ponownie",
                TABELA, len(rows), type(e).__name__, str(e)[:160],
            )

    async def _distill(
        self, branza: str, names: list[str], meta: dict[str, tuple[str, str | None]]
    ) -> list[dict[str, Any]]:
        cli = _client()
        if cli is None:
            logger.warning("destylacja profili: brak TYPESAFE_API_KEY — weto bez %d nazw", len(names))
            return []
        questions = questions_for_branch(branza)
        budget_tokens = BUDZET_USD_NA_RAPORT / USD_ZA_TOKEN
        sem = asyncio.Semaphore(ROWNOLEGLE)
        rows: list[dict[str, Any]] = []
        state: dict[str, Any] = {"stop": False, "errors": 0, "first_error": None}
        tokens_before = self.tokens
        started = time.monotonic()

        async def one(n: str) -> None:
            async with sem:
                if state["stop"] or self.tokens >= budget_tokens:
                    self.skipped += 1
                    return
                name, category = meta[n]
                try:
                    res = await cli.system_one(build_state(name, category, branza), questions)
                except Exception as e:  # noqa: BLE001 — jedna nazwa nie wywraca reszty
                    state["errors"] += 1
                    state["first_error"] = state["first_error"] or e
                    if _is_auth_error(e):
                        state["stop"] = True  # zły klucz: każde kolejne wywołanie też padnie
                    return
            used = int(getattr(getattr(res, "usage", None), "input_tokens", 0) or 0)
            self.tokens += used
            prof = profile(res)
            self.cache[(branza, n)] = prof
            rows.append({
                "name_key": n, "branza": branza, "wersja": WERSJA, "name_sample": name,
                "kategoria": category, "profil": prof, "model": MODEL, "input_tokens": used,
            })

        tasks = [asyncio.create_task(one(n)) for n in names]
        try:
            remaining = max(LIMIT_CZASU_S - (time.monotonic() - self.started), 0.0)
            _, pending = await asyncio.wait(tasks, timeout=remaining)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                self.skipped += len(pending)
        finally:
            await cli.aclose()

        used_now = self.tokens - tokens_before
        await capture_ai_generation(
            provider="typesafe", model=MODEL, span_name="pricing_profile_distill",
            started_at=started, input_tokens=used_now, output_tokens=0, error=state["first_error"],
        )
        logger.info(
            "destylacja profili: %d/%d nazw (branża %s, %.4f USD, %.0f s, błędów %d, pominiętych łącznie %d)",
            len(rows), len(names), branza, used_now * USD_ZA_TOKEN, time.monotonic() - started,
            state["errors"], self.skipped,
        )
        if state["first_error"] is not None:
            e = state["first_error"]
            logger.warning("destylacja profili: pierwszy błąd %s: %s", type(e).__name__, str(e)[:160])
        return rows
