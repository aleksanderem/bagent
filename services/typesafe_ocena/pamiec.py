"""Ocena par z trwałą pamięcią — każda para płaci za model tylko raz.

Najpierw pamięć (tabela matching_pair_verdict, mig 200), potem TypeSafe dla
brakujących, na końcu zapis nowych ocen. Kolejny pomiar (albo kolejny raport)
widzi te same pary za darmo; koszt rośnie tylko z liczbą NOWYCH par.

Bezpieczniki: budżet USD na sesję (pary ponad budżet zostają bez oceny)
i równoległość na sufit limitu API. Błąd jednej pary = log i brak oceny,
nigdy wyjątek na zewnątrz.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from services.posthog_analytics import capture_ai_generation

from .pytania import MODEL, PYTANIA, WERSJA, para_klucz, stan, uporzadkuj, wynik

logger = logging.getLogger(__name__)

TABELA = "matching_pair_verdict"
# Cena zweryfikowana w konsoli TypeSafe 21.09.2026 (wyjście nierozliczane).
USD_ZA_TOKEN = 0.042 / 1_000_000
# Zmierzone przy destylacji: 6 zapytań naraz to sufit limitu API.
ROWNOLEGLE = 6
# Szacunek przed pierwszym pomiarem; aktualizowany średnią z sesji.
TOKENY_NA_PARE_START = 900
_ODCZYT_PACZKA = 100
_ZAPIS_PACZKA = 200

Strona = dict[str, str]
Para = tuple[Strona, Strona]


class OcenaPar:
    """Jedna sesja oceny: wspólna pamięć, budżet i licznik kosztu."""

    def __init__(self, db: Any, client: Any, budzet_usd: float) -> None:
        self.db = db              # klient Supabase (table().select/upsert) albo None
        self.client = client      # AsyncTypeSafeClient
        self.budzet_tokenow = int(budzet_usd / USD_ZA_TOKEN)
        self.tokeny = 0
        self.z_pamieci = 0
        self.nowych = 0
        self.pominietych = 0
        self._wyniki: dict[str, dict[str, Any]] = {}

    @property
    def koszt_usd(self) -> float:
        return self.tokeny * USD_ZA_TOKEN

    async def ocen(self, pary: list[Para]) -> dict[str, dict[str, Any]]:
        """klucz pary → wynik. Pary bez oceny (budżet, błąd) nie ma w słowniku."""
        unikalne: dict[str, Para] = {}
        for a, b in pary:
            unikalne.setdefault(para_klucz(a, b), (a, b))
        brak = [k for k in unikalne if k not in self._wyniki]
        z_bazy = self._wczytaj(brak)
        self._wyniki.update(z_bazy)
        self.z_pamieci += len(z_bazy)
        do_pytania = [k for k in brak if k not in self._wyniki]
        if do_pytania:
            start, tok0 = time.monotonic(), self.tokeny
            nowe = await self._pytaj({k: unikalne[k] for k in do_pytania})
            self._zapisz(nowe)
            # koszt do podsumowania dobowego (panel) — jak każde wywołanie modelu
            await capture_ai_generation(
                provider="typesafe", model=MODEL, span_name="matching_pair_judge",
                started_at=start, input_tokens=self.tokeny - tok0, output_tokens=0,
            )
        return {k: self._wyniki[k] for k in unikalne if k in self._wyniki}

    def _wczytaj(self, klucze: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        if self.db is None or not klucze:
            return out
        try:
            for i in range(0, len(klucze), _ODCZYT_PACZKA):
                rows = (
                    self.db.table(TABELA).select("para_klucz,wynik")
                    .eq("wersja", WERSJA).in_("para_klucz", klucze[i : i + _ODCZYT_PACZKA])
                    .execute().data or []
                )
                out.update({r["para_klucz"]: r["wynik"] for r in rows if r.get("wynik")})
        except Exception as e:  # noqa: BLE001 — brak pamięci = pytamy od zera
            logger.warning("%s: odczyt niedostępny (%s): %s", TABELA, type(e).__name__, str(e)[:160])
        return out

    async def _pytaj(self, pary: dict[str, Para]) -> list[dict[str, Any]]:
        sem = asyncio.Semaphore(ROWNOLEGLE)
        wiersze: list[dict[str, Any]] = []
        srednia = [TOKENY_NA_PARE_START]

        async def jedna(klucz: str, a: Strona, b: Strona) -> None:
            async with sem:
                if self.tokeny + srednia[0] > self.budzet_tokenow:
                    self.pominietych += 1
                    return
                try:
                    res = await self.client.system_one(stan(a, b), PYTANIA, model=MODEL)
                except Exception as e:  # noqa: BLE001 — jedna para bez oceny, reszta idzie dalej
                    logger.warning("ocena pary: %s: %s", type(e).__name__, str(e)[:160])
                    self.pominietych += 1
                    return
            tok = res.usage.input_tokens or 0
            self.tokeny += tok
            self.nowych += 1
            srednia[0] = max(srednia[0], tok)
            w = wynik(res)
            self._wyniki[klucz] = w
            wiersze.append(_wiersz(klucz, a, b, w, tok))

        await asyncio.gather(*[jedna(k, a, b) for k, (a, b) in pary.items()])
        return wiersze

    def _zapisz(self, wiersze: list[dict[str, Any]]) -> None:
        if self.db is None or not wiersze:
            return
        try:
            for i in range(0, len(wiersze), _ZAPIS_PACZKA):
                self.db.table(TABELA).upsert(
                    wiersze[i : i + _ZAPIS_PACZKA], on_conflict="para_klucz,wersja"
                ).execute()
        except Exception as e:  # noqa: BLE001 — ocena działa w tej sesji i bez zapisu
            logger.warning("%s: zapis nieudany (%s): %s", TABELA, type(e).__name__, str(e)[:160])


def _wiersz(klucz: str, a: Strona, b: Strona, w: dict[str, Any], tokeny: int) -> dict[str, Any]:
    x, y = uporzadkuj(a, b)
    return {
        "para_klucz": klucz,
        "wersja": WERSJA,
        "a_nazwa": x["nazwa"], "a_kategoria": x["kategoria_w_cenniku"] or None, "a_typ_salonu": x["typ_salonu"] or None,
        "b_nazwa": y["nazwa"], "b_kategoria": y["kategoria_w_cenniku"] or None, "b_typ_salonu": y["typ_salonu"] or None,
        "werdykt": w["werdykt"],
        "wynik": w,
        "model": MODEL,
        "input_tokens": tokeny,
    }
