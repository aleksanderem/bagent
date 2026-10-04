"""Klient punktów z modelami (vLLM, zgodny z OpenAI) dla b-card i b-match: Runpod Serverless i Modal.

Pułapki zmierzone 2026-10-03 (runbook ~/brain/runbooks/runpod-vllm-karty.md):
- punkt z dyskiem sieciowym NIE budzi się sam z zera → przed pracą `workersMin=1` (rozgrzej), model gotowy po ~2,5 min;
- `workersMin=0` NIE gasi działającego pracownika → gasimy przez `workersMax=0`, potem przywracamy `workersMax=1`.
Dlatego `Punkt` jest menedżerem kontekstu: rozgrzewa na wejściu i ZAWSZE gasi na wyjściu (także przy błędzie).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Any

import httpx

logger = logging.getLogger(__name__)
REST = "https://rest.runpod.io/v1/endpoints"
API = "https://api.runpod.ai/v2"


class BladPunktu(RuntimeError):
    pass


class Punkt:
    def __init__(self, endpoint_id: str, api_key: str, model: str, *, naraz: int = 128, zarzadzaj: bool = True):
        if not endpoint_id or not api_key:
            raise BladPunktu("brak id punktu albo klucza Runpod")
        self.id, self.klucz, self.model, self.naraz, self.zarzadzaj = endpoint_id, api_key, model, naraz, zarzadzaj
        self._http = httpx.AsyncClient(limits=httpx.Limits(max_connections=naraz))
        self.ponowienia = 0
        self.sekundy = 0.0
        self._start = 0.0

    async def _ustaw(self, **pola: int) -> None:
        r = await self._http.patch(f"{REST}/{self.id}", json=pola, headers=self._naglowki(), timeout=30)
        if r.status_code >= 400:
            raise BladPunktu(f"PATCH {self.id} {pola}: {r.status_code} {r.text[:200]}")

    def _naglowki(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.klucz}"}

    def _url_czatu(self) -> str:
        return f"{API}/{self.id}/openai/v1/chat/completions"

    async def __aenter__(self) -> "Punkt":
        self._start = asyncio.get_running_loop().time()
        if self.zarzadzaj:
            await self._ustaw(workersMax=1, workersMin=1)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            if self.zarzadzaj:
                await self._ustaw(workersMin=0, workersMax=0)
                await asyncio.sleep(5)
                await self._ustaw(workersMax=1)
        except Exception as e:  # noqa: BLE001 — błąd gaszenia logujemy głośno (pracownik nalicza ~4,8 USD/h)
            logger.error("bmatch: NIE udało się wyłączyć punktu %s: %s", self.id, e)
        finally:
            self.sekundy = round(asyncio.get_running_loop().time() - self._start, 1)
            await self._http.aclose()

    async def gotowy(self, limit_s: int = 600) -> None:
        """Czeka, aż punkt ma gotowego pracownika (model wczytany) — zapytania wysłane wcześniej dostają 500 i ponowienia."""
        t0 = asyncio.get_running_loop().time()
        while asyncio.get_running_loop().time() - t0 < limit_s:
            try:
                r = await self._http.get(f"{API}/{self.id}/health", headers=self._naglowki(), timeout=30)
                w = r.json().get("workers", {}) if r.status_code == 200 else {}
                if (w.get("ready") or 0) + (w.get("idle") or 0) + (w.get("running") or 0) > 0:
                    return
            except (httpx.HTTPError, ValueError):
                pass
            await asyncio.sleep(10)
        raise BladPunktu(f"{self.id}: brak gotowego pracownika po {limit_s} s")

    async def _czat(self, wiadomosci: list[dict[str, str]], **param: Any) -> dict[str, Any]:
        body = {"model": self.model, "messages": wiadomosci, "temperature": 0,
                "chat_template_kwargs": {"enable_thinking": False}, **param}
        blad = ""
        for proba in range(8):
            try:
                r = await self._http.post(self._url_czatu(), json=body, headers=self._naglowki(), timeout=600)
                if r.status_code == 200:
                    return r.json()
                blad = f"{r.status_code} {r.text[:200]}"
            except httpx.HTTPError as e:
                blad = f"{type(e).__name__}: {e}"
            self.ponowienia += 1
            if self.ponowienia <= 3:
                logger.warning("bmatch punkt %s: ponowienie (%s)", self.id, blad[:150])
            await asyncio.sleep(min(30, 5 * (proba + 1)))
        raise BladPunktu(f"{self.id}: {blad}")

    async def karty(self, wejscia: list[list[dict[str, str]]]) -> list[dict[str, Any] | None]:
        """b-card: lista wiadomości → karty (None = nieczytelna odpowiedź)."""
        sem = asyncio.Semaphore(self.naraz)

        async def jedna(w):
            async with sem:
                odp = await self._czat(w, max_tokens=400)
            s = odp["choices"][0]["message"]["content"] or ""
            try:
                return json.loads(s[s.index("{"):s.rindex("}") + 1])
            except ValueError:
                return None
        return await asyncio.gather(*(jedna(w) for w in wejscia))

    async def werdykty(self, pary: list[tuple[list[dict[str, str]], list[dict[str, str]]]]) -> list[list[float]]:
        """b-match: [(wiadomości A/B, wiadomości B/A)] → [p_inna, p_odmiana, p_ta_sama] (średnia obu kolejności)."""
        sem = asyncio.Semaphore(self.naraz)

        async def p(w):
            async with sem:
                odp = await self._czat(w, max_tokens=1, logprobs=True, top_logprobs=10)
            top = odp["choices"][0]["logprobs"]["content"][0]["top_logprobs"]
            lp = {t["token"].strip(): t["logprob"] for t in top}
            e = [math.exp(lp.get(c, -50.0)) for c in "012"]
            s = sum(e)
            return [x / s for x in e]

        async def para(ab, ba):
            x, y = await asyncio.gather(p(ab), p(ba))
            return [(a + b) / 2 for a, b in zip(x, y, strict=True)]
        return await asyncio.gather(*(para(ab, ba) for ab, ba in pary))


class PunktModal(Punkt):
    """Ten sam model na Modal (b-card/modal/app.py): Modal sam włącza kontener przy pierwszym zapytaniu i gasi po
    bezczynności — bez zarządzania pracownikami. `url` = adres funkcji (…modal.run), `klucz` = BMATCH_TOKEN."""

    def __init__(self, url: str, token: str, model: str, *, naraz: int = 128):
        if not url or not token:
            raise BladPunktu("brak adresu albo tokenu Modal")
        super().__init__(url.rstrip("/"), token, model, naraz=naraz, zarzadzaj=False)

    def _url_czatu(self) -> str:
        return f"{self.id}/v1/chat/completions"

    async def gotowy(self, limit_s: int = 600) -> None:
        """Pierwsze zapytanie budzi kontener; Modal trzyma je do startu vLLM (zwykle 1–3 min)."""
        t0 = asyncio.get_running_loop().time()
        while (zostalo := limit_s - (asyncio.get_running_loop().time() - t0)) > 0:
            try:
                r = await self._http.get(f"{self.id}/health", timeout=max(5.0, zostalo))
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(5)
        raise BladPunktu(f"{self.id}: model nie wstał po {limit_s} s")
