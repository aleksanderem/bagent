"""Kolejka dostawców kart graficznych dla b-card / b-match (BMATCH_DOSTAWCY, np. "modal,runpod").

Jeden dostawca bywa bez wolnej karty (Runpod EUR-IS-3, H100 „Low”, 2026-10-03: 10 min czekania i raport na starym
silniku). `Lancuch` budzi pierwszego dostawcę od razu (rozgrzewanie idzie równolegle z doborem i odczytem danych),
a gdy ten nie wstanie w BMATCH_GOTOWY_S, przechodzi do następnego. Dopiero gdy nikt nie wstanie — błąd, a raport
liczy stary silnik. Liczy ten, kto wstał pierwszy; budzeni są tylko kolejni po porażce poprzednika.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from config import settings

from .runpod import BladPunktu, Punkt, PunktModal

logger = logging.getLogger(__name__)
ROLE = ("bmatch", "bcard")


def _fabryki(rola: str) -> list[tuple[str, Callable[[], Punkt]]]:
    out: list[tuple[str, Callable[[], Punkt]]] = []
    for d in (x.strip().lower() for x in (settings.bmatch_dostawcy or "").split(",")):
        if d == "modal":
            url = settings.modal_bmatch_url if rola == "bmatch" else settings.modal_bcard_url
            if url and settings.modal_bmatch_token:
                out.append(("modal", lambda url=url: PunktModal(url, settings.modal_bmatch_token, rola)))
        elif d == "runpod":
            ep = settings.bmatch_endpoint_id if rola == "bmatch" else settings.bcard_endpoint_id
            if ep and settings.runpod_api_key:
                out.append(("runpod", lambda ep=ep: Punkt(ep, settings.runpod_api_key, rola)))
    return out


def dostepny(rola: str) -> bool:
    return bool(_fabryki(rola))


def dostawcy(rola: str) -> list[str]:
    return [n for n, _ in _fabryki(rola)]


class Lancuch:
    def __init__(self, fabryki: list[tuple[str, Callable[[], Punkt]]], limit_s: int):
        if not fabryki:
            raise BladPunktu("brak skonfigurowanego dostawcy (BMATCH_DOSTAWCY, klucze, adresy)")
        self._fabryki, self._limit = fabryki, limit_s
        self._otwarte: list[tuple[str, Punkt, asyncio.Task[None]]] = []
        self._wybrany: Punkt | None = None
        self.dostawca = ""
        self.proby: list[str] = []
        self.sekundy = 0.0

    async def _otworz(self, i: int) -> None:
        nazwa, fabryka = self._fabryki[i]
        p = fabryka()
        await p.__aenter__()
        self._otwarte.append((nazwa, p, asyncio.create_task(p.gotowy(self._limit))))

    async def __aenter__(self) -> Lancuch:
        await self._otworz(0)
        return self

    async def gotowy(self, limit_s: int | None = None) -> None:  # noqa: ARG002 — limit per dostawca z ustawień
        if self._wybrany is not None:
            return
        i = 0
        while True:
            nazwa, p, zadanie = self._otwarte[i]
            try:
                await zadanie
                self._wybrany, self.dostawca = p, nazwa
                self.proby.append(f"{nazwa}: ok")
                return
            except BladPunktu as e:
                self.proby.append(f"{nazwa}: {str(e)[:120]}")
                logger.warning("bmatch: dostawca %s nie wstał (%s) — następny", nazwa, str(e)[:200])
            i += 1
            if i >= len(self._fabryki):
                raise BladPunktu("żaden dostawca nie wstał: " + " | ".join(self.proby))
            await self._otworz(i)

    async def karty(self, wejscia: list[list[dict[str, str]]]) -> list[dict[str, Any] | None]:
        await self.gotowy()
        assert self._wybrany is not None
        return await self._wybrany.karty(wejscia)

    async def werdykty(self, pary: list[tuple[list[dict[str, str]], list[dict[str, str]]]]) -> list[list[float]]:
        await self.gotowy()
        assert self._wybrany is not None
        return await self._wybrany.werdykty(pary)

    async def __aexit__(self, *exc: Any) -> None:
        for _, p, zadanie in self._otwarte:
            zadanie.cancel()
            await p.__aexit__(*exc)
            self.sekundy += p.sekundy


def punkt(rola: str) -> Lancuch:
    return Lancuch(_fabryki(rola), settings.bmatch_gotowy_s)
