"""MiniMax M2.7 client — thin wrapper around anthropic.AsyncAnthropic."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

import anthropic
import httpx

from services.json_repair import parse_llm_json

logger = logging.getLogger(__name__)


class MiniMaxClient:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self.client = anthropic.AsyncAnthropic(
            base_url=base_url,
            api_key=api_key,
            default_headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(120.0, connect=15.0),  # 120s total, 15s connect
        )
        self.model = model

    async def create_message(
        self,
        system: str,
        messages: list[dict],
        tools: list[dict] | None = None,
        max_tokens: int = 16384,
        temperature: float = 0.3,
    ) -> anthropic.types.Message:
        """Raw Anthropic-compatible message creation with tools support.

        Returns the full response object with content, stop_reason, usage.
        """
        kwargs: dict = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools
        return await self.client.messages.create(**kwargs)

    async def generate_json(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 16384,
    ) -> dict:
        """Simple JSON generation (no tools).

        Parsowanie przez services.json_repair.parse_llm_json — naprawia znane
        tryby awarii MiniMax (fences/proza, <think>, surowe \\n w stringach,
        polskie cytaty z prostym zamknięciem, odpowiedzi ucięte). Patrz
        docs/FINDINGS-2026-06-11.md P0-1.
        """
        sys = system or (
            "Odpowiadaj WYŁĄCZNIE poprawnym JSON. Bez markdown, bez komentarzy, "
            "bez tekstu przed lub po JSON. W wartościach tekstowych NIE używaj "
            "prostego cudzysłowu (\") — cytaty i nazwy pisz w «...» albo bez "
            "cudzysłowów. Pisz wyłącznie po polsku."
        )
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0.3,
            system=sys,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in response.content:
            if block.type == "text":
                text = block.text.strip()
                break
        if not text:
            raise ValueError("No text response from MiniMax (only thinking blocks)")
        parsed = parse_llm_json(text)
        if parsed is not None:
            return parsed
        # Diagnostyka: pełny zrzut do logów (bez tego nie da się ustalić,
        # który fragment psuje JSON) — a potem dotychczasowy kontrakt ValueError.
        logger.error("generate_json: unrecoverable JSON (%d chars)", len(text))
        for i in range(0, min(len(text), 12000), 1500):
            logger.error("generate_json raw[%d]: %r", i, text[i : i + 1500])
        raise ValueError(f"Invalid JSON from MiniMax after repair: {text[:200]}")

    async def generate_text(
        self,
        prompt: str,
        system: str | None = None,
        max_tokens: int = 4096,
    ) -> str:
        """Simple text generation."""
        kwargs: dict = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": 0.3,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system
        response = await self.client.messages.create(**kwargs)
        for block in response.content:
            if block.type == "text":
                return block.text
        raise ValueError("No text response from MiniMax")


async def with_retry(
    fn: Callable[[], Awaitable],
    max_attempts: int = 2,
    base_delay: float = 2.0,
    max_delay: float = 10.0,
):
    """Retry with exponential backoff. Skip retry for auth errors."""
    for attempt in range(max_attempts):
        try:
            return await fn()
        except Exception as e:
            error_msg = str(e).lower()
            if any(term in error_msg for term in ["unauthorized", "401", "403", "invalid key"]):
                raise
            if attempt == max_attempts - 1:
                raise
            delay = min(base_delay * (2**attempt), max_delay)
            logger.warning(f"Retry {attempt + 1}/{max_attempts} after {delay}s: {e}")
            await asyncio.sleep(delay)
