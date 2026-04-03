"""MiniMax M2.7 client — thin wrapper around anthropic.AsyncAnthropic."""

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable

import anthropic

logger = logging.getLogger(__name__)


class MiniMaxClient:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self.client = anthropic.AsyncAnthropic(
            base_url=base_url,
            api_key=api_key,
            default_headers={"Authorization": f"Bearer {api_key}"},
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
        """Simple JSON generation (no tools). Parses response as JSON, handles markdown code blocks."""
        sys = system or "Odpowiadaj WYŁĄCZNIE poprawnym JSON. Bez markdown, bez komentarzy, bez tekstu przed lub po JSON."
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0.3,
            system=sys,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
            if match:
                return json.loads(match.group(1).strip())
            raise ValueError(f"Invalid JSON from MiniMax: {text[:200]}")

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
