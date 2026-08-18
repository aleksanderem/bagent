"""AI-wnioski z kreacji reklamowych konkurencji (zakładka Reklamy).

Wejście: reklamy jednego salonu z salon_meta_ads (tekst kreacji, dni emisji,
platformy, status). Wyjście: strukturalny JSON dla panelu — zwycięskie
kreacje z uzasadnieniem, promowane zabiegi, mechanizmy (hooki), co warto
zawrzeć we własnych kreacjach i czego unikać / jakie luki zostawia konkurent.

Proxy skuteczności: DNI EMISJI. Reklamodawca nie utrzymuje nieskutecznej
reklamy tygodniami — kreacje emitowane najdłużej „zarabiają na siebie".
Model dostaje tę heurystykę wprost w system prompcie, żeby nie zgadywał.

Klient: gpt-4o-mini + Structured Outputs (wzorzec z openai_synthesis.py) —
tani, szybki, wystarczający do analizy tekstów reklamowych. Bez fallbacku:
brak wniosków nie psuje skanu (worker łyka wyjątek i loguje).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any

from services.llm_rate_limiter import provider_slot

logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"

_OAI_CLIENT = None


def _get_openai_client():
    global _OAI_CLIENT
    if _OAI_CLIENT is None:
        if not os.getenv("OPENAI_API_KEY"):
            logger.warning("[meta-ads-insights] OPENAI_API_KEY missing")
            _OAI_CLIENT = False
            return None
        try:
            from openai import AsyncOpenAI

            _OAI_CLIENT = AsyncOpenAI()
        except Exception as e:  # noqa: BLE001
            logger.warning("[meta-ads-insights] OpenAI client init failed: %s", e)
            _OAI_CLIENT = False
            return None
    return _OAI_CLIENT if _OAI_CLIENT else None


_INSIGHTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {
            "type": "string",
            "description": (
                "2-3 zdania po polsku: jaką strategię reklamową prowadzi ten "
                "salon (ton, oferta, częstotliwość, na co stawia)."
            ),
        },
        "winners": {
            "type": "array",
            "description": (
                "Max 3 najskuteczniejsze kreacje (najdłużej emitowane). "
                "whyItWorks: konkretnie CO w tekście działa, nie ogólniki."
            ),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "adArchiveId": {"type": "string"},
                    "whyItWorks": {"type": "string"},
                },
                "required": ["adArchiveId", "whyItWorks"],
            },
        },
        "treatments": {
            "type": "array",
            "description": "Zabiegi/usługi promowane w reklamach, z liczbą kreacji.",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "adCount": {"type": "integer"},
                },
                "required": ["name", "adCount"],
            },
        },
        "hooks": {
            "type": "array",
            "description": (
                "Mechanizmy przyciągające uwagę używane w kreacjach, po polsku "
                "(np. 'pytanie o problem skórny', 'obietnica efektu w 1 wizycie')."
            ),
            "items": {"type": "string"},
        },
        "dos": {
            "type": "array",
            "description": (
                "Co WARTO zawrzeć we własnych kreacjach, wnioskując z tego co "
                "u konkurenta działa. Konkretne, wykonalne, po polsku."
            ),
            "items": {"type": "string"},
        },
        "donts": {
            "type": "array",
            "description": (
                "Czego unikać (co u konkurenta NIE działa — krótkie emisje) "
                "oraz luki: czego konkurent nie robi, a można to wykorzystać."
            ),
            "items": {"type": "string"},
        },
    },
    "required": ["summary", "winners", "treatments", "hooks", "dos", "donts"],
}

_SYSTEM_PROMPT = (
    "Jesteś strategiem reklamowym dla polskich salonów beauty. Analizujesz "
    "kreacje Meta Ads KONKURENTA, żeby właścicielka salonu wiedziała, co "
    "działa na jej rynku i jak pisać własne reklamy.\n\n"
    "Kluczowa heurystyka: DNI EMISJI ≈ skuteczność. Nikt nie płaci tygodniami "
    "za reklamę, która nie konwertuje — kreacje z najdłuższą emisją traktuj "
    "jako zwycięskie, a szybko wyłączone (kilka dni, o ile nie są świeże) "
    "jako nieudane eksperymenty.\n\n"
    "Pisz po polsku, konkretnie, bez marketingowego lania wody. W winners "
    "wskazuj cytaty/elementy tekstu, które robią robotę. W dos/donts dawaj "
    "wskazówki wykonalne od ręki (struktura, hook, oferta, CTA, emoji), "
    "nie ogólniki typu 'bądź autentyczny'."
)


def ads_fingerprint(ads: list[dict[str, Any]]) -> str:
    """Odcisk zestawu reklam: id + kubełek dni emisji (co 7 dni).

    Kubełkowanie tłumi szum — codzienny +1 dzień emisji nie wymusza
    regeneracji, dopiero nowa/zdjęta reklama albo tydzień różnicy.
    """
    parts = sorted(f"{a['adArchiveId']}:{int(a['daysRunning']) // 7}" for a in ads)
    return hashlib.sha1("|".join(parts).encode()).hexdigest()


def build_context(salon_name: str, ads: list[dict[str, Any]]) -> str:
    """Kontekst dla modelu: reklamy posortowane od najdłużej emitowanych."""
    lines = [f"Salon konkurenta: {salon_name}", f"Liczba reklam: {len(ads)}", ""]
    for a in sorted(ads, key=lambda x: -int(x["daysRunning"])):
        status = "AKTYWNA" if a.get("isActive") else "ZAKOŃCZONA"
        platforms = ",".join(a.get("platforms") or []) or "?"
        text = (a.get("creativeText") or "(bez tekstu)").strip()
        if len(text) > 420:
            text = text[:420] + "…"
        lines.append(
            f"[id={a['adArchiveId']}] {status}, {a['daysRunning']} dni emisji, "
            f"start {a.get('startedRunningOn') or '?'}, platformy: {platforms}\n"
            f"TEKST: {text}\n"
        )
    return "\n".join(lines)


async def generate_insights(salon_name: str, ads: list[dict[str, Any]]) -> dict[str, Any]:
    """Jedno wywołanie gpt-4o-mini → zwalidowany JSON wniosków."""
    client = _get_openai_client()
    if client is None:
        raise RuntimeError("OpenAI client unavailable (no OPENAI_API_KEY)")

    async with provider_slot(MODEL):
        response = await client.chat.completions.create(
            model=MODEL,
            temperature=0.3,
            max_tokens=2500,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": build_context(salon_name, ads)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "meta_ads_insights",
                    "strict": True,
                    "schema": _INSIGHTS_SCHEMA,
                },
            },
        )
    parsed = json.loads(response.choices[0].message.content or "{}")
    # Winners tylko z realnymi id z wejścia — model nie może ich wymyślić.
    valid_ids = {str(a["adArchiveId"]) for a in ads}
    parsed["winners"] = [
        w for w in parsed.get("winners", []) if str(w.get("adArchiveId")) in valid_ids
    ][:3]
    return parsed
