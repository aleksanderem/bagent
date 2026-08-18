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
        "marketSignals": {
            "type": "array",
            "description": (
                "Sygnały z KRZYŻOWANIA reklam z rynkiem: czy reklamowany zabieg "
                "zbiera opinie (opinie ≈ wykonane zabiegi = reklama konwertuje), "
                "ruchy cen po startach kampanii, promocje wspierające kreacje. "
                "Tylko wnioski poparte danymi z kontekstu, z liczbami."
            ),
            "items": {"type": "string"},
        },
    },
    "required": [
        "summary", "winners", "treatments", "hooks", "dos", "donts", "marketSignals"
    ],
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
    "nie ogólniki typu 'bądź autentyczny'.\n\n"
    "Dostajesz też KONTEKST RYNKOWY salonu (ruchy cen z historii skanów, "
    "aktywne promocje, opinie per zabieg — opinie to proxy WYKONANYCH "
    "zabiegów). Krzyżuj go z reklamami: reklamowany zabieg zbierający opinie "
    "= ekspozycja konwertuje na zabiegi; reklamowany zabieg bez opinii = "
    "kampania nie domyka; podwyżka ceny po długiej emisji = salon monetyzuje "
    "popyt z reklamy. Takie skrzyżowane obserwacje umieszczaj w marketSignals "
    "(zawsze z liczbami z kontekstu) i uwzględniaj w winners/dos/donts."
)


def ads_fingerprint(ads: list[dict[str, Any]]) -> str:
    """Odcisk zestawu reklam: id + kubełek dni emisji (co 7 dni).

    Kubełkowanie tłumi szum — codzienny +1 dzień emisji nie wymusza
    regeneracji, dopiero nowa/zdjęta reklama albo tydzień różnicy.
    """
    parts = sorted(f"{a['adArchiveId']}:{int(a['daysRunning']) // 7}" for a in ads)
    return hashlib.sha1("|".join(parts).encode()).hexdigest()


def build_market_context(sb: Any, booksy_id: int, salon_ref_id: int) -> str:
    """Kontekst rynkowy z historii skanów: ceny, promocje, opinie per zabieg.

    Każde źródło jest best-effort — brak danych nie blokuje analizy reklam.
    """
    from datetime import datetime, timedelta, timezone

    lines = ["", "KONTEKST RYNKOWY SALONU (ostatnie 90 dni):"]

    try:
        moves = (
            sb.rpc(
                "fn_salon_price_moves_window",
                {"p_booksy_id": booksy_id, "p_days": 90},
            ).execute().data
            or []
        )
        if moves:
            lines.append(f"Ruchy cen ({len(moves)}):")
            for m in moves[:20]:
                lines.append(
                    f"- {m['service_name']}: "
                    f"{m['prev_price_grosze'] / 100:.0f} zł → "
                    f"{m['current_price_grosze'] / 100:.0f} zł "
                    f"(między {m['baseline_date']} a {m['current_date_']})"
                )
        else:
            lines.append("Ruchy cen: brak zmian cen w oknie.")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[meta-ads-insights] price moves %s: %s", salon_ref_id, exc)

    try:
        promo_rows = (
            sb.table("salon_scrape_services")
            .select("name, price_grosze")
            .eq("booksy_id", booksy_id)
            .eq("is_promo", True)
            .gte(
                "scraped_at",
                (datetime.now(timezone.utc) - timedelta(days=14)).isoformat(),
            )
            .limit(60)
            .execute()
            .data
            or []
        )
        promo_names = sorted({r["name"] for r in promo_rows})
        if promo_names:
            lines.append(
                f"Aktywne promocje ({len(promo_names)} usług): "
                + ", ".join(promo_names[:15])
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[meta-ads-insights] promos %s: %s", salon_ref_id, exc)

    try:
        since = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        reviews = (
            sb.table("salon_reviews")
            .select("services, review_created_at")
            .eq("salon_id", salon_ref_id)
            .gte("review_created_at", since)
            .limit(500)
            .execute()
            .data
            or []
        )
        cutoff_30 = datetime.now(timezone.utc) - timedelta(days=30)
        cutoff_60 = datetime.now(timezone.utc) - timedelta(days=60)
        last30 = prev30 = 0
        per_treatment: dict[str, int] = {}
        for r in reviews:
            created_raw = r.get("review_created_at")
            try:
                created = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
            except ValueError:
                created = None
            if created:
                if created >= cutoff_30:
                    last30 += 1
                elif created >= cutoff_60:
                    prev30 += 1
            for svc in r.get("services") or []:
                name = (svc or {}).get("name")
                if name:
                    per_treatment[name] = per_treatment.get(name, 0) + 1
        lines.append(
            f"Opinie (≈ wykonane zabiegi): {len(reviews)} w 90 dni; "
            f"ostatnie 30 dni: {last30}, poprzednie 30 dni: {prev30}."
        )
        if per_treatment:
            top = sorted(per_treatment.items(), key=lambda kv: -kv[1])[:15]
            lines.append(
                "Opinie per zabieg (90 dni): "
                + ", ".join(f"{n} ×{c}" for n, c in top)
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[meta-ads-insights] reviews %s: %s", salon_ref_id, exc)

    return "\n".join(lines)


def build_context(
    salon_name: str, ads: list[dict[str, Any]], market_context: str = ""
) -> str:
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
    if market_context:
        lines.append(market_context)
    return "\n".join(lines)


async def generate_insights(
    salon_name: str, ads: list[dict[str, Any]], market_context: str = ""
) -> dict[str, Any]:
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
                {"role": "user", "content": build_context(salon_name, ads, market_context)},
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
