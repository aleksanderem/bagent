"""AI-wnioski z kreacji reklamowych konkurencji (zakładka Reklamy).

Wejście: reklamy jednego salonu z salon_meta_ads (tekst kreacji, dni emisji,
platformy, status) + kontekst rynkowy (ruchy cen, promocje, opinie). Wyjście:
strukturalny JSON dla panelu — ANALIZA ZACHOWANIA KONKURENTA: summary, winners
(najdłużej emitowane kreacje), treatments, moves (reklamy krzyżowane z ruchami
cen/promocji/opinii). ZERO porad dla klienta — od tego jest moduł Strategia.

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
import re
from typing import Any

from config import settings
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
                "2-3 zdania po polsku, CZAS TERAŹNIEJSZY, opisujące CO ROBI ten "
                "konkurent: na jakich zabiegach koncentruje reklamy, jak długo je "
                "utrzymuje, jak rusza cenami i promocjami. Sam opis zachowania — "
                "ZERO porad dla czytelnika."
            ),
        },
        "winners": {
            "type": "array",
            "description": (
                "Max 3 kreacje najdłużej emitowane (dni emisji = ich stawka na "
                "tę kreację). whyItWorks: SUCHY OPIS kreacji konkurenta — jaki "
                "zabieg, jaka oferta/cena/CTA w tekście, ile dni leci. Same "
                "elementy kreacji, BEZ ogonków 'co może przyciągać/zwiększać' i "
                "bez rad. Np. 'Reklamuje Icoone®, pierwszy zabieg za 150 zł, "
                "emisja 61 dni'."
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
            "description": "Zabiegi/usługi, które konkurent promuje w reklamach, z liczbą kreacji.",
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
        "moves": {
            "type": "array",
            "description": (
                "NAJWAŻNIEJSZE. Obserwacje krzyżujące reklamy z RUCHAMI konkurenta "
                "(ceny, promocje, opinie) — każda to twarde zdanie o tym, co "
                "konkurent zrobił, z LICZBAMI z kontekstu. Przykłady dobrej formy: "
                "'Reklamuje Hydrafacial od 32 dni i w tym czasie zebrał 4 nowe "
                "opinie za ten zabieg', 'Równolegle z kampanią podniósł cenę "
                "depilacji laserowej ze 150 na 190 zł', 'Uruchomił promocję na "
                "Geneo wspierającą 5 aktywnych kreacji tego zabiegu'. Jeśli brak "
                "ruchu — powiedz to wprost ('Mimo 28 dni emisji nie zmienił cen "
                "reklamowanych zabiegów'). NIGDY porada dla czytelnika."
            ),
            "items": {"type": "string"},
        },
    },
    "required": ["summary", "winners", "treatments", "moves"],
}

_SYSTEM_PROMPT = (
    "Jesteś analitykiem wywiadu konkurencyjnego dla polskich salonów beauty. "
    "Twoje zadanie: OPISAĆ, CO ROBI KONKURENT — na podstawie jego reklam Meta "
    "ORAZ jego ruchów na rynku (ceny, promocje, opinie). To jest raport "
    "obserwacyjny o konkurencie, NIE poradnik dla czytelnika.\n\n"
    "ŻELAZNE ZASADY:\n"
    "1. Piszesz WYŁĄCZNIE o tym, co konkurent robi/zrobił. Podmiotem każdego "
    "zdania jest konkurent ('reklamuje…', 'podniósł…', 'utrzymuje…', "
    "'zebrał…'). Czytelnik NIE istnieje w tych zdaniach.\n"
    "2. ABSOLUTNY ZAKAZ porad, rekomendacji i trybu rozkazującego. Zabronione "
    "słowa i frazy: 'zastosuj', 'wprowadź', 'warto', 'powinieneś', 'rozważ', "
    "'zadbaj', 'wykorzystaj', 'stwórz', 'dodaj', 'skup się', 'pamiętaj', "
    "'atrakcyjne ceny/oferty', 'buduj', 'postaw na'. Za doradzanie czytelnikowi "
    "jest OSOBNY moduł (Strategia) — tu tego NIE robisz.\n"
    "3. Każde zdanie w 'moves' MUSI opierać się na konkretnej liczbie z "
    "kontekstu (dni emisji, kwota, %, liczba opinii, liczba kreacji). Bez "
    "danych — nie piszesz zdania. Zero ogólników typu 'stawia na jakość'.\n"
    "3b. W KAŻDYM polu (summary, winners, moves) podajesz sam FAKT i kończysz. "
    "ZAKAZ spekulacyjnych ogonków w całym outputcie: 'co może sugerować…', 'co "
    "wskazuje na…', 'co może przyciągać…', 'co może zwiększać…', "
    "'prawdopodobnie…', 'być może…'. Nie zgadujesz intencji ani skutków — "
    "relacjonujesz to, co konkurent zrobił.\n"
    "4. Heurystyka: DNI EMISJI = jak mocno konkurent stawia na daną kreację "
    "(nikt nie płaci tygodniami za reklamę, której nie przedłuża). OPINIE ≈ "
    "wykonane zabiegi (czy reklamowany zabieg realnie się sprzedaje).\n\n"
    "Dostajesz KONTEKST RYNKOWY (ruchy cen z historii skanów, aktywne "
    "promocje, opinie per zabieg). To jest rdzeń analizy — krzyżuj go z "
    "reklamami i wkładaj do 'moves'. Reklama bez ruchów rynkowych to połowa "
    "obrazu; Twoja wartość to połączenie jednego z drugim."
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


# Kontrakt JSON dokładany do promptu M3 (M3 nie ma sztywnych Structured
# Outputs jak gpt-4o-mini — kształt wymuszamy opisem + json_repair).
_JSON_CONTRACT = (
    "\n\nZwróć WYŁĄCZNIE JSON o DOKŁADNIE takim kształcie (bez markdown):\n"
    '{"summary": "…", '
    '"winners": [{"adArchiveId": "…", "whyItWorks": "…"}], '
    '"treatments": [{"name": "…", "adCount": 1}], '
    '"moves": ["…"]}\n'
    "winners: max 3, adArchiveId TYLKO z podanych reklam. moves: twarde fakty "
    "z liczbami. Wszystko po polsku."
)

_MINIMAX_CLIENT: "Any | None | bool" = None


def _get_minimax_client() -> Any:
    """Leniwy MiniMax M3 — None gdy brak klucza/klienta (wtedy fallback OpenAI)."""
    global _MINIMAX_CLIENT
    if _MINIMAX_CLIENT is None:
        if not settings.minimax_api_key:
            logger.warning("[meta-ads-insights] brak MINIMAX_API_KEY — fallback OpenAI")
            _MINIMAX_CLIENT = False
            return None
        try:
            from services.minimax import MiniMaxClient

            _MINIMAX_CLIENT = MiniMaxClient(
                settings.minimax_api_key,
                settings.minimax_base_url,
                settings.minimax_model,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("[meta-ads-insights] MiniMax init failed: %s", e)
            _MINIMAX_CLIENT = False
            return None
    return _MINIMAX_CLIENT if _MINIMAX_CLIENT else None


async def _generate_via_openai(context: str) -> dict[str, Any]:
    """Fallback: gpt-4o-mini + Structured Outputs (sztywny schemat)."""
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
                {"role": "user", "content": context},
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
    return json.loads(response.choices[0].message.content or "{}")


async def generate_insights(
    salon_name: str, ads: list[dict[str, Any]], market_context: str = ""
) -> dict[str, Any]:
    """MiniMax M3 (primary, reasoning) → gpt-4o-mini (fallback) → guard.

    M3 daje lepsze analitycznie krzyżowanie reklam z ruchami; gpt-4o-mini
    ratuje, gdy M3 padnie lub zwróci niepoprawny JSON mimo json_repair.
    """
    context = build_context(salon_name, ads, market_context)
    parsed: dict[str, Any] | None = None
    used_model = ""

    mm = _get_minimax_client()
    if mm is not None:
        try:
            async with provider_slot(settings.minimax_model):
                parsed = await mm.generate_json(
                    context + _JSON_CONTRACT, system=_SYSTEM_PROMPT, max_tokens=3000
                )
                used_model = settings.minimax_model
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[meta-ads-insights] M3 padł (%s) — fallback gpt-4o-mini", exc
            )
    if not parsed or "summary" not in parsed:
        parsed = await _generate_via_openai(context)
        used_model = MODEL
    # Realny model do telemetrii (worker czyta i usuwa z blobu).
    parsed["_model"] = used_model

    # Winners tylko z realnymi id z wejścia — model nie może ich wymyślić.
    valid_ids = {str(a["adArchiveId"]) for a in ads}
    parsed["winners"] = [
        w
        for w in parsed.get("winners", [])
        if isinstance(w, dict) and str(w.get("adArchiveId")) in valid_ids
    ][:3]

    # Deterministyczny strażnik anty-slop: żaden model nie ma prawa przemycić
    # spekulacyjnego ogonka ("…, co może sugerować…") — ucinamy twardo.
    parsed["summary"] = _strip_speculation(str(parsed.get("summary", "")))
    parsed["moves"] = [_strip_speculation(str(m)) for m in parsed.get("moves", [])]
    parsed["treatments"] = parsed.get("treatments", [])
    for w in parsed["winners"]:
        w["whyItWorks"] = _strip_speculation(str(w.get("whyItWorks", "")))
    return parsed


# Spekulacyjne klauzule doklejane przez model — ucinamy od przecinka/spacji
# przed nimi do końca zdania.
_SPECULATION_RE = re.compile(
    r"[,;]?\s*(?:co\s+(?:może|mogłoby|wskazuje|sugeruje|oznacza|świadczy|"
    r"pokazuje|potwierdza)|prawdopodobnie|być może|zapewne)\b.*?(?=[.!?]|$)",
    re.IGNORECASE | re.DOTALL,
)


def _strip_speculation(text: str) -> str:
    """Czyści string: ucina spekulacyjne ogonki i prostuje skróty modelu."""
    cleaned = _SPECULATION_RE.sub("", text).strip()
    # M3 czasem skraca "Salon" do "Sal" ("…Sal zebrał 9 opinii…").
    cleaned = re.sub(r"\bSal\b", "Salon", cleaned)
    cleaned = re.sub(r"\s+([.!?])", r"\1", cleaned)
    if cleaned and cleaned[-1] not in ".!?":
        cleaned += "."
    return cleaned
