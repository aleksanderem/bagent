"""Keyword analysis pipeline — SEO keyword extraction + AI suggestions."""

from __future__ import annotations

import logging
import re
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

BEAUTY_KEYWORDS = [
    "lifting", "mezoterapia", "botox", "kwas hialuronowy", "peeling",
    "mikrodermabrazja", "oczyszczanie", "nawilżanie", "odmładzanie",
    "redukcja zmarszczek", "ujędrnianie", "kolagen", "retinol",
    "witamina c", "hydrafacial", "peel", "laser", "rf",
    "endermologia", "lipoliza", "kriolipoliza", "masaż",
    "drenaż limfatyczny", "cellulit", "rozstępy", "wyszczuplanie",
    "modelowanie", "ujędrnianie ciała", "karboksyterapia", "liposukcja",
    "cavitation", "ultradźwięki",
    "depilacja laserowa", "depilacja", "woskowanie", "ipl", "shr",
    "bikini", "nogi", "pachy", "twarz", "wąsik", "broda",
    "manicure", "pedicure", "hybryda", "żel", "przedłużanie",
    "paznokcie", "stylizacja paznokci", "japoński",
    "strzyżenie", "koloryzacja", "balayage", "ombre", "keratyna",
    "botox na włosy", "regeneracja", "laminowanie", "prostowanie",
    "brwi", "rzęsy", "henna", "laminowanie brwi",
    "przedłużanie rzęs", "microblading", "pmu", "makijaż permanentny",
    "konsultacja", "pakiet", "promocja", "bestseller", "nowość",
    "premium", "relaks", "spa", "wellness", "detoks", "kroplówka",
    "infuzja",
]


def extract_keywords(categories: list[dict]) -> list[dict]:
    """Rule-based keyword extraction from pricelist categories."""
    keyword_map: dict[str, dict] = {}

    for cat in categories:
        cat_name = cat.get("name", cat.get("categoryName", ""))
        for svc in cat.get("services", []):
            text = f"{svc.get('name', '')} {svc.get('description', '')}".lower()
            for keyword in BEAUTY_KEYWORDS:
                if keyword.lower() in text:
                    if keyword in keyword_map:
                        keyword_map[keyword]["count"] += 1
                        if cat_name not in keyword_map[keyword]["categories"]:
                            keyword_map[keyword]["categories"].append(cat_name)
                        keyword_map[keyword]["services"].append(svc.get("name", ""))
                    else:
                        keyword_map[keyword] = {
                            "keyword": keyword,
                            "count": 1,
                            "categories": [cat_name],
                            "services": [svc.get("name", "")],
                        }

    return sorted(keyword_map.values(), key=lambda k: k["count"], reverse=True)[:50]


def calculate_category_distribution(categories: list[dict], keywords: list[dict]) -> list[dict]:
    """Calculate keyword distribution per category."""
    distribution = []
    for cat in categories:
        cat_name = cat.get("name", cat.get("categoryName", ""))
        cat_keywords = [k for k in keywords if cat_name in k["categories"]]
        distribution.append({
            "categoryName": cat_name,
            "keywordCount": len(cat_keywords),
            "topKeywords": [k["keyword"] for k in cat_keywords[:5]],
        })
    return sorted(distribution, key=lambda d: d["keywordCount"], reverse=True)


async def generate_keyword_suggestions(
    client: Any,
    salon_name: str,
    categories: list[dict],
    found_keywords: list[dict],
    total_services: int,
) -> list[str]:
    """Generate AI suggestions for missing SEO keywords."""
    found_list = ", ".join(k["keyword"] for k in found_keywords)
    cat_list = ", ".join(c.get("name", c.get("categoryName", "")) for c in categories)

    prompt = f"""Jesteś ekspertem SEO dla salonów beauty na platformie Booksy.

KONTEKST:
- Salon: {salon_name or "Nieznany"}
- Kategorie: {cat_list}
- Znalezione słowa kluczowe: {found_list}
- Liczba usług: {total_services}

ZADANIE:
Zasugeruj 5-8 słów kluczowych SEO, które BRAKUJĄ w tym cenniku, a które pomogłyby w pozycjonowaniu na Booksy.

FORMAT ODPOWIEDZI:
- [słowo kluczowe 1]: [krótkie uzasadnienie dlaczego warto dodać]
- [słowo kluczowe 2]: [krótkie uzasadnienie]

ZASADY:
1. Sugeruj tylko słowa kluczowe pasujące do usług, które salon prawdopodobnie oferuje
2. Skup się na słowach, które klienci faktycznie wyszukują
3. Nie powtarzaj słów, które już są w cenniku
4. Używaj polskich nazw"""

    response = await client.create_message(
        system="Jesteś ekspertem SEO dla salonów beauty.",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
        temperature=0.5,
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    suggestions = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        # Match lines starting with: - , * , 1. , 2. , etc.
        cleaned = re.sub(r"^[-*•]\s*|^\d+[.)]\s*", "", line).strip()
        # Strip markdown bold
        cleaned = re.sub(r"\*{1,2}([^*]+)\*{1,2}", r"\1", cleaned).strip()
        if cleaned and cleaned != line.strip() and len(cleaned) > 3:
            suggestions.append(cleaned)

    return suggestions[:8]


async def run_keyword_pipeline(
    scraped_data: dict,
    audit_id: str,
    on_progress: Callable[[int, str], Awaitable[None]] | None = None,
) -> dict:
    """Full keyword pipeline: extract → distribute → AI suggest."""
    from config import settings
    from services.minimax import MiniMaxClient

    async def _noop(p: int, m: str) -> None:
        pass
    progress = on_progress or _noop
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)

    categories = scraped_data.get("categories", [])
    salon_name = scraped_data.get("salonName", "")
    total_services = scraped_data.get("totalServices", 0)

    await progress(20, "Ekstrakcja słów kluczowych...")
    keywords = extract_keywords(categories)
    logger.info("[%s] Extracted %d keywords", audit_id, len(keywords))

    await progress(40, "Analiza dystrybucji kategorii...")
    distribution = calculate_category_distribution(categories, keywords)

    await progress(60, "Generowanie sugestii AI...")
    try:
        suggestions = await generate_keyword_suggestions(client, salon_name, categories, keywords, total_services)
    except Exception as e:
        logger.error("[%s] AI keyword suggestions failed: %s", audit_id, e)
        suggestions = ["Nie udało się wygenerować sugestii"]

    await progress(100, "Raport słów kluczowych gotowy")

    return {
        "keywords": keywords,
        "categoryDistribution": distribution,
        "suggestions": suggestions,
    }
