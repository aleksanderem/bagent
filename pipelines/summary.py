"""BAGENT #3 pipeline — audit summary with basic competitor preview.

Single responsibility: aggregate data from all previous audit phases
(scraped data, BAGENT #1 report, BAGENT #2 cennik) plus user-selected
competitor list, and produce:
    1. Narrative summary text (via MiniMax) — ~500-1000 chars
    2. Score delta (before/after optimization) with component breakdown
    3. Top 3-5 key wins from audit_transformations by impact
    4. Basic competitor preview — top selected competitors with minimal metrics
    5. CTA text for upgrading to premium competitor analysis

Writes to Supabase table: audit_summaries.

Does NOT generate a full competitor report with SWOT / radar / action plan —
that is a separate paid product (next sprint, bagent/pipelines/_premium/).
This pipeline produces ONLY the basic preview as a teaser.

Inputs (from HTTP payload):
    audit_id, user_id, salon_id, scrape_id, selected_competitor_ids[]

Loads from Supabase (no bulk data in payload):
    audit_scraped_data          — original pricelist
    audit_reports + children    — from BAGENT #1
    optimized_pricelists + ch.  — from BAGENT #2
    salons + services           — for each selected competitor by ID

Webhook callbacks:
    POST Convex /api/audit/summary/progress (during pipeline)
    POST Convex /api/audit/summary/complete (on success)
    POST Convex /api/audit/summary/fail     (on failure after retries)
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def _fill_prompt(template: str, **kwargs: str) -> str:
    """Replace {var_name} placeholders in prompt template."""
    result = template
    for key, value in kwargs.items():
        result = result.replace(f"{{{key}}}", value)
    return result


def _extract_key_wins(
    transformations: list[dict[str, Any]],
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Pick top N most impactful transformations as key wins for the summary.

    Sort by impactScore descending, then filter to unique service names, then
    take top N. Returns a lightweight shape for frontend display:
    [{ "type", "title", "before", "after", "reason" }, ...]
    """
    sorted_transforms = sorted(
        transformations,
        key=lambda t: (t.get("impactScore", 0), t.get("type", "")),
        reverse=True,
    )

    seen_services: set[str] = set()
    wins: list[dict[str, Any]] = []

    for t in sorted_transforms:
        service = (t.get("serviceName") or t.get("before", "")).strip()
        if not service or service in seen_services:
            continue
        seen_services.add(service)

        wins.append({
            "type": t.get("type", "name"),
            "title": service,
            "before": t.get("before", ""),
            "after": t.get("after", ""),
            "reason": t.get("reason", ""),
        })

        if len(wins) >= top_n:
            break

    return wins


def _compute_seo_coverage(
    pricelist: dict[str, Any],
    seo_keywords: list[dict[str, Any]],
) -> float:
    """Compute what percentage of missing SEO keywords now appear in the
    optimized pricelist (in any service name or description).

    Returns percentage (0-100).
    """
    if not seo_keywords:
        return 100.0

    corpus = ""
    for cat in pricelist.get("categories", []):
        for svc in cat.get("services", []):
            corpus += " " + (svc.get("name") or "")
            corpus += " " + (svc.get("description") or "")
    corpus = corpus.lower()

    present = 0
    for kw in seo_keywords:
        keyword = (kw.get("keyword") or "").lower().strip()
        if keyword and keyword in corpus:
            present += 1

    return round(present / len(seo_keywords) * 100, 2)


async def _build_basic_competitor_data(
    supabase: Any,
    selected_competitor_ids: list[int],
) -> list[dict[str, Any]]:
    """Fetch basic competitor metrics for each selected Booksy salon ID.

    Returns a lightweight preview list (not full SWOT). Max 5 entries.
    """
    competitors: list[dict[str, Any]] = []

    for salon_id in selected_competitor_ids[:5]:
        data = await supabase.get_salon_with_services(salon_id)
        salon = data.get("salon")
        if not salon:
            continue

        services = data.get("services", [])
        prices_grosze = [
            s.get("price_grosze") for s in services
            if s.get("price_grosze") is not None
        ]

        price_range: dict[str, Any] = {}
        if prices_grosze:
            price_range = {
                "min_pln": min(prices_grosze) / 100,
                "max_pln": max(prices_grosze) / 100,
                "avg_pln": round(sum(prices_grosze) / len(prices_grosze) / 100, 2),
            }

        competitors.append({
            "salon_id": salon.get("booksy_id"),
            "name": salon.get("name"),
            "distance_km": None,  # computed from subject salon location if available
            "reviews_rank": salon.get("reviews_rank"),
            "reviews_count": salon.get("reviews_count"),
            "service_count": len(services),
            "price_range": price_range,
            "thumbnail_photo": salon.get("thumbnail_photo"),
        })

    return competitors


async def _generate_summary_text(
    client: Any,
    salon_name: str,
    score_before: int,
    score_after: int,
    key_wins: list[dict[str, Any]],
    stats: dict[str, Any],
) -> str:
    """Generate the narrative summary text via MiniMax."""
    summary_prompt = _load_prompt("audit_summary.txt")
    if not summary_prompt:
        summary_prompt = (
            "Napisz przyjazne, motywujące podsumowanie audytu cennika salonu "
            "beauty. 500-1000 znaków. Zaznacz największe wygrane i zachęć do "
            "wdrożenia zmian.\n\n"
            "SALON: {salon_name}\n"
            "WYNIK PRZED: {score_before}/100\n"
            "WYNIK PO: {score_after}/100\n"
            "TOP WYGRANE:\n{key_wins_text}"
        )

    key_wins_text = "\n".join(
        f"- {w['title']}: {w['before']} → {w['after']}"
        for w in key_wins[:5]
    )

    full_prompt = _fill_prompt(
        summary_prompt,
        salon_name=salon_name or "Nieznany",
        score_before=str(score_before),
        score_after=str(score_after),
        key_wins_text=key_wins_text,
        names_improved=str(stats.get("namesImproved", 0)),
        descriptions_added=str(stats.get("descriptionsAdded", 0)),
    )

    try:
        return await client.generate_text(
            full_prompt,
            system="Jesteś ekspertem od audytów cenników beauty i copywriterem.",
        )
    except Exception as e:
        logger.warning("Summary text generation failed: %s", e)
        return (
            f"Audyt salonu {salon_name or 'Twój salon'} zakończony. "
            f"Wynik: {score_after}/100. "
            f"Zaktualizowano {stats.get('namesImproved', 0)} nazw usług i "
            f"dodano/poprawiono {stats.get('descriptionsAdded', 0)} opisów."
        )


async def run_summary_pipeline(
    audit_id: str,
    user_id: str,
    selected_competitor_ids: list[int],
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Full BAGENT #3 pipeline — generate audit summary with basic competitor preview.

    Steps:
        1. Load all inputs from Supabase (scraped data, report, optimized pricelist,
           competitor salons)
        2. Compute score delta, key wins, SEO coverage
        3. Build basic competitor preview (no SWOT)
        4. Generate narrative summary text via MiniMax
        5. Save to Supabase audit_summaries

    Returns the assembled summary dict.
    """
    from config import settings
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(
        settings.minimax_api_key,
        settings.minimax_base_url,
        settings.minimax_model,
    )
    supabase = SupabaseService()

    # ── Step 1: Load all inputs ──
    await progress(5, "Ładowanie danych audytu z Supabase...")
    t0 = time.time()

    raw_scraped = await supabase.get_scraped_data(audit_id)
    if not raw_scraped:
        raise RuntimeError(f"No scraped_data for audit {audit_id}")

    report_data = await supabase.get_audit_report(audit_id) or {}
    if not report_data:
        raise RuntimeError(f"No audit_report for audit {audit_id} — BAGENT #1 must finish first")

    # TODO: load optimized_pricelist from Supabase. For now the table schema uses
    #       qualityScore/stats only — we'll fetch it via a dedicated RPC when one exists.
    #       Shape expected: {"pricelist": {...}, "stats": {...}}.
    optimized_data: dict[str, Any] = {}
    try:
        optimized_rows = supabase.client.table("optimized_pricelists").select("*").eq("convex_audit_id", audit_id).limit(1).execute()
        if optimized_rows.data:
            optimized_data = optimized_rows.data[0]
    except Exception as e:
        logger.warning("[%s][summary] Failed to load optimized_pricelists: %s", audit_id, e)

    dt = int((time.time() - t0) * 1000)
    await progress(
        20,
        f"Dane załadowane ({dt}ms): raport + {len(selected_competitor_ids)} konkurentów do przetworzenia",
    )

    # ── Step 2: Compute derived values ──
    salon_name = raw_scraped.get("salon_name") or report_data.get("salonName") or "Twój salon"
    score_before = int(report_data.get("totalScore", 0))
    score_after = int(optimized_data.get("quality_score", 0)) if optimized_data else score_before

    transformations = report_data.get("transformations", [])
    key_wins = _extract_key_wins(transformations, top_n=5)

    stats = {
        "namesImproved": optimized_data.get("names_improved", 0) if optimized_data else 0,
        "descriptionsAdded": optimized_data.get("descriptions_added", 0) if optimized_data else 0,
        "categoriesOptimized": optimized_data.get("categories_restructured", 0) if optimized_data else 0,
        "totalChanges": optimized_data.get("total_changes", 0) if optimized_data else 0,
    }

    score_delta = {
        "before": score_before,
        "after": score_after,
        "delta": score_after - score_before,
        "breakdown": report_data.get("scoreBreakdown", {}),
    }

    seo_keywords = report_data.get("missingSeoKeywords", [])
    seo_coverage = _compute_seo_coverage(
        {"categories": raw_scraped.get("categories", [])},
        seo_keywords,
    )

    # ── Step 3: Basic competitor preview ──
    await progress(40, f"Pobieranie danych o {len(selected_competitor_ids)} konkurentach...")
    basic_competitor_data = await _build_basic_competitor_data(
        supabase, selected_competitor_ids
    )
    await progress(
        60,
        f"Konkurenci: {len(basic_competitor_data)} z {len(selected_competitor_ids)} znalezionych",
    )

    # ── Step 4: Generate narrative summary text ──
    await progress(75, "Generowanie narracji podsumowania...")
    t0 = time.time()
    summary_text = await _generate_summary_text(
        client=client,
        salon_name=salon_name,
        score_before=score_before,
        score_after=score_after,
        key_wins=key_wins,
        stats=stats,
    )
    dt = int((time.time() - t0) * 1000)
    await progress(85, f"Narracja wygenerowana: {len(summary_text)} znaków ({dt}ms)")

    cta_text = (
        "Chcesz pełnej analizy konkurencji? SWOT, ranking lokalny, rekomendacje "
        "strategiczne i plan działania — dostępne w pakiecie Premium Competitor Analysis."
    )

    summary_payload = {
        "convex_audit_id": audit_id,
        "convex_user_id": user_id,
        "summary_text": summary_text,
        "score_delta": score_delta,
        "key_wins": key_wins,
        "transformations_count": len(transformations),
        "seo_coverage_percent": seo_coverage,
        "basic_competitor_data": basic_competitor_data,
        "cta_text": cta_text,
        "version": "v1",
    }

    # ── Step 5: Save to Supabase audit_summaries ──
    await progress(95, "Zapis podsumowania do Supabase audit_summaries...")
    # NOTE: save_audit_summary is a new SupabaseService method added as part of
    #       task BEAUTY_AUDIT-3ia. Until it exists, we upsert directly.
    try:
        supabase.client.table("audit_summaries").upsert(
            summary_payload, on_conflict="convex_audit_id"
        ).execute()
    except Exception as e:
        logger.error("[%s][summary] Failed to save audit_summary: %s", audit_id, e)
        raise

    await progress(
        100,
        f"Podsumowanie gotowe: {len(key_wins)} key wins, "
        f"{len(basic_competitor_data)} konkurentów, "
        f"score {score_before}→{score_after}",
    )

    return summary_payload
