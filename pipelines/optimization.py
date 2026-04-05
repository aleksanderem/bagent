"""Pricelist optimization pipeline — 8-step AI-driven optimization."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

PROMO_KEYWORDS = ("promocja", "rabat", "zniżka", "promo", "okazja", "-50%", "-30%", "-20%", "gratis", "pakiet")


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


def _normalize_item(item: Any) -> dict[str, Any] | None:
    """Normalize a tool call array item — MiniMax sometimes returns strings instead of dicts."""
    if isinstance(item, dict):
        return item
    if isinstance(item, str):
        try:
            parsed = json.loads(item)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def _detect_promo(name: str) -> bool:
    """Detect promo services by name keywords."""
    lower = name.lower()
    return any(kw in lower for kw in PROMO_KEYWORDS)


def _find_optimization(
    original_name: str,
    optimized_map: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Find optimization for a service by original name (case-insensitive)."""
    key = original_name.strip().lower()
    if key in optimized_map:
        return optimized_map[key]
    # Fallback: try partial match
    for map_key, opt in optimized_map.items():
        if map_key in key or key in map_key:
            return opt
    return None


async def run_optimization_pipeline(
    audit_id: str,
    selected_options: list[str] | None = None,
    on_progress: ProgressCallback | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Legacy wrapper — runs all 4 phases sequentially."""
    from pipelines.optimize_phases import (
        run_phase1_seo,
        run_phase2_content,
        run_phase3_categories,
        run_phase4_finalize,
    )
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress

    await progress(5, "Faza 1: SEO — wstrzykiwanie słów kluczowych...")
    phase1 = await run_phase1_seo(audit_id=audit_id, on_progress=progress)

    await progress(35, "Faza 2: Treści — nazwy i opisy usług...")
    phase2 = await run_phase2_content(audit_id=audit_id, pricelist=phase1["pricelist"], on_progress=progress)

    await progress(60, "Faza 3: Kategorie — reorganizacja struktury...")
    phase3 = await run_phase3_categories(audit_id=audit_id, pricelist=phase2["pricelist"], on_progress=progress)

    await progress(80, "Faza 4: Finalizacja — poprawki i lista zmian...")
    supabase = SupabaseService()
    original = await supabase.get_scraped_data(audit_id)
    phase4 = run_phase4_finalize(audit_id=audit_id, pricelist=phase3["pricelist"], original_pricelist=original)

    await supabase.save_optimized_pricelist(
        convex_audit_id=audit_id,
        optimization_data=phase4,
        salon_name=original.get("salonName", ""),
    )

    await progress(100, f"Optymalizacja gotowa! Score: {phase4['qualityScore']}/100, {phase4['summary']['totalChanges']} zmian")
    return phase4
