"""Category restructuring helper — extracted from pipelines/cennik.py.

This module owns the agent loop that proposes a new category structure for
an (already content-transformed) salon pricelist. Moved out of BAGENT #2
(cennik) into its own module so BAGENT #1 (report) can call it as part of
the main report pipeline (Etap 1 of the Unified Report Pipeline plan —
docs/plans/2026-04-08-unified-report-pipeline.md).

Design notes:
    * The input pricelist should already have naming + description
      transformations applied mechanically — the agent sees the best
      available context that way.
    * Non-fatal on agent failure: if MiniMax flakes, we return an empty
      mapping so the caller can fall through with unchanged categories
      rather than failing the whole report pipeline.
    * We intentionally duplicate `_normalize_item` and
      `_build_pricelist_text_from_dict` here rather than importing from
      cennik.py, so this module has no back-reference to the pipeline
      that originally owned this logic. cennik.py will keep its own
      copies of those helpers for the deterministic transformation path.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


ProgressCallback = Callable[[int, str], Awaitable[None]]


def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


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


def _build_pricelist_text_from_dict(pricelist: dict[str, Any]) -> str:
    """Build full pricelist text from a plain dict (not a ScrapedData model)."""
    text = f"SALON: {pricelist.get('salonName') or 'Nieznany'}\n\n"
    for cat in pricelist.get("categories", []):
        cat_name = cat.get("name", "Bez kategorii")
        services = cat.get("services", [])
        text += f"\n## KATEGORIA: {cat_name} ({len(services)} usług)\n"
        for s in services:
            text += f'- "{s.get("name", "")}" | {s.get("price", "")}'
            if s.get("duration"):
                text += f" | {s['duration']}"
            if s.get("description"):
                desc = s["description"]
                if len(desc) > 150:
                    desc = desc[:150] + "..."
                text += f" | OPIS: {desc}"
            text += "\n"
    return text


async def restructure_categories(
    client: Any,
    transformed_pricelist: dict[str, Any],
    top_issues: list[dict[str, Any]],
    audit_id: str,
    on_progress: ProgressCallback,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    """Run the category-restructuring agent loop.

    Args:
        client: MiniMaxClient instance — must support run_agent_loop.
        transformed_pricelist: dict {salonName, categories: [{name, services: [...]}]},
            ideally already with naming + description transformations applied.
        top_issues: full list of issues from the report. We filter internally
            for dimension == "structure".
        audit_id: for log lines.
        on_progress: async progress callback — unused for now, kept in the
            signature so future progress reporting can be plugged in without
            touching callers.

    Returns:
        (category_mapping, category_changes)
          category_mapping: {original_category_name: new_category_name}
          category_changes: [{type:"category", before, after, reason}, ...]

    Non-fatal: if the agent loop or parsing fails, returns empty structures
    so the caller can fall through with unchanged categories.
    """
    from agent.runner import run_agent_loop
    from agent.tools import CATEGORY_MAPPING_TOOL

    # Intentionally a no-op reference so type checkers don't flag on_progress
    # as unused. Callers may pass a noop; we don't emit progress from here
    # because the parent pipeline owns the overall progress bar.
    _ = on_progress

    structure_issues = [i for i in top_issues if i.get("dimension") == "structure"]
    structure_issues_text = "\n".join(
        f"- [{i.get('severity', 'minor')}] {i.get('issue', '')} (fix: {i.get('fix', 'brak')})"
        for i in structure_issues[:10]
    )

    pricelist_text = _build_pricelist_text_from_dict(transformed_pricelist)

    cat_prompt = _load_prompt("optimization_categories.txt")
    if not cat_prompt:
        cat_prompt = (
            "Zaproponuj nową strukturę kategorii cennika. "
            "Użyj narzędzia submit_category_mapping.\n\n"
            "CENNIK:\n{pricelist_text}\n\n"
            "PROBLEMY STRUKTURALNE:\n{structure_issues_text}"
        )
    user_msg = (
        cat_prompt
        .replace("{pricelist_text}", pricelist_text)
        .replace("{structure_issues_text}", structure_issues_text)
    )

    category_mapping: dict[str, str] = {}
    category_changes: list[dict[str, Any]] = []

    t0 = time.time()
    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od struktury cenników salonów beauty.",
            user_message=user_msg,
            tools=[CATEGORY_MAPPING_TOOL],
            max_steps=10,
            on_step=lambda step, count: logger.info(
                "[%s][category_restructure] step %d, %d calls", audit_id, step, count
            ),
        )
        dt = int((time.time() - t0) * 1000)
        logger.info(
            "[%s][category_restructure] agent done: %d steps (%dms)",
            audit_id, agent_result.total_steps, dt,
        )

        for tc in agent_result.tool_calls:
            if tc.name == "submit_category_mapping":
                for raw_mapping in tc.input.get("mappings", []):
                    mapping = _normalize_item(raw_mapping)
                    if mapping is None:
                        continue
                    orig = mapping.get("originalCategory", "")
                    new = mapping.get("newCategory", "")
                    if orig and new and orig != new:
                        category_mapping[orig] = new
                        category_changes.append({
                            "type": "category",
                            "before": orig,
                            "after": new,
                            "reason": mapping.get("reason", "Category restructuring"),
                        })
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning(
            "[%s][category_restructure] Agent failed: %s (%dms)", audit_id, e, dt
        )
        # Non-fatal — fall through with empty mapping (no restructuring)

    return category_mapping, category_changes
