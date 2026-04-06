"""Phased pricelist optimization pipeline — 4 independent AI-driven phases."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

PROMO_KEYWORDS = ("promocja", "rabat", "zniżka", "promo", "okazja", "-50%", "-30%", "-20%", "gratis", "pakiet")

ProgressCallback = Callable[[int, str], Awaitable[None]]


async def _noop_progress(progress: int, message: str) -> None:
    pass


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


def _detect_promo(name: str) -> bool:
    """Detect promo services by name keywords."""
    lower = name.lower()
    return any(kw in lower for kw in PROMO_KEYWORDS)


def _build_pricelist_text_from_dict(pricelist: dict[str, Any]) -> str:
    """Build full pricelist text from a plain dict (not a ScrapedData model).

    Same output format as helpers.build_full_pricelist_text but works on dicts.
    """
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


def _build_original_service_map(pricelist: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Build a lookup map from lowercase service name to service dict."""
    service_map: dict[str, dict[str, Any]] = {}
    for cat in pricelist.get("categories", []):
        for svc in cat.get("services", []):
            key = svc.get("name", "").strip().lower()
            service_map[key] = svc
    return service_map


# ---------------------------------------------------------------------------
# Phase 1: SEO Enrichment
# ---------------------------------------------------------------------------


async def run_phase1_seo(
    audit_id: str,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Phase 1: Enrich service names with missing SEO keywords.

    Loads data from Supabase, runs agent loop with SEO prompt,
    returns pricelist with SEO-enriched names.
    """
    from agent.runner import run_agent_loop
    from agent.tools import OPTIMIZED_SERVICES_TOOL
    from config import settings
    from models.scraped_data import ScrapedData
    from pipelines.helpers import build_full_pricelist_text, clean_service_name, fix_caps_lock
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()

    # Load scraped data
    await progress(5, "Ładowanie danych z Supabase...")
    raw_scraped = await supabase.get_scraped_data(audit_id)
    scraped_data = ScrapedData(**raw_scraped)
    logger.info("[%s][seo] Loaded %d services, %d categories", audit_id, scraped_data.totalServices, len(scraped_data.categories))

    # Load audit report for SEO keywords
    report_data = await supabase.get_audit_report(audit_id) or {}
    missing_seo = report_data.get("missingSeoKeywords", [])
    seo_text = ", ".join(k.get("keyword", "") for k in missing_seo[:15])
    logger.info("[%s][seo] Missing SEO keywords: %d", audit_id, len(missing_seo))

    # All categories in parallel — one generate_json call each
    optimized_map: dict[str, dict[str, Any]] = {}
    seo_changes: list[dict[str, Any]] = []
    keywords_added = 0

    async def _seo_one_category(cat: Any) -> None:
        nonlocal keywords_added
        services_list = [{"name": svc.name, "price": svc.price} for svc in cat.services]
        prompt = (
            f"Wzbogać nazwy usług o słowa kluczowe SEO dla salonu beauty.\n"
            f"Brakujące słowa kluczowe: {seo_text or 'brak konkretnych — dodaj ogólne beauty keywords'}\n\n"
            f"Kategoria: {cat.name}\n"
            f"Usługi (JSON):\n{json.dumps(services_list, ensure_ascii=False)}\n\n"
            f"Zwróć JSON: {{\"services\": [{{\"originalName\": \"...\", \"newName\": \"...\"}}]}}\n"
            f"Jeśli nazwa nie wymaga zmiany, zwróć ją bez zmian."
        )
        t0 = time.time()
        try:
            result = await client.generate_json(prompt, system="Jesteś ekspertem SEO dla salonów beauty. Odpowiadaj WYŁĄCZNIE JSON.")
            dt = int((time.time() - t0) * 1000)
            logger.info("[%s][seo] Category '%s' done (%dms)", audit_id, cat.name[:30], dt)
            for svc_item in result.get("services", []):
                original_name = svc_item.get("originalName", "")
                new_name = svc_item.get("newName", original_name)
                new_name = clean_service_name(new_name)
                new_name = fix_caps_lock(new_name)
                key = original_name.strip().lower()
                optimized_map[key] = {"newName": new_name, "categoryName": cat.name}
                if new_name != original_name:
                    seo_changes.append({"type": "seo", "serviceName": original_name, "before": original_name, "after": new_name, "reason": "SEO keyword enrichment"})
                    keywords_added += 1
        except Exception as e:
            logger.warning("[%s][seo] Category '%s' failed: %s (%dms)", audit_id, cat.name[:30], e, int((time.time() - t0) * 1000))

    await progress(20, f"SEO: {len(scraped_data.categories)} kategorii równolegle...")
    await asyncio.gather(*[_seo_one_category(cat) for cat in scraped_data.categories])

    # Build output pricelist
    output_categories: list[dict[str, Any]] = []
    for cat in scraped_data.categories:
        services_out: list[dict[str, Any]] = []
        for svc in cat.services:
            key = svc.name.strip().lower()
            opt = optimized_map.get(key)
            new_name = opt["newName"] if opt else svc.name
            services_out.append({
                "name": new_name,
                "price": svc.price,
                "duration": svc.duration,
                "description": svc.description,
                "imageUrl": svc.imageUrl,
                "variants": [v.model_dump() for v in svc.variants] if svc.variants else None,
                "tags": None,
                "isPromo": False,
            })
        output_categories.append({"name": cat.name, "services": services_out})

    output_pricelist = {
        "salonName": scraped_data.salonName,
        "salonAddress": scraped_data.salonAddress,
        "salonLogoUrl": scraped_data.salonLogoUrl,
        "totalServices": scraped_data.totalServices,
        "categories": output_categories,
    }

    await progress(100, f"SEO done: {keywords_added} keywords added")
    return {"pricelist": output_pricelist, "seoChanges": seo_changes, "keywordsAdded": keywords_added}


# ---------------------------------------------------------------------------
# Phase 2: Content Optimization
# ---------------------------------------------------------------------------


async def run_phase2_content(
    audit_id: str,
    pricelist: dict[str, Any],
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Phase 2: Improve service names and add/improve descriptions.

    Takes SEO-enriched pricelist from phase 1, returns content-optimized pricelist.
    """
    from agent.runner import run_agent_loop
    from agent.tools import OPTIMIZED_SERVICES_TOOL
    from config import settings
    from pipelines.helpers import clean_service_name, fix_caps_lock
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()

    # Load audit report for issues and transformations
    await progress(5, "Ładowanie raportu audytu...")
    report_data = await supabase.get_audit_report(audit_id) or {}

    top_issues = report_data.get("topIssues", [])
    transformations = report_data.get("transformations", [])

    audit_issues_text = "\n".join(
        f"- [{i.get('severity', 'minor')}] {i.get('issue', '')} (fix: {i.get('fix', 'brak')})"
        for i in top_issues[:20]
    )
    transformations_text = "\n".join(
        f"- {t.get('serviceName', '')}: {t.get('before', '')} → {t.get('after', '')}"
        for t in transformations[:20]
    )

    # Load prompt template
    content_prompt = _load_prompt("optimization_content.txt")
    if not content_prompt:
        content_prompt = (
            "Popraw nazwy i opisy usług. Użyj submit_optimized_services.\n\n"
            "CENNIK:\n{pricelist_text}\n\n"
            "PROBLEMY:\n{audit_issues_text}\n\n"
            "TRANSFORMACJE:\n{transformations_text}"
        )

    # Build service lookup from input pricelist
    service_map = _build_original_service_map(pricelist)

    # All categories in parallel
    optimized_map: dict[str, dict[str, Any]] = {}
    content_changes: list[dict[str, Any]] = []
    names_improved = 0
    descriptions_added = 0
    categories = pricelist.get("categories", [])

    async def _content_one_category(cat: dict[str, Any]) -> None:
        nonlocal names_improved, descriptions_added
        cat_name = cat.get("name", "Bez kategorii")
        services = cat.get("services", [])
        services_json = json.dumps(
            [{"name": s.get("name", ""), "price": s.get("price", ""), "description": s.get("description") or ""} for s in services],
            ensure_ascii=False,
        )
        prompt = (
            f"Popraw nazwy usług i dodaj/ulepsz opisy dla salonu beauty.\n\n"
            f"Kategoria: {cat_name}\n"
            f"Usługi:\n{services_json}\n\n"
            f"PROBLEMY Z AUDYTU:\n{audit_issues_text}\n\n"
            f"Zwróć JSON: {{\"services\": [{{\"originalName\": \"...\", \"newName\": \"...\", \"newDescription\": \"...\"}}]}}\n"
            f"Opis max 2 zdania, język korzyści. Jeśli opis OK — zwróć bez zmian."
        )
        t0 = time.time()
        try:
            result = await client.generate_json(prompt, system="Jesteś copywriterem salonów beauty. Odpowiadaj WYŁĄCZNIE JSON.")
            logger.info("[%s][content] Category '%s' done (%dms)", audit_id, cat_name[:30], int((time.time() - t0) * 1000))
            for svc_item in result.get("services", []):
                original_name = svc_item.get("originalName", "")
                new_name = clean_service_name(svc_item.get("newName", original_name))
                new_name = fix_caps_lock(new_name)
                new_desc = svc_item.get("newDescription")
                if new_desc:
                    new_desc = fix_caps_lock(new_desc)
                key = original_name.strip().lower()
                orig_svc = service_map.get(key)
                if not orig_svc:
                    continue
                optimized_map[key] = {"newName": new_name, "newDescription": new_desc, "tags": None}
                if new_name != orig_svc.get("name", ""):
                    content_changes.append({"type": "name", "serviceName": orig_svc["name"], "before": orig_svc["name"], "after": new_name, "reason": "Content optimization"})
                    names_improved += 1
                if new_desc and new_desc != (orig_svc.get("description") or ""):
                    content_changes.append({"type": "description", "serviceName": orig_svc["name"], "before": orig_svc.get("description") or "", "after": new_desc, "reason": "Content optimization"})
                    descriptions_added += 1
        except Exception as e:
            logger.warning("[%s][content] Category '%s' failed: %s (%dms)", audit_id, cat_name[:30], e, int((time.time() - t0) * 1000))

    await progress(20, f"Content: {len(categories)} kategorii równolegle...")
    await asyncio.gather(*[_content_one_category(cat) for cat in categories])

    # Build output pricelist
    output_categories: list[dict[str, Any]] = []
    for cat in pricelist.get("categories", []):
        services_out: list[dict[str, Any]] = []
        for svc in cat.get("services", []):
            key = svc.get("name", "").strip().lower()
            opt = optimized_map.get(key)
            new_name = opt["newName"] if opt else svc["name"]
            new_desc = (opt.get("newDescription") or svc.get("description")) if opt else svc.get("description")
            tags = opt.get("tags") if opt else svc.get("tags")
            services_out.append({
                "name": new_name,
                "price": svc["price"],
                "duration": svc.get("duration"),
                "description": new_desc,
                "imageUrl": svc.get("imageUrl"),
                "variants": svc.get("variants"),
                "tags": tags,
                "isPromo": svc.get("isPromo", False),
            })
        output_categories.append({"name": cat["name"], "services": services_out})

    output_pricelist = {
        "salonName": pricelist.get("salonName"),
        "salonAddress": pricelist.get("salonAddress"),
        "salonLogoUrl": pricelist.get("salonLogoUrl"),
        "totalServices": pricelist.get("totalServices", 0),
        "categories": output_categories,
    }

    await progress(100, f"Content done: {names_improved} names, {descriptions_added} descriptions")
    return {
        "pricelist": output_pricelist,
        "contentChanges": content_changes,
        "namesImproved": names_improved,
        "descriptionsAdded": descriptions_added,
    }


# ---------------------------------------------------------------------------
# Phase 3: Category Restructuring
# ---------------------------------------------------------------------------


async def run_phase3_categories(
    audit_id: str,
    pricelist: dict[str, Any],
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Phase 3: Restructure categories for better UX.

    Takes content-optimized pricelist from phase 2, returns pricelist with new categories.
    """
    from agent.runner import run_agent_loop
    from agent.tools import CATEGORY_MAPPING_TOOL
    from config import settings
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    supabase = SupabaseService()

    # Load audit report for structure issues
    await progress(5, "Ładowanie raportu audytu...")
    report_data = await supabase.get_audit_report(audit_id) or {}

    structure_issues = [
        i for i in report_data.get("topIssues", [])
        if i.get("dimension") in ("structure", "ux")
    ]
    structure_issues_text = "\n".join(
        f"- [{i.get('severity', 'minor')}] {i.get('issue', '')}"
        for i in structure_issues
    )

    # Build pricelist text from input
    pricelist_text = _build_pricelist_text_from_dict(pricelist)

    # Load and fill prompt
    cat_prompt = _load_prompt("optimization_categories.txt")
    if not cat_prompt:
        cat_prompt = (
            "Zaproponuj nową strukturę kategorii cennika. "
            "Użyj narzędzia submit_category_mapping.\n\n"
            "CENNIK:\n{pricelist_text}\n\n"
            "PROBLEMY:\n{structure_issues_text}"
        )
    user_msg = (
        cat_prompt
        .replace("{pricelist_text}", pricelist_text)
        .replace("{structure_issues_text}", structure_issues_text)
    )

    # Run agent loop
    await progress(20, f"Categories: wysyłanie do AI ({len(pricelist.get('categories', []))} kategorii)...")
    t0 = time.time()

    category_mapping: dict[str, str] = {}
    category_changes: list[dict[str, Any]] = []

    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od struktury cenników salonów beauty.",
            user_message=user_msg,
            tools=[CATEGORY_MAPPING_TOOL],
            max_steps=10,
            on_step=lambda step, count: logger.info("[%s][categories] step %d, %d calls", audit_id, step, count),
        )
        dt = int((time.time() - t0) * 1000)
        await progress(60, f"Categories agent done: {agent_result.total_steps} steps ({dt}ms)")

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
        logger.warning("[%s][categories] Agent failed: %s (%dms)", audit_id, e, dt)
        await progress(60, f"Categories: FAILED ({dt}ms): {e}")

    # Apply category mapping to pricelist
    output_categories: list[dict[str, Any]] = []
    for cat in pricelist.get("categories", []):
        cat_name = category_mapping.get(cat["name"], cat["name"])
        output_categories.append({
            "name": cat_name,
            "services": cat["services"],  # services unchanged in this phase
        })

    output_pricelist = {
        "salonName": pricelist.get("salonName"),
        "salonAddress": pricelist.get("salonAddress"),
        "salonLogoUrl": pricelist.get("salonLogoUrl"),
        "totalServices": pricelist.get("totalServices", 0),
        "categories": output_categories,
    }

    await progress(100, f"Categories done: {len(category_mapping)} changes")
    return {"pricelist": output_pricelist, "categoryChanges": category_changes}


# ---------------------------------------------------------------------------
# Phase 4: Finalize (deterministic, no AI)
# ---------------------------------------------------------------------------


def run_phase4_finalize(
    audit_id: str,
    pricelist: dict[str, Any],
    original_pricelist: dict[str, Any],
) -> dict[str, Any]:
    """Phase 4: Apply programmatic fixes and generate diff. No AI calls.

    Applies clean_service_name, fix_caps_lock, promo detection, duplicate detection.
    Generates a diff comparing each service to the original pricelist (matched by position).
    """
    from pipelines.helpers import clean_service_name, fix_caps_lock

    changes: list[dict[str, Any]] = []
    names_improved = 0
    descriptions_added = 0
    categories_optimized = 0
    duplicates_found = 0
    seo_keywords_added = 0

    # Apply programmatic fixes and build final pricelist
    final_categories: list[dict[str, Any]] = []
    seen_names: dict[str, str] = {}  # normalized_name -> category

    for cat_idx, cat in enumerate(pricelist.get("categories", [])):
        services_out: list[dict[str, Any]] = []

        for svc_idx, svc in enumerate(cat.get("services", [])):
            name = svc.get("name", "")

            # Apply programmatic fixes
            name = clean_service_name(name)
            name = fix_caps_lock(name)

            desc = svc.get("description")
            if desc:
                desc = fix_caps_lock(desc)

            is_promo = _detect_promo(name)

            # Duplicate detection
            normalized = name.strip().lower()
            if normalized in seen_names and seen_names[normalized] != cat.get("name", ""):
                duplicates_found += 1
            seen_names[normalized] = cat.get("name", "")

            services_out.append({
                "name": name,
                "price": svc.get("price", ""),
                "duration": svc.get("duration"),
                "description": desc,
                "imageUrl": svc.get("imageUrl"),
                "variants": svc.get("variants"),
                "tags": svc.get("tags"),
                "isPromo": is_promo,
            })

        final_categories.append({"name": cat["name"], "services": services_out})

    final_pricelist = {
        "salonName": pricelist.get("salonName"),
        "salonAddress": pricelist.get("salonAddress"),
        "salonLogoUrl": pricelist.get("salonLogoUrl"),
        "totalServices": pricelist.get("totalServices", 0),
        "categories": final_categories,
    }

    # Generate diff by comparing to original pricelist (matched by position)
    orig_cats = original_pricelist.get("categories", [])
    final_cats = final_pricelist.get("categories", [])

    for cat_idx in range(min(len(orig_cats), len(final_cats))):
        orig_cat = orig_cats[cat_idx]
        final_cat = final_cats[cat_idx]

        orig_cat_name = orig_cat.get("name", "")
        final_cat_name = final_cat.get("name", "")
        if orig_cat_name != final_cat_name:
            categories_optimized += 1
            changes.append({
                "type": "category",
                "before": orig_cat_name,
                "after": final_cat_name,
                "reason": "Category name optimization",
            })

        orig_services = orig_cat.get("services", [])
        final_services = final_cat.get("services", [])

        for svc_idx in range(min(len(orig_services), len(final_services))):
            orig_svc = orig_services[svc_idx]
            final_svc = final_services[svc_idx]

            orig_name = orig_svc.get("name", "")
            final_name = final_svc.get("name", "")
            if orig_name != final_name:
                names_improved += 1
                changes.append({
                    "type": "name",
                    "serviceName": orig_name,
                    "before": orig_name,
                    "after": final_name,
                    "reason": "Name optimization",
                })

            orig_desc = orig_svc.get("description") or ""
            final_desc = final_svc.get("description") or ""
            if orig_desc != final_desc:
                descriptions_added += 1
                changes.append({
                    "type": "description",
                    "serviceName": orig_name,
                    "before": orig_desc,
                    "after": final_desc,
                    "reason": "Description optimization",
                })

    # Calculate quality score: percentage of services with meaningful changes
    total_services = pricelist.get("totalServices", 0)
    if total_services > 0:
        changed_services = names_improved + descriptions_added
        quality_score = min(100, round(changed_services / total_services * 100))
    else:
        quality_score = 0

    summary = {
        "totalChanges": len(changes),
        "namesImproved": names_improved,
        "descriptionsAdded": descriptions_added,
        "categoriesOptimized": categories_optimized,
        "duplicatesFound": duplicates_found,
        "seoKeywordsAdded": seo_keywords_added,
    }

    return {
        "finalPricelist": final_pricelist,
        "changes": changes,
        "summary": summary,
        "qualityScore": quality_score,
    }
