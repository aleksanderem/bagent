"""Pricelist optimization pipeline — 8-step AI-driven optimization."""

from __future__ import annotations

import logging
import time
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
    scraped_data: Any,  # ScrapedData model
    audit_report: dict,  # EnhancedAuditReport as dict
    selected_options: list[str],  # ["descriptions", "seo", "categories", "order", "tags"]
    audit_id: str,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """8-step optimization pipeline."""
    from agent.runner import run_agent_loop
    from agent.tools import CATEGORY_MAPPING_TOOL, OPTIMIZED_SERVICES_TOOL
    from config import settings
    from pipelines.helpers import build_full_pricelist_text, clean_service_name, fix_caps_lock
    from services.minimax import MiniMaxClient

    progress = on_progress or _noop_progress
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)
    total_services = scraped_data.totalServices

    # Build original service map for price/duration/variant verification
    original_service_map: dict[str, Any] = {}
    for cat in scraped_data.categories:
        for svc in cat.services:
            original_service_map[svc.name.strip().lower()] = svc

    # ── Step 1: Context Loading ──
    logger.info("[%s] Step 1: Loading optimization context...", audit_id)
    await progress(5, "Ładowanie kontekstu z audytu...")
    t0 = time.time()

    context = {
        "topIssues": audit_report.get("topIssues", []),
        "transformations": audit_report.get("transformations", []),
        "missingSeoKeywords": audit_report.get("missingSeoKeywords", []),
        "quickWins": audit_report.get("quickWins", []),
        "stats": audit_report.get("stats", {}),
    }

    n_issues = len(context["topIssues"])
    n_transforms = len(context["transformations"])
    n_seo = len(context["missingSeoKeywords"])
    n_qw = len(context["quickWins"])
    dt = int((time.time() - t0) * 1000)

    logger.info(
        "[%s] Context: %d issues, %d transformations, %d SEO keywords, %d quick wins",
        audit_id, n_issues, n_transforms, n_seo, n_qw,
    )
    await progress(
        8,
        f"Kontekst: {n_issues} problemów, {n_transforms} transformacji, "
        f"{n_seo} słów kluczowych SEO, {n_qw} quick wins ({dt}ms)",
    )

    # ── Step 2: Category Restructuring ──
    category_mapping: dict[str, str] = {}  # original_name -> new_name
    if "categories" in selected_options:
        logger.info("[%s] Step 2: Category restructuring...", audit_id)
        await progress(10, "Wysyłanie do AI — restrukturyzacja kategorii...")
        t0 = time.time()

        categories_prompt = _load_prompt("optimization_categories.txt")
        if not categories_prompt:
            categories_prompt = (
                "Zoptymalizuj strukturę kategorii cennika. "
                "Użyj narzędzia submit_category_mapping."
            )

        structure_issues = [
            i for i in context["topIssues"]
            if i.get("dimension") in ("structure", "ux")
        ]
        issues_text = "\n".join(
            f"- [{i.get('severity', 'minor')}] {i.get('issue', '')}" for i in structure_issues
        )
        pricelist_text = build_full_pricelist_text(scraped_data)

        user_msg = categories_prompt.replace("{issues}", issues_text) + f"\n\nCENNIK:\n{pricelist_text}"

        try:
            agent_result = await run_agent_loop(
                client=client,
                system_prompt="Jesteś ekspertem od struktury cenników salonów beauty.",
                user_message=user_msg,
                tools=[CATEGORY_MAPPING_TOOL],
                max_steps=10,
                on_step=lambda step, count: logger.info(
                    "[categories] Agent step %d, %d tool calls so far", step, count
                ),
            )
            dt = int((time.time() - t0) * 1000)
            tokens = agent_result.total_input_tokens + agent_result.total_output_tokens

            for tc in agent_result.tool_calls:
                if tc.name == "submit_category_mapping":
                    for mapping in tc.input.get("mappings", []):
                        orig = mapping.get("originalCategory", "")
                        new = mapping.get("newCategory", "")
                        if orig and new and orig != new:
                            category_mapping[orig] = new

            await progress(
                18,
                f"Kategorie: {len(category_mapping)} zmian, "
                f"{agent_result.total_steps} kroków, {tokens} tokenów ({dt}ms)",
            )
        except Exception as e:
            dt = int((time.time() - t0) * 1000)
            logger.warning("Category restructuring failed: %s", e)
            await progress(18, f"Kategorie: FAILED ({dt}ms): {e}")
    else:
        await progress(18, "Kategorie: pominięto (nie wybrano)")

    # ── Step 3: Service Optimization (core step) ──
    logger.info("[%s] Step 3: Service optimization...", audit_id)
    await progress(20, f"Wysyłanie do AI — optymalizacja {total_services} usług...")
    t0 = time.time()

    services_prompt = _load_prompt("optimization_services.txt")
    if not services_prompt:
        services_prompt = (
            "Zoptymalizuj nazwy i opisy usług. "
            "Użyj narzędzia submit_optimized_services partiami po 15-20."
        )

    issues_text = "\n".join(
        f"- [{i.get('severity', 'minor')}] {i.get('issue', '')}" for i in context["topIssues"][:20]
    )
    seo_text = ", ".join(k.get("keyword", "") for k in context["missingSeoKeywords"][:15])
    options_text = ", ".join(selected_options)
    pricelist_text = build_full_pricelist_text(scraped_data)

    user_msg = (
        services_prompt
        .replace("{issues}", issues_text)
        .replace("{seo_keywords}", seo_text)
        .replace("{selected_options}", options_text)
        + f"\n\nCENNIK:\n{pricelist_text}"
    )

    optimized_service_map: dict[str, dict[str, Any]] = {}
    changes: list[dict[str, Any]] = []

    try:
        agent_result = await run_agent_loop(
            client=client,
            system_prompt="Jesteś ekspertem od optymalizacji cenników salonów beauty.",
            user_message=user_msg,
            tools=[OPTIMIZED_SERVICES_TOOL],
            max_steps=30,
            on_step=lambda step, count: logger.info(
                "[services] Agent step %d, %d tool calls so far", step, count
            ),
        )
        dt = int((time.time() - t0) * 1000)
        tokens = agent_result.total_input_tokens + agent_result.total_output_tokens
        await progress(
            45,
            f"Agent done: {agent_result.total_steps} kroków, "
            f"{len(agent_result.tool_calls)} wywołań, {tokens} tokenów ({dt}ms)",
        )

        # Post-processing: validate and collect optimizations
        accepted = 0
        rejected = 0
        for tc in agent_result.tool_calls:
            if tc.name == "submit_optimized_services":
                for svc in tc.input.get("services", []):
                    original_name = svc.get("originalName", "")
                    new_name = svc.get("newName", original_name)
                    new_desc = svc.get("newDescription")
                    tags = svc.get("tags")
                    reason = svc.get("reason", "Optymalizacja")

                    # Apply programmatic fixes
                    new_name = clean_service_name(new_name)
                    new_name = fix_caps_lock(new_name)
                    if new_desc:
                        new_desc = fix_caps_lock(new_desc)

                    # CRITICAL: verify prices/durations/variants are NOT changed
                    # (agent should not send them, but verify the output only touches names/descriptions)
                    orig_svc = original_service_map.get(original_name.strip().lower())

                    if not orig_svc:
                        rejected += 1
                        logger.warning("Rejected optimization: original service '%s' not found", original_name)
                        continue

                    key = original_name.strip().lower()
                    optimized_service_map[key] = {
                        "newName": new_name,
                        "newDescription": new_desc,
                        "tags": tags,
                        "reason": reason,
                    }
                    accepted += 1

                    # Track changes
                    if new_name != orig_svc.name:
                        changes.append({
                            "type": "name",
                            "serviceName": orig_svc.name,
                            "before": orig_svc.name,
                            "after": new_name,
                            "reason": reason,
                        })
                    if new_desc and new_desc != (orig_svc.description or ""):
                        changes.append({
                            "type": "description",
                            "serviceName": orig_svc.name,
                            "before": orig_svc.description or "",
                            "after": new_desc,
                            "reason": reason,
                        })

        logger.info("[%s] Service optimization: %d accepted, %d rejected", audit_id, accepted, rejected)
        await progress(50, f"Usługi: {accepted} zaakceptowanych, {rejected} odrzuconych")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Service optimization failed: %s", e)
        await progress(50, f"Optymalizacja usług: FAILED ({dt}ms): {e}")

    # ── Step 4: SEO Enrichment ──
    seo_keywords_added = 0
    if "seo" in selected_options and context["missingSeoKeywords"]:
        logger.info("[%s] Step 4: SEO enrichment...", audit_id)
        await progress(55, f"SEO: wzbogacanie nazw o {n_seo} brakujących słów kluczowych...")
        t0 = time.time()

        seo_prompt = (
            "Wzbogać nazwy usług o brakujące słowa kluczowe SEO. "
            "Dodawaj słowa kluczowe TYLKO tam, gdzie brzmi to naturalnie. "
            "NIE zmieniaj cen, czasów trwania, wariantów. "
            "Użyj narzędzia submit_optimized_services.\n\n"
            f"BRAKUJĄCE SŁOWA KLUCZOWE SEO:\n{seo_text}\n\n"
            f"CENNIK:\n{pricelist_text}"
        )

        try:
            seo_result = await run_agent_loop(
                client=client,
                system_prompt="Jesteś ekspertem SEO dla salonów beauty.",
                user_message=seo_prompt,
                tools=[OPTIMIZED_SERVICES_TOOL],
                max_steps=20,
                on_step=lambda step, count: logger.info(
                    "[seo] Agent step %d, %d tool calls so far", step, count
                ),
            )
            dt = int((time.time() - t0) * 1000)

            for tc in seo_result.tool_calls:
                if tc.name == "submit_optimized_services":
                    for svc in tc.input.get("services", []):
                        original_name = svc.get("originalName", "")
                        new_name = svc.get("newName", original_name)
                        key = original_name.strip().lower()

                        new_name = clean_service_name(new_name)
                        new_name = fix_caps_lock(new_name)

                        orig_svc = original_service_map.get(key)
                        if not orig_svc:
                            continue

                        # Only accept if it enriches existing optimization or adds new one
                        existing = optimized_service_map.get(key)
                        if existing:
                            # Update name only if SEO added something
                            if new_name != existing["newName"] and new_name != orig_svc.name:
                                existing["newName"] = new_name
                                seo_keywords_added += 1
                        elif new_name != orig_svc.name:
                            optimized_service_map[key] = {
                                "newName": new_name,
                                "newDescription": svc.get("newDescription"),
                                "tags": svc.get("tags"),
                                "reason": "SEO enrichment",
                            }
                            seo_keywords_added += 1
                            changes.append({
                                "type": "name",
                                "serviceName": orig_svc.name,
                                "before": orig_svc.name,
                                "after": new_name,
                                "reason": "Wzbogacenie SEO",
                            })

            await progress(62, f"SEO: {seo_keywords_added} nazw wzbogaconych ({dt}ms)")
        except Exception as e:
            dt = int((time.time() - t0) * 1000)
            logger.warning("SEO enrichment failed: %s", e)
            await progress(62, f"SEO: FAILED ({dt}ms): {e}")
    else:
        await progress(62, "SEO: pominięto (nie wybrano lub brak brakujących słów kluczowych)")

    # ── Step 5: Programmatic Fixes ──
    logger.info("[%s] Step 5: Programmatic fixes...", audit_id)
    await progress(65, "Programmatyczne poprawki (CAPS LOCK, nazwy, duplikaty)...")
    t0 = time.time()

    caps_fixed = 0
    names_cleaned = 0
    duplicates_found = 0
    promo_detected = 0

    # Fix caps lock and clean names in all optimized services
    for key, opt in optimized_service_map.items():
        original_name = opt["newName"]
        fixed_name = fix_caps_lock(original_name)
        if fixed_name != original_name:
            opt["newName"] = fixed_name
            caps_fixed += 1

        cleaned = clean_service_name(opt["newName"])
        if cleaned != opt["newName"]:
            opt["newName"] = cleaned
            names_cleaned += 1

        if opt.get("newDescription"):
            original_desc = opt["newDescription"]
            fixed_desc = fix_caps_lock(original_desc)
            if fixed_desc != original_desc:
                opt["newDescription"] = fixed_desc
                caps_fixed += 1

    # Detect duplicates across categories
    seen_names: dict[str, str] = {}  # normalized_name -> category
    for cat in scraped_data.categories:
        for svc in cat.services:
            key = svc.name.strip().lower()
            opt = _find_optimization(svc.name, optimized_service_map)
            name = opt["newName"] if opt else svc.name
            normalized = name.strip().lower()
            if normalized in seen_names and seen_names[normalized] != cat.name:
                duplicates_found += 1
            seen_names[normalized] = cat.name

    # Detect promo services
    for cat in scraped_data.categories:
        for svc in cat.services:
            if _detect_promo(svc.name):
                promo_detected += 1

    dt = int((time.time() - t0) * 1000)
    logger.info(
        "[%s] Programmatic fixes: %d caps, %d names, %d duplicates, %d promo",
        audit_id, caps_fixed, names_cleaned, duplicates_found, promo_detected,
    )
    await progress(
        70,
        f"Poprawki: {caps_fixed} CAPS, {names_cleaned} nazw, "
        f"{duplicates_found} duplikatów, {promo_detected} promo ({dt}ms)",
    )

    # ── Step 6: Quality Verification ──
    logger.info("[%s] Step 6: Quality verification...", audit_id)
    await progress(72, "Weryfikacja jakości optymalizacji...")
    t0 = time.time()

    quality_score = 75  # default
    quality_fixed: list[str] = []
    quality_remaining: list[str] = []
    quality_recommendations: list[str] = []

    verify_prompt = _load_prompt("optimization_verify.txt")
    if not verify_prompt:
        verify_prompt = (
            "Zweryfikuj jakość optymalizacji. "
            "Zwróć JSON z: fixed, remaining, qualityScore, recommendations."
        )

    issues_summary = "\n".join(
        f"- {i.get('issue', '')}" for i in context["topIssues"][:10]
    )
    opt_summary = (
        f"Zoptymalizowano {len(optimized_service_map)} usług z {total_services}.\n"
        f"Zmieniono: {len(changes)} elementów.\n"
        f"Kategorie zmapowane: {len(category_mapping)}.\n"
        f"SEO wzbogacone: {seo_keywords_added}.\n"
        f"Duplikaty znalezione: {duplicates_found}."
    )

    full_verify = (
        verify_prompt
        .replace("{refinement_prefix}", "")
        .replace("{original_issues_text}", issues_summary)
        .replace("{optimized_pricelist_text}", opt_summary)
        .replace("{issues}", issues_summary)
        .replace("{optimization_summary}", opt_summary)
    )

    try:
        verification = await client.generate_json(
            full_verify,
            system="Jesteś ekspertem od jakości cenników beauty.",
        )
        quality_score = min(100, max(0, int(verification.get("qualityScore", 75))))
        quality_fixed = verification.get("fixed", [])
        quality_remaining = verification.get("remaining", [])
        quality_recommendations = verification.get("recommendations", [])
        dt = int((time.time() - t0) * 1000)

        if quality_score < 60:
            logger.warning("[%s] Quality score low: %d/100", audit_id, quality_score)
            await progress(78, f"Jakość: {quality_score}/100 (NISKA) — {len(quality_fixed)} naprawionych, {len(quality_remaining)} pozostałych ({dt}ms)")
        else:
            await progress(78, f"Jakość: {quality_score}/100 — {len(quality_fixed)} naprawionych, {len(quality_remaining)} pozostałych ({dt}ms)")
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        logger.warning("Quality verification failed: %s", e)
        await progress(78, f"Weryfikacja jakości: FAILED ({dt}ms): {e}")

    # ── Step 7: Assembly ──
    logger.info("[%s] Step 7: Assembling optimized pricelist...", audit_id)
    await progress(82, "Składanie zoptymalizowanego cennika...")
    t0 = time.time()

    # Build optimizedPricingData with SAME structure as original
    optimized_categories: list[dict[str, Any]] = []
    names_improved = 0
    descriptions_added = 0

    for cat in scraped_data.categories:
        cat_name = category_mapping.get(cat.name, cat.name)
        optimized_services: list[dict[str, Any]] = []

        for svc in cat.services:
            opt = _find_optimization(svc.name, optimized_service_map)

            new_name = opt["newName"] if opt else svc.name
            new_desc = (opt.get("newDescription") or svc.description) if opt else svc.description
            tags = opt.get("tags") if opt else None

            if new_name != svc.name:
                names_improved += 1
            if new_desc and new_desc != (svc.description or ""):
                descriptions_added += 1

            optimized_services.append({
                "name": new_name,
                "price": svc.price,  # NEVER CHANGE
                "duration": svc.duration,  # NEVER CHANGE
                "description": new_desc,
                "imageUrl": svc.imageUrl,  # PRESERVE
                "variants": [v.model_dump() for v in svc.variants] if svc.variants else None,  # NEVER CHANGE
                "isPromo": _detect_promo(svc.name),
                "tags": tags,
            })

        optimized_categories.append({
            "categoryName": cat_name,
            "services": optimized_services,
        })

    optimized_pricing_data = {
        "salonName": scraped_data.salonName,
        "salonAddress": scraped_data.salonAddress,
        "salonLogoUrl": scraped_data.salonLogoUrl,
        "totalServices": total_services,
        "categories": optimized_categories,
    }

    summary = {
        "totalChanges": len(changes),
        "namesImproved": names_improved,
        "descriptionsAdded": descriptions_added,
        "duplicatesFound": duplicates_found,
        "categoriesOptimized": len(category_mapping),
        "seoKeywordsAdded": seo_keywords_added,
    }

    dt = int((time.time() - t0) * 1000)
    await progress(
        90,
        f"Cennik złożony: {names_improved} nazw, {descriptions_added} opisów, "
        f"{len(category_mapping)} kategorii, {len(changes)} zmian ({dt}ms)",
    )

    # ── Step 8: Return ──
    result: dict[str, Any] = {
        "optimizedPricingData": optimized_pricing_data,
        "changes": changes,
        "summary": summary,
        "recommendations": quality_recommendations,
        "qualityScore": quality_score,
    }

    await progress(
        100,
        f"Optymalizacja gotowa! Score: {quality_score}/100, "
        f"{len(changes)} zmian, {names_improved} nazw, {descriptions_added} opisów",
    )

    return result
