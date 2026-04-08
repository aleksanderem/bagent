"""BAGENT #2 pipeline — new pricelist generation (Kategorie tab).

Single responsibility: take the original scraped pricelist + the report from
BAGENT #1 (which already contains all content transformations for names and
descriptions) and produce a new pricelist structure with:
    1. Transformations from audit_transformations mechanically applied
       (name + description updates — no new AI calls needed, BAGENT #1 did this)
    2. Restructured categories via agentic loop (THIS is the value-add AI work)
    3. Programmatic finalization (clean_service_name, fix_caps_lock, promo
       detection, duplicate detection) and diff generation

Writes to Supabase tables: optimized_pricelists, optimized_categories,
optimized_services, optimization_changes.

Writes to Convex categoryProposals via webhook callback.

Does NOT regenerate names or descriptions (that's BAGENT #1's job — and the
results are already in audit_transformations).
Does NOT fetch competitor data (that's BAGENT #3's job).

Inputs (from HTTP payload):
    audit_id, salon_id, scrape_id

Loads from Supabase (no data in payload):
    audit_scraped_data  — original pricelist
    audit_reports       — overall audit context
    audit_transformations — list of name/description changes to apply
    audit_issues        — structural issues to inform category restructuring

Webhook callbacks:
    POST Convex /api/audit/cennik/progress (during pipeline)
    POST Convex /api/audit/cennik/complete (on success, with categoryProposals data)
    POST Convex /api/audit/cennik/fail     (on failure after retries)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

PROMO_KEYWORDS = (
    "promocja", "rabat", "zniżka", "promo", "okazja",
    "-50%", "-30%", "-20%", "gratis", "pakiet",
)


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


def _normalize_lookup_key(name: str) -> str:
    """Normalize a service name for fuzzy lookup. Strips pricelist row
    formatting that BAGENT #1 sometimes accidentally includes in the
    `before` field (e.g. `"Laser CO2" | 1 500,00 zł | 45min |`)."""
    if not name:
        return ""
    # Drop trailing pricelist suffix if present: anything after the first ' | '
    # AFTER the service name. Handles "X | 250 zł+ | 30min |" style.
    cleaned = name.split(" | ")[0]
    # Drop wrapping quotes left over from JSON serialization
    cleaned = cleaned.strip().strip('"').strip("'")
    return cleaned.lower().strip()


def _build_transformation_map(
    transformations: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str]]:
    """Build lookup maps from original name → new name / description.

    Returns:
        (name_map, description_map) — both keyed by normalized lookup key
        (lowercase, stripped of pricelist row formatting).
    """
    name_map: dict[str, str] = {}
    desc_map: dict[str, str] = {}
    for t in transformations:
        raw = t.get("before") or t.get("serviceName") or ""
        original = _normalize_lookup_key(raw)
        if not original:
            continue
        ttype = t.get("type", "")
        after = t.get("after", "")
        if ttype == "name" and after:
            name_map[original] = after
        elif ttype == "description" and after:
            desc_map[original] = after
    return name_map, desc_map


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


async def run_cennik_pipeline(
    audit_id: str,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Full BAGENT #2 pipeline — generate new pricelist from report + original scrape.

    Steps:
        1. Load original scraped data + audit report + transformations from Supabase
        2. Apply transformations mechanically (no AI) — name and description updates
        3. Agent loop: restructure categories (the only AI step)
        4. Deterministic finalize: clean names, detect promo, detect duplicates, diff
        5. Write to Supabase: optimized_pricelists + children + optimization_changes
        6. Build categoryProposals payload for Convex webhook

    Returns a dict containing:
        pricelist            — final optimized pricelist structure
        changes              — all applied changes (name, description, category)
        category_proposal    — payload for Convex categoryProposals webhook
        stats                — counts for telemetry
    """
    from agent.runner import run_agent_loop
    from agent.tools import CATEGORY_MAPPING_TOOL
    from config import settings
    from models.scraped_data import ScrapedData
    from pipelines.helpers import clean_service_name, fix_caps_lock, sanitize_text
    from services.minimax import MiniMaxClient
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress
    client = MiniMaxClient(
        settings.minimax_api_key,
        settings.minimax_base_url,
        settings.minimax_model,
    )
    supabase = SupabaseService()

    # ── Step 1: Load data from Supabase ──
    await progress(5, "Ładowanie danych z Supabase...")
    t0 = time.time()
    raw_scraped = await supabase.get_scraped_data(audit_id)
    if not raw_scraped:
        raise RuntimeError(f"No scraped_data for audit {audit_id}")
    scraped_data = ScrapedData(**raw_scraped)

    report_data = await supabase.get_audit_report(audit_id) or {}
    transformations = report_data.get("transformations", [])
    top_issues = report_data.get("topIssues", [])
    dt = int((time.time() - t0) * 1000)
    await progress(
        10,
        f"Dane załadowane ({dt}ms): {scraped_data.totalServices} usług, "
        f"{len(transformations)} transformacji do zaaplikowania, "
        f"{len(top_issues)} problemów do uwzględnienia",
    )

    # ── Step 2: Apply transformations mechanically (no AI) ──
    # Transformations already generated by BAGENT #1 — just look them up and apply.
    await progress(15, "Mechanizne aplikowanie transformacji z raportu...")
    name_map, desc_map = _build_transformation_map(transformations)
    names_applied = 0
    descriptions_applied = 0

    original_categories: list[dict[str, Any]] = []
    transformed_categories: list[dict[str, Any]] = []

    for cat in scraped_data.categories:
        original_services: list[dict[str, Any]] = []
        transformed_services: list[dict[str, Any]] = []
        for svc in cat.services:
            svc_dict = {
                "name": svc.name,
                "price": svc.price,
                "duration": svc.duration,
                "description": svc.description,
                "imageUrl": svc.imageUrl,
                "variants": [v.model_dump() for v in svc.variants] if svc.variants else None,
                "tags": None,
                "isPromo": False,

                # Carry Booksy canonical taxonomy + provenance through the pipeline
                # so save_optimized_pricelist can populate optimized_services with
                # full parity to salon_scrape_services (no rozjazd). These fields
                # are populated by SupabaseService.get_scraped_data.
                "scrape_service_id": svc.scrape_service_id,
                "canonical_id": svc.canonical_id,
                "booksy_treatment_id": svc.booksy_treatment_id,
                "booksy_service_id": svc.booksy_service_id,
                "treatment_name": svc.treatment_name,
                "treatment_parent_id": svc.treatment_parent_id,
                "body_part": svc.body_part,
                "target_gender": svc.target_gender,
                "technology": svc.technology,
                "classification_confidence": svc.classification_confidence,
                "price_grosze": svc.price_grosze,
                "is_from_price": svc.is_from_price,
                "duration_minutes": svc.duration_minutes,
            }
            original_services.append(dict(svc_dict))

            # Use the same normalized key shape as _build_transformation_map
            # so transformations with stray pricelist formatting still match.
            key = _normalize_lookup_key(svc.name)
            new_svc = dict(svc_dict)
            original_description = svc.description
            original_name = svc.name
            original_price = svc.price
            # Track which transformations were applied for was_renamed /
            # was_description_changed flags on the optimized_services row.
            was_renamed = False
            was_description_changed = False
            if key in name_map and name_map[key] != original_name:
                new_svc["name"] = name_map[key]
                names_applied += 1
                was_renamed = True
            if key in desc_map and desc_map[key] != (original_description or ""):
                new_svc["description"] = desc_map[key]
                descriptions_applied += 1
                was_description_changed = True
            new_svc["_original_name"] = original_name
            new_svc["_original_description"] = original_description
            new_svc["_original_price"] = original_price
            new_svc["_was_renamed"] = was_renamed
            new_svc["_was_description_changed"] = was_description_changed
            transformed_services.append(new_svc)

        original_categories.append({"name": cat.name, "services": original_services})
        transformed_categories.append({"name": cat.name, "services": transformed_services})

    original_pricelist = {
        "salonName": scraped_data.salonName,
        "salonAddress": scraped_data.salonAddress,
        "salonLogoUrl": scraped_data.salonLogoUrl,
        "totalServices": scraped_data.totalServices,
        "categories": original_categories,
    }
    transformed_pricelist = {
        **original_pricelist,
        "categories": transformed_categories,
    }
    await progress(
        30,
        f"Transformacje zaaplikowane: {names_applied} nazw, {descriptions_applied} opisów",
    )

    # ── Step 3: Agent loop — category restructuring (the only AI step here) ──
    await progress(35, "Agent loop: restrukturyzacja kategorii...")
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
                "[%s][cennik] categories step %d, %d calls", audit_id, step, count
            ),
        )
        dt = int((time.time() - t0) * 1000)
        await progress(65, f"Agent done: {agent_result.total_steps} steps ({dt}ms)")

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
        logger.warning("[%s][cennik] Agent failed: %s (%dms)", audit_id, e, dt)
        await progress(65, f"Agent: FAILED ({dt}ms): {e}")
        # Non-fatal — fall through with empty mapping (no restructuring)

    # Apply category mapping to transformed pricelist. Annotate each service
    # with its original category name + was_recategorized flag so the finalize
    # step can propagate it into save_optimized_pricelist → optimized_services.
    restructured_categories = []
    for cat in transformed_pricelist["categories"]:
        original_cat_name = cat["name"]
        new_cat_name = category_mapping.get(original_cat_name, original_cat_name)
        was_recategorized = original_cat_name != new_cat_name

        annotated_services = []
        for svc in cat["services"]:
            svc_copy = dict(svc)
            svc_copy["_original_category"] = original_cat_name
            svc_copy["_was_recategorized"] = was_recategorized
            annotated_services.append(svc_copy)

        restructured_categories.append(
            {"name": new_cat_name, "services": annotated_services}
        )
    restructured_pricelist = {**transformed_pricelist, "categories": restructured_categories}

    # ── Step 4: Deterministic finalize ──
    await progress(75, "Finalizacja: czyszczenie nazw, detekcja promo i duplikatów...")

    final_changes: list[dict[str, Any]] = list(category_changes)
    names_improved = names_applied
    descriptions_added = descriptions_applied
    categories_optimized = len(category_changes)
    duplicates_found = 0
    final_categories: list[dict[str, Any]] = []
    seen_names: dict[str, str] = {}

    for cat in restructured_pricelist["categories"]:
        services_out: list[dict[str, Any]] = []
        for svc in cat["services"]:
            pre_sanitize_name = svc.get("name", "")
            name = clean_service_name(pre_sanitize_name)
            name = fix_caps_lock(name)
            name = sanitize_text(name)

            pre_sanitize_desc = svc.get("description")
            desc = pre_sanitize_desc
            if desc:
                desc = fix_caps_lock(desc)
                desc = sanitize_text(desc)

            # If sanitize_text cleaned emoji/decor from a name or description
            # that the agent didn't rewrite, promote the was_* flag so that
            # the frontend "Lista zmian" tab and the optimized_services row
            # both reflect the change. Also bump the stats counters so the
            # summary header shows an accurate count of names/descriptions
            # improved (not just those touched by the naming/description
            # agent — also those cleaned of promo markers in finalize).
            already_renamed = bool(svc.get("_was_renamed"))
            if not already_renamed and name != pre_sanitize_name:
                svc["_was_renamed"] = True
                names_improved += 1
            already_desc_changed = bool(svc.get("_was_description_changed"))
            if (
                not already_desc_changed
                and desc != pre_sanitize_desc
                and (pre_sanitize_desc or desc)
            ):
                svc["_was_description_changed"] = True
                descriptions_added += 1

            is_promo = _detect_promo(name)

            normalized = name.strip().lower()
            if normalized in seen_names and seen_names[normalized] != cat["name"]:
                duplicates_found += 1
            seen_names[normalized] = cat["name"]

            services_out.append({
                "name": name,
                "price": svc.get("price", ""),
                "duration": svc.get("duration"),
                "description": desc,
                "imageUrl": svc.get("imageUrl"),
                "variants": svc.get("variants"),
                "tags": svc.get("tags"),
                "isPromo": is_promo,

                # Preserve provenance + Booksy canonical taxonomy through the
                # deterministic finalize step — save_optimized_pricelist needs
                # these to link optimized_services back to salon_scrape_services.
                "scrape_service_id": svc.get("scrape_service_id"),
                "canonical_id": svc.get("canonical_id"),
                "booksy_treatment_id": svc.get("booksy_treatment_id"),
                "booksy_service_id": svc.get("booksy_service_id"),
                "treatment_name": svc.get("treatment_name"),
                "treatment_parent_id": svc.get("treatment_parent_id"),
                "body_part": svc.get("body_part"),
                "target_gender": svc.get("target_gender"),
                "technology": svc.get("technology"),
                "classification_confidence": svc.get("classification_confidence"),
                "price_grosze": svc.get("price_grosze"),
                "is_from_price": svc.get("is_from_price"),
                "duration_minutes": svc.get("duration_minutes"),

                # Change-tracking flags + original snapshot (for Lista zmian)
                "_original_name": svc.get("_original_name"),
                "_original_description": svc.get("_original_description"),
                "_original_price": svc.get("_original_price"),
                "_original_category": svc.get("_original_category"),
                "_was_renamed": svc.get("_was_renamed", False),
                "_was_description_changed": svc.get("_was_description_changed", False),
                "_was_recategorized": svc.get("_was_recategorized", False),
            })
        final_categories.append(
            {"name": sanitize_text(cat["name"]), "services": services_out}
        )

    final_pricelist = {
        "salonName": scraped_data.salonName,
        "salonAddress": scraped_data.salonAddress,
        "salonLogoUrl": scraped_data.salonLogoUrl,
        "totalServices": scraped_data.totalServices,
        "categories": final_categories,
    }

    # Generate full diff vs original by position
    orig_cats = original_pricelist["categories"]
    final_cats = final_pricelist["categories"]
    for cat_idx in range(min(len(orig_cats), len(final_cats))):
        orig_services = orig_cats[cat_idx].get("services", [])
        final_services = final_cats[cat_idx].get("services", [])
        for svc_idx in range(min(len(orig_services), len(final_services))):
            orig_svc = orig_services[svc_idx]
            final_svc = final_services[svc_idx]
            orig_name = orig_svc.get("name", "")
            final_name = final_svc.get("name", "")
            if orig_name != final_name:
                final_changes.append({
                    "type": "name",
                    "serviceName": orig_name,
                    "before": orig_name,
                    "after": final_name,
                    "reason": "Transformation from report",
                })
            orig_desc = orig_svc.get("description") or ""
            final_desc = final_svc.get("description") or ""
            if orig_desc != final_desc:
                final_changes.append({
                    "type": "description",
                    "serviceName": final_name,
                    "before": orig_desc,
                    "after": final_desc,
                    "reason": "Transformation from report",
                })

    stats = {
        "namesImproved": names_improved,
        "descriptionsAdded": descriptions_added,
        "categoriesOptimized": categories_optimized,
        "duplicatesFound": duplicates_found,
        "totalChanges": len(final_changes),
    }

    # Quality score computed from four coverage metrics, clamped 0-100.
    # Previously hardcoded to 100. Weights prioritize AI-applied
    # transformations (40%) since that is what the user paid for, then
    # classification taxonomy (20%), pricing completeness (20%), duration
    # completeness (20%) reflecting overall scrape data quality.
    total_services_flat = sum(len(c.get("services") or []) for c in final_categories)
    if total_services_flat > 0:
        services_flat = [
            svc
            for cat in final_categories
            for svc in (cat.get("services") or [])
        ]
        transformation_coverage = (
            (names_improved + descriptions_added) / max(total_services_flat, 1)
        )
        canonical_coverage = (
            sum(1 for s in services_flat if s.get("canonical_id")) / total_services_flat
        )
        price_coverage = (
            sum(1 for s in services_flat if s.get("price_grosze") and s.get("price_grosze") > 0)
            / total_services_flat
        )
        duration_coverage = (
            sum(1 for s in services_flat if s.get("duration_minutes") and s.get("duration_minutes") > 0)
            / total_services_flat
        )
        quality_score_float = (
            transformation_coverage * 40
            + canonical_coverage * 20
            + price_coverage * 20
            + duration_coverage * 20
        )
        quality_score = max(0, min(100, round(quality_score_float)))
        logger.info(
            "[%s][cennik] quality_score=%d (transform=%.2f canonical=%.2f price=%.2f duration=%.2f)",
            audit_id, quality_score,
            transformation_coverage, canonical_coverage, price_coverage, duration_coverage,
        )
    else:
        quality_score = 0

    # ── Step 5: Write to Supabase (optimized_pricelists + children) ──
    await progress(85, "Zapis zoptymalizowanego cennika do Supabase...")
    optimization_data = {
        "pricelist": final_pricelist,
        "changes": final_changes,
        "summary": stats,
        "qualityScore": quality_score,
    }
    await supabase.save_optimized_pricelist(
        convex_audit_id=audit_id,
        optimization_data=optimization_data,
        salon_name=scraped_data.salonName or "",
    )

    # ── Step 6: Build category_proposal payload for Convex webhook ──
    await progress(95, "Budowanie payloadu dla Convex categoryProposals...")
    category_proposal = {
        "auditId": audit_id,
        "originalStructureJson": json.dumps(
            {"categories": [{"name": c["name"]} for c in original_pricelist["categories"]]},
            ensure_ascii=False,
        ),
        "proposedStructureJson": json.dumps(
            {"categories": [{"name": c["name"]} for c in final_pricelist["categories"]]},
            ensure_ascii=False,
        ),
        "changes": [
            {
                "type": c["type"],
                "before": c.get("before", ""),
                "after": c.get("after", ""),
                "reason": c.get("reason", ""),
            }
            for c in category_changes
        ],
    }

    await progress(
        100,
        f"Cennik gotowy: {stats['namesImproved']} nazw, "
        f"{stats['descriptionsAdded']} opisów, "
        f"{stats['categoriesOptimized']} kategorii, "
        f"{stats['totalChanges']} zmian razem",
    )

    return {
        "pricelist": final_pricelist,
        "changes": final_changes,
        "category_proposal": category_proposal,
        "stats": stats,
    }
