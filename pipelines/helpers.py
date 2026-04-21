"""Pure helper functions for audit pipeline. No side effects, no AI calls."""

from __future__ import annotations

import re
from typing import Any


# Strips emoji / dingbats / pictographs from user-facing text so the raport
# finding on inconsistent promo markers ("✦", "💪", "🔴", "⭐", etc.) actually
# gets applied to the optimized cennik — not just reported. Applied
# deterministically in cennik.py finalize step to category names, service
# names, and descriptions.
#
# Covers:
#   - Misc Technical (U+2300-U+23FF): ⌚ ⌛ ⏰ ⏳ ⌨
#   - Misc Symbols & Dingbats (U+2600-U+27BF): ✦ ★ ☀ ☁ ☂ ✨ ❤ ❌ ✅ ✔
#   - Misc Symbols and Arrows (U+2B00-U+2BFF): ⭐ ⬆ ⬇ ⬅ ➡ ⬛ ⬜
#   - Supplementary pictographs & emoji (U+1F000-U+1FFFF): 💪 🔴 ⭕ 😀 🎉
#   - Zero-width joiner + variation selectors (used by composite emoji)
#
# Does NOT touch ASCII, Polish diacritics (all < U+2300), mathematical
# operators (U+2200-U+22FF: ± × ÷ ≤ ≥), or arrows used in text (U+2190-U+21FF).
_EMOJI_RE = re.compile(
    "["
    "\U00002300-\U000023FF"
    "\U00002600-\U000027BF"
    "\U00002B00-\U00002BFF"
    "\U0001F000-\U0001FFFF"
    "\u200d\ufe0f\ufe0e"
    "]+",
    flags=re.UNICODE,
)


def sanitize_text(text: str) -> str:
    """Strip emoji/dingbats/pictographs and collapse resulting whitespace.

    Preserves ASCII, Polish diacritics, digits, and standard punctuation.
    After stripping, collapses multiple spaces and trims stranded leading/
    trailing separators left behind by removed decor (e.g. `"✦ PROMOCJE ✦"`
    → `"PROMOCJE"`).
    """
    if not text:
        return text
    cleaned = _EMOJI_RE.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = cleaned.strip(" \t\n\r-–—·:")
    return cleaned


def clean_service_name(name: str) -> str:
    """Programmatic cleanup: trailing dots, spacing around +/-."""
    cleaned = name.strip()
    cleaned = re.sub(r"\.+$", "", cleaned).strip()
    cleaned = re.sub(r"(\w)\+\s*", r"\1 + ", cleaned)
    cleaned = re.sub(r"\s*\+(\w)", r" + \1", cleaned)
    cleaned = re.sub(r"(\w)-\s+", r"\1 - ", cleaned)
    return cleaned


def fix_caps_lock(text: str) -> str:
    """Fix ALL CAPS text to sentence case."""
    letters = re.sub(r"[^a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]", "", text)
    upper = re.sub(r"[^A-ZĄĆĘŁŃÓŚŹŻ]", "", letters)
    if len(letters) > 5 and len(upper) / len(letters) > 0.6:
        return text[0].upper() + text[1:].lower()
    return text


def calculate_similarity(a: str, b: str) -> float:
    """Jaccard similarity on words."""
    words_a = {w for w in a.split() if len(w) > 1}
    words_b = {w for w in b.split() if len(w) > 1}
    if not words_a and not words_b:
        return 1.0
    intersection = len(words_a & words_b)
    union = len(words_a | words_b)
    return intersection / union if union > 0 else 1.0


def validate_name_transformation(before: str, after: str) -> bool:
    """Validate that a name transformation is sensible.

    Two types: CLEANING (shorter result) and ENRICHING (longer, with benefit after ' - ').
    """
    before_len = len(before.strip())
    after_len = len(after.strip())

    # Rule 1: min 3 chars
    if after_len < 3:
        return False
    # Rule 2: max 80 chars
    if after_len > 80:
        return False
    # Rule 3: reject empty marketing garbage
    garbage_patterns = [
        r"100%\s*skuteczn",
        r"niezależnie\s+od",
        r"profesjonaln[yae]\s+(zabieg|usługa)",
        r"najlepsz[yae]",
        r"jedyn[yae]\s+w\s+swoim",
        r"wyjątkow[yae]",
    ]
    for pattern in garbage_patterns:
        if re.search(pattern, after, re.IGNORECASE):
            return False
    # Rule 4: enriched names need separator and preserve original
    if after_len > before_len + 5:
        if " - " not in after and ": " not in after:
            return False
        similarity = calculate_similarity(before.lower(), after.lower())
        if similarity < 0.4:
            original_start = before.lower().split("-")[0].split(":")[0].strip()
            if not after.lower().startswith(original_start):
                return False
    elif after_len < before_len:
        pass  # Cleaning -- OK
    else:
        # Same length -- must be meaningful
        if calculate_similarity(before.lower(), after.lower()) < 0.5:
            return False
    return True


def is_fixed_price(price: str) -> bool:
    """Check if price is fixed (not 'od...', ranges, etc.)."""
    lower = price.lower().strip()
    if lower.startswith("od ") or (lower.startswith("od") and len(lower) > 2 and lower[2].isdigit()):
        return False
    if "od " in lower or "from " in lower or " - " in lower or "\u2013" in lower:
        return False
    return True


def build_full_pricelist_text(data: Any) -> str:
    """Build full pricelist text for AI analysis prompts.

    data should have .salonName, .categories[].name, .categories[].services[]
    """
    text = f"SALON: {data.salonName or 'Nieznany'}\n\n"
    for cat in data.categories:
        text += f"\n## KATEGORIA: {cat.name} ({len(cat.services)} us\u0142ug)\n"
        for s in cat.services:
            text += f'- "{s.name}" | {s.price}'
            if s.duration:
                text += f" | {s.duration}"
            if s.description:
                desc = s.description[:150] + "..." if len(s.description) > 150 else s.description
                text += f" | OPIS: {desc}"
            text += "\n"
    return text


def build_pricelist_summary(data: Any) -> str:
    """Build condensed pricelist summary (max 5 services per category, max 10 categories)."""
    max_per_cat = 5
    max_cats = 10
    categories = data.categories[:max_cats]
    summary = f"SALON: {data.salonName or 'Nieznany'}\n"
    summary += f"ADRES: {data.salonAddress or 'Nieznany'}\n"
    summary += f"STATYSTYKI: {len(data.categories)} kategorii, {data.totalServices} us\u0142ug\n\nPRZYK\u0141ADOWE US\u0141UGI:\n"
    for cat in categories:
        summary += f"\n## {cat.name} ({len(cat.services)} us\u0142ug)\n"
        for s in cat.services[:max_per_cat]:
            summary += f"- {s.name}: {s.price}"
            if s.duration:
                summary += f" ({s.duration})"
            if s.description:
                summary += f" - {s.description[:50]}..."
            summary += "\n"
        if len(cat.services) > max_per_cat:
            summary += f"  ... i {len(cat.services) - max_per_cat} wi\u0119cej us\u0142ug\n"
    if len(data.categories) > max_cats:
        summary += f"\n... i {len(data.categories) - max_cats} wi\u0119cej kategorii\n"
    return summary


def calculate_completeness_score(stats: Any) -> int:
    """0-15 points based on field completeness."""
    total = _get_attr(stats, "totalServices", 0)
    desc_rate = _get_attr(stats, "servicesWithDescription", 0) / total if total > 0 else 0
    dur_rate = _get_attr(stats, "servicesWithDuration", 0) / total if total > 0 else 0
    fixed_rate = _get_attr(stats, "servicesWithFixedPrice", 0) / total if total > 0 else 0
    score = round(desc_rate * 6 + dur_rate * 5 + fixed_rate * 4)
    return min(15, score)


def calculate_seo_score(missing_keywords: list[Any], stats: Any) -> int:
    """0-10 points. Fewer missing keywords = better."""
    high = sum(1 for k in missing_keywords if _get_attr(k, "searchVolume", "") == "high")
    medium = sum(1 for k in missing_keywords if _get_attr(k, "searchVolume", "") == "medium")
    low = sum(1 for k in missing_keywords if _get_attr(k, "searchVolume", "") == "low")
    score = 10 - high * 2 - medium * 1 - low * 0.5
    if missing_keywords and score > 9:
        score = 9
    return max(0, min(10, round(score)))


def calculate_ux_score(stats: Any) -> int:
    """0-5 points based on UX factors."""
    score = 5
    if _get_attr(stats, "emptyCategories", []):
        score -= 1
    if _get_attr(stats, "oversizedCategories", []):
        score -= 1
    if len(_get_attr(stats, "undersizedCategories", [])) > 2:
        score -= 1
    if _get_attr(stats, "duplicateNames", []):
        score -= 1
    largest = _get_attr(stats, "largestCategory", {})
    smallest = _get_attr(stats, "smallestCategory", {})
    largest_count = largest.get("count", 0) if isinstance(largest, dict) else getattr(largest, "count", 0)
    smallest_count = smallest.get("count", 1) if isinstance(smallest, dict) else getattr(smallest, "count", 1)
    if smallest_count > 0 and largest_count > smallest_count * 10:
        score -= 1
    return max(0, score)


def calculate_audit_stats(data: Any) -> dict[str, Any]:
    """Calculate comprehensive audit statistics (deterministic, no AI).

    Returns a dict matching AuditStats model fields.
    """
    all_services = [s for cat in data.categories for s in cat.services]
    total_services = len(all_services)
    total_categories = len(data.categories)

    total_variants = 0
    services_with_variants = 0
    variants_with_duration = 0
    for svc in all_services:
        variants = getattr(svc, "variants", None) or []
        if len(variants) > 0:
            services_with_variants += 1
            total_variants += len(variants)
            for v in variants:
                dur = getattr(v, "duration", None) or ""
                if dur.strip():
                    variants_with_duration += 1

    services_without_variants = total_services - services_with_variants
    total_price_points = services_without_variants + total_variants

    services_with_description = sum(
        1 for s in all_services if (getattr(s, "description", None) or "").strip()
    )
    services_with_duration = sum(
        1 for s in all_services if (getattr(s, "duration", None) or "").strip()
    )
    services_with_fixed_price = sum(
        1 for s in all_services if getattr(s, "price", None) and is_fixed_price(s.price)
    )
    services_with_image = sum(
        1 for s in all_services if (getattr(s, "imageUrl", None) or "").strip()
    )

    # Category sizes (counting price points including variants)
    category_sizes: list[dict[str, Any]] = []
    for cat in data.categories:
        count = 0
        for svc in cat.services:
            variants = getattr(svc, "variants", None) or []
            if len(variants) > 0:
                count += len(variants)
            else:
                count += 1
        category_sizes.append({"name": cat.name, "count": count})

    total_for_avg = sum(c["count"] for c in category_sizes)
    avg_per_cat = round(total_for_avg / total_categories * 10) / 10 if total_categories > 0 else 0

    sorted_sizes = sorted(category_sizes, key=lambda c: c["count"], reverse=True)
    largest = sorted_sizes[0] if sorted_sizes else {"name": "Brak", "count": 0}
    smallest = sorted_sizes[-1] if sorted_sizes else {"name": "Brak", "count": 0}

    # Duplicate names
    name_counts: dict[str, int] = {}
    for svc in all_services:
        variants = getattr(svc, "variants", None) or []
        if len(variants) > 0:
            for v in variants:
                full_name = f"{svc.name} - {v.label}".lower().strip()
                name_counts[full_name] = name_counts.get(full_name, 0) + 1
        else:
            normalized = svc.name.lower().strip()
            name_counts[normalized] = name_counts.get(normalized, 0) + 1
    duplicates = [name for name, count in name_counts.items() if count > 1]

    empty_cats = [c.name for c in data.categories if len(c.services) == 0]
    oversized = [c["name"] for c in category_sizes if c["count"] > 20]
    undersized = [c["name"] for c in category_sizes if 0 < c["count"] < 3]

    return {
        "totalServices": total_services,
        "totalVariants": total_variants,
        "totalPricePoints": total_price_points,
        "servicesWithVariants": services_with_variants,
        "totalCategories": total_categories,
        "servicesWithDescription": services_with_description,
        "servicesWithDuration": services_with_duration,
        "servicesWithFixedPrice": services_with_fixed_price,
        "servicesWithImage": services_with_image,
        "variantsWithDuration": variants_with_duration,
        "avgServicesPerCategory": avg_per_cat,
        "largestCategory": largest,
        "smallestCategory": smallest,
        "duplicateNames": duplicates,
        "emptyCategories": empty_cats,
        "oversizedCategories": oversized,
        "undersizedCategories": undersized,
    }


def _get_attr(obj: Any, key: str, default: Any = None) -> Any:
    """Get attribute from object or dict."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


# ---------------------------------------------------------------------------
# Score capping
# ---------------------------------------------------------------------------

# Max score a base Booksy audit can produce. The remaining ~12 points are
# reserved for marketing spend, paid campaigns, and full competitor
# positioning — pillars measured by separate paid products (Kampanie,
# Raport Konkurencji). Keeping the audit below 100/100 prevents misleading
# users into thinking a clean Booksy profile equals a fully optimized
# business.
MAX_AUDIT_SCORE = 88


def cap_audit_score(
    total_score: int,
    critical_count: int,
    major_count: int,
    all_issues_count: int,
) -> int:
    """Apply issue-based caps plus the MAX_AUDIT_SCORE hard ceiling.

    Rules:
      - 3+ critical issues → cap 60
      - 1+ critical issue  → cap 75
      - 1+ major issue     → cap 85
      - any issue          → cap 95
      - hard ceiling       → MAX_AUDIT_SCORE (88) for all audits
    """
    capped = total_score
    if critical_count >= 3 and capped > 60:
        capped = 60
    elif critical_count > 0 and capped > 75:
        capped = 75
    elif major_count > 0 and capped > 85:
        capped = 85
    elif all_issues_count > 0 and capped > 95:
        capped = 95

    if capped > MAX_AUDIT_SCORE:
        capped = MAX_AUDIT_SCORE

    return capped
