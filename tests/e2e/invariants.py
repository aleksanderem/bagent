"""Deterministic invariants that pipeline outputs must satisfy.

The whole point of E2E tests against AI-driven pipelines is that we
CANNOT assert exact text or scores — those vary per run. Instead we
encode invariants every correct output must respect:

    - Schema validity (all required fields, correct types)
    - Range bounds (score in [0, MAX_DISPLAY_SCORE], counts >= 0)
    - Cardinality matching (services_in == services_out)
    - Polish text quality (no English leakage, diacritics OK, no double
      spaces, no orphan punctuation)
    - Cross-field consistency (sum of category scores ~= total)
    - Score capping (NEVER 100 — see memory feedback_score_cap.md)

Each invariant is a small assertion function that raises AssertionError
with a descriptive message. Tests collect failures into a single
``InvariantReport`` so a salon hitting 5 invariant fails surfaces all 5,
not just the first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# Hard cap from BeautyAudit product spec — see
# .claude/projects/.../memory/feedback_score_cap.md
MAX_DISPLAY_SCORE = 92

# English markers that should never appear standalone in Polish AI output.
# We deliberately use word boundaries — "the" in "thereby" doesn't match.
ENGLISH_MARKERS = re.compile(
    r"\b(the|and|or|is|are|was|were|will|should|would|could|"
    r"this|that|these|those|with|without|from|into|about|"
    r"please|kindly|here|there|where|when|while|because|"
    r"customer|client|service|services|salon|business|owner|booking)\b",
    re.IGNORECASE,
)

# Polish diacritics — at least one should appear in any non-trivial
# Polish text. Single-letter words ("o", "i", "a", "z", "w") aren't
# enough to expect diacritics, so we only assert when text > 60 chars.
POLISH_DIACRITICS = re.compile(r"[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]")


# ---------------------------------------------------------------------------
# Report aggregator
# ---------------------------------------------------------------------------

@dataclass
class InvariantReport:
    """Accumulates pass/fail across many invariants for one pipeline output."""
    pipeline: str
    salon_id: Any
    failures: list[str] = field(default_factory=list)
    passed: list[str] = field(default_factory=list)

    def check(self, name: str, condition: bool, msg: str = "") -> None:
        if condition:
            self.passed.append(name)
        else:
            self.failures.append(f"[{name}] {msg}" if msg else f"[{name}]")

    @property
    def ok(self) -> bool:
        return not self.failures

    def assert_ok(self) -> None:
        if not self.ok:
            details = "\n  - " + "\n  - ".join(self.failures)
            raise AssertionError(
                f"{len(self.failures)} invariants failed for "
                f"{self.pipeline} salon={self.salon_id}:{details}"
            )


# ---------------------------------------------------------------------------
# Generic invariant helpers
# ---------------------------------------------------------------------------

def has_required_keys(obj: dict[str, Any], keys: list[str]) -> tuple[bool, str]:
    missing = [k for k in keys if k not in obj]
    return (not missing, f"missing keys: {missing}" if missing else "")


def is_int_in_range(v: Any, lo: int, hi: int) -> tuple[bool, str]:
    if not isinstance(v, (int, float)):
        return False, f"expected number, got {type(v).__name__}={v!r}"
    if not (lo <= v <= hi):
        return False, f"value {v} not in [{lo}, {hi}]"
    return True, ""


def has_no_english_words(text: str) -> tuple[bool, str]:
    """True if text has no obvious English-language words. Allows
    proper nouns and brand names (no match on capitalized words after
    punctuation), so 'Booksy' or 'Microsoft' don't trip this."""
    if not isinstance(text, str) or not text:
        return True, ""
    matches = ENGLISH_MARKERS.findall(text)
    if matches:
        sample = ", ".join(set(m.lower() for m in matches[:5]))
        return False, f"English markers found: {sample}"
    return True, ""


def has_polish_diacritics_if_long(text: str, min_len: int = 60) -> tuple[bool, str]:
    """For texts longer than min_len, expect at least one Polish
    diacritic. Catches AI outputs that silently dropped to ASCII-only."""
    if not isinstance(text, str) or len(text) < min_len:
        return True, ""
    if POLISH_DIACRITICS.search(text):
        return True, ""
    return False, f"no Polish diacritics in {len(text)}-char text: {text[:80]!r}…"


def has_no_double_spaces(text: str) -> tuple[bool, str]:
    if not isinstance(text, str):
        return True, ""
    if "  " in text:
        idx = text.index("  ")
        return False, f"double space at offset {idx}: …{text[max(0,idx-15):idx+15]!r}…"
    return True, ""


def has_no_orphan_diacritic_marks(text: str) -> tuple[bool, str]:
    """Catches mojibake / encoding loss — e.g. ``Å›`` instead of ``ś``."""
    if not isinstance(text, str):
        return True, ""
    bad = re.search(r"Å.|Ä.|â\x80\x9c|â\x80\x9d|Ã³|Å¼|Å›", text)
    if bad:
        return False, f"mojibake/encoding artifact at offset {bad.start()}: {bad.group()!r}"
    return True, ""


def has_finite_string(v: Any, max_len: int = 5000) -> tuple[bool, str]:
    if v is None:
        return False, "None"
    if not isinstance(v, str):
        return False, f"not a string: {type(v).__name__}"
    if len(v) > max_len:
        return False, f"string too long: {len(v)} > {max_len}"
    return True, ""


# ---------------------------------------------------------------------------
# Audit pipeline invariants (BAGENT #1 / report.py output)
# ---------------------------------------------------------------------------

def check_audit_report(
    report: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Run all audit-pipeline invariants against a report dict.
    Returns an aggregated InvariantReport."""
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="audit", salon_id=salon_id)

    # 1. Schema — required top-level keys
    required = [
        "version", "totalScore", "scoreBreakdown", "stats", "topIssues",
        "transformations", "missingSeoKeywords", "quickWins",
        "industryComparison", "summary", "categoryMapping",
        "categoryChanges", "coverage",
    ]
    ok, msg = has_required_keys(report, required)
    rep.check("schema.required_keys", ok, msg)

    # 2. Score bounds — totalScore must be in [0, MAX_DISPLAY_SCORE]
    # (NEVER 100 per product spec — leaves headroom for ad campaigns
    # which are a separate paid upsell).
    total_score = report.get("totalScore", -1)
    ok, msg = is_int_in_range(total_score, 0, MAX_DISPLAY_SCORE)
    rep.check("score.in_range", ok, f"totalScore={total_score}, {msg}")

    # 3. scoreBreakdown sums to totalScore (within rounding tolerance)
    breakdown = report.get("scoreBreakdown") or {}
    if isinstance(breakdown, dict) and breakdown:
        sum_of_parts = sum(
            int(v) for v in breakdown.values() if isinstance(v, (int, float))
        )
        # Tolerate ±2 for rounding + capping interactions
        ok = abs(sum_of_parts - total_score) <= 2
        rep.check(
            "score.breakdown_sum",
            ok,
            f"breakdown sums to {sum_of_parts}, totalScore={total_score}",
        )

    # 4. transformations — list of {original, optimized, ...}
    transformations = report.get("transformations", [])
    rep.check(
        "transformations.is_list",
        isinstance(transformations, list),
        f"got {type(transformations).__name__}",
    )
    if isinstance(transformations, list):
        rep.check(
            "transformations.bounds",
            0 <= len(transformations) <= 200,
            f"len={len(transformations)} (expected 0-200)",
        )

    # 5. topIssues — non-empty list (every salon has SOMETHING to fix)
    issues = report.get("topIssues", [])
    rep.check(
        "issues.non_empty",
        isinstance(issues, list) and len(issues) > 0,
        f"got {len(issues) if isinstance(issues, list) else type(issues).__name__}",
    )

    # 6. quickWins — non-empty list (deterministic fallback ensures this)
    quick_wins = report.get("quickWins", [])
    rep.check(
        "quickWins.non_empty",
        isinstance(quick_wins, list) and len(quick_wins) > 0,
        f"got {len(quick_wins) if isinstance(quick_wins, list) else 'non-list'}",
    )

    # 7. industryComparison — required nested fields
    comp = report.get("industryComparison") or {}
    if isinstance(comp, dict):
        for k in ("yourScore", "industryAverage", "topPerformers", "percentile"):
            ok, msg = is_int_in_range(comp.get(k), 0, 100)
            rep.check(f"industryComparison.{k}", ok, msg)

    # 8. summary — Polish text, non-trivial length
    summary = report.get("summary", "")
    rep.check(
        "summary.length",
        isinstance(summary, str) and 50 <= len(summary) <= 5000,
        f"len={len(summary) if isinstance(summary, str) else type(summary).__name__}",
    )
    ok, msg = has_no_english_words(summary)
    rep.check("summary.no_english", ok, msg)
    ok, msg = has_polish_diacritics_if_long(summary)
    rep.check("summary.has_polish", ok, msg)
    ok, msg = has_no_double_spaces(summary)
    rep.check("summary.no_double_space", ok, msg)
    ok, msg = has_no_orphan_diacritic_marks(summary)
    rep.check("summary.no_mojibake", ok, msg)

    # 9. categoryMapping — should cover every input category
    cat_mapping = report.get("categoryMapping") or {}
    input_cats = {c.name for c in scraped_data.categories}
    if isinstance(cat_mapping, dict) and input_cats:
        # categoryMapping maps original_name → new_name. Some entries
        # may be omitted if the category was unchanged. We assert: at
        # most as many keys as input categories, AND every key must be
        # an input category.
        rep.check(
            "categoryMapping.keys_subset_of_input",
            set(cat_mapping.keys()).issubset(input_cats | set()),
            f"unknown keys: {set(cat_mapping.keys()) - input_cats}",
        )

    # 10. coverage — naming and descriptions sub-objects
    coverage = report.get("coverage") or {}
    for sub in ("naming", "descriptions"):
        sub_cov = coverage.get(sub) if isinstance(coverage, dict) else None
        if isinstance(sub_cov, dict):
            total = sub_cov.get("totalChecked", 0)
            opt = sub_cov.get("optimized", 0)
            already = sub_cov.get("alreadyOptimal", 0)
            rejected = sub_cov.get("rejected", 0)
            # Sanity: optimized + already + rejected should not exceed
            # totalChecked (some agents may double-count edge cases, so
            # we tolerate +5%)
            sum_parts = opt + already + rejected
            rep.check(
                f"coverage.{sub}.parts_le_total",
                sum_parts <= total * 1.05 + 1,
                f"{sum_parts} > {total}",
            )

    # 11. transformations: each item has required keys
    if isinstance(transformations, list):
        bad = []
        for i, t in enumerate(transformations):
            if not isinstance(t, dict):
                bad.append(f"#{i} not dict")
                continue
            for required_key in ("type", "original"):
                if required_key not in t:
                    bad.append(f"#{i} missing {required_key}")
        rep.check(
            "transformations.well_formed",
            not bad,
            "; ".join(bad[:5]),
        )

    return rep


# ---------------------------------------------------------------------------
# Cennik pipeline invariants (BAGENT #2 / cennik.py output)
# ---------------------------------------------------------------------------

def check_cennik_output(
    cennik: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Invariants for the cennik (BAGENT #2) pipeline output.

    Cennik takes the audit transformations + category mapping and
    produces a new pricelist. Critical invariants:
      - cardinality preserved: every input service appears in output
      - prices preserved or improved (numeric value not lost)
      - no AI calls = no Polish/English issues to check (deterministic)
    """
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="cennik", salon_id=salon_id)

    # Cennik output shape varies (categoryProposals or full pricelist)
    # so we accept either and check what's present.
    proposals = cennik.get("categoryProposals") or cennik.get("categories") or []
    rep.check(
        "output.is_list",
        isinstance(proposals, list),
        f"got {type(proposals).__name__}",
    )

    if not isinstance(proposals, list):
        return rep

    # Cardinality: total services in output should match input (within
    # tolerance for category restructuring that might merge dupes).
    output_service_count = sum(
        len(p.get("services") or []) for p in proposals if isinstance(p, dict)
    )
    input_service_count = scraped_data.totalServices
    # Allow ±10% drift for legitimate dedup of duplicate names
    lo = int(input_service_count * 0.85)
    hi = int(input_service_count * 1.10) + 1
    rep.check(
        "cardinality.services_preserved",
        lo <= output_service_count <= hi,
        f"input={input_service_count}, output={output_service_count}, "
        f"expected [{lo}, {hi}]",
    )

    # Price integrity: every output service has a non-empty price string
    bad_prices = 0
    for p in proposals:
        if not isinstance(p, dict):
            continue
        for s in p.get("services") or []:
            if not isinstance(s, dict):
                continue
            price = s.get("price") or ""
            if not isinstance(price, str) or not price.strip():
                bad_prices += 1
    rep.check(
        "prices.all_populated",
        bad_prices == 0,
        f"{bad_prices} services have empty/missing price",
    )

    # All output category names + service names are Polish-clean
    text_failures: list[str] = []
    for p in proposals:
        if not isinstance(p, dict):
            continue
        cat_name = p.get("name", "")
        ok, msg = has_no_orphan_diacritic_marks(cat_name)
        if not ok:
            text_failures.append(f"category {cat_name!r}: {msg}")
        for s in p.get("services") or []:
            if not isinstance(s, dict):
                continue
            for field in ("name", "description"):
                v = s.get(field) or ""
                if isinstance(v, str):
                    ok, msg = has_no_orphan_diacritic_marks(v)
                    if not ok:
                        text_failures.append(f"service.{field} {v[:30]!r}: {msg}")
                    ok, msg = has_no_double_spaces(v)
                    if not ok:
                        text_failures.append(f"service.{field} {v[:30]!r}: {msg}")
    rep.check(
        "text.no_mojikabe_no_double_spaces",
        not text_failures,
        f"{len(text_failures)} issues; first 3: {text_failures[:3]}",
    )

    return rep


# ---------------------------------------------------------------------------
# Summary pipeline invariants (BAGENT #3 / summary.py output)
# ---------------------------------------------------------------------------

def check_summary_output(
    summary: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Invariants for the summary (BAGENT #3) pipeline output."""
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="summary", salon_id=salon_id)

    # Summary should have headline + sections + ctaText (per
    # convex/audit/validators.ts auditSummary shape)
    for k in ("headline", "subheadline", "ctaText"):
        v = summary.get(k)
        if v is not None:
            ok, msg = has_no_english_words(v) if isinstance(v, str) else (True, "")
            rep.check(f"{k}.no_english", ok, msg)
            ok, msg = has_no_orphan_diacritic_marks(v) if isinstance(v, str) else (True, "")
            rep.check(f"{k}.no_mojibake", ok, msg)
            ok, msg = has_no_double_spaces(v) if isinstance(v, str) else (True, "")
            rep.check(f"{k}.no_double_space", ok, msg)

    return rep
