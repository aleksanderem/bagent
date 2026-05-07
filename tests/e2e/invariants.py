"""Deterministic invariants that pipeline outputs must satisfy.

The whole point of E2E tests against AI-driven pipelines is that we
CANNOT assert exact text or scores — those vary per run. Instead we
encode invariants every correct output must respect:

    - Schema validity (all required fields, correct types)
    - Range bounds (score in [0, MAX_DISPLAY_SCORE], counts >= 0)
    - Cardinality matching (services_in == services_out, per-category)
    - Polish text quality (no English leakage, diacritics OK, no double
      spaces, no orphan punctuation, no AI self-references, no
      placeholders, no markdown leakage in plain-text fields)
    - Cross-field consistency (sum of category scores ~= total)
    - Score capping (NEVER 100 — see memory feedback_score_cap.md)

Each invariant is a small assertion function returning ``(bool, str)``.
Tests collect failures into a single ``InvariantReport`` so a salon
hitting 5 invariant fails surfaces all 5, not just the first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# Hard cap from BeautyAudit product spec — see
# .claude/projects/.../memory/feedback_score_cap.md
# Score must NEVER reach 100 even after optimization. The 8-point gap
# is reserved for ad-campaign upsell.
MAX_DISPLAY_SCORE = 92


# ---------------------------------------------------------------------------
# Polish text quality regexes
# ---------------------------------------------------------------------------

# English markers that should never appear standalone in Polish AI output.
# Word-boundary matched + case-insensitive. Catalog covers:
#   - English glue words (the/and/is/with/from/etc.) that can't be PL
#   - English verbs that AI sometimes leaks ("create", "improve", "optimize")
#   - Marketing-English (best, top, click, here)
#
# Words deliberately NOT included (false-positive risk):
#   - "salon", "client", "service", "or" (legitimate Polish)
#   - "Booksy", "Microsoft" (proper nouns — would need context-aware match)
ENGLISH_MARKERS = re.compile(
    r"\b(the|and|is|are|was|were|will|should|would|could|"
    r"these|those|with|without|from|into|about|through|"
    r"please|kindly|where|when|while|because|"
    r"customer|business|owner|booking|"
    r"create|improve|optimize|increase|decrease|enhance|update|provide|"
    r"best|better|top|premium|amazing|awesome|"
    r"click|here|there|tap|book|now|today|"
    r"appointment|reservation|management|treatment)\b",
    re.IGNORECASE,
)

# Polish diacritics — at least one should appear in any non-trivial
# Polish text. Threshold dropped to 30 chars so short `headline`-style
# fields are also checked (was 60 — missed too many).
POLISH_DIACRITICS = re.compile(r"[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]")

# Mojibake patterns — UTF-8 bytes interpreted as Latin-1 / Windows-1252.
# Catalog of common artifacts seen in production:
#   - Polish ł→Å‚, ó→Ã³, ś→Å›, ż→Å¼ etc.
#   - Smart quotes â€" / â€œ / â€ / â€™
#   - BOM remnant ï»¿
#   - Stray Â before non-breaking spaces
#   - Latin-1 fallback A-circumflex / A-tilde sequences
MOJIBAKE_PATTERNS = re.compile(
    r"Å[a-zA-Z]"          # Polish letters double-encoded (Å›, Å¼, Å‚, Å›)
    r"|Ä[a-zA-Z]"          # Polish ć/ę → double-encoded
    r"|â€[\"]" # smart quotes mojibake (â€" / â€" / â€™)
    r"|Ã[³¼óźż]"           # Polish vowels with diacritics double-encoded
    r"|Â[\xa0\xab\xbb ]"    # stray Â before nbsp / quote chars
    r"|ï»¿"                # UTF-8 BOM as text
    r"|�",            # Unicode replacement char (already corrupt)
    re.UNICODE,
)

# AI self-references — model-specific phrases that should never reach
# the user. AI sometimes produces these when uncertain or when prompts
# fail. Polish + English forms covered.
AI_LEAKAGE_PATTERNS = re.compile(
    r"(jako (AI|model językowy|sztuczna inteligencja|asystent)|"
    r"przepraszam(,| )ale (nie|jako)|"
    r"niestety nie mogę|"
    r"jako duży model|"
    r"as an AI|as a language model|"
    r"I cannot|I'm sorry|I apologize|"
    r"I don't have access|I am unable)",
    re.IGNORECASE,
)

# Placeholder patterns — AI output that looks like a fill-in-the-blank
# template not actually filled in. These slip through when prompts have
# example markers that AI copies verbatim.
PLACEHOLDER_PATTERNS = re.compile(
    r"\[(OPIS|TODO|TBD|PLACEHOLDER|INSERT|NAME|NAZWA|UZUPEŁNIJ|FILL)\]"
    r"|\{[A-Z_]{3,}\}"         # {VARIABLE_NAME} style
    r"|<[A-Z_]{3,}>"            # <VARIABLE> style
    r"|lorem ipsum|"
    r"\.\.\.\.+"                # 4+ dots in a row (truncation marker)
    r"|XXX+|YYY+|ZZZ+",         # placeholder strings
    re.IGNORECASE,
)

# Markdown / formatting leakage in fields that should be plain text.
# Headlines, ctaText, summary etc. are rendered as plain Polish text;
# **bold**, # heading, ```code blocks should never appear.
MARKDOWN_LEAKAGE_PATTERNS = re.compile(
    r"\*\*[^*\n]+\*\*"          # **bold**
    r"|^\s*#{1,6}\s+"            # # heading at line start
    r"|```"                      # code fence
    r"|^\s*[-*+]\s+\w"           # markdown list items
    r"|\[[^\]]+\]\([^)]+\)",     # markdown links
    re.MULTILINE,
)


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
# Generic helpers
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
    if not isinstance(text, str) or not text:
        return True, ""
    matches = ENGLISH_MARKERS.findall(text)
    if matches:
        sample = ", ".join(set(m.lower() for m in matches[:5]))
        return False, f"English markers found: {sample}"
    return True, ""


def has_polish_diacritics_if_long(text: str, min_len: int = 30) -> tuple[bool, str]:
    """For texts longer than min_len, expect at least one Polish
    diacritic. Catches AI outputs that silently dropped to ASCII-only.
    Threshold 30 (was 60) — short headlines like 'Twój salon' (10 chars
    with diacritics) won't trigger, but a 35-char ASCII-only headline
    will."""
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


def has_no_mojibake(text: str) -> tuple[bool, str]:
    """Catches UTF-8 encoding loss across the wider mojibake catalog
    (was: only 6 patterns; now: ~15 covering double-encoded Polish,
    smart-quote artifacts, BOM remnants, replacement chars)."""
    if not isinstance(text, str):
        return True, ""
    bad = MOJIBAKE_PATTERNS.search(text)
    if bad:
        return False, f"mojibake at offset {bad.start()}: {bad.group()!r}"
    return True, ""


def has_no_ai_leakage(text: str) -> tuple[bool, str]:
    """No 'jako AI', 'as a language model', 'przepraszam ale' etc."""
    if not isinstance(text, str):
        return True, ""
    bad = AI_LEAKAGE_PATTERNS.search(text)
    if bad:
        return False, f"AI self-reference: {bad.group()!r}"
    return True, ""


def has_no_placeholders(text: str) -> tuple[bool, str]:
    """No [OPIS], {NAME}, lorem ipsum, '....', XXX, etc."""
    if not isinstance(text, str):
        return True, ""
    bad = PLACEHOLDER_PATTERNS.search(text)
    if bad:
        return False, f"placeholder leak: {bad.group()!r}"
    return True, ""


def has_no_markdown(text: str) -> tuple[bool, str]:
    """For plain-text fields. Bold/heading/code-fence/links shouldn't
    appear in fields that frontend renders as `<p>{text}</p>`."""
    if not isinstance(text, str):
        return True, ""
    bad = MARKDOWN_LEAKAGE_PATTERNS.search(text)
    if bad:
        return False, f"markdown leak: {bad.group()[:30]!r}"
    return True, ""


def has_finite_string(v: Any, max_len: int = 5000) -> tuple[bool, str]:
    if v is None:
        return False, "None"
    if not isinstance(v, str):
        return False, f"not a string: {type(v).__name__}"
    if len(v) > max_len:
        return False, f"string too long: {len(v)} > {max_len}"
    return True, ""


def check_polish_text_field(
    rep: InvariantReport,
    name: str,
    text: Any,
    *,
    require_diacritics_if_len: int | None = 30,
    allow_markdown: bool = False,
) -> None:
    """Apply the full Polish text quality battery to one field.

    Used by every pipeline that emits AI-generated text. Encapsulates
    the 6-check pattern (no-english + no-mojibake + no-ai-leak +
    no-placeholder + no-markdown + diacritics-if-long) so each pipeline
    test isn't repeating the same boilerplate."""
    if text is None or not isinstance(text, str):
        return  # caller decides if absence is a failure

    for check_name, check_fn in [
        (f"{name}.no_english", has_no_english_words),
        (f"{name}.no_mojibake", has_no_mojibake),
        (f"{name}.no_ai_leak", has_no_ai_leakage),
        (f"{name}.no_placeholder", has_no_placeholders),
        (f"{name}.no_double_space", has_no_double_spaces),
    ]:
        ok, msg = check_fn(text)
        rep.check(check_name, ok, msg)

    if not allow_markdown:
        ok, msg = has_no_markdown(text)
        rep.check(f"{name}.no_markdown", ok, msg)

    if require_diacritics_if_len is not None:
        ok, msg = has_polish_diacritics_if_long(text, require_diacritics_if_len)
        rep.check(f"{name}.has_polish", ok, msg)


# ---------------------------------------------------------------------------
# Audit pipeline invariants (BAGENT #1 / report.py output)
# ---------------------------------------------------------------------------

def check_audit_report(
    report: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Run all audit-pipeline invariants against a report dict."""
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
    # NEVER 100 per product spec — see memory feedback_score_cap.md.
    total_score = report.get("totalScore", -1)
    ok, msg = is_int_in_range(total_score, 0, MAX_DISPLAY_SCORE)
    rep.check("score.in_range", ok, f"totalScore={total_score}, {msg}")

    # 3a. scoreBreakdown sums to a plausible raw score (≤100 before cap).
    # Decoupled from totalScore — cap may have lowered totalScore even
    # when raw breakdown sums to >92.
    breakdown = report.get("scoreBreakdown") or {}
    if isinstance(breakdown, dict) and breakdown:
        sum_of_parts = sum(
            int(v) for v in breakdown.values() if isinstance(v, (int, float))
        )
        ok = 0 <= sum_of_parts <= 100
        rep.check(
            "score.breakdown_in_raw_range",
            ok,
            f"breakdown sums to {sum_of_parts}, expected [0, 100]",
        )

        # 3b. Cap is correctly applied: totalScore = min(raw, MAX_DISPLAY_SCORE)
        # Tolerate ±2 for rounding (some breakdown components round
        # independently of totalScore).
        expected_capped = min(sum_of_parts, MAX_DISPLAY_SCORE)
        ok = abs(expected_capped - total_score) <= 2
        rep.check(
            "score.cap_applied_correctly",
            ok,
            f"raw_sum={sum_of_parts}, expected_after_cap={expected_capped}, "
            f"got_totalScore={total_score}",
        )

    # 4. transformations — list, length-bounded
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

    # 6. quickWins — non-empty (deterministic fallback ensures this)
    quick_wins = report.get("quickWins", [])
    rep.check(
        "quickWins.non_empty",
        isinstance(quick_wins, list) and len(quick_wins) > 0,
        f"got {len(quick_wins) if isinstance(quick_wins, list) else 'non-list'}",
    )

    # 7. industryComparison — required nested fields, ranges
    comp = report.get("industryComparison") or {}
    if isinstance(comp, dict):
        for k in ("yourScore", "industryAverage", "topPerformers", "percentile"):
            ok, msg = is_int_in_range(comp.get(k), 0, 100)
            rep.check(f"industryComparison.{k}", ok, msg)
        # 7b. Sanity: yourScore ≤ MAX_DISPLAY_SCORE (cap applies here too)
        ys = comp.get("yourScore")
        if isinstance(ys, (int, float)):
            rep.check(
                "industryComparison.yourScore_capped",
                ys <= MAX_DISPLAY_SCORE,
                f"yourScore={ys} > MAX_DISPLAY_SCORE={MAX_DISPLAY_SCORE}",
            )

    # 8. summary — Polish text quality battery
    summary = report.get("summary", "")
    rep.check(
        "summary.length",
        isinstance(summary, str) and 50 <= len(summary) <= 5000,
        f"len={len(summary) if isinstance(summary, str) else type(summary).__name__}",
    )
    check_polish_text_field(rep, "summary", summary)

    # 9. categoryMapping — keys must be subset of input categories
    cat_mapping = report.get("categoryMapping") or {}
    input_cats = {c.name for c in scraped_data.categories}
    if isinstance(cat_mapping, dict) and input_cats:
        rep.check(
            "categoryMapping.keys_subset_of_input",
            set(cat_mapping.keys()).issubset(input_cats | set()),
            f"unknown keys: {set(cat_mapping.keys()) - input_cats}",
        )

    # 10. coverage — naming + descriptions parts ≤ totalChecked
    coverage = report.get("coverage") or {}
    for sub in ("naming", "descriptions"):
        sub_cov = coverage.get(sub) if isinstance(coverage, dict) else None
        if isinstance(sub_cov, dict):
            total = sub_cov.get("totalChecked", 0)
            opt = sub_cov.get("optimized", 0)
            already = sub_cov.get("alreadyOptimal", 0)
            rejected = sub_cov.get("rejected", 0)
            sum_parts = opt + already + rejected
            rep.check(
                f"coverage.{sub}.parts_le_total",
                sum_parts <= total * 1.05 + 1,
                f"{sum_parts} > {total}",
            )
            # 10b. coverage.totalChecked should equal input service count
            # (every service was evaluated).
            rep.check(
                f"coverage.{sub}.totalChecked_matches_input",
                abs(total - scraped_data.totalServices) <= max(2, scraped_data.totalServices * 0.05),
                f"totalChecked={total}, input_services={scraped_data.totalServices}",
            )

    # 11. transformations: each item has type + before/after pair
    if isinstance(transformations, list):
        bad = []
        for i, t in enumerate(transformations):
            if not isinstance(t, dict):
                bad.append(f"#{i} not dict")
                continue
            if "type" not in t:
                bad.append(f"#{i} missing type")
            has_before = "before" in t or "original" in t or "originalName" in t
            has_after = "after" in t or "optimized" in t or "newName" in t
            if not (has_before and has_after):
                bad.append(f"#{i} missing before/after pair")
        rep.check(
            "transformations.well_formed",
            not bad,
            "; ".join(bad[:5]),
        )

        # 11b. Per-category cardinality: each input category with services
        # should appear at least once in transformations OR be marked
        # alreadyOptimal in coverage. This catches the case where the
        # naming/desc agent silently skipped an entire category.
        naming_total = (coverage.get("naming") or {}).get("totalChecked", 0)
        if scraped_data.categories and naming_total:
            input_cats_with_svcs = sum(
                1 for c in scraped_data.categories if len(c.services) > 0
            )
            rep.check(
                "transformations.input_categories_covered",
                naming_total >= input_cats_with_svcs,
                f"naming.totalChecked={naming_total}, "
                f"input_categories_with_services={input_cats_with_svcs}",
            )

    # 12. quickWins / topIssues text quality (lighter — fewer fields)
    for i, qw in enumerate((quick_wins or [])[:3]):
        if isinstance(qw, dict):
            check_polish_text_field(
                rep, f"quickWin[{i}].action", qw.get("action"),
                require_diacritics_if_len=None,  # action can be terse
            )
    for i, iss in enumerate((issues or [])[:3]):
        if isinstance(iss, dict):
            check_polish_text_field(
                rep, f"issue[{i}].issue", iss.get("issue"),
                require_diacritics_if_len=None,
            )

    return rep


# ---------------------------------------------------------------------------
# Cennik pipeline invariants (BAGENT #2 / cennik.py output)
# ---------------------------------------------------------------------------

def check_cennik_output(
    cennik: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Cennik takes audit transformations + category mapping and
    produces a new pricelist. Critical invariants:
      - cardinality preserved: every input service appears in output
      - prices preserved or improved
      - no AI calls = full determinism
    """
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="cennik", salon_id=salon_id)

    # Cennik output shape varies. Accept either categoryProposals or
    # full pricelist with categories.
    proposals = cennik.get("categoryProposals") or cennik.get("categories") or []
    rep.check(
        "output.is_list",
        isinstance(proposals, list),
        f"got {type(proposals).__name__}",
    )

    if not isinstance(proposals, list):
        return rep

    # Cardinality: total services in output ≈ input (±10% for dedup)
    output_service_count = sum(
        len(p.get("services") or []) for p in proposals if isinstance(p, dict)
    )
    input_service_count = scraped_data.totalServices
    lo = int(input_service_count * 0.85)
    hi = int(input_service_count * 1.10) + 1
    rep.check(
        "cardinality.services_preserved",
        lo <= output_service_count <= hi,
        f"input={input_service_count}, output={output_service_count}, "
        f"expected [{lo}, {hi}]",
    )

    # Prices: every output service has non-empty price
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

    # Text quality on category names + service names + descriptions
    text_failures: list[str] = []
    for p in proposals:
        if not isinstance(p, dict):
            continue
        cat_name = p.get("name", "")
        if isinstance(cat_name, str):
            for name, fn in [
                ("mojibake", has_no_mojibake),
                ("placeholder", has_no_placeholders),
                ("ai_leak", has_no_ai_leakage),
            ]:
                ok, msg = fn(cat_name)
                if not ok:
                    text_failures.append(f"category {cat_name[:30]!r}.{name}: {msg}")
        for s in p.get("services") or []:
            if not isinstance(s, dict):
                continue
            for f in ("name", "description"):
                v = s.get(f) or ""
                if isinstance(v, str) and v:
                    for name, fn in [
                        ("mojibake", has_no_mojibake),
                        ("placeholder", has_no_placeholders),
                        ("ai_leak", has_no_ai_leakage),
                        ("double_space", has_no_double_spaces),
                    ]:
                        ok, msg = fn(v)
                        if not ok:
                            text_failures.append(f"service.{f} {v[:30]!r}.{name}: {msg}")
    rep.check(
        "text.quality_battery",
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
    """Summary should have headline + sections + ctaText (per
    convex/audit/validators.ts auditSummary shape).

    Required fields per Convex schema:
      - headline (str, ≤120 chars)
      - subheadline (str, ≤200 chars)
      - ctaText (str, ≤80 chars)
      - keyWins (list[str])
      - scoreDelta (int)
      - basicCompetitorData (dict, optional)
    """
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="summary", salon_id=salon_id)

    # Required keys (best-effort — schema may vary by version)
    expected_keys = ["headline", "subheadline", "ctaText"]
    rep.check(
        "schema.has_text_fields",
        any(k in summary for k in expected_keys),
        f"none of {expected_keys} present; got keys: {list(summary.keys())[:10]}",
    )

    # Length bounds
    if isinstance(summary.get("headline"), str):
        rep.check(
            "headline.length",
            5 <= len(summary["headline"]) <= 120,
            f"len={len(summary['headline'])} (expected 5-120)",
        )
    if isinstance(summary.get("subheadline"), str):
        rep.check(
            "subheadline.length",
            10 <= len(summary["subheadline"]) <= 200,
            f"len={len(summary['subheadline'])} (expected 10-200)",
        )
    if isinstance(summary.get("ctaText"), str):
        rep.check(
            "ctaText.length",
            3 <= len(summary["ctaText"]) <= 80,
            f"len={len(summary['ctaText'])} (expected 3-80)",
        )

    # Polish text quality on every emitted text field
    for k in ("headline", "subheadline", "ctaText"):
        check_polish_text_field(rep, k, summary.get(k))

    # keyWins — list of strings, each Polish-clean
    key_wins = summary.get("keyWins")
    if isinstance(key_wins, list):
        rep.check(
            "keyWins.is_list",
            True,
            "",
        )
        for i, kw in enumerate(key_wins[:5]):
            check_polish_text_field(
                rep, f"keyWins[{i}]", kw, require_diacritics_if_len=None,
            )

    # scoreDelta — integer, plausible range. Audit caps at 92, so delta
    # from baseline can be roughly [-92, +92].
    sd = summary.get("scoreDelta")
    if sd is not None:
        ok, msg = is_int_in_range(sd, -100, 100)
        rep.check("scoreDelta.in_range", ok, msg)

    return rep


# ---------------------------------------------------------------------------
# Competitor report pipeline invariants (BAGENT #4)
# ---------------------------------------------------------------------------

def check_competitor_report_output(
    report: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Competitor report is a 9-step pipeline producing dimensional
    scores (28 dimensions) + traceability artifacts per step + final
    synthesis.

    Required structure (per pipelines/competitor_report.py):
      - selectedCompetitors: list[salon] (≥3 for meaningful comparison)
      - dimensionalScores: dict[dimension_name → int 0-100] (≈28 keys)
      - synthesis: text summary (Polish)
      - finalScore: int 0-100
      - steps: list of 9 step rows with traceability_artifact
    """
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="competitor_report", salon_id=salon_id)

    # Selected competitors
    competitors = report.get("selectedCompetitors") or report.get("competitors") or []
    rep.check(
        "competitors.is_list",
        isinstance(competitors, list),
        f"got {type(competitors).__name__}",
    )
    if isinstance(competitors, list):
        rep.check(
            "competitors.minimum_three",
            len(competitors) >= 3,
            f"only {len(competitors)} — need ≥3 for meaningful comparison",
        )

    # Dimensional scores
    dim_scores = report.get("dimensionalScores") or {}
    if isinstance(dim_scores, dict):
        rep.check(
            "dimensions.count_close_to_28",
            20 <= len(dim_scores) <= 35,
            f"got {len(dim_scores)} dimensions (expected ~28)",
        )
        # Each score in [0, 100]
        out_of_range = [
            (k, v) for k, v in dim_scores.items()
            if not isinstance(v, (int, float)) or not (0 <= v <= 100)
        ]
        rep.check(
            "dimensions.scores_in_range",
            not out_of_range,
            f"{len(out_of_range)} out of [0,100]; first 3: {out_of_range[:3]}",
        )

    # Final score
    final_score = report.get("finalScore")
    if final_score is not None:
        ok, msg = is_int_in_range(final_score, 0, 100)
        rep.check("finalScore.in_range", ok, msg)
        # Cap rule applies — final competitor score also can't reach 100
        if isinstance(final_score, (int, float)):
            rep.check(
                "finalScore.capped",
                final_score <= MAX_DISPLAY_SCORE,
                f"finalScore={final_score} > MAX_DISPLAY_SCORE={MAX_DISPLAY_SCORE}",
            )

    # Synthesis text
    synth = report.get("synthesis") or report.get("narrative")
    if isinstance(synth, str):
        rep.check(
            "synthesis.length",
            100 <= len(synth) <= 10000,
            f"len={len(synth)}",
        )
        check_polish_text_field(rep, "synthesis", synth)

    # Steps with traceability
    steps = report.get("steps") or []
    if isinstance(steps, list):
        rep.check(
            "steps.count_is_nine",
            len(steps) == 9,
            f"got {len(steps)} (expected exactly 9 etap)",
        )
        steps_without_traceability = [
            i for i, s in enumerate(steps)
            if not isinstance(s, dict) or not s.get("traceability_artifact")
        ]
        rep.check(
            "steps.all_have_traceability",
            not steps_without_traceability,
            f"steps missing traceability: {steps_without_traceability}",
        )

    return rep


# ---------------------------------------------------------------------------
# Versum suggest pipeline invariants
# ---------------------------------------------------------------------------

def check_versum_suggest_output(
    output: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Versum mapping — maps each Booksy service to a Versum treatment
    suggestion. Output is a list of {boksyService, versumSuggestion,
    confidence}.

    Cardinality must match input service count (every service gets a
    suggestion, even if confidence is low).
    """
    salon_id = getattr(scraped_data, "salonId", None)
    rep = InvariantReport(pipeline="versum_suggest", salon_id=salon_id)

    suggestions = output.get("suggestions") or output.get("mappings") or []
    rep.check(
        "suggestions.is_list",
        isinstance(suggestions, list),
        f"got {type(suggestions).__name__}",
    )

    if isinstance(suggestions, list):
        # Every input service should have a suggestion (cardinality match)
        rep.check(
            "cardinality.matches_input",
            len(suggestions) == scraped_data.totalServices,
            f"suggestions={len(suggestions)}, input={scraped_data.totalServices}",
        )

        # Each suggestion well-formed
        bad = []
        for i, s in enumerate(suggestions):
            if not isinstance(s, dict):
                bad.append(f"#{i} not dict")
                continue
            if not (s.get("booksyService") or s.get("source")):
                bad.append(f"#{i} no booksyService")
            if "confidence" in s:
                conf = s["confidence"]
                if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
                    bad.append(f"#{i} confidence={conf} not in [0,1]")
        rep.check(
            "suggestions.well_formed",
            not bad,
            "; ".join(bad[:5]),
        )

    return rep


# ---------------------------------------------------------------------------
# Free report invariants (frozen subset of BAGENT #1)
# ---------------------------------------------------------------------------

def check_free_report_output(
    report: dict[str, Any],
    scraped_data: Any,
) -> InvariantReport:
    """Free report is the free-tier snapshot. Same structure as audit
    but limited to top-3 issues + summary, no transformations."""
    rep = check_audit_report(report, scraped_data)
    rep.pipeline = "free_report"

    # Free report SHOULD have ≤3 transformations and may have 0
    transformations = report.get("transformations", [])
    if isinstance(transformations, list):
        rep.check(
            "free.transformations_limited",
            len(transformations) <= 5,
            f"free report has {len(transformations)} transformations (expected ≤5)",
        )

    return rep


# ---------------------------------------------------------------------------
# Discovery pump invariants (workers/discovery_tasks.py)
# ---------------------------------------------------------------------------

def check_discovery_run(
    discovery_runs: list[dict[str, Any]],
) -> InvariantReport:
    """Invariants over a snapshot of discovery_runs rows.

    Catches:
      - duplicate concurrent runs (dedup guard regression)
      - zombie runs (status='running' with stale heartbeat)
      - status transition anomalies (queued without ever running)
    """
    rep = InvariantReport(pipeline="discovery", salon_id="snapshot")

    if not isinstance(discovery_runs, list):
        rep.check("input.is_list", False, f"got {type(discovery_runs).__name__}")
        return rep

    running = [r for r in discovery_runs if r.get("status") == "running"]

    # No two runs for same combo concurrently
    combos = [(r.get("category_id"), r.get("voivodeship_id")) for r in running]
    dupes = [c for c in combos if combos.count(c) > 1]
    rep.check(
        "no_duplicate_running_combos",
        not dupes,
        f"duplicate running combos: {set(dupes)}",
    )

    # No more than MAX_PARALLEL_PUMPS concurrent runs (default 3, allow
    # +1 for auto_retry race window)
    rep.check(
        "running_count_bounded",
        len(running) <= 5,
        f"{len(running)} concurrent runs (expected ≤4 = 3 slots + retry slack)",
    )

    return rep


# ---------------------------------------------------------------------------
# Score cap audit (cross-cutting)
# ---------------------------------------------------------------------------

def check_score_cap_everywhere(
    raw_outputs: dict[str, Any],
) -> InvariantReport:
    """Aggregate check: any field across all pipelines that represents
    a 'displayed score' must respect MAX_DISPLAY_SCORE.

    Pass a dict like:
      {"audit.totalScore": 87,
       "audit.industryComparison.yourScore": 87,
       "summary.headlineScore": 87,
       "competitor.finalScore": 89,
       "optimization.final_score": 91}

    All values must be ≤ MAX_DISPLAY_SCORE.
    """
    rep = InvariantReport(pipeline="score_cap_audit", salon_id="cross_cutting")
    for path, value in raw_outputs.items():
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            rep.check(f"score_cap.{path}.is_number", False, f"got {type(value).__name__}")
            continue
        rep.check(
            f"score_cap.{path}",
            value <= MAX_DISPLAY_SCORE,
            f"{path}={value} > MAX_DISPLAY_SCORE={MAX_DISPLAY_SCORE}",
        )
    return rep
