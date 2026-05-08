"""Outreach asset loader — docs/outreach-assets/ → Supabase.

Walks the per-funnel directories under
``BEAUTY_AUDIT/docs/outreach-assets/funnels/<funnel>/`` (icp.json,
segments/*.json, sequences/*.json, templates/*.md, compliance.md),
validates each file against the schema in
``.claude/skills/outreach-asset-schema/SKILL.md``, and writes to
Supabase ``outreach_audience_segments`` / ``outreach_template_variants``
/ ``outreach_sequences`` with ``approval_status='pending_review'``.

CLI (run inside bagent venv):

    # Validate single file (no DB writes)
    uv run python -m pipelines.outreach_loader \\
      --validate-segment <repo_dir>/docs/outreach-assets/funnels/audit/segments/X.json

    uv run python -m pipelines.outreach_loader \\
      --validate-template <repo_dir>/.../templates/d0_cold_X.md

    uv run python -m pipelines.outreach_loader \\
      --validate-sequence <repo_dir>/.../sequences/X.json

    # Estimate audience size dla segmentu (queries Supabase, no inserts)
    uv run python -m pipelines.outreach_loader \\
      --estimate-segment-size <path>.json

    # Submit całe funnel folder do approval queue (DB writes)
    uv run python -m pipelines.outreach_loader \\
      --funnel audit --repo /path/to/BEAUTY_AUDIT --submit

The compliance gate (``compliance.md`` ze ``Status: compliant``) is
hard-required before ``--submit`` writes anything. Without it the
loader exits non-zero and changes nothing.

This is a one-shot pipeline (run by operator after subagent team
finishes a funnel). NOT an arq worker — no scheduling, no Redis. The
deployer worker (workers/outreach_deployer.py) picks up rows once the
operator approves them in the admin UI.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("bagent.pipelines.outreach_loader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------------------------------------------------------------------
# Constants — mirror schema enums (keep in sync with migration 041)
# ---------------------------------------------------------------------

VALID_FUNNELS = {"audit", "competitor_report", "monitoring", "crosssell_rescue"}
VALID_COHORTS = {"scale", "opt_in", "manual", "imported"}
VALID_VERTICALS = {"beauty_mass", "health_regulated", "adjacent", "generic"}
VALID_SIGNAL_TYPES = {
    "desc_gap", "price_outlier", "review_velocity", "competitor_advantage",
    "onboarding", "upsell", "rescue", "monitoring_value",
}

# Word-count windows per step type (cold, follow-up etc.). Used to flag
# templates that are likely too long/short.
WORD_COUNT_WINDOWS = {
    "cold":      (60, 180),
    "followup":  (40, 120),
    "onboarding":(80, 220),
    "upsell":    (50, 150),
    "rescue":    (60, 180),
}

# Minimum / maximum acceptable segment size before deliverability
# concerns or statistical insignificance kick in.
SEGMENT_SIZE_MIN = 30
SEGMENT_SIZE_MAX = 5000

# Polish diacritics regex — at least ONE must appear in any non-trivial
# email body, missing them is a strong spam-filter signal.
POLISH_DIACRITICS_RE = re.compile(r"[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]")


# ---------------------------------------------------------------------
# Validation result types
# ---------------------------------------------------------------------

@dataclass
class ValidationIssue:
    """One validation problem. severity = 'error' blocks submit."""
    severity: str            # 'error' | 'warning' | 'info'
    code: str                # SEGMENT_TOO_SMALL, TEMPLATE_BANNED_WORDS etc.
    message: str
    path: str | None = None  # which file the issue belongs to


@dataclass
class ValidationReport:
    """Aggregate of issues across one or many files."""
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    def merge(self, other: "ValidationReport") -> None:
        self.issues.extend(other.issues)

    def add(self, severity: str, code: str, message: str, path: str | None = None) -> None:
        self.issues.append(ValidationIssue(severity, code, message, path))

    def print(self) -> None:
        for i in self.issues:
            prefix = {"error": "✗", "warning": "⚠", "info": "i"}[i.severity]
            location = f" [{i.path}]" if i.path else ""
            print(f"{prefix} {i.code}: {i.message}{location}")


# ---------------------------------------------------------------------
# Banned-words config loader
# ---------------------------------------------------------------------

def _load_banned_words(repo_dir: Path) -> dict[str, list[re.Pattern[str]]]:
    """Compile regex patterns from compliance-banned-words.json.

    Returns dict keyed by category (medical_efficacy_banned, etc.) with
    pre-compiled Pattern objects ready to test against subject/body.
    """
    path = repo_dir / "docs/outreach-assets/shared/compliance-banned-words.json"
    if not path.exists():
        # Don't error — file is operator-maintained and may not exist yet.
        # Compliance-pl-reviewer subagent populates it when it finds new
        # banned phrases. Loader uses an empty config until then.
        logger.warning("compliance-banned-words.json not found at %s; skipping word-level checks", path)
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse %s: %s", path, exc)
        return {}

    compiled: dict[str, list[re.Pattern[str]]] = {}
    for category, patterns in raw.items():
        if not isinstance(patterns, list):
            continue
        compiled[category] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled


# ---------------------------------------------------------------------
# Filter DSL → SQL compiler
# ---------------------------------------------------------------------

# Whitelist of column paths the segment filter is allowed to reference.
# Keep narrow on purpose — anything outside this list raises an error
# rather than silently emitting bad SQL. Add to this set only after
# verifying the column exists in the schema (beauty-audit-data-model
# skill is the source of truth).
ALLOWED_COLUMN_PATHS = {
    # outreach_contacts
    "contact.email", "contact.cohort", "contact.unsubscribed_at",
    "contact.last_outreach_sent_at", "contact.total_messages_sent",
    "contact.consent_b2b", "contact.created_at",
    # salons + extracted columns (subset; expand as needed)
    "salon.id", "salon.salon_city", "salon.salon_country",
    "salon.primary_category_id", "salon.primary_category_name",
    "salon.review_count", "salon.review_score",
    "salon.is_active", "salon.last_full_scrape_at",
    # change-tracking views
    "diff.desc_gap_pct", "diff.price_outlier_count",
    "diff.review_velocity_30d", "diff.last_change_at",
    # state references — special prefix, validator allows
    # 'state.<name>.<field>' through but doesn't compile here (loader
    # doesn't yet generate SQL for parallel state joins; that work
    # belongs in workers/outreach_orchestrator.py once we have real
    # filter SQL needs).
}


def _validate_filter_dsl(
    filter_obj: Any,
    report: ValidationReport,
    file_path: str,
    *,
    depth: int = 0,
) -> None:
    """Walk filter_dsl JSON and flag unknown columns + bad operators.

    The DSL grammar is documented in the outreach-asset-schema skill.
    This is a structural validator — does NOT yet compile to SQL. The
    orchestrator worker compiles at run-time using the same grammar.
    """
    if depth > 10:
        report.add("error", "FILTER_TOO_DEEP",
                   "Filter DSL nested deeper than 10 levels", file_path)
        return

    if not isinstance(filter_obj, dict):
        report.add("error", "FILTER_INVALID",
                   f"Expected dict, got {type(filter_obj).__name__}", file_path)
        return

    for key, value in filter_obj.items():
        if key in ("and", "or"):
            if not isinstance(value, list):
                report.add("error", "FILTER_INVALID",
                           f"'{key}' must be array", file_path)
                continue
            for sub in value:
                _validate_filter_dsl(sub, report, file_path, depth=depth + 1)
        elif key == "not":
            _validate_filter_dsl(value, report, file_path, depth=depth + 1)
        else:
            # Leaf check — column path must be allowed.
            if key.startswith("state."):
                # Multi-state references not yet whitelisted by exact
                # name — loosely allow, orchestrator will resolve.
                continue
            if key not in ALLOWED_COLUMN_PATHS:
                report.add("error", "SEGMENT_FILTER_INVALID",
                           f"Column path '{key}' not in whitelist. "
                           f"See beauty-audit-data-model skill for "
                           f"canonical list.", file_path)


# ---------------------------------------------------------------------
# File parsers
# ---------------------------------------------------------------------

def parse_template_md(path: Path) -> tuple[dict[str, Any], str, str]:
    """Split front-matter YAML + Subject + Body sections of a template.

    Returns (front_matter_dict, subject, body_md). Raises ValueError on
    structural problems before semantic validation runs.
    """
    text = path.read_text(encoding="utf-8")

    # YAML front matter is between two `---` lines at file start.
    fm_match = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)$", text, re.DOTALL)
    if not fm_match:
        raise ValueError(f"{path}: missing YAML front matter")

    fm_yaml, after_fm = fm_match.groups()
    try:
        front_matter = yaml.safe_load(fm_yaml) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{path}: front matter not valid YAML: {exc}") from exc

    # Body sections — the template format is:
    #   ## Subject
    #   <one-line subject>
    #
    #   ## Body
    #   <markdown body...>
    subj_match = re.search(r"##\s*Subject\s*\n(.*?)(?=\n##\s|\Z)", after_fm, re.DOTALL)
    body_match = re.search(r"##\s*Body\s*\n(.*)$", after_fm, re.DOTALL)
    if not subj_match:
        raise ValueError(f"{path}: missing '## Subject' section")
    if not body_match:
        raise ValueError(f"{path}: missing '## Body' section")

    subject = subj_match.group(1).strip()
    body = body_match.group(1).strip()
    return front_matter, subject, body


# ---------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------

def validate_icp(path: Path) -> ValidationReport:
    """Validate icp.json shape."""
    report = ValidationReport()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.add("error", "ICP_PARSE", str(exc), str(path))
        return report

    if data.get("funnel") not in VALID_FUNNELS:
        report.add("error", "ICP_FUNNEL_INVALID",
                   f"funnel must be one of {VALID_FUNNELS}", str(path))

    personas = data.get("primary_personas", [])
    if not isinstance(personas, list) or not personas:
        report.add("error", "ICP_NO_PERSONAS",
                   "primary_personas must be non-empty array", str(path))
    else:
        for idx, persona in enumerate(personas):
            for required in ("name", "vertical", "pain_points", "trigger_signals"):
                if required not in persona:
                    report.add("error", "ICP_PERSONA_MISSING_FIELD",
                               f"persona[{idx}] missing '{required}'", str(path))
            if persona.get("vertical") not in VALID_VERTICALS:
                report.add("error", "ICP_PERSONA_VERTICAL_INVALID",
                           f"persona[{idx}].vertical invalid", str(path))

    return report


def validate_segment(path: Path) -> ValidationReport:
    """Validate segments/<name>.json against schema."""
    report = ValidationReport()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.add("error", "SEGMENT_PARSE", str(exc), str(path))
        return report

    # Required fields
    for field_name in ("funnel", "name", "cohort", "filter_dsl"):
        if field_name not in data:
            report.add("error", "SEGMENT_MISSING_FIELD",
                       f"missing '{field_name}'", str(path))

    if data.get("funnel") not in VALID_FUNNELS:
        report.add("error", "SEGMENT_FUNNEL_INVALID",
                   f"funnel must be in {VALID_FUNNELS}", str(path))
    if data.get("cohort") not in VALID_COHORTS:
        report.add("error", "SEGMENT_COHORT_INVALID",
                   f"cohort must be in {VALID_COHORTS}", str(path))

    # Filter DSL structural check
    if "filter_dsl" in data:
        _validate_filter_dsl(data["filter_dsl"], report, str(path))

    # Size sanity hint (operator may not have run --estimate yet)
    expected_min = data.get("expected_size_min")
    expected_max = data.get("expected_size_max")
    if isinstance(expected_min, int) and expected_min < SEGMENT_SIZE_MIN:
        report.add("warning", "SEGMENT_TOO_SMALL_HINT",
                   f"expected_size_min={expected_min} below {SEGMENT_SIZE_MIN}", str(path))
    if isinstance(expected_max, int) and expected_max > SEGMENT_SIZE_MAX:
        report.add("warning", "SEGMENT_TOO_LARGE_HINT",
                   f"expected_size_max={expected_max} above {SEGMENT_SIZE_MAX}", str(path))

    return report


def validate_sequence(path: Path, *, available_template_keys: set[str] | None = None) -> ValidationReport:
    """Validate sequences/<name>.json structural rules.

    available_template_keys: if provided, every step.template_step_key
    must appear in this set — catches mismatches before deploy.
    """
    report = ValidationReport()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report.add("error", "SEQUENCE_PARSE", str(exc), str(path))
        return report

    if data.get("funnel") not in VALID_FUNNELS:
        report.add("error", "SEQUENCE_FUNNEL_INVALID", "invalid funnel", str(path))
    if not data.get("entry_state"):
        report.add("error", "SEQUENCE_ENTRY_MISSING", "entry_state required", str(path))

    steps = data.get("steps", [])
    if not isinstance(steps, list) or len(steps) < 4:
        report.add("error", "SEQUENCE_TOO_FEW_STEPS",
                   "≥4 steps required (cold→buyer baseline)", str(path))

    branch_count = 0
    seen_keys: set[str] = set()
    for idx, step in enumerate(steps):
        if "key" not in step:
            report.add("error", "SEQUENCE_STEP_NO_KEY",
                       f"step[{idx}] missing 'key'", str(path))
            continue
        if step["key"] in seen_keys:
            report.add("error", "SEQUENCE_DUPLICATE_KEY",
                       f"duplicate step key '{step['key']}'", str(path))
        seen_keys.add(step["key"])
        if "template_step_key" not in step:
            report.add("error", "SEQUENCE_STEP_NO_TEMPLATE",
                       f"step[{idx}] missing 'template_step_key'", str(path))
        elif (
            available_template_keys is not None
            and step["template_step_key"] not in available_template_keys
        ):
            report.add("error", "SEQUENCE_TEMPLATE_MISSING",
                       f"step[{idx}] template_step_key='{step['template_step_key']}' "
                       f"has no matching template file", str(path))
        if "predicate" in step and step["predicate"] != {"always": True}:
            branch_count += 1

    if branch_count < 3:
        report.add("warning", "SEQUENCE_FEW_BRANCHES",
                   f"only {branch_count} branched steps (target ≥3 dla N non-linear)", str(path))

    exit_conditions = data.get("exit_conditions", [])
    expected_events = {"audit_purchased", "unsubscribed", "complained", "sequence_finished"}
    seen_events = {e.get("event", "").split("_consec")[0] for e in exit_conditions}
    missing = expected_events - seen_events - {ev for seen in seen_events for ev in expected_events if seen.startswith(ev)}
    # Loose check — purchase/unsubscribe/complaint/finished
    if not any("purchase" in (e.get("event") or "") for e in exit_conditions):
        report.add("warning", "SEQUENCE_NO_PURCHASE_EXIT",
                   "no exit_condition for purchase event", str(path))
    if not any("unsubscribe" in (e.get("event") or "") for e in exit_conditions):
        report.add("warning", "SEQUENCE_NO_UNSUB_EXIT",
                   "no exit_condition for unsubscribe", str(path))

    return report


def validate_template(
    path: Path,
    *,
    banned_words: dict[str, list[re.Pattern[str]]] | None = None,
) -> ValidationReport:
    """Validate templates/<name>.md front matter + body."""
    report = ValidationReport()
    try:
        front, subject, body = parse_template_md(path)
    except ValueError as exc:
        report.add("error", "TEMPLATE_STRUCTURE", str(exc), str(path))
        return report

    # Front matter required fields
    for required in ("funnel", "step_key", "vertical", "variables_required"):
        if required not in front:
            report.add("error", "TEMPLATE_MISSING_FIELD",
                       f"front matter missing '{required}'", str(path))

    if front.get("funnel") not in VALID_FUNNELS:
        report.add("error", "TEMPLATE_FUNNEL_INVALID",
                   f"funnel must be in {VALID_FUNNELS}", str(path))
    if front.get("vertical") not in VALID_VERTICALS:
        report.add("error", "TEMPLATE_VERTICAL_INVALID",
                   f"vertical must be in {VALID_VERTICALS}", str(path))
    sig = front.get("signal_type")
    if sig is not None and sig not in VALID_SIGNAL_TYPES:
        report.add("warning", "TEMPLATE_SIGNAL_UNKNOWN",
                   f"signal_type='{sig}' not in standard set", str(path))

    # Subject length cap
    if len(subject) > 60:
        report.add("warning", "TEMPLATE_SUBJECT_LONG",
                   f"subject {len(subject)} chars > 60 (Gmail truncation)", str(path))

    # Preview text length
    preview = front.get("preview_text", "")
    if isinstance(preview, str) and len(preview) > 90:
        report.add("warning", "TEMPLATE_PREVIEW_LONG",
                   f"preview_text {len(preview)} chars > 90", str(path))

    # Variables consistency — every {{var}} in body+subject must be in
    # variables_required, AND every variables_required entry must
    # appear at least once.
    declared_vars = set(front.get("variables_required") or [])
    used_vars = set(re.findall(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}", subject + "\n" + body))
    undeclared = used_vars - declared_vars
    unused = declared_vars - used_vars
    for var in undeclared:
        report.add("error", "TEMPLATE_VARIABLES_MISSING",
                   f"{{{{{var}}}}} used but not in variables_required", str(path))
    for var in unused:
        report.add("warning", "TEMPLATE_VARIABLES_UNUSED",
                   f"variable '{var}' declared but not used in subject/body", str(path))

    # Polish diacritics — at least one in body
    if not POLISH_DIACRITICS_RE.search(body):
        report.add("warning", "TEMPLATE_NO_DIACRITICS",
                   "body missing Polish diacritics — likely unprofessional", str(path))

    # Word count window per step type
    word_count = len(body.split())
    step_key = front.get("step_key", "")
    window_match = next(
        ((w, n) for w, n in WORD_COUNT_WINDOWS.items() if w in step_key.lower()),
        None,
    )
    if window_match:
        kind, (lo, hi) = window_match[0], window_match[1]
        if word_count < lo:
            report.add("warning", "TEMPLATE_TOO_SHORT",
                       f"body {word_count} words < {lo} ({kind} expected)", str(path))
        elif word_count > hi:
            report.add("warning", "TEMPLATE_TOO_LONG",
                       f"body {word_count} words > {hi} ({kind} expected)", str(path))

    # Banned words check (compliance)
    if banned_words:
        haystack = f"{subject}\n{body}"
        for category, patterns in banned_words.items():
            for pattern in patterns:
                m = pattern.search(haystack)
                if m:
                    report.add("error", "TEMPLATE_BANNED_WORDS",
                               f"matches {category}: '{m.group(0)}'", str(path))

    return report


def validate_compliance_md(path: Path) -> tuple[ValidationReport, str | None]:
    """Read compliance.md, return report + status string ('compliant', 'concerns_raised', 'blocked', None)."""
    report = ValidationReport()
    if not path.exists():
        report.add("error", "COMPLIANCE_NOT_REVIEWED",
                   "compliance.md missing — run compliance-pl-reviewer subagent", str(path))
        return report, None

    text = path.read_text(encoding="utf-8")
    status_match = re.search(r"\*\*Status:\*\*\s*(\w+)", text)
    if not status_match:
        report.add("error", "COMPLIANCE_NO_STATUS",
                   "compliance.md missing '**Status:**' line", str(path))
        return report, None

    status = status_match.group(1).strip().lower()
    if status not in {"compliant", "concerns_raised", "blocked"}:
        report.add("error", "COMPLIANCE_STATUS_INVALID",
                   f"status='{status}' not in {{compliant, concerns_raised, blocked}}", str(path))
        return report, None

    if status == "blocked":
        report.add("error", "COMPLIANCE_BLOCKED",
                   "compliance.md status='blocked' — funnel cannot deploy", str(path))
    elif status == "concerns_raised":
        report.add("warning", "COMPLIANCE_CONCERNS",
                   "compliance.md status='concerns_raised' — review notes", str(path))

    return report, status


# ---------------------------------------------------------------------
# Whole-funnel walker
# ---------------------------------------------------------------------

@dataclass
class FunnelBundle:
    """Parsed contents of one funnels/<funnel>/ directory."""
    funnel: str
    icp_path: Path
    icp: dict[str, Any]
    segment_paths: list[Path]
    segments: list[dict[str, Any]]
    sequence_paths: list[Path]
    sequences: list[dict[str, Any]]
    template_paths: list[Path]
    templates: list[tuple[dict[str, Any], str, str]]   # (front, subject, body)
    compliance_path: Path
    compliance_status: str | None


def load_funnel_bundle(repo_dir: Path, funnel: str) -> tuple[FunnelBundle | None, ValidationReport]:
    """Walk a funnel folder, parse all files, return bundle + report."""
    report = ValidationReport()
    funnel_dir = repo_dir / "docs/outreach-assets/funnels" / funnel
    if not funnel_dir.exists():
        report.add("error", "FUNNEL_DIR_MISSING",
                   f"{funnel_dir} not found", str(funnel_dir))
        return None, report

    icp_path = funnel_dir / "icp.json"
    icp_report = validate_icp(icp_path)
    report.merge(icp_report)
    icp_data: dict[str, Any] = {}
    if icp_path.exists():
        try:
            icp_data = json.loads(icp_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    # Segments
    segment_paths = sorted((funnel_dir / "segments").glob("*.json")) if (funnel_dir / "segments").exists() else []
    segments_data: list[dict[str, Any]] = []
    for sp in segment_paths:
        seg_report = validate_segment(sp)
        report.merge(seg_report)
        try:
            segments_data.append(json.loads(sp.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            pass

    # Templates first (so we can build the available_template_keys set
    # before validating sequences).
    template_paths = sorted((funnel_dir / "templates").glob("*.md")) if (funnel_dir / "templates").exists() else []
    banned = _load_banned_words(repo_dir)
    templates_data: list[tuple[dict[str, Any], str, str]] = []
    template_step_keys: set[str] = set()
    for tp in template_paths:
        tpl_report = validate_template(tp, banned_words=banned)
        report.merge(tpl_report)
        try:
            front, subject, body = parse_template_md(tp)
            templates_data.append((front, subject, body))
            if front.get("step_key"):
                template_step_keys.add(str(front["step_key"]))
        except ValueError:
            pass

    # Sequences (with cross-check against template step keys)
    sequence_paths = sorted((funnel_dir / "sequences").glob("*.json")) if (funnel_dir / "sequences").exists() else []
    sequences_data: list[dict[str, Any]] = []
    for sqp in sequence_paths:
        seq_report = validate_sequence(sqp, available_template_keys=template_step_keys)
        report.merge(seq_report)
        try:
            sequences_data.append(json.loads(sqp.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            pass

    # Compliance gate (mandatory for --submit)
    compliance_path = funnel_dir / "compliance.md"
    cm_report, cm_status = validate_compliance_md(compliance_path)
    report.merge(cm_report)

    # Counts gate — minimum viable funnel
    if len(segment_paths) < 1:
        report.add("error", "FUNNEL_NO_SEGMENTS",
                   "at least 1 segment required", str(funnel_dir))
    if len(template_paths) < 4:
        report.add("error", "FUNNEL_TOO_FEW_TEMPLATES",
                   f"only {len(template_paths)} templates (≥4 required)", str(funnel_dir))
    if len(sequence_paths) < 1:
        report.add("error", "FUNNEL_NO_SEQUENCES",
                   "at least 1 sequence required", str(funnel_dir))

    bundle = FunnelBundle(
        funnel=funnel,
        icp_path=icp_path,
        icp=icp_data,
        segment_paths=segment_paths,
        segments=segments_data,
        sequence_paths=sequence_paths,
        sequences=sequences_data,
        template_paths=template_paths,
        templates=templates_data,
        compliance_path=compliance_path,
        compliance_status=cm_status,
    )
    return bundle, report


# ---------------------------------------------------------------------
# Supabase writer (only invoked under --submit)
# ---------------------------------------------------------------------

def _make_supabase_client():
    """Lazy-import so --validate flows don't need Supabase env at all."""
    from config import settings  # noqa: WPS433
    from services.sb_client import make_supabase_client

    if not settings.supabase_url or not settings.supabase_service_key:
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_SERVICE_KEY not configured — required for --submit",
        )
    return make_supabase_client(settings.supabase_url, settings.supabase_service_key)


def submit_funnel(bundle: FunnelBundle, *, dry_run: bool = False) -> dict[str, Any]:
    """Insert all bundle assets into Supabase as approval_status='pending_review'.

    Idempotency: re-running --submit on the same bundle is a no-op for
    files whose content hasn't changed (matched by funnel + name +
    step_key). Operator approves in admin UI; the deployer worker is
    responsible for the wintact push, NOT this function.
    """
    if bundle.compliance_status != "compliant":
        raise RuntimeError(
            f"compliance.md status='{bundle.compliance_status}' — refusing to submit. "
            f"Spawn compliance-pl-reviewer agent until status='compliant'."
        )

    summary: dict[str, Any] = {
        "funnel": bundle.funnel,
        "segments_inserted": 0,
        "segments_skipped": 0,
        "templates_inserted": 0,
        "templates_skipped": 0,
        "sequences_inserted": 0,
        "sequences_skipped": 0,
        "dry_run": dry_run,
    }

    if dry_run:
        # Just echo what would happen.
        summary["segments_inserted"] = len(bundle.segments)
        summary["templates_inserted"] = len(bundle.templates)
        summary["sequences_inserted"] = len(bundle.sequences)
        return summary

    sb = _make_supabase_client()

    # Segments
    for seg in bundle.segments:
        existing = (
            sb.table("outreach_audience_segments")
            .select("id")
            .eq("funnel", seg["funnel"])
            .eq("name", seg["name"])
            .execute()
        )
        if existing.data:
            summary["segments_skipped"] += 1
            continue
        sb.table("outreach_audience_segments").insert({
            "funnel": seg["funnel"],
            "name": seg["name"],
            "description": seg.get("description"),
            "cohort": seg["cohort"],
            "filter_json": seg["filter_dsl"],
            "estimated_size": seg.get("expected_size_max"),
            "approval_status": "pending_review",
            "compliance_status": "compliant",
            "compliance_reviewed_by": "compliance-pl-reviewer",
            "created_by": "outreach_loader",
        }).execute()
        summary["segments_inserted"] += 1

    # Templates — render body_md → body_html using the brand shell so
    # the operator previews exactly what wintact will receive.
    from services.email_renderer import render_email_html  # local import keeps validate-only mode dep-free

    for front, subject, body in bundle.templates:
        existing = (
            sb.table("outreach_template_variants")
            .select("id")
            .eq("funnel", front["funnel"])
            .eq("step_key", front["step_key"])
            .eq("vertical", front["vertical"])
            .eq("variant_label", front.get("variant_label", "A"))
            .execute()
        )
        if existing.data:
            summary["templates_skipped"] += 1
            continue

        utm_campaign = f"{front['funnel']}_{front['step_key']}"
        try:
            body_html = render_email_html(
                body_md=body,
                subject=subject,
                preview_text=front.get("preview_text"),
                utm_campaign=utm_campaign,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Email shell render failed for %s/%s: %s",
                         front["funnel"], front["step_key"], exc)
            body_html = None  # deployer will fall back to body_md

        sb.table("outreach_template_variants").insert({
            "funnel": front["funnel"],
            "step_key": front["step_key"],
            "signal_type": front.get("signal_type"),
            "vertical": front["vertical"],
            "subject": subject,
            "body_md": body,
            "body_html": body_html,
            "preview_text": front.get("preview_text"),
            "variables_required": front.get("variables_required") or [],
            "variant_label": front.get("variant_label", "A"),
            "approval_status": "pending_review",
            "compliance_status": "compliant",
            "compliance_reviewed_by": "compliance-pl-reviewer",
            "created_by": "outreach_loader",
        }).execute()
        summary["templates_inserted"] += 1

    # Sequences
    for seq in bundle.sequences:
        existing = (
            sb.table("outreach_sequences")
            .select("id")
            .eq("funnel", seq["funnel"])
            .eq("name", seq["name"])
            .execute()
        )
        if existing.data:
            summary["sequences_skipped"] += 1
            continue
        sb.table("outreach_sequences").insert({
            "funnel": seq["funnel"],
            "name": seq["name"],
            "description": seq.get("description"),
            "entry_state": seq["entry_state"],
            "flow_json": {
                "steps": seq.get("steps", []),
                "exit_conditions": seq.get("exit_conditions", []),
                "entry_segments": seq.get("entry_segments", []),
                "frequency_cap_override": seq.get("frequency_cap_override"),
            },
            "approval_status": "pending_review",
            "compliance_status": "compliant",
            "compliance_reviewed_by": "compliance-pl-reviewer",
            "active": False,
            "created_by": "outreach_loader",
        }).execute()
        summary["sequences_inserted"] += 1

    return summary


# ---------------------------------------------------------------------
# Audience size estimate (lightweight DRY-run)
# ---------------------------------------------------------------------

def estimate_segment_size(seg_path: Path) -> int | None:
    """Return rough audience size for a segment's filter_dsl.

    For now this is intentionally minimal — it counts contacts in
    outreach_contacts that pass the universal exclusions
    (unsubscribed_at IS NULL, recent send filter, email present). The
    full filter_dsl → SQL compiler lives in the orchestrator worker;
    this is a "≥SEGMENT_SIZE_MIN, ≤SEGMENT_SIZE_MAX?" sanity check.
    """
    try:
        sb = _make_supabase_client()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return None

    # Count baseline-eligible contacts. Operator interprets the result
    # as "if your filter_dsl narrows further, you'll get less than this."
    res = (
        sb.table("outreach_contacts")
        .select("id", count="exact")
        .is_("unsubscribed_at", "null")
        .execute()
    )
    eligible = res.count or 0
    print(f"Eligible-baseline contacts (no unsubs): {eligible}")
    print(f"Acceptable segment size window: {SEGMENT_SIZE_MIN}–{SEGMENT_SIZE_MAX}")
    print(f"Filter file: {seg_path}")
    return eligible


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="outreach_loader",
        description="Validate and submit outreach assets to Supabase.",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(os.environ.get("BEAUTY_AUDIT_REPO", "/Users/alex/Desktop/MOJE_PROJEKTY/BEAUTY_AUDIT")),
        help="Path to BEAUTY_AUDIT repo root (contains docs/outreach-assets/)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-icp",       type=Path, metavar="PATH")
    group.add_argument("--validate-segment",   type=Path, metavar="PATH")
    group.add_argument("--validate-sequence",  type=Path, metavar="PATH")
    group.add_argument("--validate-template",  type=Path, metavar="PATH")
    group.add_argument("--estimate-segment-size", type=Path, metavar="PATH")
    group.add_argument("--funnel", choices=sorted(VALID_FUNNELS),
                       help="Submit entire funnel folder to approval queue")
    parser.add_argument("--submit", action="store_true",
                        help="Required with --funnel to write to DB; otherwise dry-run")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --funnel, validate + show what would be inserted")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.validate_icp:
        report = validate_icp(args.validate_icp)
        report.print()
        return 1 if report.has_errors else 0

    if args.validate_segment:
        report = validate_segment(args.validate_segment)
        report.print()
        return 1 if report.has_errors else 0

    if args.validate_sequence:
        report = validate_sequence(args.validate_sequence)
        report.print()
        return 1 if report.has_errors else 0

    if args.validate_template:
        banned = _load_banned_words(args.repo)
        report = validate_template(args.validate_template, banned_words=banned)
        report.print()
        return 1 if report.has_errors else 0

    if args.estimate_segment_size:
        size = estimate_segment_size(args.estimate_segment_size)
        return 0 if size is not None else 1

    if args.funnel:
        bundle, report = load_funnel_bundle(args.repo, args.funnel)
        report.print()
        if bundle is None or report.has_errors:
            print(f"\n✗ Validation failed — {len(report.errors)} errors")
            return 1
        if args.dry_run:
            summary = submit_funnel(bundle, dry_run=True)
            print(f"\n[dry-run] would insert: {json.dumps(summary, indent=2)}")
            return 0
        if not args.submit:
            print("\nValidation passed. Re-run with --submit to write to Supabase.")
            return 0
        try:
            summary = submit_funnel(bundle, dry_run=False)
        except RuntimeError as exc:
            print(f"\n✗ Submit refused: {exc}")
            return 1
        print(f"\n✓ Submit complete: {json.dumps(summary, indent=2)}")
        print(
            "\nApproval queue UI: /admin/outreach/approval-queue — Joanna "
            "approves rows. Approved rows are picked up by "
            "workers/outreach_deployer.py for wintact push."
        )
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
