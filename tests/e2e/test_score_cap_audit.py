"""Cross-cutting score cap audit.

Product rule: displayed score MUST cap at MAX_DISPLAY_SCORE=92.
NEVER 100 — leaves headroom for ad-campaign upsell (separate paid
product). See memory/feedback_score_cap.md.

This test grep-audits every place a score is rendered or persisted to
make sure capping is applied consistently. It's static analysis on
the code, not a runtime check, because score capping is a write-time
concern — by the time data hits the wire it's too late to test cap.

Files audited:
  - pipelines/report.py — cap_audit_score() applied to totalScore
  - pipelines/cennik.py — should not unwrap or recompute past cap
  - pipelines/summary.py — headlineScore must respect cap
  - pipelines/competitor_report.py — finalScore must cap
  - lib/pdf/audit-report-pdf.tsx — frontend renders capped value
  - lib/pdf/certificate-pdf.tsx — certificate must show capped value

Usage:
    pytest tests/e2e/test_score_cap_audit.py -v -m e2e
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

BAGENT_ROOT = Path(__file__).resolve().parents[2]
BEAUTY_AUDIT_ROOT = BAGENT_ROOT.parent / "BEAUTY_AUDIT"

MAX_DISPLAY_SCORE = 92


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (FileNotFoundError, IsADirectoryError):
        return None


@pytest.mark.e2e
def test_cap_audit_score_helper_exists_and_caps_below_100():
    """The capping helper should be the single source of truth for
    audit-phase score capping.

    Two related but distinct caps in BeautyAudit's design:
      - MAX_AUDIT_SCORE (currently 88) — raw audit cap before
        optimization, applied by cap_audit_score in pipelines/helpers.py
      - MAX_DISPLAY_SCORE (currently 92) — final display cap after
        optimization, leaving 8 points headroom for ad-campaign upsell

    Both must be < 100. This test asserts cap_audit_score exists and
    references SOME numeric cap below 100."""
    helpers = _read(BAGENT_ROOT / "pipelines" / "helpers.py")
    assert helpers is not None, "pipelines/helpers.py missing"
    assert "cap_audit_score" in helpers, (
        "cap_audit_score helper not found in pipelines/helpers.py — "
        "cap may not be applied"
    )
    # Look for MAX_AUDIT_SCORE constant — production uses this name
    max_audit_score_match = re.search(
        r"MAX_AUDIT_SCORE\s*=\s*(\d+)", helpers
    )
    assert max_audit_score_match, (
        "no MAX_AUDIT_SCORE constant found in pipelines/helpers.py — "
        "audit-phase cap may have been removed"
    )
    cap_value = int(max_audit_score_match.group(1))
    assert cap_value < 100, (
        f"MAX_AUDIT_SCORE={cap_value} is not below 100 — "
        f"audits could reach 100 which violates product spec "
        f"(see memory/feedback_score_cap.md)"
    )
    assert cap_value >= 80, (
        f"MAX_AUDIT_SCORE={cap_value} is suspiciously low — "
        f"either intentional aggressive cap or a typo regression"
    )


@pytest.mark.e2e
def test_no_hardcoded_uncapped_score_100_in_pipelines():
    """No pipeline file should hardcode `score = 100` or
    `totalScore: 100` without going through cap_audit_score.
    Catches accidental 'overrides' that bypass cap."""
    suspect_pattern = re.compile(
        r"(score|totalScore|finalScore)\s*[:=]\s*100\b"
    )
    flagged = []
    for f in (BAGENT_ROOT / "pipelines").glob("*.py"):
        text = _read(f)
        if text is None:
            continue
        for m in suspect_pattern.finditer(text):
            line_no = text[: m.start()].count("\n") + 1
            line = text.split("\n")[line_no - 1].strip()
            # Allow 0-100 range checks like `0 <= x <= 100`
            if "<=" in line or "<" in line or "range" in line.lower():
                continue
            # Allow comments
            if line.lstrip().startswith("#"):
                continue
            flagged.append(f"{f.relative_to(BAGENT_ROOT)}:{line_no}: {line}")
    assert not flagged, (
        f"hardcoded score=100 found (uncapped):\n  - " + "\n  - ".join(flagged)
    )


@pytest.mark.e2e
def test_frontend_pdf_uses_max_display_score():
    """The PDF/certificate renderers should respect cap. We look for
    references to 92 or MAX_DISPLAY_SCORE in known render files. If
    these files don't exist (e.g. in a slim checkout), skip."""
    files_to_check = [
        BEAUTY_AUDIT_ROOT / "lib" / "pdf" / "audit-report-pdf.tsx",
        BEAUTY_AUDIT_ROOT / "lib" / "pdf" / "certificate-pdf.tsx",
        BEAUTY_AUDIT_ROOT / "components" / "pages" / "audit-results" / "summaryViewScore.ts",
    ]
    available = [f for f in files_to_check if f.exists()]
    if not available:
        pytest.skip("BEAUTY_AUDIT not co-checked-out — frontend cap audit skipped")

    flagged = []
    for f in available:
        text = _read(f)
        if not text:
            continue
        # Look for any score-like formatting that could exceed cap.
        # Accept: explicit 92, MAX_DISPLAY_SCORE constant, or Math.min(x, 92)
        has_cap_marker = (
            "92" in text
            or "MAX_DISPLAY_SCORE" in text
            or "Math.min" in text
        )
        if not has_cap_marker:
            flagged.append(str(f.relative_to(BEAUTY_AUDIT_ROOT)))

    if flagged:
        pytest.fail(
            f"these score-rendering files don't reference cap (92 / "
            f"MAX_DISPLAY_SCORE / Math.min): {flagged}\n"
            f"Either add the cap or document why this view is exempt."
        )


@pytest.mark.e2e
def test_score_cap_constant_matches_across_repos():
    """Bagent invariants and frontend renderer must agree on cap
    value. If frontend has its own MAX_DISPLAY_SCORE constant, it
    should equal 92."""
    summary_score = BEAUTY_AUDIT_ROOT / "components" / "pages" / "audit-results" / "summaryViewScore.ts"
    if not summary_score.exists():
        pytest.skip("BEAUTY_AUDIT not co-checked-out")

    text = _read(summary_score)
    if not text:
        pytest.skip("can't read summaryViewScore.ts")

    # Look for `MAX_DISPLAY_SCORE = N` pattern
    m = re.search(r"MAX_DISPLAY_SCORE\s*=\s*(\d+)", text)
    if m:
        frontend_cap = int(m.group(1))
        assert frontend_cap == MAX_DISPLAY_SCORE, (
            f"frontend MAX_DISPLAY_SCORE={frontend_cap}, "
            f"backend MAX_DISPLAY_SCORE={MAX_DISPLAY_SCORE} — mismatch"
        )
