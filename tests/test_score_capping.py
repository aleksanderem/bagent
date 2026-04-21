"""Tests for cap_audit_score — Bug 2 regression guard.

Base Booksy audit cannot exceed 88/100 regardless of issue count.
The remaining 12 points are reserved for marketing + competitor
positioning pillars (separate paid products).
"""

from __future__ import annotations

from pipelines.helpers import MAX_AUDIT_SCORE, cap_audit_score


class TestHardCeiling:
    """MAX_AUDIT_SCORE (88) applies regardless of how clean the audit is."""

    def test_perfect_score_100_capped_to_88(self) -> None:
        assert cap_audit_score(
            total_score=100,
            critical_count=0,
            major_count=0,
            all_issues_count=0,
        ) == MAX_AUDIT_SCORE

    def test_no_issues_score_99_capped_to_88(self) -> None:
        assert cap_audit_score(
            total_score=99,
            critical_count=0,
            major_count=0,
            all_issues_count=0,
        ) == MAX_AUDIT_SCORE

    def test_score_at_cap_passes_through(self) -> None:
        assert cap_audit_score(
            total_score=88,
            critical_count=0,
            major_count=0,
            all_issues_count=0,
        ) == 88

    def test_score_below_cap_passes_through(self) -> None:
        assert cap_audit_score(
            total_score=72,
            critical_count=0,
            major_count=0,
            all_issues_count=0,
        ) == 72

    def test_max_audit_score_constant_is_88(self) -> None:
        assert MAX_AUDIT_SCORE == 88


class TestIssueBasedCaps:
    """Issue-based caps still apply before the hard ceiling."""

    def test_3_critical_caps_at_60(self) -> None:
        assert cap_audit_score(
            total_score=80,
            critical_count=3,
            major_count=0,
            all_issues_count=3,
        ) == 60

    def test_1_critical_caps_at_75(self) -> None:
        assert cap_audit_score(
            total_score=85,
            critical_count=1,
            major_count=0,
            all_issues_count=1,
        ) == 75

    def test_1_major_caps_at_85(self) -> None:
        # 85 from major cap is below MAX_AUDIT_SCORE, so final is 85
        assert cap_audit_score(
            total_score=90,
            critical_count=0,
            major_count=1,
            all_issues_count=1,
        ) == 85

    def test_any_issue_caps_at_95_then_hard_ceiling(self) -> None:
        # 95 from issue cap is above MAX_AUDIT_SCORE, so final is 88
        assert cap_audit_score(
            total_score=100,
            critical_count=0,
            major_count=0,
            all_issues_count=5,  # some minor issue
        ) == MAX_AUDIT_SCORE


class TestBeauty4everScenario:
    """Real-world data point: Beauty4ever audit scored 50/100."""

    def test_preserves_low_scores(self) -> None:
        assert cap_audit_score(
            total_score=50,
            critical_count=1,
            major_count=2,
            all_issues_count=3,
        ) == 50


class TestEdgeCases:
    def test_zero_score(self) -> None:
        assert cap_audit_score(
            total_score=0,
            critical_count=0,
            major_count=0,
            all_issues_count=0,
        ) == 0

    def test_score_below_first_cap_untouched(self) -> None:
        # Even with 3 critical, if total_score < 60 we don't lift it to 60
        assert cap_audit_score(
            total_score=30,
            critical_count=3,
            major_count=0,
            all_issues_count=3,
        ) == 30

    def test_critical_and_major_both_present_uses_critical_cap(self) -> None:
        # Critical takes precedence over major
        assert cap_audit_score(
            total_score=95,
            critical_count=1,
            major_count=2,
            all_issues_count=3,
        ) == 75
