"""Testy FUNNEL_AUDIT R5 — copy kampanii z wniosków audytu.

Pokrywają czystą część (extract_audit_insights); ścieżka LLM ma fallback
na szablon i jest weryfikowana operacyjnie (notes + copySource w proposalu).
"""

from pipelines.campaign_setup import extract_audit_insights


def test_none_report_gives_none():
    assert extract_audit_insights(None) is None


def test_empty_report_gives_none():
    assert extract_audit_insights({}) is None


def test_non_dict_gives_none():
    assert extract_audit_insights("not a dict") is None  # type: ignore[arg-type]


def test_full_report_extracts_compact_insights():
    report = {
        "totalScore": 61,
        "marketPosition": {"pricePosition": "premium", "percentile": 78},
        "quickWins": [
            {"title": "Dodaj opisy do 12 usług", "description": "..."},
            {"title": "Skonsoliduj duplikaty laserów"},
            {"notTitle": "ignorowane"},
        ],
        "summary": "Salon premium z chaosem cenowym." + "x" * 700,
        "topIssues": [{"issue": "nie powinno wejść do insightów"}],
    }
    insights = extract_audit_insights(report)
    assert insights is not None
    assert insights["totalScore"] == 61
    assert insights["marketPosition"]["pricePosition"] == "premium"
    # tylko elementy z title, w kolejności
    assert insights["quickWins"] == [
        "Dodaj opisy do 12 usług",
        "Skonsoliduj duplikaty laserów",
    ]
    # summary przycięte do 600 znaków
    assert len(insights["summary"]) == 600
    assert "topIssues" not in insights


def test_report_without_signal_gives_none():
    # Raport istnieje, ale nie ma nic, co zabarwi copy
    assert extract_audit_insights({"totalScore": 40, "quickWins": [], "summary": ""}) is None


def test_quick_wins_capped_at_four():
    report = {
        "quickWins": [{"title": f"QW{i}"} for i in range(10)],
    }
    insights = extract_audit_insights(report)
    assert insights is not None
    assert insights["quickWins"] == ["QW0", "QW1", "QW2", "QW3"]
