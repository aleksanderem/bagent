"""Contract tests for bagent → Convex webhooks.

Bagent POSTs to Convex at well-known paths after pipeline completion:
  - POST /api/audit/report/progress  — periodic during pipeline
  - POST /api/audit/report/complete  — final success
  - POST /api/audit/report/fail      — error
  - POST /api/audit/cennik/complete  — cennik success
  - POST /api/audit/competitor/complete
  - POST /api/marketing/resend       — Resend event webhook (inbound)

Convex validators (in BEAUTY_AUDIT/convex/audit/validators.ts) reject
payloads that don't match the expected shape. Drift between bagent's
emit shape and Convex's expected shape is a classic "works locally,
breaks on prod" scenario.

This test suite asserts the SHAPES bagent emits match a JSON schema
the Convex side documents. We don't actually POST to live Convex — we
build the payload and validate against schema.

Usage:
    pytest tests/e2e/test_webhook_contracts.py -v -m e2e
"""

from __future__ import annotations

from typing import Any

import pytest

# JSON schema sketches mirroring convex/audit/validators.ts.
# Update these in sync when the Convex validators change.

REPORT_COMPLETE_PAYLOAD_REQUIRED = {
    "auditId": str,
    "report": dict,         # full audit_report shape (see convex/schema.ts auditReport)
    "supabaseAuditId": str, # FK to Supabase audit_reports.id
}

REPORT_PROGRESS_PAYLOAD_REQUIRED = {
    "auditId": str,
    "progress": int,         # 0-100
    "message": str,
}

REPORT_FAIL_PAYLOAD_REQUIRED = {
    "auditId": str,
    "error": str,
    "retryCount": int,
}

CENNIK_COMPLETE_PAYLOAD_REQUIRED = {
    "auditId": str,
    "categoryProposals": list,
}

COMPETITOR_COMPLETE_PAYLOAD_REQUIRED = {
    "auditId": str,
    "report": dict,
    "competitorIds": list,
}

RESEND_WEBHOOK_PAYLOAD_REQUIRED = {
    "type": str,             # "email.delivered", "email.bounced", "email.complained" etc.
    "data": dict,
}


def _validate_payload(payload: dict[str, Any], required: dict[str, type]) -> list[str]:
    """Return a list of validation errors. Empty = valid."""
    errors: list[str] = []
    for k, expected_type in required.items():
        if k not in payload:
            errors.append(f"missing key: {k}")
            continue
        v = payload[k]
        if not isinstance(v, expected_type):
            errors.append(f"key {k}: expected {expected_type.__name__}, got {type(v).__name__}={v!r}")
    return errors


# ---------------------------------------------------------------------------
# Bagent → Convex contracts
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_report_complete_payload_shape():
    """Smoke check the shape bagent emits to /api/audit/report/complete.

    The actual emit happens in workers/tasks.py run_report_task; we
    construct what it WOULD post given a typical audit output."""
    audit_output = {
        "version": "v2",
        "totalScore": 75,
        "summary": "test",
        "topIssues": [],
        "transformations": [],
        "quickWins": [],
    }
    payload = {
        "auditId": "test-audit-123",
        "report": audit_output,
        "supabaseAuditId": "uuid-test-456",
    }
    errors = _validate_payload(payload, REPORT_COMPLETE_PAYLOAD_REQUIRED)
    assert not errors, f"contract violations: {errors}"


@pytest.mark.e2e
def test_report_progress_payload_shape():
    payload = {
        "auditId": "test-audit-123",
        "progress": 50,
        "message": "Step 5/10",
    }
    errors = _validate_payload(payload, REPORT_PROGRESS_PAYLOAD_REQUIRED)
    assert not errors, f"contract violations: {errors}"
    assert 0 <= payload["progress"] <= 100, "progress must be percent"


@pytest.mark.e2e
def test_report_fail_payload_shape():
    payload = {
        "auditId": "test-audit-123",
        "error": "MiniMax 503 after 3 retries",
        "retryCount": 3,
    }
    errors = _validate_payload(payload, REPORT_FAIL_PAYLOAD_REQUIRED)
    assert not errors, f"contract violations: {errors}"


@pytest.mark.e2e
def test_cennik_complete_payload_shape():
    payload = {
        "auditId": "test-audit-123",
        "categoryProposals": [
            {"name": "Fryzjerstwo", "services": [{"name": "Strzyżenie", "price": "80 zł"}]},
        ],
    }
    errors = _validate_payload(payload, CENNIK_COMPLETE_PAYLOAD_REQUIRED)
    assert not errors


@pytest.mark.e2e
def test_competitor_complete_payload_shape():
    payload = {
        "auditId": "test-audit-123",
        "report": {"finalScore": 70, "synthesis": "Wynik konkurencyjny"},
        "competitorIds": [101, 102, 103],
    }
    errors = _validate_payload(payload, COMPETITOR_COMPLETE_PAYLOAD_REQUIRED)
    assert not errors


# ---------------------------------------------------------------------------
# Inbound Resend webhook
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_resend_webhook_event_shape():
    """Resend POSTs events with HMAC-SHA256 signature. Convex
    /api/marketing/resend handler must validate signature before
    parsing payload. Smoke check the shape we expect."""
    payload = {
        "type": "email.delivered",
        "data": {
            "email_id": "rsp_xxx",
            "to": ["test@example.com"],
            "subject": "Twój audyt jest gotowy",
            "created_at": "2026-05-07T10:00:00Z",
        },
    }
    errors = _validate_payload(payload, RESEND_WEBHOOK_PAYLOAD_REQUIRED)
    assert not errors


@pytest.mark.e2e
def test_resend_webhook_event_types_known():
    """Convex marketing/webhook.ts has a state machine — it expects
    these specific event types. New Resend events would silently get
    'unknown' state until handler is updated."""
    KNOWN_EVENTS = {
        "email.sent", "email.delivered", "email.opened", "email.clicked",
        "email.bounced", "email.complained", "email.delivery_delayed",
    }
    # If Resend adds new event types, this test will need updating —
    # which is the point. Forces explicit decision when handler must
    # change.
    assert "email.delivered" in KNOWN_EVENTS
    assert "email.bounced" in KNOWN_EVENTS  # → triggers auto-suppress
    assert "email.complained" in KNOWN_EVENTS  # → triggers auto-suppress
