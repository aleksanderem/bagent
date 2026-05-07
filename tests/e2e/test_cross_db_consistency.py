"""Cross-DB consistency check: Convex audits ↔ Supabase audit_reports.

BeautyAudit splits operational data across two DBs:
  - Convex (keen-mouse-438) — `audits`, `users`, `auditSummaries`,
    `categoryProposals` (UI state, real-time)
  - Supabase (db.booksyaudit.pl) — `audit_reports` (large blobs),
    benchmarks, competitor data

The link is `audit_reports.convex_audit_id` ⇄ `audits._id`. One bug
in mapping and reports rotted on the user side ("you have no audit").

This test samples 10 random recent Convex audits in 'done' state and
asserts:
  1. Each has a matching Supabase row by convex_audit_id
  2. user_id matches between the two
  3. status is consistent (Convex 'done' ⇒ Supabase has totalScore)

Read-only — does NOT need read_only_supabase fixture.

Usage:
    pytest tests/e2e/test_cross_db_consistency.py -v -m e2e
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import pytest

from config import settings
from services.sb_client import make_supabase_client


def _convex_query_done_audits(limit: int = 10) -> list[dict[str, Any]]:
    """Pull recent Convex audits via `npx convex run`. Returns empty
    list if Convex CLI not configured or function missing."""
    try:
        result = subprocess.run(
            [
                "npx", "convex", "run", "--prod",
                "dev:debugListRecentDoneAudits",  # may not exist — skip if so
                f'{{"limit": {limit}}}',
            ],
            capture_output=True, text=True, timeout=30,
            cwd=os.environ.get("BEAUTY_AUDIT_PATH", "/Users/alex/Desktop/MOJE_PROJEKTY/BEAUTY_AUDIT"),
        )
        if result.returncode != 0:
            return []
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        return []


@pytest.mark.integration
@pytest.mark.e2e
def test_convex_done_audits_have_supabase_rows():
    """For 10 recent Convex 'done' audits, verify each has a matching
    Supabase audit_reports row.

    Skips gracefully if the Convex helper function isn't deployed or
    Convex CLI isn't authenticated."""
    if not settings.supabase_url or not settings.supabase_service_key:
        pytest.skip("requires SUPABASE credentials")

    convex_audits = _convex_query_done_audits(limit=10)
    if not convex_audits:
        pytest.skip(
            "Convex helper dev:debugListRecentDoneAudits not available — "
            "deploy this helper to enable cross-DB consistency tests"
        )

    supa = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    convex_ids = [a["_id"] for a in convex_audits if a.get("_id")]

    rows = (
        supa.table("audit_reports")
        .select("id, convex_audit_id, user_id, total_score")
        .in_("convex_audit_id", convex_ids)
        .execute()
        .data
        or []
    )
    found_ids = {r.get("convex_audit_id") for r in rows}

    missing = [aid for aid in convex_ids if aid not in found_ids]
    assert not missing, (
        f"{len(missing)}/{len(convex_ids)} Convex audits in 'done' state "
        f"have NO Supabase audit_reports row — cross-DB consistency broken: "
        f"{missing[:3]}"
    )

    # Spot-check user_id consistency on the rows we found
    convex_by_id = {a["_id"]: a for a in convex_audits}
    user_mismatches = []
    for row in rows:
        cid = row["convex_audit_id"]
        ca = convex_by_id.get(cid)
        if ca and ca.get("userId") and row.get("user_id"):
            if ca["userId"] != row["user_id"]:
                user_mismatches.append({
                    "convex_id": cid,
                    "convex_user": ca["userId"],
                    "supa_user": row["user_id"],
                })
    assert not user_mismatches, (
        f"user_id mismatch between Convex audits.userId and Supabase "
        f"audit_reports.user_id: {user_mismatches[:3]}"
    )

    # Spot-check totalScore presence
    no_score = [r for r in rows if r.get("total_score") is None]
    assert not no_score, (
        f"{len(no_score)} Supabase rows have total_score=None despite Convex "
        f"status='done': {[r['convex_audit_id'] for r in no_score[:3]]}"
    )


@pytest.mark.integration
@pytest.mark.e2e
def test_supabase_audit_reports_have_unique_convex_ids():
    """Multiple audit_reports rows for the same convex_audit_id mean
    cleanup is broken (audits get re-saved without dedup)."""
    if not settings.supabase_url or not settings.supabase_service_key:
        pytest.skip("requires SUPABASE credentials")

    supa = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    rows = (
        supa.table("audit_reports")
        .select("convex_audit_id")
        .not_.is_("convex_audit_id", "null")
        .limit(2000)
        .execute()
        .data
        or []
    )

    seen: dict[str, int] = {}
    for r in rows:
        cid = r["convex_audit_id"]
        seen[cid] = seen.get(cid, 0) + 1
    dupes = {cid: n for cid, n in seen.items() if n > 1}
    assert not dupes, (
        f"{len(dupes)} convex_audit_id values have >1 audit_reports row — "
        f"cleanup/upsert logic broken; first 3: {list(dupes.items())[:3]}"
    )
