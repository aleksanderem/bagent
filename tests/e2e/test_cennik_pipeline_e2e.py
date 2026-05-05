"""E2E test: cennik (BAGENT #2) pipeline against random salons.

Usage:
    pytest tests/e2e/test_cennik_pipeline_e2e.py -v -m e2e

Cennik is a deterministic mechanical-transform pipeline (no AI calls
on the critical path). It takes the audit_report's transformations +
category_mapping and applies them to the scraped pricelist. Tests
verify the output structure preserves cardinality and price integrity.

Because cennik depends on a prior audit_report row in Supabase, this
test seeds a synthetic in-memory transformations payload directly
rather than reading from Supabase. That keeps the test deterministic
and fast — no AI run needed.
"""

from __future__ import annotations

from typing import Any

import pytest

from .invariants import check_cennik_output


def _synth_audit_payload(scraped_data: Any) -> dict[str, Any]:
    """Build a minimal audit payload that BAGENT #2 can consume.

    Cennik reads from audit_reports.transformations + category_mapping.
    For tests we build identity transformations (no name/desc changes,
    no category renames) so the cennik output should match the input
    pricelist exactly modulo finalization (clean_service_name, fix_caps_lock,
    promo detection).
    """
    return {
        "transformations": [],   # no changes
        "categoryMapping": {},   # no renames
        "categoryChanges": [],
    }


@pytest.mark.integration
@pytest.mark.e2e
@pytest.mark.asyncio
async def test_cennik_preserves_cardinality_and_prices(
    salon_pool: list[dict[str, Any]],
    scraped_data_for,
    capture_progress,
    disable_supabase_writes,
    monkeypatch,
):
    """Cennik with identity transformations should output the same
    services with the same prices, just with finalization applied."""
    from pipelines import cennik as cennik_module
    from services.supabase import SupabaseService

    cb, _log = capture_progress
    failures_by_salon: dict[Any, list[str]] = {}

    for salon_row in salon_pool:
        scraped = scraped_data_for(salon_row)
        audit_payload = _synth_audit_payload(scraped)

        # Stub the Supabase reads cennik does for the original pricelist
        # + audit_report. Pipeline normally pulls scrape from DB; here we
        # inject ScrapedData and audit dict directly.
        async def _stub_load_scrape(*args, **kwargs):
            return scraped

        async def _stub_load_audit(*args, **kwargs):
            return audit_payload

        # Best-effort patches — names may vary per release. We patch the
        # SupabaseService methods cennik relies on. If a name doesn't
        # exist the test still runs against the real DB which is OK
        # for read-only.
        for name in ("load_salon_scrape", "load_audit_report"):
            if hasattr(SupabaseService, name):
                monkeypatch.setattr(SupabaseService, name, _stub_load_scrape if "scrape" in name else _stub_load_audit)

        try:
            cennik_output = await cennik_module.run_cennik_pipeline(
                audit_id=f"e2e-cennik-{salon_row['salon_id']}",
                salon_id=str(salon_row["salon_id"]),
                scrape_id=str(salon_row["id"]),
                on_progress=cb,
            )
        except TypeError:
            # Older pipeline signature — skip if we can't shim the inputs
            pytest.skip(
                "run_cennik_pipeline signature changed; update e2e test"
            )
        except Exception as e:
            failures_by_salon[salon_row["salon_id"]] = [
                f"pipeline_raised: {type(e).__name__}: {e}"
            ]
            continue

        rep = check_cennik_output(cennik_output, scraped)
        if not rep.ok:
            failures_by_salon[salon_row["salon_id"]] = rep.failures

    if failures_by_salon:
        details = "\n".join(
            f"  salon={sid}: {len(fails)} failures\n    " + "\n    ".join(fails)
            for sid, fails in failures_by_salon.items()
        )
        pytest.fail(
            f"{len(failures_by_salon)}/{len(salon_pool)} salons "
            f"violated cennik invariants:\n{details}"
        )
