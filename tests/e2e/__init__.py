"""End-to-end pipeline tests with random salon sampling.

These tests invoke real bagent pipelines (audit, cennik, summary,
competitor_report, versum_suggest) against random salons sampled from
production Supabase, then verify deterministic invariants on the output.

The pipelines are called in-process (no HTTP, no arq queue, no Convex
webhooks, no DB writes — SupabaseService is shimmed in tests/e2e/fixtures).
This keeps tests fast (~30-60s per audit) and avoids polluting prod tables.

AI nondeterminism is intentionally NOT asserted away — instead we encode
invariants that any correct AI output must satisfy (score bounds,
schema validity, Polish ortho, cardinality matching, no English leakage,
etc.). See ``invariants.py`` for the catalog.

Pytest markers:
    @pytest.mark.integration  # requires Supabase + MiniMax credentials
    @pytest.mark.e2e          # the salon-driven E2E pipeline tests

Run all e2e:
    pytest tests/e2e -m "e2e" -v

Run via CLI runner (better for ad-hoc / CI):
    python -m tests.e2e.runner --pipeline audit --salons 5 --seed 42
"""
