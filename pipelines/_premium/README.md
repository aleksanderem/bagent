# `_premium` — cold code for the next sprint

This directory holds pipelines that are NOT part of the main audit flow
today. They are fully implemented but intentionally disconnected from the
FastAPI router and the Convex orchestration layer. They will be wired up
as paid Premium add-ons in a future sprint.

## Status

- Code: working, previously used in production
- Router: NOT mounted in `server.py`
- Convex: NO internal action calls this
- Frontend: NO CTA triggers it in the current sprint
- Tests: frozen — do not modify unless you are the next-sprint owner

## Contents

### `competitor.py`

Full competitor intelligence report — 12-step pipeline producing pricing
comparisons, service gap analysis, SWOT, market segmentation, radar metrics,
strategic recommendations, and an action timeline. Writes to Supabase
`competitor_reports` (JSONB) and uses embeddings in `service_embeddings`.

Previously invoked via:

- `POST /api/competitor` FastAPI endpoint (REMOVED from router)
- `convex/competitor/reportPipeline.ts generateCompetitorReport` internal
  action (kept as legacy path during the 3-bagent migration window but not
  triggered automatically anymore)

In the new 3-bagent architecture (see `BEAUTY_AUDIT/docs/migration-plan-3-bagent.md`)
the basic competitor preview (top 3-5 competitors with distance, reviews,
price range — no SWOT) is produced by `bagent/pipelines/summary.py` as part
of BAGENT #3 (Podsumowanie tab). The full SWOT lives here and is reserved
for a dedicated "Premium Competitor Analysis" product.

## How to reactivate (next sprint)

1. Decide the commercial model (separate paid product, add-on, tier).
2. Add a FastAPI endpoint, e.g. `POST /api/premium/competitor`, with the
   same payload shape as the old `/api/competitor`.
3. Add a matching background job in `server.py` (see the removed
   `run_competitor_job` for the pattern).
4. Add a new Convex mutation `startPremiumCompetitorAnalysis` guarded by
   a credit check (separate credit type from the audit credit).
5. Add a frontend CTA in the Podsumowanie tab using the `cta_text` field
   from `audit_summaries.basic_competitor_data`.
6. Update `services/convex.py` with new webhook methods
   (premium_competitor_progress/complete/fail).

## Do NOT

- Do NOT import from `_premium` anywhere in the main flow.
- Do NOT re-add a route for `/api/competitor` without a credit check.
- Do NOT remove this directory — it's the seed for the next sprint.
