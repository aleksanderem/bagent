# POST `/api/audit/free_report`

Frozen free-tier snapshot of BAGENT #1 (report + content optimization).

## Intent

This endpoint exists so a future **free audit tier** can keep working on a stable, unchanging pipeline while the main `/api/audit/report` pipeline evolves through the Unified Report Pipeline plan (Etap 1, 2, 3 — see `docs/plans/2026-04-08-unified-report-pipeline.md`).

The free-tier snapshot was taken on `2026-04-08` at commit `47220b7` and mirrors the then-current behavior of `pipelines/report.py`. Its implementation lives in `pipelines/free_report.py` which is a byte-compatible copy. The rule is simple:

> `pipelines/report.py` evolves. `pipelines/free_report.py` does not.

If the frozen snapshot becomes stale enough to be a problem (e.g., main pipeline gains a critical bug fix the free tier should also benefit from), take a **new** snapshot at a later commit by copying `report.py` to `free_report.py` again and bumping the "frozen at commit X" header. Never hand-edit `free_report.py` in place.

## Relationship to `/api/audit/report`

| Aspect | `/api/audit/report` | `/api/audit/free_report` |
|---|---|---|
| Pipeline module | `pipelines/report.py` | `pipelines/free_report.py` (frozen) |
| Public function | `run_audit_pipeline` | `run_free_report_pipeline` |
| Background job | `run_report_job` | `run_free_report_job` |
| Request model | `ReportRequest` | `FreeReportRequest` (empty subclass) |
| Convex webhooks | `/api/audit/report/progress/complete/fail` | same (reused intentionally) |
| Supabase tables | `audit_reports` + children | same (no tier column yet) |
| Job store `type` | `"report"` | `"free_report"` |
| Evolving | yes (Etap 1–3) | no, frozen |

Both endpoints write to the same Supabase tables with the same schema. Convex currently cannot distinguish a free-tier call from a paid-tier call — that product-level distinction will be wired in later, likely via an explicit `tier` field on `FreeReportRequest` plus a routing decision in Convex webhook handlers.

## Request

```http
POST /api/audit/free_report HTTP/1.1
Content-Type: application/json
x-api-key: <BAGENT_API_KEY>

{
  "auditId": "j57d37db8e4695c1b3yqmtmtws84f385",
  "userId": "jh719yjx33t0jfehn067prrjp17ze2na",
  "sourceUrl": "https://booksy.com/pl-pl/12345_salon",
  "scrapedData": null
}
```

Fields:

- `auditId` (required, string): Convex audit row id. Used as primary key for Supabase writes and webhook correlation.
- `userId` (required, string): Convex user id. Stored on the audit report for ownership.
- `sourceUrl` (optional, string): original Booksy URL, persisted to `audit_reports.source_url` for traceability.
- `scrapedData` (optional, object): inline scrape payload for dev/testing. When `null`, the job loads scraped data from Supabase `salon_scrapes` + `salon_scrape_services` (falls back to legacy `audit_scraped_data` for pre-migration audits) by `auditId`.

## Response

```json
{
  "jobId": "9596d229-0eea-46ba-8ee4-1e1436b3f7fa",
  "status": "accepted"
}
```

HTTP `202 Accepted`. The job runs as a FastAPI `BackgroundTask` — progress is observable via the `/api/jobs`, `/api/jobs/{id}/logs`, and `/api/events` SSE endpoints on the bagent dashboard, and via Convex webhooks that fire during the run.

Authentication: `x-api-key` header must match `settings.api_key` (populated from the `API_KEY` env var). Missing or wrong key returns `401`.

## What the pipeline produces

The pipeline runs 10 steps (see `pipelines/free_report.py:run_free_report_pipeline`):

1. **Stats** — `calculate_audit_stats` on the raw scrape (service count, category count, description coverage, duration coverage, duplicate detection, oversized categories).
2. **Naming score + agent** — `naming_score.txt` prompt scores naming quality 0–20; `naming_agent.txt` runs a tool-use loop (`submit_naming_results`) to propose per-service name rewrites. Validated by `validate_name_transformation`.
3. **Descriptions score + agent** — analogous for descriptions, scored 0–20.
4. **Structure analysis** — `structure.txt` prompt scores structure + pricing (each 0–15) and produces `issues`, `quickWins`, `missingSeoKeywords`.
5. **Scoring** — `completeness`, `naming`, `descriptions`, `structure`, `pricing`, `seo`, `ux` combined into `totalScore` with critical-issue capping.
6. **Summary text** — `summary.txt` prompt writes a 2–3 sentence human-readable summary.
7. **Industry benchmarks** — Supabase `benchmarks` table lookup scoped by city and primary Booksy category, falling back to national and hardcoded defaults.
8. **Quality validation** — `_validate_quality` runs 7 business checks (score range, issues present, transformation sanity, score consistency, summary present, stats match, severity not inflated).
9. **Report assembly** — `report` dict with `version`, `totalScore`, `scoreBreakdown`, `stats`, `topIssues`, `transformations`, `missingSeoKeywords`, `quickWins`, `industryComparison`, `summary`.
10. **Persistence** — `save_report` writes `audit_reports` + `audit_issues` + `audit_transformations` + `audit_seo_keywords` + `audit_quick_wins` (same tables as the paid pipeline).

## What the pipeline does NOT produce

Deliberate omissions that define the free-tier scope:

- **No new pricelist.** Categories stay as-is, services stay in their original categories. The frozen pipeline does not run the category restructuring agent (that moved into the paid `/api/audit/report` path in Etap 1).
- **No `category_mapping` field** in the saved report.
- **No traceability linkage** between `topIssues` and `transformations` (added to paid path in Etap 2).
- **No guaranteed coverage** — naming and descriptions agents skip services judged "already good" (Etap 3 removes this escape in the paid path only).
- **No competitor context.** Neither competitor snippets nor competitive benchmarks. Those are the BAGENT #3 (`/api/audit/summary`) responsibility and require user consent to scrape competitors.
- **No optimized pricelist write.** `optimized_pricelists` is only written by `/api/audit/cennik`, which is not auto-triggered by a free-report run (Convex does auto-trigger for paid reports — this behavior difference will need explicit handling when the free tier is wired into the product).

## Performance & cost

At snapshot time, typical run on a Polish salon with 200–300 services:

- **Latency**: ~60–180 seconds wall-clock (naming + descriptions agents run in parallel inside `_asyncio.gather`).
- **Token cost**: ~40k–80k MiniMax M2.7 input + ~10k–20k output, dominated by naming and description agent loops.
- **External calls**: 1× MiniMax naming_score, 1× MiniMax descriptions_score, 1× MiniMax structure, 1× MiniMax summary, 5–15× MiniMax naming agent tool calls, 5–15× MiniMax descriptions agent tool calls, 1–2× Supabase reads (scraped data, benchmarks), 5–10× Supabase writes (audit_reports + children).

The frozen pipeline inherits whatever flakiness and cost characteristics the paid pipeline had on 2026-04-08. Any improvements to reliability, batching, or efficiency in `pipelines/report.py` after the snapshot will NOT reach `pipelines/free_report.py`.

## Stability contract

This endpoint and its pipeline behavior are **frozen**. You can rely on:

- The JSON request and response shapes never changing.
- The Supabase table writes never changing schema.
- The set of MiniMax prompts used (`naming_score.txt`, `naming_agent.txt`, `descriptions_score.txt`, `descriptions_agent.txt`, `structure.txt`, `summary.txt`) never changing.
- The `run_free_report_pipeline` public signature `(scraped_data, audit_id, on_progress)` never changing.

The only reason to touch `pipelines/free_report.py` is a clean re-snapshot (`cp pipelines/report.py pipelines/free_report.py` + re-apply header + rename public function). Anything else is a bug in the plan.

## Testing

Smoke tests live in `tests/test_free_report.py` and cover:

- importability of `run_free_report_pipeline`
- signature parity `(scraped_data, audit_id, on_progress)`
- presence of all `_` helpers the frozen pipeline depends on
- `/api/audit/free_report` route registered in the FastAPI app
- 401/422 on missing `x-api-key`
- 202 + `jobId` on valid request (background task patched to no-op)
- `type="free_report"` tag in the in-memory job store

Run with:

```bash
pytest tests/test_free_report.py -v
```

These are intentionally lightweight. Full pipeline regression is guaranteed by the byte-compatible copy of `report.py` at snapshot time, not by re-running the whole flow in every test.
