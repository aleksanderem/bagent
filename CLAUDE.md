# bagent — Beauty Audit AI Analyzer

Python FastAPI service that runs AI analysis pipelines for BeautyAudit salons using MiniMax M2.7 with tool_use agent loops. Replaces Convex-side AI analysis which had a 600s timeout limit. Deployed alongside bextract on api.booksyaudit.pl.

Three pipelines: audit analysis (10 steps), competitor report (12 steps), pricelist optimization (8 steps). All share the same agent loop, MiniMax client, job store, SSE dashboard, and Convex webhook infrastructure.

## Architecture

```
                        ┌─────────────────────────┐
                        │      Convex (frontend)   │
                        └──────────┬──────────────┘
                                   │
              ┌────────────────────┼───────────────────┐
              │                    │                    │
    POST /api/analyze    POST /api/competitor    POST /api/optimize
              │                    │                    │
              ▼                    ▼                    ▼
        Audit Pipeline     Competitor Pipeline    Optimization Pipeline
        (10 steps)         (12 steps)             (8 steps)
              │                    │                    │
              ▼                    ▼                    ▼
          Supabase            Supabase              Supabase
       (audit_reports)    (competitor_reports)   (optimized_pricelists)
              │                    │                    │
              └────────────────────┼───────────────────┘
                                   │
                          Convex HTTP webhook
                        (progress/complete/fail)
```

Each endpoint returns 202 immediately with a jobId. The pipeline runs as a FastAPI background task with no time limit. Progress is pushed to: the in-memory JobStore (for dashboard SSE), and Convex webhooks (for frontend progress bar). On completion, the report is saved to Supabase, then Convex is notified (best-effort — job is marked completed before the webhook call).

## Tech Stack

Python 3.12+, FastAPI, Pydantic v2, anthropic SDK (wrapping MiniMax M2.7's Anthropic-compatible API), supabase-py, httpx, sse-starlette (SSE for live dashboard), uv for dependency management.

## Project Layout

```
server.py              → FastAPI app, 3 pipeline endpoints + monitoring endpoints + 3 background job runners
config.py              → Pydantic Settings from .env
job_store.py           → Job, LogEntry, JobStore (in-memory tracking + SSE pub/sub + cancel support)
test_local.py          → Local test script: converts booksy JSON → ScrapedData, tests MiniMax calls

agent/
  runner.py            → Multi-turn tool_use agent loop (run_agent_loop)
  tools.py             → 6 tool definitions: NAMING_TOOL, DESCRIPTION_TOOL, MARKET_ANALYSIS_TOOL,
                         STRATEGIC_ANALYSIS_TOOL, CATEGORY_MAPPING_TOOL, OPTIMIZED_SERVICES_TOOL

pipelines/
  audit.py             → 10-step audit pipeline (run_audit_pipeline)
  competitor.py        → 12-step competitor report pipeline (run_competitor_pipeline)
  optimization.py      → 8-step optimization pipeline (run_optimization_pipeline)
  helpers.py           → Pure functions: stats, scoring, text builders, validation

services/
  minimax.py           → MiniMaxClient (AsyncAnthropic wrapper) + with_retry
  supabase.py          → SupabaseService (scraped data, reports, benchmarks, geocoding, salon loading, RPCs)
  convex.py            → ConvexClient (progress/complete/fail webhooks)

models/
  scraped_data.py      → ScrapedData, ScrapedCategory, ScrapedService, ServiceVariant
  report.py            → EnhancedAuditReport, AuditStats, AuditIssue, ScoreBreakdown, etc.
  analysis.py          → NamingAnalysisResult, DescriptionAnalysisResult, QualityReport
  competitor.py        → CompetitorReportData, CompetitorRequest, MarketSummary, SwotAnalysis, etc. (19 models)
  optimization.py      → OptimizationRequest, OptimizationResult, ChangeRecord, OptimizedService, etc. (8 models)

prompts/               → 12 Polish-language prompt templates (.txt), loaded via _load_prompt()
  naming_score.txt, naming_agent.txt           → audit: naming analysis
  descriptions_score.txt, descriptions_agent.txt → audit: description analysis
  structure.txt, summary.txt                   → audit: structure/pricing + summary
  competitor_market.txt                        → competitor: market analysis agent
  competitor_swot.txt                          → competitor: SWOT (single JSON call)
  competitor_strategy.txt                      → competitor: strategic analysis agent
  optimization_categories.txt                  → optimization: category restructuring agent
  optimization_services.txt                    → optimization: service optimization agent
  optimization_verify.txt                      → optimization: quality verification

templates/
  dashboard.html       → Single-file monitoring dashboard (HTML+CSS+JS, SSE-powered, cancel button)

tests/                 → pytest suite, asyncio_mode=auto, 124 unit + 10 integration tests
```

## API Endpoints

All endpoints defined in `server.py`.

**Authenticated (x-api-key header via FastAPI Depends):**

`POST /api/analyze` (202) — audit pipeline. Accepts `{auditId, userId, sourceUrl?, scrapedData}`. Creates job, launches `run_analysis_job` background task. Returns `{jobId, status: "accepted"}`.

`POST /api/competitor` (202) — competitor report pipeline. Accepts `{auditId, userId, subjectSalonId, salonName, salonCity, salonLat?, salonLng?, selectedCompetitorIds, services}`. Creates job with `meta.type = "competitor"`, launches `run_competitor_job`. Returns `{jobId, status: "accepted"}`.

`POST /api/optimize` (202) — optimization pipeline. Accepts `{auditId, userId, pricelistId, jobId, scrapedData, auditReport, selectedOptions, promptTemplates?}`. Creates job with `meta.type = "optimization"`, launches `run_optimization_job`. Returns `{jobId, status: "accepted"}`.

`POST /api/jobs/{job_id}/cancel` — request cancellation of a running job. Returns `{jobId, status: "cancel_requested"}`. The pipeline checks `job.cancel_requested` at each `on_progress()` callback and raises `CancelledError` if set.

**Public (no auth, internal monitoring):**

`GET /api/jobs` — list all tracked jobs as summaries (running first, then newest).
`GET /api/jobs/{job_id}` — single job summary, 404 if not found.
`GET /api/jobs/{job_id}/logs` — full job detail including all log entries.
`GET /api/events` — SSE stream pushing `job_created`, `job_progress`, `job_completed`, `job_failed`, `job_cancelled` events.
`GET /api/health` — returns `{status: "ok", jobs_running: N, jobs_total: N}`.
`GET /dashboard` — serves monitoring dashboard HTML.

## Pipeline 1: Audit (pipelines/audit.py)

Input: `ScrapedData` (Booksy pricelist). Output: `EnhancedAuditReport` (score 0-100, issues, transformations, quick wins, SEO, summary).

1. `calculate_audit_stats()` — deterministic stats (30+ metrics), no AI
2. Naming score — `client.generate_json()` with `naming_score.txt`, returns 0-20 + issues
3. Naming transformations — `run_agent_loop()` with `NAMING_TOOL`, M2.7 calls `submit_naming_results` in batches of 15-20. Post-processing: `clean_service_name()`, `fix_caps_lock()`, `validate_name_transformation()`. Logs accepted vs rejected count.
4. Description score — same pattern, `descriptions_score.txt`
5. Description transformations — `run_agent_loop()` with `DESCRIPTION_TOOL`
6. Structure & pricing — `client.generate_json()` with `structure.txt`, returns structureScore (0-15), pricingScore (0-15), quickWins, missingSeoKeywords
7. Summary — `client.generate_text()` with `summary.txt`, 2-3 sentences in Polish
8. Competitor context — Supabase queries (geocode_salon, get_competitors, get_benchmarks)
9. Quality validation — 7 weighted business checks, accept if score >= 0.7
10. Assemble report — combine all results, apply progressive score capping

Score capping: >=3 critical issues cap at 60, any critical cap at 75, any major cap at 85, any issues cap at 95.

Supabase save: parent row in `audit_reports` (total_score, score_breakdown, stats, summary, etc.) + child rows in `audit_issues`, `audit_transformations`, `audit_seo_keywords`, `audit_quick_wins`, `audit_competitors`. All upserted by `convex_audit_id`.

## Pipeline 2: Competitor Report (pipelines/competitor.py)

Input: subject salon ID, competitor IDs, services list. Output: `CompetitorReportData` (17 sections).

Steps 1-2 (Supabase data loading):
1. Load subject salon via `supabase.get_salon_with_services(subject_salon_id)` — salon metadata + services
2. Load each competitor salon the same way. Log how many loaded successfully.

Steps 3-7 (Supabase RPCs, no AI):
3. `get_competitor_price_comparison` → price positioning per category (below_p25, p25_p50, etc.)
4. `get_local_salon_rank` → rank/total in city, percentile
5. `get_service_gap_matrix` → which services competitors have that salon doesn't
6. `get_competitor_promotions` → active discounts across competitors
7. `get_per_service_pricing` → per-service price comparison with percentile

Each RPC call uses `supabase.call_rpc()` wrapped in try/except with fallback to empty list. Results are formatted as readable text blocks via `_format_price_comparison()`, `_format_local_ranking()`, `_format_service_gaps()`, `_format_promotions()`, `_format_per_service()` for AI context.

Steps 8-10 (AI analysis):
8. Market analysis — agent loop with `MARKET_ANALYSIS_TOOL`. All RPC data injected as context. Produces: marketSummary (headline + paragraphs + keyInsights), competitive advantages (win/loss/neutral per area), recommendations (with estimated PLN impact), radarMetrics.
9. SWOT — single `client.generate_json()` with `competitor_swot.txt`. Produces: strengths, weaknesses, opportunities, threats (3-5 each).
10. Strategic analysis — agent loop with `STRATEGIC_ANALYSIS_TOOL`. May call tool multiple times. Produces: marketNiches, actionPlan (timeline + priority items).

Steps 11-12:
11. Assemble full report dict matching `CompetitorReportData` shape.
12. Return (server.py saves to `competitor_reports` table via `supabase.save_competitor_report()`).

Estimated: ~100K tokens, 5-8 minutes, ~$0.10-0.15 per report.

## Pipeline 3: Optimization (pipelines/optimization.py)

Input: `ScrapedData` + `EnhancedAuditReport` + `selectedOptions`. Output: `OptimizationResult` (optimized pricelist + changes + quality score).

CRITICAL INVARIANT: prices, durations, and variants are NEVER changed. Triple protection: prompt rules, post-processing validation, unit tests.

1. Context loading — extract from audit report: topIssues, transformations, missingSeoKeywords, quickWins, stats. These are injected into all AI prompts.
2. Category restructuring (conditional: only if "categories" in selectedOptions) — agent loop with `CATEGORY_MAPPING_TOOL`. Input: pricelist + structure issues. Output: list of category rename/merge/split proposals.
3. Service optimization (core step) — agent loop with `OPTIMIZED_SERVICES_TOOL`. Processes in batches of 15-20. For each service: new name (max 80 chars), new description (50-200 chars, benefit-focused), tags (max 2: Bestseller, Nowość, Premium, Promocja), sortOrder. Post-processing validates price/duration/variant immutability, applies `clean_service_name()` and `fix_caps_lock()`.
4. SEO enrichment (conditional: only if "seo" in selectedOptions) — reuses `OPTIMIZED_SERVICES_TOOL` with SEO-focused prompt. Uses `missingSeoKeywords` from audit as input.
5. Programmatic fixes — no AI: `fix_caps_lock()`, `clean_service_name()`, deduplicate services, detect promo services via `_detect_promo()` (checks for "promocja", "rabat", "zniżka", etc.).
6. Quality verification — single `client.generate_json()` with `optimization_verify.txt`. Input: original audit issues + optimized pricelist. Output: `{fixed: [...], remaining: [...], qualityScore: 0-100}`.
7. Assembly — builds `optimizedPricingData` (same structure as original pricelist but with optimized names/descriptions/tags), `changes` (list of ChangeRecord with type/before/after/reason), `summary` (counts), `qualityScore`.
8. Return (server.py saves via `supabase.save_optimized_pricelist()`).

The `_find_optimization()` helper matches services by name (case-insensitive, with partial match fallback). Building `optimizedPricingData` iterates over original categories/services and applies optimizations while preserving original price/duration/variants/imageUrl.

Estimated: ~80-110K tokens, 8-15 minutes (depends on salon size), ~$0.08-0.15 per optimization.

## Agent Loop (agent/runner.py)

The `run_agent_loop()` function implements multi-turn tool_use with MiniMax M2.7:

1. Send user message + tool definitions
2. M2.7 responds with tool_use blocks (batch of transformations)
3. Append FULL assistant response to messages (critical for interleaved thinking — ThinkingBlock + TextBlock + ToolUseBlock all preserved)
4. Send tool_result acknowledgments ("OK. Kontynuuj przetwarzanie pozostałych usług.")
5. M2.7 continues until `stop_reason="end_turn"` or max_steps reached (default 30)
6. Return `AgentResult` with `tool_calls`, `final_text`, `total_steps`, `total_input_tokens`, `total_output_tokens`

The model self-decomposes work into batches. 204K context window means full pricelist fits in one conversation.

## Tool Definitions (agent/tools.py)

6 tools, all Anthropic tool_use format with `input_schema`:

Audit tools:
- `submit_naming_results` — batch of {name, improved} pairs for service name transformations
- `submit_description_results` — batch of {serviceName, newDescription} pairs

Competitor tools:
- `submit_market_analysis` — marketSummary, advantages, recommendations, radarMetrics in one call
- `submit_strategic_analysis` — marketNiches, actionPlan (may be called multiple times)

Optimization tools:
- `submit_category_mapping` — list of {originalCategory, newCategory, services, reason}
- `submit_optimized_services` — batch of {originalName, categoryName, newName, newDescription?, tags?, sortOrder?}

## MiniMax Client (services/minimax.py)

Thin wrapper around `anthropic.AsyncAnthropic`. MiniMax M2.7 uses Anthropic-compatible API at `https://api.minimax.io/anthropic`.

- `create_message(system, messages, tools, max_tokens, temperature)` — raw call with tools (for agent loops). Returns `anthropic.types.Message`. IMPORTANT: `response.content` may contain ThinkingBlock (no `.text` attr) — always filter for `block.type == "text"`.
- `generate_json(prompt, system, max_tokens)` — single prompt → parsed JSON. Handles ThinkingBlock by iterating content for text block. Falls back to regex extraction from markdown code blocks.
- `generate_text(prompt, system, max_tokens)` — single prompt → text string.

`with_retry(fn, max_attempts, base_delay, max_delay)` — exponential backoff (2s base, 10s max, 2 attempts). Skips retries for auth errors (401/403).

## Supabase Service (services/supabase.py)

Uses `supabase-py` SDK with `ClientOptions(headers={"ngrok-skip-browser-warning": "true"})` (needed because Supabase URL goes through ngrok in dev).

Audit methods: `get_scraped_data(convex_audit_id)` — reads from `audit_scraped_data`, reconstructs ScrapedData shape from normalized columns (salon_name, categories_json, etc.). `save_report(...)` — upserts parent in `audit_reports` + inserts children in `audit_issues`, `audit_transformations`, `audit_seo_keywords`, `audit_quick_wins`, `audit_competitors`. All by `convex_audit_id`.

Competitor methods: `get_salon_with_services(salon_id)` — joins salons + services by booksy_id. `get_salon_basic(salon_id)` — metadata only. `call_rpc(rpc_name, params)` — generic RPC caller, returns list[dict]. `save_competitor_report(...)` — upserts to `competitor_reports`.

Optimization methods: `save_optimized_pricelist(...)` — upserts to `optimized_pricelists`.

Shared methods: `get_benchmarks(city?)`, `get_competitors(lat, lng, radius_km, service_names)`, `geocode_salon(name?, address?)`.

## Job Store (job_store.py)

In-memory job tracking with structured logging, SSE pub/sub, and cancel support.

`LogEntry`: timestamp, level (info/warning/error), message, step?, progress?, data?.

`Job`: job_id, audit_id, status (queued/running/completed/failed/cancelled), timestamps, current_step, progress (0-100), progress_message, error, logs[], meta{}, steps{} (per-step timing). Methods: `add_log()`, `mark_running()`, `mark_completed()`, `mark_failed(error)`, `mark_cancelled()`, `request_cancel()`, `to_summary()`, `to_dict()`. The `cancel_requested` property is checked by `on_progress()` callbacks in pipeline background jobs.

`JobStore`: max 50 completed jobs (oldest evicted, running/queued never evicted). Pub/sub via `subscribe()`/`unsubscribe()`. Events pushed to all subscriber queues on job state changes. Full queues silently drop events.

Pipeline log capture: `server.py` attaches `_JobLogHandler` (logging.Handler subclass) to each pipeline's logger during execution. Regex `_STEP_PATTERN` extracts step names from log messages like `[audit_id] Step N: description`. Handler is removed in `finally` block. This means zero changes needed in pipeline code for monitoring.

## Background Job Flow (server.py)

All three pipelines follow the same pattern in their `run_*_job` functions:

1. Get Job from store → `mark_running()` → notify SSE
2. Attach `_JobLogHandler` to pipeline logger
3. Define `on_progress` callback: check `cancel_requested`, log to job, call Convex webhook
4. Run pipeline → get result dict
5. Save to Supabase (specific table per pipeline)
6. `mark_completed()` → notify SSE (BEFORE Convex webhook)
7. Call Convex `complete_audit()` (best-effort — failure only logs warning, job stays completed)
8. On `CancelledError`: `mark_cancelled()`, notify Convex `fail_audit`
9. On other Exception: `mark_failed(error)`, notify Convex `fail_audit`
10. Always: remove log handler in `finally`

All service imports are late (inside the function) to avoid circular deps.

## Monitoring Dashboard

Single HTML file at `templates/dashboard.html`, served at `/dashboard`. Dark theme, monospace font, no external dependencies.

Features: job cards with status badges (color-coded: blue=running, green=completed, red=failed, yellow=cancelled), progress bars, elapsed time (live-updating every 1s), Cancel button on running jobs, expandable log panels (lazy-loaded, auto-opened for running jobs), step timing visualization, progress percentage in log entries. Job type visible in subtitle (audit/competitor/optimization from meta).

Auto-updates via SSE to `/api/events`. Reconnects on disconnect (3s delay). Terminal status changes (completed/failed/cancelled) trigger full card re-render to update buttons.

## Helpers (pipelines/helpers.py)

Pure functions, no side effects. Accept duck-typed objects via `_get_attr()` helper.

Text builders: `build_full_pricelist_text(data)`, `build_pricelist_summary(data)` (condensed, max 5/category, 10 categories).

Validation: `validate_name_transformation(before, after)` — rejects too short (<3), too long (>80), marketing garbage, enriched without separator, low-similarity. `is_fixed_price(price)` — detects "od", ranges, dashes.

Scoring: `calculate_completeness_score(stats)` (0-15), `calculate_seo_score(keywords, stats)` (0-10), `calculate_ux_score(stats)` (0-5). `calculate_audit_stats(data)` — deterministic 30+ metrics.

Programmatic fixes: `clean_service_name(name)`, `fix_caps_lock(text)`.

String: `calculate_similarity(a, b)` — Jaccard on words.

## Environment Variables

See `.env.example`. Required: `MINIMAX_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `CONVEX_URL`, `API_KEY`. Optional with defaults: `MINIMAX_BASE_URL` (https://api.minimax.io/anthropic), `MINIMAX_MODEL` (MiniMax-M2.7), `PORT` (3001), `CONVEX_DEPLOY_KEY` (empty).

IMPORTANT: `CONVEX_URL` must use `.convex.site` domain (NOT `.convex.cloud`). Convex HTTP routes live on the `.site` domain. Example: `https://reliable-scorpion-10.convex.site`.

## Commands

```bash
uv sync --extra dev                              # install all deps including dev
uv run pytest tests/ -v -m "not integration"     # unit tests (124 tests, no API keys needed)
uv run pytest tests/ -v -m integration           # integration tests (need real API keys)
uv run uvicorn server:app --host 0.0.0.0 --port 3001  # run server
uv run python server.py                          # alternative: run via __main__
uv run python test_local.py path/to/booksy.json  # local test with booksy-extractor JSON
curl localhost:3001/api/health                    # verify server is up
open http://localhost:3001/dashboard              # monitoring dashboard
```

## Testing

Tests use `pytest` with `asyncio_mode=auto`. Unit tests mock MiniMaxClient with `AsyncMock`. Integration tests marked `@pytest.mark.integration`.

Test files and counts:
- `test_audit_pipeline.py` — 51 tests (helper functions: clean, fix, validate, similarity, stats, scoring, pricelist builders)
- `test_competitor_pipeline.py` — 30 tests (RPC formatting, market/SWOT/strategy parsing, report assembly, empty/partial data, full pipeline mocked)
- `test_optimization_pipeline.py` — 11 tests (context loading, category mapping, service optimization, SEO skip, programmatic fixes, quality verification, CRITICAL: prices/durations/variants never change, report assembly)
- `test_job_store.py` — 18 tests (LogEntry, Job lifecycle, JobStore CRUD, eviction, SSE pub/sub)
- `test_api.py` — 9 tests (health, analyze/competitor/optimize 202, auth, validation, jobs list, logs, dashboard, SSE)
- `test_agent_runner.py` — 5 tests (end_turn, tool calls, history, max_steps, errors)
- `test_minimax_client.py` — 5 integration tests
- `test_supabase.py` — 5 integration tests

Total: 124 unit tests + 10 integration tests.

Fixtures in `conftest.py`: `sample_scraped_data` (3 categories, 10 services with variants), `large_scraped_data` (15 categories, 180 services).

## Coding Conventions

- All prompts in Polish (target market: Polish beauty salons)
- camelCase field names in Pydantic models (JSON compat with Convex/Supabase)
- Late imports inside background job functions and pipeline entry points to avoid circular deps
- `logging` module everywhere, not `print()`
- `pipelines/helpers.py` functions accept duck-typed objects via `_get_attr()` helper
- Prompt templates use Python `.format()` — double braces `{{}}` for literal JSON in templates
- Job monitoring endpoints unauthenticated (internal, not proxied by nginx)
- SSE events fire-and-forget — slow clients have events dropped (queue maxsize=100)
- Convex webhooks best-effort — job marked completed BEFORE webhook call
- Optimization pipeline: prices/durations/variants NEVER modified (triple protection: prompt, post-processing, tests)

## Related Repos

- BEAUTY_AUDIT (Convex + Next.js frontend) — calls bagent's 3 pipeline endpoints, receives progress via Convex HTTP webhooks, displays reports from Supabase
- bextract — Booksy scraper service on same server (api.booksyaudit.pl)

## Deployment

```bash
cd /opt/bagent && git pull && uv sync
uv run uvicorn server:app --host 0.0.0.0 --port 3001
```

### Nginx Configuration (api.booksyaudit.pl)

```nginx
# Pipeline endpoints (authenticated via x-api-key in app)
location /api/analyze    { proxy_pass http://127.0.0.1:3001; }
location /api/competitor { proxy_pass http://127.0.0.1:3001; }
location /api/optimize   { proxy_pass http://127.0.0.1:3001; }
location /api/health     { proxy_pass http://127.0.0.1:3001; }

# Monitoring endpoints (no app-level auth — consider nginx basic_auth)
location /api/jobs    { proxy_pass http://127.0.0.1:3001; }
location /dashboard   { proxy_pass http://127.0.0.1:3001; }

# SSE stream — proxy_buffering MUST be off, otherwise nginx buffers
# the event stream and dashboard gets no live updates
location /api/events  {
    proxy_pass http://127.0.0.1:3001;
    proxy_buffering off;
    proxy_cache off;
    proxy_set_header Connection '';
    proxy_http_version 1.1;
    chunked_transfer_encoding off;
}
```

Optional basic auth for dashboard:

```nginx
location /dashboard {
    auth_basic "bagent monitor";
    auth_basic_user_file /etc/nginx/.htpasswd_bagent;
    proxy_pass http://127.0.0.1:3001;
}
```

Generate htpasswd: `htpasswd -c /etc/nginx/.htpasswd_bagent admin`
