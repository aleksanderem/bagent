# bagent — Beauty Audit AI Analyzer

Python FastAPI service that runs AI analysis pipelines for BeautyAudit salons using MiniMax M2.7 with tool_use agent loops. Replaces Convex-side AI analysis which had a 600s timeout limit. Deployed alongside bextract on api.booksyaudit.pl.

## Architecture

```
Convex (analyzeWithAI action)
  → POST /api/analyze { auditId, userId, scrapedData }
  → bagent returns 202 Accepted immediately
  → background task runs 10-step audit pipeline (no time limit)
  → each step logs to in-memory JobStore + sends SSE to dashboard
  → each step calls Convex webhook for frontend progress bar
  → saves report to Supabase (audit_reports table)
  → calls Convex HTTP webhook to mark audit complete
  → Convex updates audit status in frontend
```

Job state lives in two places: the in-memory `JobStore` (for monitoring via dashboard/API, lost on restart) and external systems (Supabase for reports, Convex for audit status — persistent).

## Tech Stack

Python 3.12+, FastAPI, Pydantic v2, anthropic SDK (wrapping MiniMax M2.7's Anthropic-compatible API), supabase-py, httpx, sse-starlette (SSE for live dashboard), uv for dependency management.

## Project Layout

```
server.py              → FastAPI app, all endpoints, background job runner, _JobLogHandler
config.py              → Pydantic Settings from .env
job_store.py           → Job, LogEntry, JobStore (in-memory tracking + SSE pub/sub)

agent/
  runner.py            → Multi-turn tool_use agent loop (run_agent_loop)
  tools.py             → NAMING_TOOL, DESCRIPTION_TOOL definitions

pipelines/
  audit.py             → 10-step audit pipeline (run_audit_pipeline), private step functions
  helpers.py           → Pure functions: stats, scoring, text builders, validation

services/
  minimax.py           → MiniMaxClient (AsyncAnthropic wrapper) + with_retry
  supabase.py          → SupabaseService (scraped data, reports, benchmarks, geocoding)
  convex.py            → ConvexClient (progress/complete/fail webhooks)

models/
  scraped_data.py      → ScrapedData, ScrapedCategory, ScrapedService, ServiceVariant
  report.py            → EnhancedAuditReport, AuditStats, AuditIssue, ScoreBreakdown, etc.
  analysis.py          → NamingAnalysisResult, DescriptionAnalysisResult, QualityReport

prompts/               → Polish-language prompt templates (.txt), loaded via _load_prompt()
templates/
  dashboard.html       → Single-file monitoring dashboard (HTML+CSS+JS, SSE-powered)
tests/                 → pytest suite, asyncio_mode=auto
```

## Data Flow

Input is `ScrapedData` — scraped Booksy salon pricelist with categories, services, prices, descriptions, variants. All field names are camelCase (JSON compat with Convex/Supabase).

Output is `EnhancedAuditReport` — score breakdown (7 dimensions summing to 0-100), issues sorted by severity, naming/description transformations (before/after), quick wins, SEO keywords, competitor context, summary.

## The 10-Step Pipeline (pipelines/audit.py)

1. `calculate_audit_stats()` — deterministic stats (30+ metrics), no AI
2. Naming score — `client.generate_json()` with `naming_score.txt` prompt, returns 0-20 + issues
3. Naming transformations — `run_agent_loop()` with `NAMING_TOOL`, M2.7 calls `submit_naming_results` in batches
4. Description score — same pattern as naming, `descriptions_score.txt`
5. Description transformations — `run_agent_loop()` with `DESCRIPTION_TOOL`
6. Structure & pricing — `client.generate_json()` with `structure.txt`, returns structureScore/pricingScore/quickWins/SEO
7. Summary — `client.generate_text()` with `summary.txt`, 2-3 sentences in Polish
8. Competitor context — Supabase queries (geocode, nearby salons, benchmarks)
9. Quality validation — 7 weighted business checks, accept if score >= 0.7
10. Assemble report — combine all results, apply progressive score capping

Score capping rules: >=3 critical issues cap at 60, any critical cap at 75, any major cap at 85, any issues cap at 95.

Private step functions in audit.py: `_analyze_naming()`, `_analyze_descriptions()`, `_analyze_structure()`, `_generate_summary()`, `_validate_quality()`. Each loads prompts from `prompts/` via `_load_prompt()` with hardcoded fallbacks if files are missing.

## Agent Loop (agent/runner.py)

The `run_agent_loop()` function implements multi-turn tool_use with MiniMax M2.7:

1. Send user message + tool definitions
2. M2.7 responds with tool_use blocks (batch of transformations)
3. Append FULL assistant response to messages (critical for interleaved thinking)
4. Send tool_result acknowledgments ("OK. Kontynuuj przetwarzanie pozostałych usług.")
5. M2.7 continues until `stop_reason="end_turn"` or max_steps reached
6. Return collected ToolCallResult list

Returns `AgentResult` dataclass with `tool_calls`, `final_text`, `total_steps`, `total_input_tokens`, `total_output_tokens`. The model self-decomposes work into batches of 15-20 services. 204K context window means full pricelist fits in one conversation.

## MiniMax Client (services/minimax.py)

Thin wrapper around `anthropic.AsyncAnthropic`. Three methods:

- `create_message(system, messages, tools, max_tokens, temperature)` — raw Anthropic-compatible call with tools (for agent loops), returns `anthropic.types.Message`
- `generate_json(prompt, system, max_tokens)` — single prompt, parsed JSON (handles markdown code blocks via regex fallback)
- `generate_text(prompt, system, max_tokens)` — single prompt, text string

`with_retry(fn, max_attempts, base_delay, max_delay)` — exponential backoff (2s base, 10s max, 2 attempts). Skips retries for auth errors (401/403/unauthorized/invalid key).

## Job Store (job_store.py)

In-memory job tracking with structured logging and SSE pub/sub.

`LogEntry` dataclass: timestamp, level (info/warning/error), message, optional step name, optional progress percentage, optional data dict.

`Job` dataclass: job_id, audit_id, status (queued/running/completed/failed), created_at/started_at/completed_at timestamps, current_step, progress (0-100), progress_message, error, logs (list of LogEntry), meta dict (salon name, service count, userId), steps dict (per-step timing with started_at/completed_at/duration_ms). Methods: `add_log()` (also updates progress/step/timing), `mark_running()`, `mark_completed()`, `mark_failed(error)`, `to_summary()` (without logs, for list view), `to_dict()` (with logs, for detail view).

`JobStore`: holds `_jobs` dict, max 50 completed jobs (oldest evicted, running never evicted). Pub/sub via `subscribe()` returning `asyncio.Queue`, `unsubscribe(queue)`. `notify_progress(job)` and `notify_status(job)` push events to all subscribers. Full queue (slow client) silently drops events.

Pipeline step detection: `server.py` attaches a `_JobLogHandler` (logging.Handler subclass) to the `pipelines.audit` logger during job execution. It intercepts log messages matching `[audit_id] Step N: description` via regex `_STEP_PATTERN` and writes them as structured LogEntry records to the job. The handler is removed in the `finally` block. This means zero changes to `pipelines/audit.py` for monitoring.

## API Endpoints

All endpoints are defined in `server.py`.

**Authenticated (x-api-key header):**
- `POST /api/analyze` (202) — accepts `{auditId, userId, sourceUrl?, scrapedData}`, creates job in JobStore, launches background task, returns `{jobId, status: "accepted"}`

**Public (no auth, internal monitoring):**
- `GET /api/jobs` — list all tracked jobs as summaries (running first, then newest), returns array of job objects with progress/status/meta/steps
- `GET /api/jobs/{job_id}` — single job summary, 404 if not found
- `GET /api/jobs/{job_id}/logs` — full job detail including all log entries
- `GET /api/events` — SSE stream, pushes events: `job_created`, `job_progress`, `job_completed`, `job_failed`, each with JSON data containing jobId/status/progress/progressMessage
- `GET /api/health` — returns `{status: "ok", jobs_running: N, jobs_total: N}`
- `GET /dashboard` — serves monitoring dashboard HTML

Auth: `verify_api_key()` FastAPI dependency checks `x-api-key` header against `settings.api_key`. Only `/api/analyze` is authenticated; monitoring endpoints are internal-only (not exposed via nginx).

## Monitoring Dashboard

Single HTML file at `templates/dashboard.html`, served at `/dashboard`. Dark theme, monospace font, no external dependencies.

Features: job cards with status badges (color-coded), progress bars, elapsed time (live-updating every 1s for running jobs), expandable log panels (lazy-loaded from `/api/jobs/{id}/logs`), step timing visualization. Auto-updates via SSE connection to `/api/events` — no polling, no manual refresh. Reconnects automatically on disconnect (3s delay). Job cards update in-place (DOM mutation, not full re-render).

SSE events handled: `job_created` (new card), `job_progress` (update progress/message/step), `job_completed`/`job_failed` (update status badge and progress bar color).

## Background Job Flow (server.py: run_analysis_job)

1. Get Job from store, call `job.mark_running()`, notify SSE subscribers
2. Attach `_JobLogHandler` to `pipelines.audit` logger
3. Parse `request.scrapedData` into `ScrapedData` model
4. Create `on_progress` callback that logs to job AND calls Convex webhook
5. Call `run_audit_pipeline(scraped_data, audit_id, on_progress)`
6. Save report to Supabase via `SupabaseService.save_report()`
7. Notify Convex via `ConvexClient.complete_audit()` with score and stats
8. Call `job.mark_completed()`, notify SSE
9. On exception: `job.mark_failed(error)`, notify SSE, call `ConvexClient.fail_audit()`
10. Always: remove `_JobLogHandler` from pipeline logger in `finally`

All service imports are late (inside the function) to avoid circular deps and allow the server to start even when external services aren't configured.

## Helpers (pipelines/helpers.py)

Pure functions, no side effects, no AI calls. Functions accept duck-typed objects via `_get_attr()` helper (works with both Pydantic models and plain dicts).

Text builders: `build_full_pricelist_text(data)` — full pricelist for AI prompts, `build_pricelist_summary(data)` — condensed (max 5 services/category, max 10 categories).

Validation: `validate_name_transformation(before, after)` — rejects too short (<3), too long (>80), marketing garbage patterns, enriched names without separator, low-similarity replacements. `is_fixed_price(price)` — detects "od", ranges, en-dashes.

Scoring: `calculate_completeness_score(stats)` (0-15), `calculate_seo_score(missing_keywords, stats)` (0-10), `calculate_ux_score(stats)` (0-5). `calculate_audit_stats(data)` — deterministic, returns 30+ metrics dict matching AuditStats model.

Programmatic fixes: `clean_service_name(name)` — trailing dots, spacing around +/-. `fix_caps_lock(text)` — sentence case when >60% uppercase.

String: `calculate_similarity(a, b)` — Jaccard on words (ignoring single-char words).

## Environment Variables

See `.env.example`. Required (no defaults): `MINIMAX_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `CONVEX_URL`, `API_KEY`. Optional with defaults: `MINIMAX_BASE_URL` (https://api.minimax.io/anthropic), `MINIMAX_MODEL` (MiniMax-M2.7), `PORT` (3001), `CONVEX_DEPLOY_KEY` (empty).

IMPORTANT: `CONVEX_URL` must use `.convex.site` domain (NOT `.convex.cloud`). Convex HTTP routes live on the `.site` domain. Example: `https://reliable-scorpion-10.convex.site`.

## Commands

```bash
uv sync --extra dev                              # install all deps including dev
uv run pytest tests/ -v -m "not integration"     # unit tests (83 tests, no API keys needed)
uv run pytest tests/ -v -m integration           # integration tests (need real API keys)
uv run uvicorn server:app --host 0.0.0.0 --port 3001  # run server
uv run python server.py                          # alternative: run via __main__
curl localhost:3001/api/health                    # verify server is up
open http://localhost:3001/dashboard              # monitoring dashboard
```

## Testing

Tests use `pytest` with `asyncio_mode=auto`. Unit tests mock MiniMaxClient with `AsyncMock`. Integration tests are marked `@pytest.mark.integration` and need real API keys.

Test files and counts:
- `test_job_store.py` — 18 tests (LogEntry, Job lifecycle, JobStore CRUD, eviction, SSE pub/sub)
- `test_audit_pipeline.py` — 51 tests (all helper functions: clean, fix, validate, similarity, stats, scoring, pricelist builders)
- `test_api.py` — 9 tests (health, analyze 202, auth 422, validation 422, list jobs, job logs, job 404, dashboard HTML, SSE route exists)
- `test_agent_runner.py` — 5 tests (end_turn, tool call collection, history preservation, max_steps, error propagation)
- `test_minimax_client.py` — 5 integration tests (text gen, JSON gen, tool use single/multi turn, client init)
- `test_supabase.py` — 5 integration tests (read data, missing data, benchmarks, geocode, competitors)

Total: 83 unit tests + 10 integration tests. Fixtures in `conftest.py`: `sample_scraped_data` (3 categories, 10 services with variants), `large_scraped_data` (15 categories, 180 services).

## Coding Conventions

- All prompts are in Polish (target market is Polish beauty salons)
- camelCase field names in Pydantic models (JSON compat with Convex/Supabase)
- Late imports inside `run_analysis_job()` and `run_audit_pipeline()` to avoid circular deps
- `logging` module everywhere, not `print()`
- `pipelines/helpers.py` functions accept duck-typed objects via `_get_attr()` helper
- Prompt templates use Python `.format()` — double braces `{{}}` for literal JSON braces in templates
- Job monitoring endpoints are unauthenticated (internal only, not proxied by nginx)
- SSE events are fire-and-forget — slow clients have events dropped (queue maxsize=100)

## Related Repos

- BEAUTY_AUDIT (Convex + Next.js frontend) — the app that calls bagent's `/api/analyze` endpoint, receives progress via Convex webhooks
- bextract — Booksy scraper service on same server (api.booksyaudit.pl)

## Deployment

```bash
cd /opt/bagent && git pull && uv sync
uv run uvicorn server:app --host 0.0.0.0 --port 3001
```

### Nginx Configuration (api.booksyaudit.pl)

```nginx
# API endpoints (authenticated via x-api-key in app)
location /api/analyze { proxy_pass http://127.0.0.1:3001; }
location /api/health  { proxy_pass http://127.0.0.1:3001; }

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

If you want to protect the dashboard with basic auth at the nginx level:

```nginx
location /dashboard {
    auth_basic "bagent monitor";
    auth_basic_user_file /etc/nginx/.htpasswd_bagent;
    proxy_pass http://127.0.0.1:3001;
}
# Apply same auth to /api/jobs and /api/events if needed
```

Generate the htpasswd file: `htpasswd -c /etc/nginx/.htpasswd_bagent admin`
