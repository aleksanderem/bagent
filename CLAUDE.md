# bagent — Beauty Audit AI Analyzer

Python FastAPI service that runs AI analysis pipelines for BeautyAudit salons using MiniMax M2.7 with tool_use agent loops. Replaces Convex-side AI analysis which had a 600s timeout limit. Deployed alongside bextract on api.booksyaudit.pl.

## Architecture

```
Convex (analyzeWithAI action)
  → POST /api/analyze { auditId, userId, scrapedData }
  → bagent returns 202 Accepted immediately
  → background task runs 10-step audit pipeline (no time limit)
  → saves report to Supabase (audit_reports table)
  → calls Convex HTTP webhook to mark audit complete
  → Convex updates audit status in frontend
```

The service is stateless. Job tracking is in-memory (dict in server.py) for status polling, but the real state flows through Supabase (report storage) and Convex webhooks (progress/completion notifications).

## Tech Stack

Python 3.12+, FastAPI, Pydantic v2, anthropic SDK (wrapping MiniMax M2.7's Anthropic-compatible API), supabase-py, httpx, uv for dependency management.

## Project Layout

```
server.py              → FastAPI app, endpoints, background job runner
config.py              → Pydantic Settings from .env

agent/
  runner.py            → Multi-turn tool_use agent loop (run_agent_loop)
  tools.py             → NAMING_TOOL, DESCRIPTION_TOOL definitions

pipelines/
  audit.py             → 10-step audit pipeline (run_audit_pipeline)
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

Score capping rules: >=3 critical issues → cap at 60, any critical → 75, any major → 85, any issues → 95.

## Agent Loop (agent/runner.py)

The `run_agent_loop()` function implements multi-turn tool_use with MiniMax M2.7:

1. Send user message + tool definitions
2. M2.7 responds with tool_use blocks (batch of transformations)
3. Append FULL assistant response to messages (critical for interleaved thinking)
4. Send tool_result acknowledgments ("OK. Kontynuuj przetwarzanie pozostałych usług.")
5. M2.7 continues until `stop_reason="end_turn"` or max_steps reached
6. Return collected ToolCallResult list

The model self-decomposes work into batches of 15-20 services. 204K context window means full pricelist fits in one conversation.

## MiniMax Client (services/minimax.py)

Thin wrapper around `anthropic.AsyncAnthropic`. Three methods:

- `create_message()` — raw Anthropic-compatible call with tools (for agent loops)
- `generate_json()` — single prompt → parsed JSON (handles markdown code blocks)
- `generate_text()` — single prompt → text string

`with_retry()` — exponential backoff (2s base, 10s max, 2 attempts). Skips retries for auth errors (401/403).

## API Endpoints

- `POST /api/analyze` (202) — requires `x-api-key` header, accepts `{auditId, userId, scrapedData}`, returns `{jobId, status: "accepted"}`
- `GET /api/jobs/{job_id}` — returns `{jobId, status, error?}`
- `GET /api/health` — returns `{status: "ok"}`

Auth: `x-api-key` header validated against `settings.api_key` via FastAPI Depends.

## Environment Variables

See `.env.example`. Critical ones: `MINIMAX_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `CONVEX_URL`, `API_KEY`. Port defaults to 3001.

## Commands

```bash
uv sync --extra dev                              # install deps
uv run pytest tests/ -v -m "not integration"     # unit tests (60 tests, no API keys needed)
uv run pytest tests/ -v -m integration           # integration tests (need real API keys)
uv run uvicorn server:app --host 0.0.0.0 --port 3001  # run server
uv run python server.py                          # alternative: run via __main__
```

## Testing

Tests use `pytest` with `asyncio_mode=auto`. Unit tests mock MiniMaxClient with `AsyncMock`. Integration tests are marked `@pytest.mark.integration` and need real API keys.

Test files: `test_agent_runner.py` (5 tests), `test_audit_pipeline.py` (51 tests for helpers), `test_api.py` (4 tests), `test_minimax_client.py` (5 integration), `test_supabase.py` (5 integration).

Fixtures in `conftest.py`: `sample_scraped_data` (3 categories, 10 services), `large_scraped_data` (15 categories, 180 services).

## Coding Conventions

- All prompts are in Polish (target market is Polish beauty salons)
- camelCase field names in Pydantic models (JSON compat with Convex/Supabase)
- Late imports inside `run_audit_pipeline()` to avoid circular deps when services aren't configured
- `logging` module everywhere, not `print()`
- `pipelines/helpers.py` functions accept duck-typed objects (dict or model with matching attributes) via `_get_attr()` helper
- Prompt templates use Python `.format()` — double braces `{{}}` for literal JSON braces

## Related Repos

- BEAUTY_AUDIT (Convex + Next.js frontend) — the app that calls bagent's `/api/analyze` endpoint
- bextract — Booksy scraper service on same server (api.booksyaudit.pl)

## Deployment

```bash
cd /opt/bagent && git pull && uv sync
uv run uvicorn server:app --host 0.0.0.0 --port 3001
```

Nginx reverse proxy on api.booksyaudit.pl routes `/api/analyze`, `/api/jobs`, `/api/health` to port 3001.
