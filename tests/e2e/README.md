# E2E Pipeline Tests

End-to-end tests that pick **random real salons** from production
Supabase and run **full pipelines in-process** (or static-audit critical
code paths), then verify **deterministic invariants** on the output.

## Why

AI pipelines (audit, summary, competitor_report) produce
non-deterministic text — we cannot snapshot-test them. But every
correct output **must** satisfy invariants we encode here. AI variability
is captured by checking shape and bounds, not exact text matches.

Mechanical pipelines (cennik) get stricter checks (no AI = full
determinism + cardinality preservation).

## Coverage matrix

| Surface | Test file | Cost | Runs against |
|---|---|---|---|
| Audit pipeline (BAGENT #1) | `test_audit_pipeline_e2e.py` | High AI | Real salons |
| Cennik pipeline (BAGENT #2) | `test_cennik_pipeline_e2e.py` | None | Real salons + mocked Supabase reads |
| Summary pipeline (BAGENT #3) | `test_summary_pipeline_e2e.py` | Medium AI | Real salons + mocked Supabase reads |
| Competitor pipeline (BAGENT #4) | `test_competitor_pipeline_e2e.py` | Heavy AI | Real salons + mocked Supabase reads |
| Versum mapping | `test_versum_pipeline_e2e.py` | Medium AI | Real salons (no DB read) |
| Discovery pump priority logic | `test_discovery_pump_regression.py` | None | Mocked DB rows |
| HTTP/2 cascade prevention | `test_http2_cascade_regression.py` | None | Live Supabase (read) |
| Cross-DB consistency (Convex↔Supabase) | `test_cross_db_consistency.py` | None | Live both |
| bagent↔Convex webhook contracts | `test_webhook_contracts.py` | None | Static |
| Score cap NEVER 100 (cross-cutting) | `test_score_cap_audit.py` | None | Static (grep) |
| AI determinism (same salon 2×) | `test_determinism.py` | High AI ×2 | Real salons |

## What invariants are checked

### Polish text quality (any AI-emitted text field)

- **No English leakage**: 30+ word denylist (the/and/is/with/please/customer/improve/optimize/click/here…)
- **No AI self-references**: `jako AI`, `jako model językowy`, `Przepraszam, ale`, `as a language model`
- **No placeholders**: `[OPIS]`, `{NAME}`, `lorem ipsum`, `....`, `XXX`
- **No markdown leakage**: `**bold**`, `# heading`, ```` ```code``` ````, `[link](url)` in plain-text fields
- **No mojibake**: `Å›`, `Ã³`, `â€"`, `ï»¿`, replacement chars (15+ patterns)
- **Polish diacritics**: at least one in any text >30 chars (was 60)
- **No double spaces**

### Score discipline

- `totalScore` ∈ `[0, MAX_DISPLAY_SCORE]` (92) — **NEVER 100**
- `scoreBreakdown` sums to a plausible raw score in `[0, 100]`
- `totalScore = min(raw_sum, MAX_DISPLAY_SCORE)` (cap correctly applied, ±2 rounding)
- `industryComparison.yourScore` also capped
- `MAX_AUDIT_SCORE` constant exists in code and is < 100

### Cardinality

- Cennik output service count ≈ input ±10% (dedup-allowable)
- Coverage `totalChecked` ≈ input service count ±5%
- Per-category coverage: every input category with services gets walked
- Versum suggestions count == input service count

### Discovery pump correctness

- Top-K picker returns distinct combos for parallel slots
- Cooldown skipping works for done + saturated combos
- Largest-gap combo sorts first
- Running combos never picked
- Lock key prefixes are stable (Redis state survives deploys)

### HTTP/2 cascade prevention

- `make_supabase_client()` produces an HTTP/1.1 client (no http2)
- 20 concurrent reads against prod produce zero `ConnectionTerminated`/`RECV_WINDOW_UPDATE` errors

### Cross-DB consistency

- Every Convex `done` audit has a Supabase `audit_reports` row by `convex_audit_id`
- `user_id` matches between the two
- No duplicate Supabase rows for the same `convex_audit_id`

### Webhook payload contracts

- Bagent → Convex `report/complete` requires `auditId`, `report`, `supabaseAuditId`
- Bagent → Convex `report/progress` requires `progress` ∈ `[0,100]`
- Resend webhook event types match a known set (forces explicit decision when Resend adds new types)

## Running

### Pytest (developer iteration)

```bash
# Install dev deps
uv sync --extra dev

# Static / no-AI tests (fast, deterministic, ~1 sec)
pytest tests/e2e -v -m e2e \
  --ignore=tests/e2e/test_audit_pipeline_e2e.py \
  --ignore=tests/e2e/test_summary_pipeline_e2e.py \
  --ignore=tests/e2e/test_competitor_pipeline_e2e.py \
  --ignore=tests/e2e/test_versum_pipeline_e2e.py \
  --ignore=tests/e2e/test_determinism.py

# All E2E (runs AI — costs money + slow)
pytest tests/e2e -v -m e2e --e2e-salons=2

# Reproducible run with fixed seed
pytest tests/e2e -v -m e2e --e2e-seed=42

# Skip slow (determinism + competitor)
pytest tests/e2e -v -m "e2e and not slow"
```

### CLI runner (CI / dashboards)

```bash
# Single pipeline, 5 salons, write JSON report:
python -m tests.e2e.runner --pipeline audit --salons 5 --report e2e-results.json

# All pipelines with baseline comparison:
python -m tests.e2e.runner --pipeline all --salons 3 --baseline .e2e-baseline.json

# Manual cost-controlled run:
python -m tests.e2e.runner --pipeline cennik --salons 10 --max-services 50
```

## Adding a new pipeline test

1. **Invariants**: add `check_<pipeline>_output()` in `invariants.py`. Use
   `check_polish_text_field()` for any AI-emitted text — covers the
   6-check Polish quality battery.
2. **Pipeline runner**: add `run_<pipeline>()` in `pipeline_runners.py`
   that mocks Supabase reads via `_FakeSupabaseReads`.
3. **Test file**: copy a sibling `test_<existing>_pipeline_e2e.py`,
   replace pipeline call + invariant function.
4. **Runner wire-up**: add `run_<pipeline>` in `runner.py` and add to
   `pipeline_dispatch` + `PIPELINES` + `SLA_BUDGET_SEC`.

## Determinism strategy

| Layer | Approach |
|-------|----------|
| Salon selection | Stratified random, seeded — `--e2e-seed=N` reproduces |
| Pipeline AI calls | Real MiniMax — variability accepted |
| Output verification | Invariants only — never exact-match on AI text |
| DB writes | Blocked via `read_only_supabase` fixture (postgrest-level) |
| DB reads (cennik/summary/competitor) | Mocked via `_FakeSupabaseReads` injection |
| Determinism re-check | `test_determinism.py` runs same salon 2× — flags drift |

## Edge-case fixtures (`fixtures.py`)

Synthetic salons that pin pathological cases:

- `edge_case_zero_services()` — 0-service salon, pipeline must not crash
- `edge_case_all_caps_names()` — CAPS LOCK, fix_caps_lock should normalize
- `edge_case_emoji_in_names()` — Unicode emoji, pipeline preserves or strips cleanly
- `edge_case_very_long_names()` — 200+ char service name, PDF must not overflow
- `edge_case_duplicate_names()` — exact duplicates, audit should flag
- `edge_case_empty_descriptions()` — null descriptions, AI agent should fill
- `edge_case_huge_pricelist()` — 120 services × 10 categories, no OOM/timeout

Plus `FailingMiniMaxClient` — drop-in replacement that fails at specific
call indices, lets tests verify pipeline fallback paths trigger.

## CI integration

`.github/workflows/e2e.yml`:

- **Pre-merge** (every PR): static + regression tests only (~1 min, no AI cost)
  - Discovery pump regression
  - Webhook contracts
  - Score cap audit
  - HTTP/2 cascade
- **Nightly** (03:30 UTC): full E2E with AI calls, baseline tracking
- **Manual dispatch**: pick pipeline + salon count via UI

Required GitHub secrets: `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`,
`MINIMAX_API_KEY`, `MINIMAX_BASE_URL`, `MINIMAX_MODEL`,
`BEXTRACT_API_URL`, `BEXTRACT_API_KEY`.

## Performance / cost tracking

The runner writes `--baseline path.json` with per-pipeline avg
durations. Subsequent runs compare against the baseline and flag a
regression if avg duration increased >50%.

`PipelineRunResult` has `ai_calls` + `ai_tokens` fields populated by
pipelines that emit `minimax_calls` / `minimax_tokens` in their output
dict. Used to detect cost regressions (refactor that doubles AI calls).

## Known limitations

1. **AI cost per run** — full audit ≈ 10 MiniMax calls; 5 salons × audit
   ≈ 50 calls. Use `--e2e-salons=1` for fast smoke checks. Pre-merge
   workflow only runs no-AI tests.
2. **Real Supabase reads** — tests pull live `salon_scrapes`. Tests can
   flake if a sampled salon got deleted between sample and pipeline run.
   Re-run with same seed to confirm.
3. **`run_only_supabase` blocks at postgrest level** — pipelines that go
   around the SDK (raw httpx) won't be blocked. Add such call sites to
   the catalog if/when they appear.
4. **Cross-DB consistency tests skip if Convex helper missing** — they
   need `dev:debugListRecentDoneAudits` deployed in Convex. If skipped,
   add the helper.
5. **Webhook contract tests are static** — they don't actually POST to
   Convex. The shapes are documented; drift between bagent emit and
   Convex validator is caught by Convex throwing on bad payload in
   production. To make tests live, replace with actual POSTs to a
   staging Convex deployment.

## Suggested CI cadence

- **Pre-merge**: static + regression (already wired in `.github/workflows/e2e.yml`)
- **Nightly**: full all-pipeline run with 5 salons (already wired)
- **Pre-release**: manual workflow dispatch with 10 salons against staging
