# E2E Pipeline Tests

End-to-end tests that pick **random real salons** from production
Supabase and run **full pipelines in-process**, then verify
**deterministic invariants** on the output.

## Why

AI pipelines (audit, summary) produce non-deterministic text — we
cannot snapshot-test them. But every correct AI output **must** satisfy
invariants:

- Schema validity (all required fields, correct types)
- Range bounds (`totalScore` in `[0, 92]`, **never 100** per product spec)
- Cardinality matching (services in == services out, modulo dedup)
- Polish text quality (no English leakage, no mojibake, diacritics OK
  in long text, no double spaces)
- Cross-field consistency (`scoreBreakdown` sums to `totalScore` ±2)
- Progress monotonicity (pipeline never jumps backwards)

Mechanical pipelines (cennik) get stricter checks (no AI = full
determinism + cardinality preservation).

## Layout

```
tests/e2e/
├── conftest.py                      # salon picker + ScrapedData builder
├── invariants.py                    # reusable invariant assertions
├── test_audit_pipeline_e2e.py       # BAGENT #1
├── test_cennik_pipeline_e2e.py      # BAGENT #2
└── runner.py                        # CLI for ad-hoc / CI
```

## Running

### Pytest (developer iteration)

```bash
# Install dev deps if you haven't:
uv sync --extra dev

# All e2e tests, default 3 salons:
pytest tests/e2e -v -m e2e

# Reproducible run with fixed seed:
pytest tests/e2e -v -m e2e --e2e-seed=42

# Bigger sample, narrower service range:
pytest tests/e2e -v -m e2e --e2e-salons=10 --e2e-min-services=10 --e2e-max-services=30

# Skip e2e (default behaviour with -m "not integration"):
pytest -m "not integration"
```

### CLI runner (CI dashboards / ad-hoc)

```bash
# Single pipeline, 5 salons, write JSON report:
python -m tests.e2e.runner --pipeline audit --salons 5 --report e2e-results.json

# All pipelines, fixed seed:
python -m tests.e2e.runner --pipeline all --salons 3 --seed 42

# CI cron — tighter sample, bigger report:
python -m tests.e2e.runner --pipeline all --salons 10 --report e2e-$(date +%F).json
```

The runner exits non-zero on any failure → drop in any CI matrix.

## Adding a new pipeline test

1. Add an invariants function in `invariants.py`:

   ```python
   def check_summary_output(summary: dict, scraped_data: Any) -> InvariantReport:
       rep = InvariantReport(pipeline="summary", salon_id=...)
       rep.check("required_keys", ...)
       return rep
   ```

2. Add a test file `test_<pipeline>_pipeline_e2e.py` that loops
   `salon_pool`, calls the pipeline in-process, and runs the
   invariants. Use `@pytest.mark.integration @pytest.mark.e2e`.

3. Add a `run_<pipeline>` async function in `runner.py` and wire it
   into `main_async`.

4. Update this README.

## Determinism strategy

| Layer | Approach |
|-------|----------|
| Salon selection | Stratified random, seeded — `--e2e-seed=N` reproduces |
| Pipeline AI calls | Real MiniMax/z.ai — variability accepted |
| Output verification | Invariants only — never exact-match on AI text |
| DB writes | Blocked via `disable_supabase_writes` fixture |
| Convex webhooks | Skipped — pipelines invoked in-process, no callback URL |

## Known limitations

1. **AI cost per run** — each audit pipeline run hits MiniMax for ~10
   calls. 5 salons × audit ≈ 50 AI calls. Budget accordingly. Use
   `--e2e-salons=1` for fast smoke checks.

2. **Real Supabase reads** — tests pull live `salon_scrapes` data.
   Tests can flake if a sampled salon got deleted/cleaned between
   sampling and pipeline invocation. Re-run with same seed; if it
   reproduces, file an issue against the picker filters.

3. **Cennik test currently uses identity transformations** — it doesn't
   exercise the merge/rename code paths. Extend with a synthetic
   transformation payload to test those.

4. **No competitor / summary E2E yet** — those pipelines are longer
   (multi-step, multi-AI-pass). Pattern is the same; add when needed.

## Suggested CI cadence

- **Pre-merge (every PR)** — `--pipeline cennik --salons 3` (no AI cost,
  catches schema breaks)
- **Nightly** — `--pipeline all --salons 5` (full coverage, AI included)
- **Pre-deploy** — `--pipeline all --salons 10` (gate the deploy pipeline)
