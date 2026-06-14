# embeddings-local

A quota-proof local embedding sidecar serving `sdadas/mmlw-e5-large` (1024-dim,
Polish) as a fallback for BeautyAudit's pipeline when OpenAI embeddings are
exhausted (which happened 2026-06-14 — `insufficient_quota` took embeddings
down with everything else). Domain: booksyaudit.pl.

It is a small, self-contained FastAPI service in its OWN venv. `torch` /
`sentence-transformers` live only here, never in the main bagent worker — the
worker stays light and talks to this sidecar over `127.0.0.1` exactly like an
embeddings API.

This is Phase 1 of beads `BEAUTY_AUDIT-lrh`: the sidecar only. Wiring it into
the live pipeline (the second embedding column, the fallback logic, the
chain-head gate widening) is Phase 2 — see the research runbook
`docs/runbooks/2026-06-14-embedding-backup-research.md` §5.2–§5.6.

## Run locally

```bash
uv sync --extra dev
uv run uvicorn server:app --port 3010
```

The FIRST `/embed` request downloads ~1.2 GB of model weights from HuggingFace
and caches them locally — cold start is slow. Subsequent requests reuse the
warm in-process model. The CPU torch wheel (pinned via `[[tool.uv.index]]`
`pytorch-cpu` in `pyproject.toml`) resolves on the first `uv sync` on
Linux x86_64; tytan is CPU-only (no GPU). If `uv sync` ever fails to resolve
the CPU torch wheel, the operator confirms/adjusts the CPU index at deploy.

## API

`POST /embed`

```json
{ "inputs": ["Botoks 1 okolica", "Manicure hybrydowy"] }
```

returns

```json
{ "embeddings": [[ ...1024 floats... ], ...], "model": "mmlw-e5-large", "dim": 1024 }
```

Callers pass BARE text. The sidecar prepends the `passage: ` E5 prefix
internally (consistent prefix on both subject and candidate sides keeps cosine
meaningful) — do NOT add it yourself. Inputs are capped to 1500 chars before
prefixing and encoded in batches of 64. Empty `inputs` returns `[]` without
loading the model.

`GET /health` → `{ "status": "ok", "model_loaded": <bool> }` — `model_loaded`
reflects the lazy-load state (False until the first `/embed`).

## Tests

```bash
uv run --extra dev pytest tests/ -q
```

Runs WITHOUT downloading the model: every test monkeypatches `server.get_model`
to a fake encoder, and `torch` / `sentence-transformers` are imported lazily
inside `get_model()` only, so the suite needs just fastapi + the fake.

## Deploy (LEAD + user-approved — NOT done in this phase)

PM2 app `embeddings-local`, its OWN venv, bound to localhost:

```bash
uv run uvicorn server:app --host 127.0.0.1 --port 3010
```

Mirror `ecosystem.config.cjs`: a `run_with_env.sh` wrapper, `autorestart: true`,
`max_restarts: 50`, `cwd` the deployed dir. Bind `127.0.0.1` only (the worker
calls it locally; it is not publicly exposed). The first start downloads the
~1.2 GB mmlw weights. Confirm the chosen port (3010 default) is free on tytan
at deploy time — the worker API runs on 3003.

Phase 2 then wires the worker to call this sidecar as the OpenAI fallback.
