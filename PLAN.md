# bagent — Beauty Audit AI Analyzer

Python FastAPI service that runs AI analysis pipelines for BeautyAudit using MiniMax M2.7 with tool_use agent loops. Replaces Convex-side AI analysis (600s timeout limit).

## Architecture

```
Convex (analyzeWithAI) 
  → POST /api/analyze { auditId, scrapedData }
  → bagent returns 202 Accepted immediately
  → bagent runs pipeline async (no time limit)
  → bagent saves report to Supabase
  → bagent calls Convex HTTP webhook to complete audit
  → Convex updates audit status + progress
```

## Project Structure

```
bagent/
├── pyproject.toml              # Dependencies, project config (uv)
├── .env.example                # Environment variables template
├── .gitignore
├── README.md
├── server.py                   # FastAPI app entry point
├── config.py                   # Settings from env vars
├── agent/
│   ├── __init__.py
│   ├── runner.py               # Generic agent loop (multi-turn tool_use)
│   └── tools.py                # Tool definitions (naming, descriptions)
├── pipelines/
│   ├── __init__.py
│   ├── audit.py                # Audit report pipeline (naming → descriptions → structure → summary → competitors → assembly)
│   ├── optimization.py         # Pricelist optimization pipeline (future)
│   └── competitor.py           # Competitor report pipeline (future)
├── services/
│   ├── __init__.py
│   ├── minimax.py              # MiniMax M2.7 client (anthropic SDK wrapper)
│   ├── supabase.py             # Supabase client (read scraped data, write reports)
│   └── convex.py               # Convex HTTP client (progress updates, audit completion)
├── models/
│   ├── __init__.py
│   ├── scraped_data.py         # ScrapedData pydantic model
│   ├── report.py               # EnhancedAuditReport pydantic model
│   └── analysis.py             # Intermediate analysis result models
├── prompts/
│   ├── naming_score.txt        # Prompt for naming score + issues
│   ├── naming_agent.txt        # System prompt for naming agent
│   ├── descriptions_score.txt  # Prompt for descriptions score + issues
│   ├── descriptions_agent.txt  # System prompt for descriptions agent
│   ├── structure.txt           # Prompt for structure/pricing analysis
│   └── summary.txt             # Prompt for summary generation
└── tests/
    ├── __init__.py
    ├── conftest.py             # Shared fixtures (sample scraped data, mock MiniMax)
    ├── test_agent_runner.py    # Agent loop unit tests
    ├── test_audit_pipeline.py  # Full pipeline integration test
    ├── test_minimax_client.py  # MiniMax API integration test
    ├── test_supabase.py        # Supabase read/write test
    └── test_api.py             # FastAPI endpoint tests
```

## Dependencies

```toml
[project]
name = "bagent"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn>=0.34.0",
    "anthropic>=0.52.0",
    "supabase>=2.15.0",
    "httpx>=0.28.0",
    "pydantic>=2.10.0",
    "pydantic-settings>=2.7.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.25.0",
    "pytest-httpx>=0.35.0",
    "ruff>=0.9.0",
]
```

## Configuration (.env)

```bash
# MiniMax
MINIMAX_API_KEY=sk-cp-...
MINIMAX_BASE_URL=https://api.minimax.io/anthropic
MINIMAX_MODEL=MiniMax-M2.7

# Supabase
SUPABASE_URL=https://462c-83-175-179-98.ngrok-free.app
SUPABASE_SERVICE_KEY=eyJ...

# Convex (for webhook callbacks)
CONVEX_URL=https://reliable-scorpion-10.convex.cloud  # dev
CONVEX_DEPLOY_KEY=...  # for calling internal HTTP endpoints

# Server
API_KEY=...  # x-api-key for authenticating requests from Convex
PORT=3001
```

## Modules — detailed spec

### 1. `config.py` — Settings

Pydantic Settings class loading from .env:

```python
class Settings(BaseSettings):
    minimax_api_key: str
    minimax_base_url: str = "https://api.minimax.io/anthropic"
    minimax_model: str = "MiniMax-M2.7"
    supabase_url: str
    supabase_service_key: str
    convex_url: str
    convex_deploy_key: str = ""
    api_key: str
    port: int = 3001
```

### 2. `services/minimax.py` — MiniMax Client

Thin wrapper around `anthropic.AsyncAnthropic`:

```python
class MiniMaxClient:
    def __init__(self, api_key, base_url, model):
        self.client = anthropic.AsyncAnthropic(
            base_url=base_url,
            api_key=api_key,
            default_headers={"Authorization": f"Bearer {api_key}"},
        )
        self.model = model

    async def create_message(self, system, messages, tools=None, max_tokens=16384):
        """Raw Anthropic-compatible message creation."""
        ...

    async def generate_json(self, prompt, system=None, max_tokens=16384) -> dict:
        """Simple JSON generation (no tools)."""
        ...

    async def generate_text(self, prompt, system=None, max_tokens=4096) -> str:
        """Simple text generation."""
        ...
```

Key: uses official `anthropic` SDK — no raw fetch, proper timeouts, retry built in.

### 3. `agent/runner.py` — Agent Loop

Port of the Convex agentRunner but in Python, async, with proper error handling:

```python
@dataclass
class ToolCallResult:
    name: str
    input: dict

@dataclass
class AgentResult:
    tool_calls: list[ToolCallResult]
    final_text: str
    total_steps: int
    total_tokens: int

async def run_agent_loop(
    client: MiniMaxClient,
    system_prompt: str,
    user_message: str,
    tools: list[dict],
    max_steps: int = 30,
    on_step: Callable | None = None,
) -> AgentResult:
    """Multi-turn agent loop with tool_use.

    1. Send task + tools to M2.7
    2. M2.7 thinks (Interleaved Thinking) + calls tool with batch of results
    3. We acknowledge, append full response (with thinking) to conversation
    4. M2.7 continues until stop_reason="end_turn"
    5. Return all collected tool call results
    """
```

Critical implementation detail from MiniMax docs: append FULL `response.content` (including thinking blocks) to message history to maintain reasoning chain.

### 4. `agent/tools.py` — Tool Definitions

```python
NAMING_TOOL = {
    "name": "submit_naming_results",
    "description": "Wyślij poprawione nazwy usług. Wywołuj wielokrotnie z partiami po 15-20.",
    "input_schema": {
        "type": "object",
        "properties": {
            "transformations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "improved": {"type": "string"},
                    },
                    "required": ["name", "improved"],
                },
            },
        },
        "required": ["transformations"],
    },
}

DESCRIPTION_TOOL = {
    "name": "submit_description_results",
    "description": "Wyślij poprawione opisy usług. Wywołuj wielokrotnie z partiami po 15-20.",
    "input_schema": {
        "type": "object",
        "properties": {
            "transformations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "serviceName": {"type": "string"},
                        "newDescription": {"type": "string"},
                    },
                    "required": ["serviceName", "newDescription"],
                },
            },
        },
        "required": ["transformations"],
    },
}
```

### 5. `services/supabase.py` — Supabase Client

```python
class SupabaseService:
    async def get_scraped_data(self, convex_audit_id: str) -> ScrapedData:
        """Read scraped data from audit_scraped_data table."""

    async def save_report(self, convex_audit_id: str, convex_user_id: str,
                          report: EnhancedAuditReport, salon_name: str,
                          salon_address: str, source_url: str) -> str:
        """Save report to audit_reports table. Returns report ID."""

    async def get_benchmarks(self, city: str | None) -> dict:
        """Get industry comparison data."""

    async def get_competitors(self, lat: float, lng: float, radius_km: int,
                              service_names: list[str]) -> dict:
        """Get competitor context from salons table."""

    async def geocode_salon(self, salon_name: str, address: str) -> tuple[float, float] | None:
        """Find salon coordinates by name/address match."""
```

### 6. `services/convex.py` — Convex Webhook Client

```python
class ConvexClient:
    async def update_progress(self, audit_id: str, progress: int, message: str):
        """Call Convex HTTP endpoint to update audit progress."""

    async def complete_audit(self, audit_id: str, score: int, report_stats: dict):
        """Call Convex HTTP endpoint to mark audit as completed."""

    async def fail_audit(self, audit_id: str, error_message: str):
        """Call Convex HTTP endpoint to mark audit as failed."""
```

Requires a Convex HTTP route (added to `convex/http.ts`) that accepts these webhook calls with API key auth.

### 7. `pipelines/audit.py` — Audit Report Pipeline

The core pipeline. Steps:

```python
async def run_audit_pipeline(
    scraped_data: ScrapedData,
    audit_id: str,
    on_progress: Callable,
) -> EnhancedAuditReport:
    """Full audit analysis pipeline.

    Step 1: Calculate stats (pure Python, no AI)
    Step 2: Naming score + issues (single MiniMax call)
    Step 3: Naming transformations (agent loop with tool_use)
    Step 4: Descriptions score + issues (single MiniMax call)
    Step 5: Descriptions transformations (agent loop with tool_use)
    Step 6: Structure & pricing analysis (single MiniMax call)
    Step 7: Summary generation (single MiniMax call)
    Step 8: Competitor context (Supabase queries)
    Step 9: Quality validation + optional refinement
    Step 10: Assemble EnhancedAuditReport
    """
```

Each step calls `on_progress(progress_pct, "message")` for UI updates.

Programmatic pre-processing (before AI):
- `clean_service_name()` — trailing dots, spacing around +/-
- `fix_caps_lock()` — sentence case for ALL CAPS text
- `validate_name_transformation()` — reject invalid AI transforms

### 8. `models/` — Pydantic Models

Port of TypeScript types to Pydantic:

```python
# scraped_data.py
class ServiceVariant(BaseModel):
    label: str
    price: str
    duration: str | None = None

class ScrapedService(BaseModel):
    name: str
    price: str
    duration: str | None = None
    description: str | None = None
    imageUrl: str | None = None
    variants: list[ServiceVariant] | None = None

class ScrapedCategory(BaseModel):
    name: str
    services: list[ScrapedService]

class ScrapedData(BaseModel):
    salonName: str | None = None
    salonAddress: str | None = None
    salonLogoUrl: str | None = None
    categories: list[ScrapedCategory]
    totalServices: int

# report.py
class ScoreBreakdown(BaseModel):
    completeness: int
    naming: int
    descriptions: int
    structure: int
    pricing: int
    seo: int
    ux: int

class AuditIssue(BaseModel):
    severity: Literal["critical", "major", "minor"]
    dimension: str
    issue: str
    impact: str
    affectedCount: int
    example: str
    fix: str

class ServiceTransformation(BaseModel):
    type: Literal["name", "description"]
    serviceName: str
    before: str
    after: str
    reason: str
    impactScore: int

class EnhancedAuditReport(BaseModel):
    version: str = "v2"
    totalScore: int
    scoreBreakdown: ScoreBreakdown
    stats: dict  # AuditStats — complex, keep as dict
    topIssues: list[AuditIssue]
    transformations: list[ServiceTransformation]
    missingSeoKeywords: list[dict]
    quickWins: list[dict]
    industryComparison: dict | None = None
    competitorContext: dict | None = None
    competitors: list[dict] | None = None
    salonLocation: dict | None = None
    summary: str
```

### 9. `server.py` — FastAPI App

```python
app = FastAPI(title="bagent — Beauty Audit AI Analyzer")

@app.post("/api/analyze", status_code=202)
async def start_analysis(request: AnalyzeRequest, background_tasks: BackgroundTasks):
    """Start async audit analysis. Returns immediately with job ID."""
    job_id = str(uuid4())
    background_tasks.add_task(run_analysis_job, job_id, request)
    return {"jobId": job_id, "status": "accepted"}

@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Check job status (optional — Convex gets webhook)."""
    ...

@app.get("/api/health")
async def health():
    """Health check."""
    return {"status": "ok"}
```

`AnalyzeRequest`:
```python
class AnalyzeRequest(BaseModel):
    auditId: str           # Convex audit ID
    userId: str            # Convex user ID
    sourceUrl: str | None  # Original Booksy URL
    scrapedData: ScrapedData  # Full scraped data (OR)
    # OR: convexAuditId to load from Supabase
```

### 10. `prompts/` — Prompt Templates

Text files loaded at startup. Variables injected at runtime via `.format()` or f-string.

Key prompts (port from analysisSteps.ts):
- `naming_score.txt` — "Oceń JAKOŚĆ NAZW usług. Skala 0-20. Zwróć TYLKO ocenę + zagregowane problemy."
- `naming_agent.txt` — "Popraw nazwy usług. Użyj narzędzia submit_naming_results. Wywołuj wielokrotnie."
- `descriptions_score.txt` — "Oceń JAKOŚĆ OPISÓW. Skala 0-20. TYLKO ocenę + problemy."
- `descriptions_agent.txt` — "Przepisz opisy usług. Użyj submit_description_results. Partiami po 15-20."
- `structure.txt` — "Oceń STRUKTURĘ (0-15) i CENĘ (0-15). Podaj WSZYSTKIE problemy, quick wins, SEO."
- `summary.txt` — "Napisz 2-3 zdania podsumowania audytu."

## Convex-side changes (in BEAUTY_AUDIT repo)

### 1. New HTTP endpoints in `convex/http.ts`

```typescript
// POST /api/audit/progress — bagent calls to update progress
http.route({
  path: "/api/audit/progress",
  method: "POST",
  handler: httpAction(async (ctx, req) => {
    // Verify API key, parse body { auditId, progress, progressMessage }
    // Call internal.audit.internalMutations.updateProgress
  }),
});

// POST /api/audit/complete — bagent calls when done
http.route({
  path: "/api/audit/complete",
  method: "POST",
  handler: httpAction(async (ctx, req) => {
    // Verify API key, parse body { auditId, overallScore, reportStats }
    // Call internal.audit.internalMutations.completeAudit
  }),
});

// POST /api/audit/fail — bagent calls on error
http.route({
  path: "/api/audit/fail",
  method: "POST",
  handler: httpAction(async (ctx, req) => {
    // Verify API key, parse body { auditId, errorMessage }
    // Call internal.audit.internalMutations.failAudit
  }),
});
```

### 2. Modified `analyzeWithAI` in `convex/audit/analysis.ts`

Becomes thin fire-and-forget:

```typescript
export const analyzeWithAI = internalAction({
  args: { auditId: v.id("audits") },
  returns: v.null(),
  handler: async (ctx, args) => {
    // Load scraped data from Supabase
    // POST to bagent /api/analyze
    // Return immediately — bagent handles the rest via webhooks
  },
});
```

## Tests

### `tests/conftest.py` — Fixtures

```python
@pytest.fixture
def sample_scraped_data() -> ScrapedData:
    """Salon with 13 categories, ~30 services (small, fast for tests)."""

@pytest.fixture
def large_scraped_data() -> ScrapedData:
    """Salon 100080 with 169 services (real data from Supabase)."""

@pytest.fixture
def mock_minimax(httpx_mock):
    """Mock MiniMax API responses for unit tests."""
```

### `tests/test_agent_runner.py`

1. `test_agent_completes_on_end_turn` — mock M2.7 returning text without tool_use → loop exits
2. `test_agent_collects_tool_calls` — mock M2.7 calling tool 3 times → verify all collected
3. `test_agent_maintains_conversation_history` — verify thinking blocks preserved
4. `test_agent_respects_max_steps` — mock infinite tool calls → verify max_steps limit
5. `test_agent_handles_api_error` — mock 500 → verify error propagation

### `tests/test_minimax_client.py` (integration — needs API key)

1. `test_basic_text_generation` — simple Polish prompt → response
2. `test_json_generation` — prompt for JSON → valid JSON returned
3. `test_tool_use_single_turn` — 3 services + tool → tool_use response with transformations
4. `test_tool_use_multi_turn` — send tool_result → M2.7 continues or ends
5. `test_timeout_handling` — verify timeout doesn't hang

Mark with `@pytest.mark.integration` — skip in CI without API key.

### `tests/test_audit_pipeline.py`

1. `test_calculate_stats` — pure function, no AI
2. `test_naming_score_call` — mock MiniMax → returns score + issues
3. `test_naming_agent_loop` — mock MiniMax multi-turn → collects transformations
4. `test_descriptions_agent_loop` — same for descriptions
5. `test_structure_call` — mock MiniMax → returns structure analysis
6. `test_full_pipeline_small_salon` — 10 services, mocked MiniMax → full report assembled
7. `test_full_pipeline_real` — integration test with real MiniMax, small salon data

### `tests/test_supabase.py` (integration)

1. `test_get_scraped_data` — read real data for known audit
2. `test_save_and_read_report` — write report, read back, verify

### `tests/test_api.py`

1. `test_health_endpoint` — GET /api/health → 200
2. `test_analyze_returns_202` — POST /api/analyze → 202 Accepted with jobId
3. `test_analyze_requires_api_key` — no x-api-key → 401
4. `test_analyze_validates_input` — missing fields → 422

## Verification checklist

After implementation:

- [ ] `uv run pytest tests/ -v` — all unit tests pass
- [ ] `uv run pytest tests/ -v -m integration` — MiniMax + Supabase integration tests pass
- [ ] `uv run python server.py` — server starts on port 3001
- [ ] `curl localhost:3001/api/health` — returns 200
- [ ] `curl -X POST localhost:3001/api/analyze -H "x-api-key: ..." -d '{...}'` — returns 202
- [ ] Full pipeline test: Convex triggers analysis → bagent processes → report in Supabase → audit completed
- [ ] Salon 100080 (169 services) completes without timeout
- [ ] Progress updates visible in frontend during processing
- [ ] Error handling: MiniMax down → audit marked as failed in Convex

## Deployment

Same server as bextract (api.booksyaudit.pl):

```bash
# On server
cd /opt/bagent
git pull
uv sync
uv run uvicorn server:app --host 0.0.0.0 --port 3001

# Reverse proxy (nginx)
location /api/analyze { proxy_pass http://127.0.0.1:3001; }
location /api/jobs    { proxy_pass http://127.0.0.1:3001; }
location /api/health  { proxy_pass http://127.0.0.1:3001; }
```

Or with systemd service for auto-restart.
