# bagent V2 — Competitor Report + Optimization Pipeline

## Overview

Rozszerzenie bagenta o dwa nowe pipeline'y, oba delegowane z Convex jak audit pipeline:

1. **Competitor Report** — pełny raport konkurencji (17 sekcji) z danymi z Supabase + AI analysis
2. **Optimization Pipeline** — AI-driven optymalizacja cennika na bazie raportu audytowego

Oba korzystają z tego samego MiniMax M2.7 agent loop + tool_use pattern co audit pipeline.

## Current State

bagent ma:
- `POST /api/analyze` — audit pipeline (10 steps, działa end-to-end)
- Agent runner z tool_use (multi-turn, MiniMax M2.7)
- Supabase client (read/write reports)
- Convex webhook client (progress/complete/fail)
- Job store z SSE dashboard

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

---

## Part 1: Competitor Report Pipeline

### Trigger

Convex `generateCompetitorReport` action → POST `/api/competitor`:

```json
{
  "auditId": "j57...",
  "userId": "jh7...",
  "subjectSalonId": 100080,
  "salonName": "Instytut Piękna",
  "salonCity": "Płock",
  "salonLat": 52.5465,
  "salonLng": 19.6973,
  "selectedCompetitorIds": [100081, 100082, 100083, 100084, 100085],
  "services": ["Manicure hybrydowy", "Pedicure", ...]
}
```

Returns 202 + jobId. Pipeline runs async, webhooks progress to Convex.

### 12-Step Pipeline

```
Step 1:  Load subject salon data from Supabase (salons + services tables)
Step 2:  Load competitor data (salons + services for each selectedCompetitorId)
Step 3:  Call RPC get_competitor_price_comparison → PriceCategoryComparison[]
Step 4:  Call RPC get_local_salon_rank → LocalRanking
Step 5:  Call RPC get_service_gap_matrix → ServiceGapEntry[]
Step 6:  Call RPC get_competitor_promotions → CompetitorPromotion[]
Step 7:  Call RPC get_per_service_pricing → PerServicePrice[]
Step 8:  AI: Market summary + competitive advantages + recommendations (agent with tool_use)
Step 9:  AI: SWOT analysis (single JSON call)
Step 10: AI: Market niches + segmentation + strategic recommendations + action plan (agent with tool_use)
Step 11: Assemble CompetitorReportData (all 17 sections)
Step 12: Save to Supabase competitor_reports + call Convex webhook
```

### Step Details

**Steps 1-2: Data Loading (pure Supabase queries)**

```python
# Step 1
subject = await supabase.get_salon_with_services(subject_salon_id)
# Returns: { salon: {...}, services: [...], categories: [...] }

# Step 2  
competitors = []
for comp_id in selected_competitor_ids:
    comp = await supabase.get_salon_with_services(comp_id)
    competitors.append(comp)
```

New methods in `services/supabase.py`:
- `get_salon_with_services(salon_id)` — joins salons + services + variants
- `get_salon_basic(salon_id)` — just salon metadata (for profiles)

**Steps 3-7: Supabase RPCs (no AI)**

Each step calls an existing RPC and transforms the result:

```python
# Step 3
price_comparison = await supabase.rpc("get_competitor_price_comparison", {
    "p_salon_ids": selected_competitor_ids,
    "p_subject_salon_id": subject_salon_id,
})
# Returns: PriceCategoryComparison[] with positioning (below_p25, p25_p50, etc.)

# Step 4
local_ranking = await supabase.rpc("get_local_salon_rank", {
    "p_salon_id": subject_salon_id,
})
# Returns: { rank, total_in_city, city, percentile }

# Step 5
gap_matrix = await supabase.rpc("get_service_gap_matrix", {
    "p_subject_salon_id": subject_salon_id,
    "p_competitor_ids": selected_competitor_ids,
})
# Returns: ServiceGapEntry[] (competitor_has / salon_has gaps)

# Step 6
promotions = await supabase.rpc("get_competitor_promotions", {
    "p_competitor_ids": selected_competitor_ids,
})
# Returns: CompetitorPromotion[] with discount_percent

# Step 7
per_service = await supabase.rpc("get_per_service_pricing", {
    "p_subject_salon_id": subject_salon_id,
    "p_competitor_ids": selected_competitor_ids,
})
# Returns: PerServicePrice[] with percentile_position
```

New method in `services/supabase.py`:
- `call_rpc(name, params)` — generic RPC caller, returns list[dict]

**Step 8: AI Market Analysis (agent loop)**

Tool: `submit_market_analysis`

```python
MARKET_ANALYSIS_TOOL = {
    "name": "submit_market_analysis",
    "description": "Wyślij analizę rynkową — podsumowanie, przewagi konkurencyjne, rekomendacje.",
    "input_schema": {
        "type": "object",
        "properties": {
            "marketSummary": {
                "type": "object",
                "properties": {
                    "headline": {"type": "string"},
                    "paragraphs": {"type": "array", "items": {"type": "string"}},
                    "keyInsights": {"type": "array", "items": {"type": "string"}}
                }
            },
            "advantages": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string"},
                        "type": {"type": "string", "enum": ["win", "loss", "neutral"]},
                        "description": {"type": "string"},
                        "impact": {"type": "string", "enum": ["high", "medium", "low"]}
                    }
                }
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "estimatedImpactPln": {"type": "number"},
                        "priority": {"type": "number"},
                        "category": {"type": "string"}
                    }
                }
            },
            "radarMetrics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "metric": {"type": "string"},
                        "salonValue": {"type": "number"},
                        "marketAvg": {"type": "number"}
                    }
                }
            }
        },
        "required": ["marketSummary", "advantages", "recommendations", "radarMetrics"]
    }
}
```

System prompt contains ALL data from steps 1-7 as context. Agent produces one tool call with full analysis.

**Step 9: SWOT Analysis (single JSON call)**

Simple `generate_json()` — no agent loop needed (small output):

```
Input: salon data + competitor summary + price positioning
Output: { strengths: [...], weaknesses: [...], opportunities: [...], threats: [...] }
```

**Step 10: Strategic Analysis (agent loop)**

Tool: `submit_strategic_analysis`

Produces: detailedProfiles, marketNiches, marketSegmentation, strategicRecommendations, actionPlanTimeline.

This is a big output so agent self-decomposes: calls tool multiple times (profiles in batches, then niches, then segmentation, etc).

**Steps 11-12: Assembly + Save**

Combine all data into `CompetitorReportData` dict matching existing TypeScript type. Save to `competitor_reports` table via Supabase upsert (by convex_audit_id).

### New Files

```
pipelines/
  competitor.py          → 12-step pipeline (run_competitor_pipeline)
prompts/
  competitor_market.txt  → System prompt for market analysis agent
  competitor_swot.txt    → Prompt for SWOT analysis
  competitor_strategy.txt → System prompt for strategic analysis agent
agent/
  tools.py               → Add MARKET_ANALYSIS_TOOL, STRATEGIC_ANALYSIS_TOOL
services/
  supabase.py            → Add get_salon_with_services, call_rpc methods
models/
  competitor.py           → CompetitorReportData, all sub-types (Pydantic)
```

### Convex-side Changes

```typescript
// convex/competitor/reportPipeline.ts — rewrite generateCompetitorReport:
export const generateCompetitorReport = internalAction({
  // Instead of running pipeline in Convex, POST to bagent /api/competitor
  // Same fire-and-forget pattern as analyzeWithAI
});

// convex/http.ts — new webhook endpoints:
// POST /api/competitor/progress
// POST /api/competitor/complete  (saves report status in competitor_selections)
// POST /api/competitor/fail
```

### Tests

```
tests/
  test_competitor_pipeline.py
    - test_load_subject_salon (mock Supabase)
    - test_load_competitors (mock Supabase)
    - test_price_comparison_rpc (mock RPC)
    - test_local_ranking_rpc (mock RPC)
    - test_gap_matrix_rpc (mock RPC)
    - test_market_analysis_agent (mock MiniMax, verify tool calls)
    - test_swot_generation (mock MiniMax)
    - test_strategic_analysis_agent (mock MiniMax)
    - test_report_assembly (all sections present, types correct)
    - test_full_pipeline_small (mock everything, verify report shape)
  test_competitor_integration.py (@integration)
    - test_real_supabase_rpcs (salon 100080, real data)
    - test_real_ai_market_analysis (small competitor set)
```

---

## Part 2: Optimization Pipeline

### Trigger

Convex `startOptimization` mutation → POST `/api/optimize`:

```json
{
  "auditId": "j57...",
  "userId": "jh7...",
  "pricelistId": "js7...",
  "jobId": "js7...",
  "scrapedData": { ... },
  "auditReport": { ... },
  "selectedOptions": ["descriptions", "seo", "categories", "order", "tags"],
  "promptTemplates": { ... }
}
```

Returns 202 + jobId.

### 8-Step Pipeline

```
Step 1:  Parse input, load audit report context (issues, transformations, quickWins, SEO keywords)
Step 2:  AI: Category restructuring proposal (agent with tool_use if >10 categories, else single call)
Step 3:  AI: Service optimization per category — names, descriptions, tags (agent with tool_use)
Step 4:  AI: SEO keyword enrichment across all services (agent with tool_use)
Step 5:  Programmatic: Apply price formatting, duration estimation, duplicate detection
Step 6:  AI: Quality verification against audit issues (single call — "which issues are still present?")
Step 7:  Assemble OptimizationResult (optimizedPricingData + changes + summary)
Step 8:  Save to Supabase + Convex webhook (complete job, update pricelist)
```

### Step Details

**Step 1: Context Loading**

Parse audit report and extract optimization context:
```python
context = {
    "topIssues": audit_report["topIssues"],
    "transformations": audit_report["transformations"],  # naming + desc suggestions from audit
    "missingSeoKeywords": audit_report["missingSeoKeywords"],
    "quickWins": audit_report["quickWins"],
    "stats": audit_report["stats"],
}
```

This context is injected into ALL AI prompts so the model knows what problems to fix.

**Step 2: Category Restructuring (conditional agent)**

Only runs if "categories" in selectedOptions.

Tool: `submit_category_mapping`

```python
CATEGORY_MAPPING_TOOL = {
    "name": "submit_category_mapping",
    "description": "Wyślij propozycję nowej struktury kategorii.",
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalCategory": {"type": "string"},
                        "newCategory": {"type": "string"},
                        "services": {"type": "array", "items": {"type": "string"}},
                        "reason": {"type": "string"}
                    }
                }
            }
        }
    }
}
```

Input: full pricelist + audit issues related to structure. Output: category rename/merge/split proposals.

**Step 3: Service Optimization (agent loop — core step)**

Tool: `submit_optimized_services`

```python
OPTIMIZED_SERVICES_TOOL = {
    "name": "submit_optimized_services",
    "description": "Wyślij zoptymalizowane usługi. Wywołuj wielokrotnie — partia po 15-20 usług.",
    "input_schema": {
        "type": "object",
        "properties": {
            "services": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "originalName": {"type": "string"},
                        "categoryName": {"type": "string"},
                        "newName": {"type": "string"},
                        "newDescription": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "sortOrder": {"type": "number"}
                    },
                    "required": ["originalName", "categoryName", "newName"]
                }
            }
        }
    }
}
```

Critical rules enforced in prompt AND post-processing:
- NEVER change price (keep original)
- NEVER change duration (keep original)
- NEVER change variants (keep original)
- Description: 50-200 chars, benefit-focused, no prices in text
- Name: max 80 chars, separator " - " for enrichment
- Tags: Bestseller, Nowość, Premium, Promocja (max 2 per service)

Agent sees full pricelist + audit context + selected options. Self-decomposes by category.

**Step 4: SEO Enrichment (agent loop)**

Only runs if "seo" in selectedOptions.

Reuses `submit_optimized_services` tool. System prompt focuses on SEO keywords from audit's `missingSeoKeywords` list. Agent enriches service names with missing keywords where natural.

**Step 5: Programmatic Fixes**

No AI — pure Python:
```python
- fix_caps_lock() on all descriptions
- clean_service_name() on all names  
- deduplicate_services() — mark exact name duplicates
- format_prices() — ensure consistent "X zł" format
- estimate_durations() — if "duration" in options and service has no duration
- detect_promo_services() — mark services with promo indicators
```

**Step 6: Quality Verification (single JSON call)**

```
Input: original audit issues + optimized data
Prompt: "Które z poniższych problemów z audytu zostały naprawione, a które nadal istnieją?"
Output: { fixed: [...], remaining: [...], qualityScore: 0-100 }
```

If qualityScore < 60 and we haven't retried: re-run Step 3 with refinement prompt containing remaining issues.

**Step 7: Assembly**

Build `OptimizationResult`:
```python
{
    "optimizedPricingData": { categories with optimized services },
    "changes": [ all change records with before/after/reason ],
    "summary": {
        "totalChanges": N,
        "namesImproved": N,
        "descriptionsAdded": N,
        "duplicatesFound": N,
        "categoriesOptimized": N,
    },
    "recommendations": [...],
    "qualityScore": N,
}
```

**Step 8: Save + Webhook**

- Save to Supabase `optimized_pricelists` table (or existing pattern)
- Call Convex webhook: complete optimization job
- Convex updates pricelist record + creates notification + sends email

### New Files

```
pipelines/
  optimization.py          → 8-step pipeline (run_optimization_pipeline)
prompts/
  optimization_categories.txt  → Category restructuring agent prompt
  optimization_services.txt    → Service optimization agent prompt
  optimization_seo.txt         → SEO enrichment agent prompt
  optimization_verify.txt      → Quality verification prompt
agent/
  tools.py                     → Add CATEGORY_MAPPING_TOOL, OPTIMIZED_SERVICES_TOOL
models/
  optimization.py              → OptimizationResult, ChangeRecord, etc. (Pydantic)
```

### Convex-side Changes

```typescript
// convex/optimization/pipelineV2.ts — rewrite runPipelineV2:
export const runPipelineV2 = internalAction({
  // POST to bagent /api/optimize, fire-and-forget
});

// convex/http.ts — new webhook endpoints:
// POST /api/optimization/progress
// POST /api/optimization/complete (updates job + pricelist)
// POST /api/optimization/fail
```

### Tests

```
tests/
  test_optimization_pipeline.py
    - test_context_loading (audit report parsing)
    - test_category_restructuring_agent (mock MiniMax)
    - test_service_optimization_agent (mock MiniMax, verify tool calls)
    - test_seo_enrichment (mock MiniMax)
    - test_programmatic_fixes (caps lock, dedup, price format)
    - test_quality_verification (mock MiniMax)
    - test_never_changes_prices (critical — verify prices untouched)
    - test_never_changes_durations (critical — verify durations untouched)
    - test_never_changes_variants (critical — verify variants untouched)
    - test_report_assembly (changes, summary, qualityScore)
    - test_full_pipeline_small (mock everything)
  test_optimization_integration.py (@integration)
    - test_real_optimization_small_salon (10 services, real MiniMax)
```

---

## Part 3: Server Changes

### New Endpoints in `server.py`

```python
@app.post("/api/competitor", status_code=202)
async def start_competitor_report(request: CompetitorRequest, background_tasks: BackgroundTasks):
    """Start async competitor report generation."""
    job = job_store.create(request.auditId, meta={"type": "competitor", ...})
    background_tasks.add_task(run_competitor_job, job.job_id, request)
    return {"jobId": job.job_id, "status": "accepted"}

@app.post("/api/optimize", status_code=202)
async def start_optimization(request: OptimizationRequest, background_tasks: BackgroundTasks):
    """Start async pricelist optimization."""
    job = job_store.create(request.auditId, meta={"type": "optimization", ...})
    background_tasks.add_task(run_optimization_job, job.job_id, request)
    return {"jobId": job.job_id, "status": "accepted"}
```

### Request Models

```python
class CompetitorRequest(BaseModel):
    auditId: str
    userId: str
    subjectSalonId: int
    salonName: str
    salonCity: str
    salonLat: float | None = None
    salonLng: float | None = None
    selectedCompetitorIds: list[int]
    services: list[str]  # subject salon service names for gap analysis

class OptimizationRequest(BaseModel):
    auditId: str
    userId: str
    pricelistId: str
    jobId: str
    scrapedData: ScrapedData
    auditReport: dict  # EnhancedAuditReport as dict
    selectedOptions: list[str]  # ["descriptions", "seo", "categories", ...]
    promptTemplates: dict | None = None  # admin overrides
```

---

## Part 4: Supabase Client Extensions

New methods for `services/supabase.py`:

```python
# Competitor data
async def get_salon_with_services(self, salon_id: int) -> dict
async def get_salon_basic(self, salon_id: int) -> dict
async def call_rpc(self, rpc_name: str, params: dict) -> list[dict]
async def save_competitor_report(self, convex_audit_id: str, report_data: dict, ...) -> int

# Optimization data
async def save_optimized_pricelist(self, convex_audit_id: str, data: dict) -> None
async def get_service_photos(self, salon_id: int) -> dict[str, str]  # service_name → photo_url
```

---

## Part 5: Convex Webhook Extensions

New endpoints in `convex/http.ts`:

```
POST /api/competitor/progress   → updates audit progress (reuse updateProgress mutation)
POST /api/competitor/complete   → saves competitor report status
POST /api/competitor/fail       → marks competitor report as failed

POST /api/optimization/progress → updates job progress
POST /api/optimization/complete → completes optimization job + updates pricelist
POST /api/optimization/fail     → fails optimization job
```

---

## Implementation Order

### Phase 1: Competitor Report (priority — upsell product)
1. Pydantic models (`models/competitor.py`)
2. Supabase extensions (RPC calls, salon data loading)
3. Prompts (market analysis, SWOT, strategy)
4. Tool definitions (MARKET_ANALYSIS_TOOL, STRATEGIC_ANALYSIS_TOOL)
5. Pipeline (`pipelines/competitor.py`)
6. Server endpoint + background job
7. Tests (unit + integration)
8. Convex-side: rewrite `generateCompetitorReport` to fire-and-forget
9. Convex HTTP webhook endpoints
10. End-to-end test

### Phase 2: Optimization Pipeline
1. Pydantic models (`models/optimization.py`)
2. Prompts (categories, services, SEO, verify)
3. Tool definitions (CATEGORY_MAPPING_TOOL, OPTIMIZED_SERVICES_TOOL)
4. Pipeline (`pipelines/optimization.py`)
5. Server endpoint + background job
6. Tests (unit + integration, especially price/duration/variant immutability)
7. Convex-side: rewrite `runPipelineV2` to fire-and-forget
8. Convex HTTP webhook endpoints
9. End-to-end test

### Phase 3: Polish & Monitoring
1. Dashboard updates (show competitor + optimization jobs)
2. Error handling hardening
3. Retry logic for failed steps
4. Performance optimization (parallel Supabase RPC calls where independent)

---

## Token Budget Estimates

### Competitor Report
- Steps 1-7 (Supabase): 0 tokens, ~5s total
- Step 8 (market analysis agent): ~30K input + ~5K output = ~35K tokens
- Step 9 (SWOT): ~10K input + ~3K output = ~13K tokens
- Step 10 (strategy agent): ~40K input + ~10K output = ~50K tokens (multi-turn)
- Total: ~100K tokens per report, well within 200K context window
- Estimated time: 5-8 minutes
- Estimated cost: ~$0.10-0.15 per report (MiniMax pricing)

### Optimization Pipeline
- Step 2 (categories): ~15K tokens (if needed)
- Step 3 (services agent): ~50K tokens (full pricelist + audit context, multi-turn)
- Step 4 (SEO): ~30K tokens (if needed)
- Step 6 (verify): ~15K tokens
- Total: ~80-110K tokens per optimization
- Estimated time: 8-15 minutes (depends on salon size)
- Estimated cost: ~$0.08-0.15 per optimization

---

## Risk Mitigation

1. **MiniMax instability (500 errors)**: Already handled — `with_retry` in minimax.py, fallbacks in pipeline steps
2. **Large salons (200+ services)**: Agent loop self-decomposes, no batching limits
3. **Supabase RPC failures**: Each RPC wrapped in try/catch, partial data still produces report
4. **Convex webhook failures**: bagent retries webhook 3x, logs error, marks job as completed-with-warnings
5. **Price mutation in optimization**: Triple protection — prompt rules, post-processing validation, unit tests that verify prices are NEVER changed
