# Optimization Pipeline Redesign — 4-Phase Sequential Architecture

Date: 2026-04-05

## Problem

Current optimization pipeline is a monolithic 614-line function that runs all steps in one call. SEO happens AFTER content optimization (wrong order), categories are optional and disconnected, and there's no user intervention point between phases. Quality verification gets dry statistics instead of actual before/after data.

## Target Architecture

4 sequential HTTP endpoints in bagent, each self-contained:

```
POST /api/optimize/seo          → Faza 1: SEO keyword injection into names
POST /api/optimize/content      → Faza 2: Copywriting (names + descriptions)
POST /api/optimize/categories   → Faza 3: Category reorganization
POST /api/optimize/finalize     → Faza 4: Apply user edits → final pricelist + diff
```

Each phase:
- Is a separate synchronous endpoint (returns result, not fire-and-forget)
- Loads audit report from Supabase itself (self-sufficient)
- Accepts pricelist from previous phase as input
- Returns modified pricelist as output
- Never changes prices, durations, or variants

### Phase 1: `/api/optimize/seo`

Request: `{ auditId: string }`

Loads from Supabase:
- `audit_scraped_data` (original pricelist)
- `audit_reports` via RPC (missingSeoKeywords)

AI task: Inject missing SEO keywords into service names. Don't touch descriptions or categories.

Response:
```json
{
  "pricelist": { "salonName": "...", "categories": [...] },
  "seoChanges": [{ "serviceName": "...", "before": "...", "after": "...", "keyword": "..." }],
  "keywordsAdded": 12
}
```

### Phase 2: `/api/optimize/content`

Request: `{ auditId: string, pricelist: {...} }` — pricelist is output of phase 1.

Loads from Supabase:
- `audit_reports` (topIssues, transformations as baseline suggestions)

AI task: Rewrite names (copywriting, benefit language) and add/improve descriptions. Uses audit transformations as a starting point. Operates on SEO-enriched names from phase 1.

Response:
```json
{
  "pricelist": { "salonName": "...", "categories": [...] },
  "contentChanges": [{ "serviceName": "...", "type": "name|description", "before": "...", "after": "..." }],
  "namesImproved": 18,
  "descriptionsAdded": 25
}
```

### Phase 3: `/api/optimize/categories`

Request: `{ auditId: string, pricelist: {...} }` — pricelist is output of phase 2.

Loads from Supabase:
- `audit_reports` (structure/ux issues, quick wins)

AI task: Reorganize categories — merge small (<3 services), split large (>20), rename for clarity, reorder logically. Operates on already-optimized names and descriptions.

Response:
```json
{
  "pricelist": { "salonName": "...", "categories": [...] },
  "categoryChanges": [{ "type": "merge|split|rename|reorder", "from": "...", "to": "...", "reason": "..." }]
}
```

This output goes to UI. User reviews, edits categories if needed, then submits to phase 4.

### Phase 4: `/api/optimize/finalize`

Request: `{ auditId: string, pricelist: {...} }` — pricelist is phase 3 output with user edits applied.

No AI calls. Deterministic:
1. Programmatic fixes: CAPS lock, service name cleanup, duplicate detection, promo tagging
2. Diff generation: compare final pricelist vs original (from Supabase) → per-service change list
3. Quality scoring: check how many audit issues were resolved
4. Save to Supabase: `optimized_pricelists` + children

Response:
```json
{
  "finalPricelist": { "salonName": "...", "categories": [...] },
  "changes": [{ "type": "name|description|category", "serviceName": "...", "before": "...", "after": "...", "reason": "..." }],
  "summary": { "totalChanges": 62, "namesImproved": 18, "descriptionsAdded": 25, "categoriesOptimized": 4, "duplicatesFound": 3, "seoKeywordsAdded": 12 },
  "qualityScore": 78
}
```

## Backwards Compatibility

Existing `/api/optimize` endpoint stays as a wrapper that runs phases 1→2→3→4 sequentially without user intervention. Same request/response contract. Used by legacy Convex delegation code until frontend migrates to per-phase calls.

## Reused Code

From current `pipelines/optimization.py`:
- `_find_optimization()`, `_detect_promo()`, `_normalize_item()` → phase 4 (finalize)
- `clean_service_name()`, `fix_caps_lock()` from helpers → phase 4
- `build_full_pricelist_text()` from helpers → phases 1, 2, 3
- `OPTIMIZED_SERVICES_TOOL` → phases 1 and 2
- `CATEGORY_MAPPING_TOOL` → phase 3
- `optimization_services.txt` prompt → split into SEO prompt (phase 1) + content prompt (phase 2)
- `optimization_categories.txt` prompt → phase 3 (unchanged)
- `optimization_verify.txt` prompt → removed (quality check in phase 4 is deterministic diff, no AI)
- Agent runner (`run_agent_loop`) → phases 1, 2, 3
- Supabase save logic → phase 4
- Assembly logic (building optimized pricelist structure) → phase 4

## New Prompts

`prompts/optimization_seo.txt` — phase 1: "Wstrzyknij brakujące słowa kluczowe SEO do nazw usług..."
`prompts/optimization_content.txt` — phase 2: "Przepisz nazwy i opisy usług, język korzyści..." (uses audit transformations as baseline)

## File Structure

```
pipelines/
  optimization.py          → refactored: 4 functions + legacy wrapper
                              run_phase1_seo()
                              run_phase2_content()
                              run_phase3_categories()
                              run_phase4_finalize()
                              run_optimization_pipeline()  ← legacy wrapper

server.py                  → 4 new endpoints + keep existing /api/optimize
```

## Data Flow

```
Supabase audit_scraped_data ──────────────────────────────────────┐
Supabase audit_reports ──────────────────────────────────┐        │
                                                         │        │
Phase 1 (SEO):        scraped_data + missingSeoKeywords ─┼→ pricelist_v1
Phase 2 (Content):    pricelist_v1 + issues/transforms ──┼→ pricelist_v2
Phase 3 (Categories): pricelist_v2 + structure issues ───┘→ pricelist_v3
                                                              │
                              ┌── User edits in UI ──────────┘
                              ▼
Phase 4 (Finalize):   pricelist_v3_edited + scraped_data → final + diff + save
```
