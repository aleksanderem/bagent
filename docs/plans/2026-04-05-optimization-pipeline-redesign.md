# Optimization Pipeline Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monolithic optimization pipeline with 4 sequential phase endpoints, each self-sufficient (loads data from Supabase), cascading pricelist output from one phase to the next.

**Architecture:** 4 new sync endpoints in server.py call 4 functions in optimization.py. Each phase loads audit report from Supabase, accepts pricelist from previous phase, returns modified pricelist. Legacy `/api/optimize` stays as a wrapper. Phase 4 is deterministic (no AI).

**Tech Stack:** Python 3.12, FastAPI, MiniMax M2.7 (via agent runner), Supabase, Pydantic

---

### Task 1: Create SEO prompt template

**Files:**
- Create: `prompts/optimization_seo.txt`

- [ ] **Step 1: Write SEO prompt**

```
Jesteś ekspertem SEO dla salonów beauty na platformie Booksy.

ZADANIE: Wstrzyknij brakujące słowa kluczowe SEO do nazw usług.
Użyj narzędzia submit_optimized_services partiami po 15-20 usług.

BRAKUJĄCE SŁOWA KLUCZOWE SEO:
{seo_keywords}

AKTUALNY CENNIK:
{pricelist_text}

ZASADY:
- Dodaj słowo kluczowe do nazwy przez separator " - " lub " / "
- Nie zmieniaj znaczenia nazwy, tylko wzbogać o keyword
- Nie zmieniaj opisów — to będzie w następnym kroku
- NIGDY nie zmieniaj ceny, czasu trwania ani wariantów
- Nazwa max 80 znaków
- Przetwórz WSZYSTKIE usługi — nawet te bez zmian (wyślij z oryginalną nazwą)
```

- [ ] **Step 2: Commit**

```bash
git add prompts/optimization_seo.txt
git commit -m "feat: add SEO-specific prompt for phase 1"
```

---

### Task 2: Create content optimization prompt template

**Files:**
- Create: `prompts/optimization_content.txt`

- [ ] **Step 1: Write content prompt**

```
Jesteś copywriterem specjalizującym się w cennikach salonów beauty w Polsce.

ZADANIE: Popraw nazwy i opisy usług — język korzyści, czytelność, spójność.
Użyj narzędzia submit_optimized_services partiami po 15-20 usług.

AKTUALNY CENNIK (nazwy już zawierają słowa kluczowe SEO z poprzedniego kroku):
{pricelist_text}

PROBLEMY Z AUDYTU DO NAPRAWIENIA:
{audit_issues_text}

SUGEROWANE TRANSFORMACJE Z AUDYTU (użyj jako bazę):
{transformations_text}

ZASADY KRYTYCZNE:
- NIGDY nie zmieniaj CENY, CZASU TRWANIA ani WARIANTÓW
- Zachowaj słowa kluczowe SEO które już są w nazwie
- Nazwa: max 80 znaków, separator " - " dla wzbogacenia
- Opis: 50-200 znaków, korzyść dla klienta, bez cen w tekście
- Tagi: max 2 na usługę (Bestseller, Nowość, Premium, Promocja)
- Przetwórz WSZYSTKIE usługi — nawet te które są OK
```

- [ ] **Step 2: Commit**

```bash
git add prompts/optimization_content.txt
git commit -m "feat: add content-specific prompt for phase 2"
```

---

### Task 3: Write phase functions + tests

**Files:**
- Create: `pipelines/optimize_phases.py`
- Create: `tests/test_optimize_phases.py`

This is the core file with 4 async functions: `run_phase1_seo`, `run_phase2_content`, `run_phase3_categories`, `run_phase4_finalize`. Each is self-contained.

- [ ] **Step 1: Write tests for phase 4 (finalize — deterministic, no AI, easiest to test)**

```python
"""Tests for optimization phase functions."""

import os
os.environ["API_KEY"] = "test-api-key-12345"

import pytest
from pipelines.optimize_phases import run_phase4_finalize


def _make_pricelist(categories):
    return {
        "salonName": "Test Salon",
        "salonAddress": "ul. Testowa 1",
        "salonLogoUrl": None,
        "totalServices": sum(len(c["services"]) for c in categories),
        "categories": categories,
    }


def _make_original():
    return _make_pricelist([
        {
            "name": "Fryzjerstwo",
            "services": [
                {"name": "Strzyżenie damskie", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None},
                {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
            ],
        },
    ])


class TestPhase4Finalize:
    @pytest.mark.asyncio
    async def test_no_changes_returns_empty_diff(self):
        original = _make_original()
        result = await run_phase4_finalize("test-audit", original, original)
        assert result["changes"] == []
        assert result["summary"]["totalChanges"] == 0

    @pytest.mark.asyncio
    async def test_name_change_detected(self):
        original = _make_original()
        modified = _make_pricelist([
            {
                "name": "Fryzjerstwo",
                "services": [
                    {"name": "Strzyżenie damskie - precyzyjne cięcie", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None},
                    {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
                ],
            },
        ])
        result = await run_phase4_finalize("test-audit", modified, original)
        assert result["summary"]["namesImproved"] == 1
        assert any(c["type"] == "name" for c in result["changes"])

    @pytest.mark.asyncio
    async def test_description_change_detected(self):
        original = _make_original()
        modified = _make_pricelist([
            {
                "name": "Fryzjerstwo",
                "services": [
                    {"name": "Strzyżenie damskie", "price": "120 zł", "duration": "45 min", "description": "Profesjonalne cięcie dostosowane do kształtu twarzy", "imageUrl": None, "variants": None},
                    {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
                ],
            },
        ])
        result = await run_phase4_finalize("test-audit", modified, original)
        assert result["summary"]["descriptionsAdded"] == 1

    @pytest.mark.asyncio
    async def test_category_change_detected(self):
        original = _make_original()
        modified = _make_pricelist([
            {
                "name": "Strzyżenie i stylizacja",
                "services": [
                    {"name": "Strzyżenie damskie", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None},
                    {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
                ],
            },
        ])
        result = await run_phase4_finalize("test-audit", modified, original)
        assert result["summary"]["categoriesOptimized"] == 1

    @pytest.mark.asyncio
    async def test_caps_lock_fix_applied(self):
        original = _make_original()
        modified = _make_pricelist([
            {
                "name": "Fryzjerstwo",
                "services": [
                    {"name": "STRZYŻENIE DAMSKIE", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None},
                    {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
                ],
            },
        ])
        result = await run_phase4_finalize("test-audit", modified, original)
        svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert svc["name"] != "STRZYŻENIE DAMSKIE"

    @pytest.mark.asyncio
    async def test_promo_detection(self):
        original = _make_original()
        modified = _make_pricelist([
            {
                "name": "Fryzjerstwo",
                "services": [
                    {"name": "Strzyżenie damskie - PROMOCJA", "price": "120 zł", "duration": "45 min", "description": None, "imageUrl": None, "variants": None},
                    {"name": "Koloryzacja", "price": "250 zł", "duration": "120 min", "description": None, "imageUrl": None, "variants": None},
                ],
            },
        ])
        result = await run_phase4_finalize("test-audit", modified, original)
        svc = result["finalPricelist"]["categories"][0]["services"][0]
        assert svc.get("isPromo") is True
```

- [ ] **Step 2: Run tests — verify they fail (functions don't exist yet)**

Run: `python3 -m pytest tests/test_optimize_phases.py -v`
Expected: ImportError — `cannot import name 'run_phase4_finalize'`

- [ ] **Step 3: Implement all 4 phase functions**

Create `pipelines/optimize_phases.py` with functions extracted and reorganized from existing `optimization.py`. Key changes from current code:

- `run_phase1_seo(audit_id, on_progress)` — loads scraped data + report from Supabase, runs SEO agent loop, returns `{pricelist, seoChanges, keywordsAdded}`
- `run_phase2_content(audit_id, pricelist, on_progress)` — loads report from Supabase (issues + transformations), runs content agent loop on the SEO-enriched pricelist, returns `{pricelist, contentChanges, namesImproved, descriptionsAdded}`
- `run_phase3_categories(audit_id, pricelist, on_progress)` — loads report from Supabase (structure issues), runs category agent loop on optimized pricelist, returns `{pricelist, categoryChanges}`
- `run_phase4_finalize(audit_id, pricelist, original_pricelist)` — deterministic: applies programmatic fixes (CAPS, cleanup, dupes, promo), generates diff vs original, computes quality score, returns `{finalPricelist, changes, summary, qualityScore}`

Reuse from existing code:
- `_normalize_item()`, `_find_optimization()`, `_detect_promo()` — copy into optimize_phases.py
- `clean_service_name()`, `fix_caps_lock()` — import from `pipelines.helpers`
- `build_full_pricelist_text()` — import from `pipelines.helpers`
- `OPTIMIZED_SERVICES_TOOL`, `CATEGORY_MAPPING_TOOL` — import from `agent.tools`
- `run_agent_loop` — import from `agent.runner`
- `SupabaseService` — import from `services.supabase`
- `MiniMaxClient` — import from `services.minimax`

Pricelist format flowing between phases:
```python
{
    "salonName": "...",
    "salonAddress": "...",
    "salonLogoUrl": "...",
    "totalServices": 32,
    "categories": [
        {
            "name": "Fryzjerstwo",  # Note: "name" not "categoryName"
            "services": [
                {"name": "...", "price": "...", "duration": "...", "description": "...", "imageUrl": "...", "variants": [...], "tags": [...]}
            ]
        }
    ]
}
```

- [ ] **Step 4: Run tests — verify phase 4 tests pass**

Run: `python3 -m pytest tests/test_optimize_phases.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add pipelines/optimize_phases.py tests/test_optimize_phases.py
git commit -m "feat: implement 4-phase optimization functions with tests"
```

---

### Task 4: Add 4 new endpoints to server.py

**Files:**
- Modify: `server.py`

- [ ] **Step 1: Add Pydantic request models**

Add after existing `EmbeddingRequest` model:

```python
class OptimizeSeoRequest(BaseModel):
    auditId: str


class OptimizeContentRequest(BaseModel):
    auditId: str
    pricelist: dict


class OptimizeCategoriesRequest(BaseModel):
    auditId: str
    pricelist: dict


class OptimizeFinalizeRequest(BaseModel):
    auditId: str
    pricelist: dict
```

- [ ] **Step 2: Add 4 synchronous endpoints**

Add after the `/api/embeddings` endpoint, before `# --- Background job ---`:

```python
@app.post("/api/optimize/seo", dependencies=[Depends(verify_api_key)])
async def optimize_seo(request: OptimizeSeoRequest) -> dict:
    """Phase 1: Inject missing SEO keywords into service names."""
    from pipelines.optimize_phases import run_phase1_seo
    return await run_phase1_seo(audit_id=request.auditId)


@app.post("/api/optimize/content", dependencies=[Depends(verify_api_key)])
async def optimize_content(request: OptimizeContentRequest) -> dict:
    """Phase 2: Rewrite names + descriptions (copywriting, benefit language)."""
    from pipelines.optimize_phases import run_phase2_content
    return await run_phase2_content(audit_id=request.auditId, pricelist=request.pricelist)


@app.post("/api/optimize/categories", dependencies=[Depends(verify_api_key)])
async def optimize_categories(request: OptimizeCategoriesRequest) -> dict:
    """Phase 3: Reorganize categories based on optimized content."""
    from pipelines.optimize_phases import run_phase3_categories
    return await run_phase3_categories(audit_id=request.auditId, pricelist=request.pricelist)


@app.post("/api/optimize/finalize", dependencies=[Depends(verify_api_key)])
async def optimize_finalize(request: OptimizeFinalizeRequest) -> dict:
    """Phase 4: Apply programmatic fixes, generate diff, save to Supabase."""
    from pipelines.optimize_phases import run_phase4_finalize
    from services.supabase import SupabaseService
    supabase = SupabaseService()
    original = await supabase.get_scraped_data(request.auditId)
    result = await run_phase4_finalize(
        audit_id=request.auditId,
        pricelist=request.pricelist,
        original_pricelist=original,
    )
    # Save to Supabase
    await supabase.save_optimized_pricelist(
        convex_audit_id=request.auditId,
        optimization_data=result,
        salon_name=original.get("salonName", ""),
    )
    return result
```

- [ ] **Step 3: Run server import check**

Run: `python3 -c "from server import app; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add server.py
git commit -m "feat: add 4 phase-specific optimization endpoints"
```

---

### Task 5: Update legacy `/api/optimize` wrapper

**Files:**
- Modify: `pipelines/optimization.py`

- [ ] **Step 1: Replace monolithic pipeline with sequential phase calls**

Replace `run_optimization_pipeline` body to call phases 1→2→3→4 in sequence:

```python
async def run_optimization_pipeline(
    audit_id: str,
    selected_options: list[str] | None = None,
    on_progress: ProgressCallback | None = None,
    **kwargs: Any,  # Legacy args ignored
) -> dict[str, Any]:
    """Legacy wrapper — runs all 4 phases sequentially."""
    from pipelines.optimize_phases import (
        run_phase1_seo,
        run_phase2_content,
        run_phase3_categories,
        run_phase4_finalize,
    )
    from services.supabase import SupabaseService

    progress = on_progress or _noop_progress

    await progress(5, "Faza 1: SEO — wstrzykiwanie słów kluczowych...")
    phase1 = await run_phase1_seo(audit_id=audit_id, on_progress=progress)

    await progress(35, "Faza 2: Treści — nazwy i opisy usług...")
    phase2 = await run_phase2_content(audit_id=audit_id, pricelist=phase1["pricelist"], on_progress=progress)

    await progress(60, "Faza 3: Kategorie — reorganizacja struktury...")
    phase3 = await run_phase3_categories(audit_id=audit_id, pricelist=phase2["pricelist"], on_progress=progress)

    await progress(80, "Faza 4: Finalizacja — poprawki i lista zmian...")
    supabase = SupabaseService()
    original = await supabase.get_scraped_data(audit_id)
    phase4 = await run_phase4_finalize(audit_id=audit_id, pricelist=phase3["pricelist"], original_pricelist=original)

    await supabase.save_optimized_pricelist(
        convex_audit_id=audit_id,
        optimization_data=phase4,
        salon_name=original.get("salonName", ""),
    )

    await progress(100, f"Optymalizacja gotowa! Score: {phase4['qualityScore']}/100, {phase4['summary']['totalChanges']} zmian")
    return phase4
```

- [ ] **Step 2: Remove old step code from optimization.py**

Delete everything between `run_optimization_pipeline` function and the module-level helpers (`_load_prompt`, `_normalize_item`, `_detect_promo`, `_find_optimization`). Keep those helpers since `optimize_phases.py` imports them. The file should shrink from ~614 lines to ~80.

- [ ] **Step 3: Run existing tests**

Run: `python3 -m pytest tests/test_optimization_pipeline.py tests/test_new_endpoints.py -v`
Expected: All pass (legacy wrapper calls new phases, API contract unchanged)

- [ ] **Step 4: Commit**

```bash
git add pipelines/optimization.py
git commit -m "refactor: legacy /api/optimize now calls 4 sequential phases"
```

---

### Task 6: Endpoint integration tests

**Files:**
- Modify: `tests/test_new_endpoints.py`

- [ ] **Step 1: Add endpoint tests for all 4 phases**

```python
class TestOptimizeSeoEndpoint:
    def test_seo_requires_api_key(self):
        response = client.post("/api/optimize/seo", json={"auditId": "a1"})
        assert response.status_code == 422

    def test_seo_validates_input(self):
        response = client.post("/api/optimize/seo", json={}, headers=HEADERS)
        assert response.status_code == 422


class TestOptimizeContentEndpoint:
    def test_content_requires_api_key(self):
        response = client.post("/api/optimize/content", json={"auditId": "a1", "pricelist": {}})
        assert response.status_code == 422

    def test_content_validates_input(self):
        response = client.post("/api/optimize/content", json={"auditId": "a1"}, headers=HEADERS)
        assert response.status_code == 422


class TestOptimizeCategoriesEndpoint:
    def test_categories_requires_api_key(self):
        response = client.post("/api/optimize/categories", json={"auditId": "a1", "pricelist": {}})
        assert response.status_code == 422


class TestOptimizeFinalizeEndpoint:
    def test_finalize_requires_api_key(self):
        response = client.post("/api/optimize/finalize", json={"auditId": "a1", "pricelist": {}})
        assert response.status_code == 422
```

- [ ] **Step 2: Run all tests**

Run: `python3 -m pytest tests/ --ignore=tests/test_supabase.py -v`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add tests/test_new_endpoints.py
git commit -m "test: add endpoint validation tests for 4 optimization phases"
```

---

### Task 7: Final cleanup and push

**Files:**
- Delete: `prompts/optimization_verify.txt` (quality check is now deterministic in phase 4)
- Modify: `prompts/optimization_services.txt` (no longer used — replaced by seo + content prompts)

- [ ] **Step 1: Delete unused prompts**

```bash
rm prompts/optimization_verify.txt
```

Keep `optimization_services.txt` for now (legacy wrapper might reference it indirectly) but it's dead code after Task 5.

- [ ] **Step 2: Run full test suite**

Run: `python3 -m pytest tests/ --ignore=tests/test_supabase.py -v`
Expected: All pass

- [ ] **Step 3: Push**

```bash
git add -A
git commit -m "chore: cleanup unused optimization prompts"
git push
```

---

### Task 8: Deploy to production server

- [ ] **Step 1: Push all changes**

```bash
git push
```

- [ ] **Step 2: SSH to server and pull**

```bash
ssh root@tytan
cd /home/booksy/webapps/bagent-booksyauditor
git pull
```

- [ ] **Step 3: Fix file ownership — ALL files must belong to user booksy**

```bash
chown -R booksy:booksy /home/booksy/webapps/bagent-booksyauditor
```

Verify:
```bash
ls -la /home/booksy/webapps/bagent-booksyauditor/pipelines/
```
Expected: all files owned by `booksy booksy`

- [ ] **Step 4: Restart bagent server AS user booksy**

The server MUST run as user `booksy`, not root. Check how it runs and restart accordingly:

```bash
# Check current process
ps aux | grep uvicorn | grep -v grep

# Restart as booksy (method depends on setup — systemd/supervisor/manual)
# If manual:
su - booksy -c "cd /home/booksy/webapps/bagent-booksyauditor && kill $(cat pid.txt 2>/dev/null) 2>/dev/null; nohup python3 -m uvicorn server:app --host 0.0.0.0 --port 3001 > logs/server.log 2>&1 & echo $! > pid.txt"
```

CRITICAL: Verify process runs as booksy:
```bash
ps aux | grep uvicorn
```
Expected: `booksy` in the USER column, NOT `root`

- [ ] **Step 5: Verify health**

```bash
curl -s https://bagent.booksyaudit.pl/api/health
```

Expected: `{"status":"ok",...}`

---

### Task 9: E2E production test — full audit flow

This task verifies the ENTIRE system end-to-end: Convex → bextract → bagent → Supabase → frontend. Covers all 4 optimization phases with a real salon on production.

**Prereq:** Task 8 completed — bagent deployed and healthy.

- [ ] **Step 1: Start fresh audit via Convex CLI**

```bash
npx convex run --no-push audit/internalMutations:startAuditCLI \
  '{"sourceUrl": "https://booksy.com/pl-pl/189983_salon-fryzjerski-karolina_salon-fryzjerski_3_warszawa"}'
```

Save the returned auditId. Poll until status=completed:

```bash
npx convex run dev:getAuditById '{"auditId": "<AUDIT_ID>"}'
```

Expected: status=completed, overallScore>0, scrapedServicesCount>0

- [ ] **Step 2: Health check — cross-DB consistency**

```bash
npx convex run devOps:debugHealthCheckAudit '{"auditId": "<AUDIT_ID>"}'
```

Expected: 0 issues, basePricelist exists, Supabase report exists with matching score

- [ ] **Step 3: Test Phase 1 — SEO**

```bash
BAGENT_KEY="<key>"
curl -s -X POST https://bagent.booksyaudit.pl/api/optimize/seo \
  -H "Content-Type: application/json" \
  -H "x-api-key: $BAGENT_KEY" \
  -d '{"auditId": "<AUDIT_ID>"}'
```

Expected: 200, response has `pricelist` (with categories+services), `seoChanges` array, `keywordsAdded` > 0. Service names should contain injected SEO keywords. Prices/durations unchanged.

Save response pricelist for next step.

- [ ] **Step 4: Test Phase 2 — Content**

```bash
curl -s -X POST https://bagent.booksyaudit.pl/api/optimize/content \
  -H "Content-Type: application/json" \
  -H "x-api-key: $BAGENT_KEY" \
  -d '{"auditId": "<AUDIT_ID>", "pricelist": <PHASE1_PRICELIST>}'
```

Expected: 200, response has `pricelist`, `contentChanges`, `namesImproved` > 0, `descriptionsAdded` > 0. Names should still have SEO keywords from phase 1. Descriptions should be benefit-oriented.

Save response pricelist for next step.

- [ ] **Step 5: Test Phase 3 — Categories**

```bash
curl -s -X POST https://bagent.booksyaudit.pl/api/optimize/categories \
  -H "Content-Type: application/json" \
  -H "x-api-key: $BAGENT_KEY" \
  -d '{"auditId": "<AUDIT_ID>", "pricelist": <PHASE2_PRICELIST>}'
```

Expected: 200, response has `pricelist` (possibly reorganized categories), `categoryChanges` array. Service names/descriptions from phase 2 preserved.

Save response pricelist for next step.

- [ ] **Step 6: Test Phase 4 — Finalize**

```bash
curl -s -X POST https://bagent.booksyaudit.pl/api/optimize/finalize \
  -H "Content-Type: application/json" \
  -H "x-api-key: $BAGENT_KEY" \
  -d '{"auditId": "<AUDIT_ID>", "pricelist": <PHASE3_PRICELIST>}'
```

Expected: 200, response has `finalPricelist`, `changes` (before/after per service), `summary` (totalChanges, namesImproved, descriptionsAdded, categoriesOptimized), `qualityScore` > 50.

Verify: no prices or durations changed. Changes list shows actual meaningful transformations.

- [ ] **Step 7: Test legacy wrapper — full pipeline in one call**

```bash
curl -s -X POST https://bagent.booksyaudit.pl/api/optimize \
  -H "Content-Type: application/json" \
  -H "x-api-key: $BAGENT_KEY" \
  -d '{"auditId": "<AUDIT_ID>", "userId": "test", "pricelistId": "test", "jobId": "test", "scrapedData": {}, "auditReport": {}, "selectedOptions": []}'
```

Expected: 202 accepted, job runs all 4 phases, completes with qualityScore > 50.

- [ ] **Step 8: Reassign audit to logged-in user for browser test**

```bash
npx convex run dev:debugReassignAudit '{"auditId": "<AUDIT_ID>", "targetEmail": "amiesak@gmail.com"}'
```

- [ ] **Step 9: Browser E2E — audit results page**

Open Chrome, navigate to:
```
https://localhost:3000/audit-results?audit=<AUDIT_ID>&dev=true
```

Verify visually:
1. Tab "1. Raport" — score > 0, summary text present, score breakdown (7 dimensions), issues listed, quick wins present
2. Tab "3. Optymalizacja treści" — "Obecne słowa kluczowe" shows count > 0, "Po optymalizacji" shows new keywords + improved phrases
3. Tab "2. Konkurenci" — shows either matched competitors or "Nie znaleziono" empty state (both valid)
4. No console errors (open DevTools, check Console tab)

Take screenshots of each tab for verification.

- [ ] **Step 10: Browser E2E — Playwright automated tests**

```bash
npx playwright test e2e/full-pipeline-e2e.spec.ts --reporter=line
```

Expected: All 7 tests pass:
- landing page loads without errors
- no references to deleted AI modules
- no legacy URLs in page source
- audit results page loads with existing audit
- pricing page loads without legacy generator code
- embed script uses auditorai.pl domain
- navigation between main pages works

- [ ] **Step 11: Verify console errors across all pages**

Navigate through each page in Chrome with DevTools Console open:
- `/` (landing)
- `/audit` (audit promo)
- `/pricing` (packages)
- `/audit-results?audit=<AUDIT_ID>&dev=true` (results)
- `/profile` (user profile)

Expected: ZERO console errors on all pages (excluding favicon/cert warnings).

- [ ] **Step 12: Final commit with test results**

```bash
git add -A
git commit -m "test: full E2E verification — 4-phase optimization + browser tests pass"
git push
```
