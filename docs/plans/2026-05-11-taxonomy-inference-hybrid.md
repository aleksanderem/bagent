# Taxonomy Inference — Hybrid Matching (bagent side)

> Pełna dokumentacja w **BEAUTY_AUDIT/docs/plans/2026-05-11-taxonomy-inference-hybrid.md**.
> Tu krótki overview tego co dotyczy bagent.

## Co zostało dodane w bagent

### `services/taxonomy_inference.py`
Moduł poprawiający źle przypisane `booksy_treatment_id` z Booksy API.

API:
```python
from services.taxonomy_inference import infer_and_apply

# In pipelines/competitor_analysis.py (already wired):
await infer_and_apply(supabase, subject_data["services"], label="subject")
for _, cdata in aligned_competitors:
    await infer_and_apply(supabase, cdata["services"], label=f"competitor {cdata['booksy_id']}")
```

Mutates each `svc` dict in place:
- `booksy_treatment_id` = inferred canonical tid (or unchanged if not overridden)
- `booksy_treatment_id_raw` = original (preserved for audit)
- `treatment_name` = canonical name (replaces denormalized stale string)
- `inference_score`, `inference_source`, `inference_applied`, etc.

Internals:
1. Batch-embed all service names via `OpenAI text-embedding-3-small` (single
   call, ~0.5s for 30-300 names, ~$0.0001).
2. Call `match_treatment_hybrid` SQL RPC per service with name + embedding +
   parent_hint. RPC returns top match scoring trigram (0.35w) + cosine (0.65w)
   + parent_boost (1.2x) + marker_compatibility (1.5x / 1.0 / 0.3).
3. Apply override only when score ≥ 0.55, no specificity-marker conflict,
   inferred ≠ raw.

Graceful fallback when OpenAI unavailable: drops to trigram-only RPC
(`match_treatment_by_name`). Pipeline still works, just lower coverage
(55% vs 86% on Floral).

### `services/openai_synthesis.py`
Fallback synthesis using `gpt-4o-mini` Structured Outputs. Used by
`pipelines/competitor_synthesis.py` when MiniMax M2.7 fails:

```python
# Synthesis chain: MiniMax → OpenAI → deterministic template
try:
    insights = await _run_minimax_synthesis(context)
except Exception:
    try:
        from services.openai_synthesis import synthesize_via_openai
        insights = await synthesize_via_openai(context)
    except Exception:
        insights = _deterministic_fallback(...)
```

Returns same dict shape as MiniMax (`positioning_narrative`, `swot`,
`recommendations`). Latency ~5-10s vs MiniMax 60-600s with retries. Cost
~$0.001 per call.

### `workers/taxonomy_refresh.py`
Three nightly cron tasks running on `bagent-worker` (arq):

```
03:00 UTC — refresh_taxonomy_views
            REFRESH MATERIALIZED VIEW CONCURRENTLY for
            mv_booksy_treatments + mv_treatment_name_lookup

03:15 UTC — embed_new_services
            Find rows where embedding_applied_at IS NULL, embed via OpenAI
            in batches of 500 names, cap 50k/night (~$0.015 budget)

03:30 UTC — refresh_inferred_treatments
            mark_stale_inferred_treatments_for_refresh(7d) +
            drain backfill_inferred_treatments() until exhausted
            or 200k cap. Catches new scrapes + taxonomy evolution.
```

Idempotent — partial runs / restarts are safe.

### `pipelines/competitor_analysis.py` — integration point

After Versum mapping (progress=35%) and before pricing comparisons
(progress=45%), inject taxonomy inference at progress=40%:

```python
# Apply versum mappings as before
_apply_versum_mappings(subject_data, versum_map)
for _, cdata in aligned_competitors:
    _apply_versum_mappings(cdata, versum_map)

# NEW: Taxonomy inference (canonical name + hybrid embeddings)
await progress(40, "Inferencja taxonomy z crowd lookup...")
try:
    await infer_and_apply(service, subject_data.get("services") or [], label="subject")
    for _, cdata in aligned_competitors:
        await infer_and_apply(service, cdata.get("services") or [], ...)
except Exception as e:
    # Non-fatal: pipeline falls through to raw tids
    logger.warning("Taxonomy inference failed: %s", e)

# Downstream code reads service["booksy_treatment_id"] (now corrected)
# and service["treatment_name"] (now canonical, not denormalized)
pricing_rows = _compute_pricing_comparisons(...)
```

### Scripts (one-shots, not in pipeline)

- `scripts/embed_taxonomy.py` — embed all 368 canonical Booksy treatment names
  into `booksy_treatment_embeddings`. Run once after applying migration 045.
- `scripts/embed_services.py` — batch backfill `salon_scrape_services.name_embedding`
  for 2.28M rows. Optional — pipeline does its own per-run embedding. Useful
  for downstream analytics consumers that want pre-baked semantic search.
- `scripts/test_hybrid_match.py` — sanity check on known problem cases.
- `scripts/run_competitor_report_local.py` — local pipeline runner against
  prod Supabase, no Redis/arq required.

## Required env vars

In `/home/booksy/webapps/bagent-booksyauditor/.env`:

```
OPENAI_API_KEY=sk-proj-...   # required for hybrid mode + synthesis fallback
                              # graceful degradation when missing
```

Optional but recommended for full functionality.

## Required Postgres migrations (in BEAUTY_AUDIT repo)

Apply in order:
1. `042_booksy_treatments_taxonomy.sql`
2. `043_match_treatment_by_canonical_name.sql`
3. `044_mass_normalize_treatments.sql`
4. `045_hybrid_matching_embeddings.sql`
5. `046_marker_aware_hybrid_matching.sql`

After 045 applied, run `python -m scripts.embed_taxonomy` to populate the
368 canonical embeddings.

## Verification

Test run on Emesthetic Clinic (audit `j5796vaa2zbzn30bay75xv32d184gef9`,
285 services, Medycyna Estetyczna) on 2026-05-11:

- 29 pricing_comparisons rows with clean canonical names
- 15 service gaps
- 15 SWOT items + 9 recommendations (MiniMax succeeded, no fallback)
- Runtime **102 seconds** (vs baseline 670s)

Test run on Floral Nails & Lashes (audit `j57e45tp7cjm8yhprz093ds05d84jd9s`,
29 services, Paznokcie):

- 7 pricing_comparisons (clean)
- 86% override coverage (22 overrides + 3 keeps out of 29)
- All canonical names: Pedicure hybrydowy, Manicure hybrydowy, Manicure
  klasyczny, Pedicure klasyczny, Henna brwi, etc.
- MANICURE MESKI → 290 "Manicure i pedicure dla mężczyzn" ✓
  (was → 292 "Manicure japoński" before marker-aware boost)

## See also

- BEAUTY_AUDIT/docs/plans/2026-05-11-taxonomy-inference-hybrid.md — full
  doc including problem statement, iteration history (4 tiers), database
  schema, deployment procedure, decisions log
- BEAUTY_AUDIT/docs/SYSTEM_OPERATIONS_MAP.md — overall operations reference
