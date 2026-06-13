# Rollout — taxonomy router per-salon timeout (quick 260613-kiu)

> Status: code merged-ready on branch `fix/taxonomy-router-per-salon-timeout` (commit `e55f3f4`).
> NOT deployed. This runbook is for the lead to execute WITH explicit user sign-off.
> Nothing in quick 260613-kiu pushed, deployed, or regenerated any report.

## Problem

The competitor-analysis pipeline (`pipelines/competitor_analysis.py`) runs every salon
(subject + ~15 competitors) through `_apply_llm_taxonomy_to_null_tid_services` inside one
`asyncio.gather` during the `taxonomy.router` phase (Etap 4, "AI kategoryzacja usług bez
taxonomy match", progress 38). A single LLM call inside that routing step can hang
indefinitely. Because the gather had no per-call or per-salon timeout and ran without
`return_exceptions`, one hung salon froze the entire gather. The job got stuck around
26-38% and never reached pricing or completion. Normal router wall-clock is ~8.8s; a hang
was observed lasting over 10 minutes and blocked roughly 5 Emesthetic competitor-report
regenerations.

## What changed

The fix is concurrency-orchestration only. The taxonomy decision logic
(`_resolve_service_taxonomy`, rules 1-4), the Pass-5 cross-salon consistency block, and the
batch pricing path (quick 260613-cl2 / #28) are byte-unchanged.

Each salon's routing call is now wrapped in `asyncio.wait_for(..., timeout=ROUTER_PER_SALON_TIMEOUT_S)`.
The gather switched to `return_exceptions=True`, and a per-result loop inspects each outcome:
a salon whose `wait_for` raised `TimeoutError` is logged with outcome `router_timeout`, a
salon whose routing raised any other exception is logged with outcome `router_error`, and in
both cases the salon degrades to 0 routed. Successful salons keep their real routed count.
Every skip emits a `logger.warning("Etap 4: taxonomy router skipped salon ...")` line and adds
a new `taxonomy.router_skip` trace step (`{label, outcome, error}`) when a tracer is present.
The outer pipeline try/except (which deliberately re-raises a corrupt-foundation error and
aborts the etap) is intact — individual-salon failures now degrade inside the gather and never
reach it.

## Why the degradation is safe

The router is an enrichment layer: it assigns an LLM-inferred `treatment_id` to services that
Booksy left without a taxonomy match (NULL `booksy_treatment_id`). When a salon is skipped, its
null-tid services simply stay un-routed — exactly the state that existed before the router was
ever added. Those services are not promoted into the pricing matrix for that salon, but nothing
is corrupted and the pipeline proceeds. The subject salon's own real `booksy_treatment_id`
values are unaffected (the subject carries its native tids), so subject pricing and the report
core are unaffected even in the rare case the subject itself is the salon that times out.

## Env knob

`ROUTER_PER_SALON_TIMEOUT_S` — per-salon wall-clock cap for the router, default `90` seconds
(generous headroom over the ~8.8s normal pass). The module constant in
`pipelines/competitor_analysis.py` documents the default; the router block re-reads the env at
call time, so an override applies without code change. Lower it to tighten the cap; raise it for
unusually large salons whose legitimate routing approaches the limit. It does not need to be set
in prod for the fix to work — the 90s default ships in code.

## Rollout steps (lead-executed, gated on explicit user confirmation)

1. Confirm with the user that this should ship now.
2. Push the feature branch: `git -C /Users/alex/Desktop/MOJE_PROJEKTY/bagent push -u origin fix/taxonomy-router-per-salon-timeout`.
3. Open a PR, review the diff (two files: `pipelines/competitor_analysis.py` router block + `tests/test_competitor_analysis.py`), merge to `main`.
4. Deploy bagent per the standard procedure: on the server, `cd /opt/bagent && git pull && uv sync`, then restart the PM2 process (`bagent-booksyauditor`).
5. Regenerate one Emesthetic competitor report and watch it progress past ~38% into pricing and reach `completed`.
6. Check whether any `taxonomy.router_skip` trace rows were written for that run (a skip is informational, not a failure — but if every salon skips, investigate the LLM/RPC layer).

## Observability — spotting a skip in prod

A timed-out or errored salon is visible two ways. In the PM2 logs, grep for the warning line:

```bash
pm2 logs bagent-booksyauditor --lines 500 | grep "taxonomy router skipped salon"
```

In Supabase, query the trace table for the new step:

```sql
select * from pipeline_traces where step = 'taxonomy.router_skip' order by created_at desc limit 50;
```

The `data` payload carries `label` (the salon, e.g. `subject` or `competitor booksy_id=...`),
`outcome` (`router_timeout` or `router_error`), and `error` (the repr of the caught exception).

## Test status at merge time

Full bagent unit suite green: `uv run pytest tests/ -m "not integration"` → 446 passed
(443 baseline from quick 260613-cl2 + 3 new router tests), 24 deselected. The three new tests
(`test_router_timeout_salon_does_not_hang_pipeline`, `test_router_exception_salon_is_caught_others_continue`,
`test_router_normal_salon_unchanged_no_skip_warning`) drive the public `compute_competitor_analysis`
entry point with the taxonomy routing call monkeypatched — zero live LLM calls.

Pre-existing, unrelated: `tests/test_promote_gates.py` emits 5 `RuntimeWarning: coroutine ...
'_execute_mock_call' was never awaited` warnings (from the promote-gate mock setup added in
quick 260613-cl2 / 260612-plk, not from this change). These are warnings, not failures, and
were not fixed in this plan.
