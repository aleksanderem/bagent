# salon_scrapes delta encoding — bagent side

> Full doc with schema, decisions, deployment in
> **BEAUTY_AUDIT/docs/plans/2026-05-12-scrape-delta-encoding.md**.
> This file only covers the bagent-side write path + helpers.

## What changed in bagent

### `services/scrape_history.py` (NEW)

Single-file module hosting:

* `compute_content_hash(payload)` — SHA-256 over a canonical
  (`sort_keys=True`, no whitespace), volatile-stripped form of the raw
  Booksy payload. Used by the write path to detect "this crawl is
  semantically identical to the current chain head".
* `compute_reverse_patch(new_payload, old_payload)` — wraps
  `jsonpatch.JsonPatch.from_diff` and returns RFC 6902 ops such that
  `apply(new_payload, ops) == old_payload`. Stored on the demoted
  previous head so it can be reconstructed by walking backwards from
  the new head.
* `apply_patch(target, ops)` — thin wrapper around
  `jsonpatch.JsonPatch.apply`. Used by `reconstruct_raw_response`.
* `reconstruct_raw_response(client, scrape_id)` — calls the
  Postgres-side `scrape_chain_trail` RPC and applies the deltas in
  reverse order to materialise any historical snapshot's raw payload.
  Returns `None` for missing rows or corrupt chains.
* `CHECKPOINT_EVERY = 30` — module-level constant matching the
  migration's documented expectation.

The volatile-strip list lives in `_VOLATILE_BUSINESS_KEYS`. Conservative
on purpose: `reviews_count`, `reviews_rank`, `last_updated`,
`synced_at`, `cached_at`, `popularity_score`, `trending_score`,
`view_count`, `booking_count`. Everything else (including the full
`reviews` array) is hash-significant.

### `scripts/ingest_salon_jsons.py` — modified

Three new methods on `SalonJsonIngester`:

* `_fetch_chain_head(booksy_id)` — pulls the current chain head row
  (`is_chain_head=TRUE`) for a `booksy_id`. None if no new-system head.
* `_bump_unchanged_head(head_id, scraped_at)` — rolls up an unchanged
  crawl into the head via `UPDATE last_seen_at + unchanged_crawl_count`.
* `_promote_chain_head(new_scrape_id, prev_head, new_payload)` —
  atomically flips the head pointer after a changed-content insert.
  When `prev_head.chain_length + 1 >= CHECKPOINT_EVERY` the previous
  head stays a base (checkpoint). Otherwise it's converted in-place to
  a delta via `compute_reverse_patch` and its `raw_response` is set to
  NULL.

`_insert_scrape` now returns `tuple[scrape_id, action, prev_head]`
where `action` is one of `'new'`, `'unchanged'`, `'changed'`. The
`'unchanged'` branch returns the existing head's id and never inserts
a new row.

`ingest_file` orchestrates the new flow:

```
upsert salon
↓
_insert_scrape → (scrape_id, action, prev_head)
↓
if action != 'unchanged':
    _insert_services
_insert_reviews              # idempotent
_insert_top_services         # idempotent
↓
_log_ingestion               # audit row commits first
↓
if action != 'unchanged':
    _promote_chain_head      # flip heads + convert prev to delta
```

Audit-log row commits BEFORE chain promotion so unwind safety is
preserved: if `_insert_services` fails on a changed-content write, the
new row gets cleaned up and the old head stays intact. Promotion
failure (rare) is logged but non-fatal — next ingest self-heals by
treating the booksy_id as fresh.

### `pyproject.toml` — modified

Added `jsonpatch>=1.33` for RFC 6902 patch generation and application.
The library is mature, pure-Python (no native deps), trivial to deploy.

## Production deploy

```bash
ssh tytan
cd /home/booksy/webapps/bagent-booksyauditor
git pull
source venv/bin/activate
pip install -e .                                         # installs jsonpatch
pm2 restart bagent-booksyauditor bagent-worker-booksyauditor
pm2 logs bagent-worker-booksyauditor --lines 50 --nostream  # sanity check
```

Required Postgres migrations applied to `db.booksyaudit.pl`:

```
047_scrape_delta_encoding.sql
048_autovacuum_hot_tables.sql
```

Plus a one-shot manual VACUUM per the comment block at the bottom of
migration 048 (15–60 minutes total runtime).

## Verification

* `v_scrape_dedup_stats` view shows `unchanged_crawls_absorbed`
  climbing rapidly compared to `new_base_rows` + `new_delta_rows`.
  Healthy ratio: ≥ 10 unchanged absorbs per new row written.
* `json_ingestion_log.rows_inserted->>'dedup_action'` distribution
  query (see plan doc) confirms most crawls hit the `'unchanged'`
  branch.
* For any salon with `chain_length > 0`, calling
  `reconstruct_raw_response(client, scrape_id)` on a delta row should
  return a `dict` that round-trips equality with what was stored on
  the prior chain head when it was last a base.

## Why no legacy backfill

The user requested a cut-off rollout so we can ship to production
quickly. The 109 GB of legacy `raw_response` data stays in place — it
remains readable through the existing `ORDER BY scraped_at DESC`
pattern. A batched legacy backfill is documented as future work in
the BEAUTY_AUDIT plan doc.

## See also

* BEAUTY_AUDIT/docs/plans/2026-05-12-scrape-delta-encoding.md — full
  design, decisions, deployment, rollback
* BEAUTY_AUDIT/supabase/migrations/047_scrape_delta_encoding.sql
* BEAUTY_AUDIT/supabase/migrations/048_autovacuum_hot_tables.sql
