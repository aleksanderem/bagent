# bagent — Faktyczna mapa systemu (stan: 2026-06-11)

> Dokument powstał z pełnej analizy kodu, bo `CLAUDE.md` opisuje stan sprzed wielu iteracji
> (3 pipeline'y i stary layout plików). Ten plik dokumentuje RZECZYWISTOŚĆ. Sekcje oznaczone
> „(do weryfikacji)" pochodzą z analizy automatycznej i warto je potwierdzić przy najbliższej
> pracy w danym obszarze.

## 1. Czym bagent jest naprawdę

Nie „AI analyzer z 3 pipeline'ami", tylko **6 podsystemów** na wspólnej infrastrukturze
(FastAPI + arq/Redis + Supabase + webhooki Convex):

1. **Pipeline'y audytu** — BAGENT #1 raport (`pipelines/report.py`), #2 cennik (`pipelines/cennik.py`,
   deterministyczny), #3 podsumowanie (`pipelines/summary.py`), restrukturyzacja kategorii
   (`pipelines/category_restructure.py`), proofread, versum_suggest.
2. **Raport konkurencji** — split: selekcja (`competitor_selection.py`, deterministyczna,
   PostGIS+embeddingi) → analiza (`competitor_analysis.py`, SQL/math, ~28 wymiarów) → synteza AI
   (`competitor_synthesis.py`, MiniMax→OpenAI→deterministyczny fallback) → refresh premium
   (`competitor_report_refresh.py`, snapshoty). Stary monolit żyje w `pipelines/_premium/`
   (zimny kod, nigdzie nie podpięty — zarezerwowany na osobny produkt).
3. **Free report** — `pipelines/free_report.py`: ZAMROŻONA kopia #1 (commit 47220b7), lead magnet.
   Kontrakt: nie modyfikować in-place (patrz `docs/free_report.md`).
4. **Monitoring/scrape orchestration** — `scheduler/refresh_scheduler.py` (tiery 0-3) +
   `workers/scrape_refresh.py` (drain kolejki co 15 s, diff, alerty do Convex) +
   `ingestion/live_scrape.py` (idempotentny zapis przez content_hash).
5. **Outreach (lejek e-mail)** — `pipelines/outreach_loader.py` (walidacja assetów z repo
   BEAUTY_AUDIT → kolejka akceptacji) + `workers/outreach_deployer.py` (deploy do wintact.io) +
   `workers/outreach_orchestrator.py` (state machine, DAG sekwencji, frequency caps, wysyłka) +
   `workers/state_transition_processor.py` (cold ingest, przejścia zakupowe, wygaszanie).
6. **Meta Ads** — `pipelines/campaign_setup.py` (propozycja kampanii: targeting + kreacje
   gpt-image-1 z brand overlay) + `workers/campaign_tasks.py` (push do Meta, dzienne metryki,
   atrybucja rezerwacji last-click po UTM). Uwaga: `services/meta_ads.py` częściowo DISABLED
   (2026-05-20, legacy ConvexClient — do weryfikacji).

Plus: **discovery** (`workers/discovery_tasks.py` — równoległa pompa odkrywania salonów Booksy,
saturation cooldown), **taksonomia nightly** (`workers/taxonomy_refresh.py` 03:00-04:00),
**staff identity** (migracje pracowników między salonami), **SLO probes** (semantyczne sondy
poprawności, nie liveness).

## 2. Endpointy HTTP (server.py)

Auth: nagłówek `x-api-key` (poza /api/jobs*, /api/events, /api/health, /dashboard).
Wszystkie POST pipeline'owe zwracają 202 + jobId; praca leci przez arq.

| Endpoint | Co robi |
|---|---|
| POST /api/audit/report | BAGENT #1 — pełny raport audytu (workers/tasks.py: run_report_task) |
| POST /api/audit/free_report | Zamrożony darmowy raport (free_report.py) |
| POST /api/audit/cennik | BAGENT #2 — zoptymalizowany cennik (deterministyczny) |
| POST /api/audit/summary | BAGENT #3 — podsumowanie + podstawowy podgląd konkurencji |
| POST /api/competitor/report | Raport konkurencji (selekcja+analiza+synteza) |
| POST /api/competitor/report/refresh | Refresh premium (idempotencja: JobStore lokalny — patrz FINDINGS C) |
| POST /api/versum/suggest-mappings (+GET result) | Mapowanie usług Versum przez MiniMax |
| GET /api/staff/team-summary/{booksy_id} | RPC fn_salon_team_summary |
| GET /api/competitor/profile/{salon_id} | Profil konkurenta (salons + services) |
| GET /api/competitor/methods_for_audit/{audit_id} | RPC fn_subject_methods |
| POST /api/competitor/salons_offering_methods | RPC fn_find_salons_offering_method |
| POST /api/competitor/method_pricing | RPC fn_compute_method_pricing |
| GET /api/staff/competitor-changes/{booksy_id} | RPC fn_competitor_recent_changes |
| GET /api/staff/migrations/{booksy_id} | RPC fn_staff_migrations_for_salon |
| POST /api/ai/text | Generyczny tekst przez MiniMax (sync) |
| POST /api/embeddings | Embeddingi przez Gemini (batch 100) |
| POST /api/dev/trace-taxonomy, GET /api/dev/synthetic-categories | Gated: BAGENT_DEV_ENDPOINTS=1 |
| GET /api/jobs, /api/jobs/{id}, /api/jobs/{id}/logs, POST /api/jobs/{id}/cancel | JobStore (in-memory!) |
| GET /api/events | SSE dla dashboardu |
| GET /api/health | status + redis ok/down |
| GET /dashboard | HTML dashboard (templates/dashboard.html) |

Usunięte legacy (komentarze w server.py): /api/competitor (stary), /api/optimize/*.

## 3. Workers i crony (workers/main.py → WorkerSettings)

Start przez PM2 (`ecosystem.config.cjs`): proces `bagent-worker` = `uv run arq workers.WorkerSettings`,
autorestart, max_restarts 50. arq: max_jobs=20, job_timeout=4h, keep_result=24h, max_tries=3.

Najważniejsze crony:
- `drain_scrape_queue` co 15 s (batch 6, claim przez RPC FOR UPDATE SKIP LOCKED)
- `schedule_refresh_cron` co godz. :05 (tiery → salon_refresh_queue)
- `reap_stuck_jobs` co 10 min; `reap_stuck_discovery_runs` co godz. :30
- discovery: `enqueue_discovered_to_refresh_queue` co 30 s, `bootstrap_discovery_pump` co 30 min,
  `auto_retry_failed_discovery_runs` :15/:45
- outreach: `deploy_approved_templates` co 2 min, `deploy_approved_segments`/`activate_approved_sequences`/
  `enroll_due_contacts` co 5 min, `send_due_messages` co 1 min (cap 25/min — limit wintact/Mailgun),
  `ingest_new_cold_contacts` co 30 min, `apply_purchase_transitions` co 15 min, `expire_stale_states` :45
- taksonomia nightly: 03:00 MV refresh, 03:15 embeddingi (cap 50k/noc), 03:30 inferred backfill,
  03:45 staff identity, 04:00 salon focus
- `worker_heartbeat` co 5 min (Healthchecks) + 6× SLO probes

## 4. Monitoring refresh (tier-0) — przepływ

```
scheduler (tiery: 0 = watchlisty per-row 6/24/48h, 1 = subjekty premium 7d,
           2 = audyty 90d co 30d, 3 = zimny katalog 7d)
  → INSERT salon_refresh_queue (RPC enqueue_salon_refresh — dedup)
  → drain_scrape_queue co 15 s → bextract fetch → ingestion (chain head + delty, content_hash dedup)
  → post-scrape: RPC fn_salon_service_diffs (diff vs poprzedni head)
  → tier-0: _build_monitoring_alerts → POST Convex /api/competitor/alert/ingest
  → RPC advance_monitoring_due (następny termin)
  → jeśli booksy_id = subject raportu premium → self-POST /api/competitor/report/refresh
```

## 5. Warstwa LLM

- **MiniMax M2.7** (`services/minimax.py`): AsyncAnthropic z base_url api.minimax.io/anthropic;
  timeout httpx 120 s (connect 15 s); with_retry max_attempts=2 (backoff 2→10 s, skip na auth).
  Tryby: create_message (tool_use, zachowuje ThinkingBlock), generate_json (json.loads + fallback
  regex na ```json fences — NIC WIĘCEJ, patrz FINDINGS P0), generate_text.
- **Agent loop** (`agent/runner.py`): multi-turn tool_use, max_steps=30, pełny content (z thinking)
  appendowany do messages; AgentResult.total_steps = liczba wiadomości asystenta (nie wywołań API).
- **OpenAI**: `openai_synthesis.py` — fallback syntezy konkurencji (gpt-4o-mini, Structured Outputs);
  `openai_taxonomy_client.py` — Taxonomy Pass 5 (TAXONOMY_PASS5_PROVIDER default openai; chunking
  po 30 klastrów po incydencie obcięcia odpowiedzi 192→10 decyzji); embeddingi usług.
- **Gemini**: endpoint /api/embeddings.
- Fallback chain syntezy konkurencji: MiniMax → gpt-4o-mini → `_deterministic_fallback()`
  (zdegradowany: max 3 rekomendacje z odchyleń cen, BEZ SWOT).
- BAGENT #1 (report.py) NIE MA fallbacku na OpenAI — porażka MiniMax po retry = fail pipeline'u
  (z wyjątkiem deterministycznego fallbacku quick winów przy pustej liście).

## 6. Raport konkurencji — co jest LLM, a co deterministyczne

LLM (competitor_synthesis.txt → tool `submit_competitor_insights`): positioning_narrative (200-400
znaków), SWOT (sanityzowany — punkty bez poprawnych sourceDataPoints są DROPOWANE po cichu),
recommendations (5-12; `estimatedRevenueImpactGrosze` BEZ zdefiniowanej metodologii — patrz FINDINGS).

Deterministyczne: localRanking (RPC), competitors/detailedProfiles, priceComparison/perServicePricing,
serviceGapMatrix, seasonalCalendar (hardcoded wzorce miesięczne per kategoria), shortStrategy (mapowanie
rekomendacji na 4 tygodnie), longStrategy (sumy grosze + STAŁE kwoty Q2=1.0M, Q3=1.5M, Q4=2.2M gr),
customerJourney/funnel (hybryda: realne reviews/desc coverage + benchmarki branżowe, np. konwersja
~1.5-3% wg WordStream), calendarComparison (open_hours), actionPlan 14 dni (szablon faz), summary.

Sekcje typu CompetitorReportData we froncie, których synteza NIE wypełnia (legacy/premium):
marketSummary, advantages, radarMetrics, marketNiches, marketSegmentation, strategicRecommendations,
actionPlanTimeline — częściowo zarezerwowane dla `_premium/`.

**Decision points z „Multi-Stage Analysis Pipeline.txt" (BEAUTY_AUDIT): NIE ISTNIEJĄ.**
Selekcja zawsze: PostGIS 15 km → scoring v2 (focus_tid 30 + profile_overlap 25 + focus_var 20 +
portfolio_emb 20 + reviews 10 − kara za dystans) → buckety direct/cluster/aspirational/new → cap.
Brak przełączania radiusa, brak detekcji nisz, brak warunkowego flow.

## 7. Kontrakty webhooków do Convex (services/convex.py; testy: tests/e2e/test_webhook_contracts.py)

- /api/audit/report/{progress|complete|fail} — complete: {auditId, userId, overallScore, reportStats}
- /api/audit/cennik/{progress|complete|fail}
- /api/audit/summary/{progress|complete|fail}
- /api/competitor/report/{progress|complete|fail}
- /api/competitor/alert/ingest — {userId, watchlistId, salonId, salonName, type, severity, title, body, metadataJson?}

Semantyka: best-effort (job oznaczany completed PRZED webhookiem; brak retry/backoff — FINDINGS B).

## 8. Konfiguracja (config.py — kluczowe env)

MINIMAX_API_KEY / MINIMAX_BASE_URL (api.minimax.io/anthropic) / MINIMAX_MODEL (MiniMax-M2.7);
GEMINI_API_KEY; OPENAI_API_KEY; SUPABASE_URL + SUPABASE_SERVICE_KEY; CONVEX_URL (.convex.site!) +
CONVEX_DEPLOY_KEY; API_KEY; REDIS_*; BEXTRACT_API_URL/KEY; WINTACT_API_KEY + WINTACT_WEBHOOK_SECRET;
OUTREACH_DAILY_SEND_CAP_SCALE=200 / _OPT_IN=50; TAXONOMY_PASS5_PROVIDER (default openai);
TAXONOMY_ANCHOR_MIN_CONFIDENCE; BAGENT_DEV_ENDPOINTS; BUGSINK_DSN_BAGENT / _SCRAPE_ORCHESTRATOR;
HC_PING_* (Healthchecks, no-op gdy brak).

## 9. Observability

Sentry/Bugsink (FastAPI integration + worker logging integration, release=GIT_SHA, traces 0.0);
Healthchecks fire-and-forget; SLO probes (scrape progressing, chain heads growing, reviews ingesting,
discovery active; storage_budget i logflare_bounded = TODO zwracają ok); `services/pipeline_trace.py` —
trwałe ślady decyzji pipeline'ów (tabela pipeline_traces, wymaga flush()).

## 10. Status dokumentacji repo

- `CLAUDE.md`: opisuje nieistniejący layout (pipelines/audit.py, competitor.py, optimization.py),
  starą listę promptów, brak ~2500 linii podsystemów (outreach, Meta Ads, discovery, scheduler,
  SLO, taxonomy nightly). Score breakdown ma 7 pól i hard ceiling 88 (MAX_AUDIT_SCORE) —
  nieopisane. **Wymaga przepisania**.
- `PLAN.md`/`PLAN_V2.md`: historyczne plany, częściowo zrealizowane inaczej.
- `docs/free_report.md`: AKTUALNY i dobry (kontrakt zamrożenia).
- Ten plik + `FINDINGS-2026-06-11.md` + `FUNNEL_AUDIT-2026-06-11.md` = stan na 2026-06-11.
