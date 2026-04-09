# Plan: Competitor Report Pipeline (BAGENT #4)

Data: 2026-04-08
Status: research spike done → Comp Etap 0.1 ready to dispatch
Branch: main
Related plan: `docs/plans/2026-04-08-unified-report-pipeline.md` (pattern reference)

## Research spike findings (2026-04-08, po initial plan draft)

Zanim dispatch agents team, zrobiony został research spike na istniejącym datasetcie i schema bazy. Kluczowe findings które zmieniają kilka sekcji poniższego planu — **te findings mają pierwszeństwo nad tym co jest dalej** w miejscach gdzie się rozjeżdżają.

### 1. Istnieje ~5267 JSON scrapes w `/Users/alex/Desktop/MOJE_PROJEKTY/BEAUTY_AUDIT/json/`

User dostarczył folder z raw Booksy responses dla 5267 salonów z 2 województw (nie całej Polski — to tylko pierwszy batch z automated scraper który będzie dostarczał kolejne partie). Pliki nazwane `{booksy_id}.json`. Każdy to kompletny dump business endpoint response. Folder jest dodany do `.gitignore` (zbyt duży dla repo, ~3GB).

**Implication #1**: Nie musimy scrape'ować niczego od zera dla first MVP. Mamy gotowy dataset 5267 salonów do bulk load.

**Implication #2**: User będzie dostarczać kolejne batche JSONów (np. kolejne województwa). Ingester MUSI być re-runnable — idempotentny na content hash, detect-and-skip już ingerowane pliki, append-only dla time-series.

**Implication #3**: Skala docelowa jest ~30-80k salonów gdy cała Polska zostanie dostarczona. Batched INSERTs, nie jednorazowe bulk commit.

### 2. Native Booksy vs Versum split — 95.4% vs 4.6%

Analiza wszystkich 5267 plików:
- Native Booksy: **5027 salonów (95.4%)** — `partners: []` (pusta lista)
- Versum: **240 salonów (4.6%)** — `partners: ["versum"]`
- Inni partnerzy: **0**

Versum to legacy system akwizycyjny. Jedyny inny partner występujący w dataset'cie. Booksy najwyraźniej wycofał pozostałe integracje do natywnej platformy.

### 3. Treatment_id coverage — native vs versum

Per-salon analiza pokrycia `treatment_id`:

**Native Booksy (5027):**
- 95-100% coverage: 3578 (71%)
- 50-95% coverage: 1372 (27%)
- 0-50% coverage: 74 (1.5%)

**Versum (240):**
- 95-100% coverage: 2 (<1%)
- 50-95% coverage: 27 (11%)
- 0-50% coverage: **211 (88%)** ← systemic

**Kluczowe wnioski:**
- Parser extraction w bagent działa poprawnie dla native Booksy (98.5% salonów ma ≥50% coverage)
- Versum systemowo nie wypełnia canonical `treatment_id` — to jest property source data, nie bug parsera
- Beauty4ever (nasze testowe konto) jest jednym z 240 Versum salonów, stąd jej outlier coverage 46/417 (11%)

### 4. Versum handling — self-service mapping widget, NIE auto-LLM

Decyzja produktowa: dla Versum salonów nie robimy automatycznego mapowania treatment_id przez LLM. Zamiast tego:

- Dodajemy tabelę `versum_service_mappings` (salon_id, booksy_service_id, mapped_treatment_id, mapped_treatment_parent_id, confidence, mapped_by_user_id)
- Budujemy UI widget „Mapowanie usług Versum" dostępny tylko dla salon ownerów z `partner_system='versum'`
- Widget pokazuje unmapped services + smart dropdown z Booksy treatments + bulk apply
- User raz klika mapowanie, persistent w bazie, każdy kolejny raport konkurencji używa tych mapowań przy computing pricing comparisons
- Onboarding banner dla Versum userów: „Dodaj mapowanie aby zobaczyć pełną analizę porównawczą cen"
- My (team) też korzystamy z widget'u żeby zmapować Beauty4ever — nasze testowe konto dostanie pełny raport po manual mapping

To dodaje **Comp Etap 9: Versum mapping widget** na końcu planu (Phase 5). Nie blokuje MVP — native user'zy nie widzą widget'u wcale, Beauty4ever dostaje częściowy raport dopóki nie zmapujemy, potem pełny.

### 5. Reviews — twardy limit 3 per salon z business endpoint

**Każdy z 5267 salonów** ma maksymalnie **3 reviews** w `raw_response.business.reviews` — bez wyjątków. Salon z 17609 reviews w `reviews_count` metadata ma i tak tylko 3 w sample. To jest Booksy business endpoint sample — zwraca zawsze 3 najnowsze/top, niezależnie od rzeczywistej liczby.

**Implications:**
- Sentiment analysis z 3 reviews per salon nie ma wartości produktowej
- Popularity-by-mentions analysis (najczęściej recenzowane usługi) wymaga pełnych reviews
- **Potrzebny jest nowy endpoint w bextract**: `GET /businesses/{id}/reviews?page=N&per_page=50` który pageuje pełną historię reviews z Booksy profile page
- To dodaje **Comp Etap 10: bextract reviews pagination endpoint** (parked do czasu aż zdecydujemy że sentiment upsell jest priorytetem)

### 6. Top services coverage — 70% salonów

- 3691 salonów (70%) ma 3 top_services
- 1473 salonów (28%) ma 0 — mniejsze/nowsze których Booksy nie wyróżnia
- 103 ma 1-2

Frontend sekcja „Top services comparison" musi obsłużyć brak danych dla 28% salonów. Fallback: „Booksy nie wyróżnia top services dla tego salonu, brak danych porównawczych".

### 7. Wszystkie pozostałe pola pokrycie 100%

W próbie 10 salonów (9 native + 1 Versum), wszystkie miały:
- `business_categories[]` z `female_weight` per kategoria ✓
- `booking_max_modification_time`, `booking_max_lead_time`, `deposit_cancel_time` ✓
- `pos_pay_by_app_enabled`, `pos_market_pay_enabled` ✓
- `has_online_services`, `has_online_vouchers`, `has_safety_rules` ✓
- `low_availability`, `is_recommended`, `max_discount_rate`, `salon_network` ✓
- `open_hours`, `regions`, `location.coordinate` ✓
- `description`, `subdomain`, `facebook_link`, `instagram_link`, `website` ✓

Plus bonus pola nieplanowane w original planie: `accept_booksy_pay`, `best_of_booksy_badge`, `manual_boost_score`, `simplified_booking_feature_checklist`, `waitlist_disabled`, `is_renting_venue`, `umbrella_venue_name`, `contractor_description`, `traveling`, `promoted_labels`, `is_b_listing`, `profile_type`. Każdy to potencjalny dodatkowy wymiar do `competitor_dimensional_scores`.

### 8. Data layering — Opcja C (hybrid upsert registry + append time-series)

Zatwierdzona decyzja architektoniczna dla collision między kolejnymi batchami tego samego booksy_id:

- `salons` table = latest state per salon (UPSERT by booksy_id) — persistent registry
- `salon_scrapes` + `salon_scrape_services` = pełny time-series (APPEND always, no update) — historia
- `json_ingestion_log` = audit trail z content_hash dla idempotency

### Plan adjustment summary

Oryginalny plan zakłada jednen Etap 0 jako monolityczny „schema extension + backfill". Po research spike rozbijamy na:

- **Comp Etap 0.1** — Schema migrations (ALTER + CREATE tables, female_weight seed, json_ingestion_log + versum_service_mappings + partner_system flag)
- **Comp Etap 0.2** — Re-runnable JSON ingester (`scripts/ingest_salon_jsons.py`) + first full load obecnego folderu `json/`

Oryginalny Etap 8 (sentiment) pozostaje **parked** do czasu Etap 10 (bextract reviews endpoint).

**Nowy Comp Etap 9** — Versum mapping widget (self-service UI, osobne UX flow, osobna tabela persistentna).

**Nowy Comp Etap 10** — bextract reviews pagination endpoint (parked, prerequisite dla Etap 8).

Pozostałe etapy (1-7) bez zmian w scope, ale:
- **Etap 4** dostaje graceful degradation dla Versum salonów (LEFT JOIN versum_service_mappings, skip treatment-level pricing comparisons dla untreated services, compute non-pricing wymiary normalnie)
- **Etap 7** subscription uruchomi się normalnie ale pierwszy sensowny price history chart pojawi się po 2-3 snapshots (2-3 tygodnie)

## Problem

Obecny Konkurenci tab w wizardzie audytu pokazuje listę ~10 salonów z podstawowymi polami (name, rating, distance, review count) — to jest preview, nie raport. User audytu Beauty4ever płaci 79.90 zł za raport ale jedyna wartość dotycząca konkurencji to lista „oto twoi sąsiedzi". Brakuje deep analizy cen, gap analysis usługowego, pozycjonowania, dimensional scoring i actionable recommendations.

Raport konkurencji z prawdziwego zdarzenia to produkt-wewnątrz-produktu: deep scrape 5-10 starannie wybranych konkurentów → ~20 dimensional comparisons + per-treatment_id pricing benchmark + service gap analysis + SWOT data-driven + recommendations z traceability. Nie jest to „PDF z bullet pointami" tylko dashboard z ~30 porównaniami które razem opowiadają historię pozycji rynkowej.

## Architektura

**BAGENT #4** jako osobny pipeline w `pipelines/competitor_report.py`. Nie rozszerzenie BAGENT #3 (`pipelines/summary.py`) bo ma własny lifecycle, własny billing tier, własny refresh schedule i własną logic retriggerowania. Czyste architectural split.

Entry point: `async def run_competitor_report_pipeline(audit_id, tier, selection_mode, on_progress)`.

Tiers:
- `base` — single-shot raport, zapłacone raz, żadnego auto-refresh
- `premium` — subskrypcja z recurring snapshots co tydzień/dwa tygodnie, price change detection, historical charts
- `sentiment` — upsell DO dowolnego z powyższych, dokłada LLM analizę review text

Selection modes:
- `auto` — deterministyczny algorytm wybiera top N kandydatów
- `manual` — user dostaje listę 15 kandydatów z algorytmu i sam wybiera N (default `auto`)

Endpoint: `POST /api/competitor/report` z payloadem `{auditId, userId, tier, selectionMode, selectedCompetitorBooksyIds?}` (ostatni tylko dla manual). Response `{jobId, status: "accepted"}`, 202.

Webhooki do Convex:
- `POST /api/competitor/report/progress` — progress updates
- `POST /api/competitor/report/complete` — success z competitor_report_id
- `POST /api/competitor/report/fail` — failure z error message

## Co już jest w Supabase (discovery 2026-04-08)

Spora część infrastruktury jest już zbudowana — prawdopodobnie we wcześniejszej iteracji projektu. Inventory:

**Persistent registry:**
- `salons` — booksy_id PK natural, full identity fields (name, slug, description, city, address, lat, lng, geom PostGIS, primary_category_id, reviews_rank, reviews_count, pricing_level, is_recommended, phone, email, website, facebook_url, instagram_url, booking_policy text, amenities jsonb, top_service_names jsonb, photos, staff_count, last_scraped_at)
- `salon_staff` — per-staffer z reviews_rank
- `salon_open_hours` — day_of_week + open_from + open_till
- `salon_gallery` — photos
- `salon_categories` — junction z business_categories
- `business_categories` — reference taxonomy

**Per-scrape time-series:**
- `salon_scrapes` — header z **raw_response JSONB** (pełen Booksy payload), plus strukturalne kolumny, indeksowane `(booksy_id, scraped_at DESC)`. Multiple rows per booksy_id.
- `salon_scrape_services` — services z canonical taxonomy (booksy_treatment_id, treatment_parent_id, body_part, target_gender, technology), pricing (price_grosze, is_from_price, omnibus_price_grosze), variants jsonb, photos jsonb, staffer_ids jsonb, description_type, combo_type/children, suggest_tokens/context.

**Competitor report infrastructure (zastane):**
- `competitor_reports` — header z subject_salon_id FK, report_data jsonb, status, competitor_count, overall_score, market_rank, metadata, UNIQUE per convex_audit_id
- `competitor_matches` — per-competitor z composite_score, `bucket` CHECK IN ('direct', 'cluster', 'aspirational'), similarity_scores jsonb, distance_km
- `competitor_report_snapshots` — **time-series** keyed by (report_id, snapshot_date) z key_metrics jsonb — gotowa pod premium subscription tier

**Legacy:**
- `audit_competitors` — light preview używany obecnie przez BAGENT #3 summary tab (zostaje, nie ruszamy)

Co siedzi w `raw_response` ale nie jest jeszcze strukturalnie:
- indywidualne reviews z per-service/per-staffer linkage
- top_services full records (mamy tylko names w salons.top_service_names)
- booking policy details (booking_max_modification_time, booking_max_lead_time, deposit_cancel_time)
- payment flags (pos_pay_by_app_enabled, pos_market_pay_enabled)
- operational flags (has_online_services, has_online_vouchers, has_safety_rules)
- popularity signals (low_availability, max_discount_rate, is_recommended — częściowo jest)
- chain affiliation (salon_network)
- female_weight per business_category (needs verification)

Te gaps naprawiamy w Comp Etap 0.

## Schema changes (Comp Etap 0)

### Extend existing tables

```sql
-- salon_scrapes: add structured columns backfillable from raw_response
ALTER TABLE salon_scrapes
  ADD COLUMN IF NOT EXISTS salon_subdomain text,
  ADD COLUMN IF NOT EXISTS booking_max_modification_time integer,  -- minutes
  ADD COLUMN IF NOT EXISTS booking_max_lead_time integer,           -- seconds
  ADD COLUMN IF NOT EXISTS deposit_cancel_days integer,
  ADD COLUMN IF NOT EXISTS pos_pay_by_app boolean,
  ADD COLUMN IF NOT EXISTS pos_market_pay boolean,
  ADD COLUMN IF NOT EXISTS has_online_services boolean,
  ADD COLUMN IF NOT EXISTS has_online_vouchers boolean,
  ADD COLUMN IF NOT EXISTS has_safety_rules boolean,
  ADD COLUMN IF NOT EXISTS low_availability boolean,
  ADD COLUMN IF NOT EXISTS max_discount_rate numeric(5,2),
  ADD COLUMN IF NOT EXISTS salon_network text,
  ADD COLUMN IF NOT EXISTS service_fee numeric(6,2),
  ADD COLUMN IF NOT EXISTS parking_info text,
  ADD COLUMN IF NOT EXISTS wheelchair_access text;

-- competitor_reports: tier + subscription support
ALTER TABLE competitor_reports
  ADD COLUMN IF NOT EXISTS tier text NOT NULL DEFAULT 'base'
    CHECK (tier IN ('base', 'premium')),
  ADD COLUMN IF NOT EXISTS refresh_schedule text
    CHECK (refresh_schedule IN ('weekly', 'biweekly', 'monthly') OR refresh_schedule IS NULL),
  ADD COLUMN IF NOT EXISTS next_refresh_at timestamp with time zone,
  ADD COLUMN IF NOT EXISTS sentiment_upsell_purchased boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS selection_mode text NOT NULL DEFAULT 'auto'
    CHECK (selection_mode IN ('auto', 'manual'));

CREATE INDEX IF NOT EXISTS idx_competitor_reports_next_refresh
  ON competitor_reports(next_refresh_at) WHERE tier = 'premium';

-- competitor_matches: new bucket value + counts_in_aggregates flag
ALTER TABLE competitor_matches DROP CONSTRAINT IF EXISTS competitor_matches_bucket_check;
ALTER TABLE competitor_matches
  ADD CONSTRAINT competitor_matches_bucket_check
    CHECK (bucket IN ('direct', 'cluster', 'aspirational', 'new'));
ALTER TABLE competitor_matches
  ADD COLUMN IF NOT EXISTS counts_in_aggregates boolean NOT NULL DEFAULT true;

-- business_categories: verify female_weight exists (probably needs adding)
-- Will check in Comp Etap 0 migration before adding
```

### New tables

```sql
-- Individual reviews with per-service/per-staffer linkage
-- Enables: review velocity, popularity-by-mentions, sentiment analysis
CREATE TABLE IF NOT EXISTS salon_reviews (
  id bigserial PRIMARY KEY,
  salon_id integer NOT NULL REFERENCES salons(id) ON DELETE CASCADE,
  booksy_review_id integer NOT NULL,
  rank integer NOT NULL CHECK (rank BETWEEN 1 AND 5),
  title text,
  review_text text,
  user_first_name text,
  user_last_initial text,
  user_avatar_url text,
  services jsonb,  -- array of {id, name, treatment_id}
  staff jsonb,     -- array of {id, name}
  reply_content text,
  reply_updated_at timestamp with time zone,
  review_created_at timestamp with time zone NOT NULL,
  review_updated_at timestamp with time zone,
  source text,
  scraped_at timestamp with time zone NOT NULL DEFAULT now(),
  UNIQUE(salon_id, booksy_review_id)
);
CREATE INDEX idx_salon_reviews_salon_created ON salon_reviews(salon_id, review_created_at DESC);
CREATE INDEX idx_salon_reviews_rank ON salon_reviews(salon_id, rank);

-- Top services (Booksy's own algorithmic ranking)
-- Enables: salon positioning signal, flagship overlap analysis
CREATE TABLE IF NOT EXISTS salon_top_services (
  id bigserial PRIMARY KEY,
  salon_id integer NOT NULL REFERENCES salons(id) ON DELETE CASCADE,
  booksy_service_id integer NOT NULL,
  booksy_treatment_id integer,
  name text NOT NULL,
  category_name text,
  description text,
  variants jsonb NOT NULL,  -- full variant array with price, duration, staff
  is_online_service boolean,
  is_traveling_service boolean,
  sort_order integer NOT NULL,
  scraped_at timestamp with time zone NOT NULL DEFAULT now(),
  UNIQUE(salon_id, booksy_service_id)
);
CREATE INDEX idx_salon_top_services_salon ON salon_top_services(salon_id);
CREATE INDEX idx_salon_top_services_treatment ON salon_top_services(booksy_treatment_id);

-- Per-treatment_id pricing comparison row
-- One row per (report, treatment_id, competitor)
CREATE TABLE IF NOT EXISTS competitor_pricing_comparisons (
  id bigserial PRIMARY KEY,
  report_id integer NOT NULL REFERENCES competitor_reports(id) ON DELETE CASCADE,
  booksy_treatment_id integer NOT NULL,
  treatment_name text NOT NULL,
  treatment_parent_id integer,
  subject_price_grosze integer,
  subject_is_from_price boolean,
  subject_duration_minutes integer,
  market_min_grosze integer,
  market_median_grosze integer,
  market_max_grosze integer,
  market_p25_grosze integer,
  market_p75_grosze integer,
  subject_percentile numeric(5,2),
  deviation_pct numeric(6,2),  -- (subject - market_median) / market_median * 100
  sample_size integer NOT NULL,
  recommended_action text,  -- "raise", "lower", "hold"
  created_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_competitor_pricing_report ON competitor_pricing_comparisons(report_id);
CREATE INDEX idx_competitor_pricing_treatment ON competitor_pricing_comparisons(report_id, booksy_treatment_id);

-- Service gap analysis
CREATE TABLE IF NOT EXISTS competitor_service_gaps (
  id bigserial PRIMARY KEY,
  report_id integer NOT NULL REFERENCES competitor_reports(id) ON DELETE CASCADE,
  gap_type text NOT NULL CHECK (gap_type IN ('missing', 'unique_usp')),
  booksy_treatment_id integer NOT NULL,
  treatment_name text NOT NULL,
  treatment_parent_id integer,
  competitor_count integer NOT NULL,  -- for 'missing': how many competitors have it
  avg_price_grosze integer,            -- market avg (for 'missing') or subject price (for 'unique_usp')
  popularity_score numeric(5,2),       -- derived from top_services mentions + review mentions
  sort_order integer NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_competitor_gaps_report ON competitor_service_gaps(report_id);

-- Dimensional scoring (~20 comparison axes)
-- No single overall score — each dimension is its own row
CREATE TABLE IF NOT EXISTS competitor_dimensional_scores (
  id bigserial PRIMARY KEY,
  report_id integer NOT NULL REFERENCES competitor_reports(id) ON DELETE CASCADE,
  dimension text NOT NULL,  -- 'description_coverage', 'photo_coverage', 'opening_hours_per_week', etc.
  subject_value numeric(10,2) NOT NULL,
  market_min numeric(10,2),
  market_p25 numeric(10,2),
  market_p50 numeric(10,2),
  market_p75 numeric(10,2),
  market_max numeric(10,2),
  subject_percentile numeric(5,2),
  better_is_higher boolean NOT NULL DEFAULT true,
  unit text,  -- 'percent', 'hours', 'count', 'zloty', 'days'
  category text,  -- grouping: 'content_quality', 'pricing', 'operations', 'social_proof'
  sort_order integer NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_competitor_dimensions_report ON competitor_dimensional_scores(report_id);
CREATE INDEX idx_competitor_dimensions_category ON competitor_dimensional_scores(report_id, category);

-- Actionable recommendations with traceability
-- Each recommendation points back to the competitors + data points that drove it
CREATE TABLE IF NOT EXISTS competitor_recommendations (
  id bigserial PRIMARY KEY,
  report_id integer NOT NULL REFERENCES competitor_reports(id) ON DELETE CASCADE,
  action_title text NOT NULL,
  action_description text NOT NULL,
  category text NOT NULL,  -- 'pricing', 'content', 'services', 'operations', 'social'
  impact text NOT NULL CHECK (impact IN ('low', 'medium', 'high')),
  effort text NOT NULL CHECK (effort IN ('low', 'medium', 'high')),
  confidence numeric(3,2) NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  estimated_revenue_impact_grosze integer,
  source_competitor_ids integer[] NOT NULL,  -- which competitors informed this
  source_data_points jsonb NOT NULL,           -- references: [{type:'pricing_comparison', id:123}, {type:'dimensional_score', id:45}]
  sort_order integer NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_competitor_recommendations_report ON competitor_recommendations(report_id);

-- Sentiment analysis results (upsell tier)
CREATE TABLE IF NOT EXISTS competitor_review_sentiments (
  id bigserial PRIMARY KEY,
  report_id integer NOT NULL REFERENCES competitor_reports(id) ON DELETE CASCADE,
  competitor_salon_id integer NOT NULL REFERENCES salons(id),
  booksy_treatment_id integer,  -- null = overall salon sentiment
  sentiment_score numeric(3,2) NOT NULL CHECK (sentiment_score BETWEEN -1 AND 1),
  review_count integer NOT NULL,
  top_themes jsonb NOT NULL,        -- ["professional staff", "clean facility", "punctual"]
  recurring_praises jsonb NOT NULL,
  recurring_complaints jsonb NOT NULL,
  highlighted_quotes jsonb,          -- ["best cut i ever had", "always late"]
  language_quality_score numeric(3,2),
  analyzed_at timestamp with time zone NOT NULL DEFAULT now(),
  UNIQUE(report_id, competitor_salon_id, booksy_treatment_id)
);
CREATE INDEX idx_competitor_sentiments_report ON competitor_review_sentiments(report_id);
```

### Backfill existing data

After ALTER TABLE, run UPDATE jobs to populate new columns from existing `raw_response` JSONB:

```sql
UPDATE salon_scrapes
SET
  salon_subdomain = raw_response->>'subdomain',
  booking_max_modification_time = (raw_response->>'booking_max_modification_time')::integer,
  booking_max_lead_time = (raw_response->>'booking_max_lead_time')::integer,
  deposit_cancel_days = (raw_response->'deposit_cancel_time'->>'days')::integer,
  pos_pay_by_app = (raw_response->>'pos_pay_by_app_enabled')::boolean,
  -- etc.
WHERE salon_subdomain IS NULL AND raw_response IS NOT NULL;
```

Plus separate backfill jobs for `salon_reviews` and `salon_top_services` from `salon_scrapes.raw_response` for historical data (last 30 days of scrapes). Older data stays as raw_response only — we don't backfill indefinitely, just recent enough to give the first Beauty4ever competitor report something to work with.

## Competitor selection algorithm (Comp Etap 1)

Deterministyczny, debuggable, bez LLM:

```python
async def select_competitors(
    subject_audit_id: str,
    target_count: int = 5,
    mode: str = 'auto',
) -> list[CompetitorCandidate]:
    # Load subject salon
    subject = await load_subject_salon(subject_audit_id)
    subject_lat = subject.salon_lat
    subject_lng = subject.salon_lng
    subject_primary_cat = subject.primary_category_id
    subject_female_weight = compute_avg_female_weight(subject.business_categories)
    subject_top_treatments = set(s.booksy_treatment_id for s in subject.top_services)

    # Query candidates via bextract listing API
    # GET /businesses?category={primary_cat}&region_id={city_id}&order_by=popularity&limit=30
    candidates = await bextract.list_businesses(
        category_id=subject_primary_cat,
        region_id=subject.city_region_id,
        limit=30,
    )

    # Filter
    filtered = []
    for c in candidates:
        if c.booksy_id == subject.booksy_id:
            continue  # skip self
        c_female_weight = compute_avg_female_weight(c.business_categories)
        if abs(c_female_weight - subject_female_weight) > 20:
            continue
        distance = haversine(subject_lat, subject_lng, c.lat, c.lng)
        if distance > 15:  # km
            continue
        filtered.append((c, distance, c_female_weight))

    # Score
    scored = []
    for c, dist, c_fw in filtered:
        score = 0
        # Always true due to filter, but keep for explicit scoring
        if c.primary_category_id == subject_primary_cat:
            score += 30
        # Business categories jaccard
        c_cats = set(bc['id'] for bc in c.business_categories)
        s_cats = set(bc['id'] for bc in subject.business_categories)
        jaccard = len(c_cats & s_cats) / max(len(c_cats | s_cats), 1)
        score += int(20 * jaccard)
        # Top services treatment overlap (requires lightweight scrape of top_services only)
        c_top = await bextract.get_top_services(c.booksy_id)
        c_top_treatments = set(ts.booksy_treatment_id for ts in c_top)
        ts_overlap = len(c_top_treatments & subject_top_treatments) / max(len(subject_top_treatments), 1)
        score += int(25 * ts_overlap)
        # Reviews count similarity
        if subject.reviews_count > 0:
            ratio = min(c.reviews_count, subject.reviews_count) / max(c.reviews_count, subject.reviews_count)
            score += int(10 * ratio)
        # Distance penalty
        score -= int(2 * max(dist - 5, 0))
        scored.append((c, score, dist))

    # Bucket assignment
    bucketed = []
    for c, score, dist in scored:
        if c.reviews_count < 20:
            bucket = 'new'
            counts_in_aggregates = False
        elif score >= 70:
            bucket = 'direct'
            counts_in_aggregates = True
        elif score >= 40:
            bucket = 'cluster'
            counts_in_aggregates = True
        elif c.reviews_rank and c.reviews_rank >= subject.reviews_rank + 0.3:
            bucket = 'aspirational'
            counts_in_aggregates = True
        else:
            continue  # not interesting enough
        bucketed.append(CompetitorCandidate(c, score, dist, bucket, counts_in_aggregates))

    # Sort: direct first (by score), then cluster, then aspirational, new last
    bucket_priority = {'direct': 0, 'cluster': 1, 'aspirational': 2, 'new': 3}
    bucketed.sort(key=lambda c: (bucket_priority[c.bucket], -c.score))

    if mode == 'auto':
        return bucketed[:target_count]
    else:  # manual
        return bucketed[:15]  # frontend shows 15, user picks
```

Key design notes:
- Primary category match is a strict filter, not just a score boost
- Female weight filter is ±20 — prevents Barber vs Salon Kosmetyczny mismatches
- Distance cap is 15km (adjustable per city density — maybe 5km in Warsaw, 30km in small towns)
- Top services overlap is the strongest positive signal (0.25× max) — salons with overlapping top services are real competitors even if they're in different exact categories
- New bucket = reviews_count < 20 is shown to user but excluded from aggregates per user decision #4
- Aspirational bucket captures "better than you, track them" — higher rating, lower overlap, worth monitoring

## Pipeline steps (Comp Etap 2-5)

```
Step 1: Load subject salon from salon_scrapes by audit_id (progress 5)
Step 2: Run candidate selection algorithm (progress 10-15)
Step 3: Deep scrape each selected competitor via bextract in parallel (progress 15-55, bounded concurrency 3)
Step 4: Persist scrape results to salon_scrapes + salon_scrape_services + salon_reviews + salon_top_services (progress 60)
Step 5: Compute pricing comparisons per treatment_id (progress 65)
Step 6: Compute service gap analysis (progress 70)
Step 7: Compute dimensional scores (~20 axes) (progress 75)
Step 8: AI synthesis - positioning narrative + SWOT + recommendations with traceability (progress 80-95)
Step 9: Save competitor_reports + children tables (progress 95)
Step 10: If premium tier: set next_refresh_at = now + refresh_schedule (progress 98)
Step 11: Fire /api/competitor/report/complete webhook (progress 100)
```

Expected timing: 3-10 min end-to-end for base tier (dominated by bextract scrape time for N competitors + single MiniMax agent call for synthesis).

Concurrency: `asyncio.gather` with semaphore limiting bextract calls to 3 parallel (respect tytan rate limits).

Failure handling: per-competitor failures (bextract timeout, parse error) are non-fatal — pipeline continues with remaining competitors, final report notes `{scraped: 4, failed: 1}` in metadata.

## Report sections (Comp Etap 6 — frontend)

Sub-taby w Konkurenci tab po redesignie:

### Sub-tab 1: Mapa rynku
- Leaflet map centered on subject lat/lng
- Subject highlighted (gold marker + pulse)
- Each competitor as marker with thumbnail + bucket color (direct/cluster/aspirational/new)
- Click competitor → side panel z quick stats (name, rating, reviews, distance, top 3 services)
- Distance rings (5km, 10km, 15km)

### Sub-tab 2: Pozycjonowanie
- Self-description comparison: subject's `description` vs each competitor's — shows how market positions itself
- Top services side-by-side: 3 usługi per salon, overlap highlighted
- Dimensional scores grouped by category (content_quality, pricing, operations, social_proof)
- Per dimension: horizontal bar showing subject's percentile vs market distribution (p25/p50/p75 shaded)
- Visual: if subject is in top quartile → green check, bottom → red flag

### Sub-tab 3: Cennik vs konkurencja
- Interactive table, one row per treatment_id that subject offers AND ≥2 competitors offer
- Columns: service name, subject price, market median, subject quartile, deviation %, recommended action
- Sortable by deviation % (biggest outliers first)
- Visual: inline sparkline of market distribution with subject's position marked
- Click row → expand to show per-competitor prices with logos

### Sub-tab 4: Luki i USP
- Two lists side by side:
  - Missing services (gap_type='missing') — sorted by popularity score
  - Unique USPs (gap_type='unique_usp') — services only subject offers
- Each item: treatment name, competitor count, avg market price, "dodaj do oferty" / "podkreśl w marketingu" action

### Sub-tab 5: SWOT + Akcje
- SWOT quadrants (Strengths, Weaknesses, Opportunities, Threats) — each bullet data-driven with source link
- Click bullet → see which dimensional score / pricing row / service gap drove it
- Below: Recommendations list sorted by (impact × confidence) / effort
- Each recommendation expandable with traceability panel: "bazując na tych danych" pokazuje konkretne konkurenty + dane które uzasadniają akcję (identyczny pattern jak Etap 2 audit traceability)

### Sub-tab 6: Historia cen (premium only)
- Line chart per treatment_id showing price movement over time
- Subject vs market median over last 3-6 months
- Alert markers gdzie konkurent zmienił cenę > 5%
- Filter: show top N treatment_ids by subject revenue (if known) or by deviation

### Sub-tab 7: Sentiment (sentiment upsell only)
- Per competitor card: overall sentiment score (-1 to 1), review_count sample size
- Top themes word cloud
- Recurring praises (green) vs complaints (red)
- Highlighted quotes (positive + negative) z datami

## Subscription cron architecture (Comp Etap 7)

Convex cron w `convex/crons.ts`:

```typescript
import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";

const crons = cronJobs();

crons.interval(
  "refresh-premium-competitor-reports",
  { hours: 6 },
  internal.competitorReports.refreshDue,
  {}
);

export default crons;
```

Internal action `competitorReports.refreshDue`:
1. Query `competitor_reports` WHERE `tier = 'premium'` AND `next_refresh_at < now()`
2. For each due report, trigger bagent `/api/competitor/report/refresh` z `{reportId}`
3. Bagent pipeline `refresh_competitor_report(report_id)`:
   - Load existing competitor_matches for this report
   - Re-scrape each competitor via bextract
   - Save new rows to salon_scrapes (time-series, adds row not update)
   - Compute new key_metrics
   - INSERT into competitor_report_snapshots (report_id, snapshot_date=today, key_metrics jsonb)
   - Compare with previous snapshot — if any treatment_id price delta > 5%, mark as significant_change
   - Update competitor_reports.next_refresh_at = now + refresh_schedule_interval
4. If significant_change, trigger email via Resend

Price change detection query using window function:

```sql
WITH latest_prices AS (
  SELECT
    booksy_id,
    booksy_treatment_id,
    booksy_service_id,
    price_grosze,
    scraped_at,
    LAG(price_grosze) OVER (
      PARTITION BY booksy_id, canonical_id
      ORDER BY scraped_at DESC
    ) AS previous_price
  FROM salon_scrape_services
  WHERE booksy_id = ANY($1)
    AND scraped_at >= now() - interval '14 days'
)
SELECT booksy_id, booksy_treatment_id, price_grosze, previous_price,
       (price_grosze - previous_price)::numeric / previous_price * 100 AS pct_change
FROM latest_prices
WHERE previous_price IS NOT NULL
  AND price_grosze != previous_price
  AND ABS((price_grosze - previous_price)::numeric / previous_price) > 0.05;
```

## Sentiment upsell pipeline (Comp Etap 8)

Separate pipeline `pipelines/competitor_sentiment.py`. Triggered by `POST /api/competitor/sentiment` z payloadem `{competitorReportId, userId}`. Only runs if user bought the upsell (`competitor_reports.sentiment_upsell_purchased = true`).

Pipeline:
1. Load `competitor_matches` for the report
2. For each competitor, load recent reviews from `salon_reviews` (last 90 days, top 50 by created_at)
3. Cluster reviews by `services[].treatment_id` inside the review JSONB
4. Per (competitor, treatment_id) cluster, call MiniMax with prompt "Analyze the tone, recurring praises, recurring complaints, language quality of these Polish beauty salon reviews. Return JSON."
5. Also do a per-competitor overall analysis (no treatment filter)
6. Save results to `competitor_review_sentiments`
7. Fire `/api/competitor/sentiment/complete` webhook

Cost estimate: 5 konkurentów × ~10 treatment clusters × 30-50 reviews per cluster × single MiniMax call per cluster = ~50 AI calls per sentiment run, ~100k input tokens, ~30k output. Budget ~$0.50-$1.00 per sentiment upsell generation.

## Traceability framework

Extends pattern from Etap 2 audit traceability:

Every `competitor_recommendations` row has:
- `source_competitor_ids integer[]` — which competitors' data drove this recommendation (FK to salons.id)
- `source_data_points jsonb` — references to specific data rows, e.g.:
  ```json
  [
    {"type": "pricing_comparison", "id": 123, "treatment_id": 681},
    {"type": "dimensional_score", "id": 45, "dimension": "photo_coverage"},
    {"type": "service_gap", "id": 67, "treatment_id": 294}
  ]
  ```

Frontend recommendation card ma expand button „Dlaczego to polecamy?" który renderuje:
- Logos + names of the source competitors
- Specific data points as small cards linking back to Sub-tab 3 (Cennik), Sub-tab 2 (Pozycjonowanie), Sub-tab 4 (Luki)
- Clicking a data point scrolls/jumps to that comparison in the other tab

Same pattern w SWOT items — każdy bullet SWOT ma `source_data_points jsonb` i expand „Dlaczego tak uważamy?"

## Dimensional scores — pełna lista ~20 wymiarów

Grouped by category:

**content_quality:**
- description_coverage (% serwisów z description_type='M')
- photo_coverage (% serwisów ze zdjęciami)
- self_description_length (length of salon.description in words)
- avg_description_length (avg length per service description)

**pricing:**
- fixed_price_ratio (% serwisów z variant type='X' zamiast 'S')
- avg_price_per_category (per business_category)
- price_range_spread (max - min price)
- omnibus_price_compliance (% z omnibus_price_grosze set)
- promo_intensity (% serwisów z is_promo=true)

**operations:**
- opening_hours_per_week (sum of (open_till - open_from) across open_hours)
- weekend_availability (boolean: open on Saturday/Sunday)
- evening_availability (boolean: open after 18:00 any day)
- booking_flexibility (derived: 10 - abs(120 - booking_max_modification_time) / 30)
- booking_lead_time_days (booking_max_lead_time / 86400)
- deposit_required (boolean: deposit_cancel_days > 0)

**digital_maturity:**
- has_online_services (boolean)
- has_online_vouchers (boolean)
- pos_pay_by_app (boolean)
- digital_maturity_score (count of above flags that are true)

**social_proof:**
- reviews_count
- reviews_rank (average star rating)
- review_velocity_30d (reviews count in last 30 days / 30)
- owner_reply_rate (% of reviews z reply_content)
- social_presence_count (count of non-null: facebook_url, instagram_url, website)

**portfolio:**
- total_services
- total_categories
- services_per_category (avg)
- combo_ratio (% of services z combo_type)
- unique_treatment_count (distinct booksy_treatment_id count)

Każdy dimension jest row w `competitor_dimensional_scores`, z subject value + market distribution (min/p25/p50/p75/max) + subject percentile.

## Etapy implementacji (revised post research spike)

Zależności i kolejność:

```
Phase 1 — Foundation
  Comp Etap 0.1 (schema migrations) — blokuje wszystkie pozostałe
    ↓
  Comp Etap 0.2 (re-runnable JSON ingester + first full load z json/ folderu)
    ↓
Phase 2 — Competitor Report Core
  Comp Etap 1 (candidate selection z już-załadowanego salons registry)
    ↓
  Comp Etap 2 (BAGENT #4 scaffold — używa istniejących scrape'ów + deep scrape ewentualnych missing)
    ↓
  Comp Etap 3 (reviews sample + top_services ingestion) || Comp Etap 4 (pricing + gaps + dimensional)
    ↓
  Comp Etap 5 (AI synthesis + traceability)
    ↓
Phase 3 — Frontend
  Comp Etap 6 (Konkurenci tab redesign) — sequential bo używa stable data z Etap 4+5
    ↓
Phase 4 — Subscription & upsells
  Comp Etap 7 (premium subscription cron) — równoległy z Etap 9
  Comp Etap 9 (Versum mapping widget) — równoległy z Etap 7, backend table + frontend widget
    ↓
Phase 5 — Parked (prerequisite deps not met)
  Comp Etap 10 (bextract reviews pagination endpoint) — separate bextract repo work
    ↓
  Comp Etap 8 (sentiment upsell) — depends on Etap 10
```

Orientacyjny time budget: Phase 1-3 to ~2 dni agents team (podobny pattern jak Unified Report Pipeline ale z większą liczbą etap'ów). Phase 4 dodatkowy 1 dzień. Phase 5 parked do osobnej decyzji.

## Kryteria sukcesu per etap

**Comp Etap 0.1 (schema):** Migrations applied, `\d competitor_reports` pokazuje nowe kolumny tier/refresh_schedule/next_refresh_at/sentiment_upsell_purchased/selection_mode, `\d salon_scrapes` ma nowe kolumny (partner_system, booking_max_*, pos_*, has_*, low_availability, max_discount_rate, salon_network, salon_subdomain, service_fee, parking_info, wheelchair_access), `\d business_categories` ma kolumnę female_weight z wartościami seededami, nowe tabele (salon_reviews, salon_top_services, competitor_pricing_comparisons, competitor_service_gaps, competitor_dimensional_scores, competitor_recommendations, competitor_review_sentiments, versum_service_mappings, json_ingestion_log) istnieją z odpowiednimi indeksami i FK.

**Comp Etap 0.2 (ingester + first load):** Skrypt `scripts/ingest_salon_jsons.py` istnieje w bagent repo, jest re-runnable (idempotentny po content_hash), przetwarza całe folder `json/` w <30 min, po zakończeniu: `SELECT COUNT(*) FROM salons` = 5267, `SELECT COUNT(*) FROM salon_scrapes` = 5267, `SELECT COUNT(*) FROM salon_scrape_services` > 100000, `SELECT COUNT(*) FROM json_ingestion_log` = 5267, `SELECT COUNT(DISTINCT booksy_id) FROM salon_scrapes WHERE partner_system = 'versum'` = 240, re-run skryptu bez nowych plików loguje „5267 already ingested, 0 new" i kończy w <30s.

**Comp Etap 1:** Funkcja `select_competitors(beauty4ever_audit_id, 5, 'auto')` zwraca listę 5 salonów, wszystkie z `primary_category_id = subject.primary_category_id`, wszystkie z abs(female_weight diff) ≤ 20, wszystkie w promieniu 15km. Spot check ręczny — 5 zwróconych to faktycznie sensowni konkurenci Beauty4ever w Warszawie.

**Comp Etap 2:** `POST /api/competitor/report` dla Beauty4ever z `{tier: "base", selectionMode: "auto"}` zwraca 202 + jobId, background job kończy w <10 min, w Supabase jest wiersz `competitor_reports` z `status='completed'`, 5 wierszy w `competitor_matches`, 5 nowych scrape'ów w `salon_scrapes` (po jednym per competitor).

**Comp Etap 3:** `salon_reviews` ma wiersze dla Beauty4ever oraz jej 5 konkurentów, każdy review linkuje do service + staff (jeśli były w źródle), `salon_top_services` ma 3 rows per salon.

**Comp Etap 4:** Dla Beauty4ever competitor report wiersze w `competitor_pricing_comparisons` istnieją dla każdego treatment_id który Beauty4ever oferuje i min. 2 konkurentów też. `competitor_service_gaps` ma top 10 missing + top 5 unique_usp. `competitor_dimensional_scores` ma 20+ wierszy dla tego reportu.

**Comp Etap 5:** `competitor_recommendations` ma 8-12 wierszy, każdy z niepustym `source_competitor_ids` i `source_data_points`, `competitor_reports.report_data` ma pola `positioning_narrative` i `swot`.

**Comp Etap 6:** Frontend Konkurenci tab renderuje 5 sub-tabów (Mapa, Pozycjonowanie, Cennik, Luki, SWOT+Akcje), wszystkie ciągną dane z Supabase przez Convex queries, recommendation card's expand pokazuje traceability panel z logo konkurenta.

**Comp Etap 7:** Convex cron pokazuje się w dashboard, po ręcznym set `next_refresh_at = now()` dla test report, cron triggeruje refresh, dodaje nowy snapshot, `competitor_report_snapshots` ma nowy wiersz, `next_refresh_at` zostaje zaktualizowany o 7 dni.

**Comp Etap 8:** `POST /api/competitor/sentiment {competitorReportId}` dla test reportu kończy w <5 min, `competitor_review_sentiments` ma wiersze per (competitor × treatment cluster), frontend Sub-tab 7 renderuje sentiment scores z themes. **PARKED — wymaga Etap 10 (bextract reviews endpoint) jako prerequisite.**

**Comp Etap 9 (Versum mapping widget):** Tabela `versum_service_mappings` istnieje i ma unique constraint na `(salon_id, booksy_service_id)`. Frontend ścieżka `/settings/versum-mapping` (albo modal w profile page) pokazuje się tylko dla Convex user'a którego salon ma `partner_system='versum'`. Widget listuje unmapped services z salon_scrape_services WHERE booksy_treatment_id IS NULL, smart dropdown pokazuje fuzzy search po Booksy treatments z reference table, bulk apply button działa, zapis persistuje do versum_service_mappings. Po zapisie i retrigger raportu konkurencji dla Beauty4ever, `competitor_pricing_comparisons` pokazuje więcej rows (bo więcej services ma teraz treatment_id via mapping). E2E test: zmapować minimum 10 usług Beauty4ever, retrigger raport, sprawdzić że liczba pricing_comparison rows wzrosła.

**Comp Etap 10 (bextract reviews endpoint):** Nowy endpoint `GET /api/reviews/{booksy_id}?page=N&per_page=50` w bextract service na tytanie, pageuje reviews z Booksy UI, zwraca JSON array review records w tym samym shape co `raw_response.business.reviews[]`. Rate-limited żeby nie dostać blokady, logs w bextract. Post-deploy smoke test: GET dla Beauty4ever booksy_id zwraca strony po 50 reviews do łącznej liczby 2359. **PARKED — osobna praca w bextract repo, nie w bagent.**

## Wzorzec dispatchowania agents team

Identyczny jak Unified Report Pipeline:
- Sekwencyjnie Etap 0 → 1 → 2 (każdy ma dependency od poprzedniego)
- Parallel: Etap 3 + Etap 4 (różne tabele, różne agenty)
- Sekwencyjnie: Etap 5 (wymaga Etap 4)
- Parallel: Etap 6 frontend + Etap 7 cron (różne repo, różne agenty)
- Na końcu: Etap 8 jako osobna dispatch gdy user potwierdzi że chce sentiment upsell w v1

Każdy agent dostaje:
1. Link do tego plan doc'u
2. Konkretne pliki do stworzenia/zmodyfikowania
3. Instrukcja nie ruszać pipelines/free_report.py
4. Testy i deploy procedure
5. Kryterium sukcesu dla jego etapu

Po każdym commit agenta weryfikacja w Supabase + ewentualny retrigger pipeline dla zweryfikowania E2E na Beauty4ever.
