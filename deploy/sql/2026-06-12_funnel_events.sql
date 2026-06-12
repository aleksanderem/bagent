-- FUNNEL_AUDIT R6 — jednolity strumień zdarzeń lejka (2026-06-12).
-- Jedno miejsce na przejścia między modułami trójkąta: zakupy (Convex),
-- kliknięcia/odpowiedzi outreach (wintact webhook), atrybucje kampanii Meta.
-- Zasilanie: services/funnel_events.py (idempotentnie po dedupe_key).
--
-- APLIKACJA: jednorazowo na Supabase (SQL editor / supabase mcp execute_sql).

create table if not exists funnel_events (
  id bigint generated always as identity primary key,
  occurred_at timestamptz not null default now(),

  -- np. purchase_audit | purchase_competitor_report | purchase_monitoring |
  --     outreach_click | outreach_reply | ad_attribution | purchase_attributed
  event_type text not null,

  -- skąd przyszło zdarzenie: convex | outreach | meta_ads
  source text not null,

  -- identyfikatory — wypełniane te, które źródło zna (reszta NULL):
  user_id text,            -- Convex user id
  salon_ref_id bigint,     -- salons.id
  contact_id bigint,       -- outreach_contacts.id
  audit_id text,           -- Convex audit id
  campaign_id text,        -- Meta campaign id

  metadata jsonb not null default '{}'::jsonb,

  -- idempotencja zasilania (źródła mogą retry'ować)
  dedupe_key text not null unique
);

create index if not exists funnel_events_type_time_idx
  on funnel_events (event_type, occurred_at desc);
create index if not exists funnel_events_user_idx
  on funnel_events (user_id) where user_id is not null;
create index if not exists funnel_events_contact_idx
  on funnel_events (contact_id) where contact_id is not null;

comment on table funnel_events is
  'FUNNEL_AUDIT R6: przejscia lejka trojkata (audyt/raport/monitoring) z 3 zrodel; zasilanie services/funnel_events.py';
