"""Application settings loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    minimax_api_key: str = ""
    minimax_base_url: str = "https://api.minimax.io/anthropic"
    minimax_model: str = "MiniMax-M2.7"
    gemini_api_key: str = ""
    supabase_url: str = ""
    supabase_service_key: str = ""
    convex_url: str = ""
    convex_deploy_key: str = ""
    api_key: str = ""
    port: int = 3001

    # Redis / arq job queue
    # Default: localhost:6379 with no password (override in production via env).
    # On tytan (production) Redis runs in Docker compose under the host
    # `redis-bagent` (or `127.0.0.1:6379` if exposed on host network).
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_password: str = ""
    redis_database: int = 0

    # Issue #23 — bextract scraper service used by the scrape orchestrator.
    # bextract caches the Booksy x-api-key and serves /api/salon/:id which
    # returns the raw business JSON we already know how to ingest.
    bextract_api_url: str = "https://api.booksyaudit.pl"
    bextract_api_key: str = ""

    # Worker self-identification for SELECT ... FOR UPDATE SKIP LOCKED.
    # Set per-process in PM2 ecosystem so logs + locked_by columns disambiguate.
    scrape_worker_id: str = "scrape-worker-default"

    # Iter 8 — Outreach automation (wintact.io + Mailgun EU).
    # The wintact API key is a long-lived JWT; workspace_id is hardcoded
    # to "booksyaudit" inside services/wintact.py.
    wintact_api_key: str = ""
    # Shared secret for /api/wintact/webhook/<secret> URL path. Wintact
    # doesn't support Bearer auth on outgoing webhooks, so the operator
    # generates a UUID-style token and configures it in BOTH the
    # wintact webhook URL AND this env var. Falls back to api_key when
    # unset (lets dev work without an extra env), but production MUST
    # set a dedicated value.
    wintact_webhook_secret: str = ""
    # Daily send caps per cohort (orchestrator obeys these on top of the
    # per-contact frequency caps stored in outreach_frequency_caps).
    outreach_daily_send_cap_scale: int = 200
    outreach_daily_send_cap_opt_in: int = 50
    # Default UTM source written into all outreach links so attribution
    # joins back through outreach_messages.utm_* columns.
    outreach_utm_source: str = "outreach"
    # Outreach worker id (mirrors scrape_worker_id pattern).
    outreach_worker_id: str = "outreach-worker-default"

    # Stage-5 commit 2 — taxonomy consistency anchors. Rule 0 lookup
    # in `_resolve_service_taxonomy` only applies an anchor when the
    # confidence_count (= COUNT(DISTINCT audit_id) from
    # taxonomy_anchor_audit_log) reaches this threshold. Default 1 =
    # first cross-audit confirmation already forces the anchor.
    # Increase later once production data shows the right balance
    # between stability and over-locking.
    taxonomy_anchor_min_confidence: int = 1

    # Pass 5 LLM provider (taxonomy consistency layer). 'openai' is
    # default because MiniMax M2.7 empirically exhausts thinking-
    # token budget on prompts with method gate + 25+ mixed clusters.
    # 'minimax' kept as fallback for A/B testing or when OPENAI_API_KEY
    # is unavailable.
    openai_api_key: str = ""

    # --- Embedding fallback (beads BEAUTY_AUDIT-lrh Phase 2) ---
    # OpenAI text-embedding-3-small is PRIMARY. On a 429/quota or connection
    # error the ingest path falls back to the local mmlw sidecar (Phase 1,
    # embeddings-local/, default :3010). Rollout order is sidecar-UP THEN this
    # code, so default embedding_local_enabled=True is safe — if the sidecar is
    # down the helper degrades to None gracefully (today's behavior). Flip
    # embedding_primary to "mmlw" to pin the sidecar (soft, reversible step
    # toward self-hosting); set embedding_local_enabled=False for a fast
    # rollback to today's OpenAI-only behavior.
    embedding_primary: str = "openai"          # "openai" | "mmlw"
    embedding_local_url: str = "http://127.0.0.1:3010"
    embedding_local_enabled: bool = True

    # extra="ignore" lets us put arbitrary env vars in .env (e.g. BUGSINK_DSN_*,
    # HC_PING_*) without having to declare each one as a pydantic field.
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
