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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
