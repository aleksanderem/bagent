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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
