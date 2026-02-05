from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PHARMA_INTEL_", env_file=".env", extra="ignore")

    # where we keep the SQLite DB by default
    db_url: str = "sqlite:///data/intel.db"

    # evidence store root
    evidence_root: Path = Path("data/evidence")

    # network
    http_timeout_s: int = 45
    http_user_agent: str = "pharma-intel-mvp/0.1 (contact: you@example.com)"

    # ingestion throttling
    ctg_page_size: int = 100
    ctg_max_pages_per_query: int = 50  # safety cap to avoid unbounded loops
    ctg_sleep_s: float = 0.2

    # matching
    fuzzy_threshold: int = 92
    min_alias_len_for_trial_search: int = 4

    # optional LLM-assisted cleaning (Gemini)
    # Enable with: PHARMA_INTEL_LLM_CLEAN_ENABLED=true
    llm_clean_enabled: bool = False
    # Provide via env: PHARMA_INTEL_GEMINI_API_KEY
    gemini_api_key: str | None = None
    # Default to the free/fast model; override with PHARMA_INTEL_GEMINI_MODEL
    gemini_model: str = "gemini-1.5-flash"
    gemini_timeout_s: int = 45
    # Safety valve for free-tier quotas
    gemini_max_calls_per_run: int = 200

settings = Settings()
