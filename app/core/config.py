"""
Core configuration module.

Loads environment variables via Pydantic Settings and parses models.yaml
to produce a list of model backend configurations.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import List, Optional

import yaml
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ModelConfig:
    """Configuration for a single LLM backend model."""

    def __init__(self, model_id: str, base_url: str, api_key: str) -> None:
        self.model_id = model_id
        # Strip trailing slash so we can always append /endpoint cleanly
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def __repr__(self) -> str:  # pragma: no cover
        return f"ModelConfig(model_id={self.model_id!r}, base_url={self.base_url!r})"


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = "mysql+aiomysql://root:password@localhost:3306/llm_router"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # JWT / Auth
    SECRET_KEY: str = "change-me-in-production-use-long-random-string"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # LDAP
    LDAP_SERVER: str = "ldap://ldap.example.com"
    LDAP_BASE_DN: str = "dc=example,dc=com"
    LDAP_BIND_DN: str = "cn=service,dc=example,dc=com"
    LDAP_BIND_PASSWORD: str = "service-password"

    # Super-user bootstrap
    SUPERUSER_USERNAME: str = "admin"

    # Model config file path (relative to CWD or absolute)
    MODELS_CONFIG_PATH: str = "config/models.yaml"

    # Prometheus
    PROMETHEUS_PORT: int = 9090

    # Fallback API key for backends whose api_key field is left empty
    API_KEY: str = ""

    # ------------------------------------------------------------------ #
    # Derived helpers
    # ------------------------------------------------------------------ #

    def load_model_configs(self) -> List[ModelConfig]:
        """
        Parse MODELS_CONFIG_PATH and return a list of ModelConfig objects.

        If a model's api_key field is empty the global API_KEY env var is used.
        """
        path = self.MODELS_CONFIG_PATH
        if not os.path.isabs(path):
            # Resolve relative to the current working directory
            path = os.path.join(os.getcwd(), path)

        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        models: List[ModelConfig] = []
        for entry in raw.get("models", []):
            api_key = entry.get("api_key", "") or self.API_KEY
            models.append(
                ModelConfig(
                    model_id=entry["model_id"],
                    base_url=entry["base_url"],
                    api_key=api_key,
                )
            )
        return models


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance (cached after first call)."""
    return Settings()


# Convenience module-level alias
settings = get_settings()
