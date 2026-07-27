from functools import lru_cache

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_base_url: AnyHttpUrl = "https://api.escuelajs.co/api/v1"

    api_page_size: int = Field(default=50, ge=1, le=100)
    api_max_pages: int = Field(default=100, ge=1, le=10_000)

    api_connect_timeout_seconds: float = Field(default=5, gt=0)
    api_read_timeout_seconds: float = Field(default=30, gt=0)
    api_write_timeout_seconds: float = Field(default=10, gt=0)
    api_pool_timeout_seconds: float = Field(default=5, gt=0)

    api_max_attempts: int = Field(default=4, ge=1, le=10)
    api_backoff_seconds: float = Field(default=1, ge=0, le=60)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PLATZI_",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
        validate_default=True,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
