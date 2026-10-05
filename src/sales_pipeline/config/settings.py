"""Centralised, validated configuration.

Every environment variable the application reads is declared here, once. The rest of the
code receives a ``Settings`` object and never calls ``os.environ`` directly, which keeps
configuration testable and makes a missing/invalid value fail fast at start-up instead of
half-way through a run.

AWS credentials are deliberately *not* fields on this class: boto3 resolves them itself
(env vars locally, an IAM role in AWS), so the application never holds or logs them.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

from sales_pipeline.utils.exceptions import ConfigurationError


class StorageBackend(StrEnum):
    LOCAL = "local"
    S3 = "s3"


class Settings(BaseSettings):
    """Application settings loaded from environment variables (and an optional .env file)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Runtime
    environment: str = "local"
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    log_format: str = Field(default="text", pattern="^(text|json)$")

    # PostgreSQL
    database_host: str = "localhost"
    database_port: int = Field(default=5432, ge=1, le=65535)
    database_name: str = "sales_dw"
    database_user: str = "sales_user"
    database_password: SecretStr = SecretStr("")

    # Storage
    storage_backend: StorageBackend = StorageBackend.LOCAL
    local_data_dir: Path = Path("data")
    source_data_dir: Path = Path("data/source")
    # When set, source CSVs are read from the storage backend under this prefix
    # (e.g. "landing" -> s3://bucket/landing/orders.csv) instead of SOURCE_DATA_DIR.
    source_prefix: str = ""

    # AWS
    aws_region: str | None = None
    s3_bucket: str | None = None
    s3_prefix: str = ""

    # Simulated / real payments API
    api_base_url: str = "http://localhost:8000"
    api_token: SecretStr | None = None
    api_page_size: int = Field(default=100, ge=1, le=1000)
    api_timeout_seconds: float = Field(default=10.0, gt=0)
    api_max_retries: int = Field(default=3, ge=0, le=10)
    api_backoff_seconds: float = Field(default=1.0, ge=0)

    # Data quality
    max_rejection_rate: float = Field(default=0.20, ge=0, le=1)

    @model_validator(mode="after")
    def _check_storage_settings(self) -> Settings:
        if self.storage_backend is StorageBackend.S3:
            missing = [name for name in ("s3_bucket", "aws_region") if not getattr(self, name)]
            if missing:
                raise ValueError(
                    f"STORAGE_BACKEND=s3 requires {', '.join(m.upper() for m in missing)}"
                )
        return self

    @property
    def database_url(self) -> URL:
        """SQLAlchemy URL object (the password is never rendered into log output)."""
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.database_user,
            password=self.database_password.get_secret_value(),
            host=self.database_host,
            port=self.database_port,
            database=self.database_name,
        )


def load_settings(**overrides: object) -> Settings:
    """Build settings, converting pydantic errors into a clear ConfigurationError."""
    try:
        return Settings(**overrides)  # type: ignore[arg-type]
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in err['loc']) or 'settings'}: {err['msg']}"
            for err in exc.errors()
        )
        raise ConfigurationError(f"Invalid configuration: {problems}") from exc


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton used by the CLI."""
    return load_settings()
