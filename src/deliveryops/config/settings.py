"""Typed application configuration backed by pydantic-settings."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Centralized environment settings for DeliveryOps Intelligence."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Core Environment
    app_env: Literal["local", "test", "prod"] = "local"
    log_level: str = "INFO"

    # Storage Backend
    storage_backend: Literal["local", "s3"] = "local"
    local_lake_dir: Path = Field(default=Path("./data/lake"))

    # PostgreSQL 16 Alpine
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "deliveryops"
    postgres_user: str = "deliveryops"
    postgres_password: str = "deliveryops_local_dev"

    # Redpanda Streaming
    redpanda_brokers: str = "localhost:19092"
    redpanda_topic_events: str = "deliveryops.events.raw"
    redpanda_topic_dlq: str = "deliveryops.events.dlq"

    # MinIO Object Storage
    minio_endpoint: str = "http://localhost:9000"
    minio_console_port: int = 9001
    minio_root_user: str = "minioadmin"
    minio_root_password: str = "minioadmin_local_dev"
    minio_bucket_bronze: str = "deliveryops-bronze"
    minio_bucket_silver: str = "deliveryops-silver"
    minio_bucket_gold: str = "deliveryops-gold"

    # DuckDB Analytical Engine Resource Caps
    duckdb_memory_limit: str = "1GB"
    duckdb_threads: int = Field(default=2, ge=1, le=8)

    # OpenTelemetry & Jaeger
    otel_service_name: str = "deliveryops-intelligence"
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    jaeger_ui_port: int = 16686

    # Serving Layer
    fastapi_host: str = "127.0.0.1"
    fastapi_port: int = 8000
    streamlit_port: int = 8501

    @property
    def postgres_dsn(self) -> str:
        """Return standard PostgreSQL connection URI."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def get_settings() -> AppSettings:
    """Load and return validated application settings."""
    return AppSettings()
