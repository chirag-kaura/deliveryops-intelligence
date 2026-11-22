"""Unit tests for typed application configuration."""

import pytest

from deliveryops.config import AppSettings


def test_default_settings() -> None:
    settings = AppSettings(_env_file=None)
    assert settings.app_env == "local"
    assert settings.storage_backend == "local"
    assert settings.postgres_port == 5433
    assert settings.duckdb_threads == 2
    assert settings.postgres_dsn == (
        "postgresql://deliveryops:deliveryops_local_dev@localhost:5433/deliveryops"
    )


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PORT", "5439")
    monkeypatch.setenv("DUCKDB_THREADS", "4")
    settings = AppSettings(_env_file=None)
    assert settings.postgres_port == 5439
    assert settings.duckdb_threads == 4
