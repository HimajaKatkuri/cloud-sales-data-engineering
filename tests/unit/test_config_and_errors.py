"""Configuration validation and error-handling behaviour that doesn't need a database."""

from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine

from sales_pipeline.config.settings import StorageBackend, load_settings
from sales_pipeline.loading.db import split_statements, wait_for_database
from sales_pipeline.utils.exceptions import ConfigurationError, LoadError
from sales_pipeline.utils.logging import log_duration


def test_defaults_are_local_and_safe():
    settings = load_settings(_env_file=None)

    assert settings.storage_backend is StorageBackend.LOCAL
    assert settings.max_rejection_rate == 0.20


def test_environment_variables_are_read(monkeypatch):
    monkeypatch.setenv("DATABASE_HOST", "db.internal")
    monkeypatch.setenv("DATABASE_PORT", "6543")

    settings = load_settings(_env_file=None)

    assert settings.database_url.host == "db.internal"
    assert settings.database_url.port == 6543


def test_s3_backend_requires_bucket_and_region(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    with pytest.raises(ConfigurationError, match="S3_BUCKET"):
        load_settings(_env_file=None, storage_backend="s3")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("database_port", 70000),
        ("max_rejection_rate", 1.5),
        ("log_level", "LOUD"),
        ("storage_backend", "ftp"),
    ],
)
def test_invalid_values_raise_configuration_error(field, value):
    with pytest.raises(ConfigurationError, match=field):
        load_settings(_env_file=None, **{field: value})


def test_password_is_never_rendered(monkeypatch):
    monkeypatch.setenv("DATABASE_PASSWORD", "super-secret-value")

    settings = load_settings(_env_file=None)

    assert "super-secret-value" not in repr(settings)
    assert "super-secret-value" not in str(settings.database_url)  # URL masks it as ***


def test_unreachable_database_raises_load_error_after_retries():
    engine = create_engine(
        "postgresql+psycopg://nobody:nothing@127.0.0.1:1/none",
        connect_args={"connect_timeout": 1},
    )

    with pytest.raises(LoadError, match="unreachable after 2 attempts"):
        wait_for_database(engine, attempts=2, delay_seconds=0)


def test_split_statements_ignores_comments_and_blank_lines():
    sql = "-- header; with semicolon\nCREATE TABLE a (x int);\n\n-- c\nSELECT 1;\n"

    assert split_statements(sql) == ["CREATE TABLE a (x int)", "SELECT 1"]


def test_log_duration_logs_and_reraises(caplog):
    logger = logging.getLogger("test")

    with (
        caplog.at_level(logging.INFO),
        pytest.raises(RuntimeError),
        log_duration(logger, "boom"),
    ):
        raise RuntimeError("fail")

    assert "Stage failed: boom" in caplog.text
    assert any(r.levelno == logging.ERROR for r in caplog.records)
