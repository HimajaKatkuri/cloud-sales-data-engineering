"""Integration fixtures: a real PostgreSQL database and the simulated API over HTTP.

The tests use a dedicated database (TEST_DATABASE_NAME, default ``sales_dw_test``) and drop
the pipeline schemas before each test. They are skipped automatically if PostgreSQL is not
reachable, so ``pytest`` still works on a laptop without Docker running - unless
REQUIRE_INTEGRATION_TESTS=1 (set in CI), in which case an unreachable database fails.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError

from sales_pipeline.config.settings import Settings, load_settings
from sales_pipeline.ingestion.mock_api import create_server
from sales_pipeline.loading.db import create_db_engine, initialize_schema
from sales_pipeline.sample_data import generate

TEST_DB = os.environ.get("TEST_DATABASE_NAME", "sales_dw_test")


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    if not TEST_DB.endswith("_test"):
        pytest.exit("TEST_DATABASE_NAME must end with _test (tests drop schemas)", returncode=2)
    return load_settings(database_name=TEST_DB, api_backoff_seconds=0.01)


def _create_test_database_if_missing(settings: Settings) -> None:
    """Connect to the always-present 'postgres' maintenance DB and create the test DB.
    Best effort: if it fails, the connection check below reports the real problem."""
    admin = create_db_engine(settings.model_copy(update={"database_name": "postgres"}))
    try:
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": TEST_DB}
            ).first()
            if not exists:
                conn.execute(
                    text(f"CREATE DATABASE \"{TEST_DB}\" ENCODING 'UTF8' TEMPLATE template0")
                )
    except Exception:  # noqa: S110 - deliberately best effort, see docstring
        pass
    finally:
        admin.dispose()


@pytest.fixture(scope="session")
def engine(base_settings: Settings) -> Iterator[Engine]:
    _create_test_database_if_missing(base_settings)
    eng = create_db_engine(base_settings)
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        message = f"PostgreSQL not reachable for integration tests: {exc.orig}"
        if os.environ.get("REQUIRE_INTEGRATION_TESTS") == "1":  # set in CI: never skip silently
            pytest.fail(message)
        pytest.skip(message)
    yield eng
    eng.dispose()


@pytest.fixture
def clean_db(engine: Engine) -> Engine:
    with engine.begin() as conn:
        for schema in ("analytics", "core", "pipeline"):
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    initialize_schema(engine)
    return engine


@pytest.fixture
def workspace(tmp_path: Path, base_settings: Settings) -> Settings:
    source = tmp_path / "source"
    generate(source, batch=1)
    return base_settings.model_copy(
        update={"local_data_dir": tmp_path / "lake", "source_data_dir": source}
    )


@pytest.fixture
def api_url(workspace: Settings) -> Iterator[str]:
    server = create_server(workspace.source_data_dir / "payments.json", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
