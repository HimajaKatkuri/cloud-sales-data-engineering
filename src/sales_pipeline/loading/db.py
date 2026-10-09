"""Database connectivity and SQL-file execution."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from sales_pipeline.config.settings import Settings
from sales_pipeline.utils.exceptions import ConfigurationError, LoadError

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[3] / "sql"
_COMMENT = re.compile(r"--[^\n]*")
_PERMANENT_CONNECTION_ERRORS = (
    "password authentication failed",
    "no password supplied",
    "does not exist",  # database or role
)


def sql_dir() -> Path:
    """Locate the sql/ directory (repo checkout, or /app/sql inside the container)."""
    for candidate in (SQL_DIR, Path.cwd() / "sql", Path("/app/sql")):
        if candidate.is_dir():
            return candidate
    raise LoadError("Could not locate the sql/ directory")


def create_db_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,  # transparently replace connections dropped by the server
        connect_args={"connect_timeout": 10, "application_name": "sales-pipeline"},
    )


def wait_for_database(engine: Engine, attempts: int = 10, delay_seconds: float = 2.0) -> None:
    """Retry the *initial* connection only (e.g. Postgres still starting in Docker).
    Failures inside a transaction are not retried: the transaction is rolled back and the
    run fails, because blindly replaying writes is how data gets duplicated."""
    for attempt in range(1, attempts + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except OperationalError as exc:
            reason = str(exc.orig)
            if any(marker in reason for marker in _PERMANENT_CONNECTION_ERRORS):
                # Wrong password / missing database will not fix itself: fail fast.
                raise ConfigurationError(
                    f"Cannot connect to the database: {reason.strip()}"
                ) from exc
            if attempt == attempts:
                raise LoadError(
                    f"Database unreachable after {attempts} attempts: {exc.orig}"
                ) from exc
            logger.warning("Database not reachable (attempt %d/%d), retrying", attempt, attempts)
            time.sleep(delay_seconds)


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements. Convention for files in sql/: statements end with
    ';' at end of line, and no semicolons appear inside string literals."""
    without_comments = _COMMENT.sub("", sql)
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", without_comments) if s.strip()]


def run_sql_file(conn: Connection, path: Path, params: Mapping[str, Any] | None = None) -> None:
    for statement in split_statements(path.read_text(encoding="utf-8")):
        stmt = text(statement)
        # Only bind parameters the statement actually uses.
        used = {k: v for k, v in (params or {}).items() if f":{k}" in statement}
        try:
            conn.execute(stmt, used)
        except SQLAlchemyError as exc:
            raise LoadError(f"SQL failed in {path.name}: {exc}") from exc


def initialize_schema(engine: Engine) -> None:
    """Create schemas, tables and indexes (idempotent)."""
    files = sorted((sql_dir() / "schema").glob("*.sql"))
    with engine.begin() as conn:
        for path in files:
            logger.info("Applying %s", path.name)
            run_sql_file(conn, path)
    logger.info("Database schema is up to date (%d files)", len(files))
