"""Idempotent loading into PostgreSQL.

``INSERT ... ON CONFLICT (pk) DO UPDATE ... WHERE target.source_updated_at <=
EXCLUDED.source_updated_at`` means:
  * new keys are inserted, changed keys are updated,
  * replaying the same batch does not create duplicates (idempotent),
  * an older version of a record can never overwrite a newer one (out-of-order safe).

Table and column names come from internal constants (CORE_COLUMNS), never from input.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

import pandas as pd
from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError

from sales_pipeline.transformation.entities import CORE_COLUMNS
from sales_pipeline.utils.exceptions import LoadError
from sales_pipeline.validation.schemas import ENTITY_BY_NAME
from sales_pipeline.validation.validator import REJECTION_COLUMN

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1_000


def _python_value(value: Any) -> Any:
    """Convert pandas/numpy scalars to plain Python values the DB driver understands."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(col): _python_value(val) for col, val in row.items()}
        for row in df.astype(object).to_dict(orient="records")
    ]


def build_upsert_sql(entity: str) -> str:
    columns = CORE_COLUMNS[entity]
    pk = ENTITY_BY_NAME[entity].primary_key
    table = f"core.{entity}"
    updates = ",\n    ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != pk)
    return (
        f"INSERT INTO {table} ({', '.join(columns)})\n"
        f"VALUES ({', '.join(':' + c for c in columns)})\n"
        f"ON CONFLICT ({pk}) DO UPDATE SET\n    {updates},\n    loaded_at = now()\n"
        f"WHERE {table}.source_updated_at <= EXCLUDED.source_updated_at"
    )


def _chunks(records: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(records), size):
        yield records[start : start + size]


def upsert_entity(conn: Connection, entity: str, df: pd.DataFrame) -> int:
    """Upsert a transformed frame into core.<entity>. Returns rows sent."""
    if df.empty:
        logger.info("No %s rows to load", entity)
        return 0
    statement = text(build_upsert_sql(entity))
    records = dataframe_to_records(df[CORE_COLUMNS[entity]])
    try:
        for chunk in _chunks(records, CHUNK_SIZE):
            conn.execute(statement, chunk)
    except SQLAlchemyError as exc:
        raise LoadError(f"Loading core.{entity} failed: {exc}") from exc
    logger.info("Loaded %d %s rows into core.%s", len(records), entity, entity)
    return len(records)


def insert_rejected(conn: Connection, entity: str, rejected: pd.DataFrame, batch_id: str) -> int:
    if rejected.empty:
        return 0
    payload_cols = [c for c in rejected.columns if c != REJECTION_COLUMN]
    rows = [
        {
            "batch_id": batch_id,
            "entity": entity,
            "record": json.dumps({c: _python_value(rec[c]) for c in payload_cols}, default=str),
            "reasons": rec[REJECTION_COLUMN],
        }
        for rec in rejected.astype(object).to_dict(orient="records")
    ]
    conn.execute(
        text(
            "INSERT INTO pipeline.rejected_records (batch_id, entity, record, rejection_reasons) "
            "VALUES (:batch_id, :entity, CAST(:record AS JSONB), :reasons)"
        ),
        rows,
    )
    return len(rows)


def existing_keys(conn: Connection, entity: str, candidates: Iterable[str]) -> set[str]:
    """Which of ``candidates`` already exist in core.<entity>? Only the IDs referenced by
    the current batch are looked up, so this stays cheap as the warehouse grows."""
    ids = sorted({c for c in candidates if isinstance(c, str)})
    if not ids:
        return set()
    pk = ENTITY_BY_NAME[entity].primary_key
    result = conn.execute(
        text(f"SELECT {pk} FROM core.{entity} WHERE {pk} = ANY(:ids)"), {"ids": ids}
    )
    return {row[0] for row in result}


# ---------------------------------------------------------------- watermarks
def get_watermarks(conn: Connection) -> dict[str, datetime]:
    rows = conn.execute(text("SELECT entity, high_watermark FROM pipeline.watermarks"))
    return {entity: ts for entity, ts in rows}


def set_watermark(conn: Connection, entity: str, value: datetime, batch_id: str) -> None:
    conn.execute(
        text(
            "INSERT INTO pipeline.watermarks (entity, high_watermark, last_batch_id, updated_at) "
            "VALUES (:entity, :value, :batch_id, now()) "
            "ON CONFLICT (entity) DO UPDATE SET high_watermark = EXCLUDED.high_watermark, "
            "last_batch_id = EXCLUDED.last_batch_id, updated_at = now()"
        ),
        {"entity": entity, "value": value, "batch_id": batch_id},
    )


# ---------------------------------------------------------------- run audit
def record_run_start(conn: Connection, batch_id: str, full_refresh: bool) -> None:
    conn.execute(
        text(
            "INSERT INTO pipeline.pipeline_runs (batch_id, status, full_refresh) "
            "VALUES (:batch_id, 'running', :full_refresh)"
        ),
        {"batch_id": batch_id, "full_refresh": full_refresh},
    )


def record_run_end(
    conn: Connection,
    batch_id: str,
    status: str,
    metrics: dict[str, Any],
    error: str | None = None,
) -> None:
    conn.execute(
        text(
            "UPDATE pipeline.pipeline_runs SET status = :status, finished_at = now(), "
            "metrics = CAST(:metrics AS JSONB), error_message = :error WHERE batch_id = :batch_id"
        ),
        {
            "batch_id": batch_id,
            "status": status,
            "metrics": json.dumps(metrics, default=_json_default),
            "error": error,
        },
    )


def _json_default(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)
