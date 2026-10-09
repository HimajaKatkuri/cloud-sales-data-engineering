"""Landing data in the raw zone and reading it back.

Raw objects are written byte-for-byte, partitioned like a data lake:

    raw/<entity>/ingest_date=YYYY-MM-DD/batch_id=<batch_id>/<file>

Downstream stages read *from the raw zone*, not from the source. That makes every batch
replayable from storage and guarantees validation sees exactly what was archived.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import date, datetime

import pandas as pd

from sales_pipeline.ingestion.sources import SourceConnector
from sales_pipeline.storage.base import ObjectStorage
from sales_pipeline.utils.exceptions import ExtractionError

logger = logging.getLogger(__name__)


def raw_prefix(entity: str, ingest_date: date, batch_id: str) -> str:
    return f"raw/{entity}/ingest_date={ingest_date.isoformat()}/batch_id={batch_id}"


def land_raw(
    source: SourceConnector,
    storage: ObjectStorage,
    batch_id: str,
    ingest_date: date,
    since: datetime | None = None,
) -> list[str]:
    """Fetch from a source and persist the untouched bytes. Returns the storage keys."""
    objects = source.fetch(since)
    prefix = raw_prefix(source.entity, ingest_date, batch_id)
    keys: list[str] = []
    for obj in objects:
        key = f"{prefix}/{obj.name}"
        location = storage.write_bytes(key, obj.content)
        keys.append(key)
        logger.info("Landed raw object %s (%d bytes)", location, len(obj.content))
    return keys


def read_raw(storage: ObjectStorage, keys: list[str]) -> pd.DataFrame:
    """Parse raw objects into a DataFrame of *strings* (typing happens in validation)."""
    frames = [_parse(key, storage.read_bytes(key)) for key in keys]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _parse(key: str, content: bytes) -> pd.DataFrame:
    try:
        if key.endswith(".csv"):
            # dtype="string" + keep_default_na=False: nothing is guessed or coerced here.
            df = pd.read_csv(io.BytesIO(content), dtype="string", keep_default_na=False)
            return df.replace({"": pd.NA})
        if key.endswith(".json"):
            records = json.loads(content)["data"]
            return _records_to_strings(records)
    except (ValueError, KeyError, pd.errors.ParserError) as exc:
        raise ExtractionError(f"Could not parse raw object {key}: {exc}") from exc
    raise ExtractionError(f"Unsupported raw object format: {key}")


def _records_to_strings(records: list[dict[str, object]]) -> pd.DataFrame:
    """JSON gives typed values (numbers, null). Convert to strings so API data goes through
    exactly the same validation path as CSV data.

    Columns are built straight from the records rather than via ``DataFrame.from_records``:
    pandas would first infer a dtype and turn JSON null into NaN, and ``str(NaN)`` is the
    string "nan", which would then slip past the null check. A missing key is a null too."""
    columns = list(dict.fromkeys(key for record in records for key in record))
    return pd.DataFrame(
        {
            column: pd.array(
                [None if r.get(column) is None else str(r[column]) for r in records],
                dtype="string",
            )
            for column in columns
        }
    )
