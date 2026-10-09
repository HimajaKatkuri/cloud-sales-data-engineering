"""Watermark selection and advancement."""

from __future__ import annotations

from datetime import UTC, datetime

from sales_pipeline.ingestion.incremental import next_watermark, select_incremental
from tests.helpers import raw_frame

ROWS = [
    {"id": "1", "updated_at": "2025-01-01T00:00:00Z"},
    {"id": "2", "updated_at": "2025-02-01T00:00:00Z"},
    {"id": "3", "updated_at": "2025-03-01T00:00:00Z"},
    {"id": "4", "updated_at": "garbage"},
]


def test_without_watermark_everything_is_selected():
    assert len(select_incremental(raw_frame(ROWS), None)) == 4


def test_only_rows_strictly_newer_than_watermark_are_selected():
    watermark = datetime(2025, 2, 1, tzinfo=UTC)

    selected = select_incremental(raw_frame(ROWS), watermark)

    # "2" equals the watermark (already processed) -> excluded.
    # "4" has an unparseable timestamp -> kept so validation can quarantine it.
    assert selected["id"].tolist() == ["3", "4"]


def test_offsets_are_compared_as_instants():
    df = raw_frame([{"id": "1", "updated_at": "2025-02-01T02:00:00+03:00"}])  # 23:00 UTC Jan 31

    assert select_incremental(df, datetime(2025, 1, 31, 23, 30, tzinfo=UTC)).empty


def test_next_watermark_is_max_seen():
    assert next_watermark(raw_frame(ROWS), None) == datetime(2025, 3, 1, tzinfo=UTC)


def test_next_watermark_never_moves_backwards():
    current = datetime(2025, 6, 1, tzinfo=UTC)

    assert next_watermark(raw_frame(ROWS), current) == current


def test_empty_batch_keeps_current_watermark():
    current = datetime(2025, 6, 1, tzinfo=UTC)

    assert next_watermark(raw_frame(ROWS).iloc[0:0], current) == current
