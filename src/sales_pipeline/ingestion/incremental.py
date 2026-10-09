"""Watermark-based incremental selection.

Each entity has a high watermark = the largest ``updated_at`` successfully processed.
A run keeps only rows with ``updated_at > watermark``; after the load commits, the
watermark moves to the max ``updated_at`` seen in this batch. Rows whose ``updated_at``
cannot be parsed are kept so validation quarantines them instead of dropping them.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def parse_watermark_column(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values.astype(object), format="ISO8601", utc=True, errors="coerce")


def select_incremental(
    df: pd.DataFrame, watermark: datetime | None, column: str = "updated_at"
) -> pd.DataFrame:
    """Return rows newer than the watermark (all rows when there is no watermark yet)."""
    if watermark is None or df.empty or column not in df.columns:
        return df
    ts = parse_watermark_column(df[column])
    keep = ts.isna() | (ts > pd.Timestamp(watermark))
    return df.loc[keep].reset_index(drop=True)


def next_watermark(
    df: pd.DataFrame, current: datetime | None, column: str = "updated_at"
) -> datetime | None:
    """New watermark = max(current, max parseable updated_at in the batch)."""
    if df.empty or column not in df.columns:
        return current
    batch_max = parse_watermark_column(df[column]).max()
    if pd.isna(batch_max):
        return current
    candidate: datetime = batch_max.to_pydatetime()
    return candidate if current is None or candidate > current else current
