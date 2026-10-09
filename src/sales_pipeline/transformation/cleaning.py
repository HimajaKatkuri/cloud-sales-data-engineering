"""Generic, entity-agnostic cleaning helpers."""

from __future__ import annotations

import re

import pandas as pd

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def to_snake_case(name: str) -> str:
    """Normalise a header: 'Order Date' -> 'order_date', 'customerID' -> 'customer_id'."""
    name = _CAMEL_BOUNDARY.sub("_", name.strip())
    return _NON_ALNUM.sub("_", name).strip("_").lower()


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with snake_case column names; fail loudly if two columns collide."""
    renamed = [to_snake_case(str(c)) for c in df.columns]
    duplicates = sorted({c for c in renamed if renamed.count(c) > 1})
    if duplicates:
        raise ValueError(f"Column names collide after cleaning: {duplicates}")
    out = df.copy()
    out.columns = renamed
    return out


def clean_text(series: pd.Series) -> pd.Series:
    """Trim and collapse internal whitespace; keep missing values missing."""
    return series.map(lambda v: " ".join(v.split()) if isinstance(v, str) else v)
