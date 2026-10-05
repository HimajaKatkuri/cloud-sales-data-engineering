"""Test helpers shared across unit and integration tests."""

from __future__ import annotations

import pandas as pd


def raw_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a frame shaped like the raw zone produces: every value a string or <NA>.

    Built column-by-column from the dicts (not via pd.DataFrame(rows)) so that None stays
    None instead of becoming NaN -> "nan"."""
    columns = list(dict.fromkeys(k for row in rows for k in row))
    return pd.DataFrame(
        {
            c: pd.array([None if r.get(c) is None else str(r[c]) for r in rows], dtype="string")
            for c in columns
        }
    )
