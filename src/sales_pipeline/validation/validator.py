"""Row-level validation with quarantine.

Two kinds of failure are treated differently on purpose:

* **Feed-level (critical)** - a required column is missing. Every row is unusable, so we
  raise ``SchemaValidationError`` and the run stops.
* **Row-level (non-critical)** - a null, a bad type, a negative amount, an unknown
  customer... Only that row is unusable. It goes to quarantine with *all* the reasons it
  failed, and the rest of the batch continues.

Invariant (enforced at the end): ``len(valid) + len(rejected) == len(input)``.
No record is ever silently dropped.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Set
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

from sales_pipeline.transformation.cleaning import standardize_column_names
from sales_pipeline.utils.exceptions import SchemaValidationError
from sales_pipeline.validation.schemas import ColumnSpec, DType, EntitySpec

logger = logging.getLogger(__name__)

MIN_VALID_DATE = pd.Timestamp("2000-01-01", tz="UTC")
FUTURE_TOLERANCE = timedelta(days=1)
_TRUE = {"true", "1", "yes", "y", "t"}
_FALSE = {"false", "0", "no", "n", "f"}

REJECTION_COLUMN = "rejection_reasons"

Flagger = Callable[[pd.Series, str], None]


@dataclass
class ValidationResult:
    entity: str
    valid: pd.DataFrame  # typed, ready for transformation
    rejected: pd.DataFrame  # original string values + rejection_reasons
    input_count: int

    @property
    def rejection_rate(self) -> float:
        return len(self.rejected) / self.input_count if self.input_count else 0.0

    def reason_counts(self) -> dict[str, int]:
        if self.rejected.empty:
            return {}
        reasons = self.rejected[REJECTION_COLUMN].str.split("; ").explode()
        return {str(k): int(v) for k, v in reasons.value_counts().sort_index().items()}


def check_required_columns(df: pd.DataFrame, spec: EntitySpec) -> None:
    missing = [c for c in spec.column_names if c not in df.columns]
    if missing:
        raise SchemaValidationError(
            f"{spec.name}: required column(s) missing from source: {', '.join(missing)}"
        )
    extra = [c for c in df.columns if c not in spec.column_names]
    if extra:
        logger.warning("%s: ignoring unexpected column(s): %s", spec.name, ", ".join(extra))


def validate_entity(
    raw: pd.DataFrame,
    spec: EntitySpec,
    known_keys: Mapping[str, Set[str]] | None = None,
    as_of: datetime | None = None,
) -> ValidationResult:
    """Validate one entity's batch.

    ``known_keys`` maps a parent entity name to the IDs that are valid parents: IDs already
    in the warehouse plus IDs accepted earlier in this batch.
    ``as_of`` is "now" for future-date checks; injectable so tests are deterministic.
    """
    input_count = len(raw)
    df = standardize_column_names(raw).reset_index(drop=True)
    check_required_columns(df, spec)
    df = df[spec.column_names].copy()
    for column in df.columns:
        df[column] = df[column].astype("string").str.strip().replace({"": pd.NA})

    now = pd.Timestamp(as_of or datetime.now(UTC))
    reasons = pd.Series("", index=df.index, dtype=object)
    typed = pd.DataFrame(index=df.index)

    def flag(mask: pd.Series, reason: str) -> None:
        mask = mask.fillna(False).astype(bool)
        reasons.loc[mask] = reasons.loc[mask] + reason + "; "

    # 1. Column-level checks: nulls, types, formats, ranges, allowed values, dates.
    for col in spec.columns:
        typed[col.name] = _check_column(df[col.name], col, now, flag)

    # 2. Cross-column business rules.
    if spec.name == "order_items":
        gross = typed["quantity"].astype(float) * typed["unit_price"]
        flag(typed["discount_amount"] > gross.round(2), "discount_exceeds_gross")

    # 3. Referential integrity against known parent keys.
    for column, parent in spec.foreign_keys.items():
        parents = (known_keys or {}).get(parent, set())
        flag(df[column].notna() & ~df[column].isin(list(parents)), f"unknown_{column}")

    # 4. Duplicates: exact copies first, then conflicting versions of the same key.
    flag(df.duplicated(keep="first"), "duplicate_record")
    flag(_superseded_versions(df, typed, spec, reasons), "duplicate_primary_key")

    is_valid = reasons == ""
    valid = typed.loc[is_valid].reset_index(drop=True)
    rejected = df.loc[~is_valid].copy()
    rejected[REJECTION_COLUMN] = reasons.loc[~is_valid].str.rstrip("; ")
    rejected = rejected.reset_index(drop=True)

    if len(valid) + len(rejected) != input_count:  # defensive: never lose a row
        raise AssertionError(f"{spec.name}: validation lost records")

    result = ValidationResult(spec.name, valid, rejected, input_count)
    log = logger.warning if len(rejected) else logger.info
    log(
        "Validated %s: %d in, %d valid, %d rejected %s",
        spec.name,
        input_count,
        len(valid),
        len(rejected),
        result.reason_counts() or "",
        extra={"entity": spec.name, "rejected": len(rejected), "valid": len(valid)},
    )
    return result


def _check_column(
    values: pd.Series, col: ColumnSpec, now: pd.Timestamp, flag: Flagger
) -> pd.Series:
    """Return the typed column, flagging every problem found along the way."""
    present = values.notna()
    if col.required:
        flag(~present, f"null_{col.name}")

    if col.dtype is DType.STRING:
        typed: pd.Series = values.astype(object).where(present, None)
        if col.pattern:
            regex = re.compile(col.pattern)
            matches = values.map(lambda v: bool(regex.fullmatch(v)) if isinstance(v, str) else True)
            flag(~matches.astype(bool), f"invalid_format_{col.name}")
        if col.allowed_values:
            allowed = values.str.lower().isin(list(col.allowed_values))
            flag(present & ~allowed.fillna(False).astype(bool), f"invalid_value_{col.name}")
            typed = values.str.lower().astype(object).where(present, None)
        return typed

    if col.dtype in (DType.INTEGER, DType.DECIMAL):
        numbers = pd.to_numeric(values.astype(object), errors="coerce").astype(float)
        bad_type = present & numbers.isna()
        if col.dtype is DType.INTEGER:
            bad_type |= present & numbers.notna() & (numbers % 1 != 0)
        flag(bad_type, f"invalid_type_{col.name}")
        if col.min_value is not None:
            below = numbers < col.min_value
            # Negative values are clearly wrong; zero quantity / zero payment are "below minimum".
            flag(below & (numbers < 0), f"negative_{col.name}")
            flag(below & (numbers >= 0), f"below_minimum_{col.name}")
        if col.dtype is DType.INTEGER:
            return numbers.where(~bad_type).round().astype("Int64")
        return numbers.where(~bad_type).round(2)

    if col.dtype is DType.BOOLEAN:
        lowered = values.str.lower()
        known = lowered.isin(list(_TRUE | _FALSE)).fillna(False).astype(bool)
        flag(present & ~known, f"invalid_type_{col.name}")
        return lowered.map(lambda v: v in _TRUE if isinstance(v, str) else None).astype(object)

    # DATE / TIMESTAMP
    if col.dtype is DType.DATE:
        parsed = pd.to_datetime(values.astype(object), format="%Y-%m-%d", errors="coerce", utc=True)
    else:
        parsed = pd.to_datetime(values.astype(object), format="ISO8601", errors="coerce", utc=True)
    flag(present & parsed.isna(), f"invalid_date_{col.name}")
    flag(parsed.notna() & (parsed < MIN_VALID_DATE), f"date_too_old_{col.name}")
    flag(parsed.notna() & (parsed > now + FUTURE_TOLERANCE), f"future_date_{col.name}")
    if col.dtype is DType.DATE:
        return parsed.dt.date.astype(object).where(parsed.notna(), None)
    return parsed


def _superseded_versions(
    df: pd.DataFrame, typed: pd.DataFrame, spec: EntitySpec, reasons: pd.Series
) -> pd.Series:
    """Among otherwise-valid rows sharing a primary key, keep the latest ``updated_at``
    (ties: the last row in the file) and mark the others as superseded duplicates.
    Rows that already failed another check do not compete, so a broken newer version
    cannot hide a good older one."""
    candidates = (reasons == "") & df[spec.primary_key].notna()
    order = (
        typed.loc[candidates, [spec.primary_key, spec.watermark_column]]
        .assign(_pos=lambda f: range(len(f)))
        .sort_values([spec.primary_key, spec.watermark_column, "_pos"])
    )
    keep_index = order.drop_duplicates(subset=spec.primary_key, keep="last").index
    superseded = candidates.copy()
    superseded.loc[keep_index] = False
    return superseded
