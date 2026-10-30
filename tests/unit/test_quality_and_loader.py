"""Data-quality severity handling and loader SQL generation."""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from sales_pipeline.loading.loader import build_upsert_sql, dataframe_to_records
from sales_pipeline.quality.checks import CheckResult, Severity, check_feeds, enforce
from sales_pipeline.utils.exceptions import DataQualityError
from sales_pipeline.validation.validator import ValidationResult


def result(entity: str, valid: int, rejected: int) -> ValidationResult:
    return ValidationResult(
        entity,
        valid=pd.DataFrame({"x": range(valid)}),
        rejected=pd.DataFrame({"x": range(rejected), "rejection_reasons": ["r"] * rejected}),
        input_count=valid + rejected,
    )


def test_rejection_rate_above_threshold_is_critical():
    checks = check_feeds([result("orders", 70, 30)], 0.2, initial_load_entities=set())

    with pytest.raises(DataQualityError, match="rejection_rate_orders"):
        enforce(checks)


def test_rejection_rate_within_threshold_passes():
    enforce(check_feeds([result("orders", 95, 5)], 0.2, initial_load_entities=set()))


def test_empty_feed_fails_only_on_initial_load():
    empty = result("customers", 0, 0)

    enforce(check_feeds([empty], 0.2, initial_load_entities=set()))  # incremental: fine
    with pytest.raises(DataQualityError, match="row_count_customers_initial_load"):
        enforce(check_feeds([empty], 0.2, initial_load_entities={"customers"}))


def test_warnings_never_fail_the_run():
    enforce([CheckResult("w", Severity.WARNING, passed=False, observed=3, detail="meh")])


def test_upsert_sql_is_idempotent_and_never_overwrites_newer_data():
    sql = build_upsert_sql("customers")

    assert "ON CONFLICT (customer_id) DO UPDATE" in sql
    assert "WHERE core.customers.source_updated_at <= EXCLUDED.source_updated_at" in sql
    assert "customer_id = EXCLUDED.customer_id" not in sql  # the key itself isn't updated


def test_records_convert_pandas_missing_values_and_numpy_types():
    df = pd.DataFrame(
        {
            "a": pd.array([1, None], dtype="Int64"),
            "b": [np.float64(2.5), np.nan],
            "c": [pd.Timestamp("2025-01-01", tz="UTC"), pd.NaT],
        }
    )

    records = dataframe_to_records(df)

    assert records[0] == {"a": 1, "b": 2.5, "c": datetime(2025, 1, 1, tzinfo=UTC)}
    assert records[1] == {"a": None, "b": None, "c": None}
    assert type(records[0]["a"]) is int
