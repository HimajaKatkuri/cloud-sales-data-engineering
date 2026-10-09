"""Validation: every rule quarantines the right rows, with the right reasons, and never
drops a record silently."""

from __future__ import annotations

import pandas as pd
import pytest

from sales_pipeline.utils.exceptions import SchemaValidationError
from sales_pipeline.validation.schemas import CUSTOMERS, ORDER_ITEMS, ORDERS, PAYMENTS, PRODUCTS
from sales_pipeline.validation.validator import REJECTION_COLUMN, validate_entity
from tests.helpers import raw_frame

KNOWN = {
    "customers": {"C00001", "C00002"},
    "orders": {"O000001", "O000002", "O000003"},
    "products": {"P0001", "P0002"},
}


def reasons_for(result, key_col: str, key: str) -> set[str]:
    row = result.rejected.loc[result.rejected[key_col] == key, REJECTION_COLUMN]
    assert len(row) == 1, f"expected exactly one rejected row for {key}"
    return set(row.iloc[0].split("; "))


def test_valid_rows_pass_and_are_typed(customer_rows, as_of):
    result = validate_entity(raw_frame(customer_rows), CUSTOMERS, as_of=as_of)

    assert len(result.valid) == 2
    assert result.rejected.empty
    assert result.valid["updated_at"].dt.tz is not None
    assert result.valid.loc[0, "signup_date"].isoformat() == "2024-05-01"


def test_missing_required_column_fails_the_whole_feed(customer_rows, as_of):
    df = raw_frame(customer_rows).drop(columns=["email"])

    with pytest.raises(SchemaValidationError, match="email"):
        validate_entity(df, CUSTOMERS, as_of=as_of)


def test_messy_headers_are_standardised_before_checking(customer_rows, as_of):
    df = raw_frame(customer_rows).rename(
        columns={"customer_id": "Customer ID", "signup_date": "signupDate"}
    )

    result = validate_entity(df, CUSTOMERS, as_of=as_of)

    assert len(result.valid) == 2


@pytest.mark.parametrize(
    ("column", "bad_value", "expected_reason"),
    [
        ("email", "not-an-email", "invalid_format_email"),
        ("last_name", None, "null_last_name"),
        ("signup_date", "2025-02-30", "invalid_date_signup_date"),
        ("signup_date", "1999-12-31", "date_too_old_signup_date"),
        ("updated_at", "2031-01-01T00:00:00Z", "future_date_updated_at"),
        ("customer_id", "CUST-1", "invalid_format_customer_id"),
    ],
)
def test_customer_rules(customer_rows, as_of, column, bad_value, expected_reason):
    customer_rows[0][column] = bad_value

    result = validate_entity(raw_frame(customer_rows), CUSTOMERS, as_of=as_of)

    assert len(result.valid) == 1
    key = customer_rows[0]["customer_id"]
    assert expected_reason in reasons_for(result, "customer_id", key)


def test_rejected_rows_keep_original_values(customer_rows, as_of):
    customer_rows[0]["email"] = "  NOT-AN-EMAIL  "

    result = validate_entity(raw_frame(customer_rows), CUSTOMERS, as_of=as_of)

    # Values are trimmed for checking but the content is preserved for investigation.
    assert result.rejected.loc[0, "email"] == "NOT-AN-EMAIL"


def test_a_row_collects_every_reason_it_fails(item_rows, as_of):
    item_rows[0].update(quantity="-1", product_id="P9999")

    result = validate_entity(raw_frame(item_rows), ORDER_ITEMS, KNOWN, as_of=as_of)

    assert reasons_for(result, "order_item_id", "OI0000001") >= {
        "negative_quantity",
        "unknown_product_id",
    }


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("quantity", "-2", "negative_quantity"),
        ("quantity", "0", "below_minimum_quantity"),
        ("quantity", "1.5", "invalid_type_quantity"),
        ("unit_price", "abc", "invalid_type_unit_price"),
        ("unit_price", "-1", "negative_unit_price"),
        ("discount_amount", "500", "discount_exceeds_gross"),
        ("order_id", "O999999", "unknown_order_id"),
        ("product_id", "P9999", "unknown_product_id"),
    ],
)
def test_order_item_rules(item_rows, as_of, field, value, reason):
    item_rows[0][field] = value

    result = validate_entity(raw_frame(item_rows), ORDER_ITEMS, KNOWN, as_of=as_of)

    assert reason in reasons_for(result, "order_item_id", "OI0000001")
    assert len(result.valid) == len(item_rows) - 1


def test_unknown_customer_id_is_quarantined(order_rows, as_of):
    order_rows[1]["customer_id"] = "C99999"

    result = validate_entity(raw_frame(order_rows), ORDERS, KNOWN, as_of=as_of)

    assert reasons_for(result, "order_id", "O000002") == {"unknown_customer_id"}


def test_invalid_order_status_is_quarantined(order_rows, as_of):
    order_rows[0]["status"] = "lost"

    result = validate_entity(raw_frame(order_rows), ORDERS, KNOWN, as_of=as_of)

    assert reasons_for(result, "order_id", "O000001") == {"invalid_value_status"}


def test_status_is_case_insensitive_and_lowercased(order_rows, as_of):
    order_rows[0]["status"] = "COMPLETED"

    result = validate_entity(raw_frame(order_rows), ORDERS, KNOWN, as_of=as_of)

    assert result.rejected.empty
    assert result.valid.loc[0, "status"] == "completed"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("amount", "-10", "negative_amount"),
        ("amount", "0", "below_minimum_amount"),
        ("payment_method", "crypto", "invalid_value_payment_method"),
        ("status", "pending_review", "invalid_value_status"),
    ],
)
def test_invalid_payment_values(as_of, field, value, reason):
    payment = {
        "payment_id": "PAY0000001",
        "order_id": "O000001",
        "payment_date": "2025-03-01T09:05:00Z",
        "payment_method": "paypal",
        "amount": "25.50",
        "status": "captured",
        "updated_at": "2025-03-01T09:05:00Z",
    }
    payment[field] = value

    result = validate_entity(raw_frame([payment]), PAYMENTS, KNOWN, as_of=as_of)

    assert reasons_for(result, "payment_id", "PAY0000001") == {reason}


def test_invalid_boolean_is_quarantined(as_of):
    product = {
        "product_id": "P0001",
        "product_name": "Mouse",
        "category": "electronics",
        "unit_price": "24.99",
        "is_active": "maybe",
        "updated_at": "2025-01-01T00:00:00Z",
    }

    result = validate_entity(raw_frame([product]), PRODUCTS, as_of=as_of)

    assert reasons_for(result, "product_id", "P0001") == {"invalid_type_is_active"}


# ------------------------------------------------------------------ duplicates
def test_exact_duplicate_rows_keep_first_and_quarantine_the_rest(customer_rows, as_of):
    rows = [customer_rows[0], dict(customer_rows[0]), dict(customer_rows[0]), customer_rows[1]]

    result = validate_entity(raw_frame(rows), CUSTOMERS, as_of=as_of)

    assert len(result.valid) == 2
    assert result.rejected[REJECTION_COLUMN].tolist() == ["duplicate_record", "duplicate_record"]


def test_conflicting_versions_keep_the_latest_updated_at(customer_rows, as_of):
    newer = dict(customer_rows[0], city="Denver", updated_at="2025-02-01T00:00:00Z")
    rows = [newer, customer_rows[0]]  # older version appears later in the file

    result = validate_entity(raw_frame(rows), CUSTOMERS, as_of=as_of)

    kept = result.valid.loc[result.valid["customer_id"] == "C00001"]
    assert kept["city"].tolist() == ["Denver"]
    assert reasons_for(result, "customer_id", "C00001") == {"duplicate_primary_key"}


def test_invalid_newer_version_does_not_hide_valid_older_one(customer_rows, as_of):
    broken_newer = dict(customer_rows[0], email="broken", updated_at="2025-02-01T00:00:00Z")

    result = validate_entity(raw_frame([customer_rows[0], broken_newer]), CUSTOMERS, as_of=as_of)

    assert result.valid["customer_id"].tolist() == ["C00001"]
    assert reasons_for(result, "customer_id", "C00001") == {"invalid_format_email"}


# ------------------------------------------------------------------ invariants
def test_no_record_is_silently_dropped(item_rows, as_of):
    item_rows[0]["quantity"] = "-1"
    item_rows[1]["product_id"] = None
    item_rows.append(dict(item_rows[2]))

    result = validate_entity(raw_frame(item_rows), ORDER_ITEMS, KNOWN, as_of=as_of)

    assert len(result.valid) + len(result.rejected) == len(item_rows) == result.input_count
    assert result.rejected[REJECTION_COLUMN].str.len().gt(0).all()


def test_empty_batch_is_valid(as_of):
    empty = pd.DataFrame({c: pd.Series(dtype="string") for c in CUSTOMERS.column_names})

    result = validate_entity(empty, CUSTOMERS, as_of=as_of)

    assert result.input_count == 0
    assert result.rejection_rate == 0.0


def test_reason_counts_summarise_rejections(customer_rows, as_of):
    customer_rows[0]["email"] = "bad"
    customer_rows[1]["email"] = "bad"

    result = validate_entity(raw_frame(customer_rows), CUSTOMERS, as_of=as_of)

    assert result.reason_counts() == {"invalid_format_email": 2}
    assert result.rejection_rate == 1.0
