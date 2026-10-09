"""Cleaning and per-entity transformations."""

from __future__ import annotations

import pandas as pd
import pytest

from sales_pipeline.transformation.cleaning import (
    clean_text,
    standardize_column_names,
    to_snake_case,
)
from sales_pipeline.transformation.entities import CORE_COLUMNS, transform
from sales_pipeline.validation.schemas import CUSTOMERS, ORDER_ITEMS, ORDERS
from sales_pipeline.validation.validator import validate_entity
from tests.helpers import raw_frame


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Customer ID", "customer_id"),
        ("customerID", "customer_id"),
        ("signupDate", "signup_date"),
        ("  Unit-Price ", "unit_price"),
        ("updated_at", "updated_at"),
        ("Order  Date (UTC)", "order_date_utc"),
    ],
)
def test_to_snake_case(raw, expected):
    assert to_snake_case(raw) == expected


def test_standardize_column_names_rejects_collisions():
    df = pd.DataFrame(columns=["Order Date", "order_date"])

    with pytest.raises(ValueError, match="collide"):
        standardize_column_names(df)


def test_clean_text_trims_and_collapses_whitespace():
    result = clean_text(pd.Series(["  New   York ", None, "Leeds"], dtype=object))

    assert result.iloc[0] == "New York"
    assert pd.isna(result.iloc[1])  # still missing (not the string "None")
    assert result.iloc[2] == "Leeds"


def test_transform_customers_standardises_values(customer_rows, as_of):
    customer_rows[0].update(first_name="  avery  ", email="Avery.Stone1@Example.COM")
    valid = validate_entity(raw_frame(customer_rows), CUSTOMERS, as_of=as_of).valid

    out = transform("customers", valid, batch_id="b1")

    assert list(out.columns) == CORE_COLUMNS["customers"]
    assert out.loc[0, "first_name"] == "Avery"
    assert out.loc[0, "email"] == "avery.stone1@example.com"
    assert out["country_code"].tolist() == ["US", "GB"]
    assert (out["batch_id"] == "b1").all()


def test_transform_orders_renames_status_and_uppercases_currency(order_rows, as_of):
    order_rows[0]["currency"] = "usd"
    known = {"customers": {"C00001", "C00002"}}
    valid = validate_entity(raw_frame(order_rows), ORDERS, known, as_of=as_of).valid

    out = transform("orders", valid, batch_id="b1")

    assert list(out.columns) == CORE_COLUMNS["orders"]
    assert out.loc[0, "currency"] == "USD"
    assert out.loc[0, "order_status"] == "completed"


def test_transform_order_items_derives_line_revenue(item_rows, as_of):
    known = {"orders": {"O000001", "O000002", "O000003"}, "products": {"P0001", "P0002"}}
    valid = validate_entity(raw_frame(item_rows), ORDER_ITEMS, known, as_of=as_of).valid

    out = transform("order_items", valid, batch_id="b1")

    first = out.loc[out["order_item_id"] == "OI0000001"].iloc[0]
    assert first["gross_amount"] == pytest.approx(20.00)
    assert first["net_amount"] == pytest.approx(18.00)


def test_transform_empty_frame_returns_expected_columns():
    out = transform("payments", pd.DataFrame(), batch_id="b1")

    assert out.empty
    assert list(out.columns) == CORE_COLUMNS["payments"]
