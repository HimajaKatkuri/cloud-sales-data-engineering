"""Revenue calculations, checked against hand-computed values.

Fixture data (see conftest):
  O000001 completed  lines: 2 x 10.00 - 2.00 disc = 18.00 ; 1 x 5.50 = 5.50  -> net 23.50
  O000002 shipped    lines: 3 x 10.00 = 30.00                                 -> net 30.00
  O000003 cancelled  lines: 4 x 5.50 = 22.00                                  -> not revenue
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from sales_pipeline.transformation import metrics


@pytest.fixture
def orders(order_rows) -> pd.DataFrame:
    df = pd.DataFrame(order_rows)
    df["order_date"] = pd.to_datetime(df["order_date"], utc=True)
    return df


@pytest.fixture
def items(item_rows) -> pd.DataFrame:
    df = pd.DataFrame(item_rows)
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["discount_amount"] = df["discount_amount"].astype(float)
    return df


def test_line_revenue(items):
    lines = metrics.add_line_revenue(items)

    assert lines["gross_amount"].tolist() == [20.0, 5.5, 30.0, 22.0]
    assert lines["net_amount"].tolist() == [18.0, 5.5, 30.0, 22.0]


def test_line_revenue_rounds_to_cents():
    items = pd.DataFrame({"quantity": [3], "unit_price": [0.1], "discount_amount": [0.0]})

    lines = metrics.add_line_revenue(items)

    assert lines.loc[0, "gross_amount"] == 0.3  # not 0.30000000000000004


def test_order_totals(items):
    totals = metrics.order_totals(items).set_index("order_id")

    assert totals.loc["O000001", "item_count"] == 2
    assert totals.loc["O000001", "gross_amount"] == pytest.approx(25.50)
    assert totals.loc["O000001", "discount_amount"] == pytest.approx(2.00)
    assert totals.loc["O000001", "net_amount"] == pytest.approx(23.50)


def test_cancelled_orders_are_not_revenue(orders, items):
    recognised = metrics.revenue_orders(orders, items)

    assert sorted(recognised["order_id"]) == ["O000001", "O000002"]


def test_average_order_value(orders, items):
    assert metrics.average_order_value(orders, items) == pytest.approx((23.50 + 30.00) / 2)


def test_average_order_value_with_no_revenue_orders_is_zero(orders, items):
    orders["status"] = "cancelled"

    assert metrics.average_order_value(orders, items) == 0.0


def test_daily_revenue(orders, items):
    daily = metrics.daily_revenue(orders, items)

    # Both recognised orders fall on 2025-03-01 (UTC); the cancelled one on 03-02 is excluded.
    assert daily["sales_date"].tolist() == [date(2025, 3, 1)]
    row = daily.iloc[0]
    assert row["order_count"] == 2
    assert row["units_sold"] == 6
    assert row["gross_revenue"] == pytest.approx(55.50)
    assert row["discount_amount"] == pytest.approx(2.00)
    assert row["net_revenue"] == pytest.approx(53.50)
    assert row["average_order_value"] == pytest.approx(26.75)


def test_daily_revenue_uses_utc_calendar_day():
    orders = pd.DataFrame(
        {
            "order_id": ["O1"],
            "customer_id": ["C1"],
            "status": ["completed"],
            # 23:30 in New York on 1 March is already 2 March in UTC
            "order_date": [pd.Timestamp("2025-03-01T23:30:00-05:00")],
        }
    )
    items = pd.DataFrame(
        {
            "order_item_id": ["I1"],
            "order_id": ["O1"],
            "product_id": ["P1"],
            "quantity": [1],
            "unit_price": [10.0],
            "discount_amount": [0.0],
        }
    )

    assert metrics.daily_revenue(orders, items)["sales_date"].tolist() == [date(2025, 3, 2)]


def test_product_revenue(orders, items):
    result = metrics.product_revenue(orders, items).set_index("product_id")

    # P0002's 4 units belong to the cancelled order and must not count.
    assert result.loc["P0001", "units_sold"] == 5
    assert result.loc["P0001", "net_revenue"] == pytest.approx(48.00)
    assert result.loc["P0002", "units_sold"] == 1
    assert result.loc["P0002", "net_revenue"] == pytest.approx(5.50)
    assert list(result.index) == ["P0001", "P0002"]  # highest revenue first


def test_customer_lifetime_revenue(orders, items):
    result = metrics.customer_lifetime_revenue(orders, items).set_index("customer_id")

    assert result.loc["C00002", "lifetime_revenue"] == pytest.approx(30.00)
    assert result.loc["C00001", "lifetime_revenue"] == pytest.approx(23.50)
    assert result.loc["C00001", "order_count"] == 1  # the cancelled order is excluded
