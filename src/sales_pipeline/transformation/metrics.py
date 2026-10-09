"""Revenue calculations as small, pure, individually tested functions.

Definitions (also in the README):
  gross revenue  = quantity x unit_price                (per order line)
  discount       = discount_amount                      (per order line, absolute)
  net revenue    = gross - discount
  order total    = sum of net revenue over the order's lines
  recognised revenue counts only orders whose status is in REVENUE_STATUSES
  average order value (AOV) = recognised net revenue / number of recognised orders

Money is rounded to 2 decimals at each step and stored as NUMERIC in PostgreSQL.
"""

from __future__ import annotations

import pandas as pd

from sales_pipeline.validation.schemas import REVENUE_STATUSES


def add_line_revenue(items: pd.DataFrame) -> pd.DataFrame:
    """Add gross_amount and net_amount to order lines."""
    out = items.copy()
    out["gross_amount"] = (out["quantity"].astype(float) * out["unit_price"].astype(float)).round(2)
    out["net_amount"] = (out["gross_amount"] - out["discount_amount"].astype(float)).round(2)
    return out


def is_revenue_order(status: pd.Series) -> pd.Series:
    return status.str.lower().isin(list(REVENUE_STATUSES))


def order_totals(items: pd.DataFrame) -> pd.DataFrame:
    """One row per order: item_count, gross_amount, discount_amount, net_amount."""
    lines = items if "net_amount" in items.columns else add_line_revenue(items)
    totals = lines.groupby("order_id", as_index=False).agg(
        item_count=("order_item_id", "count"),
        units=("quantity", "sum"),
        gross_amount=("gross_amount", "sum"),
        discount_amount=("discount_amount", "sum"),
        net_amount=("net_amount", "sum"),
    )
    for col in ("gross_amount", "discount_amount", "net_amount"):
        totals[col] = totals[col].astype(float).round(2)
    totals["units"] = totals["units"].astype("int64")
    return totals


def revenue_orders(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Orders with their totals, restricted to recognised-revenue statuses."""
    status_col = "order_status" if "order_status" in orders.columns else "status"
    merged = orders.merge(order_totals(items), on="order_id", how="inner")
    return merged.loc[is_revenue_order(merged[status_col])].reset_index(drop=True)


def average_order_value(orders: pd.DataFrame, items: pd.DataFrame) -> float:
    recognised = revenue_orders(orders, items)
    if recognised.empty:
        return 0.0
    return round(float(recognised["net_amount"].sum()) / len(recognised), 2)


def daily_revenue(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Recognised revenue per UTC calendar day."""
    recognised = revenue_orders(orders, items)
    columns = [
        "sales_date",
        "order_count",
        "units_sold",
        "gross_revenue",
        "discount_amount",
        "net_revenue",
        "average_order_value",
    ]
    if recognised.empty:
        return pd.DataFrame(columns=columns)
    recognised["sales_date"] = pd.to_datetime(recognised["order_date"], utc=True).dt.date
    daily = recognised.groupby("sales_date", as_index=False).agg(
        order_count=("order_id", "nunique"),
        units_sold=("units", "sum"),
        gross_revenue=("gross_amount", "sum"),
        discount_amount=("discount_amount", "sum"),
        net_revenue=("net_amount", "sum"),
    )
    daily["average_order_value"] = daily["net_revenue"] / daily["order_count"]
    money = ["gross_revenue", "discount_amount", "net_revenue", "average_order_value"]
    daily[money] = daily[money].round(2)
    return daily[columns].sort_values("sales_date").reset_index(drop=True)


def product_revenue(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Units and recognised revenue per product, highest revenue first."""
    lines = items if "net_amount" in items.columns else add_line_revenue(items)
    status_col = "order_status" if "order_status" in orders.columns else "status"
    recognised_ids = orders.loc[is_revenue_order(orders[status_col]), "order_id"]
    lines = lines.loc[lines["order_id"].isin(recognised_ids)]
    result = lines.groupby("product_id", as_index=False).agg(
        units_sold=("quantity", "sum"),
        gross_revenue=("gross_amount", "sum"),
        net_revenue=("net_amount", "sum"),
    )
    result[["gross_revenue", "net_revenue"]] = result[["gross_revenue", "net_revenue"]].round(2)
    result["units_sold"] = result["units_sold"].astype("int64")
    return result.sort_values(["net_revenue", "product_id"], ascending=[False, True]).reset_index(
        drop=True
    )


def customer_lifetime_revenue(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """Recognised order count and net revenue per customer, highest first.

    In the pipeline this summarises one batch; the warehouse-wide lifetime value is
    maintained incrementally in SQL (analytics.dim_customer), because it needs history.
    """
    recognised = revenue_orders(orders, items)
    result = recognised.groupby("customer_id", as_index=False).agg(
        order_count=("order_id", "nunique"),
        lifetime_revenue=("net_amount", "sum"),
    )
    result["lifetime_revenue"] = result["lifetime_revenue"].round(2)
    return result.sort_values(
        ["lifetime_revenue", "customer_id"], ascending=[False, True]
    ).reset_index(drop=True)
