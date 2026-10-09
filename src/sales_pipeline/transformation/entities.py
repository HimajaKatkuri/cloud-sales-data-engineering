"""Per-entity transformations: validated source shape -> warehouse (core schema) shape.

Inputs are the typed frames produced by validation, so these functions only rename,
standardise and derive. Each output column list matches the core.* table definition in
sql/schema/002_core_tables.sql exactly.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from sales_pipeline.transformation.cleaning import clean_text
from sales_pipeline.transformation.metrics import add_line_revenue

CORE_COLUMNS: dict[str, list[str]] = {
    "customers": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country_code",
        "city",
        "signup_date",
        "source_updated_at",
        "batch_id",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "list_price",
        "is_active",
        "source_updated_at",
        "batch_id",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "currency",
        "source_updated_at",
        "batch_id",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "gross_amount",
        "net_amount",
        "source_updated_at",
        "batch_id",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "payment_date",
        "payment_method",
        "amount",
        "payment_status",
        "source_updated_at",
        "batch_id",
    ],
}


def _finish(df: pd.DataFrame, entity: str, batch_id: str) -> pd.DataFrame:
    out = df.rename(columns={"updated_at": "source_updated_at"}).assign(batch_id=batch_id)
    return out[CORE_COLUMNS[entity]].reset_index(drop=True)


def transform_customers(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.copy()
    for col in ("first_name", "last_name", "city"):
        out[col] = clean_text(out[col]).map(lambda v: v.title() if isinstance(v, str) else v)
    out["email"] = out["email"].str.lower()
    out["country_code"] = out["country"].str.upper()
    return _finish(out, "customers", batch_id)


def transform_products(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.copy()
    out["product_name"] = clean_text(out["product_name"])
    out["category"] = clean_text(out["category"]).str.title()
    out = out.rename(columns={"unit_price": "list_price"})
    return _finish(out, "products", batch_id)


def transform_orders(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.rename(columns={"status": "order_status"})
    out["currency"] = out["currency"].str.upper()
    return _finish(out, "orders", batch_id)


def transform_order_items(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    return _finish(add_line_revenue(df), "order_items", batch_id)


def transform_payments(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.rename(columns={"status": "payment_status"})
    return _finish(out, "payments", batch_id)


TRANSFORMERS: dict[str, Callable[[pd.DataFrame, str], pd.DataFrame]] = {
    "customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "payments": transform_payments,
}


def transform(entity: str, df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=CORE_COLUMNS[entity])
    return TRANSFORMERS[entity](df, batch_id)
