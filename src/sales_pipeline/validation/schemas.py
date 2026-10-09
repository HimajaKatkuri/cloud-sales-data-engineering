"""Data contracts for every entity the pipeline ingests.

These specs are the single place that says what a valid record looks like. The validator,
the incremental filter and the loader all read them, so adding a column or a rule is a
one-line change here rather than an edit scattered across modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class DType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: DType
    required: bool = True  # required = must be present AND non-null
    pattern: str | None = None  # regex the (stripped) value must fully match
    allowed_values: frozenset[str] | None = None  # compared case-insensitively
    min_value: float | None = None  # inclusive lower bound for numeric columns


@dataclass(frozen=True)
class EntitySpec:
    name: str
    primary_key: str
    columns: tuple[ColumnSpec, ...]
    foreign_keys: dict[str, str] = field(default_factory=dict)  # column -> parent entity
    watermark_column: str = "updated_at"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> ColumnSpec:
        return next(c for c in self.columns if c.name == name)


ORDER_STATUSES = frozenset({"pending", "completed", "shipped", "cancelled", "refunded"})
# Only these statuses count as recognised revenue (documented in README "Revenue rules").
REVENUE_STATUSES = frozenset({"completed", "shipped"})
PAYMENT_METHODS = frozenset({"credit_card", "debit_card", "paypal", "bank_transfer", "gift_card"})
PAYMENT_STATUSES = frozenset({"captured", "failed", "refunded"})

_EMAIL = r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}"

CUSTOMERS = EntitySpec(
    name="customers",
    primary_key="customer_id",
    columns=(
        ColumnSpec("customer_id", DType.STRING, pattern=r"C\d{5}"),
        ColumnSpec("first_name", DType.STRING),
        ColumnSpec("last_name", DType.STRING),
        ColumnSpec("email", DType.STRING, pattern=_EMAIL),
        ColumnSpec("country", DType.STRING, pattern=r"[A-Za-z]{2}"),
        ColumnSpec("city", DType.STRING, required=False),
        ColumnSpec("signup_date", DType.DATE),
        ColumnSpec("updated_at", DType.TIMESTAMP),
    ),
)

PRODUCTS = EntitySpec(
    name="products",
    primary_key="product_id",
    columns=(
        ColumnSpec("product_id", DType.STRING, pattern=r"P\d{4}"),
        ColumnSpec("product_name", DType.STRING),
        ColumnSpec("category", DType.STRING),
        ColumnSpec("unit_price", DType.DECIMAL, min_value=0),
        ColumnSpec("is_active", DType.BOOLEAN),
        ColumnSpec("updated_at", DType.TIMESTAMP),
    ),
)

ORDERS = EntitySpec(
    name="orders",
    primary_key="order_id",
    columns=(
        ColumnSpec("order_id", DType.STRING, pattern=r"O\d{6}"),
        ColumnSpec("customer_id", DType.STRING),
        ColumnSpec("order_date", DType.TIMESTAMP),
        ColumnSpec("status", DType.STRING, allowed_values=ORDER_STATUSES),
        ColumnSpec("currency", DType.STRING, allowed_values=frozenset({"usd"})),
        ColumnSpec("updated_at", DType.TIMESTAMP),
    ),
    foreign_keys={"customer_id": "customers"},
)

ORDER_ITEMS = EntitySpec(
    name="order_items",
    primary_key="order_item_id",
    columns=(
        ColumnSpec("order_item_id", DType.STRING, pattern=r"OI\d{7}"),
        ColumnSpec("order_id", DType.STRING),
        ColumnSpec("product_id", DType.STRING),
        ColumnSpec("quantity", DType.INTEGER, min_value=1),
        ColumnSpec("unit_price", DType.DECIMAL, min_value=0),
        ColumnSpec("discount_amount", DType.DECIMAL, min_value=0),
        ColumnSpec("updated_at", DType.TIMESTAMP),
    ),
    foreign_keys={"order_id": "orders", "product_id": "products"},
)

PAYMENTS = EntitySpec(
    name="payments",
    primary_key="payment_id",
    columns=(
        ColumnSpec("payment_id", DType.STRING, pattern=r"PAY\d{7}"),
        ColumnSpec("order_id", DType.STRING),
        ColumnSpec("payment_date", DType.TIMESTAMP),
        ColumnSpec("payment_method", DType.STRING, allowed_values=PAYMENT_METHODS),
        ColumnSpec("amount", DType.DECIMAL, min_value=0.01),
        ColumnSpec("status", DType.STRING, allowed_values=PAYMENT_STATUSES),
        ColumnSpec("updated_at", DType.TIMESTAMP),
    ),
    foreign_keys={"order_id": "orders"},
)

# Parents before children: validation and loading must respect this order so that
# foreign keys can be checked against records accepted earlier in the same batch.
ENTITIES: tuple[EntitySpec, ...] = (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS, PAYMENTS)
ENTITY_BY_NAME: dict[str, EntitySpec] = {spec.name: spec for spec in ENTITIES}
