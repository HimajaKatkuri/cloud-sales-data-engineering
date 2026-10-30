"""Automated data-quality checks.

Severity decides what happens on failure:
  CRITICAL -> raise DataQualityError. During a run this happens *inside* the load
              transaction, so the whole batch is rolled back and watermarks don't move.
  WARNING  -> logged and recorded in the run metrics; the run continues.

Two families:
  * feed checks (pre-load, on DataFrames): schema, row counts, rejection rate
  * warehouse checks (post-load, SQL): reconciliation, referential integrity, dates,
    duplicates, nulls - things that row validation alone cannot see.
Each SQL check returns the number of violating rows; 0 means pass.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum

from sqlalchemy import Connection, text

from sales_pipeline.transformation.entities import CORE_COLUMNS
from sales_pipeline.utils.exceptions import DataQualityError
from sales_pipeline.validation.validator import ValidationResult

logger = logging.getLogger(__name__)

MONEY_TOLERANCE = 0.01


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    passed: bool
    observed: float
    detail: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SqlCheck:
    name: str
    severity: Severity
    description: str
    sql: str  # must return a single number: count of violations


# ---------------------------------------------------------------- feed (pre-load) checks
def check_feeds(
    results: Iterable[ValidationResult],
    max_rejection_rate: float,
    initial_load_entities: set[str],
) -> list[CheckResult]:
    """Row-count and rejection-rate checks on validated batches.

    * A feed that is empty on its very first load means the source is broken (CRITICAL).
      On later runs an empty increment is normal ("nothing changed").
    * A rejection rate above the threshold means something systemic (a changed export
      format, a broken upstream job) rather than a few bad rows (CRITICAL).
    """
    checks: list[CheckResult] = []
    for result in results:
        if result.entity in initial_load_entities:
            checks.append(
                CheckResult(
                    f"row_count_{result.entity}_initial_load",
                    Severity.CRITICAL,
                    result.input_count > 0,
                    result.input_count,
                    "first load of an entity must not be empty",
                )
            )
        checks.append(
            CheckResult(
                f"rejection_rate_{result.entity}",
                Severity.CRITICAL,
                result.rejection_rate <= max_rejection_rate,
                round(result.rejection_rate, 4),
                f"rejected {len(result.rejected)}/{result.input_count}; "
                f"threshold {max_rejection_rate:.0%}",
            )
        )
    return checks


# ---------------------------------------------------------------- warehouse (post-load) checks
_REVENUE = "('completed', 'shipped')"

WAREHOUSE_CHECKS: tuple[SqlCheck, ...] = (
    SqlCheck(
        "row_count_orders_not_empty",
        Severity.CRITICAL,
        "the warehouse must contain orders after a successful load",
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM core.orders) THEN 0 ELSE 1 END",
    ),
    SqlCheck(
        "row_count_fact_sales_matches_order_items",
        Severity.CRITICAL,
        "every order line must appear exactly once in fact_sales",
        "SELECT abs((SELECT count(*) FROM core.order_items)"
        " - (SELECT count(*) FROM analytics.fact_sales))",
    ),
    SqlCheck(
        "referential_fact_sales_orders",
        Severity.CRITICAL,
        "fact_sales rows must point to an existing order",
        "SELECT count(*) FROM analytics.fact_sales f "
        "LEFT JOIN core.orders o ON o.order_id = f.order_id WHERE o.order_id IS NULL",
    ),
    SqlCheck(
        "referential_customers_in_dim",
        Severity.CRITICAL,
        "every core customer must exist in dim_customer",
        "SELECT count(*) FROM core.customers c "
        "LEFT JOIN analytics.dim_customer d USING (customer_id) WHERE d.customer_id IS NULL",
    ),
    SqlCheck(
        "revenue_order_totals_match_lines",
        Severity.CRITICAL,
        "orders.net_amount must equal the sum of its lines",
        "SELECT count(*) FROM core.orders o LEFT JOIN ("
        " SELECT order_id, sum(net_amount) AS net FROM core.order_items GROUP BY order_id"
        f") i USING (order_id) WHERE abs(o.net_amount - coalesce(i.net, 0)) > {MONEY_TOLERANCE}",
    ),
    SqlCheck(
        "revenue_daily_matches_fact_sales",
        Severity.CRITICAL,
        "sum of fact_daily_sales must equal recognised revenue in fact_sales",
        "SELECT CASE WHEN abs("
        " (SELECT coalesce(sum(net_revenue), 0) FROM analytics.fact_daily_sales)"
        " - (SELECT coalesce(sum(net_amount), 0) FROM analytics.fact_sales WHERE is_revenue)"
        f") > {MONEY_TOLERANCE} THEN 1 ELSE 0 END",
    ),
    SqlCheck(
        "revenue_customer_ltv_matches_orders",
        Severity.CRITICAL,
        "sum of customer lifetime revenue must equal recognised order revenue",
        "SELECT CASE WHEN abs("
        " (SELECT coalesce(sum(lifetime_revenue), 0) FROM analytics.dim_customer)"
        " - (SELECT coalesce(sum(net_amount), 0) FROM core.orders"
        f" WHERE order_status IN {_REVENUE})"
        f") > {MONEY_TOLERANCE} THEN 1 ELSE 0 END",
    ),
    SqlCheck(
        "revenue_no_negative_lines",
        Severity.CRITICAL,
        "net revenue per line must not be negative",
        "SELECT count(*) FROM analytics.fact_sales WHERE net_amount < 0",
    ),
    SqlCheck(
        "date_no_future_orders",
        Severity.CRITICAL,
        "orders must not be dated more than a day in the future",
        "SELECT count(*) FROM core.orders WHERE order_date > now() + interval '1 day'",
    ),
    SqlCheck(
        "date_payment_not_before_order",
        Severity.WARNING,
        "a payment dated before its order suggests clock or data issues",
        "SELECT count(*) FROM core.payments p JOIN core.orders o USING (order_id) "
        "WHERE p.payment_date < o.order_date - interval '1 day'",
    ),
    SqlCheck(
        "duplicate_customer_emails",
        Severity.WARNING,
        "the same email used by several customer IDs (possible duplicate accounts)",
        "SELECT count(*) FROM (SELECT lower(email) FROM core.customers "
        "GROUP BY lower(email) HAVING count(*) > 1) d",
    ),
    SqlCheck(
        "null_customer_city",
        Severity.WARNING,
        "customers without a city (optional field, monitored for completeness)",
        "SELECT count(*) FROM core.customers WHERE city IS NULL",
    ),
    SqlCheck(
        "orders_without_items",
        Severity.WARNING,
        "orders with no lines yet (late-arriving lines or rejected lines)",
        "SELECT count(*) FROM core.orders WHERE item_count = 0",
    ),
    SqlCheck(
        "revenue_orders_without_captured_payment",
        Severity.WARNING,
        "completed/shipped orders with no captured payment",
        "SELECT count(*) FROM core.orders o WHERE o.order_status IN " + _REVENUE + " "
        "AND NOT EXISTS (SELECT 1 FROM core.payments p WHERE p.order_id = o.order_id "
        "AND p.payment_status = 'captured')",
    ),
    SqlCheck(
        "payment_amount_mismatch",
        Severity.WARNING,
        "captured payments that do not add up to the order net amount",
        "SELECT count(*) FROM core.orders o JOIN ("
        " SELECT order_id, sum(amount) AS paid FROM core.payments"
        " WHERE payment_status = 'captured' GROUP BY order_id"
        f") p USING (order_id) WHERE abs(p.paid - o.net_amount) > {MONEY_TOLERANCE}",
    ),
)


def check_schema(conn: Connection) -> CheckResult:
    """Every column the loader writes must exist in the database (catches migrations
    that were not applied)."""
    rows = conn.execute(
        text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'core'"
        )
    )
    present = {(t, c) for t, c in rows}
    missing = [
        f"core.{table}.{col}"
        for table, cols in CORE_COLUMNS.items()
        for col in cols
        if (table, col) not in present
    ]
    return CheckResult(
        "schema_core_columns_present",
        Severity.CRITICAL,
        not missing,
        len(missing),
        "missing: " + ", ".join(missing) if missing else "all expected columns present",
    )


def run_warehouse_checks(
    conn: Connection, checks: Sequence[SqlCheck] = WAREHOUSE_CHECKS
) -> list[CheckResult]:
    results = [check_schema(conn)]
    for check in checks:
        violations = float(conn.execute(text(check.sql)).scalar_one())
        results.append(
            CheckResult(check.name, check.severity, violations == 0, violations, check.description)
        )
    return results


def enforce(results: Sequence[CheckResult]) -> None:
    """Log every result; raise if any CRITICAL check failed."""
    for r in results:
        if r.passed:
            logger.info("DQ PASS  %-45s observed=%s", r.name, r.observed)
        elif r.severity is Severity.WARNING:
            logger.warning("DQ WARN  %-45s observed=%s (%s)", r.name, r.observed, r.detail)
        else:
            logger.error("DQ FAIL  %-45s observed=%s (%s)", r.name, r.observed, r.detail)
    failed = [r.name for r in results if not r.passed and r.severity is Severity.CRITICAL]
    if failed:
        raise DataQualityError(f"Critical data-quality checks failed: {', '.join(failed)}")
