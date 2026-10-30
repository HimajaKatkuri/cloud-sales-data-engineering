"""End-to-end runs against PostgreSQL with the deterministic sample data."""

from __future__ import annotations

import time
from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws
from pydantic import SecretStr
from sqlalchemy import Engine, text

from sales_pipeline.config.settings import Settings
from sales_pipeline.loading.db import create_db_engine, wait_for_database
from sales_pipeline.pipeline import SalesPipeline, build_sources
from sales_pipeline.quality.checks import CheckResult, Severity, run_warehouse_checks
from sales_pipeline.sample_data import generate
from sales_pipeline.storage.local import LocalStorage
from sales_pipeline.storage.s3 import S3Storage
from sales_pipeline.utils.exceptions import (
    ConfigurationError,
    DataQualityError,
    ExtractionError,
)

pytestmark = pytest.mark.integration


def make_pipeline(settings: Settings, engine: Engine, api_url: str, storage=None) -> SalesPipeline:
    storage = storage or LocalStorage(settings.local_data_dir)
    return SalesPipeline(settings, engine, storage, build_sources(settings, storage, api_url))


def scalar(engine: Engine, sql: str) -> object:
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


def counts(engine: Engine) -> dict[str, int]:
    tables = [
        "core.customers",
        "core.products",
        "core.orders",
        "core.order_items",
        "core.payments",
        "analytics.fact_sales",
        "analytics.dim_customer",
        "pipeline.rejected_records",
    ]
    return {t: int(scalar(engine, f"SELECT count(*) FROM {t}")) for t in tables}


def test_initial_load_populates_every_layer(clean_db, workspace, api_url):
    summary = make_pipeline(workspace, clean_db, api_url).run()

    assert summary.status == "succeeded"
    stats = summary.entities
    for entity, s in stats.items():
        assert s.valid + s.rejected == s.incremental, entity  # nothing silently dropped
        assert s.loaded == s.valid, entity

    c = counts(clean_db)
    assert c["core.orders"] == stats["orders"].valid
    assert c["analytics.fact_sales"] == c["core.order_items"]
    assert c["pipeline.rejected_records"] == sum(s.rejected for s in stats.values())
    assert scalar(clean_db, "SELECT status FROM pipeline.pipeline_runs") == "succeeded"
    assert scalar(clean_db, "SELECT count(*) FROM pipeline.watermarks") == 5

    # Every layer of the lake was written.
    lake = LocalStorage(workspace.local_data_dir)
    assert lake.list_keys("raw/payments")
    assert lake.list_keys("processed/orders")
    assert lake.list_keys("rejected/order_items")


def test_injected_defects_are_quarantined_with_reasons(clean_db, workspace, api_url):
    make_pipeline(workspace, clean_db, api_url).run()

    with clean_db.connect() as conn:
        reasons = dict(
            conn.execute(
                text(
                    "SELECT entity, string_agg(DISTINCT rejection_reasons, '|') "
                    "FROM pipeline.rejected_records GROUP BY entity"
                )
            ).all()
        )
    assert "invalid_format_email" in reasons["customers"]
    assert "unknown_customer_id" in reasons["orders"]
    assert "future_date_order_date" in reasons["orders"]
    assert "negative_quantity" in reasons["order_items"]
    assert "invalid_value_payment_method" in reasons["payments"]
    assert "negative_amount" in reasons["payments"]


def test_rerun_without_new_data_is_a_no_op(clean_db, workspace, api_url):
    pipeline = make_pipeline(workspace, clean_db, api_url)
    pipeline.run()
    before = counts(clean_db)
    daily_revenue = "SELECT sum(net_revenue) FROM analytics.fact_daily_sales"
    revenue_before = scalar(clean_db, daily_revenue)

    second = pipeline.run()

    assert sum(s.incremental for s in second.entities.values()) == 0
    assert counts(clean_db) == before
    assert (
        scalar(clean_db, "SELECT sum(net_revenue) FROM analytics.fact_daily_sales")
        == revenue_before
    )


def test_full_refresh_is_idempotent(clean_db, workspace, api_url):
    pipeline = make_pipeline(workspace, clean_db, api_url)
    pipeline.run()
    before = counts(clean_db)
    revenue = scalar(clean_db, "SELECT sum(net_amount) FROM analytics.fact_sales")

    pipeline.run(full_refresh=True)  # reprocesses everything: upserts must not duplicate

    after = counts(clean_db)
    assert {k: v for k, v in after.items() if k != "pipeline.rejected_records"} == {
        k: v for k, v in before.items() if k != "pipeline.rejected_records"
    }
    assert scalar(clean_db, "SELECT sum(net_amount) FROM analytics.fact_sales") == revenue


def test_incremental_batch_processes_only_changes(clean_db, workspace, api_url):
    pipeline = make_pipeline(workspace, clean_db, api_url)
    pipeline.run()
    orders_before = scalar(clean_db, "SELECT count(*) FROM core.orders")
    generate(workspace.source_data_dir, batch=2)  # same files, two weeks later

    summary = pipeline.run()

    orders = summary.entities["orders"]
    assert orders.extracted > orders.incremental > 0  # full snapshot, small increment
    assert scalar(clean_db, "SELECT count(*) FROM core.orders") == orders_before + 150 - 1
    # Pending orders that completed are now revenue, with their July update applied.
    assert (
        scalar(
            clean_db,
            "SELECT count(*) FROM core.orders WHERE order_status = 'completed' "
            "AND source_updated_at >= '2025-07-01'",
        )
        >= 25
    )
    with clean_db.connect() as conn:
        assert all(r.passed for r in run_warehouse_checks(conn) if r.severity is Severity.CRITICAL)


def test_critical_quality_failure_rolls_back_everything(clean_db, workspace, api_url, monkeypatch):
    def failing_checks(conn):
        return [CheckResult("forced_failure", Severity.CRITICAL, False, 1, "test")]

    monkeypatch.setattr("sales_pipeline.pipeline.run_warehouse_checks", failing_checks)

    with pytest.raises(DataQualityError, match="forced_failure"):
        make_pipeline(workspace, clean_db, api_url).run()

    c = counts(clean_db)
    assert c["core.orders"] == 0 and c["pipeline.rejected_records"] == 0
    assert scalar(clean_db, "SELECT count(*) FROM pipeline.watermarks") == 0
    run = text("SELECT status, error_message FROM pipeline.pipeline_runs")
    with clean_db.connect() as conn:
        status, error = conn.execute(run).one()
    assert status == "failed"
    assert "forced_failure" in error


@pytest.fixture
def s3_lake(monkeypatch) -> Iterator[S3Storage]:
    for var, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(var, value)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="it-sales-lake")
        yield S3Storage("it-sales-lake", "us-east-1")


def test_cloud_path_s3_landing_to_s3_lake(clean_db, workspace, api_url, s3_lake):
    """The AWS configuration: sources land in s3://bucket/landing/, every zone lives in S3."""
    for path in workspace.source_data_dir.iterdir():
        s3_lake.write_bytes(f"landing/{path.name}", path.read_bytes())
    cloud = workspace.model_copy(update={"source_prefix": "landing"})

    summary = make_pipeline(cloud, clean_db, api_url, storage=s3_lake).run()

    assert summary.status == "succeeded"
    [raw_customers] = s3_lake.list_keys("raw/customers/")
    assert s3_lake.list_keys("raw/payments/")
    assert s3_lake.list_keys("processed/orders/")
    assert s3_lake.list_keys("rejected/payments/")
    # The raw zone is a byte-identical copy of what landed.
    assert s3_lake.read_bytes(raw_customers) == s3_lake.read_bytes("landing/customers.csv")
    assert scalar(clean_db, "SELECT count(*) FROM core.orders") == summary.entities["orders"].valid


def test_missing_landing_file_fails_before_touching_the_warehouse(
    clean_db, workspace, api_url, s3_lake
):
    cloud = workspace.model_copy(update={"source_prefix": "landing"})  # nothing uploaded

    with pytest.raises(ExtractionError, match="customers not found at s3://"):
        make_pipeline(cloud, clean_db, api_url, storage=s3_lake).run()

    assert scalar(clean_db, "SELECT count(*) FROM core.customers") == 0
    assert scalar(clean_db, "SELECT status FROM pipeline.pipeline_runs") == "failed"


def test_wrong_database_password_fails_fast_as_configuration_error(base_settings):
    bad = base_settings.model_copy(update={"database_password": SecretStr("definitely-wrong")})
    started = time.perf_counter()

    with pytest.raises(ConfigurationError, match="password authentication failed"):
        wait_for_database(create_db_engine(bad), attempts=5, delay_seconds=2)

    assert time.perf_counter() - started < 2  # not retried: it can never succeed
