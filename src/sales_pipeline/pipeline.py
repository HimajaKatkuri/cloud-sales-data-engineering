"""Pipeline orchestration: extract -> raw zone -> validate -> transform -> load -> check.

Transaction boundaries are the key design decision here:
  * extract / quarantine / processed files are written before the DB transaction
    (they are immutable, batch-partitioned artefacts - harmless if the run later fails);
  * everything in PostgreSQL for one batch - core upserts, rejected rows, analytics
    refresh, post-load quality checks and the watermark update - happens in ONE
    transaction. A failure anywhere rolls all of it back, and because the watermark only
    moves on commit, the next run simply picks the same records up again.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import Connection, Engine

from sales_pipeline.config.settings import Settings
from sales_pipeline.ingestion.api_client import RestApiClient
from sales_pipeline.ingestion.incremental import next_watermark, select_incremental
from sales_pipeline.ingestion.raw_zone import land_raw, read_raw
from sales_pipeline.ingestion.sources import (
    ApiSource,
    CsvFileSource,
    SourceConnector,
    StorageCsvSource,
)
from sales_pipeline.loading import loader
from sales_pipeline.loading.db import run_sql_file, sql_dir
from sales_pipeline.quality.checks import CheckResult, check_feeds, enforce, run_warehouse_checks
from sales_pipeline.storage.base import ObjectStorage
from sales_pipeline.transformation import metrics
from sales_pipeline.transformation.cleaning import standardize_column_names
from sales_pipeline.transformation.entities import transform
from sales_pipeline.utils.logging import log_duration
from sales_pipeline.validation.schemas import ENTITIES, EntitySpec
from sales_pipeline.validation.validator import ValidationResult, validate_entity

logger = logging.getLogger(__name__)

PAYMENTS_ENDPOINT = "/api/v1/payments"


@dataclass
class EntityStats:
    extracted: int = 0  # rows in the source delivery
    incremental: int = 0  # rows newer than the watermark
    valid: int = 0
    rejected: int = 0
    loaded: int = 0


@dataclass
class RunSummary:
    batch_id: str
    status: str = "running"
    full_refresh: bool = False
    duration_seconds: float = 0.0
    entities: dict[str, EntityStats] = field(default_factory=dict)
    quality: list[CheckResult] = field(default_factory=list)
    batch_metrics: dict[str, Any] = field(default_factory=dict)
    watermarks: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "full_refresh": self.full_refresh,
            "duration_seconds": self.duration_seconds,
            "entities": {k: vars(v) for k, v in self.entities.items()},
            "quality": [r.as_dict() for r in self.quality],
            "batch_metrics": self.batch_metrics,
            "watermarks": self.watermarks,
        }


def new_batch_id(now: datetime) -> str:
    """Sortable and unique: 20250715T020000Z_1a2b3c4d."""
    return f"{now:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"


CSV_ENTITIES = ("customers", "products", "orders", "order_items")


def build_sources(
    settings: Settings, storage: ObjectStorage, api_base_url: str | None = None
) -> list[SourceConnector]:
    """Default source wiring.

    * CSV exports come from SOURCE_DATA_DIR on disk, or - when SOURCE_PREFIX is set - from
      that landing prefix in the storage backend (S3 in AWS).
    * Payments come from the REST API at API_BASE_URL; point it at a real API to replace
      the simulated one.
    """
    client = RestApiClient(
        base_url=api_base_url or settings.api_base_url,
        timeout_seconds=settings.api_timeout_seconds,
        max_retries=settings.api_max_retries,
        backoff_seconds=settings.api_backoff_seconds,
        token=settings.api_token.get_secret_value() if settings.api_token else None,
    )
    sources: list[SourceConnector] = []
    for entity in CSV_ENTITIES:
        if settings.source_prefix:
            key = f"{settings.source_prefix.strip('/')}/{entity}.csv"
            sources.append(StorageCsvSource(entity, storage, key))
        else:
            sources.append(CsvFileSource(entity, settings.source_data_dir / f"{entity}.csv"))
    sources.append(ApiSource("payments", client, PAYMENTS_ENDPOINT, settings.api_page_size))
    return sources


class SalesPipeline:
    def __init__(
        self,
        settings: Settings,
        engine: Engine,
        storage: ObjectStorage,
        sources: list[SourceConnector],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.engine = engine
        self.storage = storage
        self.sources = {s.entity: s for s in sources}
        self.clock = clock
        missing = [spec.name for spec in ENTITIES if spec.name not in self.sources]
        if missing:
            raise ValueError(f"No source configured for: {missing}")

    # ------------------------------------------------------------------ entry point
    def run(self, full_refresh: bool = False) -> RunSummary:
        started = time.perf_counter()
        now = self.clock()
        summary = RunSummary(batch_id=new_batch_id(now), full_refresh=full_refresh)
        summary.entities = {spec.name: EntityStats() for spec in ENTITIES}
        log_ctx = {"batch_id": summary.batch_id}
        logger.info(
            "Pipeline started (batch %s, full_refresh=%s, storage=%s)",
            summary.batch_id,
            full_refresh,
            self.settings.storage_backend.value,
            extra=log_ctx,
        )
        with self.engine.begin() as conn:
            loader.record_run_start(conn, summary.batch_id, full_refresh)

        try:
            self._execute(summary, now, full_refresh)
            summary.status = "succeeded"
        except Exception as exc:
            summary.status = "failed"
            summary.duration_seconds = round(time.perf_counter() - started, 3)
            logger.error("Pipeline FAILED (batch %s): %s", summary.batch_id, exc, extra=log_ctx)
            self._record_end(summary, error=f"{type(exc).__name__}: {exc}")
            raise

        summary.duration_seconds = round(time.perf_counter() - started, 3)
        self._record_end(summary)
        totals = {
            k: sum(getattr(s, k) for s in summary.entities.values())
            for k in ("extracted", "incremental", "valid", "rejected", "loaded")
        }
        logger.info(
            "Pipeline completed in %.2fs (batch %s): %s",
            summary.duration_seconds,
            summary.batch_id,
            totals,
            extra={**log_ctx, **totals, "duration_seconds": summary.duration_seconds},
        )
        return summary

    # ------------------------------------------------------------------ stages
    def _execute(self, summary: RunSummary, now: datetime, full_refresh: bool) -> None:
        batch_id = summary.batch_id
        with self.engine.connect() as conn:
            watermarks = {} if full_refresh else loader.get_watermarks(conn)

        with log_duration(logger, "extract", batch_id=batch_id):
            frames = self._extract(batch_id, now, watermarks, summary)

        with log_duration(logger, "validate", batch_id=batch_id), self.engine.connect() as conn:
            results = self._validate(conn, frames, now)
        for name, result in results.items():
            summary.entities[name].valid = len(result.valid)
            summary.entities[name].rejected = len(result.rejected)

        self._quarantine(batch_id, results)
        feed_checks = check_feeds(
            results.values(),
            self.settings.max_rejection_rate,
            initial_load_entities={e for e in results if e not in watermarks},
        )
        summary.quality.extend(feed_checks)
        enforce(feed_checks)

        with log_duration(logger, "transform", batch_id=batch_id):
            transformed = {name: transform(name, r.valid, batch_id) for name, r in results.items()}
            self._write_processed(batch_id, transformed, summary)

        with log_duration(logger, "load", batch_id=batch_id), self.engine.begin() as conn:
            for spec in ENTITIES:  # parents first, so foreign keys resolve
                summary.entities[spec.name].loaded = loader.upsert_entity(
                    conn, spec.name, transformed[spec.name]
                )
                loader.insert_rejected(conn, spec.name, results[spec.name].rejected, batch_id)

            with log_duration(logger, "refresh_analytics", batch_id=batch_id):
                for path in sorted((sql_dir() / "transformations").glob("*.sql")):
                    run_sql_file(conn, path, {"batch_id": batch_id})

            with log_duration(logger, "data_quality", batch_id=batch_id):
                warehouse_checks = run_warehouse_checks(conn)
                summary.quality.extend(warehouse_checks)
                enforce(warehouse_checks)  # raises -> whole transaction rolls back

            self._advance_watermarks(conn, frames, watermarks, batch_id, summary)

    def _extract(
        self,
        batch_id: str,
        now: datetime,
        watermarks: Mapping[str, datetime],
        summary: RunSummary,
    ) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {}
        for spec in ENTITIES:
            keys = land_raw(
                self.sources[spec.name],
                self.storage,
                batch_id,
                now.date(),
                watermarks.get(spec.name),
            )
            raw = _with_spec_columns(standardize_column_names(read_raw(self.storage, keys)), spec)
            new_rows = select_incremental(raw, watermarks.get(spec.name), spec.watermark_column)
            frames[spec.name] = new_rows
            summary.entities[spec.name].extracted = len(raw)
            summary.entities[spec.name].incremental = len(new_rows)
            logger.info(
                "Extracted %s: %d rows delivered, %d new since watermark %s",
                spec.name,
                len(raw),
                len(new_rows),
                watermarks.get(spec.name, "none (initial load)"),
                extra={"entity": spec.name, "extracted": len(raw), "incremental": len(new_rows)},
            )
        return frames

    def _validate(
        self, conn: Connection, frames: Mapping[str, pd.DataFrame], now: datetime
    ) -> dict[str, ValidationResult]:
        """Validate parents before children. A child's foreign key is valid if the parent
        was accepted earlier in this batch or already exists in the warehouse."""
        accepted: dict[str, set[str]] = {}
        results: dict[str, ValidationResult] = {}
        for spec in ENTITIES:
            df = frames[spec.name]
            known: dict[str, set[str]] = {}
            for column, parent in spec.foreign_keys.items():
                referenced = set(df[column].dropna().str.strip()) if column in df else set()
                in_batch = accepted.get(parent, set())
                in_warehouse = loader.existing_keys(conn, parent, referenced - in_batch)
                known[parent] = known.get(parent, set()) | in_batch | in_warehouse
            result = validate_entity(df, spec, known, as_of=now)
            accepted[spec.name] = set(result.valid[spec.primary_key])
            results[spec.name] = result
        return results

    def _quarantine(self, batch_id: str, results: Mapping[str, ValidationResult]) -> None:
        for name, result in results.items():
            if result.rejected.empty:
                continue
            key = f"rejected/{name}/batch_id={batch_id}/{name}_rejected.csv"
            location = self.storage.write_csv(key, result.rejected.assign(batch_id=batch_id))
            logger.warning(
                "Quarantined %d %s records -> %s",
                len(result.rejected),
                name,
                location,
                extra={"entity": name, "rejected": len(result.rejected)},
            )

    def _write_processed(
        self, batch_id: str, transformed: Mapping[str, pd.DataFrame], summary: RunSummary
    ) -> None:
        for name, df in transformed.items():
            if not df.empty:
                self.storage.write_parquet(
                    f"processed/{name}/batch_id={batch_id}/{name}.parquet", df
                )
            logger.info("Transformed %d %s rows", len(df), name, extra={"entity": name})

        # Batch-level revenue report: what this batch's new/changed orders contributed.
        orders, items = transformed["orders"], transformed["order_items"]
        items_in_batch_orders = items[items["order_id"].isin(orders["order_id"])]
        if orders.empty or items_in_batch_orders.empty:
            summary.batch_metrics = {"note": "no complete orders in this batch"}
            return
        reports = {
            "daily_revenue": metrics.daily_revenue(orders, items_in_batch_orders),
            "product_revenue": metrics.product_revenue(orders, items_in_batch_orders),
            "customer_revenue": metrics.customer_lifetime_revenue(orders, items_in_batch_orders),
        }
        for report_name, report in reports.items():
            self.storage.write_parquet(
                f"processed/metrics/batch_id={batch_id}/{report_name}.parquet", report
            )
        recognised = metrics.revenue_orders(orders, items_in_batch_orders)
        summary.batch_metrics = {
            "orders_in_batch": int(orders["order_id"].nunique()),
            "recognised_orders": len(recognised),
            "gross_revenue": round(float(recognised["gross_amount"].sum()), 2),
            "discounts": round(float(recognised["discount_amount"].sum()), 2),
            "net_revenue": round(float(recognised["net_amount"].sum()), 2),
            "average_order_value": metrics.average_order_value(orders, items_in_batch_orders),
        }
        logger.info("Batch revenue summary: %s", summary.batch_metrics)

    def _advance_watermarks(
        self,
        conn: Connection,
        frames: Mapping[str, pd.DataFrame],
        current: Mapping[str, datetime],
        batch_id: str,
        summary: RunSummary,
    ) -> None:
        # Computed over every examined row (valid AND rejected), so a rejected record is not
        # re-rejected on every run. It is reprocessed when the source fixes it (its
        # updated_at changes) or on a --full-refresh run.
        for spec in ENTITIES:
            new = next_watermark(frames[spec.name], current.get(spec.name), spec.watermark_column)
            if new is not None and new != current.get(spec.name):
                loader.set_watermark(conn, spec.name, new, batch_id)
            if new is not None:
                summary.watermarks[spec.name] = new.isoformat()

    def _record_end(self, summary: RunSummary, error: str | None = None) -> None:
        try:
            with self.engine.begin() as conn:
                loader.record_run_end(
                    conn, summary.batch_id, summary.status, summary.as_dict(), error
                )
        except Exception:  # never let audit bookkeeping hide the real error
            logger.exception("Could not record run status for batch %s", summary.batch_id)


def _with_spec_columns(df: pd.DataFrame, spec: EntitySpec) -> pd.DataFrame:
    """An empty API response has no columns at all; give it the expected (empty) columns so
    'no new data' is not mistaken for a schema violation."""
    if df.empty and len(df.columns) == 0:
        return pd.DataFrame({c: pd.Series(dtype="string") for c in spec.column_names})
    return df
