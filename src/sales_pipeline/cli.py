"""Command-line interface: ``sales-pipeline <command>``.

Commands
  init-db         create schemas/tables/indexes (idempotent)
  generate-data   write synthetic source files (batch 1 = initial, batch 2 = two weeks later)
  mock-api        serve the simulated payments REST API
  run             run the ETL pipeline once (incremental by default)
  quality         run warehouse data-quality checks on demand
  analytics       run the queries in sql/analytics and print the results
  demo            init-db + generate-data (if missing) + run + quality + analytics

Exit codes: 0 success, 1 pipeline/data failure, 2 configuration error.
"""

from __future__ import annotations

import argparse
import logging
import sys
import tempfile
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sales_pipeline.config.settings import Settings, get_settings
from sales_pipeline.ingestion.mock_api import create_server
from sales_pipeline.loading.db import (
    create_db_engine,
    initialize_schema,
    sql_dir,
    wait_for_database,
)
from sales_pipeline.pipeline import SalesPipeline, build_sources
from sales_pipeline.quality.checks import enforce, run_warehouse_checks
from sales_pipeline.sample_data import generate
from sales_pipeline.storage import ObjectStorage, get_storage
from sales_pipeline.utils.exceptions import ConfigurationError, PipelineError
from sales_pipeline.utils.logging import configure_logging

logger = logging.getLogger("sales_pipeline.cli")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sales-pipeline",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create database objects")

    gen = sub.add_parser("generate-data", help="write synthetic source data")
    gen.add_argument("--batch", type=int, choices=(1, 2), default=1)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--output", type=Path, default=None, help="default: SOURCE_DATA_DIR")
    gen.add_argument("--if-missing", action="store_true", help="skip if files already exist")
    gen.add_argument(
        "--to-storage",
        action="store_true",
        help="upload to SOURCE_PREFIX in the storage backend (e.g. seed an S3 landing area)",
    )

    api = sub.add_parser("mock-api", help="serve the simulated payments API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument(
        "--fail-first-n",
        type=int,
        default=0,
        help="return HTTP 503 for the first N requests (demonstrates retries)",
    )

    for name in ("run", "demo"):
        cmd = sub.add_parser(name, help=f"{name} the pipeline")
        cmd.add_argument(
            "--full-refresh",
            action="store_true",
            help="ignore watermarks and reprocess everything (safe: loads are upserts)",
        )
        cmd.add_argument(
            "--start-mock-api",
            action="store_true",
            help="serve the simulated API in-process for this run (local convenience)",
        )

    sub.add_parser("quality", help="run warehouse data-quality checks")
    an = sub.add_parser("analytics", help="run analytics queries")
    an.add_argument("--query", help="file name filter, e.g. 06 or top_10")
    return parser


SAMPLE_FILES = ("customers.csv", "products.csv", "orders.csv", "order_items.csv", "payments.json")


def _source_key(settings: Settings, name: str) -> str:
    return f"{settings.source_prefix.strip('/')}/{name}"


@contextmanager
def _maybe_mock_api(
    settings: Settings, storage: ObjectStorage, enabled: bool
) -> Iterator[str | None]:
    """Serve the simulated payments API in-process for one run. With SOURCE_PREFIX set, its
    data file is read from the landing area in storage (so it also works inside AWS)."""
    if not enabled:
        yield None
        return
    with tempfile.TemporaryDirectory() as tmp:
        data_file = settings.source_data_dir / "payments.json"
        if settings.source_prefix:
            data_file = Path(tmp) / "payments.json"
            data_file.write_bytes(storage.read_bytes(_source_key(settings, "payments.json")))
        server = create_server(data_file, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_address[1]}"
        logger.info("Simulated payments API running in-process at %s", url)
        try:
            yield url
        finally:
            server.shutdown()
            server.server_close()


def cmd_init_db(settings: Settings) -> None:
    engine = create_db_engine(settings)
    wait_for_database(engine)
    initialize_schema(engine)


def cmd_generate(settings: Settings, args: argparse.Namespace) -> None:
    if args.to_storage:
        _generate_to_storage(settings, args)
        return
    output = args.output or settings.source_data_dir
    if args.if_missing and (output / "orders.csv").exists():
        logger.info("Source data already present in %s; skipping generation", output)
        return
    generate(output, batch=args.batch, seed=args.seed)


def _generate_to_storage(settings: Settings, args: argparse.Namespace) -> None:
    if not settings.source_prefix:
        raise ConfigurationError("--to-storage needs SOURCE_PREFIX (e.g. SOURCE_PREFIX=landing)")
    storage = get_storage(settings)
    if args.if_missing and storage.exists(_source_key(settings, "orders.csv")):
        logger.info("Source data already present at %s", storage.uri(settings.source_prefix))
        return
    with tempfile.TemporaryDirectory() as tmp:
        generate(Path(tmp), batch=args.batch, seed=args.seed)
        for name in SAMPLE_FILES:
            location = storage.write_bytes(
                _source_key(settings, name), (Path(tmp) / name).read_bytes()
            )
            logger.info("Uploaded %s", location)


def cmd_mock_api(settings: Settings, args: argparse.Namespace) -> None:
    server = create_server(
        settings.source_data_dir / "payments.json", args.host, args.port, args.fail_first_n
    )
    logger.info("Simulated payments API listening on http://%s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down mock API")
    finally:
        server.server_close()


def cmd_run(settings: Settings, args: argparse.Namespace) -> None:
    engine = create_db_engine(settings)
    wait_for_database(engine)
    storage = get_storage(settings)
    with _maybe_mock_api(settings, storage, args.start_mock_api) as api_url:
        sources = build_sources(settings, storage, api_base_url=api_url)
        SalesPipeline(settings, engine, storage, sources).run(full_refresh=args.full_refresh)


def cmd_quality(settings: Settings) -> None:
    engine = create_db_engine(settings)
    wait_for_database(engine)
    with engine.connect() as conn:
        enforce(run_warehouse_checks(conn))
    logger.info("All critical data-quality checks passed")


def cmd_analytics(settings: Settings, query_filter: str | None) -> None:
    engine = create_db_engine(settings)
    wait_for_database(engine)
    files = sorted((sql_dir() / "analytics").glob("*.sql"))
    if query_filter:
        files = [f for f in files if query_filter in f.stem]
    with (
        pd.option_context("display.width", 160, "display.max_columns", 20),
        engine.connect() as conn,
    ):
        for path in files:
            df = pd.read_sql(text(path.read_text(encoding="utf-8")), conn)
            print(f"\n=== {path.stem} ({len(df)} rows) ===")
            print(df.head(15).to_string(index=False))


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        settings = get_settings()
    except ConfigurationError as exc:
        configure_logging()
        logger.error("%s", exc)
        return 2
    configure_logging(settings.log_level, settings.log_format)

    try:
        match args.command:
            case "init-db":
                cmd_init_db(settings)
            case "generate-data":
                cmd_generate(settings, args)
            case "mock-api":
                cmd_mock_api(settings, args)
            case "run":
                cmd_run(settings, args)
            case "quality":
                cmd_quality(settings)
            case "analytics":
                cmd_analytics(settings, args.query)
            case "demo":
                cmd_init_db(settings)
                seed_args = argparse.Namespace(
                    output=None,
                    if_missing=True,
                    batch=1,
                    seed=42,
                    to_storage=bool(settings.source_prefix),
                )
                cmd_generate(settings, seed_args)
                cmd_run(settings, args)
                cmd_quality(settings)
                cmd_analytics(settings, None)
    except ConfigurationError as exc:
        logger.error("Configuration error: %s", exc)
        return 2
    except PipelineError as exc:
        logger.error("%s: %s", type(exc).__name__, exc)
        return 1
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", None) or exc).splitlines()[0]
        hint = " - run `sales-pipeline init-db` first?" if "does not exist" in detail else ""
        logger.error("Database error: %s%s", detail, hint)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
