# Cloud Sales Data Engineering Pipeline

[![CI](https://github.com/HimajaKatkuri/cloud-sales-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/HimajaKatkuri/cloud-sales-data-engineering/actions/workflows/ci.yml)

An end-to-end batch ETL pipeline in Python. It ingests customer, product, order and
order-line exports (CSV) plus payments from a paginated REST API, archives every delivery
unchanged in a data lake (local disk or AWS S3), validates each record and quarantines bad
ones with reasons, transforms the rest, loads them **incrementally and idempotently** into
PostgreSQL, maintains a star schema for analytics, and blocks bad loads with automated
data-quality checks. It ships with Docker, a CI pipeline in GitHub Actions, and a
deployment workflow for AWS (ECR + ECS Fargate + EventBridge Scheduler + RDS + S3).

All data is synthetic and generated deterministically - no real people or companies.

---

## Contents

1. [Project overview](#1-project-overview)
2. [Business problem](#2-business-problem)
3. [Architecture](#3-architecture)
4. [Technology stack](#4-technology-stack)
5. [Project structure](#5-project-structure)
6. [Data flow](#6-data-flow)
7. [Local setup](#7-local-setup)
8. [Environment variables](#8-environment-variables)
9. [Docker setup](#9-docker-setup)
10. [Running the pipeline](#10-running-the-pipeline)
11. [Running tests](#11-running-tests)
12. [Data-quality checks](#12-data-quality-checks)
13. [Database design](#13-database-design)
14. [AWS setup](#14-aws-setup)
15. [CI/CD](#15-cicd)
16. [Incremental processing](#16-incremental-processing)
17. [Monitoring and logging](#17-monitoring-and-logging)
18. [Error handling](#18-error-handling)
19. [Security considerations](#19-security-considerations)
20. [Future improvements](#20-future-improvements)

Also: [architecture/architecture.md](architecture/architecture.md) (design deep dive),
[docs/aws_deployment.md](docs/aws_deployment.md) (AWS runbook),
[INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) (how to present this project).

---

## 1. Project overview

| | |
|---|---|
| **Sources** | 4 CSV exports (customers, products, orders, order_items) + a paginated payments REST API |
| **Storage zones** | `landing/` (optional) -> `raw/` (immutable copy) -> `processed/` (Parquet) and `rejected/` (quarantine) |
| **Warehouse** | PostgreSQL: `core` (clean entities), `analytics` (star schema), `pipeline` (audit, watermarks, quarantine) |
| **Load strategy** | Watermark on `updated_at` + `INSERT ... ON CONFLICT DO UPDATE` (incremental and idempotent) |
| **Quality gates** | Row-level validation with quarantine; feed-level and warehouse-level checks; critical failures roll back the batch |
| **Runs on** | Laptop (venv or Docker Compose), GitHub Actions, AWS ECS Fargate |

What the bundled sample data demonstrates (from the actual runs; reproducible because the
generator is seeded):

| Run | Rows delivered | New since watermark | Valid / loaded | Quarantined |
|---|---|---|---|---|
| 1. Initial load (snapshot as of 2025-06-30) | 5,010 | 5,010 | 4,725 | 285 |
| 2. Rerun, sources unchanged | 3,870 (API returns 0) | 0 | 0 | 0 |
| 3. Snapshot two weeks later | 4,527 | 703 | 693 | 10 |

The quarantined rows come from defects deliberately injected into the sample data (bad
emails, impossible dates, unknown customer IDs, negative quantities, invalid payment
methods, duplicates) and from their knock-on effects (for example, order lines whose order
was rejected).

## 2. Business problem

A retailer's sales data lives in separate systems: an e-commerce/ERP platform exports
customers, products and orders as files every night, and a payment provider exposes
payments through an API. Analysts need one trustworthy place to answer questions like
*revenue by day, top customers, best-selling products, average order value* - without
double counting, without silently losing records, and without reloading the whole history
every night.

Requirements this project meets:

* **Trust:** a record either lands in the warehouse or in quarantine with a reason.
  Totals are reconciled automatically before a batch is committed.
* **Efficiency:** only new or changed records are processed each run.
* **Recoverability:** every delivery is archived unchanged, so any batch can be audited or
  replayed; a failed run leaves the warehouse exactly as it was.
* **Portability:** the same container runs on a laptop, in CI and in AWS.

## 3. Architecture

```
 SOURCES                      INGESTION (Python)             DATA LAKE (local disk or S3)
 +-------------------------+                                 +-----------------------------+
 | CSV exports             |   CsvFileSource /               | landing/   (optional input) |
 |  customers, products,   |-->StorageCsvSource ---------+-->| raw/<entity>/ingest_date=../|
 |  orders, order_items    |                             |   |     batch_id=../  (bytes    |
 +-------------------------+                             |   |     exactly as delivered)   |
 +-------------------------+   RestApiClient             |   +--------------+--------------+
 | Payments REST API       |   (pagination, timeouts,    |                  |
 |  (simulated locally)    |-->retries, backoff) --------+                  | read back from raw
 +-------------------------+                                                v
                                     +-----------------------------------------------------+
                                     | VALIDATE  schema, types, nulls, formats, ranges,    |
                                     |           dates, enums, duplicates, foreign keys    |
                                     |   invalid rows --> rejected/  +  pipeline.rejected_ |
                                     |   feed checks (empty first load, rejection rate)    |
                                     +--------------------------+--------------------------+
                                                                | valid rows
                                     +--------------------------v--------------------------+
                                     | TRANSFORM  clean names/values, derive line revenue  |
                                     |            batch revenue report --> processed/      |
                                     +--------------------------+--------------------------+
                                                                |
             ONE DATABASE TRANSACTION PER BATCH                 v
             +----------------------------------------------------------------------------+
             | PostgreSQL                                                                 |
             |  core.*        upsert (ON CONFLICT ... WHERE newer)                        |
             |  analytics.*   refresh only affected orders/days/customers/products (SQL)  |
             |  DQ checks     reconciliation, referential integrity, dates  -> fail = ROLLBACK
             |  pipeline.watermarks advanced  -> COMMIT                                   |
             +-------------------------------------+--------------------------------------+
                                                   |
                                                   v
                                   sql/analytics/*.sql  (revenue, AOV, CLV, ...)

 CI/CD:  GitHub --> GitHub Actions --+--> Lint (ruff, mypy)
                                     +--> Unit + integration tests (pytest, Postgres service)
                                     +--> Data-quality gate (real pipeline runs, 3 scenarios)
                                     +--> Docker build + smoke test
                                     +--> [main] Deploy: ECR push -> ECS task def -> migrate -> schedule
```

The detailed design rationale is in [architecture/architecture.md](architecture/architecture.md).

## 4. Technology stack

| Area | Choice | Why |
|---|---|---|
| Language | Python 3.12 | Data ecosystem; type hints + mypy for safety |
| Data processing | pandas 3, pyarrow | In-memory transforms at this scale; Parquet for the processed zone |
| Database access | SQLAlchemy 2 + psycopg 3 | Parameterised SQL, connection pooling, transactions |
| Warehouse | PostgreSQL 16 | Constraints, upserts, window functions, JSONB; free and everywhere |
| Validation/config | pydantic 2, pydantic-settings | Typed configuration and API response contracts |
| HTTP | requests | Simple, well understood; retry policy implemented explicitly |
| Cloud storage | AWS S3 via boto3 | Durable, cheap lake storage; same code path as local disk |
| Testing | pytest, moto | Deterministic unit tests; real Postgres for integration; S3 mocked in-memory |
| Quality | ruff (lint + format), mypy | Fast, strict, enforced in CI |
| Containers | Docker, Docker Compose | Identical runtime on laptop, CI and AWS |
| CI/CD | GitHub Actions | Lives next to the code; OIDC to AWS (no stored keys) |
| AWS runtime | ECS Fargate, EventBridge Scheduler, RDS, ECR, Secrets Manager, CloudWatch | Serverless containers for a batch job - see [the comparison](docs/aws_deployment.md#why-ecs-fargate-scheduled-tasks-and-not-something-else) |

## 5. Project structure

```
cloud-sales-data-engineering/
├── src/sales_pipeline/            # installable package (src layout)
│   ├── cli.py                     # `sales-pipeline` command: init-db, run, quality, ...
│   ├── pipeline.py                # orchestration and transaction boundaries
│   ├── sample_data.py             # deterministic synthetic data generator
│   ├── config/settings.py         # the ONLY place environment variables are read
│   ├── ingestion/
│   │   ├── sources.py             # CsvFileSource, StorageCsvSource, ApiSource
│   │   ├── api_client.py          # pagination, timeouts, retries, backoff
│   │   ├── mock_api.py            # simulated payments REST API (stdlib HTTP server)
│   │   ├── raw_zone.py            # land bytes unchanged; read raw back as strings
│   │   └── incremental.py         # watermark selection
│   ├── validation/
│   │   ├── schemas.py             # data contracts per entity
│   │   └── validator.py           # row-level checks + quarantine
│   ├── transformation/
│   │   ├── cleaning.py            # column-name and text standardisation
│   │   ├── entities.py            # source shape -> core table shape
│   │   └── metrics.py             # gross/net revenue, totals, AOV, CLV, daily, product
│   ├── loading/
│   │   ├── db.py                  # engine, connection retry, SQL file runner
│   │   └── loader.py              # idempotent upserts, watermarks, run audit
│   ├── quality/checks.py          # feed + warehouse data-quality checks
│   ├── storage/                   # ObjectStorage interface: local.py, s3.py
│   └── utils/                     # exceptions, logging (text/JSON), stage timing
├── sql/
│   ├── schema/                    # 001-004: schemas, core, analytics, pipeline tables
│   ├── transformations/           # 00-05: incremental refresh of totals, dims, facts
│   └── analytics/                 # 01-10: business queries
├── tests/
│   ├── unit/                      # no database or network needed
│   └── integration/               # real PostgreSQL; S3 via moto
├── data/                          # source/ raw/ processed/ rejected/ (git-ignored contents)
├── scripts/                       # reset_local.py, run_data_quality_checks.sh
├── docker/docker-compose.s3.yml   # optional override: local stack -> real S3 bucket
├── deploy/aws/                    # ECS task definition, IAM policies, S3 policies, render.sh
├── docs/aws_deployment.md         # step-by-step AWS runbook
├── architecture/architecture.md   # design deep dive
├── .github/workflows/             # ci.yml, deploy.yml
├── Dockerfile  docker-compose.yml  Makefile
├── pyproject.toml  requirements.txt  requirements-dev.txt
├── .env.example  .gitignore  .dockerignore
├── README.md  INTERVIEW_GUIDE.md
```

Deviation from the suggested layout: the code lives in a single package
`src/sales_pipeline/` (the "src layout") instead of loose folders under `src/`. It installs
cleanly with `pip install .`, gives one import root, exposes a console command, and keeps
tests from accidentally importing uninstalled code. `quality/` and `storage/` were added as
their own packages because they are distinct responsibilities.

## 6. Data flow

1. **Extract.** Each source connector returns the bytes exactly as delivered. They are
   written to `raw/<entity>/ingest_date=<date>/batch_id=<batch>/` without modification. The
   payments API is called with `updated_since=<watermark>`, page by page.
2. **Read back from raw.** Validation reads the archived copy, never the source directly,
   and parses every value as text so nothing is silently coerced.
3. **Select the increment.** Rows with `updated_at` <= the entity's watermark are skipped.
4. **Validate** in dependency order (customers, products -> orders -> order_items,
   payments) so foreign keys can be checked against records accepted earlier in the batch
   and against the warehouse. Rejected rows keep their original values plus every reason
   they failed, and go to `rejected/` and `pipeline.rejected_records`.
5. **Feed checks.** If more than `MAX_REJECTION_RATE` (default 20%) of a feed is rejected,
   or a feed is empty on its first load, the run stops before touching the database.
6. **Transform** valid rows to the `core` table shapes and derive line revenue. Write them
   to `processed/` as Parquet, plus a batch revenue report.
7. **Load** in one transaction: upsert `core` tables parents-first, insert quarantined
   rows, refresh `analytics` for affected keys only, run warehouse checks, advance
   watermarks, commit. Any failure rolls the whole batch back.

**Revenue rules.** Line gross = `quantity x unit_price`; net = gross - `discount_amount`;
order total = sum of its lines' net. Only orders with status `completed` or `shipped`
count as recognised revenue; `pending`, `cancelled` and `refunded` do not. Revenue days are
UTC calendar days.

## 7. Local setup

Prerequisites: Python 3.12, Docker (for PostgreSQL), Git. `make` is optional.

```bash
git clone https://github.com/HimajaKatkuri/cloud-sales-data-engineering.git
cd cloud-sales-data-engineering

python3.12 -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pip install -e . --no-deps

cp .env.example .env                   # defaults work as-is for local Docker Postgres
docker compose up -d postgres          # just the database
```

Then initialise and run:

```bash
sales-pipeline init-db
sales-pipeline generate-data           # writes data/source/
sales-pipeline run --start-mock-api    # serves the simulated API in-process for this run
sales-pipeline quality
sales-pipeline analytics
```

or simply `make install`, `make db-up`, `make demo`.

## 8. Environment variables

All configuration is read in one place, `src/sales_pipeline/config/settings.py`, and
validated at start-up. Invalid configuration exits with code 2 and a one-line message.

| Variable | Default | Purpose |
|---|---|---|
| `ENVIRONMENT` | `local` | `local`, `ci` or `production` (reset script refuses to run in production) |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FORMAT` | `text` | `text` for humans, `json` for CloudWatch (the Docker image defaults to `json`) |
| `DATABASE_HOST` / `_PORT` / `_NAME` / `_USER` | `localhost` / `5432` / `sales_dw` / `sales_user` | PostgreSQL connection |
| `DATABASE_PASSWORD` | *(none)* | Secret. `.env` locally, Secrets Manager in AWS |
| `STORAGE_BACKEND` | `local` | `local` (under `LOCAL_DATA_DIR`) or `s3` |
| `LOCAL_DATA_DIR` | `data` | Root of the lake zones for the local backend |
| `SOURCE_DATA_DIR` | `data/source` | Where CSV exports are read from (local files) |
| `SOURCE_PREFIX` | *(empty)* | If set, CSV exports are read from this prefix in the storage backend, e.g. `landing` -> `s3://bucket/landing/` |
| `AWS_REGION`, `S3_BUCKET`, `S3_PREFIX` | | Required (region, bucket) when `STORAGE_BACKEND=s3` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | | **Not read by the app.** boto3 picks them up for local experiments; prefer `AWS_PROFILE` locally and IAM roles in AWS |
| `API_BASE_URL` | `http://localhost:8000` | Payments API; point at a real API to replace the simulator |
| `API_TOKEN` | | Optional bearer token for a real API |
| `API_PAGE_SIZE`, `API_TIMEOUT_SECONDS`, `API_MAX_RETRIES`, `API_BACKOFF_SECONDS` | `100`, `10`, `3`, `1.0` | API client behaviour |
| `MAX_REJECTION_RATE` | `0.20` | Fail the run if a feed's rejection rate exceeds this |

## 9. Docker setup

```bash
cp .env.example .env
docker compose up --build
```

This starts four services:

| Service | What it does |
|---|---|
| `postgres` | PostgreSQL 16, data kept in the `pgdata` volume, exposed on `localhost:5432` |
| `seed-data` | One-shot: writes sample source files to `./data/source` if they are missing |
| `mock-api` | The simulated payments API on `http://localhost:8000` (health: `/health`) |
| `pipeline` | One-shot: `init-db` -> `run` -> `quality` -> `analytics`, then exits 0 |

The image is multi-stage (wheels built in a builder stage; the runtime stage has no build
tools, tests or source checkout) and runs as a non-root user. On Linux, if your user ID is
not 1000, build with `APP_UID=$(id -u) docker compose build` so the container can write to
the `./data` bind mount.

Useful follow-ups:

```bash
docker compose run --rm pipeline run                         # incremental rerun -> 0 new rows
docker compose run --rm pipeline generate-data --batch 2     # "two weeks later" snapshot
docker compose run --rm pipeline run                         # loads only the 703 changed rows
docker compose run --rm pipeline analytics --query top_10
docker compose exec postgres psql -U sales_user -d sales_dw  # explore the warehouse
docker compose down          # stop (keeps data)  |  docker compose down -v  (also deletes the DB)
```

To use a real S3 bucket from the local stack: set `S3_BUCKET` in `.env`, configure an AWS
CLI profile, then `docker compose -f docker-compose.yml -f docker/docker-compose.s3.yml up`.

## 10. Running the pipeline

```bash
sales-pipeline run --start-mock-api              # incremental run
sales-pipeline run --start-mock-api --full-refresh   # ignore watermarks, reprocess all (safe)
sales-pipeline mock-api --port 8000              # or run the API separately...
sales-pipeline run                               # ...and call it via API_BASE_URL
sales-pipeline mock-api --fail-first-n 2         # simulate an outage to watch retries
```

Try the incremental scenario end to end:

```bash
sales-pipeline generate-data --batch 1 && sales-pipeline run --start-mock-api   # 5,010 rows
sales-pipeline run --start-mock-api                                             # 0 new rows
sales-pipeline generate-data --batch 2 && sales-pipeline run --start-mock-api   # 703 new/changed
```

Inspect results:

```sql
SELECT batch_id, status, finished_at - started_at AS duration,
       metrics -> 'entities' -> 'orders' AS orders
FROM pipeline.pipeline_runs ORDER BY started_at;

SELECT entity, rejection_reasons, count(*)
FROM pipeline.rejected_records GROUP BY 1, 2 ORDER BY 3 DESC;

SELECT * FROM pipeline.watermarks;
```

Start over locally with `python scripts/reset_local.py --yes` (refuses unless
`ENVIRONMENT` is `local` or `ci`).

## 11. Running tests

```bash
pytest                          # everything; integration tests auto-skip without Postgres
pytest tests/unit               # fast, no database or network
pytest tests/integration        # needs PostgreSQL (docker compose up -d postgres)
pytest --cov                    # with coverage
ruff check src tests scripts && ruff format --check src tests scripts && mypy
bash scripts/run_data_quality_checks.sh   # the CI data-quality gate, locally
```

The integration tests create and use a separate database (`sales_dw_test`, override with
`TEST_DATABASE_NAME`; the name must end in `_test` because tests drop schemas).

What is tested (137 tests):

* **Validation** - every rule (nulls, types, formats, dates, ranges, enums, unknown
  customer/product/order IDs, negative amounts, invalid payment values), exact and
  conflicting duplicates, and the invariant that no record is ever dropped.
* **Transformations and revenue** - hand-computed expected values for line revenue, order
  totals, AOV, daily/product/customer revenue, UTC day boundaries, cent rounding.
* **API client** - pagination, retries on timeouts/connection errors/429/5xx, *no* retry on
  4xx or bad JSON, exponential backoff, `Retry-After`, broken pagination detection.
* **Storage** - identical behaviour for local disk and S3 (moto), unsafe keys rejected,
  S3 errors surfaced as `StorageError`.
* **Integration (real PostgreSQL)** - initial load, idempotent rerun and full refresh,
  incremental batch, rollback when a critical check fails, the full S3 cloud path,
  fail-fast on a wrong password.

Tests are deterministic: fixed seeds and timestamps, injected `sleep` in retry tests, and
an in-memory S3.

## 12. Data-quality checks

| Stage | Check | Severity | On failure |
|---|---|---|---|
| Validation | Required columns present | Critical | Run stops (whole feed unusable) |
| Validation | Nulls, types, formats, dates, ranges, enums, duplicates, foreign keys | Row level | Row quarantined with reasons |
| Feed | First load of an entity is not empty | Critical | Run stops before the DB transaction |
| Feed | Rejection rate <= `MAX_REJECTION_RATE` | Critical | Run stops before the DB transaction |
| Warehouse | All expected `core` columns exist (schema) | Critical | Transaction rolled back |
| Warehouse | Orders exist; `fact_sales` rows = `order_items` rows | Critical | Rolled back |
| Warehouse | Fact rows reference existing orders; every customer is in `dim_customer` | Critical | Rolled back |
| Warehouse | Order totals = sum of lines; daily fact = line fact; sum of CLV = recognised revenue | Critical | Rolled back |
| Warehouse | No negative revenue; no orders dated in the future | Critical | Rolled back |
| Warehouse | Payment before order date; duplicate emails; missing city; orders without lines; revenue orders without a captured payment; payment != order total | Warning | Logged and stored in run metrics |

Run the warehouse checks on demand with `sales-pipeline quality` (exit code 1 if any
critical check fails). Warnings in the sample data are expected: they are the downstream
effects of the injected defects (for example, a captured payment no longer matches an order
whose lines were partly quarantined) - exactly what a data team would want surfaced.

## 13. Database design

**`core` schema** - one table per source entity, cleaned and typed: `customers`,
`products`, `orders`, `order_items`, `payments`. Natural primary keys, foreign keys
(`orders -> customers`, `order_items -> orders, products`, `payments -> orders`), `CHECK`
constraints that mirror the validation rules (quantities > 0, discounts <= gross, net =
gross - discount, allowed statuses and payment methods), `NUMERIC` for money,
`TIMESTAMPTZ` for timestamps, and indexes on every foreign key and on `batch_id`. Lineage
columns on every row: `source_updated_at`, `batch_id`, `loaded_at`.

**`analytics` schema** - a star schema:

| Table | Grain | Notes |
|---|---|---|
| `dim_customer` | one row per customer | surrogate `customer_key`; SCD Type 1; lifetime revenue, order count, first/last order date |
| `dim_product` | one row per product | surrogate `product_key`; SCD Type 1 |
| `fact_sales` | one row per order line | FKs to both dimensions; `is_revenue` flag; indexed by date, customer, product, order |
| `fact_daily_sales` | one row per UTC day | pre-aggregated recognised revenue, orders, units, unique customers, AOV |

**`pipeline` schema** - `pipeline_runs` (audit row per run with JSON metrics and error
message), `watermarks` (one row per entity), `rejected_records` (quarantined rows as JSONB
with reasons).

Constraints are deliberately duplicated in Python validation and in the database: the
validator quarantines bad rows gracefully, and the constraints are the last line of
defence if a bug ever lets one through.

## 14. AWS setup

Follow [docs/aws_deployment.md](docs/aws_deployment.md). Summary of what you create:

1. S3 bucket (private, versioned, TLS-only policy, lifecycle rules)
2. ECR repository (scan on push, immutable tags)
3. Two security groups (tasks; database accepting 5432 only from tasks)
4. RDS PostgreSQL (private, encrypted, backups; password managed in Secrets Manager)
5. CloudWatch log group `/ecs/sales-pipeline`
6. IAM roles: ECS execution role, ECS task role (S3 prefixes only), scheduler role,
   GitHub OIDC deploy role
7. ECS cluster + task definition (`deploy/aws/task-definition.json`)
8. EventBridge Scheduler schedule (daily)
9. GitHub repository secret/variables for `deploy.yml`

Authentication in AWS uses IAM roles end to end: the task role grants S3 access, the
execution role reads the database secret, and GitHub Actions assumes a role through OIDC.
No long-lived access keys exist anywhere.

## 15. CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | Steps | Fails when |
|---|---|---|
| Lint & type-check | ruff lint, ruff format check, mypy | any issue |
| Unit & integration tests | pytest against a PostgreSQL 16 service container, coverage + JUnit artifacts | any test fails, or Postgres is unreachable (`REQUIRE_INTEGRATION_TESTS=1` turns skips into failures) |
| Data-quality gate | `scripts/run_data_quality_checks.sh`: initial load + checks, unchanged rerun must process 0 rows, incremental batch + checks, all analytics queries | any critical check, a non-idempotent rerun, or a broken query |
| Docker build & smoke test | build image (layer cache), run `--help`, assert non-root user, validate compose file | build or smoke test fails |

`.github/workflows/deploy.yml` runs after CI succeeds on `main` (or manually). It assumes an
AWS role through OIDC, pushes the image to ECR tagged with the commit SHA, registers a new
task-definition revision, runs `init-db` as a one-off migration task, and only if that
exits 0 points the EventBridge schedule at the new revision. If the repository has not been
configured yet, the job logs a notice and exits successfully, so the workflow is safe to
keep in a fork. Required secret/variables are listed at the top of the file and in the
runbook.

## 16. Incremental processing

* **Watermark per entity** in `pipeline.watermarks`: the largest source `updated_at`
  processed so far.
* **Selection:** CSV exports are full snapshots, so the pipeline keeps only rows with
  `updated_at > watermark` after reading them. For the API, the watermark is pushed down as
  `updated_since`, so unchanged payments are never transferred at all.
* **Idempotent writes:** `INSERT ... ON CONFLICT (pk) DO UPDATE ... WHERE
  target.source_updated_at <= EXCLUDED.source_updated_at`. Reprocessing never duplicates,
  and an older version can never overwrite a newer one.
* **Targeted refresh:** the loader stamps each written row with the `batch_id`; the SQL in
  `sql/transformations/` uses that to recompute only the affected orders, days, customers
  and products (delete-and-insert per affected key).
* **Atomic advance:** watermarks move in the same transaction as the data. If anything
  fails, nothing moves, and the next run naturally retries the same records.
* **Rejected rows** also advance the watermark (otherwise they would be re-rejected every
  night). They are reprocessed when the source corrects them (new `updated_at`) or with
  `--full-refresh`.
* **Known limitation:** strict `>` assumes a source never writes a *new* row with a
  timestamp at or below one already processed (late-arriving data with old timestamps).
  The standard mitigation is a small look-back window (`>= watermark - N minutes`), which
  the idempotent upsert already makes safe; it is listed under future improvements.

## 17. Monitoring and logging

* Python `logging` with INFO/WARNING/ERROR; `LOG_FORMAT=json` emits one JSON object per
  line with structured fields (`batch_id`, `stage`, `entity`, counts, `duration_seconds`)
  that CloudWatch Logs Insights can filter and aggregate.
* Logged events: pipeline start/completion, rows extracted and selected per entity,
  validation results with reason counts, quarantine locations, rows transformed and
  loaded, every DQ check result, per-stage durations, errors.
* Run history is queryable: `pipeline.pipeline_runs` (status, duration, metrics JSON,
  error message) and `pipeline.rejected_records`.
* Exit codes: 0 success, 1 pipeline/data failure, 2 configuration error - what schedulers
  and CI use to detect failure.

## 18. Error handling

| Failure | Handling |
|---|---|
| API timeout, connection error, HTTP 429/5xx | Retried with exponential backoff (honours `Retry-After`); `ApiError` after the last attempt |
| API 4xx, invalid JSON, broken pagination envelope | Not retried (would fail identically); `ApiError` immediately |
| Missing source file | `ExtractionError`; the run stops before touching the database |
| Missing required column | `SchemaValidationError`; the run stops |
| Bad individual records | Quarantined with reasons; the run continues |
| Too many bad records | Feed check fails the run before loading |
| Database not ready yet | Initial connection retried (e.g. Postgres still starting) |
| Wrong password / missing database | `ConfigurationError` immediately - retrying cannot help |
| Error during load or a critical DQ check | Whole transaction rolled back; run marked `failed` with the error; watermarks unchanged |
| S3 throttling / transient 5xx | boto3 "standard" retry mode; permanent errors (AccessDenied, NoSuchBucket) raise `StorageError` |
| Invalid configuration | `ConfigurationError` at start-up, exit code 2 |

Exceptions are never swallowed: each is logged once with context and re-raised; the CLI
turns them into a clear message and a non-zero exit code.

## 19. Security considerations

* No credentials in code or images. `.env` is git-ignored and docker-ignored;
  `.env.example` contains placeholders only.
* AWS access uses IAM roles everywhere (ECS task role, GitHub OIDC). The task role can
  only read `landing/` and read/write `raw/`, `processed/`, `rejected/` in one bucket.
* The database password lives in Secrets Manager (managed by RDS) and is injected at
  container start; settings hold it as `SecretStr`, so it never appears in logs or reprs.
* RDS is not publicly accessible; only the tasks' security group can reach port 5432.
* S3: public access blocked, TLS-only bucket policy, server-side encryption on every
  write, versioning on.
* Container runs as a non-root user; CI asserts this.
* All SQL with values uses bound parameters. The only interpolated identifiers come from
  internal constants (table/column names), never from input.
* Storage keys are validated to prevent path traversal on the local backend.
* The CI database password is a throwaway value for an ephemeral service container, not a
  secret.

## 20. Future improvements

Each of these has a concrete trigger - they are not needed at the current scale:

* **Look-back window** on watermarks for late-arriving records (when a source is known to
  deliver late).
* **Alerting:** CloudWatch metric filter + alarm on ERROR logs or failed ECS tasks.
* **SCD Type 2** dimensions when analysts need history (e.g. revenue by the customer's
  country *at the time of the order*).
* **Orchestration** (Airflow/MWAA, Dagster, or Step Functions) when there are several
  dependent jobs, backfills and SLAs to manage.
* **Private subnets + VPC endpoints** instead of public IPs for tasks; TLS `verify-full` to
  RDS; a least-privilege application DB role instead of the master user.
* **Infrastructure as code** (Terraform or AWS CDK) replacing the CLI runbook.
* **Schema migrations tool** (Alembic) once DDL starts changing in non-additive ways.
* **Partitioning** `fact_sales` by month and moving analytics to a columnar warehouse
  (Redshift, Snowflake, BigQuery) or Parquet + Athena when data outgrows one Postgres.
* **Distributed processing** (Spark/Glue, or Polars/DuckDB first) when a batch no longer
  fits comfortably in one machine's memory.
* **Data contracts with producers** and a schema registry if sources change often.

---

## Suggested Git commit messages

If you rebuild the history yourself, a clean sequence of conventional commits:

```
chore: scaffold project structure, tooling and configuration
feat(config): typed settings with validation and SecretStr password
feat(storage): local and S3 object storage behind one interface
feat(ingestion): CSV sources, paginated REST client with retries, simulated payments API
feat(ingestion): immutable raw zone partitioned by ingest date and batch
feat(validation): entity contracts, row-level validation and quarantine
feat(transform): cleaning, entity transforms and revenue metrics
feat(db): core, analytics and pipeline schemas with constraints and indexes
feat(load): idempotent upserts, watermarks and run audit in one transaction
feat(sql): incremental refresh of order totals, dimensions and facts
feat(quality): feed and warehouse data-quality checks with severities
feat(cli): sales-pipeline command with init-db, run, quality, analytics, demo
test: unit tests for validation, transforms, API client, storage
test: integration tests against PostgreSQL including rollback and S3 path
build: multi-stage Dockerfile and docker compose stack
ci: lint, tests, data-quality gate and Docker build in GitHub Actions
cd: OIDC deployment to ECR, ECS task definition and EventBridge schedule
docs: README, architecture deep dive, AWS runbook and interview guide
```
