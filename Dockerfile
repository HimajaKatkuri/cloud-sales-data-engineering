# syntax=docker/dockerfile:1
# Multi-stage build: compile wheels in a builder, install them into a slim runtime that
# contains no build tools, no tests and no source checkout - only the installed package
# and the SQL files it executes.

ARG PYTHON_VERSION=3.12

# ---------------------------------------------------------------- builder
FROM python:${PYTHON_VERSION}-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Dependencies first: this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip wheel --wheel-dir /wheels -r requirements.txt

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --no-deps --wheel-dir /wheels .

# ---------------------------------------------------------------- runtime
FROM python:${PYTHON_VERSION}-slim AS runtime

ARG APP_UID=1000
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LOCAL_DATA_DIR=/app/data \
    SOURCE_DATA_DIR=/app/data/source \
    LOG_FORMAT=json

RUN useradd --create-home --uid "${APP_UID}" app

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links=/wheels cloud-sales-pipeline \
    && rm -rf /wheels

COPY sql ./sql
RUN mkdir -p data/source data/raw data/processed data/rejected \
    && chown -R app:app /app

# Never run as root.
USER app

ENTRYPOINT ["sales-pipeline"]
CMD ["run"]
