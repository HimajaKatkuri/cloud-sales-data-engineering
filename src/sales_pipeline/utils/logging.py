"""Logging setup: readable text locally, one-JSON-object-per-line in the cloud.

CloudWatch Logs Insights can query JSON fields directly (e.g. ``filter batch_id = "..."``),
which is why ``LOG_FORMAT=json`` is the recommended production setting.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {"message"}


class JsonFormatter(logging.Formatter):
    """Render records as JSON, including any ``extra={...}`` fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure the root logger once for the whole process."""
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # Third-party libraries are noisy at INFO; keep their warnings and errors.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


@contextmanager
def log_duration(logger: logging.Logger, stage: str, **context: object) -> Iterator[None]:
    """Log how long a stage took, and log + re-raise if it fails."""
    start = time.perf_counter()
    logger.info("Stage started: %s", stage, extra={"stage": stage, **context})
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - start
        logger.error(
            "Stage failed: %s after %.2fs",
            stage,
            elapsed,
            extra={"stage": stage, "duration_seconds": round(elapsed, 3), **context},
        )
        raise
    elapsed = time.perf_counter() - start
    logger.info(
        "Stage finished: %s in %.2fs",
        stage,
        elapsed,
        extra={"stage": stage, "duration_seconds": round(elapsed, 3), **context},
    )
