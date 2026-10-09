"""Object-storage interface shared by the local-disk and S3 implementations.

The pipeline only ever talks to this interface using S3-style keys such as
``raw/customers/ingest_date=2025-07-01/batch_id=.../customers.csv``. Switching between a
laptop and AWS is therefore a configuration change (STORAGE_BACKEND), not a code change.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod

import pandas as pd

from sales_pipeline.utils.exceptions import StorageError


class ObjectStorage(ABC):
    """Minimal blob-store API: enough for a data lake's raw/processed/rejected zones."""

    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> str:
        """Store ``data`` under ``key`` and return a human-readable URI."""

    @abstractmethod
    def read_bytes(self, key: str) -> bytes:
        """Return the bytes stored under ``key``; raise StorageError if missing."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """True if an object exists at ``key``."""

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]:
        """Sorted list of keys that start with ``prefix``."""

    @abstractmethod
    def uri(self, key: str) -> str:
        """Location of ``key`` for logs, e.g. ``s3://bucket/raw/...``."""

    # Convenience helpers built on the primitives above -------------------------------
    def write_parquet(self, key: str, df: pd.DataFrame) -> str:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        return self.write_bytes(key, buffer.getvalue())

    def write_csv(self, key: str, df: pd.DataFrame) -> str:
        return self.write_bytes(key, df.to_csv(index=False).encode("utf-8"))

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self.read_bytes(key)))


def validate_key(key: str) -> str:
    """Reject keys that could escape the storage root (``..``) or are absolute."""
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise StorageError(f"Invalid storage key: {key!r}")
    return key
