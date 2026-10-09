"""Source connectors.

A connector fetches *bytes exactly as the source delivered them*. It does no parsing or
cleaning, which keeps the raw zone a faithful, replayable copy of what arrived.
Adding a new source (SFTP drop, another API, a database export) means adding a class with
the same ``fetch`` method; nothing downstream changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from sales_pipeline.ingestion.api_client import RestApiClient
from sales_pipeline.storage.base import ObjectStorage
from sales_pipeline.utils.exceptions import ExtractionError, StorageError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawObject:
    name: str  # file name inside the raw zone partition
    content: bytes
    fmt: str  # "csv" or "json_pages"


class SourceConnector(Protocol):
    entity: str

    def fetch(self, since: datetime | None) -> list[RawObject]: ...


class CsvFileSource:
    """A CSV export dropped by an upstream system (e.g. a nightly ERP extract).

    The file is a full snapshot, so ``since`` is not used here: the incremental filter is
    applied after parsing (see ingestion/incremental.py). The raw copy keeps the whole file.
    """

    def __init__(self, entity: str, path: Path) -> None:
        self.entity = entity
        self.path = path

    def fetch(self, since: datetime | None) -> list[RawObject]:
        try:
            content = self.path.read_bytes()
        except FileNotFoundError as exc:
            raise ExtractionError(f"Source file for {self.entity} not found: {self.path}") from exc
        except OSError as exc:
            raise ExtractionError(f"Could not read {self.path}: {exc}") from exc
        logger.info("Read %s (%d bytes)", self.path, len(content))
        return [RawObject(name=f"{self.entity}.csv", content=content, fmt="csv")]


class StorageCsvSource:
    """The same CSV export, but dropped into a landing prefix of the object store
    (e.g. ``s3://bucket/landing/orders.csv``) - how upstream systems typically deliver
    files in the cloud. Selected by setting SOURCE_PREFIX."""

    def __init__(self, entity: str, storage: ObjectStorage, key: str) -> None:
        self.entity = entity
        self.storage = storage
        self.key = key

    def fetch(self, since: datetime | None) -> list[RawObject]:
        try:
            content = self.storage.read_bytes(self.key)
        except StorageError as exc:
            raise ExtractionError(
                f"Source file for {self.entity} not found at {self.storage.uri(self.key)}"
            ) from exc
        logger.info("Read %s (%d bytes)", self.storage.uri(self.key), len(content))
        return [RawObject(name=f"{self.entity}.csv", content=content, fmt="csv")]


class ApiSource:
    """A paginated REST endpoint. The watermark is pushed down as ``updated_since`` so the
    API only returns changed records - the network-efficient form of incremental loading."""

    def __init__(self, entity: str, client: RestApiClient, path: str, page_size: int) -> None:
        self.entity = entity
        self.client = client
        self.path = path
        self.page_size = page_size

    def fetch(self, since: datetime | None) -> list[RawObject]:
        params = {"updated_since": since.isoformat()} if since else {}
        return [
            RawObject(
                name=f"{self.entity}_page_{i:04d}.json", content=fetched.raw_body, fmt="json_pages"
            )
            for i, fetched in enumerate(
                self.client.iter_pages(self.path, params=params, page_size=self.page_size), start=1
            )
        ]
