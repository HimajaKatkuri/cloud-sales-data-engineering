"""Storage backends and a factory that picks one from configuration."""

from __future__ import annotations

from sales_pipeline.config.settings import Settings, StorageBackend
from sales_pipeline.storage.base import ObjectStorage
from sales_pipeline.storage.local import LocalStorage
from sales_pipeline.storage.s3 import S3Storage
from sales_pipeline.utils.exceptions import ConfigurationError


def get_storage(settings: Settings) -> ObjectStorage:
    if settings.storage_backend is StorageBackend.S3:
        if not settings.s3_bucket or not settings.aws_region:
            raise ConfigurationError("STORAGE_BACKEND=s3 requires S3_BUCKET and AWS_REGION")
        return S3Storage(settings.s3_bucket, settings.aws_region, settings.s3_prefix)
    return LocalStorage(settings.local_data_dir)


__all__ = ["LocalStorage", "ObjectStorage", "S3Storage", "get_storage"]
