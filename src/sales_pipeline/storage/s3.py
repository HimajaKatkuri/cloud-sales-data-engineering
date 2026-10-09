"""Amazon S3 implementation of ObjectStorage.

Authentication uses boto3's default credential chain: environment variables on a laptop,
the ECS task role / EC2 instance profile in AWS. No credentials are passed in code.

Retries: botocore's "standard" retry mode already retries throttling and transient 5xx
errors with exponential backoff, so we configure it rather than writing our own loop.
Permanent errors (AccessDenied, NoSuchBucket) are not retried and surface as StorageError.
"""

from __future__ import annotations

import logging
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from sales_pipeline.storage.base import ObjectStorage, validate_key
from sales_pipeline.utils.exceptions import StorageError

logger = logging.getLogger(__name__)


class S3Storage(ObjectStorage):
    def __init__(
        self,
        bucket: str,
        region: str,
        prefix: str = "",
        client: Any | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client or boto3.client(
            "s3",
            region_name=region,
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    def _key(self, key: str) -> str:
        validate_key(key)
        return f"{self.prefix}/{key}" if self.prefix else key

    def write_bytes(self, key: str, data: bytes) -> str:
        full_key = self._key(key)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=full_key,
                Body=data,
                ServerSideEncryption="AES256",
            )
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"S3 put_object failed for {self.uri(key)}: {exc}") from exc
        return self.uri(key)

    def read_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
            body: bytes = response["Body"].read()
            return body
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"S3 get_object failed for {self.uri(key)}: {exc}") from exc

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise StorageError(f"S3 head_object failed for {self.uri(key)}: {exc}") from exc

    def list_keys(self, prefix: str) -> list[str]:
        full_prefix = self._key(prefix) if prefix else self.prefix
        strip = len(self.prefix) + 1 if self.prefix else 0
        keys: list[str] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                keys.extend(obj["Key"][strip:] for obj in page.get("Contents", []))
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(f"S3 list failed for s3://{self.bucket}/{full_prefix}") from exc
        return sorted(keys)

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"
