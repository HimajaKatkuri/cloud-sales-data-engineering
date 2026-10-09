"""Local and S3 storage behave identically. S3 is exercised against moto's in-memory AWS,
so no credentials or network are needed."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from sales_pipeline.config.settings import load_settings
from sales_pipeline.storage import get_storage
from sales_pipeline.storage.base import ObjectStorage
from sales_pipeline.storage.local import LocalStorage
from sales_pipeline.storage.s3 import S3Storage
from sales_pipeline.utils.exceptions import StorageError

BUCKET = "test-sales-lake"


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fake values so boto3 never looks for real credentials.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_storage(aws_env: None) -> Iterator[S3Storage]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)
        yield S3Storage(BUCKET, "us-east-1", prefix="dev")


@pytest.fixture(params=["local", "s3"])
def storage(request: pytest.FixtureRequest, tmp_path) -> ObjectStorage:
    if request.param == "local":
        return LocalStorage(tmp_path)
    return request.getfixturevalue("s3_storage")


def test_round_trip_bytes(storage: ObjectStorage):
    storage.write_bytes("raw/customers/batch_id=b1/customers.csv", b"a,b\n1,2\n")

    assert storage.read_bytes("raw/customers/batch_id=b1/customers.csv") == b"a,b\n1,2\n"
    assert storage.exists("raw/customers/batch_id=b1/customers.csv")
    assert not storage.exists("raw/customers/batch_id=b1/other.csv")


def test_list_keys_by_prefix(storage: ObjectStorage):
    for key in ("raw/a/1.csv", "raw/a/2.csv", "raw/b/1.csv", "processed/a/1.parquet"):
        storage.write_bytes(key, b"x")

    assert storage.list_keys("raw/a") == ["raw/a/1.csv", "raw/a/2.csv"]
    assert len(storage.list_keys("")) == 4


def test_parquet_round_trip(storage: ObjectStorage):
    df = pd.DataFrame({"id": ["a", "b"], "amount": [1.5, 2.25]})

    storage.write_parquet("processed/x/part.parquet", df)

    pd.testing.assert_frame_equal(storage.read_parquet("processed/x/part.parquet"), df)


def test_reading_a_missing_key_raises_storage_error(storage: ObjectStorage):
    with pytest.raises(StorageError):
        storage.read_bytes("raw/nothing/here.csv")


@pytest.mark.parametrize("key", ["../etc/passwd", "/absolute/path", "raw/../../x", ""])
def test_unsafe_keys_are_rejected(storage: ObjectStorage, key: str):
    with pytest.raises(StorageError, match="Invalid storage key"):
        storage.write_bytes(key, b"x")


def test_s3_objects_are_prefixed_and_encrypted(s3_storage: S3Storage):
    uri = s3_storage.write_bytes("raw/a/file.csv", b"x")

    head = s3_storage.client.head_object(Bucket=BUCKET, Key="dev/raw/a/file.csv")
    assert uri == f"s3://{BUCKET}/dev/raw/a/file.csv"
    assert head["ServerSideEncryption"] == "AES256"


def test_s3_permission_or_bucket_errors_surface_as_storage_error(aws_env):
    with mock_aws():  # bucket deliberately not created
        storage = S3Storage("bucket-that-does-not-exist", "us-east-1")
        with pytest.raises(StorageError, match="put_object failed"):
            storage.write_bytes("raw/a.csv", b"x")


def test_factory_selects_backend(tmp_path, aws_env):
    assert isinstance(get_storage(load_settings(local_data_dir=tmp_path)), LocalStorage)
    s3 = get_storage(load_settings(storage_backend="s3", s3_bucket=BUCKET, aws_region="us-east-1"))
    assert isinstance(s3, S3Storage)
