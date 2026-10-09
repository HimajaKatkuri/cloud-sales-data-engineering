"""The simulated API served over real HTTP (localhost, ephemeral port), the source
connectors, and the raw zone round trip."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

from sales_pipeline.ingestion.api_client import RestApiClient
from sales_pipeline.ingestion.mock_api import create_server
from sales_pipeline.ingestion.raw_zone import land_raw, raw_prefix, read_raw
from sales_pipeline.ingestion.sources import ApiSource, CsvFileSource
from sales_pipeline.storage.local import LocalStorage
from sales_pipeline.utils.exceptions import ExtractionError


def payment(n: int, updated_at: str, **overrides: object) -> dict[str, object]:
    record = {
        "payment_id": f"PAY{n:07d}",
        "order_id": "O000001",
        "payment_date": updated_at,
        "payment_method": "paypal",
        "amount": 10.5,
        "status": "captured",
        "updated_at": updated_at,
    }
    record.update(overrides)
    return record


@pytest.fixture
def payments_file(tmp_path: Path) -> Path:
    records = [payment(n, f"2025-07-{n:02d}T00:00:00Z") for n in range(1, 8)]
    path = tmp_path / "payments.json"
    path.write_text(json.dumps(records))
    return path


def serve(path: Path, fail_first_n: int = 0) -> Iterator[str]:
    server = create_server(path, port=0, fail_first_n=fail_first_n)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def api_url(payments_file: Path) -> Iterator[str]:
    yield from serve(payments_file)


def client(url: str, sleeps: list[float] | None = None) -> RestApiClient:
    return RestApiClient(
        url,
        timeout_seconds=5,
        max_retries=3,
        backoff_seconds=0.01,
        sleep=(sleeps.append if sleeps is not None else (lambda _: None)),
    )


def test_mock_api_paginates_all_records(api_url):
    pages = list(client(api_url).iter_pages("/api/v1/payments", page_size=3))

    assert [len(p.page.data) for p in pages] == [3, 3, 1]
    assert pages[0].page.total_records == 7


def test_mock_api_filters_by_updated_since(api_url):
    pages = list(
        client(api_url).iter_pages(
            "/api/v1/payments", params={"updated_since": "2025-07-05T00:00:00+00:00"}
        )
    )

    ids = [r["payment_id"] for p in pages for r in p.page.data]
    assert ids == ["PAY0000006", "PAY0000007"]


def test_retries_recover_from_simulated_outage(payments_file):
    sleeps: list[float] = []
    for url in serve(payments_file, fail_first_n=2):
        pages = list(client(url, sleeps).iter_pages("/api/v1/payments", page_size=100))
    assert len(pages[0].page.data) == 7
    assert len(sleeps) == 2


def test_api_source_lands_each_page_unmodified(api_url, tmp_path):
    storage = LocalStorage(tmp_path / "lake")
    source = ApiSource("payments", client(api_url), "/api/v1/payments", page_size=4)

    keys = land_raw(source, storage, "b1", ingest_date=date(2025, 7, 15))

    prefix = raw_prefix("payments", date(2025, 7, 15), "b1")
    assert prefix == "raw/payments/ingest_date=2025-07-15/batch_id=b1"
    assert keys == [f"{prefix}/payments_page_0001.json", f"{prefix}/payments_page_0002.json"]
    df = read_raw(storage, keys)
    assert len(df) == 7
    assert df["amount"].tolist() == ["10.5"] * 7  # JSON numbers become strings for validation


def test_json_nulls_become_missing_values_not_the_string_nan(tmp_path):
    storage = LocalStorage(tmp_path)
    body = {
        "data": [
            payment(1, "2025-07-01T00:00:00Z", payment_method=None),
            {"payment_id": "PAY0000002"},
        ]
    }  # second record lacks most keys
    storage.write_bytes("raw/payments/x/p.json", json.dumps(body).encode())

    df = read_raw(storage, ["raw/payments/x/p.json"])

    assert df["payment_method"].isna().tolist() == [True, True]
    assert "nan" not in df.astype(object).to_numpy().tolist()


def test_csv_source_preserves_bytes_and_reads_everything_as_text(tmp_path):
    csv_bytes = b"Customer ID,Signup Date,amount\nC00001,2024-05-01,007.50\nC00002,,1e3\n"
    (tmp_path / "customers.csv").write_bytes(csv_bytes)
    storage = LocalStorage(tmp_path / "lake")
    source = CsvFileSource("customers", tmp_path / "customers.csv")

    keys = land_raw(source, storage, "b1", ingest_date=date(2025, 1, 1))
    df = read_raw(storage, keys)

    assert storage.read_bytes(keys[0]) == csv_bytes  # raw zone is an exact copy
    assert df["amount"].tolist() == ["007.50", "1e3"]  # nothing coerced at ingestion
    assert df["Signup Date"].isna().tolist() == [False, True]


def test_missing_source_file_raises_extraction_error(tmp_path):
    with pytest.raises(ExtractionError, match="not found"):
        CsvFileSource("orders", tmp_path / "missing.csv").fetch(None)
