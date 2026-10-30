"""CLI exit codes: schedulers and CI rely on them to know a run failed."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from sales_pipeline import cli
from sales_pipeline.config.settings import get_settings


@pytest.fixture(autouse=True)
def fresh_settings(monkeypatch: pytest.MonkeyPatch, tmp_path) -> Iterator[None]:
    monkeypatch.chdir(tmp_path)  # no .env file here
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_configuration_error_exits_with_code_2(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET", "")

    assert cli.main(["run"]) == 2


def test_generate_data_writes_files_and_exits_0(monkeypatch, tmp_path):
    monkeypatch.setenv("SOURCE_DATA_DIR", str(tmp_path / "src"))

    assert cli.main(["generate-data"]) == 0
    assert (tmp_path / "src" / "orders.csv").exists()


def test_to_storage_without_source_prefix_is_a_configuration_error(monkeypatch):
    monkeypatch.delenv("SOURCE_PREFIX", raising=False)

    assert cli.main(["generate-data", "--to-storage"]) == 2


def test_to_storage_uploads_sample_files(monkeypatch, tmp_path):
    monkeypatch.setenv("SOURCE_PREFIX", "landing")
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path / "lake"))

    assert cli.main(["generate-data", "--to-storage"]) == 0
    landed = sorted(p.name for p in (tmp_path / "lake" / "landing").iterdir())
    assert landed == sorted(cli.SAMPLE_FILES)
