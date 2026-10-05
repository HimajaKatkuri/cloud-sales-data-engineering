"""The generator must be reproducible, or tests and demos stop being comparable."""

from __future__ import annotations

from pathlib import Path

from sales_pipeline.sample_data import generate


def _read_all(directory: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(directory.iterdir())}


def test_same_seed_produces_identical_files(tmp_path):
    generate(tmp_path / "a", batch=2, seed=7)
    generate(tmp_path / "b", batch=2, seed=7)

    assert _read_all(tmp_path / "a") == _read_all(tmp_path / "b")


def test_batch2_is_a_superset_snapshot_of_batch1(tmp_path):
    first = generate(tmp_path / "b1", batch=1)
    second = generate(tmp_path / "b2", batch=2)

    for entity, count in first.items():
        assert second[entity] > count, entity


def test_generated_emails_are_clearly_synthetic(tmp_path):
    generate(tmp_path, batch=1)
    text = (tmp_path / "customers.csv").read_text()

    domains = {
        line.split(",")[3].rsplit("@", 1)[-1].lower()
        for line in text.splitlines()[1:]
        if "@" in line.split(",")[3]
    }
    assert domains == {"example.com"}
