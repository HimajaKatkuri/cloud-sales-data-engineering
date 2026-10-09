"""Local-filesystem implementation of ObjectStorage (used for development and CI)."""

from __future__ import annotations

from pathlib import Path

from sales_pipeline.storage.base import ObjectStorage, validate_key
from sales_pipeline.utils.exceptions import StorageError


class LocalStorage(ObjectStorage):
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()

    def _path(self, key: str) -> Path:
        return self.root / validate_key(key)

    def write_bytes(self, key: str, data: bytes) -> str:
        path = self._path(key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Write to a temp file then rename, so readers never see a half-written file.
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(path)
        except OSError as exc:
            raise StorageError(f"Could not write {path}: {exc}") from exc
        return str(path)

    def read_bytes(self, key: str) -> bytes:
        path = self._path(key)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise StorageError(f"Could not read {path}: {exc}") from exc

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix if prefix else self.root
        if not base.exists():
            return []
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in base.rglob("*")
            if p.is_file() and not p.name.endswith(".tmp")
        )

    def uri(self, key: str) -> str:
        return str(self._path(key))
