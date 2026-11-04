"""Accession-keyed caches over a small byte store, so object storage can replace the local directory."""
from __future__ import annotations

import gzip
import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from edgar_engine.sections.model import FilingSections
from edgar_engine.sections.version import SECTIONS_VERSION


class Store(Protocol):
    """Keys are '/'-separated relative paths."""

    def get(self, key: str) -> bytes | None: ...

    def put(self, key: str, data: bytes) -> None: ...

    def delete(self, key: str) -> None: ...


class LocalDirStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root or os.environ.get("EDGAR_ENGINE_CACHE") or Path.home() / ".cache" / "edgar-engine")

    def _path(self, key: str) -> Path:
        if key.startswith("/") or "\\" in key or any(part in ("", ".", "..") for part in key.split("/")):
            raise ValueError(f"invalid cache key: {key!r}")
        return self.root / key

    def get(self, key: str) -> bytes | None:
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError:
            return None

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)


class SectionsCache:
    def __init__(self, store: Store | None = None):
        self.store = store or LocalDirStore()

    @staticmethod
    def _key(accession: str) -> str:
        return f"sections/v{SECTIONS_VERSION}/{accession}.json.gz"

    def get(self, accession: str) -> FilingSections | None:
        data = self.store.get(self._key(accession))
        if data is None:
            return None
        try:
            return FilingSections.from_dict(json.loads(gzip.decompress(data)))
        except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def put(self, sections: FilingSections) -> None:
        if not sections.accession:
            raise ValueError("sections.accession is required to cache")
        payload = json.dumps(sections.to_dict(), separators=(",", ":")).encode()
        self.store.put(self._key(sections.accession), gzip.compress(payload, mtime=0))

    def get_or_compute(self, accession: str, compute: Callable[[], FilingSections]) -> FilingSections:
        cached = self.get(accession)
        if cached is not None:
            return cached
        sections = compute()
        self.put(sections)
        return sections


class SubmissionCache:
    """Raw full-text submissions, kept as bytes so a re-parse sees exactly what EDGAR served."""

    def __init__(self, store: Store | None = None):
        self.store = store or LocalDirStore()

    @staticmethod
    def _key(accession: str) -> str:
        return f"submissions/{accession}.txt.gz"

    def get(self, accession: str) -> bytes | None:
        data = self.store.get(self._key(accession))
        if data is None:
            return None
        try:
            return gzip.decompress(data)
        except (OSError, EOFError):
            return None

    def put(self, accession: str, raw: bytes) -> None:
        self.store.put(self._key(accession), gzip.compress(raw, mtime=0))

    def get_or_fetch(self, accession: str, fetch: Callable[[], bytes | str]) -> bytes:
        cached = self.get(accession)
        if cached is not None:
            return cached
        raw = fetch()
        raw = raw.encode() if isinstance(raw, str) else raw
        self.put(accession, raw)
        return raw
