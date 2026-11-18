"""Accession-keyed caches over a small byte store, so object storage can replace the local directory."""
from __future__ import annotations

import gzip
import json
import os
import tempfile
import zlib
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Protocol

from edgar_engine.sections.model import FilingSections
from edgar_engine.sections.version import SECTIONS_VERSION

__all__ = ["DocumentsCache", "LocalDirStore", "S3Store", "SectionsCache", "Store", "SubmissionCache"]

_MISSING_CODES = {"NoSuchKey", "404", "NotFound"}


def _checked(key: str) -> str:
    if key.startswith("/") or "\\" in key or any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError(f"invalid cache key: {key!r}")
    return key


class Store(Protocol):
    """Keys are '/'-separated relative paths."""

    def get(self, key: str) -> bytes | None: ...

    def put(self, key: str, data: bytes) -> None: ...

    def delete(self, key: str) -> None: ...


class LocalDirStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root or os.environ.get("EDGAR_ENGINE_CACHE") or Path.home() / ".cache" / "edgar-engine")

    def _path(self, key: str) -> Path:
        return self.root / _checked(key)

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


class S3Store:
    """A Store over an S3-compatible client with boto3's call shape (get_object, put_object, delete_object),
    injected so the library takes no dependency on boto3. Keys live under prefix, which is joined as given, so end
    a folder prefix with '/'."""

    def __init__(self, client, bucket: str, prefix: str = ""):
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def _key(self, key: str) -> str:
        return self.prefix + _checked(key)

    def get(self, key: str) -> bytes | None:
        name = self._key(key)
        try:
            return self.client.get_object(Bucket=self.bucket, Key=name)["Body"].read()
        except Exception as e:
            no_such_key = getattr(getattr(self.client, "exceptions", None), "NoSuchKey", None)
            code = (getattr(e, "response", None) or {}).get("Error", {}).get("Code")
            if (no_such_key and isinstance(e, no_such_key)) or code in _MISSING_CODES:
                return None
            raise

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(key))


class SectionsCache:
    def __init__(self, store: Store | None = None):
        self.store = store if store is not None else LocalDirStore()

    @staticmethod
    def _key(accession: str) -> str:
        return f"sections/v{SECTIONS_VERSION}/{accession}.json.gz"

    def get(self, accession: str) -> FilingSections | None:
        data = self.store.get(self._key(accession))
        if data is None:
            return None
        try:
            return FilingSections.from_dict(json.loads(gzip.decompress(data)))
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError, AttributeError):
            return None

    def put(self, sections: FilingSections) -> None:
        if not sections.accession:
            raise ValueError("sections.accession is required to cache")
        if sections.view != "as_filed":
            raise ValueError("only an as-filed view can be cached: an as-amended one depends on its amendments")
        payload = json.dumps(sections.to_dict(), separators=(",", ":")).encode()
        self.store.put(self._key(sections.accession), gzip.compress(payload, mtime=0))

    def get_or_compute(self, accession: str, compute: Callable[[], FilingSections]) -> FilingSections:
        cached = self.get(accession)
        if cached is not None:
            return cached
        sections = compute()
        self.put(sections)
        return sections


class DocumentsCache:
    """A filing's attached documents (edgar_engine.documents.attached_documents), rendered."""

    def __init__(self, store: Store | None = None):
        self.store = store if store is not None else LocalDirStore()

    @staticmethod
    def _key(accession: str) -> str:
        return f"documents/v{SECTIONS_VERSION}/{accession}.json.gz"

    def get(self, accession: str) -> list | None:
        from edgar_engine.documents import SubmissionDocument

        data = self.store.get(self._key(accession))
        if data is None:
            return None
        try:
            return [SubmissionDocument(**d) for d in json.loads(gzip.decompress(data))]
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError):
            return None

    def put(self, accession: str, documents: list) -> None:
        payload = json.dumps([asdict(d) for d in documents], separators=(",", ":")).encode()
        self.store.put(self._key(accession), gzip.compress(payload, mtime=0))


class SubmissionCache:
    """Raw full-text submissions, kept as bytes so a re-parse sees exactly what EDGAR served."""

    def __init__(self, store: Store | None = None):
        self.store = store if store is not None else LocalDirStore()

    @staticmethod
    def _key(accession: str) -> str:
        return f"submissions/{accession}.txt.gz"

    def get(self, accession: str) -> bytes | None:
        data = self.store.get(self._key(accession))
        if data is None:
            return None
        try:
            return gzip.decompress(data) or None
        except (OSError, EOFError, zlib.error):
            return None

    def put(self, accession: str, raw: bytes) -> None:
        self.store.put(self._key(accession), gzip.compress(raw, mtime=0))

    def delete(self, accession: str) -> None:
        self.store.delete(self._key(accession))

    def get_or_fetch(self, accession: str, fetch: Callable[[], bytes | str]) -> bytes:
        cached = self.get(accession)
        if cached is not None:
            return cached
        raw = fetch()
        raw = raw.encode() if isinstance(raw, str) else raw
        self.put(accession, raw)
        return raw
