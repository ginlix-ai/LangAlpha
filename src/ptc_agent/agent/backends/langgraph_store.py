"""Filesystem surface over a LangGraph ``BaseStore`` — used for memory and memo."""

from __future__ import annotations

import asyncio
import fnmatch
import re
import weakref
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import structlog
from langgraph.store.base import BaseStore

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.backends.store_cache import RequestScopedStoreCache

logger = structlog.get_logger(__name__)

_KEY_COMPONENT_RE = re.compile(r"^[A-Za-z0-9\-_.@+~]+$")

MAX_CONTENT_BYTES = 256 * 1024

# Matches the middleware's read-path budget so a stuck store can't wedge a tool call.
_STORE_OP_TIMEOUT_S = 2.0

NamespaceFactory = Callable[[], tuple[str, ...]]


class InvalidStoreKeyError(ValueError):
    """Raised for malformed store keys."""


class StoreContentTooLargeError(ValueError):
    """Raised when a write would exceed ``MAX_CONTENT_BYTES``."""


class ReadOnlyStoreError(PermissionError):
    """Raised when ``awrite_text`` is called against a read-only tier.

    Carries a tier-specific message (e.g. "Memo is user-managed. Ask the user
    to edit or upload via the memo panel.") so the agent's filesystem tool
    can surface that text rather than the generic "Write operation failed".
    """


# Shared across backend instances targeting the same namespace so concurrent
# turns within one process cannot lose updates. WeakValueDictionary auto-prunes
# entries once no caller holds the lock, so the registry never leaks across
# long-running processes. Cross-process safety still needs store-level CAS on
# ``modified_at``.
_WRITE_LOCKS: weakref.WeakValueDictionary[tuple[str, ...], asyncio.Lock] = (
    weakref.WeakValueDictionary()
)


def lock_for_namespace(namespace: tuple[str, ...]) -> asyncio.Lock:
    """Return the in-process write lock for ``namespace``.

    Public so siblings (e.g. the memo router) can serialize their own
    multi-step read-modify-write windows against the same namespace without
    rolling a parallel registry.
    """
    lock = _WRITE_LOCKS.get(namespace)
    if lock is None:
        lock = asyncio.Lock()
        _WRITE_LOCKS[namespace] = lock
    return lock


# Module-private alias kept so internal call sites don't need updating.
_lock_for_namespace = lock_for_namespace


def validate_store_key(key: str) -> None:
    """Validate a store key (path relative to a tier root). Shared with the server API."""
    if not key:
        raise InvalidStoreKeyError("Empty store key")
    if key.startswith("/") or key.endswith("/"):
        raise InvalidStoreKeyError(f"Store key must not start or end with '/': {key!r}")
    for seg in key.split("/"):
        if not seg or seg in ("..", "."):
            raise InvalidStoreKeyError(f"Invalid key segment in {key!r}")
        if not _KEY_COMPONENT_RE.match(seg):
            raise InvalidStoreKeyError(
                f"Disallowed characters in key segment {seg!r} "
                "(allowed: letters, digits, '-', '_', '.', '@', '+', '~')"
            )


class StoreBackend:
    """`BaseStore`-backed filesystem surface for a single store-backed tier."""

    def __init__(
        self,
        *,
        store: BaseStore,
        namespace_factory: NamespaceFactory,
        root_prefix: str,
        sandbox_backend: SandboxBackend,
        read_only: bool = False,
        read_only_error: str = "This path is read-only from the agent. Ask the user to edit it via the UI.",
        cache: RequestScopedStoreCache | None = None,
    ) -> None:
        if not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        self._store = store
        self._namespace_factory = namespace_factory
        self._root_prefix = root_prefix
        self._sandbox = sandbox_backend
        self._read_only = read_only
        self._read_only_error = read_only_error
        # Optional shared cache. When provided, agent-side writes invalidate
        # the affected key so middleware reads in subsequent model calls
        # within the same turn pick up the new value. None means disabled
        # (legacy / tests / dev mode).
        self._cache = cache

    def normalize_path(self, path: str) -> str:
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    @property
    def root_prefix(self) -> str:
        return self._root_prefix

    def _namespace(self) -> tuple[str, ...]:
        return self._namespace_factory()

    def _path_to_key(self, normalized_path: str) -> str:
        if not normalized_path.startswith(self._root_prefix):
            if normalized_path.rstrip("/") == self._root_prefix.rstrip("/"):
                raise InvalidStoreKeyError(
                    f"Store root '{normalized_path}' is not a file path"
                )
            raise InvalidStoreKeyError(
                f"Path '{normalized_path}' is not under store root '{self._root_prefix}'"
            )
        key = normalized_path[len(self._root_prefix):]
        validate_store_key(key)
        return key

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    def _build_value(
        self,
        *,
        content: str,
        existing: Any,
    ) -> dict[str, Any]:
        now = self._now_iso()
        created_at = now
        if isinstance(existing, dict) and isinstance(existing.get("created_at"), str):
            created_at = existing["created_at"]
        return {
            "content": content,
            "encoding": "utf-8",
            "created_at": created_at,
            "modified_at": now,
        }

    @staticmethod
    def _content_from_value(value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        raw = value.get("content")
        if isinstance(raw, str):
            return raw
        # Legacy v1 stored lines as list[str].
        if isinstance(raw, list):
            return "\n".join(raw)
        return None

    async def aread_text(self, file_path: str) -> str | None:
        try:
            key = self._path_to_key(file_path)
        except InvalidStoreKeyError:
            logger.debug("store aread_text invalid key", path=file_path)
            return None
        try:
            item = await asyncio.wait_for(
                self._store.aget(self._namespace(), key),
                timeout=_STORE_OP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "store aget timed out",
                path=file_path,
                timeout_s=_STORE_OP_TIMEOUT_S,
            )
            return None
        if item is None:
            return None
        return self._content_from_value(item.value)

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        content = await self.aread_text(file_path)
        if content is None:
            return None
        lines = content.splitlines(keepends=True)
        start = max(0, offset)
        end = start + max(0, limit)
        return "".join(lines[start:end])

    async def awrite_text(self, file_path: str, content: str) -> bool:
        if self._read_only:
            logger.debug("write rejected on read-only tier", path=file_path)
            raise ReadOnlyStoreError(self._read_only_error)
        key = self._path_to_key(file_path)
        content_bytes = len(content.encode("utf-8"))
        if content_bytes > MAX_CONTENT_BYTES:
            raise StoreContentTooLargeError(
                f"Stored content is {content_bytes} bytes; "
                f"max is {MAX_CONTENT_BYTES}. Split the content into multiple "
                "detail files or shorten the entry."
            )
        namespace = self._namespace()
        lock = _lock_for_namespace(namespace)
        async with lock:
            try:
                existing_item = await asyncio.wait_for(
                    self._store.aget(namespace, key),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "store aget timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return False
            existing_value = existing_item.value if existing_item else None
            value = self._build_value(content=content, existing=existing_value)
            try:
                await asyncio.wait_for(
                    self._store.aput(namespace, key, value),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "store aput timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return False
            except Exception:
                logger.exception("store awrite_text failed", path=file_path)
                return False
            if self._cache is not None:
                self._cache.invalidate(namespace, key)
        return True

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        if self._read_only:
            return {"success": False, "error": self._read_only_error}
        try:
            key = self._path_to_key(file_path)
        except InvalidStoreKeyError as exc:
            return {"success": False, "error": str(exc)}
        if old_string == new_string:
            return {
                "success": False,
                "error": "old_string and new_string are identical",
            }
        namespace = self._namespace()
        lock = _lock_for_namespace(namespace)
        async with lock:
            try:
                item = await asyncio.wait_for(
                    self._store.aget(namespace, key),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "store aget timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return {
                    "success": False,
                    "error": "Long-term store timed out. Retry shortly.",
                }
            if item is None:
                return {"success": False, "error": f"File not found: {file_path}"}
            content = self._content_from_value(item.value)
            if content is None:
                return {
                    "success": False,
                    "error": "Malformed store value (missing content)",
                }

            occurrences = content.count(old_string)
            if occurrences == 0:
                return {"success": False, "error": f"String not found: {old_string!r}"}
            if occurrences > 1 and not replace_all:
                return {
                    "success": False,
                    "error": (
                        f"String appears {occurrences} times. Provide more context or "
                        "set replace_all=True."
                    ),
                }

            if replace_all:
                new_content = content.replace(old_string, new_string)
            else:
                new_content = content.replace(old_string, new_string, 1)

            if len(new_content.encode("utf-8")) > MAX_CONTENT_BYTES:
                return {
                    "success": False,
                    "error": (
                        f"Edit would grow content past {MAX_CONTENT_BYTES} bytes; "
                        "split the file or shorten the replacement."
                    ),
                }

            value = self._build_value(content=new_content, existing=item.value)
            try:
                await asyncio.wait_for(
                    self._store.aput(namespace, key, value),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "store aput timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return {
                    "success": False,
                    "error": "Long-term store timed out. Retry shortly.",
                }
            except Exception as exc:
                logger.exception("store aedit_text failed", path=file_path)
                return {"success": False, "error": str(exc)}
            if self._cache is not None:
                self._cache.invalidate(namespace, key)

        return {
            "success": True,
            "occurrences": occurrences if replace_all else 1,
            "message": (
                f"Edited {file_path} ({occurrences} occurrences replaced)"
                if replace_all
                else f"Edited {file_path}"
            ),
        }

    async def _all_items(self) -> list[Any]:
        """Page through every Item under the backend's namespace.

        Pages are fetched in fan-out batches of ``fanout`` so that a 300-key
        namespace pays roughly ``ceil(N / page_size / fanout)`` round-trips
        instead of ``ceil(N / page_size)``. Termination: as soon as any page
        in a batch returns short, we know there are no more rows past that
        offset and stop. Tiny namespaces (≤ page_size) issue exactly one
        round-trip in either implementation, so there's no overhead in the
        common case.
        """
        namespace = self._namespace()
        page_size = 100
        # Modest fan-out — three concurrent paged reads is enough to win
        # against the typical postgres latency without saturating the pool.
        fanout = 3
        items: list[Any] = []
        offset = 0
        while True:
            offsets = [offset + i * page_size for i in range(fanout)]
            try:
                pages = await asyncio.wait_for(
                    asyncio.gather(
                        *(
                            self._store.asearch(
                                namespace, limit=page_size, offset=o
                            )
                            for o in offsets
                        )
                    ),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "store asearch timed out",
                    namespace=namespace,
                    offset=offset,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                break
            stop = False
            for page in pages:
                if not page:
                    stop = True
                    break
                items.extend(page)
                if len(page) < page_size:
                    stop = True
                    break
            if stop:
                break
            offset += fanout * page_size
        return items

    def _absolute(self, key: str) -> str:
        return f"{self._root_prefix}{key}"

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        normalized_path = self.normalize_path(path)
        try:
            subtree = ""
            if normalized_path.startswith(self._root_prefix):
                subtree = normalized_path[len(self._root_prefix):]
            elif normalized_path.rstrip("/") != self._root_prefix.rstrip("/"):
                return []
        except Exception:
            return []

        items = await self._all_items()
        out: list[str] = []
        for item in items:
            key = str(item.key)
            if subtree and not key.startswith(subtree.rstrip("/") + "/"):
                if key != subtree:
                    continue
            # Match basename as well so `*.md` behaves like `**/*.md`.
            if fnmatch.fnmatch(key, pattern) or fnmatch.fnmatch(
                key.rsplit("/", 1)[-1], pattern
            ):
                out.append(self._absolute(key))
        out.sort()
        return out

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 — mirror sandbox.agrep_rich
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        lines_after: int | None = None,
        lines_before: int | None = None,
        lines_context: int | None = None,
        multiline: bool = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        """Regex search. Context/multiline flags and ``type`` are accepted but ignored."""
        flags = re.IGNORECASE if case_insensitive else 0
        try:
            compiled = re.compile(pattern, flags=flags)
        except re.error as exc:
            logger.debug("store agrep invalid regex", pattern=pattern, error=str(exc))
            return []

        items = await self._all_items()
        normalized_path = self.normalize_path(path)
        subtree = ""
        if normalized_path.startswith(self._root_prefix):
            subtree = normalized_path[len(self._root_prefix):]
        elif normalized_path.rstrip("/") != self._root_prefix.rstrip("/"):
            return []

        files_with_matches: list[str] = []
        content_lines: list[str] = []
        counts: list[tuple[str, int]] = []

        for item in items:
            key = str(item.key)
            if subtree and not key.startswith(subtree.rstrip("/") + "/") and key != subtree:
                continue
            if glob and not fnmatch.fnmatch(key, glob) and not fnmatch.fnmatch(
                key.rsplit("/", 1)[-1], glob
            ):
                continue
            content = self._content_from_value(item.value)
            if content is None:
                continue
            abs_path = self._absolute(key)
            file_count = 0
            matches_here: list[str] = []
            for line_no, line in enumerate(content.splitlines(), start=1):
                if compiled.search(line):
                    file_count += 1
                    if show_line_numbers:
                        matches_here.append(f"{abs_path}:{line_no}:{line}")
                    else:
                        matches_here.append(f"{abs_path}:{line}")
            if file_count == 0:
                continue
            files_with_matches.append(abs_path)
            content_lines.extend(matches_here)
            counts.append((abs_path, file_count))

        def _slice(seq: list[Any]) -> list[Any]:
            start = max(0, offset)
            if head_limit is not None:
                return seq[start : start + head_limit]
            return seq[start:]

        if output_mode == "content":
            return _slice(content_lines)
        if output_mode == "count":
            return _slice(counts)
        return _slice(files_with_matches)
