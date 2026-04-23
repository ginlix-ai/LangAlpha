"""Memory backend — rich-method filesystem surface over a LangGraph `BaseStore`."""

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

logger = structlog.get_logger(__name__)

_KEY_COMPONENT_RE = re.compile(r"^[A-Za-z0-9\-_.@+~]+$")

MAX_CONTENT_BYTES = 256 * 1024

# Matches the middleware's read-path budget so a stuck store can't wedge a tool call.
_STORE_OP_TIMEOUT_S = 2.0

NamespaceFactory = Callable[[], tuple[str, ...]]


class InvalidMemoryKeyError(ValueError):
    """Raised for malformed memory keys."""


class MemoryContentTooLargeError(ValueError):
    """Raised when a write would exceed ``MAX_CONTENT_BYTES``."""


# Shared across backend instances targeting the same namespace so concurrent
# turns within one process cannot lose updates. WeakValueDictionary auto-prunes
# entries once no caller holds the lock, so the registry never leaks across
# long-running processes. Cross-process safety still needs store-level CAS on
# ``modified_at``.
_WRITE_LOCKS: weakref.WeakValueDictionary[tuple[str, ...], asyncio.Lock] = (
    weakref.WeakValueDictionary()
)


def _lock_for_namespace(namespace: tuple[str, ...]) -> asyncio.Lock:
    lock = _WRITE_LOCKS.get(namespace)
    if lock is None:
        lock = asyncio.Lock()
        _WRITE_LOCKS[namespace] = lock
    return lock


def validate_memory_key(key: str) -> None:
    """Validate a memory key (path relative to a tier root). Shared with the server API."""
    if not key:
        raise InvalidMemoryKeyError("Empty memory key")
    if key.startswith("/") or key.endswith("/"):
        raise InvalidMemoryKeyError(f"Memory key must not start or end with '/': {key!r}")
    for seg in key.split("/"):
        if not seg or seg in ("..", "."):
            raise InvalidMemoryKeyError(f"Invalid key segment in {key!r}")
        if not _KEY_COMPONENT_RE.match(seg):
            raise InvalidMemoryKeyError(
                f"Disallowed characters in key segment {seg!r} "
                "(allowed: letters, digits, '-', '_', '.', '@', '+', '~')"
            )


class StoreMemoryBackend:
    """`BaseStore`-backed filesystem surface for a single memory tier."""

    def __init__(
        self,
        *,
        store: BaseStore,
        namespace_factory: NamespaceFactory,
        root_prefix: str,
        sandbox_backend: SandboxBackend,
    ) -> None:
        if not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        self._store = store
        self._namespace_factory = namespace_factory
        self._root_prefix = root_prefix
        self._sandbox = sandbox_backend

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
                raise InvalidMemoryKeyError(
                    f"Memory root '{normalized_path}' is not a file path"
                )
            raise InvalidMemoryKeyError(
                f"Path '{normalized_path}' is not under memory root '{self._root_prefix}'"
            )
        key = normalized_path[len(self._root_prefix):]
        validate_memory_key(key)
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
        except InvalidMemoryKeyError:
            logger.debug("memory aread_text invalid key", path=file_path)
            return None
        try:
            item = await asyncio.wait_for(
                self._store.aget(self._namespace(), key),
                timeout=_STORE_OP_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "memory aget timed out",
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
        key = self._path_to_key(file_path)
        content_bytes = len(content.encode("utf-8"))
        if content_bytes > MAX_CONTENT_BYTES:
            raise MemoryContentTooLargeError(
                f"Memory content is {content_bytes} bytes; "
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
                    "memory aget timed out",
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
                    "memory aput timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return False
            except Exception:
                logger.exception("memory awrite_text failed", path=file_path)
                return False
        return True

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> dict[str, Any]:
        try:
            key = self._path_to_key(file_path)
        except InvalidMemoryKeyError as exc:
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
                    "memory aget timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return {
                    "success": False,
                    "error": "Memory store timed out. Retry shortly.",
                }
            if item is None:
                return {"success": False, "error": f"File not found: {file_path}"}
            content = self._content_from_value(item.value)
            if content is None:
                return {
                    "success": False,
                    "error": "Malformed memory value (missing content)",
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
                    "memory aput timed out",
                    path=file_path,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                return {
                    "success": False,
                    "error": "Memory store timed out. Retry shortly.",
                }
            except Exception as exc:
                logger.exception("memory aedit_text failed", path=file_path)
                return {"success": False, "error": str(exc)}

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
        namespace = self._namespace()
        page_size = 100
        offset = 0
        items: list[Any] = []
        while True:
            try:
                page = await asyncio.wait_for(
                    self._store.asearch(namespace, limit=page_size, offset=offset),
                    timeout=_STORE_OP_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "memory asearch timed out",
                    namespace=namespace,
                    offset=offset,
                    timeout_s=_STORE_OP_TIMEOUT_S,
                )
                break
            if not page:
                break
            items.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
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
            logger.debug("memory agrep invalid regex", pattern=pattern, error=str(exc))
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
