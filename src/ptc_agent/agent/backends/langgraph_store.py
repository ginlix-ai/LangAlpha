"""Filesystem surface over a LangGraph ``BaseStore`` — used for memory and memo."""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import re
import weakref
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import structlog
from langgraph.store.base import BaseStore

from ptc_agent.agent.backends.results import EditTextResult
from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.backends.store_cache import RequestScopedStoreCache
from ptc_agent.core.sandbox.grep_render import GrepLine
from src.observability.private_errors import failure

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


class StoreContentInvalidError(ValueError):
    """Raised for text the store cannot hold."""


#: A store value is JSON in Postgres, which holds no NUL character, and the
#: write would otherwise fail as an outage.
_NUL_REFUSAL = "Stored text cannot hold a NUL character (\\x00); keep binary content in a workspace file."


class StoreListingIncomplete(RuntimeError):
    """A namespace walk ended early, so absence cannot be concluded from it."""


class ReadOnlyStoreError(PermissionError):
    """Raised when ``awrite_text`` is called against a read-only tier.

    Carries a tier-specific message (e.g. "Memo is user-managed. Ask the user
    to edit or upload via the memo panel.") so the agent's filesystem tool
    can surface that text rather than the generic "Write operation failed".
    """


# Shared across backend instances targeting the same namespace so concurrent
# turns within one process cannot lose updates. WeakValueDictionary auto-prunes
# entries once no caller holds the lock, so the registry never leaks across
# long-running processes. Other processes are ordered by the database lock
# ``namespace_write_lock`` takes after this one.
_WRITE_LOCKS: weakref.WeakValueDictionary[tuple[str, ...], asyncio.Lock] = (
    weakref.WeakValueDictionary()
)

# Salted so these keys cannot collide with the app's other advisory locks.
_WRITE_LOCK_KEY_PREFIX = "store-write:"

# A holder keeps the lock for one read and one write of the store, each bounded
# by _STORE_OP_TIMEOUT_S, so a wait past this means the holder is stuck.
_WRITE_LOCK_WAIT = "5s"


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


@contextlib.asynccontextmanager
async def namespace_write_lock(
    store: BaseStore, namespace: tuple[str, ...]
) -> AsyncIterator[None]:
    """Hold ``namespace`` against every other writer of it, in any worker.

    A Postgres store is shared by every server worker, so this also takes an
    advisory lock on the app database, held until the write inside has
    committed. A wait past the limit raises ``TimeoutError``, as a stuck store
    does, so a writer answers the two alike.
    """
    from langgraph.store.postgres import AsyncPostgresStore

    async with lock_for_namespace(namespace):
        if not isinstance(store, AsyncPostgresStore):
            yield
            return
        from psycopg.errors import LockNotAvailable

        from src.server.database.pool import get_db_connection

        # After the in-process lock, so a worker queues at most one connection
        # per namespace here, and on the app pool: a waiter on the store's own
        # pool would hold a connection the holder's reads and writes need.
        async with get_db_connection() as conn, conn.transaction():
            try:
                await conn.execute(f"SET LOCAL lock_timeout = '{_WRITE_LOCK_WAIT}'")
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (_WRITE_LOCK_KEY_PREFIX + "/".join(namespace),),
                )
            except LockNotAvailable as exc:
                raise TimeoutError(f"store namespace {namespace} stayed locked") from exc
            yield


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


def grep_texts(
    texts: list[tuple[str, str]],
    compiled: re.Pattern[str],
    output_mode: str,
    *,
    show_line_numbers: bool,
    head_limit: int | None,
    offset: int,
) -> list[Any]:
    """Grep over files held in memory as (path, content), answered in the
    sandbox grep's output modes."""
    files: list[str] = []
    lines: list[str] = []
    counts: list[tuple[str, int]] = []
    for path, content in texts:
        found = [
            GrepLine(
                f"{path}:{number}:{line}" if show_line_numbers else f"{path}:{line}",
                path,
            )
            for number, line in enumerate(content.splitlines(), start=1)
            if compiled.search(line)
        ]
        if found:
            files.append(path)
            lines.extend(found)
            counts.append((path, len(found)))
    selected: list[Any] = {"content": lines, "count": counts}.get(output_mode, files)
    start = max(0, offset)
    return selected[start : None if head_limit is None else start + head_limit]


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

    def is_writable(self, file_path: str) -> bool:
        return not self._read_only

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

    async def awrite_text(
        self,
        file_path: str,
        content: str,
        *,
        check: Callable[[str | None], None] | None = None,
        base_content: str | None = None,
    ) -> bool:
        """Store ``content``; False when the store did not answer in time.

        ``check`` is given what the file holds just before this write replaces
        it, under the namespace lock, and refuses the write by raising, so no
        other writer lands between a caller's precondition and its write. With
        no row the file holds ``base_content``: an overlay tier's shipped file.
        """
        if self._read_only:
            logger.debug("write rejected on read-only tier", path=file_path)
            raise ReadOnlyStoreError(self._read_only_error)
        key = self._path_to_key(file_path)
        if "\x00" in content:
            raise StoreContentInvalidError(_NUL_REFUSAL)
        content_bytes = len(content.encode("utf-8"))
        if content_bytes > MAX_CONTENT_BYTES:
            raise StoreContentTooLargeError(
                f"Stored content is {content_bytes} bytes; "
                f"max is {MAX_CONTENT_BYTES}. Split the content into multiple "
                "detail files or shorten the entry."
            )
        namespace = self._namespace()
        try:
            async with namespace_write_lock(self._store, namespace):
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
                if check is not None:
                    check(
                        self._content_from_value(existing_value)
                        if existing_item
                        else base_content
                    )
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
                except Exception as exc:
                    fields, trace = failure(exc)
                    logger.error("store awrite_text failed", path=file_path, exc_info=trace, **fields)
                    return False
                if self._cache is not None:
                    self._cache.invalidate(namespace, key)
        except TimeoutError:
            logger.warning("store write lock timed out", path=file_path)
            return False
        return True

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
        base_content: str | None = None,
        max_bytes: int | None = None,
    ) -> EditTextResult:
        """Edit a stored file; ``base_content`` forks it when the key is absent.

        An overlay tier (shipped scripts shadowed by the user's own) passes the
        shipped source so the fork and the edit land as one ``aput`` under this
        lock — two separate writes would race, skip the size check, and report
        success even when the fork never persisted. ``max_bytes`` lets a tier
        whose own limit is tighter than the store's apply it to the edited
        result; it can only tighten.
        """
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
        if "\x00" in new_string:
            return {"success": False, "error": _NUL_REFUSAL}
        namespace = self._namespace()
        try:
            async with namespace_write_lock(self._store, namespace):
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
                    if base_content is None:
                        return {"success": False, "error": f"File not found: {file_path}"}
                    content: str | None = base_content
                else:
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

                cap = min(max_bytes or MAX_CONTENT_BYTES, MAX_CONTENT_BYTES)
                if len(new_content.encode("utf-8")) > cap:
                    return {
                        "success": False,
                        "error": (
                            f"Edit would grow content past {cap} bytes; "
                            "split the file or shorten the replacement."
                        ),
                    }

                value = self._build_value(
                    content=new_content, existing=item.value if item else None
                )
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
                    # The agent gets the text, which quotes its own file.
                    fields, trace = failure(exc)
                    logger.error("store aedit_text failed", path=file_path, exc_info=trace, **fields)
                    return {"success": False, "error": str(exc)}
                if self._cache is not None:
                    self._cache.invalidate(namespace, key)
        except TimeoutError:
            logger.warning("store write lock timed out", path=file_path)
            return {
                "success": False,
                "error": "Long-term store timed out. Retry shortly.",
            }

        return {
            "success": True,
            "occurrences": occurrences if replace_all else 1,
            "size": len(new_content),
            "message": (
                f"Edited {file_path} ({occurrences} occurrences replaced)"
                if replace_all
                else f"Edited {file_path}"
            ),
        }

    async def _all_items(self, *, strict: bool = False) -> list[Any]:
        """Page through every Item under the backend's namespace.

        Most namespaces fit one page, so the first round trip asks for one
        page alone rather than ``fanout`` of them; a full one means more,
        fetched in batches of ``fanout`` concurrent pages. The walk stops at
        the first short page.

        The timeout bounds each round trip, not the walk, so a large namespace
        takes more of them rather than failing. A timed-out one ends the walk
        with what it has, which reads as a short listing and not as a failure,
        right for search and grep, where partial beats nothing. ``strict`` is
        for the callers that infer absence from a miss, and for whom a
        truncated listing and an empty namespace must not look alike.
        """
        namespace = self._namespace()
        page_size = 100
        # Modest fan-out — three concurrent paged reads is enough to win
        # against the typical postgres latency without saturating the pool.
        fanout = 3
        items: list[Any] = []
        offset = 0
        width = 1
        while True:
            offsets = [offset + i * page_size for i in range(width)]
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
                if strict:
                    raise StoreListingIncomplete(
                        f"listing {namespace} timed out at offset {offset}"
                    ) from None
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
            offset += width * page_size
            width = fanout
        return items

    def _absolute(self, key: str) -> str:
        return f"{self._root_prefix}{key}"

    async def adelete_text(
        self, file_path: str, *, check: Callable[[str | None], None] | None = None
    ) -> bool:
        """Delete a stored file; False when there was none. Raises on timeout.

        ``check`` refuses the delete as ``awrite_text``'s refuses a write, on
        what the file holds under the namespace lock (None for no file).
        """
        if self._read_only:
            raise ReadOnlyStoreError(self._read_only_error)
        key = self._path_to_key(file_path)
        namespace = self._namespace()
        async with namespace_write_lock(self._store, namespace):
            item = await asyncio.wait_for(
                self._store.aget(namespace, key), timeout=_STORE_OP_TIMEOUT_S
            )
            if check is not None:
                check(self._content_from_value(item.value) if item else None)
            if item is None:
                return False
            await asyncio.wait_for(
                self._store.adelete(namespace, key), timeout=_STORE_OP_TIMEOUT_S
            )
            if self._cache is not None:
                self._cache.invalidate(namespace, key)
        return True

    async def aread_tree(self, path: str) -> dict[str, str]:
        """Every file at or under ``path`` with its content, from one listing.

        The tier is one namespace whose keys hold the path, and the store
        searches by namespace only, so the subtree is kept from a walk of
        the tier. Strict, because callers read a missing path as a file that
        does not exist: a walk a timeout cut short raises instead.
        """
        normalized = self.normalize_path(path)
        if normalized.startswith(self._root_prefix):
            subtree = normalized[len(self._root_prefix):].rstrip("/")
        elif normalized.rstrip("/") == self._root_prefix.rstrip("/"):
            subtree = ""
        else:
            return {}
        out: dict[str, str] = {}
        for item in await self._all_items(strict=True):
            key = str(item.key)
            if subtree and key != subtree and not key.startswith(subtree + "/"):
                continue
            content = self._content_from_value(item.value)
            if content is not None:
                out[self._absolute(key)] = content
        return out

    async def aglob_paths(
        self, pattern: str, path: str = ".", *, strict: bool = False
    ) -> list[str]:
        normalized_path = self.normalize_path(path)
        try:
            subtree = ""
            if normalized_path.startswith(self._root_prefix):
                subtree = normalized_path[len(self._root_prefix):]
            elif normalized_path.rstrip("/") != self._root_prefix.rstrip("/"):
                return []
        except Exception:
            return []

        items = await self._all_items(strict=strict)
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

        texts: list[tuple[str, str]] = []
        for item in items:
            key = str(item.key)
            if subtree and not key.startswith(subtree.rstrip("/") + "/") and key != subtree:
                continue
            if glob and not fnmatch.fnmatch(key, glob) and not fnmatch.fnmatch(
                key.rsplit("/", 1)[-1], glob
            ):
                continue
            content = self._content_from_value(item.value)
            if content is not None:
                texts.append((self._absolute(key), content))
        return grep_texts(
            texts, compiled, output_mode, show_line_numbers=show_line_numbers, head_limit=head_limit, offset=offset
        )
