"""Path classification, decode, and serving helpers shared by the CRUD
(`crud.py`) and serving (`serve.py`) routers; the sandbox access they share is
in ``services/workspace_files``."""

from __future__ import annotations

import asyncio
import codecs
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from charset_normalizer import from_bytes
from fastapi import HTTPException


from ptc_agent.agent.backends.db_json_route import DbJsonRoute
from ptc_agent.agent.filesystem_routes import USER_DATA_ROUTES, route_for
from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    USER_DATA_DIRS,
    ALWAYS_HIDDEN_BASENAMES as _SHARED_BASENAMES,
    ALWAYS_HIDDEN_DIR_NAMES,
    ALWAYS_HIDDEN_PATH_SEGMENTS,
    ALWAYS_HIDDEN_SUFFIXES,
    HIDDEN_DIR_NAMES,
    TRANSIENT_ROOT_PREFIX,
    SandboxLayout,
    logged_path,
)
from ptc_agent.core.sandbox.runtime import STREAM_CHUNK_BYTES
from src.server.database.blob_keys import RELAY_MAX_BYTES
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.database.workspace_folders import is_top_level
from src.server.services.persistence.transfer import (
    INPROCESS_MAX_INFLIGHT_BYTES,
    ByteBudget,
)
from src.server.services.persistence.resolve import (
    FileBytesUnavailable,
    resolve_file_byte_range,
    resolve_file_bytes,
    too_large_to_serve,
)
from src.server.services.workspace_file_containment import normalize_requested_path
from src.server.services.workspace_files import layout_for, owner_layout
from src.server.utils.error_sanitization import single_line

logger = logging.getLogger(__name__)

TOO_LARGE_DETAIL = "This file is too large to open here. Download it instead."


async def http_file_bytes(file_record: dict[str, Any], *, user_id: str) -> bytes:
    """Resolve a persisted file's bytes for a route that authenticated its caller.

    ``user_id`` is the workspace owner's (the storage namespace), which the
    caller already holds from the ownership check.

    503 when the bytes exist but object storage can't serve them right now, 404
    when the row carries no content at all. The caller is already authorized for
    this workspace, so the distinction leaks nothing to them — and a storage
    outage must not be reported as "your file is gone". Routes whose only
    credential is the URL take the opposite policy: see
    ``resolve_file_bytes_or_none``.

    The underlying error is logged rather than returned; its message names the
    object key, which the client has no business seeing.
    """
    if too_large_to_serve(file_record):
        raise HTTPException(status_code=413, detail=TOO_LARGE_DETAIL)
    with _unavailable_as_503(file_record):
        content = await resolve_file_bytes(file_record, user_id=user_id)
    if content is None:
        raise HTTPException(status_code=404, detail="File content not available")
    return content


async def http_file_byte_range(
    file_record: dict[str, Any], offset: int, length: int, *, user_id: str
) -> tuple[bytes, int] | None:
    """``resolve_file_byte_range`` with :func:`http_file_bytes`'s 503.

    Not bound by the serving limit, since nothing holds the whole file. None
    is a row to answer whole, which is also where an empty row gets its 404.
    """
    with _unavailable_as_503(file_record):
        return await resolve_file_byte_range(
            file_record, offset, length, user_id=user_id
        )


@contextmanager
def _unavailable_as_503(file_record: dict[str, Any]) -> Iterator[None]:
    try:
        yield
    except FileBytesUnavailable as e:
        logger.warning(f"Blob unavailable for {file_record.get('file_path')!r}: {e}")
        raise HTTPException(
            status_code=503, detail="File content temporarily unavailable"
        ) from None


async def http_file_text(file_record: dict[str, Any], *, user_id: str) -> str:
    """:func:`http_file_bytes`, decoded. Caller must 415 binary rows first.

    ``replace`` is a guard, not a decoding strategy; see ``resolve_file_text``.
    """
    data = await http_file_bytes(file_record, user_id=user_id)
    return data.decode("utf-8", errors="replace")


# Image MIME types that benefit from HTTP caching
_CACHEABLE_IMAGE_TYPES = frozenset(
    {
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/svg+xml",
        "image/webp",
    }
)

# DB-backed virtual files: rows the agent reaches through a composite route
# (UserDataBackend, AutomationsBackend, SubagentsBackend), never files on the
# sandbox FS. These paths bypass the system-path filter and route to the DB
# layer. The panel serves them read-only: a write belongs to the owning surface
# or the agent's route, which make the schema and version checks this generic
# API cannot.
_VIRTUAL_DIRS = frozenset(USER_DATA_DIRS)


def _virtual_file(client_path: str) -> type[DbJsonRoute] | None:
    """The route a path would be a virtual file of, whether or not one is
    there (an automation's or a custom subagent's file exists only while its
    row does)."""
    return route_for(client_path)


def _is_virtual_dir(client_path: str) -> bool:
    """True when the path is a virtual file's directory itself."""
    return client_path.rstrip("/") in _VIRTUAL_DIRS


async def _virtual_files_in_scope(requested_path: str, user_id: str) -> list[str]:
    """The virtual files a listing rooted at ``requested_path`` covers.

    A root inside a virtual directory covers all of its files, so a listing of
    one of them still shows its siblings.
    """
    paths: list[str] = []
    for route in USER_DATA_ROUTES:
        directory = route.directory
        if not (
            requested_path == ""
            or f"{directory}/".startswith(f"{requested_path}/")
            or requested_path.startswith(f"{directory}/")
        ):
            continue
        try:
            names = await route.names(user_id)
        except Exception:
            # The rest of the listing still serves; these files just don't show.
            logger.exception("virtual file listing failed", extra={"path": directory})
            continue
        paths += [f"{directory}/{name}" for name in names]
    return paths


async def _present_virtual_file(client_path: str, user_id: str) -> bool:
    """Whether ``client_path`` is a virtual file the user has."""
    route = _virtual_file(client_path)
    if route is None:
        return False
    try:
        return client_path.rsplit("/", 1)[-1] in await route.names(user_id)
    except Exception:
        # Resolved as before; the read that follows reports the failure.
        logger.exception("virtual file listing failed", extra={"path": logged_path(client_path)})
        return True


# Derived from shared constants (source of truth: ptc_agent.core.paths)
_SYSTEM_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(AGENT_SYSTEM_DIRS))
_HIDDEN_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(HIDDEN_DIR_NAMES))
_ALWAYS_HIDDEN_SEGMENTS = ALWAYS_HIDDEN_PATH_SEGMENTS
_ALWAYS_HIDDEN_BASENAMES = _SHARED_BASENAMES + (".file_sync_marker",)
_ALWAYS_HIDDEN_SUFFIXES = ALWAYS_HIDDEN_SUFFIXES

_ALWAYS_HIDDEN_DIR_SEGMENTS = tuple(f"/{d}/" for d in ALWAYS_HIDDEN_DIR_NAMES)

# Generous but bounded defaults.
DEFAULT_READ_LIMIT_LINES = 20_000

# This route buffers the whole body in the server before handing it to the
# sandbox, which is the same constraint the relay transfer path has, so it
# takes the same number rather than restating one. It is a ceiling, not the
# limit: the route also asks what the next backup could store and takes
# whichever is tighter, so an upload is never accepted only to be dropped.
MAX_UPLOAD_BYTES = RELAY_MAX_BYTES

_FILES_HELD_AT_ONCE = 8
_held_budgets: dict[asyncio.AbstractEventLoop, ByteBudget] = {}
# A streamed download holds about one ranged read at a time, but it holds it
# for as long as its client takes to read, so it is bounded by count.
_STREAMS_AT_ONCE = 16
_stream_budgets: dict[asyncio.AbstractEventLoop, ByteBudget] = {}


def held_bytes_budget() -> ByteBudget:
    """This worker's allowance for whole files held in memory at once.

    Upload bodies are read whole before they reach the sandbox, and nothing
    else bounds how many a worker holds at once.

    Memory is the one thing a worker owns alone, so the bound is per process
    by design. Keyed by loop because an asyncio primitive belongs to the loop
    that first waits on it.
    """
    loop = asyncio.get_running_loop()
    budget = _held_budgets.get(loop)
    if budget is None:
        _held_budgets.clear()
        budget = _held_budgets[loop] = ByteBudget(
            INPROCESS_MAX_INFLIGHT_BYTES, _FILES_HELD_AT_ONCE
        )
    return budget


def streamed_download_budget() -> ByteBudget:
    """This worker's allowance for downloads streamed from a sandbox at once.

    Apart from the held-bytes budget because a stream's memory does not grow
    with its file: sharing that one would let a few slow clients stall every
    upload. Per loop for the same reason as ``held_bytes_budget``.
    """
    loop = asyncio.get_running_loop()
    budget = _stream_budgets.get(loop)
    if budget is None:
        _stream_budgets.clear()
        budget = _stream_budgets[loop] = ByteBudget(
            _STREAMS_AT_ONCE * STREAM_CHUNK_BYTES, _STREAMS_AT_ONCE
        )
    return budget


# Known binary file extensions that cannot be read as text
_BINARY_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".bmp",
        ".ico",
        ".tiff",
        ".zip",
        ".tar",
        ".gz",
        ".bz2",
        ".7z",
        ".rar",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".mp3",
        ".mp4",
        ".wav",
        ".avi",
        ".mov",
        ".mkv",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".sqlite",
        ".db",
        ".pickle",
        ".pkl",
    }
)


def _is_binary(path: str) -> bool:
    """Check if file extension suggests binary content."""
    suffix = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    return f".{suffix}" in _BINARY_EXTENSIONS


# charset-normalizer's `chaos` score: 0.0 = perfectly coherent text, ~0.1 is
# the practical "good match" cutoff. PNG/JPEG header bytes score ~0.14+; real
# CJK / Cyrillic / Japanese / Korean content scores well under 0.05. Above
# this we'd rather 415 than render Urdu-codepage gibberish to the user.
_CHARSET_DETECT_CHAOS_MAX = 0.1

# Detection on very short non-UTF-8 inputs is unreliable (the library will
# happily match a 3-byte sequence to ``cp1006`` with chaos=0.000). Real text
# files clear this floor easily; adversarial micro-payloads do not.
_CHARSET_DETECT_MIN_BYTES = 8


def _decode_file_text(raw_bytes: bytes) -> str | None:
    """Decode file bytes to text, with UTF-8 fast-path + charset detection.

    Agent-generated reports in non-UTF-8 locales (mainland Chinese GBK,
    Traditional Chinese Big5, Japanese Shift-JIS, etc.) routinely land on
    disk in the system's default codec, so UTF-8-only would 415 those files
    even though they're plain text. Falls back to charset-normalizer's
    confidence-scored detection across ~70 encodings, gated on a chaos
    threshold and a minimum-bytes floor so binary content with a text-like
    extension still surfaces as None (caller 415s).
    """
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        pass
    if len(raw_bytes) < _CHARSET_DETECT_MIN_BYTES:
        return None
    match = from_bytes(raw_bytes).best()
    if match is None or match.chaos > _CHARSET_DETECT_CHAOS_MAX:
        return None
    return str(match)


async def folder_unmoved(workspace_id: str, dir_name: str | None) -> bool:
    """Whether the row still names ``dir_name`` as a settled folder.

    A URL-only route checks this after reading a warm sandbox, in place of the
    folder hold, through which a visitor's requests could keep the folder from
    ever moving. It is enough because a move stages the row before it touches
    the folder, and a sibling lands on the old name only once this row has left
    it. A failed lookup is False, which those routes answer like a missing file.
    """
    try:
        workspace = await db_get_workspace(workspace_id)
    except Exception as e:
        logger.warning(
            f"Folder recheck failed for workspace {workspace_id}: {single_line(str(e))}"
        )
        return False
    if not workspace:
        return False
    current = workspace.get("dir_name")
    return current == dir_name and (not current or is_top_level(current))


def _to_client_path(
    sandbox: Any, absolute_path: str, work_dir: str | None = None
) -> str:
    """Convert an absolute sandbox path into a virtual client path.

    The CLI and web UX prefer paths like "work/task/foo.txt" (no leading slash),
    while still preserving true absolute /tmp paths. *work_dir* is the workspace
    folder the route serves: a client path is relative to that folder, not to
    the computer root the sandbox handle folds against.
    """

    if work_dir and absolute_path.startswith(f"{work_dir.rstrip('/')}/"):
        return absolute_path[len(work_dir.rstrip("/")) + 1:]

    virtual_path = sandbox.virtualize_path(absolute_path)

    # Keep /tmp paths absolute.
    if virtual_path.startswith("/tmp/"):
        return virtual_path

    # Strip the leading slash for working-directory paths.
    if virtual_path.startswith("/"):
        return virtual_path[1:]

    return virtual_path


def _is_system_path(client_path: str) -> bool:
    # Virtual DB-backed files live under .agents/ but are first-class
    # user data, so never hide them from the file panel.
    if _virtual_file(client_path) or _is_virtual_dir(client_path):
        return False
    return any(client_path.startswith(prefix) for prefix in _SYSTEM_DIR_PREFIXES)


def _is_hidden_path(client_path: str) -> bool:
    if client_path == SandboxLayout.INTERNAL_DIR:
        return True
    return any(client_path.startswith(prefix) for prefix in _HIDDEN_DIR_PREFIXES)


def _is_always_hidden_path(client_path: str) -> bool:
    normalized = f"/{client_path.lstrip('/')}"

    # A transfer's partial or staged file, whose bytes are not yet the file
    # they will become; one a user edits or removes fails the transfer.
    if normalized.rfind("/") == 0 and normalized[1:].startswith(TRANSIENT_ROOT_PREFIX):
        return True

    if normalized.endswith(_ALWAYS_HIDDEN_BASENAMES):
        return True

    if normalized.endswith(_ALWAYS_HIDDEN_SUFFIXES):
        return True

    if any(seg in normalized for seg in _ALWAYS_HIDDEN_SEGMENTS):
        return True

    if any(seg in normalized for seg in _ALWAYS_HIDDEN_DIR_SEGMENTS):
        return True

    return False


def _is_serve_blocked_path(client_path: str) -> bool:
    """True if a path must never be served by the file-serving core.

    Mirrors the hidden/system/always-hidden gate the read/download/list
    endpoints apply, so the grant-gated wsfiles route and the public share
    serve route never expose agent-infrastructure dirs (``.agents``, ``tools``,
    ``mcp_servers``, ``_internal``, ...) that those endpoints deliberately hide.

    Stricter than the panel: it skips ``_is_system_path``'s carve-out for the
    user's profile, automation and subagent files. Their owner reads them through the
    file routes, never this core, and the file mount links them into every
    workspace folder, so only containment would stand between a share link and
    the user's holdings.
    """
    return (
        _is_always_hidden_path(client_path)
        or _is_hidden_path(client_path)
        or client_path.startswith(_SYSTEM_DIR_PREFIXES)
    )


def work_dir_for(workspace: dict[str, Any], *, manager: Any = None) -> str:
    """The serve root as a path, for the routes that only need the string."""
    return layout_for(workspace, manager=manager).workspace


def owner_work_dir(workspace: dict[str, Any], *, manager: Any = None) -> str:
    return owner_layout(workspace, manager=manager).workspace


def previous_roots_of(workspace: Mapping[str, Any] | None) -> tuple[str, ...]:
    """The roots of the computers this workspace moved off, longest first.

    A turn from before a move spells its links under one of them. The longest
    goes first because one can sit inside another.
    """
    roots = (r.rstrip("/") for r in (workspace or {}).get("previous_root_dirs") or ())
    return tuple(sorted((r for r in roots if r), key=len, reverse=True))


def _requested_hidden_ok(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> bool:
    """Return True if caller explicitly requested a hidden directory."""
    normalized = normalize_requested_path(path, work_dir, previous_dir_names)
    if not normalized:
        return False
    internal = SandboxLayout.INTERNAL_DIR
    return normalized == internal or normalized.startswith(f"{internal}/")


def _requested_system_ok(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> bool:
    """Return True if caller explicitly requested a system directory."""
    normalized = normalize_requested_path(path, work_dir, previous_dir_names)
    if not normalized:
        return False
    return any(
        normalized == prefix.rstrip("/") or normalized.startswith(prefix)
        for prefix in _SYSTEM_DIR_PREFIXES
    )


def _is_text_content_type(content_type: str) -> bool:
    """True for content types whose bytes should be redacted / theme-injected."""
    ct = content_type.split(";", 1)[0].strip().lower()
    return (
        ct.startswith("text/")
        or ct in ("application/json", "application/xml", "image/svg+xml")
        or ct.endswith("+json")
        or ct.endswith("+xml")
    )


# One closed range. The digit bound keeps a position inside what the shell's
# integers can hold, and nothing longer than the longest such header is parsed.
_BYTE_RANGE = re.compile(r"bytes=(\d{1,18})-(\d{1,18})", re.ASCII | re.IGNORECASE)
_BYTE_RANGE_MAX_CHARS = 64
_PRECONDITIONS = (
    "if-range",
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
)


def requested_byte_range(headers: Mapping[str, str]) -> tuple[int, int] | None:
    """``(first, last)`` of a request asking for one closed byte range.

    Anything else goes whole, as without the header, which a server may always
    do: an open-ended range, since a client resuming a download with
    ``bytes=N-`` would keep a capped answer as the rest of the file; several
    ranges or a suffix, which no client of these routes sends; and any
    conditional request, since its validator belongs to the whole answer.
    """
    header = headers.get("range")
    if not header or len(header) > _BYTE_RANGE_MAX_CHARS:
        return None
    if any(name in headers for name in _PRECONDITIONS):
        return None
    match = _BYTE_RANGE.fullmatch(header.strip())
    if match is None:
        return None
    first, last = int(match[1]), int(match[2])
    if last < first:
        return None
    return first, last



def may_be_utf8(head: bytes, *, whole: bool) -> bool:
    """False once a file's first bytes already rule out :func:`_is_utf8` of the
    whole file, the test that decides whether a whole answer is redacted.

    A character the head cuts off is no error unless the head is the file.
    """
    try:
        codecs.getincrementaldecoder("utf-8")().decode(head, final=whole)
    except UnicodeDecodeError:
        return False
    return True


def _is_utf8(data: bytes) -> bool:
    """True when bytes decode cleanly as UTF-8 (i.e. text, whatever the MIME)."""
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
