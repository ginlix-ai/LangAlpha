"""Path classification, decode, and sandbox-access helpers shared by the
CRUD (`crud.py`) and serving (`serve.py`) routers."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import unquote

from charset_normalizer import from_bytes
from fastapi import HTTPException


from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    SANDBOX_ROOTS,
    ALWAYS_HIDDEN_BASENAMES as _SHARED_BASENAMES,
    ALWAYS_HIDDEN_DIR_NAMES,
    ALWAYS_HIDDEN_PATH_SEGMENTS,
    ALWAYS_HIDDEN_SUFFIXES,
    HIDDEN_DIR_NAMES,
    USER_PROFILE_DATA_DIR,
    SandboxLayout,
    WorkspaceLayout,
    USER_PROFILE_PORTFOLIO_FILE,
    USER_PROFILE_PREFERENCE_FILE,
    USER_PROFILE_WATCHLIST_FILE,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.workspace_layout import (
    WorkspaceLayoutUnavailable,
    layout_from_binding,
)
from src.server.services.persistence.resolve import (
    FileBytesUnavailable,
    resolve_file_bytes,
)
from src.server.services import user_data_io
from src.server.utils.error_sanitization import (
    sandbox_unreachable_detail,
    single_line,
)
from src.observability import safe_record, workspace_fs_bytes

logger = logging.getLogger(__name__)


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
    try:
        content = await resolve_file_bytes(file_record, user_id=user_id)
    except FileBytesUnavailable as e:
        logger.warning(f"Blob unavailable for {file_record.get('file_path')!r}: {e}")
        raise HTTPException(
            status_code=503, detail="File content temporarily unavailable"
        ) from None
    if content is None:
        raise HTTPException(status_code=404, detail="File content not available")
    return content


async def http_file_text(file_record: dict[str, Any], *, user_id: str) -> str:
    """:func:`http_file_bytes`, decoded. Caller must 415 binary rows first.

    ``replace`` is a guard, not a decoding strategy; see ``resolve_file_text``.
    """
    data = await http_file_bytes(file_record, user_id=user_id)
    return data.decode("utf-8", errors="replace")


def _record_fs_bytes(op: str, size: int | None) -> None:
    """Emit workspace.fs.bytes histogram. No-op when size is unknown / negative."""
    if not size or size < 0:
        return
    safe_record(workspace_fs_bytes, int(size), {"op": op})

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

# User-profile virtual files (served by user_data_io, not the sandbox FS).
# These three paths bypass the system-path filter and route to the DB layer.
_USER_PROFILE_PREFIX = f"{USER_PROFILE_DATA_DIR.rstrip('/')}/"
_USER_PROFILE_FILES: dict[str, str] = {
    f"{_USER_PROFILE_PREFIX}{USER_PROFILE_PORTFOLIO_FILE}": USER_PROFILE_PORTFOLIO_FILE,
    f"{_USER_PROFILE_PREFIX}{USER_PROFILE_WATCHLIST_FILE}": USER_PROFILE_WATCHLIST_FILE,
    f"{_USER_PROFILE_PREFIX}{USER_PROFILE_PREFERENCE_FILE}": USER_PROFILE_PREFERENCE_FILE,
}


def _is_user_profile_dir(client_path: str) -> bool:
    """True when the path refers to the .agents/user/profile/ directory itself."""
    return client_path.rstrip("/") == _USER_PROFILE_PREFIX.rstrip("/")


def _is_user_profile_file(client_path: str) -> bool:
    return client_path in _USER_PROFILE_FILES


async def _serialize_user_profile_file(client_path: str, user_id: str) -> str:
    """Fetch + serialize one of the three virtual user-profile JSON files."""
    filename = _USER_PROFILE_FILES[client_path]
    if filename == USER_PROFILE_PORTFOLIO_FILE:
        rows = await user_data_io.fetch_portfolio_for_user(user_id)
        payload = user_data_io.serialize_portfolio(rows)
    elif filename == USER_PROFILE_WATCHLIST_FILE:
        watchlists, items = await user_data_io.fetch_watchlist_for_user(user_id)
        payload = user_data_io.serialize_watchlist(watchlists, items)
    else:  # preference.json
        prefs = await user_data_io.fetch_preferences_for_user(user_id)
        payload = user_data_io.serialize_preferences(prefs)
    visible = {k: v for k, v in payload.items() if k != "__version__"}
    return user_data_io.serialize_json(visible)


# Derived from shared constants (source of truth: ptc_agent.core.paths)
_SYSTEM_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(AGENT_SYSTEM_DIRS))
_HIDDEN_DIR_PREFIXES = tuple(f"{d}/" for d in sorted(HIDDEN_DIR_NAMES))
_ALWAYS_HIDDEN_SEGMENTS = ALWAYS_HIDDEN_PATH_SEGMENTS
_ALWAYS_HIDDEN_BASENAMES = _SHARED_BASENAMES + (".file_sync_marker",)
_ALWAYS_HIDDEN_SUFFIXES = ALWAYS_HIDDEN_SUFFIXES

_ALWAYS_HIDDEN_DIR_SEGMENTS = tuple(f"/{d}/" for d in ALWAYS_HIDDEN_DIR_NAMES)

# Generous but bounded defaults.
DEFAULT_READ_LIMIT_LINES = 20_000
MAX_UPLOAD_BYTES = 250 * 1024 * 1024  # 250MB

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


def _is_flash_workspace(workspace: dict[str, Any]) -> bool:
    return workspace.get("status") == "flash"


async def _acquire_sandbox(workspace_id: str, user_id: str) -> Any:
    """Get a ready sandbox for the workspace, or raise 503."""
    manager = WorkspaceManager.get_instance()
    try:
        session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)
    except Exception as e:
        # Same wording as the app-level SandboxGoneError/SandboxTransientError
        # handler: the file panel categorizes on this string, and the "starting"
        # sub-case has to survive the boundary — which is exactly what
        # sandbox_unreachable_detail preserves while dropping the raw text.
        logger.warning(
            f"Sandbox unreachable for workspace {workspace_id}: {single_line(str(e))}"
        )
        raise HTTPException(
            status_code=503, detail=sandbox_unreachable_detail(e)
        ) from None

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(
            status_code=503,
            detail="Sandbox is not reachable: no sandbox attached to the session",
        )
    return sandbox


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
    # User-profile virtual files live under .agents/ but are first-class
    # user data — never hide them from the file panel.
    if _is_user_profile_file(client_path) or _is_user_profile_dir(client_path):
        return False
    return any(client_path.startswith(prefix) for prefix in _SYSTEM_DIR_PREFIXES)


def _is_hidden_path(client_path: str) -> bool:
    if client_path == SandboxLayout.INTERNAL_DIR:
        return True
    return any(client_path.startswith(prefix) for prefix in _HIDDEN_DIR_PREFIXES)


def _is_always_hidden_path(client_path: str) -> bool:
    normalized = f"/{client_path.lstrip('/')}"

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
    endpoints apply, so the unauthenticated wsfiles route and the share-token
    serve route never expose agent-infrastructure dirs (``.agents``, ``tools``,
    ``mcp_servers``, ``_internal``, ...) that those endpoints deliberately hide.
    The user-profile carve-out lives inside ``_is_system_path``.
    """
    return (
        _is_always_hidden_path(client_path)
        or _is_hidden_path(client_path)
        or _is_system_path(client_path)
    )


def layout_for(workspace: dict[str, Any], *, manager: Any = None) -> WorkspaceLayout:
    """The folder this project owns on its computer: every route's serve root.

    Several projects share one computer root, so the root on its own is no
    longer a serve root for any of them. Takes the row the caller already read
    and raises ``WorkspaceLayoutUnavailable`` when it cannot name a folder,
    because widening to the computer would serve a sibling's files.
    """
    manager = manager or WorkspaceManager.get_instance()
    root = manager.config.to_core_config().filesystem.working_directory
    workspace_id = str((workspace or {}).get("workspace_id") or "")
    return layout_from_binding(workspace_id, workspace, root=root)


def work_dir_for(workspace: dict[str, Any], *, manager: Any = None) -> str:
    """The serve root as a path, for the routes that only need the string."""
    return layout_for(workspace, manager=manager).workspace


def owner_layout(workspace: dict[str, Any], *, manager: Any = None) -> WorkspaceLayout:
    """``layout_for`` as an HTTP answer, for a route that authenticated its owner.

    503 rather than 404: the placement is a fact the row is expected to carry,
    so its absence is a workspace that is not ready rather than a workspace
    that has no files. The owner gets the failure instead of a listing, since
    the only wider answer available is their neighbours' files.
    """
    try:
        return layout_for(workspace, manager=manager)
    except WorkspaceLayoutUnavailable as e:
        logger.warning(
            f"Refusing file access to workspace "
            f"{(workspace or {}).get('workspace_id')}: {single_line(str(e))}"
        )
        raise HTTPException(
            status_code=503, detail="Workspace files are not available yet"
        ) from None


def owner_work_dir(workspace: dict[str, Any], *, manager: Any = None) -> str:
    return owner_layout(workspace, manager=manager).workspace


_FILE_URL_SCHEME = "file://"


def _file_url_path(raw: str) -> str:
    """The local path a ``file:`` URL names, percent-decoded.

    Transcript links carry this spelling. The scheme says the rest is an
    absolute path however many slashes followed it, so an authority-shaped
    remainder (``file://home/...``) folds to the same path as
    ``file:///home/...``. Decoding happens before the escape checks, so an
    encoded ``..`` is refused rather than smuggled through as a literal segment.
    """
    if raw[: len(_FILE_URL_SCHEME)].lower() != _FILE_URL_SCHEME:
        return raw
    rest = unquote(raw[len(_FILE_URL_SCHEME) :])
    if rest.lower().startswith("localhost/"):
        rest = rest[len("localhost") :]
    return rest if rest.startswith("/") else f"/{rest}"


def _fold_relative(path: str) -> str:
    """Drop the ``./`` and trailing-slash noise a relative spelling carries."""
    out = path
    while out.startswith("./"):
        out = out[2:]
    out = out.rstrip("/")
    return "" if out == "." else out


def _known_roots(work_dir: str) -> tuple[str, ...]:
    """Absolute prefixes a requested path may carry, most specific first.

    The workspace folder sorts ahead of the computer root it sits on because it
    is the longer string, and that ordering is what decides the one ambiguous
    spelling: ``<work_dir>/<dir_name>/x`` reads as this folder's own
    subdirectory rather than as a root-level folder named after the workspace.
    """
    roots = {work_dir.rstrip("/"), *(root.rstrip("/") for root in SANDBOX_ROOTS)}
    return tuple(sorted((r for r in roots if r), key=lambda r: (-len(r), r)))


def workspace_relative_path(path: str, work_dir: str) -> str:
    """Every spelling of a requested path folded to one ``work_dir``-relative form.

    A path arrives relative to the workspace, under the workspace folder, under
    a computer root that folder sits on (the layout before the split spelled
    every path that way, and transcripts still hold those), or as a ``file:``
    URL of any of the three. The sweep that split the root physically moved
    those entries into the folder, so the root spelling names the same file the
    folder spelling does.

    A leading slash survives a path no root claimed: whether that names a
    workspace file or nothing at all is the caller's policy, not this fold's.
    """
    raw = _file_url_path((path or "").strip()).replace("\\", "/")
    for root in _known_roots(work_dir):
        if raw == root:
            return ""
        if raw.startswith(f"{root}/"):
            return _fold_relative(raw[len(root) + 1 :])
    return raw if raw.startswith("/") else _fold_relative(raw)


def _normalize_requested_path(path: str, work_dir: str) -> str:
    """The folded path, reading a slash no root claimed as the workspace root.

    That is the client's virtual-absolute spelling, so one leading slash comes
    off and only one: ``//etc/passwd`` stays absolute and the containment check
    that follows refuses it. ``clean_path`` refuses the same spelling outright,
    because its callers glob what it returns.
    """
    relative = workspace_relative_path(path, work_dir)
    if relative.startswith("/"):
        return _fold_relative(relative[1:])
    return relative


def _requested_hidden_ok(path: str, work_dir: str) -> bool:
    """Return True if caller explicitly requested a hidden directory."""
    normalized = _normalize_requested_path(path, work_dir)
    if not normalized:
        return False
    internal = SandboxLayout.INTERNAL_DIR
    return normalized == internal or normalized.startswith(f"{internal}/")


def _requested_system_ok(path: str, work_dir: str) -> bool:
    """Return True if caller explicitly requested a system directory."""
    normalized = _normalize_requested_path(path, work_dir)
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


def _is_utf8(data: bytes) -> bool:
    """True when bytes decode cleanly as UTF-8 (i.e. text, whatever the MIME)."""
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True
