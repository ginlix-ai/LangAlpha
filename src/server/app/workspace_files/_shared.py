"""Path classification, decode, and sandbox-access helpers shared by the
CRUD (`crud.py`) and serving (`serve.py`) routers."""

from __future__ import annotations

import logging
from typing import Any

from charset_normalizer import from_bytes
from fastapi import HTTPException


from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    ALWAYS_HIDDEN_BASENAMES as _SHARED_BASENAMES,
    ALWAYS_HIDDEN_DIR_NAMES,
    ALWAYS_HIDDEN_PATH_SEGMENTS,
    ALWAYS_HIDDEN_SUFFIXES,
    HIDDEN_DIR_NAMES,
    USER_PROFILE_DATA_DIR,
    USER_PROFILE_PORTFOLIO_FILE,
    USER_PROFILE_PREFERENCE_FILE,
    USER_PROFILE_WATCHLIST_FILE,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services import user_data_io
from src.observability import safe_record, workspace_fs_bytes

logger = logging.getLogger(__name__)


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
        raise HTTPException(status_code=503, detail=f"Sandbox not ready: {e}") from None

    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        raise HTTPException(status_code=503, detail="Sandbox not available")
    return sandbox


def _to_client_path(sandbox: Any, absolute_path: str) -> str:
    """Convert an absolute sandbox path into a virtual client path.

    The CLI and web UX prefer paths like "results/foo.txt" (no leading slash),
    while still preserving true absolute /tmp paths.
    """

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
    if client_path == "_internal":
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


def _get_work_dir() -> str:
    """Return the configured working directory from WorkspaceManager config."""
    manager = WorkspaceManager.get_instance()
    return manager.config.to_core_config().filesystem.working_directory


def _normalize_requested_path(path: str, work_dir: str) -> str:
    """Normalize a requested path for comparison."""
    raw = (path or "").strip()
    if raw in {"", ".", "./"}:
        return ""

    normalized = raw
    work_dir_prefix = work_dir.rstrip("/") + "/"
    if normalized.startswith(work_dir_prefix):
        normalized = normalized[len(work_dir_prefix):]
    if normalized.startswith("/"):
        normalized = normalized[1:]
    if normalized.startswith("./"):
        normalized = normalized[2:]

    return normalized


def _requested_hidden_ok(path: str, work_dir: str) -> bool:
    """Return True if caller explicitly requested a hidden directory."""
    normalized = _normalize_requested_path(path, work_dir)
    if not normalized:
        return False
    return normalized == "_internal" or normalized.startswith("_internal/")


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
