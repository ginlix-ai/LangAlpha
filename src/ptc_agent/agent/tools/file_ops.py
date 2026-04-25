"""Read, Write, and Edit tool factories for the agent filesystem backend."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import structlog
from langchain_core.tools import tool

from ptc_agent.agent.backends import FilesystemBackend, ReadOnlyStoreError

logger = structlog.get_logger(__name__)

# Supported image extensions for vision/document middleware
IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp"})

# Supported document extensions for document middleware
DOCUMENT_EXTENSIONS = frozenset({".pdf"})

# Combined visual extensions (images + documents that need special handling)
VISUAL_EXTENSIONS = IMAGE_EXTENSIONS | DOCUMENT_EXTENSIONS

# Memo PDFs are extracted to text on upload and live in the store-backed memo
# tier; serve their extracted text through the regular read path instead of
# the multimodal/document middleware (which is wired to the sandbox FS, not
# the composite memo backend).
_MEMO_TEXT_PREFIX = ".agents/user/memo/"

# Type alias for operation callback
OperationCallback = Callable[[dict[str, Any]], None]

# Protected user data directory and files
_PROTECTED_USER_DIR = ".agents/user/"
_PROTECTED_USER_FILES = {"preference.md", "watchlist.md", "portfolio.md"}


def _is_protected_user_path(path: str) -> bool:
    """Check if ``path`` targets a legacy user-profile file under ``.agents/user/``."""
    name = Path(path).name
    if name not in _PROTECTED_USER_FILES:
        return False
    # Memory paths must stay writable even when the basename collides with a
    # legacy protected filename.
    normalized = path.lstrip("/")
    memory_markers = (
        ".agents/user/memory/",
        "home/daytona/.agents/user/memory/",
        "home/workspace/.agents/user/memory/",
    )
    if any(normalized.startswith(m) for m in memory_markers):
        return False
    return (
        normalized.startswith("home/daytona/.agents/user/")
        or normalized.startswith("home/workspace/.agents/user/")
        or normalized.startswith(_PROTECTED_USER_DIR)
    )


def _get_protection_error(path: str) -> str:
    return (
        f"ERROR: Cannot modify {path} directly.\n\n"
        "User data files are read-only. To update user data:\n"
        "1. Load the skill: load_skill('user-profile')\n"
        "2. Use update_user_data() or remove_user_data()\n\n"
        "See .agents/skills/user-profile/SKILL.md for details."
    )


def create_filesystem_tools(
    backend: FilesystemBackend,
    operation_callback: OperationCallback | None = None,
) -> tuple:
    """Create the Read, Write, and Edit tools bound to ``backend``.

    ``backend`` is either a plain ``SandboxBackend`` or a
    ``CompositeFilesystemBackend`` that adds store-backed memory/memo routing;
    the tools see a uniform interface either way.
    """

    def _format_cat_n(lines: list[str], *, start_line_number: int) -> str:
        return "\n".join(f"{i:6}\t{line}" for i, line in enumerate(lines, start=start_line_number))

    @tool("Read")
    async def read_file(file_path: str, offset: int | None = None, limit: int | None = None) -> str:
        """Read a file with line numbers (cat -n format). Also supports images (PNG, JPG, GIF, WebP), PDFs, and URLs.

        Args:
            file_path: Path to file (relative or absolute), or image/PDF URL.
            offset: Line offset (0-indexed). Default: 0. Ignored for images/PDFs.
            limit: Maximum number of lines. Default: 2000. Ignored for images/PDFs.

        Returns:
            File contents with line numbers, document loading confirmation, or ERROR.
        """
        try:
            # Handle URLs - middleware will inject the content
            if file_path.startswith(("http://", "https://")):
                logger.info("Loading document from URL", url=file_path)
                return f"Loading document from URL: {file_path}"

            # Check if this is a visual file (image or document) by extension.
            # Memo-tier PDFs bypass the multimodal branch — extracted text lives
            # in the store and the multimodal middleware would otherwise fail
            # to find the file on the sandbox FS.
            suffix = Path(file_path).suffix.lower()
            # Glob/Grep virtualize store-backed matches as ``/.agents/...`` and
            # users may pass ``./.agents/...`` from a relative cwd. ``lstrip``
            # would strip every leading ``.`` and ``/`` indiscriminately (it's
            # a charset, not a substring), so we match each literal prefix.
            _stripped = file_path
            if _stripped.startswith("./"):
                _stripped = _stripped[2:]
            elif _stripped.startswith("/"):
                _stripped = _stripped[1:]
            is_memo_path = _stripped.startswith(_MEMO_TEXT_PREFIX)
            if suffix in VISUAL_EXTENSIONS and not is_memo_path:
                # Validate the path exists before returning acknowledgment
                normalized_path = backend.normalize_path(file_path)
                logger.info("Loading image file", file_path=file_path, normalized_path=normalized_path)

                if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                    error_msg = f"Access denied: {file_path} is not in allowed directories"
                    logger.error(error_msg, file_path=file_path)
                    return f"ERROR: {error_msg}"

                # Return acknowledgment - middleware will handle content injection
                file_type = "image" if suffix in IMAGE_EXTENSIONS else "document"
                return f"Loading {file_type}: {file_path}"

            # Standard text file handling
            normalized_path = backend.normalize_path(file_path)
            logger.info("Reading file", file_path=file_path, normalized_path=normalized_path, offset=offset, limit=limit)

            if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            start_offset = offset or 0
            max_lines = limit or 2000

            if offset is not None or limit is not None:
                content = await backend.aread_range(normalized_path, start_offset, max_lines)
            else:
                content = await backend.aread_text(normalized_path)

            if content is None:
                error_msg = f"File not found: {file_path}"
                logger.warning(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            lines = content.splitlines()
            return _format_cat_n(lines, start_line_number=start_offset + 1)

        except Exception as e:
            error_msg = f"Failed to read file: {e!s}"
            logger.exception(error_msg, file_path=file_path)
            return f"ERROR: {error_msg}"

    @tool("Write")
    async def write_file(file_path: str, content: str) -> str:
        """Write content to a file. Overwrites existing."""
        try:
            normalized_path = backend.normalize_path(file_path)
            logger.info("Writing file", file_path=file_path, normalized_path=normalized_path, size=len(content))

            # Check for protected user data paths
            if _is_protected_user_path(normalized_path):
                logger.warning("Blocked write to protected user path", file_path=file_path)
                return _get_protection_error(file_path)

            if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            try:
                success = await backend.awrite_text(normalized_path, content)
            except ReadOnlyStoreError as exc:
                logger.info(
                    "write rejected on read-only path",
                    file_path=file_path,
                )
                return f"ERROR: {exc}"
            if not success:
                return "ERROR: Write operation failed"

            # Invoke operation callback for persistence
            if operation_callback:
                try:
                    operation_callback({
                        "operation": "write_file",
                        "file_path": normalized_path,
                        "line_count": content.count("\n") + 1,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "content": content,  # Full file content for DB persistence
                    })
                except Exception as cb_err:
                    logger.warning("Operation callback failed", error=str(cb_err))

            bytes_written = len(content.encode("utf-8"))
            virtual_path = backend.virtualize_path(normalized_path)
            return f"Wrote {bytes_written} bytes to {virtual_path}"

        except Exception as e:
            error_msg = f"Failed to write file: {e!s}"
            logger.error(error_msg, file_path=file_path, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}"

    @tool("Edit")
    async def edit_file(file_path: str, old_string: str, new_string: str, replace_all: bool = False) -> str:
        """Replace exact string in a file. Must Read file first."""
        try:
            normalized_path = backend.normalize_path(file_path)
            logger.info(
                "Editing file",
                file_path=file_path,
                normalized_path=normalized_path,
                old_string_preview=old_string[:50],
                replace_all=replace_all,
            )

            # Check for protected user data paths
            if _is_protected_user_path(normalized_path):
                logger.warning("Blocked edit to protected user path", file_path=file_path)
                return _get_protection_error(file_path)

            if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            result = await backend.aedit_text(normalized_path, old_string, new_string, replace_all=replace_all)
            if not result.get("success", False):
                error_msg = result.get("error", "Edit operation failed")
                return f"ERROR: {error_msg}"

            # Invoke operation callback for persistence
            if operation_callback:
                try:
                    content = await backend.aread_text(normalized_path)
                    operation_callback({
                        "operation": "edit_file",
                        "file_path": normalized_path,
                        "occurrences": result.get("occurrences", 1),
                        "replace_all": replace_all,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "old_string": old_string,  # Text being replaced
                        "new_string": new_string,  # Replacement text
                        "content": content,
                    })
                except Exception as cb_err:
                    logger.warning("Operation callback failed", error=str(cb_err))

            return str(result.get("message", "File edited successfully"))

        except Exception as e:
            error_msg = f"Failed to edit file: {e!s}"
            logger.error(error_msg, file_path=file_path, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}"

    return read_file, write_file, edit_file
