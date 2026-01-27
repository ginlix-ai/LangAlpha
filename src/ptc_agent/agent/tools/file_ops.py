"""File operation tools: read, write, edit."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable

import structlog
from langchain_core.tools import tool

logger = structlog.get_logger(__name__)

# Type alias for operation callback
OperationCallback = Callable[[dict[str, Any]], None]

# Protected user data directory and files
_PROTECTED_USER_DIR = "user/"
_PROTECTED_USER_FILES = {"preference.md", "watchlist.md", "portfolio.md"}


def _is_protected_user_path(path: str) -> bool:
    """Check if path is in the protected user data directory."""
    normalized = path.lstrip("/")
    # Check sandbox absolute path
    if normalized.startswith("home/daytona/user/"):
        return True
    # Check relative path
    if normalized.startswith(_PROTECTED_USER_DIR):
        return True
    return False


def _get_protection_error(path: str) -> str:
    """Get error message for protected path."""
    return (
        f"ERROR: Cannot modify {path} directly.\n\n"
        "User data files are read-only. To update user data:\n"
        "1. Load the skill: load_skill('user-profile')\n"
        "2. Use update_user_data() or remove_user_data()\n\n"
        "See @skills/user-profile/SKILL.md for details."
    )


def create_filesystem_tools(
    sandbox: Any,
    operation_callback: OperationCallback | None = None,
) -> tuple:
    """Factory function to create filesystem tools (Read, Write, Edit).

    Args:
        sandbox: PTCSandbox instance for file operations.
        operation_callback: Optional callback invoked on file operations (write, edit).
                            Receives dict with operation details for persistence/logging.
    """

    def _format_cat_n(lines: list[str], *, start_line_number: int) -> str:
        return "\n".join(f"{i:6}\t{line}" for i, line in enumerate(lines, start=start_line_number))

    @tool
    async def read_file(file_path: str, offset: int | None = None, limit: int | None = None) -> str:
        """Read a file with line numbers (cat -n format).

        Args:
            file_path: Path to file (relative or absolute).
            offset: Line offset (0-indexed). Default: 0.
            limit: Maximum number of lines. Default: 2000.

        Returns:
            File contents with line numbers, or ERROR.
        """
        try:
            normalized_path = sandbox.normalize_path(file_path)
            logger.info("Reading file", file_path=file_path, normalized_path=normalized_path, offset=offset, limit=limit)

            if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            start_offset = offset or 0
            max_lines = limit or 2000

            if offset is not None or limit is not None:
                content = await sandbox.aread_file_range(normalized_path, start_offset, max_lines)
            else:
                content = await sandbox.aread_file_text(normalized_path)

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

    @tool
    async def write_file(file_path: str, content: str) -> str:
        """Write content to a file. Overwrites existing."""
        try:
            normalized_path = sandbox.normalize_path(file_path)
            logger.info("Writing file", file_path=file_path, normalized_path=normalized_path, size=len(content))

            # Check for protected user data paths
            if _is_protected_user_path(normalized_path):
                logger.warning("Blocked write to protected user path", file_path=file_path)
                return _get_protection_error(file_path)

            if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            success = await sandbox.awrite_file_text(normalized_path, content)
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
            virtual_path = sandbox.virtualize_path(normalized_path)
            return f"Wrote {bytes_written} bytes to {virtual_path}"

        except Exception as e:
            error_msg = f"Failed to write file: {e!s}"
            logger.error(error_msg, file_path=file_path, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}"

    @tool
    async def edit_file(file_path: str, old_string: str, new_string: str, replace_all: bool = False) -> str:
        """Replace exact string in a file. Must Read file first."""
        try:
            normalized_path = sandbox.normalize_path(file_path)
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

            if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            result = await sandbox.aedit_file_text(normalized_path, old_string, new_string, replace_all=replace_all)
            if not result.get("success", False):
                error_msg = result.get("error", "Edit operation failed")
                return f"ERROR: {error_msg}"

            # Invoke operation callback for persistence
            if operation_callback:
                try:
                    content = await sandbox.aread_file_text(normalized_path)
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
