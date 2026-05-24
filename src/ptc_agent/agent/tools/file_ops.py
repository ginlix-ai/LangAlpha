"""Read, Write, and Edit tool factories for the agent filesystem backend."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import structlog
from langchain_core.tools import tool

from ptc_agent.agent.backends import FilesystemBackend, ReadOnlyStoreError
from src.server.services.user_data_io import UserDataValidationError

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

# Read tool defaults. The line cap covers the common case; the char cap is the
# floor that catches files with very long lines (OCR markdown, minified JSON)
# that would otherwise sneak past the line cap. The char cap matches
# `LargeResultEvictionMiddleware`'s 40k-token/~160KB budget so a Read result
# can never single-handedly bust the context window the eviction middleware
# is otherwise responsible for protecting.
_DEFAULT_READ_LIMIT = 2000
_MAX_READ_CHARS = 160_000


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

        Output is capped to protect the context window: at most ``limit`` lines
        (default 2000) and at most ~160k characters of formatted output. If
        either cap fires, the result ends with a marker telling you how to
        continue with a follow-up Read.

        Args:
            file_path: Path to file (relative or absolute), or image/PDF URL.
            offset: Line offset (0-indexed). Default: 0. Ignored for images/PDFs.
            limit: Maximum number of lines. Default: 2000. Ignored for images/PDFs.

        Returns:
            File contents with line numbers, document loading confirmation, or ERROR.
        """
        try:
            # Middleware injects the actual content; this return is just a sentinel.
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
            max_lines = limit or _DEFAULT_READ_LIMIT

            content = await backend.aread_range(normalized_path, start_offset, max_lines)

            if content is None:
                error_msg = f"File not found: {file_path}"
                logger.warning(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            lines = content.splitlines()
            formatted = _format_cat_n(lines, start_line_number=start_offset + 1)

            if len(formatted) > _MAX_READ_CHARS:
                # Reserve room for the truncation marker. The single-line
                # overflow path embeds ``file_path`` three times (narrative +
                # head -c + sed -n), so the budget has to cover ~200 chars of
                # static text plus 3*path. 1200 covers paths up to ~320 chars,
                # which is past the POSIX 255-char limit for any realistic
                # filesystem.
                marker_budget = 1200
                content_budget = max(_MAX_READ_CHARS - marker_budget, 0)

                # Clip to the last newline boundary inside the budget so we
                # report a line range the agent actually saw end-to-end. If
                # the first line itself is larger than the budget, there is
                # no clean cut: we keep one (truncated) line, mark it, and
                # tell the agent it must page with offset/limit to see more.
                clipped = formatted[:content_budget]
                last_nl = clipped.rfind("\n")
                if last_nl >= 0:
                    clipped = clipped[:last_nl]
                    visible_lines = clipped.count("\n") + 1
                    single_line_overflow = False
                else:
                    visible_lines = 1
                    single_line_overflow = True

                # The 0-indexed offset for the next Read call and the
                # 1-indexed last-visible-line number coincide: the last
                # shown line is start_offset + visible_lines (because the
                # off-by-one from 1-indexing cancels against the off-by-one
                # from "lines shown"). One variable, two uses.
                next_line = start_offset + visible_lines

                if single_line_overflow:
                    # Read is line-based; calling Read again with the same
                    # offset would just return the same overflowing line. The
                    # only real escape is a byte-level slice via bash.
                    # Report the offending line's size only — `len(formatted)`
                    # would include any trailing lines that fit after the
                    # huge first line and overstate the agent's slice target.
                    line_size = len(lines[0]) if lines else 0
                    slice_budget = _MAX_READ_CHARS // 20  # ~8 KB chunks
                    marker = (
                        f"\n\n[Read truncated: line {start_offset + 1} of '{file_path}' "
                        f"is ~{line_size} characters, exceeds the {_MAX_READ_CHARS}-character "
                        f"context budget. Read won't help here (it's line-based). Use bash to "
                        f"slice the file, e.g. `head -c {slice_budget} '{file_path}'` or "
                        f"`sed -n '{start_offset + 1}p' '{file_path}' | head -c {slice_budget}`.]"
                    )
                else:
                    marker = (
                        f"\n\n[Read truncated at {_MAX_READ_CHARS} characters to protect the context window. "
                        f"You saw lines {start_offset + 1}..{next_line}. "
                        f"Call Read(file_path='{file_path}', offset={next_line}, limit={max_lines}) "
                        "to continue, or pass a smaller limit to keep each chunk shorter.]"
                    )
                formatted = clipped + marker

            return formatted

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
            except UserDataValidationError as exc:
                logger.info("user-data write rejected", file_path=file_path, error=str(exc))
                return f"ERROR: {exc}"
            if not success:
                return "ERROR: Write operation failed"

            if operation_callback:
                try:
                    operation_callback({
                        "operation": "write_file",
                        "file_path": normalized_path,
                        "line_count": content.count("\n") + 1,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "content": content,
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

            if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                error_msg = f"Access denied: {file_path} is not in allowed directories"
                logger.error(error_msg, file_path=file_path)
                return f"ERROR: {error_msg}"

            result = await backend.aedit_text(normalized_path, old_string, new_string, replace_all=replace_all)
            if not result.get("success", False):
                error_msg = result.get("error", "Edit operation failed")
                return f"ERROR: {error_msg}"

            if operation_callback:
                try:
                    content = await backend.aread_text(normalized_path)
                    operation_callback({
                        "operation": "edit_file",
                        "file_path": normalized_path,
                        "occurrences": result.get("occurrences", 1),
                        "replace_all": replace_all,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "old_string": old_string,
                        "new_string": new_string,
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
