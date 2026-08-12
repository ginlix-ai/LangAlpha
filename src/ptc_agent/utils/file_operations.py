"""File operation utilities for state management and persistence.

This module provides utility functions for file operations that are shared
across the codebase, including:
- FileData type definition
- State reducers for file operations
- Conversion functions between database storage and state formats

These utilities were extracted from the deprecated src/tools/core/filesystem/
module to support active code while that module is being cleaned up.
"""

from datetime import UTC, datetime
from typing import List

from typing_extensions import TypedDict


# Constants
MAX_LINE_LENGTH = 2000


class FileData(TypedDict):
    """Data structure for storing file contents with metadata."""

    content: List[str]
    """Lines of the file."""

    created_at: str
    """ISO 8601 timestamp of file creation."""

    modified_at: str
    """ISO 8601 timestamp of last modification."""


def _create_file_data(
    content: str | list[str],
    *,
    created_at: str | None = None,
) -> FileData:
    r"""Create a FileData object with automatic timestamp generation.

    Args:
        content: File content as string or list of lines
        created_at: Optional creation timestamp (ISO 8601)

    Returns:
        FileData with content split into lines and timestamps set
    """
    lines = content.split("\n") if isinstance(content, str) else content
    lines = [line[i:i+MAX_LINE_LENGTH] for line in lines for i in range(0, len(line) or 1, MAX_LINE_LENGTH)]
    now = datetime.now(UTC).isoformat()

    return {
        "content": lines,
        "created_at": created_at or now,
        "modified_at": now,
    }


def _file_data_to_string(file_data: FileData) -> str:
    r"""Convert FileData to plain string content.

    Args:
        file_data: FileData object with content as list of lines

    Returns:
        String with lines joined by newlines
    """
    return "\n".join(file_data["content"])


def string_to_file_data(
    content: str,
    created_at: str | None = None,
    modified_at: str | None = None
) -> FileData:
    """Convert a string to FileData format (list of lines).

    This is the inverse of _file_data_to_string() and is used for
    loading files from database back into state. Ensures bidirectional
    conversion between database storage (string) and state format (list[str]).

    Args:
        content: String content to convert
        created_at: Optional creation timestamp (ISO 8601)
        modified_at: Optional modification timestamp (ISO 8601)

    Returns:
        FileData with content as list[str]

    Example:
        >>> db_content = "Line 1\\nLine 2\\nLine 3"
        >>> file_data = string_to_file_data(db_content)
        >>> file_data['content']
        ['Line 1', 'Line 2', 'Line 3']
        >>> len(file_data['content'])
        3
    """
    # Use _create_file_data for consistency (handles line chunking)
    file_data = _create_file_data(content, created_at=created_at)

    # Override modified_at if provided (created_at is already set by _create_file_data)
    if modified_at:
        file_data['modified_at'] = modified_at

    return file_data
