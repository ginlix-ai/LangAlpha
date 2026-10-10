"""Grep tool for content searching with ripgrep."""

import re
from typing import Literal

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends import FilesystemBackend
from ptc_agent.core.paths import logged_path
from ptc_agent.core.sandbox.grep_render import GrepLine
from src.observability.private_errors import failure

logger = structlog.get_logger(__name__)


def create_grep_tool(backend: FilesystemBackend) -> BaseTool:
    """Factory function to create Grep tool.

    Args:
        backend: Rich-method filesystem backend (``SandboxBackend`` or
            ``CompositeFilesystemBackend``).

    Returns:
        Configured Grep tool function
    """

    @tool("Grep")
    async def grep(
        pattern: str,
        path: str | None = None,
        output_mode: Literal["files_with_matches", "content", "count"] | None = "files_with_matches",
        glob: str | None = None,
        type: str | None = None,  # noqa: A002 - matches ripgrep's --type flag
        i: bool | None = False,
        n: bool | None = True,
        A: int | None = None,
        B: int | None = None,
        C: int | None = None,
        multiline: bool | None = False,
        head_limit: int | None = None,
        offset: int = 0,
    ) -> str:
        """Search file contents using ripgrep regex.

        Use for: Content search in files
        NOT for: bash grep/rg commands

        Args:
            pattern: Regex pattern to search
            path: Directory or file (default: ".")
            output_mode: "files_with_matches" | "content" | "count"
            glob: File filter (e.g., "*.py")
            type: File type filter (e.g., "py", "csv") - matches rg --type
            i: Case insensitive search
            n: Show line numbers in output
            A: Lines to show after each match
            B: Lines to show before each match
            C: Lines of context (before and after)
            multiline: Pattern spans multiple lines
            head_limit: Limit number of results
            offset: Skip first N results

        Returns:
            Matching lines, the list of matching files, or a count — whichever
            output_mode selects.
        """
        search_path = path if path is not None else "."
        try:
            # Validate regex pattern to prevent crashes from malformed patterns
            try:
                re.compile(pattern)
            except re.error as e:
                error_msg = f"Invalid regex pattern: {e}"
                logger.info("Invalid regex pattern", pattern_length=len(pattern))
                return f"ERROR: {error_msg}"

            # Normalize virtual path to absolute sandbox path
            normalized_path = backend.normalize_path(search_path)

            logger.info(
                "Grepping content",
                pattern_length=len(pattern),
                path=logged_path(search_path),
                normalized_path=logged_path(normalized_path),
                output_mode=output_mode,
                glob_length=len(glob) if glob else 0,
                type=type,
                case_insensitive=i,
            )

            # Validate normalized path
            if backend.filesystem_config.enable_path_validation and not backend.validate_path(search_path):
                error_msg = f"Access denied: {search_path} is not in allowed directories"
                logger.error("Access denied: path outside the allowed directories", path=logged_path(search_path))
                return f"ERROR: {error_msg}"

            results = await backend.agrep_rich(
                pattern=pattern,
                path=normalized_path,
                output_mode=output_mode,
                glob=glob,
                type=type,
                case_insensitive=i,
                show_line_numbers=n,
                lines_after=A,
                lines_before=B,
                lines_context=C,
                multiline=multiline,
                head_limit=head_limit,
                offset=offset,
            )

            if not results:
                logger.info("No matches found", path=logged_path(search_path))
                return f"No matches found for pattern '{pattern}' in '{search_path}'"

            # Format output based on mode, virtualizing paths for agent
            if output_mode == "files_with_matches":
                result = f"Found matches in {len(results)} file(s):\n"
                for file_path in results:
                    virtual_path = backend.virtualize_path(file_path)
                    result += f"{virtual_path}\n"
            elif output_mode == "content":
                result = f"Matches for pattern '{pattern}':\n\n"
                for entry in results:
                    # A line names its file by the path it holds, as a path
                    # may hold the `:` or `-` that ends it in the text. A
                    # group's `--` and an error name none.
                    if isinstance(entry, GrepLine):
                        entry = entry.at(backend.virtualize_path(entry.path))
                    result += f"{entry}\n"
            elif output_mode == "count":
                result = f"Match counts for pattern '{pattern}':\n"
                for file_path, count in results:
                    virtual_path = backend.virtualize_path(file_path)
                    result += f"{virtual_path}: {count}\n"
            else:
                result = str(results)

            logger.info(
                "Grep completed successfully",
                path=logged_path(search_path),
                output_mode=output_mode,
                results_count=len(results),
            )

            return result.rstrip()

        except Exception as e:
            error_msg = f"Failed to grep content: {e!s}"
            fields, trace = failure(e)
            logger.error("Failed to grep content", path=logged_path(search_path), exc_info=trace, **fields)
            return f"ERROR: {error_msg}"

    return grep
