"""Grep tool for content searching with ripgrep."""

import re
from typing import Any, Literal

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)


def create_grep_tool(sandbox: Any) -> BaseTool:
    """Factory function to create Grep tool.

    Args:
        sandbox: PTCSandbox instance

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
            type: File type filter (e.g., "py", "js") - matches rg --type
            i: Case insensitive search
            n: Show line numbers in output
            A: Lines to show after each match
            B: Lines to show before each match
            C: Lines of context (before and after)
            multiline: Pattern spans multiple lines
            head_limit: Limit number of results
            offset: Skip first N results

        Returns:
            Search results or ERROR
        """
        search_path = path if path is not None else "."
        try:
            # Validate regex pattern to prevent crashes from malformed patterns
            try:
                re.compile(pattern)
            except re.error as e:
                error_msg = f"Invalid regex pattern: {e}"
                logger.error(error_msg, pattern=pattern)
                return f"ERROR: {error_msg}"

            # Normalize virtual path to absolute sandbox path
            normalized_path = sandbox.normalize_path(search_path)

            logger.info(
                "Grepping content",
                pattern=pattern,
                path=search_path,
                normalized_path=normalized_path,
                output_mode=output_mode,
                glob=glob,
                type=type,
                case_insensitive=i,
            )

            # Validate normalized path
            if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(normalized_path):
                error_msg = f"Access denied: {search_path} is not in allowed directories"
                logger.error(error_msg, path=search_path)
                return f"ERROR: {error_msg}"

            # Build grep options with normalized path
            options = {
                "pattern": pattern,
                "path": normalized_path,
                "output_mode": output_mode,
                "glob": glob,
                "type": type,
                "case_insensitive": i,
                "show_line_numbers": n,
                "lines_after": A,
                "lines_before": B,
                "lines_context": C,
                "multiline": multiline,
                "head_limit": head_limit,
                "offset": offset,
            }

            results = await sandbox.agrep_content(**options)

            if not results:
                logger.info("No matches found", pattern=pattern, path=search_path)
                return f"No matches found for pattern '{pattern}' in '{search_path}'"

            # Format output based on mode, virtualizing paths for agent
            if output_mode == "files_with_matches":
                result = f"Found matches in {len(results)} file(s):\n"
                for file_path in results:
                    virtual_path = sandbox.virtualize_path(file_path)
                    result += f"{virtual_path}\n"
            elif output_mode == "content":
                result = f"Matches for pattern '{pattern}':\n\n"
                for entry in results:
                    # Content entries may contain file paths - virtualize them
                    # Format is typically "filepath:line:content" or just content
                    if isinstance(entry, str) and ":" in entry:
                        parts = entry.split(":", 2)
                        if len(parts) >= 2:
                            virtual_path = sandbox.virtualize_path(parts[0])
                            entry = ":".join([virtual_path, *parts[1:]])
                    result += f"{entry}\n"
            elif output_mode == "count":
                result = f"Match counts for pattern '{pattern}':\n"
                for file_path, count in results:
                    virtual_path = sandbox.virtualize_path(file_path)
                    result += f"{virtual_path}: {count}\n"
            else:
                result = str(results)

            logger.info(
                "Grep completed successfully",
                pattern=pattern,
                path=search_path,
                output_mode=output_mode,
                results_count=len(results),
            )

            return result.rstrip()

        except Exception as e:
            error_msg = f"Failed to grep content: {e!s}"
            logger.error(
                error_msg,
                pattern=pattern,
                path=search_path,
                error=str(e),
                exc_info=True,
            )
            return f"ERROR: {error_msg}"

    return grep
