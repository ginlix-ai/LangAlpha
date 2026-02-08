"""Glob tool for file pattern matching."""

from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)


def create_glob_tool(sandbox: Any) -> BaseTool:
    """Factory function to create Glob tool.

    Args:
        sandbox: PTCSandbox instance

    Returns:
        Configured Glob tool function
    """

    @tool("Glob")
    async def glob(pattern: str, path: str | None = None) -> str:
        """Find files matching a glob pattern.

        Use for: Finding files by name. For content search, use Grep.

        Args:
            pattern: Glob pattern (e.g., "**/*.py", "*.{js,ts}")
            path: Search directory (default: current directory)

        Returns:
            Matching file paths sorted by modification time, or ERROR
        """
        search_path = path if path is not None else "."
        try:
            # Normalize virtual path to absolute sandbox path
            normalized_path = sandbox.normalize_path(search_path)

            logger.info("Globbing files", pattern=pattern, path=search_path, normalized_path=normalized_path)

            # Validate normalized path
            if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(normalized_path):
                error_msg = f"Access denied: {search_path} is not in allowed directories"
                logger.error(error_msg, path=search_path)
                return f"ERROR: {error_msg}"

            matches = await sandbox.aglob_files(pattern, normalized_path)

            if not matches:
                logger.info("No files found", pattern=pattern, path=search_path)
                return f"No files matching pattern '{pattern}' found in '{search_path}'"

            # Virtualize paths in output (strip /home/daytona prefix)
            virtual_matches = [sandbox.virtualize_path(m) for m in matches]

            # Format output with virtual paths
            result = f"Found {len(virtual_matches)} file(s) matching '{pattern}':\n"
            for match in virtual_matches:
                result += f"{match}\n"

            logger.info(
                "Glob completed successfully",
                pattern=pattern,
                path=search_path,
                matches=len(virtual_matches),
            )

            return result.rstrip()

        except Exception as e:
            error_msg = f"Failed to glob files: {e!s}"
            logger.error(error_msg, pattern=pattern, path=search_path, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}"

    return glob
