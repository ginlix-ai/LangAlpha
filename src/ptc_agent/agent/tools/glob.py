"""Glob tool for file pattern matching."""

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends import FilesystemBackend

logger = structlog.get_logger(__name__)

# Hard cap on paths returned to the model. A recursive glob over a project with a
# dependency tree (node_modules, etc.) can match tens of thousands of files; without
# a cap the single tool message can exceed every model's context window. Matches are
# backend-sorted (sandbox: most-recent first), so the head is the most useful slice.
# Kept well under LargeResultEvictionMiddleware's threshold, which now also backstops
# this tool if the cap is ever raised.
GLOB_MATCH_LIMIT = 1000


def create_glob_tool(backend: FilesystemBackend) -> BaseTool:
    """Factory function to create Glob tool.

    Args:
        backend: Rich-method filesystem backend (``SandboxBackend`` or
            ``CompositeFilesystemBackend``).

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
            normalized_path = backend.normalize_path(search_path)

            logger.info("Globbing files", pattern=pattern, path=search_path, normalized_path=normalized_path)

            # Validate normalized path
            if backend.filesystem_config.enable_path_validation and not backend.validate_path(normalized_path):
                error_msg = f"Access denied: {search_path} is not in allowed directories"
                logger.error(error_msg, path=search_path)
                return f"ERROR: {error_msg}"

            matches = await backend.aglob_paths(pattern, normalized_path)

            if not matches:
                logger.info("No files found", pattern=pattern, path=search_path)
                return f"No files matching pattern '{pattern}' found in '{search_path}'"

            # Cap the number of paths returned to the model (see GLOB_MATCH_LIMIT),
            # virtualizing only the shown slice so a pathological match set doesn't pay
            # full virtualization cost before truncation. The header keeps the true total.
            total = len(matches)
            truncated = total > GLOB_MATCH_LIMIT
            shown = [backend.virtualize_path(m) for m in matches[:GLOB_MATCH_LIMIT]]

            lines = [f"Found {total} file(s) matching '{pattern}':", *shown]
            if truncated:
                lines.append(
                    f"\n[showing first {GLOB_MATCH_LIMIT} of {total} matches — narrow "
                    f"the pattern or pass a subdirectory path to see the rest]"
                )
            result = "\n".join(lines)

            logger.info(
                "Glob completed successfully",
                pattern=pattern,
                path=search_path,
                matches=total,
                truncated=truncated,
            )

            return result.rstrip()

        except Exception as e:
            error_msg = f"Failed to glob files: {e!s}"
            logger.error(error_msg, pattern=pattern, path=search_path, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}"

    return glob
