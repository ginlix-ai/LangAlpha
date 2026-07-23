"""Sandbox filesystem ops — read/write/upload/glob/grep and path mapping.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import base64
import shlex
import textwrap
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import structlog

from src.observability import (
    safe_record,
    workspace_fs_bytes,
)

from ptc_agent.core.paths import ALWAYS_HIDDEN_DIR_NAMES
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    SandboxTransientError,
)

from ptc_agent.core.sandbox._shared import (
    _entry_name,
    _entry_is_dir,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


def _normalize_search_path(sandbox: "PTCSandbox", path: str) -> str:
    """Normalize search path to absolute sandbox path.

        Converts relative/virtual paths to absolute paths for search operations.

        Args:
            path: Path to normalize (".", relative, or absolute)

        Returns:
            Absolute sandbox path
        """
    if path == ".":
        return sandbox._work_dir
    if not path.startswith("/"):
        return f"{sandbox._work_dir}/{path}"
    return path


async def adownload_file_bytes(sandbox: "PTCSandbox", filepath: str) -> bytes | None:
    """Download raw bytes from sandbox.

        This path is safe to retry automatically. Concurrency is bounded by a
        semaphore to limit event-loop pressure from concurrent downloads.

        Returns:
            Bytes if downloaded, or None if missing.

        Raises:
            SandboxTransientError: If a transient sandbox transport error persists.
        """
    await sandbox._wait_ready()

    try:
        async with sandbox._download_semaphore:
            result = await sandbox._runtime_call(
                sandbox.runtime.download_file,
                filepath,
                retry_policy=RetryPolicy.SAFE,
            )
        if result:
            safe_record(workspace_fs_bytes, len(result), {"op": "read"})
        return result
    except SandboxTransientError:
        raise
    except Exception as e:
        logger.debug(
            "Failed to download file bytes", filepath=filepath, error=str(e)
        )
        return None


async def aread_file_text(sandbox: "PTCSandbox", filepath: str) -> str | None:
    """Read a UTF-8 text file from the sandbox.

        This path is safe to retry automatically.
        """
    content_bytes = await sandbox.adownload_file_bytes(filepath)
    if not content_bytes:
        return None
    try:
        return content_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        logger.debug(
            "Failed to decode file as utf-8", filepath=filepath, error=str(e)
        )
        return None


async def aupload_file_bytes(sandbox: "PTCSandbox", filepath: str, content: bytes) -> bool:
    """Upload raw bytes to the sandbox.

        This path is safe to retry automatically because uploads overwrite the target.

        Raises:
            SandboxTransientError: If a transient sandbox transport error persists.
        """
    await sandbox._wait_ready()

    # Normalize the path to ensure it's absolute for the sandbox runtime
    normalized_path = sandbox.normalize_path(filepath)

    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        normalized_path
    ):
        logger.error(f"Access denied: {filepath} is not in allowed directories")
        return False

    try:
        assert sandbox.runtime is not None
        # Use normalized path for upload - runtime expects absolute paths
        await sandbox._runtime_call(
            sandbox.runtime.upload_file,
            content,
            normalized_path,
            retry_policy=RetryPolicy.SAFE,
        )
        safe_record(workspace_fs_bytes, len(content), {"op": "write"})
        return True
    except SandboxTransientError:
        raise
    except Exception as e:
        logger.debug(
            "Failed to upload file bytes",
            filepath=filepath,
            normalized_path=normalized_path,
            error=str(e),
        )
        return False


async def awrite_file_text(sandbox: "PTCSandbox", filepath: str, content: str) -> bool:
    """Write UTF-8 text to a sandbox file (overwrites).

        This path is safe to retry automatically.
        """
    try:
        return await sandbox.aupload_file_bytes(filepath, content.encode("utf-8"))
    except UnicodeEncodeError as e:
        logger.debug(
            "Failed to encode file as utf-8", filepath=filepath, error=str(e)
        )
        return False


async def aread_file_range(
    sandbox: "PTCSandbox", file_path: str, offset: int = 0, limit: int = 2000
) -> str | None:
    """Read a specific range of lines from a UTF-8 text file.

        Uses sed via process.exec to extract lines server-side, avoiding
        full-file download through the multipart parser hot path.

        Args:
            file_path: Path to the file.
            offset: Line offset (0-indexed).
            limit: Maximum number of lines.
        """
    await sandbox._wait_ready()
    normalized = sandbox.normalize_path(file_path)
    start = max(0, offset)
    start_line = start + 1  # sed is 1-indexed
    end_line = start + limit
    cmd = f"sed -n '{start_line},{end_line}p' {shlex.quote(normalized)}"

    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            cmd,
            timeout=30,
            retry_policy=RetryPolicy.SAFE,
        )
        if result.exit_code != 0:
            return await sandbox._aread_file_range_fallback(file_path, offset, limit)
        return result.stdout or ""
    except SandboxTransientError:
        raise
    except Exception as e:
        logger.debug("Failed to read file range", filepath=file_path, error=str(e))
        return await sandbox._aread_file_range_fallback(file_path, offset, limit)


async def _aread_file_range_fallback(
    sandbox: "PTCSandbox", file_path: str, offset: int, limit: int
) -> str | None:
    """Fallback: download full file and slice (original behavior)."""
    content = await sandbox.aread_file_text(file_path)
    if content is None:
        return None
    lines = content.splitlines()
    start = max(0, offset)
    end = start + limit
    return "\n".join(lines[start:end])


def normalize_path(sandbox: "PTCSandbox", path: str) -> str:
    """Normalize virtual path to absolute sandbox path (input normalization).

        Converts agent's virtual paths to real sandbox paths:
            "/" or "." or "" -> {working_directory}
            "/results/file.txt" -> {working_directory}/results/file.txt
            "data/file.txt" -> {working_directory}/data/file.txt
            "{working_directory}/file.txt" -> unchanged
            "/tmp/file.txt" -> unchanged

        Args:
            path: Virtual or relative path from agent

        Returns:
            Absolute sandbox path
        """
    # Use live working directory (updated by fetch_working_dir)
    work_dir = sandbox._work_dir

    if path in (None, "", ".", "/"):
        return work_dir

    path = path.strip()

    # Already in allowed directories - keep as is (just normalize . and ..)
    for allowed_dir in sandbox.config.filesystem.allowed_directories:
        if path.startswith(allowed_dir):
            return str(Path(path))

    # Virtual absolute path: /foo -> {working_directory}/foo
    if path.startswith("/"):
        return str(Path(f"{work_dir}{path}"))

    # Relative path: foo -> {working_directory}/foo
    return str(Path(f"{work_dir}/{path}"))


def virtualize_path(sandbox: "PTCSandbox", path: str) -> str:
    """Convert real sandbox path to virtual path (output normalization).

        Strips working_directory prefix from paths returned to agent:
            {working_directory}/results/file.txt -> /results/file.txt
            {working_directory}/tools/docs/foo.md -> /tools/docs/foo.md
            /tmp/file.txt -> /tmp/file.txt (unchanged)

        Args:
            path: Absolute sandbox path

        Returns:
            Virtual path for agent consumption
        """
    # Use live working directory (updated by fetch_working_dir)
    work_dir = sandbox._work_dir

    if path.startswith(work_dir + "/"):
        return path[len(work_dir) :]  # Strip prefix, keep leading /
    if path == work_dir:
        return "/"

    return path  # /tmp or other paths unchanged


def validate_path(sandbox: "PTCSandbox", filepath: str) -> bool:
    """Validate if a path is within allowed directories.

        Args:
            filepath: Path to validate (virtual or absolute)

        Returns:
            True if path is allowed, False otherwise
        """
    if not sandbox.config.filesystem.enable_path_validation:
        return True

    # Normalize the path first (handles virtual paths like /results/...)
    normalized_path = sandbox.normalize_path(filepath)

    # Denylist takes priority over allowlist
    for denied_dir in sandbox.config.filesystem.denied_directories:
        if normalized_path == denied_dir or normalized_path.startswith(
            denied_dir + "/"
        ):
            return False

    # Check against allowed directories
    for allowed_dir in sandbox.config.filesystem.allowed_directories:
        # Exact match or path within allowed directory
        if normalized_path == allowed_dir or normalized_path.startswith(
            allowed_dir + "/"
        ):
            return True

    logger.warning(
        "Path validation failed",
        path=filepath,
        normalized_path=normalized_path,
        allowed_dirs=sandbox.config.filesystem.allowed_directories,
    )
    return False


def validate_and_normalize_path(sandbox: "PTCSandbox", path: str) -> tuple[str, str | None]:
    """Normalize path and validate access.

        Combines path normalization and validation into a single operation.

        Args:
            path: Virtual or relative path from agent

        Returns:
            Tuple of (normalized_path, error_message_or_none)
        """
    normalized = sandbox.normalize_path(path)
    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        normalized
    ):
        return normalized, f"Access denied: {path} is not in allowed directories"
    return normalized, None


async def als_directory(sandbox: "PTCSandbox", directory: str = ".") -> list[dict[str, Any]]:
    """List contents of a directory.

        Returns entries as dicts with at least: name, path, is_dir.
        """
    await sandbox._wait_ready()

    try:
        if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
            directory
        ):
            logger.error(
                f"Access denied: {directory} is not in allowed directories"
            )
            return []

        assert sandbox.runtime is not None
        file_infos = await sandbox._runtime_call(
            sandbox.runtime.list_files,
            directory,
            retry_policy=RetryPolicy.SAFE,
        )
        if not file_infos:
            return []

        results: list[dict[str, Any]] = []
        for entry in file_infos:
            name = _entry_name(entry)
            is_dir = _entry_is_dir(entry)
            entry_path = f"{directory}/{name}" if directory != "." else name
            results.append({"name": name, "path": entry_path, "is_dir": is_dir})
        return results
    except Exception as e:
        logger.debug("Error listing directory", directory=directory, error=str(e))
        return []


async def acreate_directory(sandbox: "PTCSandbox", dirpath: str) -> bool:
    """Create a directory in the sandbox."""
    await sandbox._wait_ready()

    try:
        if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
            dirpath
        ):
            logger.error(f"Access denied: {dirpath} is not in allowed directories")
            return False

        assert sandbox.runtime is not None
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"mkdir -p {shlex.quote(dirpath)}",
            retry_policy=RetryPolicy.SAFE,
        )
        return True
    except Exception as e:
        logger.debug("Failed to create directory", dirpath=dirpath, error=str(e))
        return False


async def acreate_directories(sandbox: "PTCSandbox", dirpaths: Iterable[str]) -> bool:
    """Create multiple directories in a single ``mkdir -p`` exec call.

        Much faster than N separate ``acreate_directory`` calls for bulk
        setup (e.g. file restore), collapsing N round-trips into one.
        ``mkdir -p`` is idempotent. Returns False if any validation or
        exec fails; callers can fall back to per-dir creates.
        """
    paths = [p for p in dirpaths if p]
    if not paths:
        return True

    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation:
        for p in paths:
            if not sandbox.validate_path(p):
                logger.error(f"Access denied: {p} is not in allowed directories")
                return False

    try:
        assert sandbox.runtime is not None
        quoted = " ".join(shlex.quote(p) for p in paths)
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"mkdir -p {quoted}",
            retry_policy=RetryPolicy.SAFE,
        )
        return True
    except Exception as e:
        logger.debug(
            "Failed to bulk-create directories",
            count=len(paths),
            error=str(e),
        )
        return False


async def aedit_file_text(
    sandbox: "PTCSandbox",
    filepath: str,
    old_string: str,
    new_string: str,
    *,
    replace_all: bool = False,
) -> dict[str, Any]:
    """Async edit for tools; safe to retry underlying I/O.

        This does not retry the logical edit itself; it only makes file I/O resilient.
        """
    await sandbox._wait_ready()

    try:
        if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
            filepath
        ):
            return {
                "success": False,
                "error": f"Access denied: {filepath} is not in allowed directories",
            }

        content = await sandbox.aread_file_text(filepath)
        if content is None:
            return {"success": False, "error": "File not found"}

        if old_string == new_string:
            return {
                "success": False,
                "error": "old_string and new_string must be different",
            }

        if old_string not in content:
            return {
                "success": False,
                "error": f"old_string not found in file: {filepath}",
            }

        if not replace_all:
            occurrences = content.count(old_string)
            if occurrences > 1:
                return {
                    "success": False,
                    "error": "old_string found multiple times and requires more code context to uniquely identify the intended match",
                }

        updated = (
            content.replace(old_string, new_string)
            if replace_all
            else content.replace(old_string, new_string, 1)
        )

        if updated == content:
            return {"success": False, "error": "Edit produced no changes"}

        write_ok = await sandbox.awrite_file_text(filepath, updated)
        if not write_ok:
            return {"success": False, "error": "Failed to write updated file"}

        return {
            "success": True,
            "message": "File edited successfully",
        }

    except Exception as e:
        logger.debug("Async edit_file failed", filepath=filepath, error=str(e))
        return {"success": False, "error": f"Edit operation failed: {e!s}"}


def _validate_path_allow_denied(sandbox: "PTCSandbox", path: str) -> bool:
    """Validate path against allowlist only (ignores denied_directories).

        Intended for user-initiated inspection flows where we want to keep
        internal directories hidden by default, but still allow explicit access.
        """

    normalized_path = sandbox._normalize_search_path(path)
    for allowed_dir in sandbox.config.filesystem.allowed_directories:
        if normalized_path == allowed_dir or normalized_path.startswith(
            allowed_dir + "/"
        ):
            return True
    return False


async def aglob_files(
    sandbox: "PTCSandbox", pattern: str, path: str = ".", *, allow_denied: bool = False
) -> list[str]:
    """Async glob; safe to retry automatically."""
    await sandbox._wait_ready()

    try:
        if sandbox.config.filesystem.enable_path_validation:
            is_allowed = (
                sandbox._validate_path_allow_denied(path)
                if allow_denied
                else sandbox.validate_path(path)
            )
            if not is_allowed:
                logger.error(f"Access denied: {path} is not in allowed directories")
                return []

        search_path = sandbox._normalize_search_path(path)

        if "**" not in pattern and "/" not in pattern:
            pattern = f"**/{pattern}"

        # Drop dependency/build/cache dirs (node_modules, .git, caches, …) so a
        # recursive glob can't walk a huge dependency tree into the model context.
        # AGENT_SYSTEM_DIRS (.agents, .system) are intentionally NOT in this set,
        # so the agent's own workspace stays visible. __pycache__ is added since
        # paths.py tracks it as a segment rather than a bare dir name.
        excluded_dirs = sorted(ALWAYS_HIDDEN_DIR_NAMES | {"__pycache__"})

        glob_code = textwrap.dedent(f"""\
                import fnmatch
                import glob
                import os

                pattern = {pattern!r}
                search_path = {search_path!r}
                excluded_dirs = set({excluded_dirs!r})

                # Fast path: '**/<tail>' with a basename-only tail — the recursive
                # patterns ('**/*', '**/*.py', …) that would otherwise walk the whole
                # tree. Prune excluded dirs *during* the walk so we never descend into
                # node_modules/.git/etc. instead of enumerating them and filtering
                # afterward. For these patterns this is equivalent to glob's recursive
                # match: a case-sensitive basename match at any non-excluded depth.
                tail = pattern[3:] if pattern.startswith("**/") else None
                if tail is not None and "/" not in tail and "**" not in tail:
                    files = []
                    for dirpath, dirnames, filenames in os.walk(search_path):
                        dirnames[:] = [d for d in dirnames if d not in excluded_dirs]
                        for fn in filenames:
                            if fnmatch.fnmatchcase(fn, tail):
                                full = os.path.join(dirpath, fn)
                                if os.path.isfile(full):
                                    files.append(full)
                else:
                    # General path: exact glob semantics, then drop matches whose
                    # *intermediate* dir components intersect the excluded set — never
                    # the search-root prefix (so globbing directly into an excluded dir
                    # still works) and never the basename (so a regular file that shares
                    # a noise-dir name is not dropped).
                    full_pattern = os.path.join(search_path, pattern)
                    matches = glob.glob(full_pattern, recursive=True, include_hidden=True)
                    files = []
                    for f in matches:
                        if not os.path.isfile(f):
                            continue
                        inner_dirs = os.path.relpath(f, search_path).split(os.sep)[:-1]
                        if not (set(inner_dirs) & excluded_dirs):
                            files.append(f)

                try:
                    files_with_mtime = [(f, os.path.getmtime(f)) for f in files]
                    sorted_files = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
                    for f, _ in sorted_files:
                        print(f)  # noqa: T201
                except OSError:
                    for f in files:
                        print(f)  # noqa: T201
            """)

        encoded_code = base64.b64encode(glob_code.encode()).decode()
        cmd = f"python3 -c \"import base64; exec(base64.b64decode('{encoded_code}').decode())\""

        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            cmd,
            timeout=30,
            retry_policy=RetryPolicy.SAFE,
        )

        output = result.stdout.strip() if result.stdout else ""
        if not output:
            return []
        return output.split("\n")

    except Exception as e:
        logger.warning(
            "Async glob failed", pattern=pattern, path=path, error=str(e)
        )
        return []


async def agrep_content(
    sandbox: "PTCSandbox",
    pattern: str,
    path: str = ".",
    output_mode: str = "files_with_matches",
    glob: str | None = None,
    type: str | None = None,  # noqa: A002 - matches ripgrep's --type flag
    *,
    case_insensitive: bool = False,
    show_line_numbers: bool = True,
    lines_after: int | None = None,
    lines_before: int | None = None,
    lines_context: int | None = None,
    multiline: bool = False,
    head_limit: int | None = None,
    offset: int = 0,
) -> Any:
    """Async ripgrep; safe to retry automatically."""
    await sandbox._wait_ready()

    try:
        if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
            path
        ):
            logger.error(f"Access denied: {path} is not in allowed directories")
            return []

        cmd = ["rg"]
        if output_mode == "files_with_matches":
            cmd.append("-l")
        elif output_mode == "count":
            cmd.append("-c")

        if case_insensitive:
            cmd.append("-i")

        if output_mode == "content" and show_line_numbers:
            cmd.append("-n")

        if lines_before:
            cmd.extend(["-B", str(lines_before)])
        if lines_after:
            cmd.extend(["-A", str(lines_after)])
        if lines_context:
            cmd.extend(["-C", str(lines_context)])

        if multiline:
            cmd.extend(["-U", "--multiline-dotall"])

        if glob:
            cmd.extend(["--glob", glob])
        if type:
            cmd.extend(["--type", type])

        cmd.append(pattern)
        search_path = sandbox._normalize_search_path(path)
        cmd.append(search_path)

        cmd_str = " ".join(shlex.quote(c) for c in cmd)
        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            cmd_str,
            timeout=60,
            retry_policy=RetryPolicy.SAFE,
        )

        output = result.stdout.strip() if result.stdout else ""
        if not output:
            return []

        if output_mode == "count":
            count_results: list[tuple[str, int]] = []
            for line in output.split("\n"):
                if ":" in line:
                    parts = line.rsplit(":", 1)
                    if len(parts) == 2:
                        try:
                            count_results.append((parts[0], int(parts[1])))
                        except ValueError:
                            count_results.append((line, 0))
                else:
                    count_results.append((line, 0))

            if offset > 0:
                count_results = count_results[offset:]
            if head_limit:
                count_results = count_results[:head_limit]
            return count_results

        results_strs = output.split("\n")
        if offset > 0:
            results_strs = results_strs[offset:]
        if head_limit:
            results_strs = results_strs[:head_limit]
        return results_strs

    except Exception as e:
        logger.debug("Async grep failed", pattern=pattern, path=path, error=str(e))
        return []
