"""Sandbox filesystem ops — read/write/upload/glob/grep and path mapping.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import base64
import hashlib
import shlex
import textwrap
from collections.abc import Iterable
from contextlib import AbstractAsyncContextManager
from typing import Any

import structlog

from src.observability import (
    safe_record,
    workspace_fs_bytes,
)

from ptc_agent.core.paths import ALWAYS_HIDDEN_DIR_NAMES
from ptc_agent.core.sandbox import path_locks as _path_locks
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    RuntimeState,
    SandboxFailureKind,
    SandboxGoneError,
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

_INLINE_TEXT_WRITE_LIMIT = 256 * 1024
_INLINE_TEXT_MARKER = "__LANGALPHA_TEXT_PAYLOAD__"

# States a sandbox can still come back from. ``reconnect`` knows how to start a
# stopped or archived sandbox, so reporting those as Gone would answer a merely
# idle sandbox by deleting it and rebuilding from the last backup — losing
# everything written since. Only an unrecoverable state counts as absence.
#
# ERROR belongs here for that same reason: ``reconnect`` answers an error-state
# sandbox with a recovery ``start`` rather than giving up on it, so calling it
# absence here would destroy a sandbox that path would have revived.
_RECOVERABLE_STATES = frozenset(
    {
        RuntimeState.RUNNING,
        RuntimeState.STARTING,
        RuntimeState.STOPPED,
        RuntimeState.STOPPING,
        RuntimeState.ARCHIVED,
        RuntimeState.ERROR,
    }
)


class SandboxWriteVerificationError(SandboxTransientError):
    """A write did not read back as the bytes written, twice in a row.

    A ``SandboxTransientError`` so the HTTP layer keeps mapping it to 503 and the
    edit path keeps re-raising it instead of answering the agent with "your edit
    was wrong" for a write that never landed.
    """


def _lock_scope(sandbox: "PTCSandbox") -> str:
    """Key that separates one sandbox's path locks from another's."""
    return str(getattr(sandbox, "sandbox_id", None) or "unbound")


def path_write_lock(
    sandbox: "PTCSandbox", normalized_path: str
) -> AbstractAsyncContextManager[None]:
    """Public handle on the per-path write lock, for mutators outside this module.

    A server route that deletes or replaces a file through a shell command
    bypasses the write path entirely, so it has to take the same lock the
    upload path takes or it can land between another writer's read and write.
    """
    return _path_locks.path_write_lock(_lock_scope(sandbox), normalized_path)


def _path_write_lock(
    sandbox: "PTCSandbox", normalized_path: str
) -> AbstractAsyncContextManager[None]:
    return _path_locks.path_write_lock(_lock_scope(sandbox), normalized_path)


async def _remote_size(sandbox: "PTCSandbox", normalized_path: str) -> int | None:
    """Byte count of a sandbox file, or None when the sandbox would not say."""
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"wc -c < {shlex.quote(normalized_path)}",
            timeout=15,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception:  # noqa: BLE001 - an unanswered size falls back to the hash
        return None
    if getattr(result, "exit_code", 1) != 0:
        return None
    try:
        return int((result.stdout or "").strip())
    except ValueError:
        return None


async def _write_landed(
    sandbox: "PTCSandbox", normalized_path: str, expected: bytes
) -> bool:
    """Whether the file now holds the bytes that were just written.

    Sized rather than hashed: a lost, truncated or partially flushed write
    changes the length, and hashing means downloading back every file the agent
    writes, through a sandbox pinned to one CPU. The content hash stays as the
    fallback for a sandbox that cannot answer the size.
    """
    size = await _remote_size(sandbox, normalized_path)
    if size is not None:
        return size == len(expected)
    readback = await sandbox.adownload_file_bytes(normalized_path)
    if readback is None:
        return False
    return hashlib.sha256(readback).digest() == hashlib.sha256(expected).digest()


async def _raise_normalized(
    sandbox: "PTCSandbox", exc: Exception, *, op: str, path: str
) -> None:
    """Re-raise *exc* as a ``Sandbox*`` error, or return if it is confirmed absence.

    The single place this module decides what a failure means. Returning is the
    caller's licence to emit its "not there" sentinel; every other outcome
    raises, because a sentinel that also means "I couldn't reach the sandbox" is
    what turned a replaced sandbox into "File not found — the file has been
    deleted" for files that existed.

    ``SandboxGoneError``/``SandboxTransientError`` are ``RuntimeError`` subclasses,
    so HTTP call sites keep mapping them to 503; raw SDK errors are not, and
    would surface as a 500.
    """
    if isinstance(exc, (SandboxTransientError, SandboxGoneError)):
        raise exc

    kind = sandbox.provider.classify_error(exc)

    if kind is SandboxFailureKind.PATH_ABSENT:
        logger.debug("Sandbox path not found", op=op, filepath=path, error=str(exc))
        return

    if kind is SandboxFailureKind.SANDBOX_GONE:
        raise SandboxGoneError(str(sandbox.sandbox_id or "unknown"), str(exc)) from exc

    if kind is SandboxFailureKind.TRANSIENT:
        raise SandboxTransientError(f"{op} failed on {path}: {exc}") from exc

    # UNKNOWN — undecidable from the exception alone (a deleted sandbox answers a
    # bulk download with a status-less 404). Ask the control plane which it was
    # instead of guessing; this costs a round trip but only on failures we
    # genuinely cannot classify.
    raise await _classify_by_liveness(sandbox, exc, op=op, path=path) from exc


async def _classify_by_liveness(
    sandbox: "PTCSandbox", exc: Exception, *, op: str, path: str
) -> Exception:
    """Decide gone-vs-transient for an unclassifiable failure by probing the runtime.

    Uses ``refresh_state`` deliberately: ``get_state`` reads the SDK's cached
    ``.state`` and would confirm a sandbox that has been deleted for an hour.
    """
    runtime = sandbox.runtime
    sandbox_id = str(sandbox.sandbox_id or "unknown")
    if runtime is None:
        return SandboxTransientError(f"{op} failed on {path} with no runtime: {exc}")

    try:
        state = await runtime.refresh_state()
    except Exception as probe_exc:
        if (
            sandbox.provider.classify_error(probe_exc)
            is SandboxFailureKind.SANDBOX_GONE
        ):
            return SandboxGoneError(sandbox_id, str(exc))
        logger.warning(
            "Could not confirm sandbox liveness after an unclassifiable failure",
            op=op,
            filepath=path,
            sandbox_id=sandbox_id,
            error=str(exc),
            probe_error=str(probe_exc),
        )
        return SandboxTransientError(f"{op} failed on {path}: {exc}")

    if state not in _RECOVERABLE_STATES:
        return SandboxGoneError(sandbox_id, f"state={state.value}: {exc}")
    return SandboxTransientError(f"{op} failed on {path}: {exc}")


async def adownload_file_bytes(sandbox: "PTCSandbox", filepath: str) -> bytes | None:
    """Download raw bytes from sandbox.

    This path is safe to retry automatically. Concurrency is bounded by a
    semaphore to limit event-loop pressure from concurrent downloads.

    Returns:
        Bytes if downloaded, or None if the file genuinely does not exist.

    Raises:
        SandboxGoneError: If the sandbox itself is no longer there.
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
    except Exception as e:
        await _raise_normalized(sandbox, e, op="download_file", path=filepath)
        return None


async def aread_file_text(sandbox: "PTCSandbox", filepath: str) -> str | None:
    """Read a UTF-8 text file from the sandbox.

    This path is safe to retry automatically. ``None`` means absent (or not
    decodable as UTF-8); sandbox failures propagate from
    ``adownload_file_bytes``.
    """
    content_bytes = await sandbox.adownload_file_bytes(filepath)
    if not content_bytes:
        return None
    try:
        return content_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        logger.debug("Failed to decode file as utf-8", filepath=filepath, error=str(e))
        return None


async def aupload_file_bytes(
    sandbox: "PTCSandbox", filepath: str, content: bytes, *, verify: bool = False
) -> bool:
    """Upload raw bytes to the sandbox.

    This path is safe to retry automatically because uploads overwrite the target.
    ``False`` means the path was rejected by validation — a sandbox failure
    raises, since silently reporting "write failed" for an unreachable sandbox
    makes a recoverable outage look like a permission problem.

    Writers of one absolute path are serialised in-process. ``verify``
    checks the landed byte count and rewrites once before giving up; it is
    off by default because bulk restore uploads a whole workspace through
    here and would pay a round trip per file.

    Raises:
        SandboxGoneError: If the sandbox itself is no longer there.
        SandboxTransientError: If a transient sandbox transport error persists.
        SandboxWriteVerificationError: If a verified write never reads back.
    """
    await sandbox._wait_ready()

    # Normalize the path to ensure it's absolute for the sandbox runtime
    normalized_path = sandbox.normalize_path(filepath)

    # Validate the caller's spelling, not the normalizer's output: an absolute
    # path outside the allowed roots is a virtual path by the rule above, so
    # re-normalizing one folds it back inside and reads as allowed while the
    # upload still goes to the path that left.
    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        filepath
    ):
        logger.error(f"Access denied: {filepath} is not in allowed directories")
        return False

    async with _path_write_lock(sandbox, normalized_path):
        for attempt in (1, 2):
            written = await _upload_once(sandbox, filepath, normalized_path, content)
            if not written or not verify:
                return written
            if await _write_landed(sandbox, normalized_path, content):
                return True
            logger.warning(
                "File did not read back as written",
                normalized_path=normalized_path,
                attempt=attempt,
                size=len(content),
            )

    raise SandboxWriteVerificationError(
        f"Write verification failed for {normalized_path}: the file does not match "
        f"the {len(content)} bytes written, after one rewrite"
    )


async def _upload_once(
    sandbox: "PTCSandbox", filepath: str, normalized_path: str, content: bytes
) -> bool:
    """One upload attempt against an already validated, already locked path."""
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
    except Exception as e:
        # A write has no "absent" outcome, so a returned PATH_ABSENT (e.g. a
        # missing parent directory) is still a failure for this caller.
        await _raise_normalized(sandbox, e, op="upload_file", path=normalized_path)
        logger.warning(
            "Failed to upload file bytes",
            filepath=filepath,
            normalized_path=normalized_path,
            error=str(e),
        )
        return False


async def _exec_text_write_once(
    sandbox: "PTCSandbox", filepath: str, normalized_path: str, content: bytes
) -> tuple[bool, int | None]:
    payload = base64.b64encode(content).decode("ascii")
    target = shlex.quote(normalized_path)
    command = "\n".join(
        [
            "set -eu",
            f"target={target}",
            'tmp=$(mktemp "${target}.tmp.XXXXXX")',
            "trap 'rm -f -- \"$tmp\"' EXIT HUP INT TERM",
            f"base64 -d > \"$tmp\" <<'{_INLINE_TEXT_MARKER}'",
            payload,
            _INLINE_TEXT_MARKER,
            'count=$(wc -c < "$tmp")',
            'mv -f -- "$tmp" "$target"',
            "trap - EXIT HUP INT TERM",
            "printf '%s\\n' \"$count\"",
        ]
    )
    try:
        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            command,
            timeout=30,
            retry_policy=RetryPolicy.SAFE,
        )
        if getattr(result, "exit_code", 1) != 0:
            raise RuntimeError(
                f"Atomic text write failed with exit code {result.exit_code}: "
                f"{getattr(result, 'stderr', '')}"
            )
        safe_record(workspace_fs_bytes, len(content), {"op": "write"})
        try:
            return True, int((getattr(result, "stdout", "") or "").strip())
        except ValueError:
            return True, None
    except Exception as e:
        await _raise_normalized(sandbox, e, op="write_file", path=normalized_path)
        logger.warning(
            "Failed to write text file",
            filepath=filepath,
            normalized_path=normalized_path,
            error=str(e),
        )
        return False, None


async def awrite_file_text(sandbox: "PTCSandbox", filepath: str, content: str) -> bool:
    """Write UTF-8 text to a sandbox file (overwrites).

    This path is safe to retry automatically. The write is serialised per
    absolute path and its landed size is checked, so a lost or truncated
    write surfaces instead of being reported as success.
    """
    try:
        encoded = content.encode("utf-8")
    except UnicodeEncodeError as e:
        logger.debug("Failed to encode file as utf-8", filepath=filepath, error=str(e))
        return False

    if len(encoded) >= _INLINE_TEXT_WRITE_LIMIT:
        return await sandbox.aupload_file_bytes(filepath, encoded, verify=True)

    await sandbox._wait_ready()
    normalized_path = sandbox.normalize_path(filepath)
    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        filepath
    ):
        logger.error(f"Access denied: {filepath} is not in allowed directories")
        return False

    async with _path_write_lock(sandbox, normalized_path):
        for attempt in (1, 2):
            written, landed_size = await _exec_text_write_once(
                sandbox, filepath, normalized_path, encoded
            )
            if not written:
                return False
            landed = (
                landed_size == len(encoded)
                if landed_size is not None
                else await _write_landed(sandbox, normalized_path, encoded)
            )
            if landed:
                return True
            logger.warning(
                "File did not read back as written",
                normalized_path=normalized_path,
                attempt=attempt,
                size=len(encoded),
            )

    raise SandboxWriteVerificationError(
        f"Write verification failed for {normalized_path}: the file does not match "
        f"the {len(encoded)} bytes written, after one rewrite"
    )


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
    except Exception as e:
        # The fallback re-reads through ``aread_file_text``, which now raises on
        # sandbox failures — so falling back is only meaningful when the sandbox
        # is actually reachable. Normalize first so an unreachable sandbox
        # surfaces instead of burning a second doomed round trip.
        await _raise_normalized(sandbox, e, op="read_file_range", path=normalized)
        logger.warning("Failed to read file range", filepath=file_path, error=str(e))
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


async def als_directory(
    sandbox: "PTCSandbox", directory: str = ".", *, allow_denied: bool = False
) -> list[dict[str, Any]]:
    """List contents of a directory.

    Returns entries as dicts with at least: name, path, is_dir. An empty list
    means the directory is empty or absent — a sandbox failure raises, because
    "200 with no files" for a broken sandbox reads as a deliberately emptied
    workspace.

    ``allow_denied`` drops the denylist and keeps the allowlist, for host
    machinery reading its own runtime directories (the wrapper sweep). The
    agent's own listing never passes it.
    """
    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation:
        is_allowed = (
            sandbox._validate_path_allow_denied(directory)
            if allow_denied
            else sandbox.validate_path(directory)
        )
        if not is_allowed:
            logger.error(f"Access denied: {directory} is not in allowed directories")
            return []

    try:
        assert sandbox.runtime is not None
        file_infos = await sandbox._runtime_call(
            sandbox.runtime.list_files,
            directory,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        await _raise_normalized(sandbox, e, op="list_files", path=directory)
        return []

    if not file_infos:
        return []

    results: list[dict[str, Any]] = []
    for entry in file_infos:
        name = _entry_name(entry)
        is_dir = _entry_is_dir(entry)
        entry_path = f"{directory}/{name}" if directory != "." else name
        results.append({"name": name, "path": entry_path, "is_dir": is_dir})
    return results


async def acreate_directory(sandbox: "PTCSandbox", dirpath: str) -> bool:
    """Create a directory in the sandbox.

    ``False`` means path validation rejected it; sandbox failures raise.
    """
    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        dirpath
    ):
        logger.error(f"Access denied: {dirpath} is not in allowed directories")
        return False

    try:
        assert sandbox.runtime is not None
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"mkdir -p {shlex.quote(dirpath)}",
            retry_policy=RetryPolicy.SAFE,
        )
        return True
    except Exception as e:
        await _raise_normalized(sandbox, e, op="mkdir", path=dirpath)
        logger.warning("Failed to create directory", dirpath=dirpath, error=str(e))
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
        await _raise_normalized(sandbox, e, op="mkdir_bulk", path=paths[0])
        logger.warning(
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
    Sandbox failures propagate rather than becoming ``{"success": False}`` — the
    agent must not read "the sandbox is unreachable" as "your edit was wrong".

    The read, the replace and the write run under this path's write lock, so a
    second writer on the same file cannot land between the read and the write
    and have its change overwritten.
    """
    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        filepath
    ):
        return {
            "success": False,
            "error": f"Access denied: {filepath} is not in allowed directories",
        }

    try:
        async with _path_write_lock(sandbox, sandbox.normalize_path(filepath)):
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
                # Characters in the file after the edit, so a caller sizing the
                # result against a cap does not have to read the file back.
                "size": len(updated),
            }

    except (SandboxGoneError, SandboxTransientError):
        raise
    except Exception as e:
        logger.warning("Async edit_file failed", filepath=filepath, error=str(e))
        return {"success": False, "error": f"Edit operation failed: {e!s}"}


async def aglob_files(
    sandbox: "PTCSandbox", pattern: str, path: str = ".", *, allow_denied: bool = False
) -> list[str]:
    """Async glob; safe to retry automatically.

    An empty list means no matches. A broken sandbox raises — returning ``[]``
    made "the sandbox is unreachable" indistinguishable from "this workspace
    has no files", which call sites then reported as success.
    """
    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation:
        is_allowed = (
            sandbox._validate_path_allow_denied(path)
            if allow_denied
            else sandbox.validate_path(path)
        )
        if not is_allowed:
            logger.error(f"Access denied: {path} is not in allowed directories")
            return []

    try:
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
        await _raise_normalized(sandbox, e, op="glob", path=path)
        logger.warning("Async glob failed", pattern=pattern, path=path, error=str(e))
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
    """Async ripgrep; safe to retry automatically.

    An empty result means no matches; a broken sandbox raises.
    """
    await sandbox._wait_ready()

    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        path
    ):
        logger.error(f"Access denied: {path} is not in allowed directories")
        return []

    try:
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
        await _raise_normalized(sandbox, e, op="grep", path=path)
        logger.warning("Async grep failed", pattern=pattern, path=path, error=str(e))
        return []
