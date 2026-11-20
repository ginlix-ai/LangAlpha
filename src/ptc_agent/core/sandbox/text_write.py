"""Text writes: one script on the machine that stages, checks and renames.

Built on the upload, verification and failure classification in
:mod:`ptc_agent.core.sandbox.files`, which keeps these names importable for
the sandbox's delegators.
"""

from __future__ import annotations

import base64
import hashlib
import os
import shlex
from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio
import structlog

from src.observability import safe_record, workspace_fs_bytes

from ptc_agent.core.sandbox.file_version import version_line
from ptc_agent.core.sandbox.files import (
    SandboxWriteVerificationError,
    _raise_normalized,
    _upload_once,
    _write_landed,
    path_write_lock,
)
from ptc_agent.core.sandbox.retry import RetryPolicy

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

# The inline write carries the file as base64 inside one shell script, and a
# runtime that runs it as `bash -c <script>` (Docker) passes the script as a
# single argument, which Linux caps at 128 KiB (MAX_ARG_STRLEN): past it the
# exec fails E2BIG, exit 255 with no stderr. Base64 grows the file by 4/3, so
# 64 KiB of text stays well under the cap; larger files take the upload path.
_INLINE_TEXT_WRITE_LIMIT = 64 * 1024
_INLINE_TEXT_MARKER = "__LANGALPHA_TEXT_PAYLOAD__"
# A guarded write that found its file changed, outside sed's and the shell's.
_GUARD_REFUSED_EXIT = 75
# The payload in place before the rename is not the size sent.
_SHORT_PAYLOAD_EXIT = 74
# How many tenths of a second a guarded write waits on the lock before it
# looks at who holds it. A holder only checks and renames, which takes far
# less, so a lock still held then was most likely left by a writer that died.
_GUARD_LOCK_TRIES = 50
# Past this many minutes a lock is taken even from a holder that seems to
# run: by then its process id has most likely been reused, and no save takes
# that long.
_GUARD_LOCK_STALE_MIN = 10
_TARGET = '"$target"'


class WritePreconditionFailed(Exception):
    """A guarded write found its file changed since the caller read it, and
    wrote nothing."""


@dataclass(frozen=True)
class WriteGuard:
    """What a text write checks in its own script on the machine, under a
    lock every guarded write of the file takes, so no other lands between the
    check and the rename.

    The write lands while the file's version line is still ``base_line``
    (``FileStamp.line``) or its bytes still hash to ``base_sha256``; with
    neither, always. The hash is read only once the version has failed, so a
    save of an untouched file reads nothing more. A file that already holds
    the bytes being written passes too, so a retried write is not refused
    over its own first landing.
    """

    base_line: str | None = None
    base_sha256: str | None = None


def _sha256_is(path: str, digest: str) -> str:
    """Shell that passes while the file at ``path`` (a shell word) hashes to
    ``digest``: ``sha256sum`` where coreutils has it, ``shasum`` on a Mac.
    Neither there, it fails, as a file that differs does."""
    return (
        f"{{ __h=$(sha256sum < {path} 2>/dev/null "
        f"|| shasum -a 256 < {path} 2>/dev/null) "
        f'&& [ "${{__h%% *}}" = {shlex.quote(digest)} ]; }}'
    )


def _guard_lock(normalized_path: str) -> str:
    """The lock file guarded writes of one file take on the machine.

    Outside the workspace folder, so a listing or a backup never meets it,
    and named by a digest so any path makes one name.
    """
    key = hashlib.sha256(normalized_path.encode("utf-8")).hexdigest()[:32]
    return f"/tmp/.langalpha-save-{key}"


def _guard_check(guard: WriteGuard) -> list[str]:
    tests = []
    if guard.base_line is not None:
        tests.append(
            f'{{ {version_line(_TARGET)}}} && [ "$v" = {shlex.quote(guard.base_line)} ]'
        )
    if guard.base_sha256 is not None:
        tests.append(_sha256_is(_TARGET, guard.base_sha256))
    if not tests:
        return []
    holds = " || ".join(f"{{ {test}; }}" for test in tests)
    return [
        f'if ! {{ [ -f "$target" ] && {{ {holds}; }}; }}; then',
        f'  cmp -s -- "$tmp" "$target" || exit {_GUARD_REFUSED_EXIT}',
        "fi",
    ]


async def _exec_text_write_once(
    sandbox: "PTCSandbox",
    filepath: str,
    normalized_path: str,
    content: bytes,
    guard: WriteGuard | None = None,
    staged: str | None = None,
) -> tuple[bool, int | None, str]:
    """One write, its landed size, and the file's version line after it.

    The bytes go inline, or were uploaded to ``staged`` beside the target for
    a payload too large to inline; either way the rename is this script's.
    A guarded write holds the file's lock from its check until it has printed
    the version line, so the line is its own write's.
    """
    target = shlex.quote(normalized_path)
    # The lock is released by the same trap, once it is held.
    cleanup = '[ -z "$tmp" ] || rm -f -- "$tmp"'
    if guard is not None:
        cleanup += '; [ -z "$held" ] || rm -rf -- "$held" 2>/dev/null || true'
    trap = f"trap '{cleanup}' EXIT HUP INT TERM"
    if staged is None:
        payload = base64.b64encode(content).decode("ascii")
        staging = [
            'mkdir -p -- "$(dirname -- "$target")"',
            'tmp=$(mktemp "${target}.tmp.XXXXXX")',
            trap,
            f"base64 -d > \"$tmp\" <<'{_INLINE_TEXT_MARKER}'",
            payload,
            _INLINE_TEXT_MARKER,
        ]
    else:
        staging = [
            f"tmp={shlex.quote(staged)}",
            trap,
            'mkdir -p -- "$(dirname -- "$target")"',
        ]
    lock = (
        [
            f"lk={shlex.quote(_guard_lock(normalized_path))}",
            "n=0",
            "tried=",
            # The lock is a file the save creates exclusively (noclobber), not
            # a folder: a uutils mkdir reports success for a folder another run
            # made at the same moment. A script from before keeps its lock as
            # a folder of the same name, and each fails to make the other's.
            'until (set -C && : > "$lk") 2>/dev/null; do',
            "  n=$((n + 1))",
            f'  if [ "$n" -gt {_GUARD_LOCK_TRIES} ]; then',
            # Taken only from a holder that is gone: one still running may
            # be between its check and its rename, and would land over the
            # save that took its lock. A lock with no id yet may be a holder
            # between taking it and writing its id, so it is taken only once
            # old, as is one whose id another process has since reused.
            "    i=$(ls -di -- \"$lk\" 2>/dev/null | awk '{print $1}')",
            '    o=$(cat -- "$lk" 2>/dev/null || cat -- "$lk/pid" 2>/dev/null || true)',
            f'    if [ -z "$(find "$lk" -maxdepth 0 -mmin +{_GUARD_LOCK_STALE_MIN} 2>/dev/null)" ] \\',
            '      && { [ -z "$o" ] || kill -0 "$o" 2>/dev/null; }; then',
            '      echo "another save of this file is still running" >&2',
            "      exit 1",
            "    fi",
            # The holder and its age were read from the lock with inode $i
            # only if that is still the lock: one taken since is looked at
            # afresh, never claimed under the holder read from the last.
            "    if [ \"$(ls -di -- \"$lk\" 2>/dev/null | awk '{print $1}')\" != \"$i\" ]; then",
            "      n=0",
            "      continue",
            "    fi",
            # Of the saves that found this lock's holder gone, the one that
            # claims it (the lock's inode and holder) removes it, and all go
            # back to taking the lock. The claim stays, so a save that read
            # the same holder late cannot remove the lock taken since; one
            # that loses twice gives up. The claim is a file made exclusively,
            # for the same reason as the lock. A script from before finds no
            # id in a file lock and claims it as a folder under holder 0, so
            # a file lock is claimed under 0 too: an older script's folder
            # there means that script has it, and a claim file there is only
            # left from an earlier lock that had this inode, which the save
            # stamps anew so the sweep keeps it from an older script for as
            # long as any claim. A save that finds an older script's claim
            # drops its own, so once that one ages out the next round can
            # claim the lock.
            '    c="$lk.gone.$i.${o:-0}"',
            '    b="$lk.gone.$i.0"',
            "    won=",
            '    if [ -n "$i" ] && (set -C && : > "$c") 2>/dev/null; then',
            '      if [ -d "$lk" ] || (set -C && : > "$b") 2>/dev/null \\',
            '        || { [ -f "$b" ] && touch -c -- "$b" 2>/dev/null && [ -f "$b" ]; }; then',
            "        won=1",
            "      else",
            '        rm -f -- "$c"',
            "      fi",
            "    fi",
            '    if [ -n "$won" ]; then',
            '      rm -rf -- "$lk"',
            '    elif [ -n "$tried" ]; then',
            '      echo "another save of this file is still running" >&2',
            "      exit 1",
            "    fi",
            "    tried=1",
            "    n=0",
            # From the folder's own path: on a Mac /tmp is a link, which find
            # does not follow from its start.
            '    find "$(dirname -- "$lk")/" -maxdepth 1 -name "$(basename -- "$lk").gone.*"'
            f" -mmin +{_GUARD_LOCK_STALE_MIN} -exec rm -rf {{}} + 2>/dev/null || true",
            "  fi",
            "  sleep 0.1 2>/dev/null || sleep 1",
            "done",
            # Held before its id is written, so a write that fails (a full
            # /tmp) lets the trap remove the lock rather than leave one with
            # no id, which no save takes until it is old.
            'held=$lk',
            'echo "$$" > "$lk"',
        ]
        if guard is not None
        else []
    )
    check = _guard_check(guard) if guard is not None else []
    write = [
        'count=$(wc -c < "$tmp")',
        # A payload that arrived short is never renamed in: the retry stages
        # it again under the caller's guard, not one read off a short file.
        f'[ $((count)) -eq {len(content)} ] || exit {_SHORT_PAYLOAD_EXIT}',
        *check,
        # BSD mv (a macOS machine) has no -T. As -T does, refuse a
        # directory, and replace a link to one rather than move into it.
        'if [ "$(uname -s)" = Darwin ]; then',
        '  if [ -d "$target" ]; then',
        '    [ -L "$target" ] || { echo "mv: $target: Is a directory" >&2; exit 1; }',
        '    rm -f -- "$target"',
        "  fi",
        '  mv -f -- "$tmp" "$target"',
        "else",
        '  mv -fT -- "$tmp" "$target"',
        "fi",
    ]
    if staged is not None:
        # A staged payload is gone once renamed into place. The script run
        # again, its first run's answer lost on the way back, finds the file
        # holding these bytes and says so, rather than failing a write that
        # landed.
        landed = _sha256_is(_TARGET, hashlib.sha256(content).hexdigest())
        write = [
            'if [ ! -e "$tmp" ]; then',
            f"  {landed} || exit 1",
            '  count=$(wc -c < "$target")',
            "else",
            *(f"  {line}" for line in write),
            "fi",
        ]
    command = "\n".join(
        [
            "set -eu",
            f"target={target}",
            "tmp=",
            "held=",
            *staging,
            *lock,
            *write,
            # Renamed into place: nothing left for the trap to remove.
            "tmp=",
            "printf '%s\\n' \"$count\"",
            f'{version_line(_TARGET)}printf "%s\\n" "$v"',
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
        exit_code = getattr(result, "exit_code", -1)
        if exit_code < 0:
            # The Docker runtime folds a timeout or an exec exception into
            # exit -1 rather than raising, so this is a transport failure and
            # belongs to the same classification as a raised one.
            raise RuntimeError(
                f"exec did not complete: {getattr(result, 'stderr', '') or exit_code}"
            )
    except Exception as e:
        await _raise_normalized(sandbox, e, op="write_file", path=normalized_path)
        logger.warning(
            "Failed to write text file",
            filepath=filepath,
            normalized_path=normalized_path,
            error=str(e),
        )
        return False, None, ""

    if guard is not None and exit_code == _GUARD_REFUSED_EXIT:
        raise WritePreconditionFailed(normalized_path)
    if exit_code == _SHORT_PAYLOAD_EXIT:
        raise _ShortPayload(normalized_path)
    if exit_code != 0:
        # The sandbox answered, so this is the script failing on the file (a
        # permission, a full disk, a directory in the way), not the runtime
        # failing to reach it. Routing it through the liveness classifier
        # relabels every such error "Sandbox is not reachable".
        logger.warning(
            "Text write script failed",
            filepath=filepath,
            normalized_path=normalized_path,
            exit_code=exit_code,
            stderr=(getattr(result, "stderr", "") or "").strip()[:500],
        )
        return False, None, ""
    safe_record(workspace_fs_bytes, len(content), {"op": "write"})
    count, _, stamp = (getattr(result, "stdout", "") or "").strip().partition("\n")
    try:
        return True, int(count), stamp.strip()
    except ValueError:
        return True, None, stamp.strip()


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
    written, _ = await _write_text(sandbox, filepath, encoded, None)
    return written


async def awrite_file_text_guarded(
    sandbox: "PTCSandbox", filepath: str, content: str, guard: WriteGuard
) -> str | None:
    """``awrite_file_text`` that lands only while ``guard`` holds, checked in
    the script that renames the file into place; the file's version line
    after the write, or None for a path validation refused.

    A payload too large to inline is uploaded beside the target first, so
    the check and the rename stay one script at any size.

    Raises:
        WritePreconditionFailed: The file changed since the caller read it.
    """
    try:
        encoded = content.encode("utf-8")
    except UnicodeEncodeError as e:
        logger.debug("Failed to encode file as utf-8", filepath=filepath, error=str(e))
        return None
    written, stamp = await _write_text(sandbox, filepath, encoded, guard)
    return stamp if written else None


async def _write_text(
    sandbox: "PTCSandbox", filepath: str, encoded: bytes, guard: WriteGuard | None
) -> tuple[bool, str]:
    await sandbox._wait_ready()
    normalized_path = sandbox.normalize_path(filepath)
    if sandbox.config.filesystem.enable_path_validation and not sandbox.validate_path(
        filepath
    ):
        logger.error(f"Access denied: {filepath} is not in allowed directories")
        return False, ""

    async with path_write_lock(sandbox, normalized_path):
        for attempt in (1, 2):
            staged = None
            if len(encoded) >= _INLINE_TEXT_WRITE_LIMIT:
                staged = f"{normalized_path}.tmp.{os.urandom(6).hex()}"
            # Whatever ends the write short of the script's own end, an upload
            # that broke off or a script that never ran, leaves the staged
            # payload beside the file: in the workspace, in its listing.
            consumed = staged is None
            try:
                if staged is not None and not await _upload_once(
                    sandbox, filepath, staged, encoded
                ):
                    return False, ""
                try:
                    written, landed_size, stamp = await _exec_text_write_once(
                        sandbox,
                        filepath,
                        normalized_path,
                        encoded,
                        guard,
                        staged,
                    )
                except WritePreconditionFailed:
                    consumed = True  # The script's trap removed it.
                    raise
                except _ShortPayload:
                    consumed = True  # The script's trap removed it.
                    logger.warning(
                        "Write payload reached the machine short",
                        normalized_path=normalized_path,
                        attempt=attempt,
                        size=len(encoded),
                    )
                    continue
                consumed = written
            finally:
                if not consumed:
                    # A request cancelled after the upload cancels every
                    # await in its scope, this one too, unless shielded.
                    with anyio.CancelScope(shield=True):
                        await _discard_staged(sandbox, staged)
            if not written:
                return False, ""
            landed = (
                landed_size == len(encoded)
                if landed_size is not None
                else await _write_landed(sandbox, normalized_path, encoded)
            )
            if landed:
                return True, stamp
            logger.warning(
                "File did not read back as written",
                normalized_path=normalized_path,
                attempt=attempt,
                size=len(encoded),
            )
            if guard is not None:
                # The rewrite replaces this write's own short landing and
                # nothing else: a save that landed since keeps its version.
                if not stamp:
                    break
                guard = WriteGuard(base_line=stamp)

    raise SandboxWriteVerificationError(
        f"Write verification failed for {normalized_path}: the file does not match "
        f"the {len(encoded)} bytes written, after one rewrite"
    )


class _ShortPayload(Exception):
    """The payload reached the machine short of what was sent, so the script
    renamed nothing into place."""


async def _discard_staged(sandbox: "PTCSandbox", staged: str | None) -> None:
    """Remove a staged payload its write left behind, once: a machine that has
    stopped answering is not waited on for it."""
    if staged is None:
        return
    try:
        assert sandbox.runtime is not None
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"rm -f -- {shlex.quote(staged)}",
            timeout=10,
            retry_policy=RetryPolicy.UNSAFE,
        )
    except Exception as e:
        logger.warning(
            "Staged write payload left behind", staged=staged, error=str(e)
        )
