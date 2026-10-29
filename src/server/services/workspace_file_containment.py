"""Canonical containment for mirrored and sandbox file reads.

A requested path is first folded to one spelling relative to the workspace
folder, whichever form it arrived in. Mirror keys need canonical relative
paths; sandbox checks need realpath because lexical validation misses .. and
symlinks that can expose sibling files.
"""

from __future__ import annotations

import base64
import binascii
import logging
import posixpath
import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import unquote
from uuid import uuid4

from ptc_agent.core.paths import SANDBOX_ROOTS, SandboxLayout, strip_previous_dir_name
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.utils.error_sanitization import single_line

logger = logging.getLogger(__name__)

# Distinct denial codes distinguish escapes from resolution failures in logs.
PROBE_UNRESOLVABLE = 3
PROBE_ESCAPED = 4

# Deny slow probes rather than holding a serving connection on a sick sandbox.
_PROBE_TIMEOUT_S = 10
READ_MISSING = 2
READ_CONTAINMENT = 3
READ_TOO_LARGE = 5
# A ranged read whose offset is at or past the file's end.
READ_UNSATISFIABLE = 6
# The read returns the file as base64 on the exec's stdout. Daytona fails an
# exec whose output passes ~128 MiB (a ~95 MiB file) with a 502 and stops the
# sandbox, so a read stays well under that; larger files go through the
# provider's file download instead.
EXEC_READ_MAX_BYTES = 64 * 1024 * 1024
# Room in an exec's output for the path line ahead of the file's base64.
_EXEC_READ_HEADROOM = 64 * 1024


def _exec_read_max(runtime: Any) -> int:
    """The largest file a whole read prints within ``runtime``'s exec output.

    An SSH exec keeps its first 32 MiB, so a file between that and
    ``EXEC_READ_MAX_BYTES`` would arrive cut, fail to decode and answer as
    missing. ``base64`` prints 4 characters per 3 bytes, 76 to a line.
    """
    cap = getattr(runtime, "exec_output_cap", None)
    if not isinstance(cap, int):
        return EXEC_READ_MAX_BYTES
    fits = (cap - _EXEC_READ_HEADROOM) * 3 * 76 // (4 * 77)
    return min(EXEC_READ_MAX_BYTES, fits)


# ``realpath -m`` for a machine without it whose ``readlink -f`` needs every
# component to exist (macOS), where a path not yet written would read as
# unresolvable: links are followed wherever they lead, even dangling, as -m
# does; the directory that exists is resolved by the OS (the external pwd,
# which spells it as the disk does, so a case-insensitive disk cannot pass a
# deny list in other case); the missing rest is appended. A link loop fails.
_RESOLVE_SH = (
    "__la_rp() { p=$1; tail=; n=0; case $p in /*) ;; *) return 1 ;; esac; "
    "while :; do "
    'if [ -L "$p" ]; then n=$((n + 1)); [ "$n" -le 40 ] || return 1; '
    'l=$(readlink -- "$p") || return 1; case $l in /*) p=$l ;; *) p=${p%/*}/$l ;; esac; '
    'elif [ -d "$p" ]; then d=$(cd -P -- "$p" && env pwd -P) || return 1; '
    'r=${d%/}$tail; printf %s "${r:-/}"; return 0; '
    "else s=${p##*/}; case $s in .|..) return 1 ;; '') p=${p%/} ;; "
    "*) tail=/$s$tail; p=${p%/*}; [ -n \"$p\" ] || p=/ ;; esac; fi; done; }; "
)


# How much of a file's start decides whether a range of it may go unredacted.
UTF8_SNIFF_BYTES = 8192
# A ranged read's version line: size, mtime, ctime and inode from one stat,
# GNU's nanoseconds, then its seconds, then BSD's fraction. Each attempt fills
# ``v`` on its own, since a failed one can still print (GNU reads ``-f`` as file
# system status). ``wc`` is the last resort, for the size alone, as BusyBox's
# reads every byte; BSD's pads it with spaces.
_VERSION_LINE = (
    "v=$(stat -c '%s %.9Y %.9Z %i' -- \"$tt\" 2>/dev/null) "
    "|| v=$(stat -c '%s %Y %Z %i' -- \"$tt\" 2>/dev/null) "
    "|| v=$(stat -f '%z %Fm %Fc %i' -- \"$tt\" 2>/dev/null) "
    "|| v=$(wc -c < \"$tt\" | tr -d ' '); "
)
_TIMESTAMP = re.compile(r"\d+(\.\d+)?", re.ASCII)
# How far the sandbox clock must have left a file's ctime behind before no
# write still to come can share it. A write moves the ctime only by the clock's
# step, a kernel tick or a whole second; the second beyond that covers a
# filesystem clock a little off the sandbox's.
_VERSION_MIN_AGE_S = 2


class FileChangedWhileRead(Exception):
    """A ranged read whose file was rewritten between its size and its bytes,
    so no Content-Range could describe what was read. A retry reads it anew."""


class FileTooLargeToServe(Exception):
    """The file is contained but too large to read through an exec.

    Carries the canonical path and size the same exec resolved, so a caller
    that can fetch the file another way reads what the check approved.
    """

    def __init__(self, canonical: str, size: int) -> None:
        super().__init__(canonical)
        self.canonical = canonical
        self.size = size


@dataclass(frozen=True)
class ContainedSlice:
    """Bytes of a contained file from an offset, with its size, its version and
    its first bytes, all from the one exec, so the range, its validator and
    whether it is text describe one file.

    ``content`` is None when the offset is at or past the end, and ``version``
    None when the file has none that tells it from its next write.
    """

    canonical: str
    size: int
    version: str | None
    head: bytes
    content: bytes | None


_FILE_URL_SCHEME = "file://"


def _file_url_path(raw: str) -> str:
    """The local path a ``file:`` URL names, percent-decoded.

    Transcript links carry this spelling. The scheme says the rest is an
    absolute path however many slashes followed it, so an authority-shaped
    remainder (``file://home/...``) folds to the same path as
    ``file:///home/...``. Decoding happens before the escape checks, so an
    encoded ``..`` is refused rather than smuggled through as a literal segment.
    """
    if raw[: len(_FILE_URL_SCHEME)].lower() != _FILE_URL_SCHEME:
        return raw
    rest = unquote(raw[len(_FILE_URL_SCHEME) :])
    if rest.lower().startswith("localhost/"):
        rest = rest[len("localhost") :]
    return rest if rest.startswith("/") else f"/{rest}"


def _fold_relative(path: str) -> str:
    """Drop the ``./`` and trailing-slash noise a relative spelling carries."""
    out = path
    while out.startswith("./"):
        out = out[2:]
    out = out.rstrip("/")
    return "" if out == "." else out


def _known_roots(work_dir: str) -> tuple[str, ...]:
    """Absolute prefixes a requested path may carry, most specific first.

    The workspace folder sorts ahead of the computer root it sits on because it
    is the longer string, and that ordering is what decides the one ambiguous
    spelling: ``<work_dir>/<dir_name>/x`` reads as this folder's own
    subdirectory rather than as a root-level folder named after the workspace.
    """
    roots = {work_dir.rstrip("/"), *(root.rstrip("/") for root in SANDBOX_ROOTS)}
    return tuple(sorted((r for r in roots if r), key=lambda r: (-len(r), r)))


def workspace_relative_path(
    path: str,
    work_dir: str,
    previous_dir_names: Sequence[str] = (),
    *,
    previous_roots: Sequence[str] = (),
) -> str:
    """Every spelling of a requested path folded to one ``work_dir``-relative form.

    A path arrives relative to the workspace, under the workspace folder, under
    a computer root that folder sits on (the layout before the split spelled
    every path that way, and transcripts still hold those), under a folder the
    workspace was renamed out of, or as a ``file:`` URL of any of these. The
    sweep that split the root physically moved those entries into the folder,
    and a rename moves the folder, so each spelling names the same file the
    folder spelling does. So does a move, for the folder spelled under a root
    in ``previous_roots`` (most specific first) by any of its names.

    A leading slash survives a path no root claimed: whether that names a
    workspace file or nothing at all is the caller's policy, not this fold's.
    """
    raw = _file_url_path((path or "").strip()).replace("\\", "/")
    folder = work_dir.rstrip("/")
    if raw != folder and not raw.startswith(f"{folder}/"):
        # Another folder under a root left behind was a sibling's, so only
        # this folder's own names fold there.
        names = (posixpath.basename(folder), *previous_dir_names)
        for root in previous_roots:
            if raw.startswith(f"{root}/"):
                inside = strip_previous_dir_name(
                    _fold_relative(raw[len(root) + 1 :]), names
                )
                if inside is not None:
                    return inside
    for root in _known_roots(work_dir):
        if raw == root:
            return ""
        if raw.startswith(f"{root}/"):
            relative = _fold_relative(raw[len(root) + 1 :])
            if root != folder:
                inside = strip_previous_dir_name(relative, previous_dir_names)
                if inside is not None:
                    return inside
            return relative
    # A former folder sat beside this one, on the computer's root, which a
    # configured working directory puts outside the stock roots.
    beside = posixpath.dirname(folder).rstrip("/")
    if previous_dir_names and raw.startswith(f"{beside}/"):
        inside = strip_previous_dir_name(
            _fold_relative(raw[len(beside) + 1 :]), previous_dir_names
        )
        if inside is not None:
            return inside
    return raw if raw.startswith("/") else _fold_relative(raw)


def normalize_requested_path(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str:
    """The folded path, reading a slash no root claimed as the workspace root.

    That is the client's virtual-absolute spelling, so one leading slash comes
    off and only one: ``//etc/passwd`` stays absolute and the containment check
    that follows refuses it. ``clean_path`` refuses the same spelling outright,
    because its callers glob what it returns.
    """
    relative = workspace_relative_path(path, work_dir, previous_dir_names)
    if relative.startswith("/"):
        return _fold_relative(relative[1:])
    return relative


def contained_relative_path(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str | None:
    """Canonicalize before mirror lookup; sandbox lexical normalization misses .. and symlinks."""
    # The NUL check reads the folded path, not the request: a ``file:`` URL
    # spells one percent-encoded, and the fold is where it decodes.
    normalized = normalize_requested_path(path or "", work_dir, previous_dir_names)
    if not normalized or "\x00" in normalized:
        return None
    canonical = posixpath.normpath(normalized)
    if canonical in (".", "..") or canonical.startswith(("/", "../")):
        return None
    return canonical


def contained_listing_path(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str | None:
    """Empty and root spellings name the workspace directory, unlike empty file requests."""
    raw = (path or "").strip()
    if raw in {"", ".", "./", "/"}:
        return ""
    return contained_relative_path(raw, work_dir, previous_dir_names)


def contained_absolute_path(
    path: str, work_dir: str, previous_dir_names: Sequence[str] = ()
) -> str | None:
    """Use the workspace base: sandbox normalization also allows the shared computer root and /tmp."""
    relative = contained_relative_path(path, work_dir, previous_dir_names)
    if relative is None:
        return None
    return f"{work_dir.rstrip('/')}/{relative}"


def is_within(root: str, candidate: str) -> bool:
    normalized_root = posixpath.normpath(root or "/")
    normalized = posixpath.normpath(candidate or "")
    if normalized == normalized_root:
        return True
    prefix = normalized_root if normalized_root.endswith("/") else f"{normalized_root}/"
    return normalized.startswith(prefix)


def shared_agent_root(sandbox: Any) -> str | None:
    """The machine-wide ``.agents`` tier, which a workspace reads through its overlay.

    A workspace's ``.agents/tools/docs/<server>`` is a link into the computer's
    one copy of the tool docs, so canonicalizing it lands outside the folder.
    That is shared agent runtime rather than a sibling's work, and the
    ``_internal`` deny list still runs on whatever the link resolved to.
    """
    root = getattr(sandbox, "working_dir", None)
    if not isinstance(root, str) or not root:
        return None
    return SandboxLayout.for_root(root).agents


def serving_roots(sandbox: Any, *, work_dir: str) -> tuple[str, ...]:
    """Every directory a canonical read may land in.

    One list rather than a root plus exceptions: the shell probe and the
    recheck below it both decide containment, and they can only agree about it
    if there is a single thing to agree about. Reads are restricted to the
    project folder because the computer root and ``/tmp`` are shared with every
    sibling; the shared agent tier joins it because a workspace reaches the
    machine's one copy of the tool docs by symlink.

    Landing in the shared tier is allowed; *addressing* it is not, which is the
    ``is_within(work_dir, ...)`` test each caller runs on the request first.
    """
    shared = shared_agent_root(sandbox)
    if shared and not is_within(work_dir, shared):
        return (work_dir, shared)
    # An unsplit computer already serves the shared tier out of its own folder,
    # and naming it twice would only make the probe resolve it twice.
    return (work_dir,)


def _root_lines(roots: Sequence[str]) -> tuple[list[str], list[str]]:
    """Shell that resolves each root, and the case patterns it leaves behind.

    Every root is resolved in the shell too, because a root spelled through a
    symlink would otherwise never match the canonical target. The resolved
    value lands in its own variable: a failed assignment empties the one it
    writes, so resolving in place would hand the fallback an empty path, and an
    empty root would leave the pattern ``/*``, which every absolute path
    matches. Hence the emptiness check as well.
    """
    lines: list[str] = []
    patterns: list[str] = []
    for index, root in enumerate(roots):
        var = f"r{index}"
        lines.append(
            f"{var}in={shlex.quote(root)}; "
            f'{var}=$(realpath -m -- "${var}in" 2>/dev/null) '
            f'|| {var}=$(readlink -f -- "${var}in" 2>/dev/null) '
            f'|| {var}=$(__la_rp "${var}in") '
            f"|| exit {PROBE_UNRESOLVABLE}; "
            f'[ -n "${var}" ] || exit {PROBE_UNRESOLVABLE}; '
        )
        patterns.extend([f'"${var}"', f'"${var}"/*'])
    return lines, patterns


def probe_command(target: str, *, roots: Sequence[str]) -> str:
    """realpath -m distinguishes a missing leaf from a symlink escape; readlink -f covers other images, __la_rp macOS.

    Repeat containment in the shell so it refuses escaped paths regardless of caller behavior.
    """
    setup, patterns = _root_lines(roots)
    if not patterns:
        return f"exit {PROBE_ESCAPED}"
    return "".join(
        [
            _RESOLVE_SH,
            *setup,
            f"t={shlex.quote(target)}; ",
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            f'|| tt=$(readlink -f -- "$t" 2>/dev/null) || tt=$(__la_rp "$t") '
            f"|| exit {PROBE_UNRESOLVABLE}; ",
            f'[ -n "$tt" ] || exit {PROBE_UNRESOLVABLE}; ',
            f'case "$tt" in {"|".join(patterns)}) printf %s "$tt" ;; ',
            f"*) exit {PROBE_ESCAPED} ;; esac",
        ]
    )


def contained_read_command(
    target: str,
    *,
    roots: Sequence[str],
    denied_roots: Sequence[str] = (),
    byte_range: tuple[int, int] | None = None,
    read_max: int | None = None,
) -> str:
    """``byte_range`` is ``(offset, length)``: the read prints the file's
    version line and the sandbox clock, then its first ``UTF8_SNIFF_BYTES`` and
    at most ``length`` bytes from ``offset``, each base64 on a line of its own,
    then the version line again. It skips the size limit, since a range is how
    a file of any size gets read, and ``head`` stopping at the end clamps the
    tail. ``read_max`` lowers a whole read's limit for a runtime that keeps
    less output than ``EXEC_READ_MAX_BYTES`` prints.
    """
    setup, patterns = _root_lines(roots)
    if not patterns:
        return f"exit {READ_CONTAINMENT}"
    denied_setup: list[str] = []
    denied_patterns: list[str] = []
    for index, root in enumerate(denied_roots):
        var = f"d{index}"
        denied_setup.append(
            f"{var}in={shlex.quote(root)}; "
            f'{var}=$(realpath -m -- "${var}in" 2>/dev/null) '
            f'|| {var}=$(readlink -f -- "${var}in" 2>/dev/null) '
            f'|| {var}=$(__la_rp "${var}in") || true; '
        )
        denied_patterns.extend([f'"${var}"', f'"${var}"/*'])
    denied_check = (
        f'case "$tt" in {"|".join(denied_patterns)}) '
        f"exit {READ_CONTAINMENT} ;; *) ;; esac; "
        if denied_patterns
        else ""
    )
    if byte_range is None:
        limit = EXEC_READ_MAX_BYTES if read_max is None else read_max
        read = [
            f'sz=$(wc -c < "$tt"); [ "$sz" -le {limit} ] ',
            f'|| {{ printf %s "$sz"; exit {READ_TOO_LARGE}; }}; ',
            'base64 < "$tt"',
        ]
    else:
        offset, length = int(byte_range[0]), int(byte_range[1])
        read = [
            _VERSION_LINE,
            # The clock is read after the stat, so every write still to come
            # lands at or after it.
            'printf "%s\\n%s\\n" "$v" "$(date +%s 2>/dev/null)"; ',
            f'[ {offset} -lt "${{v%% *}}" ] || exit {READ_UNSATISFIABLE}; ',
            f'head -c {UTF8_SNIFF_BYTES} -- "$tt" | base64 | tr -d "\\n"; printf "\\n"; ',
            # head stops early on most reads, and where SIGPIPE is ignored tail
            # says so on stderr, which some providers fold into stdout.
            f'tail -c +{offset + 1} -- "$tt" 2>/dev/null | head -c {length} ',
            '| base64 | tr -d "\\n"; printf "\\n"; ',
            # Again after the slice: a rewrite between the two moves one of them.
            _VERSION_LINE,
            'printf "%s\\n" "$v"',
        ]
    return "".join(
        [
            _RESOLVE_SH,
            *setup,
            *denied_setup,
            f"t={shlex.quote(target)}; ",
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            f'|| tt=$(readlink -f -- "$t" 2>/dev/null) || tt=$(__la_rp "$t") '
            f"|| exit {READ_CONTAINMENT}; ",
            f'[ -n "$tt" ] || exit {READ_CONTAINMENT}; ',
            f'case "$tt" in {"|".join(patterns)}) ;; ',
            f"*) exit {READ_CONTAINMENT} ;; esac; ",
            denied_check,
            f'[ -f "$tt" ] || exit {READ_MISSING}; ',
            'printf %s "$tt" | base64 | tr -d "\\n"; printf "\\n"; ',
            *read,
        ]
    )


async def _contained_read(
    sandbox: Any,
    absolute_path: str,
    *,
    work_dir: str,
    byte_range: tuple[int, int] | None = None,
) -> tuple[int, str, str] | None:
    """Run the contained read and recheck where it landed.

    The exit code, the canonical path and the stdout after the path line, or
    None for anything a caller answers as a missing file.
    """
    if not _addresses_project(absolute_path, work_dir):
        return None
    runtime = getattr(sandbox, "runtime", None)
    if runtime is None:
        return None
    roots = serving_roots(sandbox, work_dir=work_dir)
    filesystem = getattr(getattr(sandbox, "config", None), "filesystem", None)
    denied_roots = tuple(getattr(filesystem, "denied_directories", ()) or ())
    try:
        result = await runtime.exec(
            contained_read_command(
                absolute_path,
                roots=roots,
                denied_roots=denied_roots,
                byte_range=byte_range,
                read_max=_exec_read_max(runtime),
            ),
            timeout=_PROBE_TIMEOUT_S,
        )
    except RuntimeError:
        raise
    except Exception as e:
        raise SandboxTransientError(
            f"Contained file read failed: {single_line(str(e))}"
        ) from e

    exit_code = getattr(result, "exit_code", None)
    if exit_code in (READ_MISSING, READ_CONTAINMENT):
        return None
    stopped = READ_TOO_LARGE if byte_range is None else READ_UNSATISFIABLE
    if exit_code not in (0, stopped):
        raise SandboxTransientError(
            f"Contained file read failed with exit code {exit_code}"
        )

    encoded_path, separator, rest = str(getattr(result, "stdout", "") or "").partition(
        "\n"
    )
    if not separator or not encoded_path:
        return None
    try:
        canonical = base64.b64decode(encoded_path, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    if not any(is_within(root, canonical) for root in roots):
        return None
    validate = getattr(sandbox, "validate_path", None)
    if callable(validate) and not validate(canonical):
        return None
    return exit_code, canonical, rest


async def read_contained_sandbox_file(
    sandbox: Any, absolute_path: str, *, work_dir: str
) -> tuple[str, bytes] | None:
    """Resolve containment and emit bytes in the same sandbox request."""
    read = await _contained_read(sandbox, absolute_path, work_dir=work_dir)
    if read is None:
        return None
    exit_code, canonical, rest = read
    try:
        if exit_code == READ_TOO_LARGE:
            size, content = int(rest.strip()), b""
        else:
            content = base64.b64decode("".join(rest.split()), validate=True)
    except (binascii.Error, ValueError):
        return None
    if exit_code == READ_TOO_LARGE:
        raise FileTooLargeToServe(canonical, size)
    return canonical, content


async def read_contained_sandbox_range(
    sandbox: Any, absolute_path: str, *, work_dir: str, offset: int, length: int
) -> ContainedSlice | None:
    """``read_contained_sandbox_file`` for at most ``length`` bytes from ``offset``.

    Raises :class:`FileChangedWhileRead` when the version line read after the
    slice differs from the one before it, or the slice from the size: answering
    would send a validator or a Content-Range the body contradicts.
    """
    read = await _contained_read(
        sandbox, absolute_path, work_dir=work_dir, byte_range=(offset, length)
    )
    if read is None:
        return None
    exit_code, canonical, rest = read
    lines = rest.split("\n")
    before = _size_and_stamps(lines[0])
    if before is None:
        return None
    size, stamps = before
    version = _version(stamps, lines[1] if len(lines) > 1 else "")
    if exit_code == READ_UNSATISFIABLE:
        return ContainedSlice(canonical, size, version, b"", None)
    if len(lines) < 5:
        return None
    try:
        head = base64.b64decode(lines[2], validate=True)
        content = base64.b64decode(lines[3], validate=True)
    except (binascii.Error, ValueError):
        return None
    if _size_and_stamps(lines[4]) != before or len(content) != min(
        length, size - offset
    ):
        raise FileChangedWhileRead(canonical)
    return ContainedSlice(canonical, size, version, head, content)


def _size_and_stamps(
    line: str,
) -> tuple[int, tuple[str, str, str] | None] | None:
    """A version line's size, and its mtime, ctime and inode when stat gave
    all three.

    They land in an ETag, so anything but stat's digits is none.
    """
    fields = line.split()
    if not fields or not fields[0].isascii() or not fields[0].isdigit():
        return None
    size = int(fields[0])
    if len(fields) != 4:
        return size, None
    mtime, ctime, inode = fields[1:]
    if not (
        _TIMESTAMP.fullmatch(mtime)
        and _TIMESTAMP.fullmatch(ctime)
        and inode.isascii()
        and inode.isdigit()
    ):
        return size, None
    return size, (mtime, ctime, inode)


def _version(stamps: tuple[str, str, str] | None, clock: str) -> str | None:
    """The stamps as one version, or None when they cannot tell the file from
    its next write.

    mtime alone cannot, since a writer can set it (``cp -p``, ``touch -r``, an
    unpacked archive). No writer sets the ctime, and a file renamed over the
    name brings its own inode; but a write moves the ctime only by the clock's
    step, a kernel tick or a whole second, so two same-size writes within one
    step share every stamp, however many digits the ctime shows. A version
    counts only once the sandbox clock has left the last write behind, as
    Apache's ETags do.
    """
    if stamps is None:
        return None
    clock = clock.strip()
    if not (clock.isascii() and clock.isdigit()):
        return None
    mtime, ctime, inode = stamps
    # Exact, so a fractional ctime is held to the same two seconds.
    if Decimal(clock) - Decimal(ctime) < _VERSION_MIN_AGE_S:
        return None
    return f"{mtime}-{ctime}-{inode}"


def batch_probe_command(
    targets: Sequence[str], *, roots: Sequence[str], mark: str
) -> str:
    """The same probe over a whole batch: one record per target, in order.

    A record is ``y`` and the canonical path, or ``n`` for anything that does
    not resolve inside the roots, and ``mark`` terminates each one. The mark is
    a fresh random token rather than a newline or a NUL, because a file name
    may legitimately contain either and the transport may not carry one.
    """
    setup, patterns = _root_lines(roots)
    if not patterns or not targets:
        return "exit 0"
    quoted = " ".join(shlex.quote(target) for target in targets)
    return "".join(
        [
            _RESOLVE_SH,
            *setup,
            f"m={shlex.quote(mark)}; ",
            f"set -- {quoted}; ",
            'for t in "$@"; do ',
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            '|| tt=$(readlink -f -- "$t" 2>/dev/null) || tt=$(__la_rp "$t") ',
            "|| { printf 'n%s' \"$m\"; continue; }; ",
            '[ -n "$tt" ] || { printf \'n%s\' "$m"; continue; }; ',
            f'case "$tt" in {"|".join(patterns)}) ',
            'printf \'y%s%s\' "$tt" "$m" ;; ',
            "*) printf 'n%s' \"$m\" ;; esac; ",
            "done",
        ]
    )


async def resolve_in_sandbox(
    sandbox: Any, absolute_path: str, *, roots: Sequence[str]
) -> str | None:
    """Use the existing handle without retry or reconnect so reads never wake a sandbox.

    Execution failures propagate as transient for caller fallback; resolved escapes deny access.
    """
    runtime = getattr(sandbox, "runtime", None)
    if runtime is None:
        return None

    try:
        result = await runtime.exec(
            probe_command(absolute_path, roots=roots), timeout=_PROBE_TIMEOUT_S
        )
    except RuntimeError:
        # Preserve sandbox errors for caller classification.
        raise
    except Exception as e:
        raise SandboxTransientError(
            f"Path containment probe failed: {single_line(str(e))}"
        ) from e
    exit_code = getattr(result, "exit_code", None)
    if exit_code != 0:
        logger.warning(
            "Path containment denied %r under %r (probe exit %s)",
            absolute_path,
            roots,
            exit_code,
        )
        return None

    # Only line endings: a filename may legitimately begin or end with a space.
    canonical = str(getattr(result, "stdout", "") or "").strip("\r\n")
    # Recheck containment in case a provider mangles exit codes.
    if not canonical or not any(is_within(root, canonical) for root in roots):
        logger.warning(
            "Path containment denied %r under %r (resolved to %r)",
            absolute_path,
            roots,
            canonical,
        )
        return None
    return canonical


async def resolve_batch_in_sandbox(
    sandbox: Any,
    absolute_paths: Sequence[str],
    *,
    roots: Sequence[str],
    strict: bool = False,
) -> list[str | None]:
    """One exec for a whole batch of probes; None for each path that escapes.

    A protocol failure denies the whole batch rather than guessing which record
    belongs to which request: these paths are about to be handed to ``rm``.
    ``strict`` raises instead, for a caller that lists what exists and would
    read a denied batch as files that are not there.
    """
    runtime = getattr(sandbox, "runtime", None)
    if runtime is None:
        return [None] * len(absolute_paths)

    mark = f"--{uuid4().hex}--"
    try:
        result = await runtime.exec(
            batch_probe_command(absolute_paths, roots=roots, mark=mark),
            timeout=_PROBE_TIMEOUT_S,
        )
    except RuntimeError:
        raise
    except Exception as e:
        raise SandboxTransientError(
            f"Path containment probe failed: {single_line(str(e))}"
        ) from e
    if getattr(result, "exit_code", None) != 0:
        if strict:
            raise SandboxTransientError(
                f"Path containment probe exited {getattr(result, 'exit_code', None)}"
            )
        logger.warning(
            "Path containment denied a batch of %d under %r (probe exit %s)",
            len(absolute_paths),
            roots,
            getattr(result, "exit_code", None),
        )
        return [None] * len(absolute_paths)

    records = str(getattr(result, "stdout", "") or "").split(mark)[:-1]
    if len(records) != len(absolute_paths):
        if strict:
            raise SandboxTransientError("Path containment probe returned a short batch")
        logger.warning(
            "Path containment denied a batch of %d under %r "
            "(probe returned %d records)",
            len(absolute_paths),
            roots,
            len(records),
        )
        return [None] * len(absolute_paths)

    resolved: list[str | None] = []
    for record in records:
        canonical = record[1:] if record[:1] == "y" else ""
        # Recheck containment in case a provider mangles exit codes.
        if not canonical or not any(is_within(root, canonical) for root in roots):
            resolved.append(None)
        else:
            resolved.append(canonical)
    return resolved


def _addresses_project(absolute_path: str, work_dir: str) -> bool:
    """Whether a request names something in this project's own folder.

    The computer root and ``/tmp`` are shared with every sibling, so the
    sandbox handle's own reach is not a serving boundary for any of them.
    """
    return is_within(work_dir, absolute_path)


async def contained_sandbox_paths(
    sandbox: Any,
    absolute_paths: Sequence[str],
    *,
    work_dir: str,
    strict: bool = False,
) -> list[str | None]:
    """``contained_sandbox_path`` for a batch, in one sandbox round trip.

    The delete route takes up to a hundred paths in one request, which is a
    hundred execs for one click if each path probes on its own.
    """
    addressed = [
        index
        for index, path in enumerate(absolute_paths)
        if _addresses_project(path, work_dir)
    ]
    resolved: list[str | None] = [None] * len(absolute_paths)
    if not addressed:
        return resolved
    roots = serving_roots(sandbox, work_dir=work_dir)
    canonicals = await resolve_batch_in_sandbox(
        sandbox, [absolute_paths[i] for i in addressed], roots=roots, strict=strict
    )
    validate = getattr(sandbox, "validate_path", None)
    for index, canonical in zip(addressed, canonicals):
        if canonical is None:
            continue
        if callable(validate) and not validate(canonical):
            logger.warning(
                "Path containment denied %r: canonical path %r is not allowed",
                absolute_paths[index],
                canonical,
            )
            continue
        resolved[index] = canonical
    return resolved


async def contained_sandbox_path(
    sandbox: Any, absolute_path: str, *, work_dir: str
) -> str | None:
    """Validate canonical paths so symlinks cannot bypass the _internal deny list.

    ``work_dir`` is the project folder the caller serves; see serving_roots.
    """
    if not _addresses_project(absolute_path, work_dir):
        return None
    roots = serving_roots(sandbox, work_dir=work_dir)
    canonical = await resolve_in_sandbox(sandbox, absolute_path, roots=roots)
    if canonical is None:
        return None
    validate = getattr(sandbox, "validate_path", None)
    if callable(validate) and not validate(canonical):
        logger.warning(
            "Path containment denied %r: canonical path %r is not allowed",
            absolute_path,
            canonical,
        )
        return None
    return canonical
