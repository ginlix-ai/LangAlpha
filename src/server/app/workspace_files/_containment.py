"""Canonical containment for mirrored and sandbox file reads.

Mirror keys need canonical relative paths; sandbox checks need realpath because
lexical validation misses .. and symlinks that can expose sibling files.
"""

from __future__ import annotations

import base64
import binascii
import logging
import posixpath
import shlex
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.runtime import SandboxTransientError

from ._shared import _normalize_requested_path
from src.server.utils.error_sanitization import single_line

logger = logging.getLogger(__name__)

# Distinct denial codes distinguish escapes from resolution failures in logs.
PROBE_UNRESOLVABLE = 3
PROBE_ESCAPED = 4

# Deny slow probes rather than holding a serving connection on a sick sandbox.
_PROBE_TIMEOUT_S = 10
READ_MISSING = 2
READ_CONTAINMENT = 3


def contained_relative_path(path: str, work_dir: str) -> str | None:
    """Canonicalize before mirror lookup; sandbox lexical normalization misses .. and symlinks."""
    # The NUL check reads the folded path, not the request: a ``file:`` URL
    # spells one percent-encoded, and the fold is where it decodes.
    normalized = _normalize_requested_path(path or "", work_dir)
    if not normalized or "\x00" in normalized:
        return None
    canonical = posixpath.normpath(normalized)
    if canonical in (".", "..") or canonical.startswith(("/", "../")):
        return None
    return canonical


def contained_listing_path(path: str, work_dir: str) -> str | None:
    """Empty and root spellings name the workspace directory, unlike empty file requests."""
    raw = (path or "").strip()
    if raw in {"", ".", "./", "/"}:
        return ""
    return contained_relative_path(raw, work_dir)


def contained_absolute_path(path: str, work_dir: str) -> str | None:
    """Use the workspace base: sandbox normalization also allows the shared computer root and /tmp."""
    relative = contained_relative_path(path, work_dir)
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
            f"|| exit {PROBE_UNRESOLVABLE}; "
            f'[ -n "${var}" ] || exit {PROBE_UNRESOLVABLE}; '
        )
        patterns.extend([f'"${var}"', f'"${var}"/*'])
    return lines, patterns


def probe_command(target: str, *, roots: Sequence[str]) -> str:
    """realpath -m distinguishes a missing leaf from a symlink escape; readlink -f covers other images.

    Repeat containment in the shell so it refuses escaped paths regardless of caller behavior.
    """
    setup, patterns = _root_lines(roots)
    if not patterns:
        return f"exit {PROBE_ESCAPED}"
    return "".join(
        [
            *setup,
            f"t={shlex.quote(target)}; ",
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            f'|| tt=$(readlink -f -- "$t" 2>/dev/null) || exit {PROBE_UNRESOLVABLE}; ',
            f'[ -n "$tt" ] || exit {PROBE_UNRESOLVABLE}; ',
            f'case "$tt" in {"|".join(patterns)}) printf %s "$tt" ;; ',
            f"*) exit {PROBE_ESCAPED} ;; esac",
        ]
    )


def contained_read_command(
    target: str, *, roots: Sequence[str], denied_roots: Sequence[str] = ()
) -> str:
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
            f'|| {var}=$(readlink -f -- "${var}in" 2>/dev/null) || true; '
        )
        denied_patterns.extend([f'"${var}"', f'"${var}"/*'])
    denied_check = (
        f'case "$tt" in {"|".join(denied_patterns)}) '
        f"exit {READ_CONTAINMENT} ;; *) ;; esac; "
        if denied_patterns
        else ""
    )
    return "".join(
        [
            *setup,
            *denied_setup,
            f"t={shlex.quote(target)}; ",
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            f'|| tt=$(readlink -f -- "$t" 2>/dev/null) || exit {READ_CONTAINMENT}; ',
            f'[ -n "$tt" ] || exit {READ_CONTAINMENT}; ',
            f'case "$tt" in {"|".join(patterns)}) ;; ',
            f"*) exit {READ_CONTAINMENT} ;; esac; ",
            denied_check,
            f'[ -f "$tt" ] || exit {READ_MISSING}; ',
            'printf %s "$tt" | base64 | tr -d "\\n"; printf "\\n"; ',
            'base64 < "$tt"',
        ]
    )


async def read_contained_sandbox_file(
    sandbox: Any, absolute_path: str, *, work_dir: str
) -> tuple[str, bytes] | None:
    """Resolve containment and emit bytes in the same sandbox request."""
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
                absolute_path, roots=roots, denied_roots=denied_roots
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
    if exit_code != 0:
        raise SandboxTransientError(
            f"Contained file read failed with exit code {exit_code}"
        )

    encoded_path, separator, encoded_content = str(
        getattr(result, "stdout", "") or ""
    ).partition("\n")
    if not separator or not encoded_path:
        return None
    try:
        canonical = base64.b64decode(encoded_path, validate=True).decode("utf-8")
        content = base64.b64decode("".join(encoded_content.split()), validate=True)
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    if not any(is_within(root, canonical) for root in roots):
        return None
    validate = getattr(sandbox, "validate_path", None)
    if callable(validate) and not validate(canonical):
        return None
    return canonical, content


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
            *setup,
            f"m={shlex.quote(mark)}; ",
            f"set -- {quoted}; ",
            'for t in "$@"; do ',
            'tt=$(realpath -m -- "$t" 2>/dev/null) ',
            '|| tt=$(readlink -f -- "$t" 2>/dev/null) ',
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
    sandbox: Any, absolute_paths: Sequence[str], *, roots: Sequence[str]
) -> list[str | None]:
    """One exec for a whole batch of probes; None for each path that escapes.

    A protocol failure denies the whole batch rather than guessing which record
    belongs to which request: these paths are about to be handed to ``rm``.
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
        logger.warning(
            "Path containment denied a batch of %d under %r (probe exit %s)",
            len(absolute_paths),
            roots,
            getattr(result, "exit_code", None),
        )
        return [None] * len(absolute_paths)

    records = str(getattr(result, "stdout", "") or "").split(mark)[:-1]
    if len(records) != len(absolute_paths):
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
    sandbox: Any, absolute_paths: Sequence[str], *, work_dir: str
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
        sandbox, [absolute_paths[i] for i in addressed], roots=roots
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
