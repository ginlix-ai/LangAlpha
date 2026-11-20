"""CopyFile: the Chief of Staff's copy of a file or folder from one of the
user's computers to another.

Each machine tool runs on one computer, so without this a file crossed
machines only through the model: Read on one, Write on the other, text only
and every byte paid twice in tokens. Where both computers reach object
storage, the bytes go through it (``ComputerReach.ship``): the source uploads
only what the store lacks, so a file a backup stored already costs no
upload, and the destination fetches the rest itself. Otherwise the server
carries them, a part at a time, so neither the model nor a worker ever holds
the whole file, and a folder travels as a tar archive packed on the source.
Either way the copy lands beside the destination and moves into place only
once it is whole.

Every check runs on where a path really is on the machine, its links
followed, and what a folder leaves out is matched as the same folder on
disk, so another spelling of a path reaches nothing a plain one would not.
"""

from __future__ import annotations

import posixpath
import re
import secrets
import shlex
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import anyio
import structlog
from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field

from ptc_agent.agent.tools.computer_reach import (
    ComputerReach,
    ComputerTarget,
    ComputerUnreachable,
    Shipment,
    Shipped,
    ShipmentTooLarge,
    file_refusal,
    while_in_use,
)
from ptc_agent.agent.tools.utils import mark_sandbox_raised
from ptc_agent.core.paths import (
    BACKUP_EXCLUDE_AGENT_SUBDIRS,
    BACKUP_EXCLUDE_DIRS,
    THREAD_INDEX,
    SandboxLayout,
    lexical_path,
    logged_path,
)
from ptc_agent.core.project_context import project_bound
from src.observability.private_errors import failure

logger = structlog.get_logger(__name__)

_MB = 1024 * 1024

#: The most one call carries through the server, a folder counted as its
#: packed size: past it the call runs for many minutes, every byte crossing
#: the server twice, and lands a second copy of a large tree on a disk the
#: user may not watch.
COPY_MAX_BYTES = 1024 * _MB

#: The most one call carries through object storage, every file counted
#: whole. The bytes never cross the server, so what bounds it is the time a
#: call waits and the destination's disk.
COPY_MAX_BYTES_VIA_STORE = 10 * 1024 * _MB

Ship = Callable[[ComputerTarget, Shipment, ComputerTarget], Awaitable[Shipped | None]]

#: What the server holds of a file at once. Each part lands as its own file on
#: the destination, joined there once all have arrived.
_PART_BYTES = 16 * _MB

#: The joining script's exits for what the agent can act on: the joined file
#: is not the size sent; something landed at the destination meanwhile; the
#: destination became a folder, or a file, meanwhile; the archive would not
#: unpack; a merge stopped partway; a folder holds names its destination's
#: disk takes for one.
_SHORT = 74
_TAKEN = 73
_CHANGED = 72
_UNPACK = 75
_PARTLY = 76
_CLASH = 77

_STEP_TIMEOUT_S = 60

#: Left out of a folder at any depth: the agent's own files in a workspace's
#: ``.agents``, which the server keeps (memory, transcripts, the user's
#: files) or each computer makes anew (command logs, tool wrappers), as a
#: backup leaves them out; a copy still landing; and sockets, which no copy
#: can carry.
_LEFT_OUT_ANYWHERE = (*BACKUP_EXCLUDE_AGENT_SUBDIRS, THREAD_INDEX)

#: What every name a copy makes for itself carries: its staging folder, its
#: archive, and its entry while it moves in. Each is hidden and ends in the
#: tag and eight hex digits, so a user's own file is not taken for one and
#: left out of a folder.
_COPY_TAG = ".langalpha-copy-"
_STAGING_NAME = ".*" + _COPY_TAG[1:] + "[0-9a-f]" * 8


def _transient(before: str = "") -> str:
    return f"{before}{_COPY_TAG}{secrets.token_hex(4)}"

#: Left out at the computer root only: the platform's own folders there, the
#: computer's credentials among them. Ordinary names anywhere else.
_LEFT_OUT_AT_ROOT = (SandboxLayout.INTERNAL_DIR, *sorted(BACKUP_EXCLUDE_DIRS))

#: Marks the line a pack reports on, and the end of a probe's records.
_PACKED = "langalpha-packed"
_LOOKED = "langalpha-looked"

#: As stat prints it, with no leading zeros: 000 is ``0``, 040 is ``40``.
_MODE = re.compile(r"^[0-7]{1,4}$")

#: Shell functions a probe runs. ``real`` prints where a path really is on
#: the machine, its links followed and, for a path not there yet, its
#: deepest existing folder resolved. ``kind`` prints dir, file or none. The
#: shell drops a line break ending what a command prints, and a Mac's
#: readlink one ending a link's target, so each step must land on the same
#: file as the path it resolved (``-ef``), or neither be there, and a target
#: holding a line break is refused; else ``!`` is printed.
_PROBE_FUNCTIONS = (
    "exec 2>/dev/null; "
    'real(){ p=$1; n=0; while [ -L "$p" ] && [ $n -lt 40 ]; do q=$p; t=$(readlink "$p"; printf x); '
    "t=${t%x}; t=${t%?}; case $t in *$'\\n'*) echo !; return;; esac; "
    'case $t in /*) p=$t;; *) p=$(dirname "$p")/$t;; esac; '
    'if [ -e "$q" ]; then [ "$q" -ef "$p" ]; else [ ! -e "$p" ]; fi || { echo !; return; }; '
    'n=$((n+1)); done; [ -L "$p" ] && { echo !; return; }; '
    'r=; while [ ! -e "$p" ] && [ "$p" != / ]; do r=/$(basename "$p")$r; p=$(dirname "$p"); done; '
    'if [ -d "$p" ]; then d=$(cd -P "$p" && pwd -P); '
    'else d=$(cd -P "$(dirname "$p")" && pwd -P)/$(basename "$p"); fi; '
    '[ "$p" -ef "$d" ] || { echo !; return; }; printf "%s%s\\n" "$d" "$r"; }; '
    'kind(){ if [ -d "$1" ]; then echo dir; elif [ -e "$1" ] || [ -L "$1" ]; then echo file; '
    "else echo none; fi; }; "
)


class CopyFileInput(BaseModel):
    source: str = Field(description="The file or folder to copy.")
    destination: str = Field(
        description=(
            "The copy's path. Ending in `/`, a folder to put it in under its own "
            "name; a file copied to an existing folder goes in it too. Missing "
            "folders are created."
        )
    )
    source_computer: str | None = Field(
        default=None,
        description=(
            "The computer the source is on, as `<activity>` lists it. Leave it "
            "out for your own computer, where paths start in Home; on another, "
            "write paths whole."
        ),
    )
    destination_computer: str | None = Field(
        default=None,
        description=(
            "The computer to copy to, named the same way. Only its workspace "
            "folders are backed up, so a copy to keep goes in one."
        ),
    )
    overwrite: bool = Field(
        default=False,
        description=(
            "Replace a file already at the destination, or merge into a folder "
            "already there, replacing its files of the same name."
        ),
    )


DESCRIPTION = """Copy a file or a folder from one of the user's computers to another, or within one. Use it to move data between machines rather than reading a file out and writing it back, which works for text only.

A folder's `.agents` memory, threads and tool files are left out: memory, memos, the profile, automations and workflows are the same on every computer and need no copy.

To move rather than copy, delete the source with Bash once the copy is done."""


@dataclass(frozen=True)
class _Source:
    """What a copy reads, found before any byte moves."""

    path: str
    folder: bool
    #: A file's bytes, or a folder's size on disk, which its archive is near.
    size: int
    #: A file's permission bits in octal, which its copy is given; a folder's
    #: archive carries every mode in it.
    mode: str = ""
    #: The name the agent gave it, which a copy into a folder keeps: through a
    #: link, where it really is may have another.
    called: str = ""

    @property
    def name(self) -> str:
        return posixpath.basename(self.path)


@dataclass(frozen=True)
class _Payload:
    """The one file a copy carries: the source file, or a folder's archive."""

    path: str
    size: int
    files: int = 1
    #: Everything a folder's archive holds, folders and links among them: as
    #: many must come out of it at the destination.
    entries: int = 1


def _size(n: int) -> str:
    if n < 1024:
        return f"{n} bytes"
    if n < _MB:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * _MB:
        return f"{n / _MB:.1f} MB"
    return f"{n / (1024 * _MB):.1f} GB"


def _detail(result: dict[str, Any]) -> str:
    return (result.get("stderr") or result.get("stdout") or "no answer").strip()[:300]


def _over_cap(spelled: str, measured: str, cap: int = COPY_MAX_BYTES) -> str:
    return (
        f"{spelled} {measured}, and CopyFile carries up to {_size(cap)} between these "
        "computers. Copy it in pieces, or compress it with Bash first."
    )


def _within(path: str, folder: str) -> bool:
    return path == folder or path.startswith(f"{folder.rstrip('/')}/")


async def _sh(
    target: ComputerTarget, command: str, timeout: int = _STEP_TIMEOUT_S
) -> dict[str, Any]:
    with project_bound(target.project):
        return await target.sandbox.execute_bash_command(
            command, working_dir=target.root, timeout=timeout
        )


def _whole(target: ComputerTarget, path: str) -> str:
    """``path`` as ``target``'s file tools resolve it, which an upload does too."""
    with project_bound(target.project):
        return target.sandbox.normalize_path(path.strip())


def _check(target: ComputerTarget, tool: str, path: str) -> None:
    """Refuse a path the file tool ``tool`` would refuse on ``target``."""
    if refused := file_refusal(target, tool, {"file_path": path}):
        raise ComputerUnreachable(refused)


def _check_real(target: ComputerTarget, tool: str, path: str) -> None:
    """``_check`` for where a file really is, links followed.

    Held to the computer's allowed folders as it stands first: the file
    tools read an absolute path outside them as one inside, which would pass
    a link out of the root.
    """
    files = target.sandbox.config.filesystem
    if (
        target.project.machine is None
        and files.enable_path_validation
        and not any(_within(path, d) for d in files.allowed_directories)
    ):
        raise ComputerUnreachable(
            f'{path} is outside what the file tools reach on computer "{target.name}": '
            f"its root `{target.root}` and `/tmp`."
        )
    _check(target, tool, path)


def _held(target: ComputerTarget) -> list[str]:
    """What no copy reads from or merges into on ``target``: the platform's
    folders at its root, its credentials among them, and on a machine of the
    user's own, LangAlpha's config folder there."""
    held = [posixpath.join(target.root, d) for d in _LEFT_OUT_AT_ROOT]
    if target.project.machine is not None:
        held += target.project.machine.denied
    return held


async def _look(target: ComputerTarget, *paths: str) -> list[tuple[str, str]]:
    """What is at each of ``paths`` on ``target`` (dir, file or none) and
    where it really is, spelled from the computer's root and allowed folders
    as the file tools spell them (a Mac's ``/tmp`` is ``/private/tmp`` on
    disk), so the checks run on that spelling."""
    anchors = list(
        dict.fromkeys([target.root, *target.sandbox.config.filesystem.allowed_directories])
    )
    script = _PROBE_FUNCTIONS + "".join(f"real {shlex.quote(a)}; " for a in anchors) + "".join(
        f"kind {shlex.quote(p)}; real {shlex.quote(p)}; " for p in paths
    ) + f"echo {_LOOKED}"
    probe = await _sh(target, script)
    lines = (probe.get("stdout") or "").split("\n")
    # Counted to the end mark: a line break in a path, or in a link's target,
    # would shift every record after it onto the wrong path.
    end = lines.index(_LOOKED) if _LOOKED in lines else -1
    reals, lines = lines[: len(anchors)], lines[len(anchors) : end]
    if len(lines) != 2 * len(paths) or not all(
        r.startswith("/") for r in (*reals, *lines[1::2])
    ):
        raise ComputerUnreachable(
            f'{paths[0]} could not be checked on computer "{target.name}": {_detail(probe)}'
        )
    seen = []
    for kind, real in zip(lines[0::2], lines[1::2], strict=True):
        for anchor, real_anchor in zip(anchors, reals):
            if _within(real, real_anchor):
                real = anchor.rstrip("/") + real[len(real_anchor.rstrip("/")) :]
                break
        seen.append((kind, lexical_path(real)))
    return seen


async def _read_source(target: ComputerTarget, spelled: str) -> _Source:
    where = f'computer "{target.name}"'
    whole = _whole(target, spelled)
    called = posixpath.basename(whole.rstrip("/"))
    [(kind, path)] = await _look(target, whole)
    if kind == "none":
        raise ComputerUnreachable(f"There is no file or folder {spelled} on {where}.")
    _check_real(target, "Read", path)
    q = shlex.quote(path)
    # GNU stat and a Mac's spell the mode differently; one of them answers.
    probe = await _sh(
        target,
        f'if [ -d {q} ]; then echo dir "$(du -sk {q} | cut -f1)"; '
        f'elif [ -f {q} ]; then echo file "$(wc -c < {q})" '
        f'"$(stat -c %a {q} 2>/dev/null || stat -f %Lp {q})"; '
        "else echo other; fi",
    )
    match (probe.get("stdout") or "").split():
        case ["other"]:
            raise ComputerUnreachable(f"{spelled} on {where} is neither a file nor a folder.")
        case ["dir", kb] if kb.isdigit() and path != "/":
            return _Source(path, folder=True, size=int(kb) * 1024, called=called)
        case ["file", size, mode] if size.isdigit() and _MODE.match(mode):
            return _Source(path, folder=False, size=int(size), mode=mode, called=called)
    raise ComputerUnreachable(f"{spelled} could not be read on {where}: {_detail(probe)}")


async def _landing(target: ComputerTarget, spelled: str, source: _Source) -> tuple[str, str]:
    """Where a copy of ``source`` lands on ``target`` for the agent's
    destination ``spelled``, whole, and what is there now (``dir``, ``file``
    or ``none``). Inside the destination when the agent ended it in ``/``,
    or for a file when it is a folder; else at it, so a folder copied onto
    one already there merges rather than nesting."""
    path = _whole(target, spelled)
    inside = _whole(target, posixpath.join(path, source.called or source.name))
    (at, at_path), within = await _look(target, path, inside)
    # Read from the agent's spelling: normalizing drops a trailing slash.
    goes_in = spelled.strip().endswith("/") or (at == "dir" and not source.folder)
    there, landing = within if goes_in else (at, at_path)
    _check_real(target, "Write", landing)
    return landing, there


def _landing_refusal(
    target: ComputerTarget, source: _Source, landing: str, there: str, *, overwrite: bool
) -> str | None:
    where = f'computer "{target.name}"'
    if there == "none":
        return None
    if (there == "dir") != source.folder:
        kind = "folder" if there == "dir" else "file"
        return f"{landing} is a {kind} on {where}, so the copy cannot go there. Name another path."
    if not overwrite:
        if source.folder:
            return (
                f"{landing} already exists on {where}. Pass overwrite=true to merge into it, "
                "end the destination with `/` to copy into it, or name another path."
            )
        return (
            f"{landing} already exists on {where}. Pass overwrite=true to replace it, or "
            "name another path."
        )
    if source.folder and any(_within(h.casefold(), landing.casefold()) for h in _held(target)):
        return (
            f"{landing} holds the computer's own folders, so nothing is merged into it. "
            "Copy into a workspace folder."
        )
    return None


def _pack_script(folder: str, archive: str, held: Iterable[str]) -> str:
    """Pack ``folder``, links as links, into ``archive``, and print a line
    with its size and how many files it holds. Walked from its parent, so
    the patterns match a ``.agents`` folder copied on its own too; ``held``
    is left out wherever it sits, matched as the same folder on disk however
    it is spelled. The pack stops one byte past the cap, so a sparse file
    cannot fill the disk. GNU tar keeps a sparse file's holes as a Mac's
    does by itself, so it lands sparse wherever it is unpacked. A Mac's tar
    leaves out its metadata files and extended attributes, which Linux would
    unpack as junk files or warnings."""
    q = shlex.quote
    tests = [
        *(f"-path {q('*/' + d)}" for d in _LEFT_OUT_ANYWHERE),
        f"-name {q(_STAGING_NAME)}",
        "-type s",
    ]
    walk = f'find {q("./" + posixpath.basename(folder))} \\( {" -o ".join(tests)} "$@" \\) -prune -o'
    return (
        # The archive is written at the computer root, so a copy of the root
        # packs the folder that holds it. Made before tar reads that folder,
        # or GNU tar sees the folder change and fails the pack.
        f'umask 077; a={q(archive)}; : > "$a" || exit 1; '
        f"cd {q(posixpath.dirname(folder))} || exit 1; set --; "
        f'for h in {" ".join(q(h) for h in held)}; do [ -e "$h" ] && set -- "$@" -o -samefile "$h"; done; '
        "x=-S; tar --version 2>/dev/null | grep -q bsdtar && x=--no-xattrs; "
        f"{walk} -print0 | COPYFILE_DISABLE=1 tar $x --null --no-recursion -T - -cf - "
        f'| head -c {COPY_MAX_BYTES + 1} > "$a"; s="${{PIPESTATUS[*]}}"; n=$(wc -c < "$a" | tr -d " "); '
        f'[ "$n" -gt {COPY_MAX_BYTES} ] || [ "$s" = "0 0 0" ] || exit 1; '
        # On a line of its own: a warning before it may not end in one.
        # Counted by NUL, so a name with a line break in it counts once.
        f'printf "\\n%s %s %s %s\\n" {_PACKED} "$n" '
        f'"$({walk} -type f -print0 | tr -dc "\\\\0" | wc -c | tr -d " ")" '
        f'"$({walk} -print0 | tr -dc "\\\\0" | wc -c | tr -d " ")"'
    )


async def _pack(target: ComputerTarget, source: _Source, spelled: str, archive: str) -> _Payload:
    packed = await _sh(
        target,
        _pack_script(source.path, archive, _held(target)),
        # Packing reads the folder and writes it once more.
        timeout=_STEP_TIMEOUT_S + source.size // (20 * _MB),
    )
    # Found by its mark: Docker hands a command's errors back in its output.
    for line in (packed.get("stdout") or "").splitlines():
        match line.split():
            case [mark, size, files, entries] if mark == _PACKED and all(
                n.isdigit() for n in (size, files, entries)
            ):
                return _Payload(archive, int(size), files=int(files), entries=int(entries))
    raise ComputerUnreachable(
        f'{spelled} could not be packed on computer "{target.name}": {_detail(packed)}'
    )


@asynccontextmanager
async def _payload(target: ComputerTarget, source: _Source, spelled: str) -> AsyncIterator[_Payload]:
    """What a copy of ``source`` carries: the file itself, or the folder
    packed into an archive at the computer's root, removed once the copy is
    done, given up, or cut off."""
    if not source.folder:
        yield _Payload(source.path, source.size)
        return
    archive = posixpath.join(target.root, _transient())
    try:
        yield await _pack(target, source, spelled, archive)
    finally:
        await _shielded_rm(target, archive)


@dataclass(frozen=True)
class _Arrival:
    """A copy in its staging folder, by either route, as the placing script
    takes it up."""

    size: int
    files: int
    #: Everything a folder holds, folders and links among them: as many must
    #: come out of it on the destination's disk.
    entries: int
    #: Run first, in the staging folder: leaves a file as ``whole`` and a
    #: folder as ``unpacked/<its name>``, or fails.
    assemble: str
    #: Prints a folder as a tar archive, its entries under ``./<its name>``,
    #: for a merge to extract over the folder there.
    archive: str


def _streamed(payload: _Payload, parts: int, *, folder: bool) -> _Arrival:
    """Parts the server carried, joined into ``whole``. Each part goes as
    soon as it is joined, so the disk holds little more than the copy at
    once. A folder's archive is unpacked from it, and a merge extracts
    straight from it, so a sparse file stays sparse, which packing it again
    would undo."""
    assemble = (
        ": > whole || fail 1; "
        f'for p in {" ".join(str(i) for i in range(parts))}; do '
        'cat "$p" >> whole && rm -f "$p" || fail 1; done; '
        f'[ "$(wc -c < whole | tr -d " ")" = {payload.size} ] || fail {_SHORT}; '
    )
    if folder:
        assemble += f"mkdir unpacked && tar -xpf whole -C unpacked --no-same-owner || fail {_UNPACK}; "
    return _Arrival(
        payload.size, payload.files, payload.entries, assemble=assemble, archive="cat whole"
    )


def _stored(shipped: Shipped, source: _Source) -> _Arrival:
    """What object storage landed, already unpacked, each file's digest
    checked as it landed. A merge packs the folder again."""
    return _Arrival(
        shipped.size,
        shipped.files,
        shipped.entries,
        assemble="",
        archive=f"(cd unpacked && COPYFILE_DISABLE=1 tar -cf - {shlex.quote('./' + source.name)})",
    )


def _placing_script(
    staging: str, landing: str, source: _Source, arrival: _Arrival, *, overwrite: bool
) -> str:
    """Assemble ``arrival`` in ``staging`` and put it at ``landing``, then
    remove ``staging``, which decides nothing: a folder unpacked read-only is
    made writable first."""
    return (
        f"set -o pipefail; umask 077; d={shlex.quote(staging)}; "
        'tidy(){ cd /; chmod -R u+rwX "$d"; rm -rf "$d"; } 2>/dev/null; '
        'fail(){ tidy; exit "$1"; }; cd "$d" || exit 1; '
        f"{arrival.assemble}{_place(landing, source, arrival, overwrite=overwrite)}; tidy; exit 0"
    )


def _place(landing: str, source: _Source, arrival: _Arrival, *, overwrite: bool) -> str:
    """The script that puts the copy at ``landing``: a file from ``whole``,
    a folder from ``unpacked/<its name>``, which a failed unpack leaves
    nothing of. A merge extracts the arrival's archive over the folder there;
    GNU tar and a Mac's both replace a link in its way rather than write
    through it."""
    at = shlex.quote(landing)
    # Moved in under a name of its own, so a folder that took the landing's
    # name meanwhile, which ln and mv put the copy inside, is told apart and
    # only this copy is taken back out of it.
    mark = _transient()
    if not source.folder:
        taken = f"{{ [ -e {at} ] || [ -L {at} ]; }}"
        place = (
            f"mv -f {mark} {at} || fail 1"
            if overwrite
            # A link fails if the name is taken, in one step, and its staged
            # name goes before the cleanup, which would change its mode
            # through the link. A disk that keeps no links gets mv -n, which
            # moves nothing over a file there.
            else f"if ln {mark} {at}; then rm -f {mark}; "
            f"else {taken} && fail {_TAKEN}; mv -n {mark} {at}; "
            f"if [ -e {mark} ]; then {taken} && fail {_TAKEN}; fail 1; fi; fi"
        )
        return (
            f"chmod {source.mode} whole || fail 1; [ -d {at} ] && fail {_CHANGED}; "
            f"mv whole {mark} || fail 1; {place}; "
            f"if [ -e {at}/{mark} ]; then rm -f {at}/{mark}; fail {_CHANGED}; fi"
        )
    unpacked = shlex.quote(f"unpacked/{source.name}")
    claim = f"unpacked/{mark}"
    merge = (
        # Its own folder's entry left off, so the folder merged into keeps
        # its mode.
        f"{arrival.archive} | tar -xpf - -C {at} --strip-components=2 --no-same-owner "
        f"|| fail {_PARTLY}"
        if overwrite
        else f"fail {_TAKEN}"
    )
    return (
        # Unpacked whole beside the landing first, on the same disk, so that
        # disk itself says whether two of its names are one to it (A and a,
        # or two spellings of é), which shows as fewer entries than were
        # sent.
        f"[ -d {unpacked} ] || fail {_UNPACK}; "
        f'[ "$(find unpacked -mindepth 1 -print0 | tr -dc "\\\\0" | wc -c | tr -d " ")" '
        f"= {arrival.entries} ] || fail {_CLASH}; "
        f"if [ -d {at} ]; then {merge}; "
        f"elif [ -e {at} ] || [ -L {at} ]; then fail {_CHANGED}; "
        f"else rm -f whole; mv {unpacked} {claim} || fail 1; mv {claim} {at} || fail 1; "
        f"if [ -e {at}/{mark} ]; then mv {at}/{mark} unpacked/; fail {_TAKEN}; fi; fi"
    )


async def _parts(stream: AsyncIterator[bytes], cap: int) -> AsyncIterator[bytes]:
    """``stream`` cut into parts of ``_PART_BYTES``, one empty part for an
    empty file, refusing a file that grows past ``cap`` while it is read."""
    buffer = bytearray()
    sent = 0
    async for chunk in stream:
        buffer.extend(chunk)
        if sent + len(buffer) > cap:
            raise ComputerUnreachable(
                f"The file grew past {cap // _MB} MB while it was copied; nothing was copied."
            )
        while len(buffer) >= _PART_BYTES:
            sent += _PART_BYTES
            yield bytes(buffer[:_PART_BYTES])
            del buffer[:_PART_BYTES]
    if buffer or not sent:
        yield bytes(buffer)


async def _shielded_rm(target: ComputerTarget, path: str) -> None:
    """Remove ``path``, even from a turn cancelled mid-copy."""
    with anyio.CancelScope(shield=True), anyio.move_on_after(_STEP_TIMEOUT_S):
        try:
            await _sh(target, f"chmod -R u+rwX {shlex.quote(path)} 2>/dev/null; rm -rf {shlex.quote(path)}")
        except Exception as e:  # noqa: BLE001 - the copy's own error is the one to raise
            logger.warning("Could not remove a copy's leftovers", path=logged_path(path), error=str(e))


async def _staging(destination: ComputerTarget, landing: str) -> str:
    """A new folder beside ``landing`` that only its owner reads, for a copy
    to land in before it moves into place."""
    # Spelled as the upload resolves it: on Home's computer a hidden name
    # beside a workspace folder resolves into Home's folder.
    staging = _whole(
        destination,
        f"{posixpath.dirname(landing)}/{_transient('.' + posixpath.basename(landing))}",
    )
    made = await _sh(destination, f"mkdir -p -m 700 {shlex.quote(staging)}")
    if not made.get("success"):
        await _shielded_rm(destination, staging)
        raise ComputerUnreachable(
            f'{landing} could not be written on computer "{destination.name}": {_detail(made)}'
        )
    return staging


async def _land(
    destination: ComputerTarget,
    landing: str,
    what: _Source,
    arrive: Callable[[str], Awaitable[_Arrival | None]],
    *,
    overwrite: bool,
) -> _Arrival | None:
    """Bring ``what`` into a new staging folder beside ``landing`` with
    ``arrive`` and put it in place; what arrived. None, with nothing left
    behind, when ``arrive`` finds its route does not serve these computers.
    Raises ``ComputerUnreachable`` when the copy was not placed."""
    staging = await _staging(destination, landing)
    try:
        arrival = await arrive(staging)
        if arrival is None:
            await _shielded_rm(destination, staging)
            return None
        placed = await _sh(
            destination,
            _placing_script(staging, landing, what, arrival, overwrite=overwrite),
            # Joining, unpacking or merging reads and writes the copy once
            # more on its disk.
            timeout=_STEP_TIMEOUT_S + arrival.size // (10 * _MB),
        )
    except BaseException:
        await _shielded_rm(destination, staging)
        raise
    if not placed.get("success"):
        # A script that ran out of time cleared nothing.
        await _shielded_rm(destination, staging)
    if failed := _unplaced(placed, what, landing, f'computer "{destination.name}"'):
        raise ComputerUnreachable(failed)
    return arrival


async def _by_store(
    ship: Ship, source: ComputerTarget, what: _Source, destination: ComputerTarget, staging: str
) -> _Arrival | None:
    """``what`` landed in ``staging`` through object storage, or None when
    that route does not serve these computers."""
    try:
        shipped = await ship(
            source,
            Shipment(
                path=what.path,
                folder=what.folder,
                size=0 if what.folder else what.size,
                staging=staging,
                max_bytes=COPY_MAX_BYTES_VIA_STORE,
                tails=_LEFT_OUT_ANYWHERE,
                names=(_STAGING_NAME,),
                same_as=tuple(_held(source)),
            ),
            destination,
        )
    except ShipmentTooLarge as e:
        raise ComputerUnreachable(
            _over_cap(what.path, f"holds {_size(e.size)}", COPY_MAX_BYTES_VIA_STORE)
        ) from None
    return None if shipped is None else _stored(shipped, what)


async def _by_server(
    source: ComputerTarget,
    payload: _Payload,
    what: _Source,
    destination: ComputerTarget,
    landing: str,
    staging: str,
) -> _Arrival:
    """``payload`` streamed through the server into parts in ``staging``."""
    with project_bound(source.project):
        stream = await source.sandbox.astream_file_bytes(payload.path)
    if stream is None:
        raise ComputerUnreachable(f'{payload.path} is gone from computer "{source.name}".')
    parts = 0
    try:
        async for part in _parts(stream, COPY_MAX_BYTES):
            with project_bound(destination.project):
                written = await destination.sandbox.aupload_file_bytes(f"{staging}/{parts}", part)
            if not written:
                raise ComputerUnreachable(
                    f'{landing} could not be written on computer "{destination.name}".'
                )
            parts += 1
    finally:
        await stream.aclose()
    return _streamed(payload, parts, folder=what.folder)


def _unplaced(
    result: dict[str, Any], source: _Source, landing: str, where: str
) -> str | None:
    """Why the joining script left nothing at ``landing``, or what it left
    half done, or None if it placed the copy."""
    if result.get("success"):
        return None
    fix = "merge into it" if source.folder else "replace it"
    why = {
        _TAKEN: (
            f"{landing} appeared on {where} while the copy ran, so it was left as it "
            f"is. Pass overwrite=true to {fix}."
        ),
        _CHANGED: f"{landing} changed on {where} while the copy ran, so it was left as it is.",
        _SHORT: (
            f"The copy on {where} did not come out whole, so nothing was kept. The "
            "source may have changed while it was copied: try again."
        ),
        _UNPACK: f"The folder could not be unpacked on {where}, so nothing was kept: {_detail(result)}",
        _CLASH: (
            f"{source.path} holds names that differ only in case, which the disk at "
            f"{landing} on {where} takes for one, so nothing was copied. Rename one of each "
            "pair first."
        ),
        _PARTLY: (
            f"The merge into {landing} on {where} stopped partway, so some of its files "
            f"may already be replaced: {_detail(result)}"
        ),
    }
    return why.get(
        result.get("exit_code"), f"The copy could not be put in place on {where}: {_detail(result)}"
    )


async def copy_file(
    source: ComputerTarget,
    source_path: str,
    destination: ComputerTarget,
    destination_path: str,
    *,
    overwrite: bool = False,
    ship: Ship | None = None,
) -> str:
    """Copy one file or folder between two computers, through object
    storage when ``ship`` serves them, else through the server. Returns the
    agent's result line; raises ComputerUnreachable with the agent's sentence
    for a refusal."""
    if "\n" in source_path or "\n" in destination_path:
        raise ComputerUnreachable("CopyFile does not take a path with a line break in it.")
    _check(source, "Read", source_path)
    _check(destination, "Write", destination_path)
    what = await _read_source(source, source_path)
    landing, there = await _landing(destination, destination_path, what)
    where = f'computer "{destination.name}"'
    if destination.computer_id == source.computer_id:
        if landing == what.path:
            raise ComputerUnreachable("The source and the destination are the same.")
        # Packed with it, an earlier copy there would be copied into itself again.
        if what.folder and _within(landing, what.path):
            raise ComputerUnreachable(
                f"{landing} is inside the folder being copied, so the copy cannot go there."
            )
    if refused := _landing_refusal(destination, what, landing, there, overwrite=overwrite):
        raise ComputerUnreachable(refused)

    landed = None
    if ship is not None:
        landed = await _land(
            destination,
            landing,
            what,
            lambda staging: _by_store(ship, source, what, destination, staging),
            overwrite=overwrite,
        )
    if landed is None:
        async with _payload(source, what, source_path) as payload:
            if payload.size > COPY_MAX_BYTES:
                # A pack stops one byte past the cap.
                measured = (
                    f"packs to more than {_size(COPY_MAX_BYTES)}"
                    if what.folder
                    else f"is {_size(payload.size)}"
                )
                raise ComputerUnreachable(_over_cap(source_path, measured))
            landed = await _land(
                destination,
                landing,
                what,
                lambda staging: _by_server(source, payload, what, destination, landing, staging),
                overwrite=overwrite,
            )

    origin = f'{what.path} on computer "{source.name}"'
    if not what.folder:
        return f"Copied {_size(landed.size)} from {origin} to {landing} on {where}."
    files = f"{landed.files} file{'' if landed.files == 1 else 's'}"
    verb, onto = ("Merged", "into") if there == "dir" else ("Copied", "to")
    return f"{verb} the folder {origin} ({files}, {_size(landed.size)}) {onto} {landing} on {where}."


def create_copy_file_tool(reach: ComputerReach) -> BaseTool:
    """The Chief of Staff's CopyFile, over the user's computers ``reach``
    resolves."""

    async def call(
        source: str,
        destination: str,
        source_computer: str | None = None,
        destination_computer: str | None = None,
        overwrite: bool = False,
    ) -> str:
        try:
            src = await reach.target(source_computer)
            # Waking the destination can take minutes, longer than the idle
            # sweep leaves the source alone.
            dst = await while_in_use(reach, (src,), reach.target(destination_computer))
            return await while_in_use(
                reach,
                (src, dst),
                copy_file(src, source, dst, destination, overwrite=overwrite, ship=reach.ship),
            )
        except ComputerUnreachable as e:
            return f"ERROR: {e}"
        except Exception as e:
            mark_sandbox_raised(e)
            fields, trace = failure(e)
            logger.error("CopyFile failed", source=logged_path(source), exc_info=trace, **fields)
            return f"ERROR: The copy failed: {e!s}"

    return StructuredTool(
        name="CopyFile",
        description=DESCRIPTION,
        args_schema=CopyFileInput,
        coroutine=call,
    )
