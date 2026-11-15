"""Moving what Home's stand-in holds into a folder of its own in Home.

The stand-in shares an OS user with every workspace on its cloud computer, so
nothing read there is trusted: its listing picks what to ask for, and the
archive packed there is rebuilt on the server from regular files with the
names asked for alone, within the size caps, before any of it reaches Home.
The archive passes through a temporary file on the server, never memory, and
reaches Home in parts. Home unpacks it into a hidden folder and renames that
folder into place once all of it is there, so a failure leaves no folder
behind and a copy is never merged into one already there.

Which files go is a diff against what earlier hand-backs carried (their size
and change time as listed), not their age: a file unpacked or copied keeps an
old change time, and a sandbox clock may be behind.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ptc_agent.core.sandbox.runtime import STREAM_CHUNK_BYTES

logger = logging.getLogger(__name__)

# What one hand-back carries across, and the largest single file. A file over
# the second is left on the cloud computer and named in the notice; what does
# not fit the first waits for the next hand-back.
MAX_TOTAL_BYTES = 500 * 1024 * 1024
MAX_FILE_BYTES = 200 * 1024 * 1024

# The listing of the stand-in's folder is refused past this.
_LISTING_CAP = 32 * 1024 * 1024
# A tar member's header, a long name's extra header, and padding: the archive
# packed on the cloud is refused past the caps plus this per file.
_MEMBER_OVERHEAD = 3 * 512
# The notes file every folder is seeded with is no work of a turn's.
_SEEDED = "agent.md"
# Ends the unpack script's output, before the folder it filled: tar's warnings
# share the output.
_DEST_MARK = "HANDBACK_DEST="

# (size, change time as ``find`` printed it), by path in the stand-in's folder.
Listing = Dict[str, Tuple[int, str]]


@dataclass
class Pick:
    """What goes this time (path to its listed size and change time), what is
    too large ever to go, and what waits for the next hand-back for room."""

    chosen: Listing = field(default_factory=dict)
    too_large: List[str] = field(default_factory=list)
    deferred: List[str] = field(default_factory=list)


@dataclass
class Moved:
    """One hand-back's outcome. ``folder`` is where in Home the files went,
    relative to Home; None when nothing went. ``carried`` is each path copied
    with its listed ``[size, change time]``."""

    folder: Optional[str] = None
    carried: Dict[str, List[Any]] = field(default_factory=dict)
    too_large: List[str] = field(default_factory=list)
    unreadable: List[str] = field(default_factory=list)
    deferred: List[str] = field(default_factory=list)
    # Removes what is left of the copy's staging once its outcome is recorded.
    stage: Optional[Tuple[Any, str]] = None

    async def clean_up(self) -> None:
        if self.stage is None:
            return
        runtime, path = self.stage
        try:
            await runtime.exec(f"rm -rf -- {shlex.quote(path)}", timeout=60)
        except Exception:  # noqa: BLE001 - a hidden folder left in Home is harmless
            logger.info("Could not remove hand-back staging %s", path, exc_info=True)


def _carriable(path: str) -> bool:
    """A path below the folder, none of its parts hidden: a hidden part is the
    harness's own, and ``.``/``..`` are hidden too."""
    return bool(path) and all(
        part and not part.startswith(".") for part in path.split("/")
    )


def parse_listing(data: bytes) -> Tuple[Listing, List[str]]:
    """``find -printf '%s\\t%T@\\t%P\\0'`` records: the files that may go, and
    the names that are not UTF-8, which cannot be named in Home's records and
    stay on the cloud. Split on NUL, so any other character a name holds
    (a newline, a tab after the first two) is its own."""
    listed: Listing = {}
    unreadable: List[str] = []
    for record in data.split(b"\0"):
        size, _, rest = record.partition(b"\t")
        mtime, _, raw = rest.partition(b"\t")
        if not raw or not size.isdigit():
            continue
        try:
            path = raw.decode("utf-8")
        except UnicodeDecodeError:
            name = raw.decode("utf-8", "replace")
            if _carriable(name):
                unreadable.append(name)
            continue
        if _carriable(path):
            listed[path] = (int(size), mtime.decode("ascii", "replace"))
    return listed, sorted(unreadable)


def pick_files(listed: Listing, handed_back: Mapping[str, Any]) -> Pick:
    """Every file that differs from, or is absent in, what was handed back
    before, in path order, within the caps."""
    pick = Pick()
    total = 0
    for path in sorted(listed):
        size, mtime = listed[path]
        before = handed_back.get(path)
        if before is not None and list(before) == [size, mtime]:
            continue
        if size > MAX_FILE_BYTES:
            pick.too_large.append(path)
        elif total + size > MAX_TOTAL_BYTES:
            pick.deferred.append(path)
        else:
            total += size
            pick.chosen[path] = (size, mtime)
    return pick


def rebuild_archive(raw: Path, out: Path, chosen: Listing) -> Moved:
    """Copy the regular files ``chosen`` names from the archive at ``raw``
    into a gzipped archive at ``out``, each owned by nobody and mode 0644.

    Anything else in ``raw`` is dropped: links, devices, folders, a name not
    asked for or asked for twice, one that escapes the folder. A file asked
    for and not found as a regular file is named ``unreadable``; one whose
    packed size broke a cap is too large or waits, as if listed so."""
    moved = Moved()
    seen: set[str] = set()
    total = 0
    with (
        tarfile.open(raw, mode="r|") as source,
        tarfile.open(
            out, mode="w:gz", format=tarfile.PAX_FORMAT, compresslevel=6
        ) as target,
    ):
        for member in source:
            name = member.name
            if name not in chosen or name in seen or not member.isreg():
                continue
            try:
                tarfile.data_filter(member, "/handback")
            except tarfile.FilterError:
                continue
            seen.add(name)
            if member.size > MAX_FILE_BYTES:
                moved.too_large.append(name)
                continue
            if total + member.size > MAX_TOTAL_BYTES:
                moved.deferred.append(name)
                continue
            data = source.extractfile(member)
            if data is None:
                continue
            info = tarfile.TarInfo(name)
            info.size = member.size
            info.mtime = member.mtime
            info.mode = 0o644
            target.addfile(info, data)
            total += member.size
            moved.carried[name] = list(chosen[name])
    moved.unreadable = sorted(set(chosen) - seen)
    return moved


def unpack_script(home: str, stage: str, base: str, day: str) -> str:
    """Unpack the parts under ``<stage>/parts`` into ``<stage>/files``, then
    rename that to the first free ``<home>/<base>/<day>[ n]`` and print it.

    The folder appears whole or not at all, and a name something already
    holds is never reused. Flags GNU tar and bsdtar (macOS) both take: the
    files belong to whoever runs it, with the modes its umask gives."""
    return (
        "set -e; "
        f"stage={shlex.quote(stage)}; "
        f"base={shlex.quote(f'{home}/{base}/{day}')}; "
        'mkdir -p "$stage/files"; '
        'cat "$stage"/parts/* | tar -x -z -k --no-same-owner --no-same-permissions'
        ' -f - -C "$stage/files"; '
        "dest=$base; n=2; "
        'while [ -e "$dest" ]; do dest="$base $n"; n=$((n+1)); done; '
        'mkdir -p "$(dirname "$dest")"; '
        'mv "$stage/files" "$dest"; '
        f"printf '\\n{_DEST_MARK}%s' \"$dest\""
    )


async def _run(
    runtime: Any, command: str, *, timeout: int = 120, ok: Tuple[int, ...] = (0,)
) -> str:
    result = await runtime.exec(command, timeout=timeout)
    if result.exit_code not in ok:
        raise RuntimeError(
            f"exit {result.exit_code}: {(result.stdout or result.stderr or '')[-400:]}"
        )
    return result.stdout or ""


async def _download(runtime: Any, path: str, dest: Path, cap: int) -> None:
    """Stream ``path`` into ``dest``, refusing it once it passes ``cap``."""
    written = 0
    with dest.open("wb") as out:
        async for chunk in runtime.download_file_stream(path):
            written += len(chunk)
            if written > cap:
                raise RuntimeError(f"{path} is larger than the {cap} bytes expected")
            await asyncio.to_thread(out.write, chunk)


async def _upload_in_parts(runtime: Any, source: Path, folder: str) -> None:
    """``source`` into numbered parts under ``folder``, one chunk in memory at
    a time: every runtime's upload holds a whole file."""
    with source.open("rb") as f:
        index = 0
        while chunk := await asyncio.to_thread(f.read, STREAM_CHUNK_BYTES):
            await runtime.upload_file(chunk, f"{folder}/{index:06d}")
            index += 1


async def _folder_of(manager: Any, workspace_id: str, user_id: str) -> Tuple[Any, str]:
    """A workspace's computer runtime, started if it has to be, and its folder there."""
    from src.server.database.workspace import get_workspace
    from src.server.services.computer_usage.causes import computer_cause
    from src.server.services.workspace_files import layout_for

    with computer_cause("files", client=None, detail="home_handback"):
        session = await manager.get_session_for_workspace(workspace_id, user_id=user_id)
    row = await get_workspace(workspace_id)
    if row is None:
        raise RuntimeError(f"workspace {workspace_id} is gone")
    return session.sandbox.runtime, layout_for(row, manager=manager).workspace.rstrip(
        "/"
    )


async def _is_seeded(runtime: Any, folder: str, size: int) -> bool:
    from src.server.services.computer_manager._types import _SEEDED_AGENT_MD

    seeded = _SEEDED_AGENT_MD.encode()
    if size != len(seeded):
        return False
    return await runtime.download_file(f"{folder}/{_SEEDED}") == seeded


async def _pack_on_cloud(
    runtime: Any, folder: str, chosen: Listing, token: str, raw: Path
) -> None:
    """Pack ``chosen`` on the cloud and bring the archive to ``raw``. A file
    that changes or goes as it is read is a warning, not a failure: the
    rebuild names what did not arrive."""
    list_path = f"/tmp/handback-{token}.list"
    tar_path = f"/tmp/handback-{token}.tar"
    await runtime.upload_file(("\0".join(chosen) + "\0").encode(), list_path)
    try:
        await _run(
            runtime,
            f"tar cf {tar_path} -C {shlex.quote(folder)} --null --ignore-failed-read"
            f" -T {list_path}",
            timeout=600,
            ok=(0, 1),
        )
        cap = MAX_TOTAL_BYTES + _MEMBER_OVERHEAD * (len(chosen) + 2) + (1 << 20)
        await _download(runtime, tar_path, raw, cap)
    finally:
        try:
            await runtime.exec(f"rm -f {list_path} {tar_path}", timeout=30)
        except Exception:  # noqa: BLE001 - /tmp on the cloud is cleared on its own
            logger.info("Could not remove %s on the cloud", tar_path, exc_info=True)


async def move_into_home(
    user_id: str, handed_back: Mapping[str, Any], *, base: str, day: str
) -> Moved:
    """Copy what the stand-in holds that differs from ``handed_back`` into
    ``<base>/<day>[ n]`` in Home.

    Home's computer is opened first, so a Home that cannot take the files
    packs nothing. Raises when either computer can't be used, or the copy did
    not land; nothing then is in Home's folders. The caller records the
    outcome before ``Moved.clean_up``, so a cleanup that fails never repeats
    the copy."""
    from src.server.database.home_workspace import (
        get_home_stand_in_id,
        get_home_workspace_id,
    )
    from src.server.database.workspace_folders import workspace_folder_in_use
    from src.server.services.workspace_manager import WorkspaceManager

    manager = WorkspaceManager.get_instance()
    home_id = get_home_workspace_id(user_id)
    token = uuid.uuid4().hex[:12]
    # Home's folder stays where it is for the copy.
    async with workspace_folder_in_use(home_id):
        home_runtime, home_dir = await _folder_of(manager, home_id, user_id)
        stage = f"{home_dir}/.handback-{token}"
        # Only one hand-back per user runs at a time, so any staging left in
        # Home is an earlier one's that never finished.
        await _run(
            home_runtime,
            f"rm -rf -- {shlex.quote(home_dir)}/.handback-* && "
            f"mkdir -p {shlex.quote(stage)}/parts",
        )
        try:
            moved = await _copy(
                manager,
                user_id,
                handed_back,
                home_runtime,
                home_dir,
                stage,
                base,
                day,
                token=token,
                stand_in_id=get_home_stand_in_id(user_id),
            )
        except Exception:
            await Moved(stage=(home_runtime, stage)).clean_up()
            raise
    moved.stage = (home_runtime, stage)
    return moved


async def _copy(
    manager: Any,
    user_id: str,
    handed_back: Mapping[str, Any],
    home_runtime: Any,
    home_dir: str,
    stage: str,
    base: str,
    day: str,
    *,
    token: str,
    stand_in_id: str,
) -> Moved:
    cloud_runtime, cloud_dir = await _folder_of(manager, stand_in_id, user_id)
    listing_path = f"/tmp/handback-{token}.files"
    with tempfile.TemporaryDirectory(prefix="handback-") as tmp:
        listing_file = Path(tmp) / "listing"
        await _run(
            cloud_runtime,
            f"cd {shlex.quote(cloud_dir)} && find . -type f "
            f"-printf '%s\\t%T@\\t%P\\0' > {listing_path}",
        )
        try:
            await _download(cloud_runtime, listing_path, listing_file, _LISTING_CAP)
        finally:
            await cloud_runtime.exec(f"rm -f {listing_path}", timeout=30)
        listed, unreadable = parse_listing(listing_file.read_bytes())
        pick = pick_files(listed, handed_back)
        seeded = pick.chosen.get(_SEEDED)
        if seeded and await _is_seeded(cloud_runtime, cloud_dir, seeded[0]):
            del pick.chosen[_SEEDED]
        if not pick.chosen:
            return Moved(too_large=pick.too_large, unreadable=unreadable)

        raw, clean = Path(tmp) / "raw.tar", Path(tmp) / "clean.tgz"
        await _pack_on_cloud(cloud_runtime, cloud_dir, pick.chosen, token, raw)
        moved = await asyncio.to_thread(rebuild_archive, raw, clean, pick.chosen)
        moved.too_large = sorted([*pick.too_large, *moved.too_large])
        moved.deferred = sorted([*pick.deferred, *moved.deferred])
        moved.unreadable = sorted([*unreadable, *moved.unreadable])
        if not moved.carried:
            return moved
        await _upload_in_parts(home_runtime, clean, f"{stage}/parts")

    out = await _run(
        home_runtime, unpack_script(home_dir, stage, base, day), timeout=600
    )
    _, mark, folder = out.rpartition(_DEST_MARK)
    if not mark:
        raise RuntimeError(f"unpacking named no folder: {out[-400:]}")
    folder = folder.strip()
    moved.folder = (
        folder[len(home_dir) :].lstrip("/") if folder.startswith(home_dir) else folder
    )
    return moved
