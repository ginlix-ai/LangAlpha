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
and mtime as listed), not their age: a file unpacked or copied keeps an
old mtime, and a sandbox clock may be behind.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
import tarfile
import tempfile
import unicodedata
import uuid
from contextlib import aclosing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ptc_agent.core.paths import TRANSIENT_ROOT_PREFIX, WorkspaceLayout
from ptc_agent.core.sandbox.runtime import STREAM_CHUNK_BYTES
from src.server.services.persistence.transfer import (
    EXCLUDE_BASENAMES,
    EXCLUDE_DIR_NAMES,
    EXCLUDE_ROOT_DIRS,
    EXCLUDE_SUFFIXES,
    SYNC_MARKER_NAME,
)

logger = logging.getLogger(__name__)

# What one hand-back carries across, and the largest single file. A file over
# the second is left on the cloud computer and named in the notice; what does
# not fit the first waits for the next hand-back.
MAX_TOTAL_BYTES = 500 * 1024 * 1024
MAX_FILE_BYTES = 200 * 1024 * 1024

# A backup keeps a repository whole or not at all (``persistence/repos.py``);
# a hand-back carries files under per-file and per-run caps and leaves what
# does not fit for the next one, which would land half of a history in Home.
_NOT_CARRIED_DIR_NAMES = EXCLUDE_DIR_NAMES | {".git"}

# The listing of the stand-in's folder is refused past this.
_LISTING_CAP = 32 * 1024 * 1024
# A tar block, and the record GNU tar pads an archive to.
_BLOCK, _RECORD = 512, 20 * 512
# The longest target a symlink may name (PATH_MAX).
_LINK_MAX = 4096
# The notes file every folder is seeded with is no work of a turn's.
_SEEDED = "agent.md"
# Ends the unpack script's output, before the folder it filled: tar's warnings
# share the output.
_DEST_MARK = "HANDBACK_DEST="
# A hand-back's staging folder in Home: this, then 12 hex digits.
_STAGE = ".handback-"

# (size, mtime as ``find`` printed it), by path in the stand-in's folder.
Listing = Dict[str, Tuple[int, str]]


@dataclass
class Pick:
    """What goes this time (path to its listed size and mtime), what is
    too large ever to go, what waits for the next hand-back for room, and
    what an earlier hand-back already carried as it is now."""

    chosen: Listing = field(default_factory=dict)
    too_large: List[str] = field(default_factory=list)
    deferred: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)


@dataclass
class Moved:
    """One hand-back's outcome. ``folder`` is where in Home the files went,
    relative to Home; None when nothing went. ``carried`` is each path copied
    with its listed ``[size, mtime]``."""

    folder: Optional[str] = None
    carried: Dict[str, List[Any]] = field(default_factory=dict)
    too_large: List[str] = field(default_factory=list)
    unreadable: List[str] = field(default_factory=list)
    deferred: List[str] = field(default_factory=list)
    #: Paths listed as an earlier hand-back carried them: a card of a turn
    #: before this hand-back opens that one's copy, not the stand-in's.
    kept: List[str] = field(default_factory=list)
    #: The computer Home was on while the files went in, read under its
    #: folder's hold, which a move waits for.
    computer_id: Optional[str] = None
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


def is_note(path: str) -> bool:
    """A file of a thread's checkpoint notes (``.agents/scratchpad/<t8>/note/``).
    Home's turns read their notes where a summary left them, so these go to
    the same path in Home, not into the dated folder."""
    parts = path.split("/")
    return (
        len(parts) >= 5
        and "/".join(parts[:2]) == WorkspaceLayout.SCRATCHPAD_DIR
        and parts[3] == WorkspaceLayout.SCRATCHPAD_NOTE_DIR
    )


def _carriable(path: str) -> bool:
    """A path below the folder that a backup of it keeps, less the agent's own
    ``.agents`` tree, whose files mean nothing in a folder of Home's, but for
    the threads' checkpoint notes. A file the user made hidden
    (``.gitignore``, ``.github/``) goes like any other."""
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        return False
    head, name = parts[0], parts[-1]
    if (head == WorkspaceLayout.AGENTS_DIR and not is_note(path)) or head in EXCLUDE_ROOT_DIRS:
        return False
    if len(parts) == 1 and (
        name == SYNC_MARKER_NAME or name.startswith(TRANSIENT_ROOT_PREFIX)
    ):
        return False
    return (
        _NOT_CARRIED_DIR_NAMES.isdisjoint(parts[:-1])
        and name not in EXCLUDE_BASENAMES
        and not name.endswith(tuple(EXCLUDE_SUFFIXES))
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


def _folded(path: str) -> str:
    """``path`` as a case-insensitive, normalizing volume (a Mac's) names it."""
    return unicodedata.normalize("NFD", path).casefold()


def pick_files(listed: Listing, handed_back: Mapping[str, Any]) -> Pick:
    """Every file that differs from, or is absent in, what was handed back
    before, in path order, within the caps.

    Names one hand-back cannot both unpack on a Mac, which differ only in
    case or Unicode form (``Report.csv`` and ``report.csv``, or a file and a
    folder so spelled), go one per pass: the rest wait, so none is dropped
    while counted as carried."""
    pick = Pick()
    total = 0
    files: set[str] = set()
    folders: set[str] = set()
    for path in sorted(listed):
        size, mtime = listed[path]
        before = handed_back.get(path)
        if before is not None and list(before) == [size, mtime]:
            pick.unchanged.append(path)
            continue
        folded = _folded(path)
        parents = {folded.rsplit("/", n)[0] for n in range(1, folded.count("/") + 1)}
        if folded in files or folded in folders or parents & files:
            pick.deferred.append(path)
        elif size > MAX_FILE_BYTES:
            pick.too_large.append(path)
        elif total + size > MAX_TOTAL_BYTES:
            pick.deferred.append(path)
        else:
            total += size
            pick.chosen[path] = (size, mtime)
            files.add(folded)
            folders |= parents
    return pick


def _up(n: int, to: int = _BLOCK) -> int:
    return -(-n // to) * to


def packed_cap(chosen: Listing) -> int:
    """The most the archive of ``chosen`` packed on the cloud may weigh, in
    GNU format: each file's header and data padding, a long name's extra
    header and payload, the end blocks and the record's padding. Data counts
    at the whole cap, so a file grown since its listing waits rather than
    failing the download."""
    meta = 0
    for path, (size, _) in chosen.items():
        meta += _BLOCK + _BLOCK - 1
        name = len(path.encode())
        if name >= 100:
            meta += _BLOCK + _up(name + 1)
        # A link's listed size is its target's length, which a long one
        # carries in a header of its own; a file's room for it is spare.
        if size >= 100:
            meta += _BLOCK + _up(min(size, _LINK_MAX) + 1)
    return _up(MAX_TOTAL_BYTES + meta + 2 * _BLOCK, _RECORD) + (1 << 20)


def rebuild_archive(raw: Path, out: Path, chosen: Listing) -> Moved:
    """Copy the regular files and symlinks ``chosen`` names from the archive
    at ``raw`` into a gzipped archive at ``out``, each owned by nobody, a
    file mode 0644.

    Anything else in ``raw`` is dropped: hard links, devices, folders, a name
    not asked for or asked for twice, one that escapes the folder. A link
    whose target leaves the folder is dropped too, holding nothing of it, so
    that nothing in Home points at a file of the user's own the cloud chose.
    A name asked for and not found as either is named ``unreadable``; one whose
    packed size broke a cap is too large or waits, as if listed so. One packed
    with another size or mtime than listed changed in between, so it waits
    rather than be recorded as the listed file it is not."""
    moved = Moved()
    seen: set[str] = set()
    total = 0
    with (
        # stream: a member read is not kept, so an archive of many headers
        # costs no memory.
        tarfile.open(raw, mode="r|", stream=True) as source,
        tarfile.open(
            out, mode="w:gz", format=tarfile.PAX_FORMAT, compresslevel=6
        ) as target,
    ):
        for member in source:
            name = member.name
            if name not in chosen or name in seen:
                continue
            if not (member.isreg() or member.issym()):
                continue
            try:
                tarfile.data_filter(member, "/handback")
            except tarfile.FilterError:
                if member.issym():
                    seen.add(name)
                continue
            seen.add(name)
            if member.issym():
                if _link_as_listed(member, chosen[name]):
                    link = tarfile.TarInfo(name)
                    link.type, link.linkname = tarfile.SYMTYPE, member.linkname
                    link.mtime = member.mtime
                    target.addfile(link)
                    moved.carried[name] = list(chosen[name])
                else:
                    moved.deferred.append(name)
                continue
            if not _as_listed(member, chosen[name]):
                moved.deferred.append(name)
                continue
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


def _as_listed(member: tarfile.TarInfo, listed: Tuple[int, str]) -> bool:
    """Whether ``member`` is the file the listing saw. The listing's mtime is
    fractional and a tar header's may be whole seconds, so seconds compare."""
    size, mtime = listed
    try:
        return member.size == size and int(member.mtime) == int(float(mtime))
    except ValueError:
        return False


def _link_as_listed(member: tarfile.TarInfo, listed: Tuple[int, str]) -> bool:
    """Whether link ``member`` is the one the listing saw, by the length of
    its target, which ``find`` gives as a link's size, and its mtime."""
    size, mtime = listed
    try:
        return len(member.linkname.encode()) == size and int(member.mtime) == int(
            float(mtime)
        )
    except ValueError:
        return False


def stage_script(home: str, stage: str) -> str:
    """Make ``stage`` for the parts, clearing what earlier hand-backs left.

    Only one hand-back per user runs at a time, so staging left in Home is an
    earlier one's that never finished: a folder named as ours are, holding
    the parts folder ours have. Nothing else in Home is touched."""
    return (
        f"cd -- {shlex.quote(home)} && for d in {_STAGE}{'[0-9a-f]' * 12}; do"
        ' if [ -d "$d/parts" ] && [ ! -L "$d" ]; then rm -rf -- "$d"; fi; done && '
        f"mkdir -p {shlex.quote(stage)}/parts"
    )


def unpack_script(home: str, stage: str, base: str, day: str) -> str:
    """Unpack the parts under ``<stage>/parts`` into a folder named as the
    stage is, then rename that to the first free ``<home>/<base>/<day>[ n]``
    and print it. Checkpoint notes go to their own path in Home instead,
    a note written there since kept; with nothing else, no folder is made
    and an empty one printed.

    The folder appears whole or not at all, and a name something already
    holds is never reused: a folder made at that name just before the rename
    takes the copy in under the stage's unique name, which is moved back out
    and tried at the next. Flags GNU tar and bsdtar (macOS) both take: the
    files belong to whoever runs it, with the modes its umask gives."""
    return (
        "set -e; "
        f"stage={shlex.quote(stage)}; "
        f"base={shlex.quote(f'{home}/{base}/{day}')}; "
        'name=$(basename "$stage"); held="$stage/$name"; '
        'mkdir -p "$held"; '
        'cat "$stage"/parts/* | tar -x -z -k --no-same-owner --no-same-permissions'
        ' -f - -C "$held"; '
        # Each note moved unless Home's is newer; one that can't go is left
        # rather than failing the copy. Not tar: GNU tar's keep-newer fails
        # on every folder that exists.
        'if [ -d "$held/.agents" ]; then '
        '(cd "$held" && find .agents -type f -exec sh -c \''
        'for f; do d="$0/$f"; if [ ! -d "$d" ] && [ ! "$d" -nt "$f" ]; then '
        'mkdir -p "$(dirname "$d")" && mv -f "$f" "$d"; fi; done; exit 0'
        f"' {shlex.quote(home)} {{}} +); "
        'rm -rf "$held/.agents"; fi; '
        f"""if [ -z "$(ls -A "$held")" ]; then printf '\\n{_DEST_MARK}'; exit 0; fi; """
        'mkdir -p "$(dirname "$base")"; '
        "dest=$base; n=2; "
        "while :; do "
        'while [ -e "$dest" ]; do dest="$base $n"; n=$((n+1)); done; '
        'mv "$held" "$dest"; '
        '[ -e "$dest/$name" ] || break; '
        'mv "$dest/$name" "$held"; '
        "done; "
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
    # Read no further than the seed: the file may have grown since its listing.
    got = bytearray()
    async with aclosing(runtime.download_file_stream(f"{folder}/{_SEEDED}")) as chunks:
        async for chunk in chunks:
            got += chunk
            if len(got) > len(seeded):
                return False
    return bytes(got) == seeded


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
            # GNU format, which ``packed_cap`` bounds; no recursion, so a
            # name turned into a folder since its listing adds nothing; each
            # hard-linked name as a file of its own, which the rebuild keeps.
            f"tar cf {tar_path} --format=gnu -C {shlex.quote(folder)} --null"
            f" --ignore-failed-read --no-recursion --hard-dereference"
            f" -T {list_path}",
            timeout=600,
            ok=(0, 1),
        )
        await _download(runtime, tar_path, raw, packed_cap(chosen))
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
    from src.server.database.computer import get_computer_for_workspace
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
        stage = f"{home_dir}/{_STAGE}{token}"
        await _run(home_runtime, stage_script(home_dir, stage))
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
        computer = await get_computer_for_workspace(home_id)
        moved.computer_id = str(computer["computer_id"]) if computer else None
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
            # Files, and links but those to an absolute path, which hold
            # nothing of the folder; a link's size is its target's length.
            f"cd {shlex.quote(cloud_dir)} && find . \\( -type f -o \\( -type l ! -lname '/*' \\) \\) "
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
            return Moved(
                too_large=pick.too_large,
                unreadable=unreadable,
                kept=pick.unchanged,
            )

        raw, clean = Path(tmp) / "raw.tar", Path(tmp) / "clean.tgz"
        await _pack_on_cloud(cloud_runtime, cloud_dir, pick.chosen, token, raw)
        moved = await asyncio.to_thread(rebuild_archive, raw, clean, pick.chosen)
        moved.kept = pick.unchanged
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
    ) or None
    return moved
