"""What the mount serves of past threads: each workspace's transcripts and
the computer's thread index. Both are renders of server state, so both are
read-only. Where the mount serves, the file tools reach them through the
sandbox paths it links in, so a search over hundreds of megabytes runs in the
sandbox rather than on the server; a machine it never serves gets them as
filesystem routes instead (``history_files``).

Each is a ``MountRoute`` of its own, listing a directory from the stored
digests rather than every file's content. The transcripts list as a tree, so
a search over them costs the sandbox one request rather than one per thread.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
from collections import Counter, OrderedDict
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from ptc_agent.agent.transcript import transcript_subdir
from ptc_agent.core.paths import THREAD_DIR_NAME, SandboxLayout, WorkspaceLayout
from ptc_agent.core.sandbox.livefs_runtime.protocol import Refusal
from src.server.services import transcripts
from src.server.services.livefs.cache import client as _cache
from src.server.services.livefs.cache import tag
from src.server.services.livefs.routes import InlineBudget, LivefsError, Removed, Saved, version_of

logger = logging.getLogger(__name__)

# A version's size never changes, so this only has to outlive the version.
_INDEX_SIZE_TTL_S = 86400

# Blob bytes a read keeps, per process.
_READ_CACHE_BYTES = 64 * 1024 * 1024

# Files one tree listing names at most: a few hundred threads, which keeps the
# answer to about a quarter of a megabyte before any content.
_TREE_MAX_FILES = 2000


def _file(name: str, size: int, version: str) -> dict[str, Any]:
    return {
        "name": name,
        "type": "file",
        "size": size,
        "version": version,
        "writable": False,
    }


def _dir(name: str) -> dict[str, Any]:
    return {"name": name, "type": "dir"}


def _directories(
    files: dict[str, tuple[str, int]], under: str
) -> dict[str, dict[str, dict[str, Any]]]:
    """Each directory at or under ``under`` by its path relative to it, with
    its entries by name, from every file's sha256 and size by its path."""
    base = f"{under}/" if under else ""
    directories: dict[str, dict[str, dict[str, Any]]] = {}
    for file_path, (sha256, size) in files.items():
        if not file_path.startswith(base):
            continue
        *folders, name = file_path[len(base) :].split("/")
        parent = ""
        for folder in folders:
            directories.setdefault(parent, {}).setdefault(folder, _dir(folder))
            parent = f"{parent}/{folder}" if parent else folder
        directories.setdefault(parent, {})[name] = _file(name, size, sha256[:32])
    return directories


def _sorted(entries: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(entries.values(), key=lambda e: e["name"])


def _read_only(path: str) -> LivefsError:
    return LivefsError(Refusal.READ_ONLY, f"{path} is read only", path)


class _ReadOnly:
    root_prefix: str

    def is_writable(self, path: str) -> bool:
        return False

    async def movable(self, path: str) -> bool:
        return False

    async def write(self, path: str, content: str, version: str | None) -> Saved:
        raise _read_only(path)

    async def delete(self, path: str, version: str | None = None) -> Removed | None:
        raise _read_only(path)

    async def rename(self, path: str, to: str) -> None:
        return None

    def _relative(self, path: str) -> str | None:
        base = self.root_prefix.rstrip("/")
        if path.rstrip("/") == base:
            return ""
        if path.startswith(base + "/"):
            return path[len(base) + 1 :].strip("/")
        return None


class _VerifiedBytes:
    """Blob bytes already fetched and checked against their digest, by
    (user, sha256), within a byte budget, the least recently read going first.

    Bytes under a digest never change, so a copy here is never stale, and a
    read resolves the row naming the digest, which is what authorizes it,
    before asking. Concurrent misses for one digest share a fetch.
    """

    def __init__(self, budget: int) -> None:
        self._budget = budget
        self._size = 0
        self._held: OrderedDict[tuple[str, str], bytes] = OrderedDict()
        self._fetching: dict[tuple[str, str], asyncio.Future[bytes]] = {}

    async def get(
        self, key: tuple[str, str], fetch: Callable[[], Awaitable[bytes]]
    ) -> bytes:
        data = self._held.get(key)
        if data is not None:
            self._held.move_to_end(key)
            return data
        pending = self._fetching.get(key)
        if pending is None or pending.get_loop() is not asyncio.get_running_loop():
            pending = asyncio.ensure_future(fetch())
            self._fetching[key] = pending
            pending.add_done_callback(lambda done: self._settle(key, done))
        # Shielded: a reader that goes away leaves the fetch to the others.
        return await asyncio.shield(pending)

    def _settle(self, key: tuple[str, str], done: asyncio.Future[bytes]) -> None:
        if self._fetching.get(key) is done:
            del self._fetching[key]
        if done.cancelled() or done.exception() is not None:
            return
        data = done.result()
        # One file never takes more than a quarter of the budget.
        if key in self._held or len(data) > self._budget // 4:
            return
        self._held[key] = data
        self._size += len(data)
        while self._size > self._budget:
            _, evicted = self._held.popitem(last=False)
            self._size -= len(evicted)


_verified = _VerifiedBytes(_READ_CACHE_BYTES)

# Threads of their own for transcript downloads, which a search makes many of
# at once: on the default pool they would hold every other thread call in
# the worker, the event loop's DNS lookups included, behind storage. Its size
# is how many any number of searches fetch at once.
_DOWNLOADS = ThreadPoolExecutor(max_workers=8, thread_name_prefix="transcript-fetch")


async def blob_bytes(user_id: str, sha256: str) -> bytes:
    """A transcript file's blob, for a caller holding the row that names it."""
    from src.server.database.workspace_file_blobs import fetch_blob

    return await _verified.get(
        (user_id, sha256), lambda: fetch_blob(user_id, sha256, executor=_DOWNLOADS)
    )


async def read_file_with_digest(
    workspace_id: str, short_id: str, path: str
) -> tuple[bytes, str] | None:
    """One file of a thread's stored transcript, by its directory name and
    the path inside it, with the sha256 its listing gave; None when there is
    no such file. The digest names these exact bytes, so a caller needs no
    hash of its own."""
    from src.server.database.thread_transcripts import load_transcript_file

    row = await load_transcript_file(workspace_id, short_id, path)
    if row is None:
        return None
    if row.content is not None:
        return row.content, row.sha256
    return await blob_bytes(row.user_id, row.sha256), row.sha256


class TranscriptRoute(_ReadOnly):
    """One workspace's stored transcripts, a directory per thread."""

    def __init__(self, workspace_id: str, layout: WorkspaceLayout) -> None:
        self._workspace_id = workspace_id
        self.root_prefix = layout.transcripts + "/"

    async def list(self, path: str) -> list[dict[str, Any]] | None:
        from src.server.database import thread_transcripts as db

        relative = self._relative(path)
        if relative is None:
            return None
        if not relative:
            ids = Counter(t[:8] for t in await db.workspace_transcripts(self._workspace_id))
            return [_dir(name) for name in sorted(ids) if ids[name] == 1]
        short_id, _, inner = relative.partition("/")
        if not THREAD_DIR_NAME.match(short_id):
            return None
        files = await db.list_transcript(self._workspace_id, short_id)
        if files is None:
            return None
        entries = _directories(files, inner).get("")
        return None if entries is None else _sorted(entries)

    async def list_tree(
        self, path: str, inline: InlineBudget
    ) -> dict[str, list[dict[str, Any]]] | None:
        """One query for every thread's files, or one thread's, and one for
        the bytes of those the listing carries, taken in the order a search
        walks them."""
        from src.server.database import thread_transcripts as db

        relative = self._relative(path)
        if relative is None:
            return None
        short_id = relative.partition("/")[0]
        if short_id and not THREAD_DIR_NAME.match(short_id):
            return None
        listed = await db.list_transcript_tree(
            self._workspace_id, short_id or None, _TREE_MAX_FILES + 1
        )
        cut = len(listed) > _TREE_MAX_FILES
        if cut:
            # The last thread may be cut short, and a directory is listed
            # whole or not at all: it is listed when a command reaches it.
            listed = [f for f in listed if f.short_id != listed[-1].short_id]
        files = {f"{f.short_id}/{f.path}": f for f in listed}
        directories = _directories(
            {name: (f.sha256, f.byte_len) for name, f in files.items()}, relative
        )
        if not relative:
            directories[""] = (
                {e["name"]: e for e in await self.list(path) or ()}
                if cut
                else directories.get("", {})
            )
        elif "" not in directories:
            return None
        base = f"{relative}/" if relative else ""
        carried = [
            f
            for name, f in sorted(files.items())
            if name.startswith(base) and inline.take(f.byte_len)
        ]
        for f, data in (await db.load_transcript_contents(carried)).items():
            name = f"{f.short_id}/{f.path}"
            parent, _, filename = name[len(base) :].rpartition("/")
            directories[parent][filename]["content"] = data.decode()
        return {parent: _sorted(entries) for parent, entries in directories.items()}

    async def read_bytes(self, path: str) -> tuple[bytes, str] | None:
        """The file at ``path`` undecoded, with its sha256."""
        relative = self._relative(path)
        if not relative:
            return None
        short_id, _, inner = relative.partition("/")
        if not inner or not THREAD_DIR_NAME.match(short_id):
            return None
        return await read_file_with_digest(self._workspace_id, short_id, inner)

    async def read(self, path: str) -> tuple[str, str] | None:
        found = await self.read_bytes(path)
        if found is None:
            return None
        data, sha256 = found
        return data.decode(), sha256[:32]


def transcript_dir(layout: WorkspaceLayout, thread_id: str) -> str:
    return layout.join(transcript_subdir(thread_id[:8]))


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def index_content(root: str, rows: list[dict[str, Any]]) -> str:
    """The computer's thread index: one line per thread of a live folder,
    with where its transcript is, or null when none is served there: before
    its first turn ends, or while another of its workspace's transcripts
    shares the directory name."""
    machine = SandboxLayout.for_root(root)
    named = Counter(
        (str(row["workspace_id"]), str(row["conversation_thread_id"])[:8])
        for row in rows
        if row.get("has_transcript")
    )
    lines = []
    for row in rows:
        # A folder staged mid-move is under _internal until it lands.
        if not row.get("dir_name") or "/" in row["dir_name"]:
            continue
        thread_id = str(row["conversation_thread_id"])
        entry = {
            "thread_id": thread_id,
            "title": row.get("title"),
            "workspace": row.get("workspace_name"),
            "workspace_id": str(row["workspace_id"]),
            "created_at": _iso(row.get("created_at")),
            "updated_at": _iso(row.get("updated_at")),
            "transcript": (
                transcript_dir(machine.for_workspace(row["dir_name"]), thread_id)
                if named[(str(row["workspace_id"]), thread_id[:8])] == 1
                and row.get("has_transcript")
                else None
            ),
        }
        lines.append(json.dumps(entry, ensure_ascii=False, default=str))
    return "".join(line + "\n" for line in lines)


@functools.cache
def _index_shape() -> str:
    """What the index makes of one fixed thread, which changes with its
    format: the daemon keeps a file's bytes by version for as long as it
    runs, past a deploy that renders the same threads differently."""
    at = datetime(2000, 1, 1, tzinfo=UTC)
    probe = {
        "conversation_thread_id": "00000000-0000-0000-0000-000000000000",
        "title": "t",
        "workspace_id": "00000000-0000-0000-0000-000000000001",
        "workspace_name": "w",
        "dir_name": "w",
        "created_at": at,
        "updated_at": at,
        "has_transcript": True,
    }
    return version_of(index_content("/r", [probe]))


def _size_key(computer_id: str, version: str) -> str:
    return f"{tag(computer_id)}:index:{version}"


class IndexRoute(_ReadOnly):
    """The computer's thread index, generated from the database per read.

    Its version comes from a digest of every field it shows, which the
    database computes without sending the threads, so a listing costs one
    small query rather than the whole render. The size needs the render, so
    it is kept by version, which fixes the bytes; a listing that finds none
    renders the file and carries its content, since a read usually follows.
    """

    def __init__(self, computer_id: str, root: str) -> None:
        self._computer_id = computer_id
        self._root = root
        self._path = transcripts.index_path(root)
        self.root_prefix = self._path.rsplit("/", 1)[0] + "/"

    def _version(self, digest: str) -> str:
        return version_of(f"{_index_shape()}\n{self._root}\n{digest}")

    async def _render(self) -> tuple[str, str, int]:
        """The index, its version and size, from one snapshot of the threads."""
        from src.server.database.conversation import list_computer_threads

        digest, rows = await list_computer_threads(self._computer_id)
        text = index_content(self._root, rows)
        version, size = self._version(digest), len(text.encode())
        cache = _cache()
        if cache is not None:
            try:
                await cache.set(
                    _size_key(self._computer_id, version), size, ex=_INDEX_SIZE_TTL_S
                )
            except Exception:
                logger.debug("livefs index size not kept", exc_info=True)
        return text, version, size

    async def _known(self, cache: Any) -> tuple[str, int] | None:
        """The current version and its size, when a render already found it."""
        from src.server.database.conversation import list_computer_threads

        digest, _ = await list_computer_threads(self._computer_id, rows=False)
        version = self._version(digest)
        try:
            size = await cache.get(_size_key(self._computer_id, version))
        except Exception:
            logger.debug("livefs index size not read", exc_info=True)
            return None
        return (version, int(size)) if size is not None else None

    async def list_tree(self, path: str, inline: InlineBudget) -> None:
        return None

    async def list(self, path: str) -> list[dict[str, Any]] | None:
        if self._relative(path) != "":
            return None
        cache = _cache()
        known = await self._known(cache) if cache is not None else None
        if known is not None:
            return [_file(transcripts.INDEX, known[1], known[0])]
        text, version, size = await self._render()
        return [{**_file(transcripts.INDEX, size, version), "content": text}]

    async def read(self, path: str) -> tuple[str, str] | None:
        if path != self._path:
            return None
        text, version, _ = await self._render()
        return text, version
