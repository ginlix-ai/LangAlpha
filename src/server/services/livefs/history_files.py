"""Past threads for the file tools on a machine the mount never serves.

Where the mount serves, the tools reach transcripts through its links and a
search runs in the sandbox. A machine it never serves (one brought over SSH)
gets the same files as read-only filesystem routes over the mount's own
routes, so Read and Grep see what Bash would there, from the rows. Grep then
runs here: the files are fetched a batch at a time and matched off the event
loop, and a long line comes back as windows around its matches, as rg's does.
Each search fetches the files anew, so it takes the newest conversations
first and stops at a byte limit, saying so in its answer.

A shared workspace's computer, mounted or not, serves only the threads shared
with the workspace. A turn in a thread its sender keeps private there reads
that thread's own transcript the same way (``ThreadHistory``), handed to that
turn alone.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from typing import Any

from ptc_agent.agent.backends import ReadOnlyStoreError
from ptc_agent.agent.backends.text_grep import PageGrep, compile_grep, grep_texts
from ptc_agent.agent.backends.results import GrepAnswer
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.core.paths import THREAD_DIR_NAME, SandboxLayout, WorkspaceLayout
from src.server.database.thread_transcripts import ON_COMPUTER, Scope, own_thread
from src.server.services import transcripts
from src.server.services.livefs.history import (
    IndexRoute,
    TranscriptRoute,
    blob_bytes,
    read_file_with_digest,
    transcript_dir,
)

logger = logging.getLogger(__name__)

_READ_ONLY = "Saved conversations are read only."

_BYTES_NOTE = (
    "[Grep searched {searched} of {listed} transcript files in {path}, the most "
    "recent conversations first, and stopped at its {limit} MB limit. Grep a "
    "narrower path, such as one conversation's folder, to search the rest.]"
)
_LINES_NOTE = (
    "[Grep stopped at {lines:,} matching lines in {path}, the most recent "
    "conversations first. Grep a narrower path or pattern to see the rest.]"
)

# Files one search or listing reads at most: past any computer's history.
_MAX_FILES = 20_000
# Bytes of transcript one search of a folder reads at most, the newest
# conversations first: a few seconds of fetching.
_SEARCH_BYTES = 128 * 1024 * 1024
# Bytes of transcript one batch of a search holds.
_BATCH_BYTES = 16 * 1024 * 1024
# Files one search asks storage for at once, so that searches side by side
# take turns at the download threads.
_FETCHES = 8
# Lines one content search of a folder's transcripts collects at most when
# the Grep sets no lower limit; past this many, paging further finds nothing
# more.
_MAX_LINES = 10_000
# A file longer than this is decoded and split into lines off the event loop.
_INLINE_READ_BYTES = 1024 * 1024


def _window(data: bytes | str, offset: int, limit: int) -> str:
    """Lines ``offset`` to ``offset + limit`` of a file, split as Grep here
    splits them."""
    text = data.decode() if isinstance(data, bytes) else data
    start = max(0, offset)
    return "".join(text.splitlines(keepends=True)[start : start + max(0, limit)])


async def _read_window(data: bytes | str, offset: int, limit: int) -> str:
    if len(data) > _INLINE_READ_BYTES:
        return await asyncio.to_thread(_window, data, offset, limit)
    return _window(data, offset, limit)


def _within(files: list[Any], budget: int) -> list[Any]:
    """The first of ``files`` that fit in ``budget`` bytes, and always the
    first one."""
    taken: list[Any] = []
    size = 0
    for f in files:
        if taken and size + f.byte_len > budget:
            break
        taken.append(f)
        size += f.byte_len
    return taken


class _ReadOnlyFiles:
    """A route the file tools read and search but never write."""

    refuses_writes = True
    # Behind the mount's links where it serves, which rg follows only when
    # given one as its path.
    searched_only_inside = True
    root_prefix: str

    async def aread_text(self, file_path: str) -> str | None:
        raise NotImplementedError

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        text = await self.aread_text(file_path)
        if text is None:
            return None
        return await _read_window(text, offset, limit)

    async def awrite_text(self, file_path: str, content: str, **_: Any) -> bool:
        raise ReadOnlyStoreError(_READ_ONLY)

    async def aedit_text(self, file_path: str, *_: Any, **__: Any) -> Any:
        raise ReadOnlyStoreError(_READ_ONLY)


class TranscriptFiles(_ReadOnlyFiles):
    """One workspace folder's transcripts, a directory per thread."""

    takes_walk = True

    def __init__(
        self, workspace_id: str, layout: WorkspaceLayout, *, scope: Scope = ON_COMPUTER
    ) -> None:
        self._workspace_id = workspace_id
        self._route = TranscriptRoute(workspace_id, layout, scope=scope)
        self.root_prefix = self._route.root_prefix

    async def _listed(self, path: str, *, newest_first: bool = False) -> list[Any]:
        from src.server.database import thread_transcripts as db

        relative = self._route._relative(path)
        if relative is None:
            return []
        short_id = relative.partition("/")[0]
        if short_id and not THREAD_DIR_NAME.match(short_id):
            return []
        listed = await db.list_transcript_tree(
            self._workspace_id,
            short_id or None,
            _MAX_FILES,
            newest_first=newest_first,
            scope=self._route.scope,
        )
        return [
            f
            for f in listed
            if not relative
            or f"{f.short_id}/{f.path}" == relative
            or f"{f.short_id}/{f.path}".startswith(relative + "/")
        ]

    def _absolute(self, listed: Any) -> str:
        return f"{self._route.root_prefix}{listed.short_id}/{listed.path}"

    def _searched(self, files: list[Any], searches: Callable[[str], bool]) -> list[Any]:
        return [f for f in files if searches(self._absolute(f))]

    async def aread_text(self, file_path: str) -> str | None:
        found = await self._route.read(file_path)
        return None if found is None else found[0]

    async def aread_range(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> str | None:
        found = await self._route.read_bytes(file_path)
        return None if found is None else await _read_window(found[0], offset, limit)

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        # The default start is the whole route, as every other route reads it.
        start = self.root_prefix if path == "." else path
        return [self._absolute(f) for f in await self._listed(start)]

    async def _contents(self, files: list[Any]) -> dict[Any, bytes]:
        """The bytes of ``files``: those their rows hold in one round trip,
        the rest from storage a few at a time."""
        from src.server.database import thread_transcripts as db
        from src.server.database.workspace_file_blobs import BlobFetchError

        held = await db.load_transcript_contents(files)
        gate = asyncio.Semaphore(_FETCHES)

        async def fetch(f: Any) -> tuple[Any, bytes | None]:
            async with gate:
                try:
                    return f, await blob_bytes(f.user_id, f.sha256)
                except BlobFetchError:
                    # No blob holds the listed bytes when they were in a row
                    # a save has replaced since; the file is read as it is now.
                    found = await read_file_with_digest(
                        self._workspace_id, f.short_id, f.path, self._route.scope
                    )
            return f, None if found is None else found[0]

        for f, data in await asyncio.gather(*(fetch(f) for f in files if f not in held)):
            if data is not None:
                held[f] = data
        return held

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        head_limit: int | None = None,
        searches: Callable[[str], bool] | None = None,
        **_: Any,
    ) -> list[Any]:
        cap = head_limit
        if output_mode == "content":
            cap = _MAX_LINES if cap is None else min(cap, _MAX_LINES)
        if cap is not None and cap <= 0:
            return []
        search = PageGrep(
            pattern,
            case_insensitive=case_insensitive,
            output_mode=output_mode,
            show_line_numbers=show_line_numbers,
            limit=cap,
        )
        listed = await self._listed(path, newest_first=True)
        if searches is not None:
            # Off the loop: a long filter costs about its length per file.
            listed = await asyncio.to_thread(self._searched, listed, searches)
        taken = _within(listed, _SEARCH_BYTES)

        def batches() -> Iterator[list[Any]]:
            batch: list[Any] = []
            size = 0
            for f in taken:
                if batch and size + f.byte_len > _BATCH_BYTES:
                    yield batch
                    batch, size = [], 0
                batch.append(f)
                size += f.byte_len
            if batch:
                yield batch

        async def pages() -> AsyncIterator[list[tuple[str, bytes]]]:
            # Fetched a batch at a time, a few blobs at once.
            for batch in batches():
                held = await self._contents(batch)
                yield [(self._absolute(f), held[f]) for f in batch if f in held]

        await search.search(pages())
        found = search.found
        where = path.rstrip("/")
        if cap is not None and len(found) >= cap:
            # A page the Grep's own limit filled needs no note.
            if cap == head_limit:
                return found
            note = _LINES_NOTE.format(lines=cap, path=where)
        elif len(taken) < len(listed):
            note = _BYTES_NOTE.format(
                searched=len(taken),
                listed=len(listed),
                path=where,
                limit=_SEARCH_BYTES // (1024 * 1024),
            )
        else:
            return found
        logger.info(
            "transcript grep of %s stopped short: %d of %d files listed",
            self._workspace_id,
            len(taken),
            len(listed),
        )
        return GrepAnswer(found, [note])


class ThreadFiles(TranscriptFiles):
    """One thread's transcript, for a turn in that thread whose sender keeps
    it private in a shared workspace: the computer's mount and its in-process
    routes serve only threads shared with the workspace, so this is the one
    path to it. Rooted at the thread's own directory, which the pointer a
    summary ends with names, so it wins over both there; every read names
    the thread and the sender, so it serves nothing the sender no longer
    reaches."""

    def __init__(
        self, workspace_id: str, layout: WorkspaceLayout, thread_id: str, sender: str
    ) -> None:
        super().__init__(workspace_id, layout, scope=own_thread(thread_id, sender))
        self.root_prefix = transcript_dir(layout, thread_id) + "/"
        # Where the mount serves transcripts, a Grep of their folder reaches
        # each thread's own through the folder's link; this one is reached
        # there too, or that Grep would say a thread's own history has no match.
        self.link_dir = posixpath.dirname(self.root_prefix.rstrip("/"))


class ThreadHistory:
    """``HistoryFiles`` for a turn in a thread its sender keeps private in a
    shared workspace: that thread's transcript alone, on any of the
    workspace's computers, mounted or not, and for a snapshot fork the
    transcripts of the threads it borrows turns from (``borrowed``), where its
    pointers send the agent for them, each while the sender still reaches it.
    Bash and code still do not see them, so the turn's reach says so and its
    pointers name the file tools."""

    def __init__(
        self,
        workspace_id: str,
        dir_name: str | None,
        thread_id: str,
        sender: str,
        borrowed: tuple[str, ...] = (),
    ) -> None:
        self._workspace_id = workspace_id
        self._dir_name = dir_name
        self._threads = (thread_id, *borrowed)
        self._sender = sender

    def routes(self, sandbox: Any) -> list[Any]:
        root = sandbox.computer_root.rstrip("/")
        layout = SandboxLayout.for_root(root).for_workspace(self._dir_name)
        return [
            ThreadFiles(self._workspace_id, layout, thread_id, self._sender)
            for thread_id in self._threads
        ]

    def serves(self, workspace_id: str | None) -> bool:
        return workspace_id is not None and str(workspace_id) == self._workspace_id

    async def save_transcript(
        self, target: TranscriptTarget, messages: Any, *, window: Any
    ) -> TranscriptTarget | None:
        return await transcripts.save_live(
            target, messages, window=window, reader=self._sender
        )


async def private_thread_history(
    workspace_id: str,
    dir_name: str | None,
    thread_id: str,
    sender: str | None,
    borrowed: tuple[str, ...] = (),
) -> ThreadHistory | None:
    """The history a turn in ``thread_id``, in the workspace folder
    ``dir_name``, reads its own transcript through, when the thread is
    ``sender``'s and not shared with its workspace; None otherwise. Read from
    the thread row at each build, so sharing the thread hands it back to the
    mount from the next turn on, and making it private again brings it back
    here."""
    from src.server.database.conversation.thread_reach import private_to

    if sender is None or str(await private_to(thread_id)) != str(sender):
        return None
    return ThreadHistory(str(workspace_id), dir_name, str(thread_id), str(sender), borrowed)


class IndexFile(_ReadOnlyFiles):
    """The computer's thread index, one file. Its root is the file itself,
    which Glob does not list; a read or a search names it."""

    def __init__(self, computer_id: str, root: str) -> None:
        self._route = IndexRoute(computer_id, root)
        self._path = transcripts.index_path(root)
        self.root_prefix = self._path + "/"

    async def aread_text(self, file_path: str) -> str | None:
        found = await self._route.read(file_path.rstrip("/"))
        return None if found is None else found[0]

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        return []

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        **_: Any,
    ) -> list[Any]:
        compiled = compile_grep(pattern, case_insensitive)
        text = await self.aread_text(self._path)
        if text is None:
            return []
        return await asyncio.to_thread(
            grep_texts,
            [(self._path, text)],
            compiled,
            output_mode,
            show_line_numbers=show_line_numbers,
            cut_long_lines=True,
        )


class ComputerHistory:
    """``HistoryFiles`` for a computer the mount never serves, over the
    workspace folders it held when the turn started."""

    def __init__(
        self,
        computer_id: str,
        folders: Sequence[tuple[str, str]],
        *,
        user_id: str | None = None,
    ) -> None:
        self._computer_id = computer_id
        self._folders = tuple(folders)
        self._user_id = user_id

    def routes(self, sandbox: Any) -> list[Any]:
        from src.server.database.home_workspace import acting_for

        root = sandbox.computer_root.rstrip("/")
        machine = SandboxLayout.for_root(root)
        # Home's stand-in serves Home's transcripts, as the mount's tree does.
        routes: list[Any] = [
            TranscriptFiles(
                acting_for(self._user_id, workspace_id), machine.for_workspace(folder)
            )
            for workspace_id, folder in self._folders
        ]
        routes.append(IndexFile(self._computer_id, root))
        return routes

    def serves(self, workspace_id: str | None) -> bool:
        return workspace_id is not None and any(
            folder_id == str(workspace_id) for folder_id, _ in self._folders
        )

    async def save_transcript(
        self, target: TranscriptTarget, messages: Any, *, window: Any
    ) -> TranscriptTarget | None:
        return await transcripts.save_live(target, messages, window=window)


async def computer_history(computer_id: str) -> ComputerHistory:
    """The history of ``computer_id`` as its folders stand now."""
    from src.server.database.workspace import get_live_workspace_folders_for_computer

    rows = await get_live_workspace_folders_for_computer(str(computer_id))
    return ComputerHistory(
        str(computer_id),
        [
            (str(row["workspace_id"]), row["dir_name"])
            # A folder staged mid-move is under _internal until it lands.
            for row in rows
            if row.get("dir_name") and "/" not in row["dir_name"]
        ],
        # One user's computer; any row names them.
        user_id=next((str(row["user_id"]) for row in rows), None),
    )
