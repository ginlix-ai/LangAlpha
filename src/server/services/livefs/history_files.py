"""Past threads for the file tools on a machine the mount never serves.

Where the mount serves, the tools reach transcripts through its links and a
search runs in the sandbox. A machine it never serves (one brought over SSH)
gets the same files as read-only filesystem routes over the mount's own
routes, so Read and Grep see what Bash would there, from the rows. Grep then
runs here: the files are fetched a batch at a time and matched off the event
loop, and a long line comes back as windows around its matches, as rg's does.
Each search fetches the files anew, so it takes the newest conversations
first and stops at a byte limit, saying so in its answer.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from typing import Any

from ptc_agent.agent.backends import ReadOnlyStoreError
from ptc_agent.agent.backends.langgraph_store import compile_grep, grep_texts
from ptc_agent.agent.backends.results import GrepAnswer
from ptc_agent.core.paths import THREAD_DIR_NAME, SandboxLayout, WorkspaceLayout
from src.server.services import transcripts
from src.server.services.livefs.history import IndexRoute, TranscriptRoute, blob_bytes, read_file_with_digest

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

    def __init__(self, workspace_id: str, layout: WorkspaceLayout) -> None:
        self._workspace_id = workspace_id
        self._route = TranscriptRoute(workspace_id, layout)
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
            self._workspace_id, short_id or None, _MAX_FILES, newest_first=newest_first
        )
        return [
            f
            for f in listed
            if not relative
            or f"{f.short_id}/{f.path}" == relative
            or f"{f.short_id}/{f.path}".startswith(relative + "/")
        ]

    def _absolute(self, listed: Any) -> str:
        return f"{self.root_prefix}{listed.short_id}/{listed.path}"

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
        return [self._absolute(f) for f in await self._listed(self.root_prefix)]

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
                    found = await read_file_with_digest(self._workspace_id, f.short_id, f.path)
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
        compiled = compile_grep(pattern, case_insensitive)
        cap = head_limit
        if output_mode == "content":
            cap = _MAX_LINES if cap is None else min(cap, _MAX_LINES)
        if cap is not None and cap <= 0:
            return []
        listed = await self._listed(path, newest_first=True)
        if searches is not None:
            # Off the loop: a long filter costs about its length per file.
            listed = await asyncio.to_thread(self._searched, listed, searches)
        taken = _within(listed, _SEARCH_BYTES)
        batches: list[list[Any]] = [[]]
        size = 0
        for f in taken:
            if batches[-1] and size + f.byte_len > _BATCH_BYTES:
                batches.append([])
                size = 0
            batches[-1].append(f)
            size += f.byte_len
        found: list[Any] = []
        for batch in batches:
            if not batch:
                continue
            held = await self._contents(batch)
            # Decoded in the worker thread, as the search reads them.
            texts = ((self._absolute(f), held[f].decode(errors="replace")) for f in batch if f in held)
            found.extend(
                await asyncio.to_thread(
                    grep_texts,
                    texts,
                    compiled,
                    output_mode,
                    show_line_numbers=show_line_numbers,
                    cut_long_lines=True,
                    max_lines=None if cap is None else cap - len(found),
                )
            )
            if cap is not None and len(found) >= cap:
                found = found[:cap]
                break
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

    def __init__(self, computer_id: str, folders: Sequence[tuple[str, str]]) -> None:
        self._computer_id = computer_id
        self._folders = tuple(folders)

    def routes(self, sandbox: Any) -> list[Any]:
        root = sandbox.computer_root.rstrip("/")
        machine = SandboxLayout.for_root(root)
        routes: list[Any] = [
            TranscriptFiles(workspace_id, machine.for_workspace(folder))
            for workspace_id, folder in self._folders
        ]
        routes.append(IndexFile(self._computer_id, root))
        return routes

    def serves(self, workspace_id: str | None) -> bool:
        return workspace_id is not None and any(
            folder_id == str(workspace_id) for folder_id, _ in self._folders
        )

    async def save_transcript(self, target: Any, messages: Any, *, window: Any) -> bool:
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
    )
