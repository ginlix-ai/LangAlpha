"""The Drive for the file tools: the user's server-held files at
``<computer root>/.agents/drive/<space>/``, read and written in process.

The Drive's own mount shows the same files at the same paths where a computer
runs it, but the file tools never go through it: this route answers on every
kind of computer, the user's own machine over SSH included, and records each
save with the agent that made it, as the mount records a save by Bash or
code, so one run's saves to a file are one version whichever way they came.
"""

from __future__ import annotations

import codecs
import contextlib
import re
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, Protocol

import structlog

from ptc_agent.agent.backends.results import (
    EditTextResult,
    GrepAnswer,
    RouteRefused,
    RouteUnavailable,
    WriteTextResult,
)
from ptc_agent.agent.backends.read_window import MAX_READ_CHARS
from ptc_agent.agent.backends.text_grep import PageGrep
from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.core.paths import DRIVE_PERSONAL, name_fold
from ptc_agent.core.sandbox.drive_mount import DRIVE_NOT_HERE, DriveDisk
from ptc_agent.core.sandbox.files import FileOverCap
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.path_locks import path_write_lock
from src.server.database.drive.policy import checkout_refusal
from src.server.database.drive.records import (
    Actor,
    Content,
    DriveError,
    Entry,
    Space,
)

logger = structlog.get_logger(__name__)


class DriveService(Protocol):
    """The calls the agent side makes of the Drive service. ``reach``, not
    ``open``: a build mounts the Drive only once the feature check passed."""

    async def reach(self, user_id: str, space_ref: str) -> Space: ...

    async def stat(self, space: Space, path: str | None = None) -> Entry: ...

    async def tree(self, space: Space, path: str = "") -> list[Entry]: ...

    async def content(self, space: Space, path: str | None = None) -> tuple[Entry, Content]: ...

    async def read_range(self, found: Content, offset: int, length: int) -> bytes: ...

    def stream(self, found: Content, start: int = 0, end: int | None = None) -> AsyncIterator[bytes]: ...

    def text_pages(
        self, space: Space, entry_ids: list[str] | None = None, *, under: str | None = None
    ) -> AsyncIterator[list[tuple[Entry, bytes]]]: ...

    async def write(
        self, space: Space, path: str, data: bytes, *, actor: Actor, if_match: str | None = None
    ) -> Entry: ...


def drive_service() -> DriveService:
    """The service, imported on first use: it brings the server's database
    and storage modules, which the agent package does not load otherwise."""
    from src.server.services import drive

    return drive


#: The spaces a computer shows, by the folder each appears as.
SPACE_FOLDERS = (DRIVE_PERSONAL,)

# A file that is not held as text is sniffed for this much before Read
# decides it is binary.
_SNIFF_BYTES = 8192

# The most of a Drive file a whole-file read (Edit, ``aread_text``) holds in
# the worker, shared by every turn on it, and how far from its start a Read
# scans for its window: each Read streams from byte 0, so paging deep into a
# file of gigabytes would fetch it again per page. A Read holds only its
# window, at most Read's own character cap.
_WHOLE_MAX_BYTES = 16 * 1024 * 1024

# Where a line ends, as ``str.splitlines`` ends it, so a streamed Read counts
# lines as one of a file held as text does.
_LINE_END = re.compile("\r\n|[\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029]")

# How much text a Grep with a ``head_limit`` fetches at once, the size of the
# service's own pages, to sort by path before searching it.
_GREP_CHUNK_BYTES = 16 * 1024 * 1024

# An Edit whose file changed between its read and its save is applied again
# to the new text, as often as this.
_EDIT_ATTEMPTS = 3

UNSEARCHED = (
    "[The Drive could not be searched just now, so nothing in it is listed "
    "here. Search it again shortly.]"
)
STOPPED = (
    "[Grep stopped partway through the Drive when it stopped answering: the "
    "matches above are only from the files it reached. Search again shortly "
    "for the rest.]"
)


class DriveRefused(RouteRefused):
    """A Drive call refused, worded for the agent."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


def refusal(error: DriveError, shown: str, ctx: CallContext | None = None) -> str:
    """What the agent is told of a save or read the Drive refused, ``shown``
    being the path as the agent spelled it; ``ctx`` is the call's, for the
    zone a date in it is in and whether the caller can run ``la``."""
    ctx = ctx or CallContext()
    code = error.code
    if code == "checkout_required":
        return checkout_refusal(
            shown,
            error.details.get("path") or shown,
            error.details.get("last_change"),
            ctx.timezone,
            bash=ctx.can_run_la,
        )
    if code == "changed":
        return (
            f"{shown} changed in the Drive since you last read it, so it was "
            "not saved. Read it again and make your change on what it holds now."
        )
    if code == "too_large":
        # The limit is the server's, worded as its sentence: "A file is at most 50 MB".
        limit = error.message[:1].lower() + error.message[1:]
        return (
            f"{shown} was not saved: {limit} in one save. Write it in the workspace, "
            "then copy it in with `cp` where Bash reaches the Drive, or `la drive put`."
        )
    if code == "over_quota":
        return (
            f"{shown} was not saved: the Drive is full. Tell the user; they can "
            "delete files or clear a file's earlier versions."
        )
    if code == "too_many_entries":
        return (
            f"{shown} was not saved: the Drive is full. Tell the user; they can "
            "empty its trash or remove files."
        )
    if code == "invalid_path":
        return f"{shown} was not saved: {error.message}. Pick another name."
    if code == "is_folder":
        return f"{shown} is a folder in the Drive. Name a file inside it."
    if code == "not_folder":
        return f"{shown} was not saved: part of its path is a file in the Drive, not a folder."
    if code == "exists":
        return f"{shown} already exists in the Drive."
    if code == "not_found":
        return f"File not found: {shown}"
    return f"The Drive did not answer for {shown}; try again shortly."


def _this_call(call: CallContext) -> tuple[CallContext, tuple[str | None, str]]:
    """``call`` as the file call being made runs it (its run and agent, as
    Bash and code read them for the mount), and which run of which agent it
    is: two runs of a subagent in one turn share the turn's run id, and only
    the task in the checkpoint namespace tells them apart."""
    try:
        from langgraph.config import get_config

        config = get_config()
    except RuntimeError:
        config = None
    ctx = call.for_call(config)
    return ctx, (ctx.run_id, run_ids()[1])


class DriveRoute:
    """The file tools' Drive, mounted at ``root_prefix``.

    One per agent build, shared by the main agent and its subagents: who a
    save is recorded as made by is read from the run each call is made in,
    never fixed here.
    """

    takes_walk = True
    # Its folder is a link on a computer the Drive's mount serves, which rg
    # follows only when given it, so a Grep from above never searches it.
    searched_only_inside = True

    def __init__(
        self,
        *,
        service: DriveService,
        user_id: str,
        sandbox_backend: Any,
        root_prefix: str,
        call: CallContext | None = None,
    ) -> None:
        if not root_prefix.endswith("/"):
            root_prefix += "/"
        self._drive = service
        self._user_id = user_id
        self._sandbox = sandbox_backend
        self._root_prefix = root_prefix
        self._call = call or CallContext()
        self._spaces: dict[str, Space] = {}
        # The head each agent run last read or saved, by file, which its next
        # Write of that file is conditional on.
        self._seen: dict[tuple[Any, ...], str] = {}

    @property
    def root_prefix(self) -> str:
        return self._root_prefix

    def normalize_path(self, path: str) -> str:
        return self._sandbox.normalize_path(path)

    def virtualize_path(self, path: str) -> str:
        return self._sandbox.virtualize_path(path)

    def validate_path(self, path: str) -> bool:
        return self._sandbox.validate_path(path)

    @property
    def filesystem_config(self) -> Any:
        return self._sandbox.filesystem_config

    # -- paths ------------------------------------------------------------

    def _shown(self, path: str) -> str:
        virtual = self._sandbox.virtualize_path(path)
        return virtual.lstrip("/") if virtual != path else path

    def _split(self, path: str) -> tuple[str | None, str]:
        """The space folder ``path`` is in and the path inside it; no space
        for the Drive's own folder, and ``''`` for a space's top."""
        rest = path[len(self._root_prefix) :] if path.startswith(self._root_prefix) else ""
        folder, _, inner = rest.strip("/").partition("/")
        return (folder or None), inner

    def _absolute(self, folder: str, inner: str) -> str:
        return f"{self._root_prefix}{folder}/{inner}" if inner else f"{self._root_prefix}{folder}"

    async def _space(self, folder: str) -> Space:
        space = self._spaces.get(folder)
        if space is None:
            space = self._spaces[folder] = await self._drive.reach(self._user_id, folder)
        return space

    @staticmethod
    def _key(folder: str, inner: str) -> str:
        """The file as the Drive tells files apart (``path_key``), so two
        spellings of one name share its lock and its remembered head."""
        return f"{folder}/{name_fold(inner)}"

    def _locked(self, folder: str, inner: str) -> Any:
        """One call at a time per file, so a call's read and its save are not
        interleaved with another's; other writers are held off by the save's
        condition."""
        return path_write_lock(f"drive:{self._user_id}", self._key(folder, inner))

    def _file_target(self, path: str) -> tuple[str, str]:
        """The space folder and inner path of a file ``path`` names, or a
        refusal saying where Drive files go."""
        folder, inner = self._split(path)
        if folder not in SPACE_FOLDERS or not inner:
            personal = self._shown(self._absolute(DRIVE_PERSONAL, ""))
            raise DriveRefused(
                f"{self._shown(path)} is not a file in the Drive: its files go "
                f"under {personal.rstrip('/')}/."
            )
        return folder, inner

    # -- reads ------------------------------------------------------------

    async def _content(self, path: str) -> tuple[Entry, Content] | None:
        folder, inner = self._split(path)
        if folder is None or (folder in SPACE_FOLDERS and not inner):
            raise IsADirectoryError(path)
        if folder not in SPACE_FOLDERS:
            return None
        try:
            return await self._drive.content(await self._space(folder), inner)
        except DriveError as e:
            if e.code == "is_folder":
                raise IsADirectoryError(path) from None
            if e.code in ("not_found", "invalid_path"):
                return None
            raise DriveRefused(refusal(e, self._shown(path))) from None

    def _few(self, paths: list[str]) -> str:
        names = ", ".join(self._shown(p) for p in paths[:3])
        return names + (f" and {len(paths) - 3} more" if len(paths) > 3 else "")

    async def _on_disk(self) -> bool:
        return await self._sandbox.sandbox.drive.on_disk() is DriveDisk.ON

    async def _binary(self, path: str, entry: Entry) -> DriveRefused:
        _folder, inner = self._split(path)
        how = (
            "Open it with code at this same path, which reads it in place, so a large file needs no copy first."
            if await self._on_disk()
            else f"Copy it into the workspace with `la drive get {inner}`, then open it with code."
        )
        return DriveRefused(
            f"{self._shown(path)} is not text ({entry.content_type or 'binary'}, "
            f"{byte_size(entry.size or 0)}), and Read shows only text, PDFs and images. {how}"
        )

    async def _long_line(self, path: str, line: int) -> DriveRefused:
        _folder, inner = self._split(path)
        how = (
            "Read it in parts with code, which reads it at this same path."
            if await self._on_disk()
            else f"Copy it into the workspace with `la drive get {inner}`, then read it in parts with code."
        )
        return DriveRefused(
            f"Line {line} of {self._shown(path)} is longer than Read shows at once "
            f"({MAX_READ_CHARS} characters). {how}"
        )

    async def _too_large(self, path: str, size: int) -> DriveRefused:
        _folder, inner = self._split(path)
        how = (
            "Change it with code at this same path."
            if await self._on_disk()
            else (
                f"Copy it into the workspace with `la drive get {inner}`, change it with code, "
                "and send it back with `la drive put`."
            )
        )
        bound = byte_size(_WHOLE_MAX_BYTES)
        return DriveRefused(
            f"{self._shown(path)} is {byte_size(size)}, more than the file tools take whole "
            f"from the Drive ({bound}); Read pages through only its first {bound}. {how}"
        )

    async def _too_deep(self, path: str, line: int) -> DriveRefused:
        _folder, inner = self._split(path)
        code = "read that part with code, such as DuckDB or a few lines of Python."
        how = (
            f"Open it at this same path and {code}"
            if await self._on_disk()
            else f"Copy it into the workspace with `la drive get {inner}`, then {code}"
        )
        return DriveRefused(
            f"Line {line} of {self._shown(path)} is past the first "
            f"{byte_size(_WHOLE_MAX_BYTES)}, as far into a Drive file as Read goes. {how}"
        )

    async def _text(self, path: str, window: tuple[int, int] | None) -> tuple[str, str] | None:
        """The file's text, or only lines ``window`` (offset, limit), with the
        head it is; refuses a file that is not text.

        A file held as text is small and read whole. Anything else is streamed
        and held only from the window's first line, up to ``limit`` lines or
        just past Read's character cap, so Read clips it and says how to go
        on; a whole read refuses past ``_WHOLE_MAX_BYTES`` before reading."""
        found = await self._content(path)
        if found is None:
            return None
        entry, content = found
        if content.is_text:
            text = (await self._drive.read_range(content, 0, content.size)).decode("utf-8")
            if window is not None:
                start, limit = window
                text = "".join(text.splitlines(keepends=True)[start : start + limit])
            return text, content.sha256
        if window is None and content.size > _WHOLE_MAX_BYTES:
            raise await self._too_large(path, content.size)
        start, limit = window if window is not None else (0, None)
        if limit == 0:
            return "", content.sha256
        decoder = codecs.getincrementaldecoder("utf-8")()
        held: list[str] = []
        size = lines = scanned = 0
        skip = start
        first = True
        # A CR that ends a chunk waits for the next, which may open with its LF.
        carry = ""

        def take(part: str) -> bool:
            """Skip ``part`` up to the window, hold the rest; True once held is enough."""
            nonlocal skip, size, lines
            if skip:
                pos = 0
                for end in _LINE_END.finditer(part):
                    pos = end.end()
                    skip -= 1
                    if not skip:
                        break
                part = "" if skip else part[pos:]
            if not part:
                return False
            held.append(part)
            size += len(part)
            lines += sum(1 for _ in _LINE_END.finditer(part))
            return limit is not None and (lines >= limit or size > MAX_READ_CHARS)

        try:
            source = self._drive.stream(content, 0, None)
            async with contextlib.aclosing(source):
                async for chunk in source:
                    if first and b"\x00" in chunk[:_SNIFF_BYTES]:
                        raise await self._binary(path, entry)
                    first = False
                    scanned += len(chunk)
                    part = carry + decoder.decode(chunk)
                    carry = "\r" if part.endswith("\r") else ""
                    if take(part[: len(part) - len(carry)]):
                        break
                    if skip and scanned >= _WHOLE_MAX_BYTES:
                        raise await self._too_deep(path, start + 1)
                else:
                    take(carry + decoder.decode(b"", final=True))
        except UnicodeDecodeError:
            raise await self._binary(path, entry) from None
        except DriveError as e:
            raise DriveRefused(refusal(e, self._shown(path))) from None
        text = "".join(held)
        if limit is not None:
            text = text[: MAX_READ_CHARS + 1]
            if len(text) > MAX_READ_CHARS and _LINE_END.search(text, 0, MAX_READ_CHARS) is None:
                raise await self._long_line(path, start + 1)
            text = "".join(text.splitlines(keepends=True)[:limit])
        return text, content.sha256

    async def _read(self, path: str, window: tuple[int, int] | None) -> str | None:
        folder, inner = self._split(path)
        if folder not in SPACE_FOLDERS or not inner:
            found = await self._text(path, window)
            return None if found is None else found[0]
        # Not under ``_locked``, which keeps an Edit's read and save together:
        # a read saves nothing, and a stale ``_seen`` left by one that raced a
        # save only makes the next save of this file refuse as changed.
        _ctx, run = _this_call(self._call)
        found = await self._text(path, window)
        if found is None:
            # Gone: a save made now is a new file, not one over what was read.
            self._seen.pop((*run, self._key(folder, inner)), None)
            return None
        text, sha = found
        self._seen[(*run, self._key(folder, inner))] = sha
        return text

    async def aread_text(self, file_path: str) -> str | None:
        return await self._read(file_path, None)

    async def aread_range(self, file_path: str, offset: int = 0, limit: int = 2000) -> str | None:
        return await self._read(file_path, (max(0, offset), max(0, limit)))

    async def adownload_file_bytes_capped(
        self, filepath: str, cap: Callable[[bytes], int]
    ) -> bytes | None:
        """The file's bytes for a Read that attaches them (a PDF, an image),
        refusing past ``cap`` of its first bytes, as the sandbox's own
        download does; None where there is no file."""
        found = await self._content(filepath)
        if found is None:
            return None
        _entry, content = found
        try:
            head = await self._drive.read_range(content, 0, min(content.size, 8))
            limit = cap(head)
            if content.size > limit:
                raise FileOverCap(limit)
            return await self._drive.read_range(content, 0, content.size)
        except DriveError as e:
            # Bytes the store could not hand over now: a refusal the Read
            # tool words, as a text read's is, not an error that ends the turn.
            raise DriveRefused(refusal(e, self._shown(filepath))) from None

    # -- writes -----------------------------------------------------------

    async def _save(
        self, path: str, data: bytes, ctx: CallContext, *, if_match: str | None
    ) -> Entry:
        folder, inner = self._file_target(path)
        try:
            return await self._drive.write(
                await self._space(folder),
                inner,
                data,
                actor=Actor.of_call(ctx, user_id=self._user_id, via="tool", sitting=ctx.sitting),
                if_match=if_match,
            )
        except DriveError as e:
            logger.info("Drive save refused", code=e.code)
            raise DriveRefused(refusal(e, self._shown(path), ctx), e.code) from None

    async def awrite_text(self, file_path: str, content: str) -> WriteTextResult:
        folder, inner = self._file_target(file_path)
        ctx, run = _this_call(self._call)
        data = content.encode("utf-8")
        key = (*run, self._key(folder, inner))
        async with self._locked(folder, inner):
            # A file this run read is saved only over what it read; one it
            # never read is written as asked, as a Write in a workspace is.
            entry = await self._save(file_path, data, ctx, if_match=self._seen.get(key))
            if entry.sha256:
                self._seen[key] = entry.sha256
        return {
            "success": True,
            "message": (
                f"Saved {len(data)} bytes to {self._shown(file_path)} in the Drive "
                f"(version {entry.head_seq})."
            ),
        }

    async def aedit_text(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        *,
        replace_all: bool = False,
    ) -> EditTextResult:
        shown = self._shown(file_path)
        if old_string == new_string:
            return {"success": False, "error": "old_string and new_string are identical"}
        try:
            folder, inner = self._file_target(file_path)
            ctx, run = _this_call(self._call)
            async with self._locked(folder, inner):
                for _ in range(_EDIT_ATTEMPTS):
                    found = await self._text(file_path, None)
                    if found is None:
                        return {"success": False, "error": f"File not found: {shown}"}
                    text, read = found
                    occurrences = text.count(old_string)
                    if occurrences == 0:
                        return {"success": False, "error": f"String not found: {old_string!r}"}
                    if occurrences > 1 and not replace_all:
                        return {
                            "success": False,
                            "error": (
                                f"String appears {occurrences} times. Provide more "
                                "context or set replace_all=True."
                            ),
                        }
                    edited = text.replace(old_string, new_string, -1 if replace_all else 1)
                    try:
                        # Conditional on the text this edit was made to, so a
                        # save by anyone else in between is edited, never
                        # overwritten.
                        entry = await self._save(
                            file_path, edited.encode("utf-8"), ctx, if_match=read
                        )
                    except DriveRefused as e:
                        if e.code == "changed":
                            continue
                        raise
                    if entry.sha256:
                        self._seen[(*run, self._key(folder, inner))] = entry.sha256
                    return {
                        "success": True,
                        "occurrences": occurrences if replace_all else 1,
                        "size": len(edited),
                        "message": (
                            f"Edited {shown} in the Drive ({occurrences} occurrences replaced)"
                            if replace_all
                            else f"Edited {shown} in the Drive"
                        ),
                    }
        except DriveRefused as e:
            return {"success": False, "error": str(e)}
        except IsADirectoryError:
            return {"success": False, "error": f"{shown} is a folder in the Drive."}
        return {
            "success": False,
            "error": f"{shown} kept changing in the Drive while it was edited; try again.",
        }

    # -- listing and search ----------------------------------------------

    async def _entries(self, path: str) -> list[tuple[str, str | None, Entry]]:
        """The files at or under ``path``, as (space folder, the folder they
        were listed under, entry): a file alone, under no folder; a folder's
        subtree; or every space for the Drive's own folder, under its top
        (``''``). Never more of the tree than that."""
        folder, inner = self._split(path.rstrip("/") + "/")
        folders = SPACE_FOLDERS if folder is None else (folder,)
        found: list[tuple[str, str | None, Entry]] = []
        for name in folders:
            if name not in SPACE_FOLDERS:
                continue
            space = await self._space(name)
            under: str | None = ""
            try:
                if inner:
                    entry = await self._drive.stat(space, inner)
                    if entry.kind == "file":
                        entries, under = [entry], None
                    else:
                        entries, under = await self._drive.tree(space, entry.path), entry.path
                else:
                    entries = await self._drive.tree(space)
            except DriveError as e:
                if e.code in ("not_found", "invalid_path", "not_folder"):
                    continue
                raise
            found.extend((name, under, e) for e in entries if e.kind == "file")
        return found

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        """The files at or under ``path``, newest first: the composite
        matches the pattern."""
        del pattern
        try:
            found = await self._entries(self.normalize_path(path))
        except Exception as e:  # noqa: BLE001 - the rest of the Glob stands
            logger.warning("Drive listing failed", error_type=e.__class__.__name__)
            raise RouteUnavailable(UNSEARCHED) from e
        found.sort(key=lambda item: item[2].modified_at, reverse=True)
        return [self._absolute(name, e.path) for name, _, e in found]

    async def agrep_rich(
        self,
        pattern: str,
        path: str = ".",
        output_mode: str = "files_with_matches",
        *,
        case_insensitive: bool = False,
        show_line_numbers: bool = True,
        head_limit: int | None = None,
        offset: int = 0,
        searches: Callable[[str], bool] | None = None,
        **_: Any,
    ) -> Any:
        """Search the Drive's text with RE2, as the service pages it.

        Only files the database holds as text are searched; a text file over
        that size is named in a note instead. Context lines and multiline are
        not supported here, as in the other server-held folders.
        """
        search = PageGrep(
            pattern,
            case_insensitive=case_insensitive,
            output_mode=output_mode,
            show_line_numbers=show_line_numbers,
            limit=None if head_limit is None else max(0, offset) + head_limit,
            utf8=True,
        )
        try:
            listed = await self._entries(self.normalize_path(path))
        except Exception as e:  # noqa: BLE001 - said in the answer, not raised
            logger.warning("Drive listing failed", error_type=e.__class__.__name__)
            raise RouteUnavailable(UNSEARCHED) from e
        kept = [
            (name, under, e)
            for name, under, e in listed
            if searches is None or searches(self._absolute(name, e.path))
        ]
        from src.server.services.drive.data_files import is_data_file

        data = sorted(
            self._absolute(name, e.path) for name, _, e in kept if not e.is_text and is_data_file(e.path)
        )
        unsearched = sorted(
            self._absolute(name, e.path)
            for name, _, e in kept
            if not e.is_text and _texty(e) and not is_data_file(e.path)
        )
        # Each space's texts as the service pages them: everything under the
        # folder listed when the walk kept every text file in it, else by id.
        plans: list[tuple[str, str | None, list[str] | None]] = []
        for name in dict.fromkeys(name for name, _, _ in kept):
            under = next(u for n, u, _ in kept if n == name)
            ids = [e.entry_id for n, _, e in kept if n == name and e.is_text]
            listed_texts = sum(1 for n, _, e in listed if n == name and e.is_text)
            whole = under is not None and len(ids) == listed_texts
            plans.append((name, under, None if whole else ids))

        async def in_path_order() -> AsyncIterator[list[tuple[str, bytes]]]:
            # A search that stops at a count must meet files in the order the
            # answer lists them, or each page of results is cut from a
            # different set; the service sends them in id order. Files are
            # fetched a bounded chunk at a time, by path, and each chunk
            # sorted before it is searched.
            for name in sorted(dict.fromkeys(name for name, _, _ in kept)):
                texts = sorted((e for n, _, e in kept if n == name and e.is_text), key=lambda e: e.path)
                if not texts:
                    continue
                space = await self._space(name)
                for chunk in _by_size(texts, _GREP_CHUNK_BYTES):
                    held: list[tuple[Entry, bytes]] = []
                    source = self._drive.text_pages(space, [e.entry_id for e in chunk])
                    async with contextlib.aclosing(source):
                        async for page in source:
                            held.extend(page)
                    held.sort(key=lambda found: found[0].path)
                    yield [(self._absolute(name, entry.path), data) for entry, data in held]

        async def pages() -> AsyncIterator[list[tuple[str, bytes]]]:
            for name, under, ids in plans:
                if ids == []:
                    continue
                space = await self._space(name)
                source = self._drive.text_pages(
                    space, ids, under=(under or None) if ids is None else None
                )
                async with contextlib.aclosing(source):
                    async for page in source:
                        yield [(self._absolute(name, entry.path), data) for entry, data in page]

        notes: list[str] = []
        try:
            await search.search(pages() if head_limit is None else in_path_order(), read_ahead=True)
        except Exception as e:  # noqa: BLE001 - what it found stands, with a note
            logger.warning("Drive search stopped", error_type=e.__class__.__name__)
            if not search.found:
                raise RouteUnavailable(UNSEARCHED) from e
            notes.append(STOPPED)
        # The service pages in its own order; the answer reads in path order.
        page = sorted(search.found, key=_grep_path)[max(0, offset) :]
        if unsearched:
            notes.append(
                f"[Grep did not search {len(unsearched)} Drive text file(s) over "
                f"1 MB: {self._few(unsearched)}. Read them to see their contents.]"
            )
        if data:
            notes.append(
                f"[Grep does not search Drive data files ({self._few(data)}); "
                "query them with code if they may hold the match.]"
            )
        return GrepAnswer(page, notes) if notes else page


class DriveAbsent:
    """``.agents/drive/`` where the build has no Drive: the feature is off,
    or the turn is a shared workspace's. A file tool's write there would make
    a real folder where the Drive's link goes, which backups skip, so every
    file call is refused in the line Bash's guard gives a command there
    (``drive_refusal``). It holds no file, so no search reads it."""

    refuses_writes = True
    fixed_names: tuple[str, ...] = ()
    searched_only_inside = True

    def __init__(self, *, root_prefix: str) -> None:
        self._root_prefix = root_prefix if root_prefix.endswith("/") else root_prefix + "/"

    @property
    def root_prefix(self) -> str:
        return self._root_prefix

    async def aread_text(self, file_path: str) -> str | None:
        raise DriveRefused(DRIVE_NOT_HERE)

    async def aread_range(self, file_path: str, offset: int = 0, limit: int = 2000) -> str | None:
        raise DriveRefused(DRIVE_NOT_HERE)

    async def adownload_file_bytes_capped(
        self, filepath: str, cap: Callable[[bytes], int]
    ) -> bytes | None:
        # Without it the composite reads the reserved path from the disk.
        raise DriveRefused(DRIVE_NOT_HERE)

    async def awrite_text(self, file_path: str, content: str) -> bool:
        raise DriveRefused(DRIVE_NOT_HERE)

    async def aedit_text(
        self, file_path: str, old_string: str, new_string: str, *, replace_all: bool = False
    ) -> EditTextResult:
        return {"success": False, "error": DRIVE_NOT_HERE}

    async def aglob_paths(self, pattern: str, path: str = ".") -> list[str]:
        return []

    async def agrep_rich(self, pattern: str, path: str = ".", **kwargs: Any) -> list[Any]:
        return []


def _texty(entry: Entry) -> bool:
    ctype = (entry.content_type or "").lower()
    return ctype.startswith("text/") or ctype in (
        "application/json",
        "application/xml",
        "application/x-ndjson",
    )


def _by_size(entries: list[Entry], most: int) -> Iterator[list[Entry]]:
    """``entries`` in runs of at most ``most`` bytes, one file to a run past it."""
    run: list[Entry] = []
    held = 0
    for entry in entries:
        if run and held + (entry.size or 0) > most:
            yield run
            run, held = [], 0
        run.append(entry)
        held += entry.size or 0
    if run:
        yield run


def _grep_path(item: Any) -> str:
    path = getattr(item, "path", None)
    if path is not None:
        return path
    return item[0] if isinstance(item, tuple) else item


def byte_size(n: int) -> str:
    size = float(n)
    for unit in ("bytes", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} {unit}" if unit == "bytes" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} bytes"
