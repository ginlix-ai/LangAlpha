"""Grep over server-held text, in the sandbox grep's output modes.

The routes whose files are rows or blobs search them here with RE2, either
held in memory (``grep_texts``) or read a page at a time (``PageGrep``), so
what is held at once stays bounded.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import re
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from typing import Any

import re2

from ptc_agent.core.sandbox.grep_render import (
    _GREP_WINDOWS_PER_LINE,
    GREP_LINE_CHARS,
    GrepLine,
    cut_grep_line,
)

_REFUSED_NOTE = (
    "[Grep did not search {path}: its files are searched with RE2, which "
    "refuses this pattern ({reason}). Grep that path again without that "
    "syntax to search them.]"
)


class UnsupportedPattern(ValueError):
    """A Grep pattern RE2 refuses, such as one with ``(?x)``, which rg runs.

    Raised rather than answered with no matches, which would read as nothing
    there: the search answers with ``note`` instead."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    def note(self, path: str) -> str:
        return _REFUSED_NOTE.format(path=path, reason=self.reason)


def compile_grep(pattern: str, case_insensitive: bool) -> Any:
    """``pattern`` compiled for ``grep_texts``; raises ``UnsupportedPattern``
    for one RE2 refuses.

    RE2 matches in time linear in the line and lets go of the GIL while it
    does, where ``re`` can take seconds over one long line (``.*x`` over a
    JSONL event) and holds the event loop throughout, even from a thread.
    Its ``\\w``, ``\\d``, ``\\s`` and ``\\b`` are ASCII only, unlike rg's."""
    options = re2.Options()
    options.case_sensitive = not case_insensitive
    # Otherwise each pattern it refuses is logged to stderr.
    options.log_errors = False
    try:
        return re2.compile(pattern, options=options)
    except re2.error as e:
        reason = e.args[0] if e.args else "unsupported syntax"
        if isinstance(reason, bytes):
            reason = reason.decode(errors="replace")
        raise UnsupportedPattern(str(reason)) from None


def _cut(line: str, matches: Iterator[Any]) -> str:
    """A long line cut as rg's output is cut. The cut takes bytes, as rg
    reports them, but only places the first few matches; the others are only
    counted, so a pattern that matches everywhere holds no list of them."""
    placed = [
        (len(line[: m.start()].encode()), len(line[: m.end()].encode()))
        for m in itertools.islice(matches, _GREP_WINDOWS_PER_LINE)
    ]
    rest = sum(1 for _ in matches)
    return cut_grep_line(line.encode(), placed, total=len(placed) + rest)


def grep_texts(
    texts: Iterable[tuple[str, str]],
    compiled: Any,
    output_mode: str,
    *,
    show_line_numbers: bool,
    head_limit: int | None = None,
    offset: int = 0,
    cut_long_lines: bool = False,
    max_lines: int | None = None,
) -> list[Any]:
    """Grep over files held in memory as (path, content), answered in the
    sandbox grep's output modes, ``compiled`` by ``compile_grep``.

    ``cut_long_lines`` cuts a long line to windows around its matches, as
    the sandbox's are cut, and a content search stops at ``max_lines``."""
    files: list[str] = []
    lines: list[str] = []
    counts: list[tuple[str, int]] = []
    full = False
    for path, content in texts:
        found = 0
        for number, line in enumerate(content.splitlines(), start=1):
            if output_mode != "content":
                if compiled.search(line) is None:
                    continue
                found += 1
                if output_mode != "count":
                    # One line lists the file.
                    break
                continue
            matches = compiled.finditer(line)
            first = next(matches, None)
            if first is None:
                continue
            found += 1
            text = line
            if cut_long_lines and len(line) > GREP_LINE_CHARS:
                text = _cut(line, itertools.chain([first], matches))
            prefix = f"{path}:{number}:" if show_line_numbers else f"{path}:"
            lines.append(GrepLine(prefix + text, path))
            full = max_lines is not None and len(lines) >= max_lines
            if full:
                break
        if found:
            files.append(path)
            counts.append((path, found))
        if full:
            break
    selected: list[Any] = {"content": lines, "count": counts}.get(output_mode, files)
    start = max(0, offset)
    return selected[start : None if head_limit is None else start + head_limit]


# Line ends ``splitlines`` breaks at besides a bare ``\n``, in UTF-8. A text
# holding one is searched line by line, since the whole-text pass only knows
# ``\n``.
_OTHER_BREAKS = tuple(
    ch.encode() for ch in ("\r", "\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029")
)
# What anchors to the whole text rather than to a line, or turns line mode off.
_TEXT_ANCHORED = re.compile(r"\\[Az]|\(\?[a-zA-Z]*-[a-zA-Z]*m")


def whole_text(pattern: str, case_insensitive: bool) -> Any | None:
    """``pattern`` for one RE2 pass over a whole file's bytes, which finds a
    match wherever a line of it would: in line mode, never across a line end.

    The line-by-line search runs in Python, seconds over tens of thousands
    of files; this pass, in C, leaves it only the files that may match. None
    where no such pass is sound.
    """
    if _TEXT_ANCHORED.search(pattern):
        return None
    options = re2.Options()
    options.case_sensitive = not case_insensitive
    options.never_nl = True
    options.log_errors = False
    try:
        return re2.compile(("(?m)" + pattern).encode(), options=options)
    except re2.error:
        return None


def may_match(data: bytes, whole: Any) -> bool:
    """Whether a line of ``data``, valid UTF-8, may match: False only where
    none can. ``whole`` is from ``whole_text``."""
    if b"\r" in data:
        # A CRLF file's lines end as an LF file's do.
        data = data.replace(b"\r\n", b"\n")
    if any(brk in data for brk in _OTHER_BREAKS):
        return True
    return whole.search(data) is not None


class PageGrep:
    """One Grep over files a source reads a page at a time.

    ``search`` takes the pages, each grepped in a thread, and stops asking
    once it has enough. What it found is
    kept in ``found`` as it goes, so a caller whose source fails part way
    still has the matches from the pages before. ``limit`` is how many
    entries the caller wants in all (offset included). ``utf8`` says the
    source holds only valid UTF-8, as a text column does: the whole-text pass
    may then rule a file out unread; otherwise only an ASCII one, since a
    replaced byte could match where the bytes do not.
    """

    def __init__(
        self,
        pattern: str,
        *,
        case_insensitive: bool,
        output_mode: str,
        show_line_numbers: bool,
        limit: int | None = None,
        utf8: bool = False,
    ) -> None:
        self._compiled = compile_grep(pattern, case_insensitive)
        self._whole = whole_text(pattern, case_insensitive)
        self._output_mode = output_mode
        self._show_line_numbers = show_line_numbers
        self._limit = limit
        self._utf8 = utf8
        self.found: list[Any] = []

    @property
    def full(self) -> bool:
        return self._limit is not None and len(self.found) >= self._limit

    async def search(
        self, pages: AsyncIterator[Sequence[tuple[str, bytes]]], *, read_ahead: bool = False
    ) -> None:
        """``read_ahead`` asks for the next page while one is searched, for a
        source whose fetch takes about as long as the search, where back to
        back they would add up; one page ahead at most. Off, a search that
        has enough fetches nothing past it."""
        ahead: asyncio.Future[Any] | None = None
        # Closed on the way out, so the source stops with us.
        async with contextlib.aclosing(pages):
            try:
                while not self.full:
                    page = await (ahead or anext(pages, None))
                    if page is None:
                        return
                    if read_ahead:
                        ahead = asyncio.ensure_future(anext(pages, None))
                    room = None if self._limit is None else self._limit - len(self.found)
                    self.found.extend(await asyncio.to_thread(self._grep, page, room))
            finally:
                if ahead is not None:
                    # A page fetched for nothing, or a fetch that failed after
                    # the search ended, is dropped with it.
                    ahead.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await ahead

    def _grep(self, page: Sequence[tuple[str, bytes]], room: int | None) -> list[Any]:
        texts = [
            (path, data.decode("utf-8", "replace"))
            for path, data in page
            if self._whole is None
            or not (self._utf8 or data.isascii())
            or may_match(data, self._whole)
        ]
        return grep_texts(
            texts,
            self._compiled,
            self._output_mode,
            show_line_numbers=self._show_line_numbers,
            cut_long_lines=True,
            head_limit=room,
            max_lines=room if self._output_mode == "content" else None,
        )
