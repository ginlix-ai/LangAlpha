"""Shared result shapes the filesystem backends hand back to the tools."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, TypedDict


class EditTextResult(TypedDict, total=False):
    """What ``aedit_text`` reports back.

    ``size`` is the character count of the file after the edit. It is here so a
    caller that has to judge the edited file against a size cap does not read
    the file back through the backend it just wrote through.
    """

    success: bool
    error: str
    message: str
    occurrences: int
    size: int


class WriteTextResult(TypedDict, total=False):
    """What a route's ``awrite_text`` may report instead of a bare ``True``.

    A DB-backed file applies a write as changes to rows, and ``message`` says
    which ones (created, updated, deleted), so the agent learns the effect of
    its write without re-reading the file. The Write tool shows it in place
    of the byte count.
    """

    success: bool
    message: str


class GrepAnswer(list):
    """A Grep answer with ``notes`` for whoever reads it, each saying what the
    search left out, such as files a route stopped before. A plain list
    answer has none."""

    def __init__(self, entries: Iterable[Any] = (), notes: Iterable[str] = ()) -> None:
        super().__init__(entries)
        self.notes = tuple(notes)
