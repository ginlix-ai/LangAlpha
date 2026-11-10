"""Conversation search as the command palette reads it.

Every range is ``[start, end)`` in UTF-16 code units, the unit a JavaScript
string is indexed in, into the string beside it.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel


class ThreadHit(BaseModel):
    thread_id: str
    #: The thread's title, or its first message (up to 120 characters) when
    #: it has none.
    title: str
    workspace_id: str
    workspace_name: str
    #: The automation that started the thread, if one did.
    automation_name: Optional[str] = None
    updated_at: datetime
    #: Where the terms are in ``title``.
    match: List[List[int]]


class TurnHit(BaseModel):
    thread_id: str
    thread_title: str
    workspace_id: str
    workspace_name: str
    turn_index: int
    #: Which side held every term: the user's message, preferred, or the
    #: agent's answer.
    side: Literal["user", "agent"]
    #: About 160 characters of that side around its first match, cleaned of
    #: markdown, with ``…`` where text was cut away.
    snippet: str
    #: Where the terms are in ``snippet``.
    matches: List[List[int]]
    turn_at: datetime


class FileHit(BaseModel):
    """A file in one of the user's workspaces, as its last backup holds it."""

    workspace_id: str
    workspace_name: str
    #: Relative to the workspace's folder.
    path: str
    name: str
    #: In bytes.
    size: int
    #: When the file last changed on its computer, else when it was backed up.
    modified_at: datetime
    #: Where the terms are in ``path``.
    match: List[List[int]]


class SearchCursors(BaseModel):
    #: Pass back with ``kinds`` set to that kind alone for the next page;
    #: None when the group has no more.
    threads: Optional[str] = None
    turns: Optional[str] = None
    files: Optional[str] = None


class SearchResponse(BaseModel):
    threads: List[ThreadHit]
    turns: List[TurnHit]
    #: Files whose name holds every term first, then those whose folders
    #: complete the match; newest first within each.
    files: List[FileHit]
    next_cursor: SearchCursors
    #: The kinds whose search took too long and came back empty; the others
    #: are whole. Every kind asked for timing out is a 503 instead.
    failed: List[Literal["threads", "turns", "files"]] = []


class TurnPreview(BaseModel):
    thread_id: str
    title: str
    workspace_id: str
    workspace_name: str
    turn_index: int
    turn_at: datetime
    #: Each clipped to 4000 characters, with ``…`` when cut.
    user_text: str
    answer_text: str
