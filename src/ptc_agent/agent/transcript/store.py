"""The files a transcript is made of: segments, and the manifest naming them.

A manifest records each segment's shape, digest and size, so a render that
changed one turn renders and stores that turn's file and the manifest, not
the whole thread. The files are a cache of the checkpoint: deleting them loses
nothing, the next render rebuilds them.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript.identity import Window, extend
from ptc_agent.agent.transcript.render import (
    SCHEMA_VERSION,
    Segment,
    render_segment,
    segment_shape,
    split_runs,
)
from ptc_agent.core.paths import WorkspaceLayout

MANIFEST = "manifest.json"
TASK_META = "meta.json"
TASKS_DIR = "tasks"

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def transcript_subdir(short_thread_id: str) -> str:
    """A thread's transcript directory, workspace-relative."""
    return f"{WorkspaceLayout.TRANSCRIPTS_DIR}/{short_thread_id}"


def task_relpath(task_id: str) -> str:
    """One background task's directory, relative to its thread's transcript."""
    return f"{TASKS_DIR}/{_UNSAFE.sub('_', task_id) or '_'}"


def task_subdir(short_thread_id: str, task_id: str) -> str:
    """One background task's transcript directory, workspace-relative."""
    return f"{transcript_subdir(short_thread_id)}/{task_relpath(task_id)}"


#: Whether a forked thread's borrowed files still match what it saw:
#: ``changed`` once the thread holding them was edited before the fork's cut,
#: ``deleted`` once it is gone.
BorrowedState = Literal["shown", "changed", "deleted", "unavailable"]


@dataclass(frozen=True)
class BorrowedFiles:
    """Turns ``first`` to ``last`` of a forked thread, whose files are in
    the transcript of thread ``thread_id``, which it was forked from."""

    thread_id: str
    first: int
    last: int
    state: BorrowedState = "shown"

    def as_manifest(self, directory: str, unit: str = "turn") -> dict[str, Any]:
        """The manifest's entry, ``directory`` being that thread's transcript
        as the reader resolves it."""
        return {
            "thread_id": self.thread_id,
            "transcript": directory,
            "files_from": segment_file(unit, self.first),
            "files_through": segment_file(unit, self.last),
        }


@dataclass(frozen=True)
class TranscriptTarget:
    """One agent's transcript: a thread's turns, or one task's runs in it."""

    thread_id: str
    task_id: str | None = None
    #: A forked thread's turns before its own, oldest first, in the
    #: transcripts that hold them: its own files start after the last.
    borrowed: tuple[BorrowedFiles, ...] = ()

    @classmethod
    def for_agent(cls, thread_id: str, checkpoint_ns: str = "") -> TranscriptTarget:
        """Resolve from graph config: a subagent's namespace opens with ``task:<id>``."""
        head = checkpoint_ns.split("|", 1)[0]
        if head.startswith("task:"):
            return cls(thread_id, head[len("task:") :])
        return cls(thread_id)

    @property
    def checkpoint_ns(self) -> str:
        """The namespace this agent's checkpoints are written in."""
        return f"task:{self.task_id}" if self.task_id else ""

    @property
    def unit(self) -> str:
        return "run" if self.task_id else "turn"

    @property
    def manifest(self) -> str:
        return TASK_META if self.task_id else MANIFEST

    @property
    def directory(self) -> str:
        """Workspace-relative, as the summary names it."""
        if self.task_id:
            return task_subdir(self.thread_id[:8], self.task_id)
        return transcript_subdir(self.thread_id[:8])

    @property
    def prefix(self) -> str:
        """Where this agent's files sit in its thread's stored copy."""
        return f"{task_relpath(self.task_id)}/" if self.task_id else ""

    def directory_of(self, number: int) -> str:
        """The directory holding file ``number``: a borrowed one's thread's."""
        for files in self.borrowed:
            if files.first <= number <= files.last:
                return TranscriptTarget(files.thread_id).directory
        return self.directory

    def file_name(self, number: int) -> str:
        """File ``number`` as a summary names it: bare in this agent's
        directory, under its directory when a chat this was forked from
        holds it."""
        name = segment_file(self.unit, number)
        directory = self.directory_of(number)
        return name if directory == self.directory else f"{directory}/{name}"

    def file_path(self, number: int) -> str:
        """File ``number``'s workspace-relative path."""
        return f"{self.directory_of(number)}/{segment_file(self.unit, number)}"

    @property
    def own_first(self) -> int:
        """The first file this agent's own manifest lists."""
        return self.borrowed[-1].last + 1 if self.borrowed else 1


def segment_file(unit: str, number: int) -> str:
    return f"{unit}-{number:04d}.jsonl"


def load_manifest(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def dump_manifest(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, ensure_ascii=False, indent=1, default=str)


def _entry(segment: Segment, unit: str, shape: str, through: str) -> dict[str, Any]:
    return {
        "file": segment_file(unit, segment.number),
        unit: segment.number,
        "at": segment.at,
        "model": segment.model,
        "events": len(segment.lines),
        "first_id": segment.first_id,
        "last_id": segment.last_id,
        "bytes": len(segment.data),
        "sha256": segment.sha256,
        "shape": shape,
        "through": through,
    }


def _reusable(entry: Any, unit: str, number: int, shape: str) -> bool:
    return _carriable(entry, unit, number) and entry.get("shape") == shape


def _carriable(entry: Any, unit: str, number: int) -> bool:
    return (
        isinstance(entry, dict)
        and isinstance(entry.get("shape"), str)
        and entry.get("file") == segment_file(unit, number)
        and isinstance(entry.get("sha256"), str)
        and len(entry["sha256"]) == 64
        and isinstance(entry.get("bytes"), int)
    )


@dataclass
class Directory:
    """One agent's transcript directory, as a render leaves it."""

    manifest: str
    #: Every segment file by name: (sha256, size).
    files: dict[str, tuple[str, int]]
    #: The bytes of the segments rendered this time, by name. A segment left
    #: out was unchanged, and its entry was carried from the previous manifest.
    rendered: dict[str, bytes]


class EarlierTurnsMissing(LookupError):
    """The previous manifest cannot stand in for the turns trimmed from the
    head of the messages: they have to be passed to render them."""


def build_directory(
    messages: list[AnyMessage],
    *,
    unit: str = "turn",
    header: dict[str, Any] | None = None,
    previous: dict[str, Any] | None = None,
    window: Window,
    borrowed: int = 0,
) -> Directory:
    """Render the segments that changed since ``previous``, the manifest the
    stored copy was rendered with, and carry the rest over from it.

    A turn end adds to one turn, so this renders one segment where a whole
    thread's render costs its size in CPU. With no previous manifest, or one
    of another schema, every segment renders.

    ``window`` was trimmed from the head of ``messages`` (see
    ``compaction.window``): its runs' entries are carried from ``previous``
    as they are, and the first turn of ``messages`` is ``window.runs + 1``.
    Each entry records the digest of every message through its turn
    (``identity``), and the last carried one must match ``window``'s: a
    copy rendered from another branch (a live save labelled with an edit's
    fork point) shares its numbering, not its turns. Raises
    ``EarlierTurnsMissing`` when ``previous`` does not hold them.

    A forked thread's first ``borrowed`` turns are files of the thread it was
    forked from, so its own start at ``borrowed + 1`` (the manifest's
    ``first``) and ``window`` is never short of them.
    """
    if window.runs < borrowed:
        raise ValueError(f"window at {window.runs} runs is short of {borrowed} borrowed")
    held: list[Any] = []
    if (
        previous
        and previous.get("schema") == SCHEMA_VERSION
        and previous.get("first", 1) == borrowed + 1
    ):
        held = previous.get("segments") or []
    names: dict[str, str] = {}
    entries: list[dict[str, Any]] = []
    rendered: dict[str, bytes] = {}
    shape = through = ""
    base = window.runs
    trimmed = base - borrowed
    if trimmed:
        carried = held[:trimmed]
        if (
            len(carried) < trimmed
            or not all(
                _carriable(entry, unit, number)
                for number, entry in enumerate(carried, start=borrowed + 1)
            )
            or carried[-1].get("through") != window.digest
        ):
            raise EarlierTurnsMissing(f"{len(carried)} of {trimmed} trimmed turns held")
        entries.extend(carried)
        # The key chains through every turn before it, the trimmed ones too.
        shape, through = carried[-1]["shape"], window.digest
    elif borrowed:
        through = window.digest
    for number, run in enumerate(split_runs(messages), start=base + 1):
        shape = segment_shape(number, run, unit=unit, previous=shape, tool_names=names)
        through = extend(through, run)
        own = number - borrowed
        entry = held[own - 1] if own <= len(held) else None
        if not _reusable(entry, unit, number, shape):
            segment = render_segment(number, run, unit=unit, tool_names=names)
            entry = _entry(segment, unit, shape, through)
            rendered[entry["file"]] = segment.data
        elif entry.get("through") != through:
            # Its render reads the same in messages that differ where no
            # render looks (reasoning, say), or it predates the field.
            entry = {**entry, "through": through}
        entries.append(entry)
    first = {"first": borrowed + 1} if borrowed else {}
    manifest = {"schema": SCHEMA_VERSION, **(header or {}), **first, "segments": entries}
    return Directory(
        manifest=dump_manifest(manifest),
        files={entry["file"]: (entry["sha256"], entry["bytes"]) for entry in entries},
        rendered=rendered,
    )
