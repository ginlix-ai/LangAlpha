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
from typing import Any

from langchain_core.messages import AnyMessage

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


@dataclass(frozen=True)
class TranscriptTarget:
    """One agent's transcript: a thread's turns, or one task's runs in it."""

    thread_id: str
    task_id: str | None = None

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


def _entry(segment: Segment, unit: str, shape: str) -> dict[str, Any]:
    return {
        "file": segment_file(unit, segment.number),
        unit: segment.number,
        "at": segment.at,
        "events": len(segment.lines),
        "first_id": segment.first_id,
        "last_id": segment.last_id,
        "bytes": len(segment.data),
        "sha256": segment.sha256,
        "shape": shape,
    }


def _reusable(entry: Any, unit: str, number: int, shape: str) -> bool:
    return (
        isinstance(entry, dict)
        and entry.get("shape") == shape
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


def build_directory(
    messages: list[AnyMessage],
    *,
    unit: str = "turn",
    header: dict[str, Any] | None = None,
    previous: dict[str, Any] | None = None,
) -> Directory:
    """Render the segments that changed since ``previous``, the manifest the
    stored copy was rendered with, and carry the rest over from it.

    A turn end adds to one turn, so this renders one segment where a whole
    thread's render costs its size in CPU. With no previous manifest, or one
    of another schema, every segment renders.
    """
    held: list[Any] = []
    if previous and previous.get("schema") == SCHEMA_VERSION:
        held = previous.get("segments") or []
    names: dict[str, str] = {}
    entries: list[dict[str, Any]] = []
    rendered: dict[str, bytes] = {}
    shape = ""
    for number, run in enumerate(split_runs(messages), start=1):
        shape = segment_shape(number, run, unit=unit, previous=shape, tool_names=names)
        entry = held[number - 1] if number <= len(held) else None
        if not _reusable(entry, unit, number, shape):
            segment = render_segment(number, run, unit=unit, tool_names=names)
            entry = _entry(segment, unit, shape)
            rendered[entry["file"]] = segment.data
        entries.append(entry)
    manifest = {"schema": SCHEMA_VERSION, **(header or {}), "segments": entries}
    return Directory(
        manifest=dump_manifest(manifest),
        files={entry["file"]: (entry["sha256"], entry["bytes"]) for entry in entries},
        rendered=rendered,
    )
