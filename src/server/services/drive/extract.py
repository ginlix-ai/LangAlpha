"""How a version's text is read, and the reading of Word and PDF files.

``text_kind`` is the one place that says whether a version's text is its
bytes, is extracted, or is not there. Extraction runs on demand, never at
save (which holds the space's lock while a PDF can take a minute), in a
child process bounded in memory, time and output (``extract_child``), and
is cached by digest (``drive_extracts``). The in-flight map and the
semaphore here are execution context only: they let callers on one worker
share a reading and bound how many run, and are never read as truth.

Reads bytes through the database layer and ``content_bytes``, never the
package core, so the core can import this module.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal

from src.config.core import get_infrastructure_config
from src.server.database.drive import reads
from src.server.database.drive import texts as rows
from src.server.database.drive.records import Content, DriveError, Entry, Space, VersionText
from src.server.database.drive.texts import Extract
from src.server.services.drive import extract_child
from src.server.services.drive.content_bytes import holding, read_range
from src.server.services.drive.extract_child import STATUSES, Status

logger = logging.getLogger(__name__)

__all__ = ["after_write", "answers", "extracted", "reader", "text_kind", "version_text", "warm"]

Kind = Literal["pdf", "docx"]
TextKind = Literal["text", "extracted", "none"]

#: Extracted text kept per digest, as text versions are at most this.
TEXT_CAP_BYTES = 1024 * 1024
#: How long a reader waits for an extraction before ``unavailable``.
EXTRACT_WAIT_S = 30.0
#: Address space a child reading one file may take (Linux only). The child
#: starts at about 95 MiB with the PDF readers loaded; a 300-page text PDF
#: peaked at 68 MiB resident, a PDF whose stream inflates to 300 MB of
#: blanks at 657 MiB.
EXTRACT_MEMORY_BYTES = 512 * 1024 * 1024
#: A child still reading after this is killed and its file cached ``failed``.
EXTRACT_DEADLINE_S = 60.0
#: Children reading files at once on this worker, Word and PDF together.
EXTRACT_CONCURRENCY = 2
#: A killed child is reaped by the loop either way; the wait can outlast it
#: where a paused stdout reader never sees the pipe close.
_REAP_WAIT_S = 5.0
RETRY_AFTER_S = 5

_PDF_TYPE = "application/pdf"
_DOCX_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_GENERIC_TYPES = {"", "application/octet-stream", "application/zip", "binary/octet-stream"}


def pending(message: str) -> DriveError:
    """Not ready within the wait; the work goes on and is cached."""
    return DriveError("unavailable", message, retry_after=RETRY_AFTER_S)


def reader(name: str, content_type: str | None, is_text: bool) -> Kind | None:
    """Which reader extracts a version: by the type it was saved with (a
    PDF can be all ASCII, so over its bytes being text), and by the file's
    name only for binary bytes saved with no type that says. So a Markdown
    file renamed ``.pdf`` keeps its text versions text."""
    media = (content_type or "").split(";")[0].strip().lower()
    if media == _PDF_TYPE:
        return "pdf"
    if media == _DOCX_TYPE:
        return "docx"
    if is_text or media not in _GENERIC_TYPES:
        return None
    lower = name.lower()
    if lower.endswith(".pdf"):
        return "pdf"
    if lower.endswith(".docx"):
        return "docx"
    return None


def text_kind(name: str, content_type: str | None, is_text: bool) -> TextKind:
    """How a version's text is read: its bytes, extracted (Word, PDF), or
    not at all. ``name`` is the file's; ``content_type`` and ``is_text``
    the version's own."""
    if reader(name, content_type, is_text):
        return "extracted"
    return "text" if is_text else "none"


# --- the child ------------------------------------------------------------

_CHILD = str(Path(extract_child.__file__).resolve())
_children: asyncio.Semaphore | None = None


def _semaphore() -> asyncio.Semaphore:
    # Made on first use, inside the worker's own loop.
    global _children
    if _children is None:
        _children = asyncio.Semaphore(EXTRACT_CONCURRENCY)
    return _children


def _child_env() -> dict[str, str]:
    """The worker's import path, so the child finds the PDF readers as the
    worker would (``-P`` keeps the child's own folder off it), and nothing
    else the worker holds: the child parses bytes anyone may have uploaded."""
    env = {k: v for k in ("PATH", "LANG", "LC_ALL") if (v := os.environ.get(k)) is not None}
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return env


async def _run_child(kind: Kind, data: bytes) -> tuple[Status, str | None, bool]:
    """``(status, text, truncated)`` of one file, read in a child process
    bounded in memory and time. A child killed at its deadline, or one that
    dies without an answer, is ``failed``, which is tried again later."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-P",
        _CHILD,
        kind,
        str(EXTRACT_MEMORY_BYTES),
        str(TEXT_CAP_BYTES),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=_child_env(),
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(data), EXTRACT_DEADLINE_S)
    except TimeoutError:
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), _REAP_WAIT_S)
        except TimeoutError:
            pass
        logger.info("[Drive] %s extraction stopped at its deadline", kind)
        return extract_child.FAILED, None, False
    except BaseException:
        # Cancelled: no child outlives the task that started it.
        if proc.returncode is None:
            proc.kill()
        raise
    head, _, body = out.partition(b"\n")
    said, _, cut = head.decode("ascii", "replace").partition(" ")
    status = next((s for s in STATUSES if s == said), None)
    if proc.returncode != 0 or status is None:
        logger.info("[Drive] %s extraction child ended %s", kind, proc.returncode)
        return extract_child.FAILED, None, False
    if status != extract_child.OK:
        return status, None, False
    return extract_child.OK, body[:TEXT_CAP_BYTES].decode("utf-8", "replace"), cut == "1"


# --- cached readings ------------------------------------------------------


def _max_input_bytes() -> int:
    """Files past what one request may carry are not read out: the worker
    holds the whole file while the child reads it."""
    return get_infrastructure_config().drive.max_request_bytes


async def _read_out(found: Content, kind: Kind) -> tuple[Status, str | None, bool]:
    """``found`` read by a child, ``EXTRACT_CONCURRENCY`` at a time on this
    worker. Its turn comes first: a reading waiting for one holds none of
    the memory every save on the worker shares."""
    async with _semaphore(), holding(found.size):
        data = await read_range(found, 0, found.size)
        return await _run_child(kind, data)


async def _reading(space: Space, found: Content, kind: Kind) -> Extract:
    if found.size > _max_input_bytes():
        status, text, truncated = extract_child.TOO_LARGE, None, False
    else:
        status, text, truncated = await _read_out(found, kind)
    await rows.put_extract(space.space_id, found.sha256, kind, status, text, truncated)
    return Extract(kind=kind, status=status, text=text, truncated=truncated, retry=False)


#: Readings running on this worker, so readers of one digest share one.
_reading_now: dict[tuple[str, str], asyncio.Task[Extract]] = {}
#: Warm-ups started here, held so they are not collected mid-run.
_background: set[asyncio.Task[None]] = set()


def shared(running: dict, key: tuple[str, str], start: Callable[[], Awaitable]) -> asyncio.Task:
    """The task ``running`` holds under ``key``, started if none: callers on
    this worker wanting the same result wait on one computation."""
    task = running.get(key)
    if task is None:
        task = asyncio.create_task(start())
        running[key] = task

        def done(t: asyncio.Task) -> None:
            running.pop(key, None)
            if not t.cancelled() and t.exception() is not None:
                logger.debug("[Drive] %s ended: %s", key, t.exception())

        task.add_done_callback(done)
    return task


def answers(cached: Extract | None, kind: Kind) -> bool:
    """Whether ``cached`` stands for a version read as ``kind``. The cache
    is by digest, and the same bytes can sit under a name that picks the
    other reader: the text that one read is these bytes' text, and anything
    else it answered says nothing about this one."""
    if cached is None or cached.retry:
        return False
    return cached.kind == kind or cached.status == extract_child.OK


async def extracted(space: Space, found: Content, kind: Kind, wait: float | None) -> Extract:
    """``found``'s extract, cached or read now, waiting ``wait`` seconds
    (None: until done) before ``unavailable``; the reading goes on."""
    cached = (await rows.extracts(space.space_id, [found.sha256])).get(found.sha256)
    if cached is not None and answers(cached, kind):
        return cached
    task = shared(
        _reading_now,
        (space.space_id, f"{kind}:{found.sha256}"),
        lambda: _reading(space, found, kind),
    )
    try:
        return await asyncio.wait_for(asyncio.shield(task), wait)
    except TimeoutError:
        raise pending("The file's text is still being read; try again shortly") from None


async def version_text(space: Space, entry_id: str, seq: int) -> VersionText:
    """Version ``seq``'s text: as stored for a text version, extracted for a
    Word or PDF one (``unavailable`` while that takes longer than
    ``EXTRACT_WAIT_S``), ``not_text`` for anything else."""
    entry = await reads.live_file(space.space_id, entry_id)
    found = await reads.content_of(space.space_id, entry.entry_id, seq)
    if found is None:
        raise DriveError("not_found", f"{entry.path} has no version {seq}")
    how = text_kind(entry.name, found.content_type, found.is_text)
    if how == "none":
        raise DriveError("not_text", f"Version {seq} of {entry.path} has no text to show")
    if how == "text":
        data = await read_range(found, 0, found.size)
        return VersionText(
            seq=seq,
            sha256=found.sha256,
            source="text",
            status=extract_child.OK,
            text=data.decode("utf-8", errors="replace"),
            truncated=False,
        )
    kind = reader(entry.name, found.content_type, found.is_text)
    assert kind is not None
    got = await extracted(space, found, kind, EXTRACT_WAIT_S)
    return VersionText(
        seq=seq,
        sha256=found.sha256,
        source="extracted",
        status=got.status,
        text=got.text,
        truncated=got.truncated,
    )


async def warm(space: Space, sha256: str, kind: Kind) -> None:
    """Extract ``sha256``'s text ahead of the first reader. Best effort:
    losing it costs that reader one extraction."""
    try:
        found = await reads.content_by_sha(space.space_id, sha256)
        if found is not None:
            await extracted(space, found, kind, None)
    except Exception as e:  # noqa: BLE001 - never raises, by contract
        logger.debug("[Drive] warm-up of %s skipped: %s", sha256, e)


def after_write(space: Space, entry: Entry) -> None:
    """Start reading out a live memo's new head when it is a Word or PDF
    file, once its version has committed; returns at once."""
    if not entry.is_memo or not entry.sha256:
        return
    kind = reader(entry.name, entry.content_type, bool(entry.is_text))
    if kind is None:
        return
    task = asyncio.create_task(warm(space, entry.sha256, kind))
    _background.add(task)
    task.add_done_callback(_background.discard)
