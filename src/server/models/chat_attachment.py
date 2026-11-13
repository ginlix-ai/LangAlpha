"""A file attached to a chat turn, as every step of the turn sees it.

A request names its files either inline, as a data URL, or by the id of one
uploaded beforehand (``services/chat_attachments``). Each is turned into a
:class:`TurnAttachment` once, when the turn resolves its request, so what
follows reads one shape whichever way the file came: a name, a type, a size,
and bytes it can ask for.
"""

from __future__ import annotations

import mimetypes
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal

AttachmentKind = Literal["image", "pdf", "file"]
# How a file reaches the model: as a content block, as a path with a note that
# the model cannot view its type, or as a path alone.
AttachmentRoute = Literal["native", "unsupported", "file"]

_MB = 1024 * 1024

# The largest image or PDF that goes in as a content block. A larger one is
# still saved to the workspace and reaches the agent by path, which is how a
# file too large for a provider's request goes in anyway.
NATIVE_MAX_BYTES = 32 * _MB
# All of one turn's content blocks together: what a request has always been
# able to carry inline. Past it the rest go in by path, since each block's
# bytes are held for the turn and the provider takes only so much at once.
NATIVE_TURN_MAX_BYTES = 64 * _MB


def kind_of_mime(mime: str) -> AttachmentKind:
    """``image`` or ``pdf`` for a type a model may take as a content block."""
    if mime == "application/pdf":
        return "pdf"
    if mime.startswith("image/"):
        return "image"
    return "file"


_MIME = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*")


def normalize_mime(raw: str | None) -> str:
    """The declared type without parameters, or octet-stream when it is not one.

    Stored as the object's content type, which the history serves the file
    under, so anything that does not parse as a type is not passed on.
    """
    mime = (raw or "").split(";", 1)[0].strip().lower()
    return mime if _MIME.fullmatch(mime) else "application/octet-stream"


def declared_type(raw: str | None, name: str) -> str:
    """:func:`normalize_mime`, or the type the name implies when the uploader
    sent none: a browser leaves it empty for some files, and an image stored
    as octet-stream would never reach the model as one."""
    mime = normalize_mime(raw)
    if mime == "application/octet-stream":
        guessed, _ = mimetypes.guess_type(name, strict=False)
        if guessed:
            return normalize_mime(guessed)
    return mime


def served_type(mime: str) -> str:
    """The content type the store serves an attachment's object under.

    The history links to the object directly, so only types a browser shows
    without running anything keep their own; the rest download.
    """
    if kind_of_mime(mime) != "file" and "svg" not in mime:
        return mime
    return "application/octet-stream"


class AttachmentUnreadable(Exception):
    """An attachment's bytes could not be read whole."""


@dataclass(frozen=True, slots=True)
class InlineSource:
    """Bytes the request carried, decoded from its data URL once."""

    data: bytes

    async def read(self) -> bytes:
        return self.data


@dataclass(frozen=True, slots=True)
class StoredSource:
    """An uploaded attachment where it waits: the store's object at
    ``object_key``, or Postgres when there is none.

    ``load`` is bound by whoever resolved the attachment, which knows where
    its bytes are; None from it means they are gone.
    """

    attachment_id: str
    object_key: str | None
    load: Callable[[], Awaitable[bytes | None]] = field(repr=False, compare=False)

    async def read(self) -> bytes:
        raw = await self.load()
        if raw is None:
            raise AttachmentUnreadable(self.attachment_id)
        return raw


@dataclass(eq=False)
class TurnAttachment:
    """One attached file. ``path`` is where the turn saved it, relative to the
    workspace folder, and ``route`` how it reaches the model; both are the
    turn's to set, and a file starts out unsaved and going in by path."""

    name: str
    mime_type: str
    size: int
    source: InlineSource | StoredSource
    path: str | None = None
    route: AttachmentRoute = "file"
    # A PDF's page count, when it was read and could be counted.
    pages: int | None = None
    _kept: bytes | None = field(default=None, repr=False)

    @property
    def kind(self) -> AttachmentKind:
        return kind_of_mime(self.mime_type)

    async def read(self, *, keep: bool = False) -> bytes:
        """The file's bytes, checked against its size. With ``keep`` they stay
        here for the turn's later reads, which is what a file going in as a
        content block wants: the block, the checks and a copy all use them.

        Raises:
            AttachmentUnreadable: The bytes are gone, or not the size declared.
        """
        if self._kept is not None:
            return self._kept
        raw = await self.source.read()
        if len(raw) != self.size:
            raise AttachmentUnreadable(self.name)
        if keep:
            self._kept = raw
        return raw

    def in_hand(self) -> bytes:
        """The bytes without a read: the request's own, or the ones a read
        kept. A file routed ``native`` always has them, since routing it so
        took that read.

        Raises:
            AttachmentUnreadable: A stored file nothing read and kept.
        """
        if self._kept is not None:
            return self._kept
        if isinstance(self.source, InlineSource):
            return self.source.data
        raise AttachmentUnreadable(self.name)

    def release(self) -> None:
        """Let go of the bytes a read kept, once their content block is built:
        the turn holds the attachment to its end."""
        self._kept = None


@dataclass(frozen=True, slots=True)
class MissingAttachment:
    """A file the request named that the turn could not get: an id that
    resolved to nothing this user can use, or inline data that would not
    decode. ``why`` finishes the sentence telling the agent so."""

    name: str
    kind: AttachmentKind
    why: str


@dataclass(frozen=True)
class TurnAttachments:
    """A turn's files, and the ones it was sent without."""

    files: list[TurnAttachment] = field(default_factory=list)
    missing: list[MissingAttachment] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.files or self.missing)
