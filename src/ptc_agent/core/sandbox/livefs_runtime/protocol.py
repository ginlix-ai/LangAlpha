"""Names the host and the sandbox daemon both use.

Stdlib only: it ships into the sandbox with the daemon, and the endpoint, the
tools and the mount service import it on the host.
"""

import json
from enum import StrEnum
from typing import Literal, NotRequired, TypedDict

#: The package directory under ``_internal/src/``, run as ``python3 -m livefs``.
PACKAGE_NAME = "livefs"

#: Where the files are served. Outside the computer root, so nothing walking
#: the root meets it; the paths the agent uses are symlinks into it, which
#: walkers, ``rm -r`` and backups leave alone unless told to follow them.
MOUNT = "/mnt/livefs"

#: Where each daemon start mounts a fresh directory that ``MOUNT`` then
#: points at.
GENERATIONS = "/mnt/.livefs"

#: The server's endpoint; each action is a path under it (``/list``, ``/write``).
PREFIX = "/api/v1/livefs"

#: A new token for ``start``, in the environment of the root command that
#: runs it, never on a command line. Agent code running as another user
#: (Docker) can neither read nor replace it there, as it could a file; where
#: agent code runs as root (Daytona) it can read the token, which reaches
#: only this user's files and only until it runs out.
CONFIG_ENV = "LIVEFS_CONFIG"

#: The tool puts its call id here, which is how a save reaches the result of
#: the tool call whose command made it.
CALL_ENV = "LIVEFS_CALL_ID"

#: The call id of the process a request is for, read from its ``CALL_ENV``.
CALL_HEADER = "X-Livefs-Call"

#: Marks a save of bytes nobody has written yet, as a shell's `> file` makes
#: before its command writes; the server does not report its refusal.
PROVISIONAL_HEADER = "X-Livefs-Provisional"

#: The most one file served here holds. The server refuses a larger body, so
#: the daemon refuses the write that would grow a file past it, where the
#: program sees it, rather than at close, which most programs never check.
MAX_FILE_BYTES = 256 * 1024

#: A listing may carry a small file's text as its entry's ``content``, the
#: same bytes ``/read`` answers for that version, so reading it costs no
#: request of its own. Up to this much per file (as UTF-8) ...
INLINE_MAX_BYTES = 16 * 1024
#: ... and this much per listing; past it, entries leave it out.
INLINE_LISTING_MAX_BYTES = 128 * 1024
#: A listing that carries the directories below it (``below``) carries up to
#: this much across all of them, which a search then reads without asking.
INLINE_TREE_MAX_BYTES = 512 * 1024


class WireEntry(TypedDict):
    """One name in a ``/list`` answer."""

    name: str
    type: Literal["file", "dir"]
    size: NotRequired[int]
    version: NotRequired[str]
    #: Absent reads as writable.
    writable: NotRequired[bool]
    content: NotRequired[str]


class WireListing(TypedDict):
    """A ``/list`` answer, and each directory its ``below`` carries."""

    entries: list[WireEntry]
    #: New files can be made in it.
    writable: NotRequired[bool]
    #: Made up by the server, so it changes only when the host runs ``link``.
    structural: NotRequired[bool]
    #: The directories below it, each whole, by path relative to it.
    below: NotRequired[dict[str, "WireListing"]]


class SaveAnswer(TypedDict):
    """A ``/write`` answer."""

    version: str
    size: int
    #: Stored byte for byte as sent, not rewritten by its route.
    as_sent: bool


class RemovedAnswer(TypedDict):
    """A ``/write`` answer for a save that deleted its file, as an
    automation's ``"status": "deleted"`` does: no version or content is left."""

    removed: Literal[True]


class Refusal(StrEnum):
    """The ``code`` of a refused request, which the daemon answers as an errno."""

    NOT_FOUND = "not_found"
    IS_DIRECTORY = "is_directory"
    NOT_DIRECTORY = "not_directory"
    EXISTS = "exists"
    CHANGED = "changed"
    READ_ONLY = "read_only"
    INVALID = "invalid"
    PRECONDITION_REQUIRED = "precondition_required"
    TOO_LARGE = "too_large"
    UNAVAILABLE = "unavailable"


def code_of(body: bytes) -> str | None:
    """A refusal's ``code``; None for a body that is not one, such as a
    proxy's error page."""
    try:
        answer = json.loads(body)
    except ValueError:
        return None
    return answer.get("code") if isinstance(answer, dict) else None


def etag(version: str) -> str:
    """A version as ``/read`` answers it in ``ETag`` and a save names it in ``If-Match``."""
    return f'"{version}"'


def etag_version(tag: str | None) -> str | None:
    """The version an ``ETag`` or ``If-Match`` names; None for no tag.

    A CDN or proxy that re-encodes the body on the way, as one does
    decompressing for a client that asked for no encoding, marks the tag weak
    (``W/"v"``). It names the same version, which the server checks a save
    against, so the weak form reads as the strong one.
    """
    return (tag or "").strip().removeprefix("W/").strip('"') or None


class MountError(StrEnum):
    """Why the mount is not serving, or a workspace not linked. ``start`` and
    ``link`` answer all but the last two, which the host adds."""

    NO_CONFIG = "no_config"
    BAD_CONFIG = "bad_config"
    #: Not root, or no /dev/fuse: fixed for the sandbox's life.
    UNSUPPORTED = "unsupported"
    #: libfuse is being installed, so asking again soon may serve.
    INSTALLING = "installing"
    UNREACHABLE = "unreachable"
    START_FAILED = "start_failed"
    LINK_FAILED = "link_failed"
    #: ``start`` found other code shipped than the host expects: an asset
    #: sync is still replacing it, so the host asks again once it lands.
    STALE_CODE = "stale_code"
    #: The command printed nothing the host could read.
    UNANSWERED = "unanswered"
    #: The computer's folders were moving, so nothing was asked.
    BUSY = "busy"
