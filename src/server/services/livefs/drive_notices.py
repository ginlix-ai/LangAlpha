"""The Drive daemon's notices about saves the server never answered, as the
agent reads them in a tool result.

The daemon keeps each notice on the computer until it reaches the server:
it sends them itself (``POST /livefs/drive/notices``) while it can, and the
host takes them from the computer after a command while it cannot
(``drive-notices``). Either way they are filed here, each once by its id,
under the call that wrote the file, exactly as the server's own outcomes
are, so ``outcomes.describe`` words them in the same result.

Whether the daemon can reach the server is what decides if the host goes to
the computer for them: every feed request marks the computer seen, and the
feed asks at least every ``FEED_WAIT_S``. Redis only, as outcomes are: a
lost key costs a line in a result, never a save.
"""

from __future__ import annotations

import logging
from typing import Any

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.livefs_runtime.drive_protocol import FEED_WAIT_S
from src.server.services.livefs import cache, outcomes

logger = logging.getLogger(__name__)

#: A computer not seen this long has a daemon that cannot reach the server
#: (its feed asks every ``FEED_WAIT_S`` at most).
SEEN_TTL_S = FEED_WAIT_S + 15
#: A notice id is remembered this long, so one that arrives both ways files once.
_FILED_TTL_S = 86400
#: Notices one request may carry at most (the daemon keeps 500).
MAX_NOTICES = 500

_KINDS = {"unsaved", "saved", "lost", "more"}


def _seen_key(computer_id: str) -> str:
    return f"{cache.tag(computer_id)}:drive-seen"


async def mark_seen(computer_id: str) -> None:
    client = cache.client()
    if client is None:
        return
    try:
        await client.set(_seen_key(computer_id), 1, ex=SEEN_TTL_S)
    except Exception:
        logger.warning("drive daemon not marked seen", exc_info=True)


async def seen(computer_id: str) -> bool:
    """Whether the computer's Drive daemon reached the server lately. False
    when Redis cannot say, so the host asks the computer itself."""
    client = cache.client()
    if client is None:
        return False
    try:
        return bool(await client.exists(_seen_key(computer_id)))
    except Exception:
        logger.warning("drive daemon seen not read", exc_info=True)
        return False


def shown(root_dir: str, space: str, rel: str) -> str:
    """A Drive path as the computer shows it (as the mount endpoint's)."""
    tail = f"/{rel.strip('/')}" if rel.strip("/") else ""
    return f"{root_dir.rstrip('/')}/{SandboxLayout.DRIVE_DIR}/{space}{tail}"


def outcome(root_dir: str, note: dict[str, Any]) -> dict[str, Any] | None:
    """The outcome a notice files as; None for one that is not a notice."""
    kind = note.get("kind")
    space, rel = note.get("space"), note.get("path")
    if (
        not isinstance(kind, str)
        or kind not in _KINDS
        or not isinstance(space, str)
        or not isinstance(rel, str)
    ):
        return None
    if kind == "more":
        count = note.get("count")
        if not isinstance(count, int) or count < 1:
            return None
        root = shown(root_dir, "", "").rstrip("/")
        return {
            "op": "write",
            "ok": True,
            "path": root,
            "report": (
                f"...and {count} more notices about Drive saves on this computer, too many "
                "to keep here; check the Drive files these commands wrote."
            ),
        }
    path = shown(root_dir, space, rel)
    if kind == "unsaved":
        reason = str(note.get("reason") or "the Drive's server did not answer")[:200]
        return {
            "op": "write",
            "ok": True,
            "path": path,
            "report": (
                f"NOT SAVED YET: {path} ({reason}). This computer keeps the file and "
                "keeps sending it; a later result says when it is saved, or that it was not."
            ),
        }
    if kind == "saved":
        return {
            "op": "write",
            "ok": True,
            "path": path,
            "report": f"Saved now: {path}.",
        }
    return {
        "op": "write",
        "ok": False,
        "path": path,
        "code": "lost",
        "error": str(
            note.get("reason") or "the computer could not keep it to send again"
        )[:200],
    }


async def _first(note_id: str) -> bool:
    """Whether this is the first time ``note_id`` is filed."""
    client = cache.client()
    if client is None:
        return True
    try:
        return bool(
            await client.set(f"drive-note:{note_id}", 1, nx=True, ex=_FILED_TTL_S)
        )
    except Exception:
        logger.warning("drive notice id not recorded", exc_info=True)
        return True


async def _forget(note_id: str) -> None:
    client = cache.client()
    if client is None:
        return
    try:
        await client.delete(f"drive-note:{note_id}")
    except Exception:
        logger.warning("drive notice id not let go", exc_info=True)


async def file(
    computer_id: str,
    root_dir: str,
    notes: list[dict[str, Any]],
    *,
    own_call: str | None = None,
) -> list[dict[str, Any]]:
    """File each notice not filed yet under its call. The ones for
    ``own_call`` (the command the host just ran, whose own outcomes it has
    already collected) are answered instead, for the caller to show."""
    own: list[dict[str, Any]] = []
    for note in notes[:MAX_NOTICES]:
        if not isinstance(note, dict) or not isinstance(note.get("id"), str):
            continue
        filed = outcome(root_dir, note)
        if filed is None or not await _first(note["id"][:64]):
            continue
        call = outcomes.valid_call_id(note.get("call"))
        if own_call is not None and call == own_call:
            own.append(filed)
            continue
        if not await outcomes.record(computer_id, call, filed):
            # Not filed after all: let the id go, so a delivery again files it.
            await _forget(note["id"][:64])
    return own
