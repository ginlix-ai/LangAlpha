"""Claude Code's own session records, kept verbatim in the thread's checkpoint.

A Claude Code thread resumes Claude's session each turn instead of rebuilding
it from langalpha's messages: a fresh CLI fed the history moves its own
context blocks, which invalidates every earlier signed thinking block and the
prompt cache past the system prompt. So the JSONL records the CLI writes are
the session, and they are stored exactly as written.

They live in the checkpoint, in the ``claude_session`` channel, beside
``messages``. A turn then commits its records in the same checkpoint write as
the messages they explain, so a rollback, a fork or a retry from any
checkpoint resumes the session as it stood at that point, and any worker that
holds the run's lock can resume it. The channel is a ``DeltaChannel`` for the
reason ``messages`` is: a long session re-serialized whole at every step
would grow storage with the square of its length.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from langgraph.channels import DeltaChannel

SESSION_SNAPSHOT_FREQUENCY = 250


class SessionDrift(RuntimeError):
    """The CLI's session file and the checkpoint stopped agreeing.

    Raised when the SDK drops a mirror batch (its ``mirror_error``) or when a
    step's records do not arrive in time. The turn fails rather than commit a
    checkpoint that cannot resume what the thread shows.
    """


class SessionRecords(TypedDict, total=False):
    """The channel's value: Claude's session id and its transcript, in write order."""

    session_id: str
    entries: list[dict[str, Any]]


class SessionWrite(TypedDict):
    """One write to the channel: records the CLI appended, in its order."""

    session_id: str
    entries: list[dict[str, Any]]


def session_records_reducer(
    state: SessionRecords | None, writes: Sequence[SessionWrite | Sequence[SessionWrite]]
) -> SessionRecords:
    """Append records, once each.

    A record with a ``uuid`` is kept the first time it is seen, which makes a
    retried mirror batch harmless; one without (titles, mode markers) is
    appended as written. Folding two batches separately or together gives
    the same value, which ``DeltaChannel`` requires to replay its writes.
    A write naming another session id starts that session over: the CLI
    mints a new id only when it starts a new session, and nothing of the old
    one is resumed after that.
    """
    session_id = (state or {}).get("session_id")
    entries = list((state or {}).get("entries") or [])
    seen = {e["uuid"] for e in entries if e.get("uuid")}
    for w in (w for item in writes for w in ([item] if isinstance(item, dict) else item)):
        if session_id and w["session_id"] != session_id:
            entries, seen = [], set()
        session_id = w["session_id"]
        for entry in w["entries"]:
            uid = entry.get("uuid")
            if uid in seen:
                continue
            if uid:
                seen.add(uid)
            entries.append(entry)
    out: SessionRecords = {"entries": entries}
    if session_id:
        out["session_id"] = session_id
    return out


def session_channel() -> DeltaChannel:
    return DeltaChannel(session_records_reducer, snapshot_frequency=SESSION_SNAPSHOT_FREQUENCY)


class Arrivals:
    """Values the CLI's side reported, by key, and a wait for the ones a step needs."""

    def __init__(self, what: str) -> None:
        self._what = what
        self._got: dict[str, str] = {}
        self._changed = asyncio.Condition()

    async def add(self, items: Mapping[str, str]) -> None:
        async with self._changed:
            self._got.update(items)
            self._changed.notify_all()

    async def wait(self, keys: Sequence[str], timeout: float) -> list[str]:
        """The value of each key, once all have arrived; ``SessionDrift`` if they don't in time."""

        async def ready() -> None:
            async with self._changed:
                await self._changed.wait_for(lambda: all(k in self._got for k in keys))

        try:
            await asyncio.wait_for(ready(), timeout)
        except TimeoutError:
            missing = [k for k in keys if k not in self._got]
            raise SessionDrift(f"{self._what} {missing} did not arrive within {timeout:g}s") from None
        return [self._got[k] for k in keys]


class CheckpointSessionStore:
    """The SDK's ``SessionStore``, written back through the checkpoint.

    ``append`` never writes to a database: it holds the records until the
    runtime's next step hands them to the graph as a channel write, which the
    run's fenced saver commits with the step's messages. A worker that has
    lost the run's lock therefore cannot write a record, the same way it
    cannot write a message. ``arrived`` lets a step wait until the records
    behind the messages it is about to commit are here, so a checkpoint never
    holds a message whose record it lacks.

    The store is never read. Given a custom transport, the SDK skips loading
    the session through it; the launcher writes the session file from the
    checkpoint instead (``launcher.py``), so ``load`` only completes the protocol.
    Only the main transcript is kept: the CLI writes another only for a
    subagent of its own Agent tool, which a turn never offers, so one arriving
    is drift.
    """

    def __init__(self) -> None:
        #: (session id, record), in the order the CLI wrote them.
        self._pending: list[tuple[str, dict[str, Any]]] = []
        self.arrived = Arrivals("session records")

    async def append(self, key: dict[str, Any], entries: list[dict[str, Any]]) -> None:
        if key.get("subpath"):
            raise SessionDrift(f"the CLI wrote a {key['subpath']} transcript, which a turn never keeps")
        self._pending.extend((key["session_id"], e) for e in copy.deepcopy(entries))
        await self.arrived.add({e["uuid"]: e["uuid"] for e in entries if e.get("uuid")})

    async def load(self, key: dict[str, Any]) -> None:
        return None

    def drain(self, through: Sequence[str] = ()) -> list[SessionWrite]:
        """The records appended since the last drain, as channel writes.

        With ``through``, only up to the last of those records: the CLI goes on
        once a step hands it the tool results, so the next message's records
        can already be here, and they belong to the step that commits it.
        """
        cut = len(self._pending)
        if through:
            want = set(through)
            cut = 1 + max((i for i, (_, e) in enumerate(self._pending) if e.get("uuid") in want), default=-1)
        taken, self._pending = self._pending[:cut], self._pending[cut:]
        writes: list[SessionWrite] = []
        for sid, entry in taken:
            if writes and writes[-1]["session_id"] == sid:
                writes[-1]["entries"].append(entry)
            else:
                writes.append({"session_id": sid, "entries": [entry]})
        return writes
