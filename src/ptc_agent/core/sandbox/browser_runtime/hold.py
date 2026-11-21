"""The person's hold on the page, which keeps the agent from acting on it.

The hold lives on the machine because this is the one place the agent's commands
and the person both reach, whichever server worker carried the request. Each
viewer holds its own lease, so one viewer handing the page back does not hand
back another's, and a renewal still in flight when its viewer lets go cannot
take the page again. A lease lapses, so a viewer that vanishes cannot keep the
agent out for more than a minute. It stops the agent walking into the person,
not a determined agent, which can reach the engine another way.

Who holds the page, how often it has been taken, and how much of that the agent
has read since are one document, changed only under one lock: a take landing
between the gate's check and its note of what the agent saw would otherwise be
marked seen, and the agent would act on a page the person changed.
"""

from __future__ import annotations

import fcntl
import json
import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TypeVar

from .protocol import (
    CHANGED_MESSAGE,
    HELD_MESSAGE,
    HOLD_ACTIONS,
    HOLDER_PATTERN,
    state_file,
    write_state,
)

#: How long a hold lasts past its last renewal.
CONTROL_LEASE_S = 60.0

_FILE = "hold"
_LOCK = "hold.lock"
_NAVIGATING = "navigating.lock"
_HOLDER = re.compile(HOLDER_PATTERN)

T = TypeVar("T")


@dataclass
class Hold:
    """Each holder whose lease has not lapsed, with when it does; ``epoch``
    counts takes of a page no one held, and ``seen`` is the one the agent last
    read after."""

    leases: dict[str, float] = field(default_factory=dict)
    epoch: int = 0
    seen: int = 0


def valid_holder(holder: str) -> bool:
    return bool(_HOLDER.fullmatch(holder))


def _read() -> Hold:
    try:
        stored = json.loads(state_file(_FILE).read_text())
    except (OSError, ValueError):
        return Hold()
    if not isinstance(stored, dict):
        return Hold()
    leases = stored.get("leases")
    now = time.time()
    return Hold(
        {
            holder: float(until)
            for holder, until in (leases.items() if isinstance(leases, dict) else ())
            if isinstance(holder, str) and isinstance(until, (int, float)) and until > now
        },
        _count(stored.get("epoch")),
        _count(stored.get("seen")),
    )


def _count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _change(apply: Callable[[Hold], T]) -> T:
    """Read the hold, *apply* a change to it and write it back, all under the lock."""
    lock = state_file(_LOCK)
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        hold = _read()
        before = json.dumps(hold.__dict__, sort_keys=True)
        result = apply(hold)
        after = json.dumps(hold.__dict__, sort_keys=True)
        if after != before:
            write_state(_FILE, after)
        return result


@contextmanager
def navigating() -> Iterator[None]:
    """Held while the person's navigation runs, which can outlast their hold."""
    lock = state_file(_NAVIGATING)
    lock.parent.mkdir(parents=True, exist_ok=True)
    with open(lock, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def held() -> bool:
    return bool(_read().leases)


def holds(holder: str) -> bool:
    return holder in _read().leases


def forget() -> None:
    """Drop everything about the last browser's page, once it is gone."""

    def clear(hold: Hold) -> None:
        hold.leases.clear()
        hold.epoch = hold.seen = 0

    _change(clear)


def mark_seen() -> None:
    def see(hold: Hold) -> None:
        hold.seen = hold.epoch

    _change(see)


def moved() -> None:
    """The person took the page somewhere: whatever the agent last read may be
    gone, however late the engine finished taking it there."""

    def move(hold: Hold) -> None:
        hold.epoch += 1

    _change(move)


def control(holder: str, action: str) -> bool:
    """Take, renew or hand back *holder*'s hold on the page; whether it holds it now.

    ``take`` starts or extends its lease, ``renew`` only extends one it still
    has, and ``release`` ends only its own. A take while no one held the page
    moves the epoch, which is how ``refusal`` knows the page may have changed
    under what the agent last read, even after every hold has ended.
    """
    if not valid_holder(holder) or action not in HOLD_ACTIONS:
        raise ValueError(f"not a hold: {holder!r} {action!r}")

    def apply(hold: Hold) -> bool:
        if action == "take":
            if not hold.leases:
                hold.epoch += 1
            hold.leases[holder] = time.time() + CONTROL_LEASE_S
        elif action == "renew":
            if holder in hold.leases:
                hold.leases[holder] = time.time() + CONTROL_LEASE_S
        else:
            hold.leases.pop(holder, None)
        return holder in hold.leases

    return _change(apply)


def refusal(use: str) -> str | None:
    """Why a command of this use may not run now, or None when it may."""
    if use != "look":
        # A navigation the person handed back in the middle of is still taking
        # the page somewhere: the command waits for it to land, and is judged
        # on the page it lands on.
        with navigating():
            pass

    def judge(hold: Hold) -> str | None:
        if use == "look":
            # What is read during a hold can be changed before the hand-back.
            if not hold.leases:
                hold.seen = hold.epoch
            return None
        if hold.leases:
            return HELD_MESSAGE
        if hold.epoch != hold.seen:
            hold.seen = hold.epoch
            return CHANGED_MESSAGE
        return None

    return _change(judge)
