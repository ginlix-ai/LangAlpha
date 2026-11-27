"""A background subagent's commands set aside for a message sent to it.

A subagent's call waits until its command ends, so a message sent to the
task meanwhile would wait behind it. When one arrives, the call returns and
the command keeps running, set aside in the run: still followed, by a task
here that waits on it without taking its end, so the computer's sweep of jobs
nobody follows never ends it and its log stays where the agent was told. The
agent's ``wait_for`` reads its end. Nothing outside the run reads the shelf,
and the run stops what is still on it when it ends (``run_shelf``), the end
a subagent's commands always had; a worker that dies leaves them as it left
a waiting call's.
"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.core.sandbox.jobs import Running

if TYPE_CHECKING:
    from ptc_agent.agent.tools.command_handoff import CommandCall, Render

logger = structlog.get_logger(__name__)

# How long a follow that could not read the computer waits to read it again.
_FOLLOW_RETRY_S = 10


@dataclass
class Aside:
    """A command set aside, as the call that started it left it."""

    running: Running
    backend: Any
    call: CommandCall
    render: Render
    log: str
    #: Done once the command is no longer running; its end is still unread.
    follow: asyncio.Future[None]


class CommandShelf:
    """One subagent run's set-aside commands, by command id."""

    def __init__(self) -> None:
        self._held: dict[str, Aside] = {}

    def keep(
        self, backend: Any, call: CommandCall, running: Running, render: Render, log: str
    ) -> None:
        self._held[running.ref.command_id] = Aside(
            running, backend, call, render, log, _follow(backend, running)
        )

    def get(self, command_id: str) -> Aside | None:
        return self._held.get(command_id)

    def take(self, command_id: str) -> Aside | None:
        """Remove and return a command, for the one call that ends it."""
        return self._held.pop(command_id, None)

    def restore(self, aside: Aside) -> None:
        """Put back a command whose ending call failed, for the run's end
        to stop: until it is stopped it is still the run's. A stop that
        failed had stopped following it, so it is followed again: a cancelled
        follow would read as the command's end."""
        if aside.follow.cancelled():
            aside.follow = _follow(aside.backend, aside.running)
        self._held.setdefault(aside.running.ref.command_id, aside)

    def take_all(self) -> list[Aside]:
        held, self._held = list(self._held.values()), {}
        return held


def _follow(backend: Any, running: Running) -> asyncio.Future[None]:
    return asyncio.ensure_future(_until_it_ends(backend, running))


async def _until_it_ends(backend: Any, running: Running) -> None:
    while True:
        try:
            await backend.watch_job(running)
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - the next read may reach it
            logger.warning(
                "Could not follow a set-aside command",
                command_id=running.ref.command_id,
                error=str(e),
            )
            await asyncio.sleep(_FOLLOW_RETRY_S)


async def unfollow(aside: Aside) -> None:
    """Stop following a set-aside command, leaving it as it is."""
    aside.follow.cancel()
    try:
        await aside.follow
    except asyncio.CancelledError:
        # The follow's own cancel is expected; the call's is not to be lost.
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise


#: The shelf of the subagent run whose calls read it, set where the run's
#: handler starts. A call with none waits on its command as before.
current_shelf: ContextVar[CommandShelf | None] = ContextVar("command_shelf", default=None)
